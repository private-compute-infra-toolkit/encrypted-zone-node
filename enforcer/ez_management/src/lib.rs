// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod types;
use std::sync::Arc;

use container_manager_requester::{
    ContainerManagerRequester, LoadWorkloadIsolatesRequest, LoadWorkloadManifestsRequest,
};
use ez_management_proto::enforcer::v2::ez_management_service_client::EzManagementServiceClient;
use ez_management_proto::enforcer::v2::load_isolates_request::Request as LoadIsolatesRequestType;
use ez_management_proto::enforcer::v2::load_isolates_response::Response as LoadIsolatesResponseType;
use ez_management_proto::enforcer::v2::{
    IsolatePackageChunk, LoadIsolatesError, LoadIsolatesRequest, LoadIsolatesResponse,
    LoadIsolatesResult, ReadyToLoadPackagesRequest,
};
use grpc_connector::GrpcChannelPool;
use isolate_info::{get_binary_services_index, BinaryServicesIndex, IsolateType};
use manifest_parser::v2::WorkloadManifests;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Streaming;
pub use types::EzManagementError;

const DEFAULT_REQUEST_CHANNEL_BUFFER_SIZE: usize = 512;
const ENV_PACKAGE_OUTPUT_DIR: &str = "EZ_PACKAGE_OUTPUT_DIR";
const DEFAULT_PACKAGE_OUTPUT_DIR: &str = "/tmp";

/// Client for communicating with the EzManagementService.
#[derive(Clone)]
pub struct EzManagementClient {
    client: EzManagementServiceClient<Channel>,
    container_manager_requester: ContainerManagerRequester,
    // The directory where the Isolate packages are stored.
    package_output_dir: PathBuf,
    // Sender for sending LoadIsolatesRequests to the EzManagementService.
    req_tx: Option<Sender<LoadIsolatesRequest>>,
    // Map from package_filename to IsolateType, loaded from the manifests.
    package_to_isolate_type: HashMap<String, IsolateType>,
    // Map from BinaryServicesIndex to the path of the Isolate package file.
    isolate_packages: HashMap<BinaryServicesIndex, String>,
    // The handle to the temporary directory. Keeps the directory alive.
    _package_temp_dir: Option<Arc<tempfile::TempDir>>,
}

impl EzManagementClient {
    /// Creates a new [`EzManagementClient`] that loads manifests and Isolate packages.
    pub async fn new(
        address: &str,
        container_manager_requester: ContainerManagerRequester,
        max_decoding_message_size: usize,
    ) -> Result<Self, EzManagementError> {
        let channel_pool = GrpcChannelPool::new_from_env(address)
            .await
            .map_err(|e| EzManagementError::ConnectionFailed(e.to_string()))?;
        let channel = channel_pool.next_channel();
        let client = EzManagementServiceClient::new(channel)
            .max_decoding_message_size(max_decoding_message_size);
        let package_output_dir = std::env::var(ENV_PACKAGE_OUTPUT_DIR)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_PACKAGE_OUTPUT_DIR));

        if let Err(e) = std::fs::create_dir_all(&package_output_dir) {
            log::warn!(
                "Failed to create base package output directory {:?}: {}",
                package_output_dir,
                e
            );
        }
        // Create a private subfolder inside package_output_dir. `tempfile::Builder` creates this
        // with 0700 (drwx------) permissions by default, granting full control to the owner while
        // completely blocking access to everyone else so no other process can create files or
        // symlinks inside it.
        let package_temp_dir =
            tempfile::Builder::new().prefix("ez_packages_").tempdir_in(&package_output_dir)?;
        let package_output_dir = package_temp_dir.path().to_path_buf();

        Ok(Self {
            client,
            container_manager_requester,
            package_output_dir,
            req_tx: None,
            package_to_isolate_type: HashMap::new(),
            isolate_packages: HashMap::new(),
            _package_temp_dir: Some(Arc::new(package_temp_dir)),
        })
    }

    /// Loads manifests and streams isolate packages from EzManagementService, launching
    /// workloads with the [`ContainerManagerRequester`]. This should be done after the EZ Node
    /// has acquired its Identity using the setup_isolate.
    pub async fn load_packages(&mut self) -> Result<Vec<BinaryServicesIndex>, EzManagementError> {
        let (req_tx, req_rx) =
            mpsc::channel::<LoadIsolatesRequest>(DEFAULT_REQUEST_CHANNEL_BUFFER_SIZE);
        self.req_tx = Some(req_tx);

        // TODO: Send ready signal only after setup_isolate is done with EZ Node identity.
        self.send_ready_signal().await?;
        let response = self
            .client
            .load_isolates(ReceiverStream::new(req_rx))
            .await
            .map_err(|e| EzManagementError::StreamError(e.to_string()))?;
        let mut stream = response.into_inner();

        // Receive both manifests and load them with ContainerManagerRequester.
        let manifests = self.receive_manifests(&mut stream).await?;
        self.package_to_isolate_type = get_package_filename_to_isolate_type(&manifests);
        self.container_manager_requester
            .load_workload_manifests(LoadWorkloadManifestsRequest { workload_manifests: manifests })
            .await
            .map_err(|e| EzManagementError::LoadIsolatesFailed(e.to_string()))?;

        // Receive and save isolate packages and load them with ContainerManagerRequester.
        self.receive_isolate_packages(&mut stream).await?;
        let response = self
            .container_manager_requester
            .load_workload_isolates(LoadWorkloadIsolatesRequest {
                isolate_packages: std::mem::take(&mut self.isolate_packages),
            })
            .await
            .map_err(|e| EzManagementError::LoadIsolatesFailed(e.to_string()))?;

        // Drop the temporary directory containing the packages.
        // ContainerManager unpacks them into its own cache during load_workload_isolates,
        // so we don't need to keep the raw .tar files around.
        self._package_temp_dir = None;
        Ok(response.loaded_indices)
    }

    /// Sends the initial ReadyToLoadPackagesRequest to the EzManagementService.
    async fn send_ready_signal(&self) -> Result<(), EzManagementError> {
        let req_tx = self.req_tx.as_ref().ok_or_else(|| {
            EzManagementError::ConnectionFailed(
                "Request channel sender not initialized".to_string(),
            )
        })?;
        let ready_request = LoadIsolatesRequest {
            request: Some(LoadIsolatesRequestType::ReadyToLoadPackages(
                ReadyToLoadPackagesRequest {},
            )),
        };
        req_tx.send(ready_request).await.map_err(|e| {
            EzManagementError::ConnectionFailed(format!(
                "Failed to send ReadyToLoadPackagesRequest: {e}"
            ))
        })
    }

    /// Reads exactly the two expected manifests (Ratified and Opaque) from the stream.
    async fn receive_manifests(
        &self,
        stream: &mut Streaming<LoadIsolatesResponse>,
    ) -> Result<WorkloadManifests, EzManagementError> {
        let mut ratified_manifest = None;
        let mut opaque_manifest = None;

        for _ in 0..2 {
            let msg = stream
                .message()
                .await
                .map_err(|e| EzManagementError::StreamError(e.to_string()))?
                .ok_or_else(|| {
                    EzManagementError::StreamError(
                        "Stream closed prematurely while waiting for manifests".to_string(),
                    )
                })?;
            match msg.response {
                Some(LoadIsolatesResponseType::RatifiedIsolateManifest(payload)) => {
                    let manifest = payload.manifest.ok_or_else(|| {
                        EzManagementError::ManifestParsingFailed(
                            "Received RatifiedIsolateManifestPayload with missing inner manifest"
                                .to_string(),
                        )
                    })?;
                    ratified_manifest = Some(manifest);
                }
                Some(LoadIsolatesResponseType::OpaqueIsolateManifest(manifest)) => {
                    opaque_manifest = Some(manifest);
                }
                Some(LoadIsolatesResponseType::IsolatePackageChunk(_))
                | Some(LoadIsolatesResponseType::AllPackagesLoaded(_)) => {
                    return Err(EzManagementError::UnexpectedMessage(
                        "Received package chunk or completion before both manifests were received"
                            .to_string(),
                    ));
                }
                None => {
                    return Err(EzManagementError::UnexpectedMessage(
                        "Received empty LoadIsolatesResponse".to_string(),
                    ));
                }
            }
        }
        let ratified = ratified_manifest.ok_or_else(|| {
            EzManagementError::ManifestParsingFailed("Missing RatifiedIsolateManifest".to_string())
        })?;
        let opaque = opaque_manifest.ok_or_else(|| {
            EzManagementError::ManifestParsingFailed("Missing OpaqueIsolateManifest".to_string())
        })?;
        Ok(WorkloadManifests::new(Some(ratified), Some(opaque)))
    }

    /// Processes incoming isolate package chunks from the stream and saves package files.
    async fn receive_isolate_packages(
        &mut self,
        stream: &mut Streaming<LoadIsolatesResponse>,
    ) -> Result<(), EzManagementError> {
        let mut current_package_name = String::new();
        let mut package_buffer = Vec::new();
        let mut expected_chunk_sequence: i32 = 0;

        while let Some(msg) =
            stream.message().await.map_err(|e| EzManagementError::StreamError(e.to_string()))?
        {
            match msg.response {
                Some(LoadIsolatesResponseType::IsolatePackageChunk(chunk)) => {
                    if accumulate_chunk(
                        &mut current_package_name,
                        &mut package_buffer,
                        &mut expected_chunk_sequence,
                        chunk,
                    )? {
                        let pkg_name = std::mem::take(&mut current_package_name);
                        let pkg_bytes = std::mem::take(&mut package_buffer);
                        self.process_package(&pkg_name, &pkg_bytes).await?;
                    }
                }
                Some(LoadIsolatesResponseType::AllPackagesLoaded(_)) => {
                    if !current_package_name.is_empty() {
                        return Err(EzManagementError::UnexpectedMessage(
                            "Received AllPackagesLoaded while a package was still being streamed"
                                .to_string(),
                        ));
                    }
                    return Ok(());
                }
                Some(LoadIsolatesResponseType::RatifiedIsolateManifest(_))
                | Some(LoadIsolatesResponseType::OpaqueIsolateManifest(_)) => {
                    // TODO: Support loading manifests/packages dynamically after startup in the future.
                    return Err(EzManagementError::UnexpectedMessage(
                        "Received unexpected manifest after initialization phase".to_string(),
                    ));
                }
                None => {
                    log::warn!("Received empty LoadIsolatesResponse; ignoring");
                }
            }
        }
        Err(EzManagementError::StreamError(
            "Stream closed before receiving AllPackagesLoaded signal".to_string(),
        ))
    }

    /// Saves package to disk, associates target path with registered binary index, and sends result back to server.
    async fn process_package(
        &mut self,
        package_name: &str,
        package_bytes: &[u8],
    ) -> Result<(), EzManagementError> {
        let binary_index = self.get_package_binary_index(package_name).await?;
        let target_path = self.write_package_to_disk(package_name, package_bytes).await?;
        let target_path_str = target_path.to_string_lossy().to_string();

        self.isolate_packages.insert(binary_index, target_path_str);

        // TODO: Validate RatifiedIsolate's endorsements with setup_isolate before loading.
        self.send_load_result(package_name.to_string(), true, None).await;
        Ok(())
    }

    /// Looks up the isolate type and resolves the BinaryServicesIndex for a package name.
    async fn get_package_binary_index(
        &self,
        package_name: &str,
    ) -> Result<BinaryServicesIndex, EzManagementError> {
        let isolate_type = match self.package_to_isolate_type.get(package_name) {
            Some(t) => t,
            None => {
                self.send_load_result(
                    package_name.to_string(),
                    false,
                    Some(LoadIsolatesError::ManifestParsingFailure),
                )
                .await;
                return Err(EzManagementError::UnexpectedMessage(format!(
                    "Package '{package_name}' is not present in the manifest"
                )));
            }
        };

        match get_binary_services_index(isolate_type) {
            Some(idx) => Ok(idx),
            None => {
                self.send_load_result(
                    package_name.to_string(),
                    false,
                    Some(LoadIsolatesError::ManifestParsingFailure),
                )
                .await;
                Err(EzManagementError::LoadIsolatesFailed(format!(
                    "Failed to get binary services index for package '{package_name}'"
                )))
            }
        }
    }

    /// Resolves the output destination path for a package file based on package_output_dir.
    async fn resolve_package_path(&self, package_name: &str) -> Result<PathBuf, EzManagementError> {
        if package_name.is_empty() || package_name.contains("..") {
            log::error!(
                "Invalid package path for package {package_name}: path traversal not allowed"
            );
            self.send_load_result(
                package_name.to_string(),
                false,
                Some(LoadIsolatesError::ManifestParsingFailure),
            )
            .await;
            return Err(EzManagementError::UnexpectedMessage(format!(
                "Invalid package name '{package_name}': path traversal not allowed"
            )));
        }

        let filename = Path::new(package_name).file_name().ok_or_else(|| {
            EzManagementError::UnexpectedMessage(format!(
                "Invalid package name '{package_name}': cannot resolve file name"
            ))
        });

        match filename {
            Ok(f) => Ok(self.package_output_dir.join(f)),
            Err(e) => {
                log::error!("Invalid package path for package {package_name}: {e:?}");
                self.send_load_result(
                    package_name.to_string(),
                    false,
                    Some(LoadIsolatesError::ManifestParsingFailure),
                )
                .await;
                Err(e)
            }
        }
    }

    /// Writes package bytes to disk, creating parent directories if necessary.
    async fn write_package_to_disk(
        &self,
        package_name: &str,
        bytes: &[u8],
    ) -> Result<PathBuf, EzManagementError> {
        let target_path = self.resolve_package_path(package_name).await?;
        if let Some(parent) = target_path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    log::error!(
                        "Failed to create directory {parent:?} for package {package_name}: {e:?}"
                    );
                    self.send_load_result(
                        package_name.to_string(),
                        false,
                        Some(LoadIsolatesError::IoFailure),
                    )
                    .await;
                    return Err(EzManagementError::LoadIsolatesFailed(format!("{e:?}")));
                }
            }
        }
        if let Err(e) = tokio::fs::write(&target_path, bytes).await {
            log::error!("Failed to write package {package_name} to disk: {e:?}");
            if target_path.exists() {
                let _ = tokio::fs::remove_file(&target_path).await;
            }
            self.send_load_result(
                package_name.to_string(),
                false,
                Some(LoadIsolatesError::IoFailure),
            )
            .await;
            return Err(EzManagementError::LoadIsolatesFailed(format!("{e:?}")));
        }
        Ok(target_path)
    }

    /// Sends a LoadIsolatesResult back to the EzManagementService.
    async fn send_load_result(
        &self,
        package_name: String,
        success: bool,
        error: Option<LoadIsolatesError>,
    ) {
        if let Some(ref req_tx) = self.req_tx {
            let result_req = LoadIsolatesRequest {
                request: Some(LoadIsolatesRequestType::LoadIsolatesResult(LoadIsolatesResult {
                    package_name,
                    success,
                    validate_isolate_endorsement_result: None,
                    load_isolates_error: error.unwrap_or(LoadIsolatesError::Unspecified) as i32,
                })),
            };
            let _ = req_tx.send(result_req).await;
        }
    }
}

/// Accumulates chunk bytes for a package. Returns whether this was the last chunk.
fn accumulate_chunk(
    current_package_name: &mut String,
    package_buffer: &mut Vec<u8>,
    expected_chunk_sequence: &mut i32,
    chunk: IsolatePackageChunk,
) -> Result<bool, EzManagementError> {
    if current_package_name.is_empty() {
        *current_package_name = chunk.package_name;
        *expected_chunk_sequence = 0;
    } else if !chunk.package_name.is_empty() && chunk.package_name != *current_package_name {
        return Err(EzManagementError::UnexpectedMessage(format!(
            "Received chunk for package '{}' while still streaming package '{}'",
            chunk.package_name, current_package_name
        )));
    }
    if current_package_name.is_empty() {
        return Err(EzManagementError::UnexpectedMessage(
            "Received IsolatePackageChunk without package_name in current or prior chunks"
                .to_string(),
        ));
    }

    if chunk.chunk_sequence != *expected_chunk_sequence {
        return Err(EzManagementError::UnexpectedMessage(format!(
            "Invalid chunk sequence for package '{current_package_name}': expected {}, got {}",
            *expected_chunk_sequence, chunk.chunk_sequence
        )));
    }
    *expected_chunk_sequence += 1;

    package_buffer.extend_from_slice(&chunk.package_tar_chunk);
    if chunk.is_last_chunk {
        *expected_chunk_sequence = 0;
    }
    Ok(chunk.is_last_chunk)
}

/// Creates a mapping from package_filename to matching IsolateType instances from manifests.
fn get_package_filename_to_isolate_type(
    manifests: &WorkloadManifests,
) -> HashMap<String, IsolateType> {
    let mut package_to_isolate_type: HashMap<String, IsolateType> = HashMap::new();
    if let Some(ref ratified) = manifests.ratified_isolate_manifest {
        for d in &ratified.ratified_isolate_descriptors {
            if let Some(ref it) = d.isolate_type {
                package_to_isolate_type.insert(
                    d.package_filename.clone(),
                    IsolateType {
                        publisher_id: it.publisher_id.clone(),
                        isolate_name: it.isolate_name.clone(),
                    },
                );
            }
        }
    }
    if let Some(ref opaque) = manifests.opaque_isolate_manifest {
        for d in &opaque.opaque_isolate_descriptors {
            if let Some(ref it) = d.isolate_type {
                package_to_isolate_type.insert(
                    d.package_filename.clone(),
                    IsolateType {
                        publisher_id: it.publisher_id.clone(),
                        isolate_name: it.isolate_name.clone(),
                    },
                );
            }
        }
    }
    package_to_isolate_type
}
