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

pub mod package_utils;
pub mod types;
use std::sync::Arc;

use common_proto::enforcer::v2::IsolateType;
use container_manager_requester::{
    ContainerManagerRequester, LoadWorkloadIsolatesRequest, LoadWorkloadManifestsRequest,
};
use ez_management_proto::enforcer::v2::ez_management_service_client::EzManagementServiceClient;
use ez_management_proto::enforcer::v2::load_isolates_request::Request as LoadIsolatesRequestType;
use ez_management_proto::enforcer::v2::load_isolates_response::Response as LoadIsolatesResponseType;
pub use ez_management_proto::enforcer::v2::LoadIsolatesError;
use ez_management_proto::enforcer::v2::{
    LoadIsolatesRequest, LoadIsolatesResponse, LoadIsolatesResult, ReadyToLoadPackagesRequest,
};
use grpc_connector::GrpcChannelPool;
use isolate_endorsement_proto::enforcer::v1::{ValidateIsolateEndorsementResponse, Validity};
use isolate_info::{get_binary_services_index, BinaryServicesIndex};
use manifest_parser::v2::WorkloadManifests;
use package_utils::{AssembledPackage, PackageAccumulator};
use setup_isolate_proto::enforcer::v2::{ExpectedClaims, ValidateIsolateEndorsementRequest};
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
    // Map from IsolateType to its package metadata, loaded from the manifests.
    package_metadata: HashMap<IsolateType, PackageMetadata>,
    // Map from BinaryServicesIndex to the path of the Isolate package file.
    isolate_packages: HashMap<BinaryServicesIndex, String>,
    // The handle to the temporary directory. Keeps the directory alive.
    _package_temp_dir: Option<Arc<tempfile::TempDir>>,
}

// Metadata for an Isolate package, loaded from the manifests.
#[derive(Clone, Debug)]
struct PackageMetadata {
    package_filename: String,
    is_ratified: bool,
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
            package_metadata: HashMap::new(),
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
        self.package_metadata = get_isolate_type_to_package_metadata(&manifests)?;
        self.container_manager_requester
            .load_workload_manifests(LoadWorkloadManifestsRequest { workload_manifests: manifests })
            .await
            .map_err(|e| {
                EzManagementError::LoadIsolatesFailed(
                    LoadIsolatesError::ManifestParsingFailure,
                    e.to_string(),
                )
            })?;

        // Receive and save isolate packages and load them with ContainerManagerRequester.
        self.receive_isolate_packages(&mut stream).await?;
        let response = self
            .container_manager_requester
            .load_workload_isolates(LoadWorkloadIsolatesRequest {
                isolate_packages: std::mem::take(&mut self.isolate_packages),
            })
            .await
            .map_err(|e| {
                EzManagementError::LoadIsolatesFailed(
                    LoadIsolatesError::UnpackingFailure,
                    e.to_string(),
                )
            })?;

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
        let mut accumulator = PackageAccumulator::new();

        while let Some(msg) =
            stream.message().await.map_err(|e| EzManagementError::StreamError(e.to_string()))?
        {
            match msg.response {
                Some(LoadIsolatesResponseType::IsolatePackageChunk(chunk)) => {
                    if let Some(package) = accumulator.push_chunk(chunk)? {
                        self.process_package(&package).await?;
                    }
                }
                Some(LoadIsolatesResponseType::AllPackagesLoaded(_)) => {
                    if accumulator.is_package_in_progress() {
                        return Err(EzManagementError::UnexpectedMessage(
                            "Received AllPackagesLoaded while a package was still being streamed"
                                .to_string(),
                        ));
                    }
                    if self.isolate_packages.len() != self.package_metadata.len() {
                        return Err(EzManagementError::IncompletePackagesLoaded {
                            expected: self.package_metadata.len(),
                            received: self.isolate_packages.len(),
                        });
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

    async fn get_setup_isolate_client(
        &self,
        isolate_type: &IsolateType,
    ) -> Result<std::sync::Arc<setup_isolate_client::SetupIsolateClient>, EzManagementError> {
        let client_opt = match self.container_manager_requester.get_setup_isolate_client().await {
            Ok(c) => c,
            Err(e) => {
                self.send_load_result(
                    isolate_type,
                    false,
                    Some(LoadIsolatesError::ValidationFailure),
                    None,
                )
                .await;
                return Err(EzManagementError::InternalError(e.to_string()));
            }
        };

        match client_opt {
            Some(client) => Ok(client),
            None => {
                self.send_load_result(
                    isolate_type,
                    false,
                    Some(LoadIsolatesError::ValidationFailure),
                    None,
                )
                .await;
                Err(EzManagementError::InternalError(
                    "Setup isolate client is not available".to_string(),
                ))
            }
        }
    }

    async fn validate_package_endorsement(
        &self,
        isolate_type: &IsolateType,
        package_filename: &str,
        package_hash: String,
        package_endorsements: &[u8],
    ) -> Result<ValidateIsolateEndorsementResponse, EzManagementError> {
        let client = self.get_setup_isolate_client(isolate_type).await?;

        let expected_claims = ExpectedClaims {
            publisher_id: isolate_type.publisher_id.clone(),
            isolate_name: isolate_type.isolate_name.clone(),
            package_filename: package_filename.to_string(),
        };
        let req = ValidateIsolateEndorsementRequest {
            expected_claims: Some(expected_claims),
            isolate_package_endorsement: package_endorsements.to_vec(),
            package_digest_sha256: package_hash,
        };
        let res = match client.validate_isolate_endorsement(req).await {
            Ok(r) => r,
            Err(e) => {
                self.send_load_result(
                    isolate_type,
                    false,
                    Some(LoadIsolatesError::ValidationFailure),
                    None,
                )
                .await;
                return Err(EzManagementError::LoadIsolatesFailed(
                    LoadIsolatesError::ValidationFailure,
                    format!("Failed to validate isolate endorsement: {e}"),
                ));
            }
        };
        let validate_response = ValidateIsolateEndorsementResponse {
            validity: res
                .validity
                .map(|v| Validity { not_before: v.not_before, not_after: v.not_after }),
            validation_error: res.validation_error,
            error_message: res.error_message,
        };

        if validate_response.validity.is_none() || validate_response.validation_error != 0 {
            self.send_load_result(
                isolate_type,
                false,
                Some(LoadIsolatesError::ValidationFailure),
                Some(validate_response),
            )
            .await;
            return Err(EzManagementError::LoadIsolatesFailed(
                LoadIsolatesError::ValidationFailure,
                "Endorsement validation failed".to_string(),
            ));
        }

        Ok(validate_response)
    }

    /// Saves package to disk, associates target path with registered binary index, and sends result back to server.
    async fn process_package(
        &mut self,
        package: &AssembledPackage,
    ) -> Result<(), EzManagementError> {
        let isolate_type = &package.isolate_type;
        let (package_filename, is_ratified) = match self.package_metadata.get(isolate_type) {
            Some(metadata) => (metadata.package_filename.clone(), metadata.is_ratified),
            None => {
                self.send_load_result(
                    isolate_type,
                    false,
                    Some(LoadIsolatesError::ManifestParsingFailure),
                    None,
                )
                .await;
                return Err(EzManagementError::LoadIsolatesFailed(
                    LoadIsolatesError::ManifestParsingFailure,
                    format!("Isolate '{isolate_type:?}' is not present in the manifest"),
                ));
            }
        };
        // Validate destination path upfront before endorsement validation.
        let target_path = self.resolve_package_path(isolate_type, &package_filename).await?;
        let binary_index = self.get_package_binary_index(isolate_type).await?;

        // Calculate the hash before writing to disk and validate Ratified packages via setup isolate.
        let mut validate_result = None;
        if is_ratified {
            let package_hash = package_utils::calculate_sha256(&package.package_bytes);
            validate_result = Some(
                self.validate_package_endorsement(
                    isolate_type,
                    &package_filename,
                    package_hash,
                    &package.endorsements,
                )
                .await?,
            );
        }

        self.write_package_to_disk(isolate_type, &package_filename, &package.package_bytes).await?;
        let target_path_str = target_path.to_string_lossy().to_string();
        self.isolate_packages.insert(binary_index, target_path_str);
        self.send_load_result(isolate_type, true, None, validate_result).await;
        Ok(())
    }

    /// Resolves the BinaryServicesIndex registered for an Isolate.
    async fn get_package_binary_index(
        &self,
        isolate_type: &IsolateType,
    ) -> Result<BinaryServicesIndex, EzManagementError> {
        match get_binary_services_index(isolate_type) {
            Some(idx) => Ok(idx),
            None => {
                self.send_load_result(
                    isolate_type,
                    false,
                    Some(LoadIsolatesError::ManifestParsingFailure),
                    None,
                )
                .await;
                Err(EzManagementError::LoadIsolatesFailed(
                    LoadIsolatesError::ManifestParsingFailure,
                    format!("Failed to get binary services index for Isolate '{isolate_type:?}'"),
                ))
            }
        }
    }

    /// Resolves the output destination path for a package file based on package_output_dir
    /// and a per-IsolateType directory (`<publisher_id>/<isolate_name>`).
    /// The file is named after the package_filename declared in the Isolate manifest.
    async fn resolve_package_path(
        &self,
        isolate_type: &IsolateType,
        package_filename: &str,
    ) -> Result<PathBuf, EzManagementError> {
        if let Err(e) = validate_package_path_components(isolate_type, package_filename) {
            log::error!("Invalid package path component for Isolate '{isolate_type:?}': {e}");
            self.send_load_result(
                isolate_type,
                false,
                Some(LoadIsolatesError::ManifestParsingFailure),
                None,
            )
            .await;
            return Err(e);
        }

        let filename = Path::new(package_filename).file_name().ok_or_else(|| {
            EzManagementError::LoadIsolatesFailed(
                LoadIsolatesError::ManifestParsingFailure,
                format!("Invalid package_filename '{package_filename}': cannot resolve file name"),
            )
        });

        match filename {
            Ok(f) => Ok(self
                .package_output_dir
                .join(&isolate_type.publisher_id)
                .join(&isolate_type.isolate_name)
                .join(f)),
            Err(e) => {
                log::error!("Invalid package path for Isolate '{isolate_type:?}': {e:?}");
                self.send_load_result(
                    isolate_type,
                    false,
                    Some(LoadIsolatesError::ManifestParsingFailure),
                    None,
                )
                .await;
                Err(e)
            }
        }
    }

    /// Writes package bytes to disk, creating parent directories if necessary.
    async fn write_package_to_disk(
        &self,
        isolate_type: &IsolateType,
        package_filename: &str,
        bytes: &[u8],
    ) -> Result<PathBuf, EzManagementError> {
        let target_path = self.resolve_package_path(isolate_type, package_filename).await?;
        if let Some(parent) = target_path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    log::error!(
                        "Failed to create directory {parent:?} for Isolate '{isolate_type:?}': {e:?}"
                    );
                    self.send_load_result(
                        isolate_type,
                        false,
                        Some(LoadIsolatesError::IoFailure),
                        None,
                    )
                    .await;
                    return Err(EzManagementError::LoadIsolatesFailed(
                        LoadIsolatesError::IoFailure,
                        format!("{e:?}"),
                    ));
                }
            }
        }
        if let Err(e) = tokio::fs::write(&target_path, bytes).await {
            log::error!("Failed to write package for Isolate '{isolate_type:?}' to disk: {e:?}");
            if target_path.exists() {
                let _ = tokio::fs::remove_file(&target_path).await;
            }
            self.send_load_result(isolate_type, false, Some(LoadIsolatesError::IoFailure), None)
                .await;
            return Err(EzManagementError::LoadIsolatesFailed(
                LoadIsolatesError::IoFailure,
                format!("{e:?}"),
            ));
        }
        Ok(target_path)
    }

    /// Sends a LoadIsolatesResult back to the EzManagementService.
    async fn send_load_result(
        &self,
        isolate_type: &IsolateType,
        success: bool,
        error: Option<LoadIsolatesError>,
        validate_result: Option<ValidateIsolateEndorsementResponse>,
    ) {
        if let Some(ref req_tx) = self.req_tx {
            let result_req = LoadIsolatesRequest {
                request: Some(LoadIsolatesRequestType::LoadIsolatesResult(LoadIsolatesResult {
                    isolate_type: Some(isolate_type.clone()),
                    success,
                    validate_isolate_endorsement_result: validate_result,
                    load_isolates_error: error.unwrap_or(LoadIsolatesError::Unspecified) as i32,
                })),
            };
            let _ = req_tx.send(result_req).await;
        }
    }
}

/// Validates that the isolate identity and package filename do not contain path traversal or invalid components.
fn validate_package_path_components(
    isolate_type: &IsolateType,
    package_filename: &str,
) -> Result<(), EzManagementError> {
    let is_invalid_component = |s: &str| {
        s.is_empty() || s == "." || s.contains("..") || s.contains('/') || s.contains('\\')
    };
    if is_invalid_component(&isolate_type.publisher_id) {
        return Err(EzManagementError::LoadIsolatesFailed(
            LoadIsolatesError::ManifestParsingFailure,
            format!(
                "Invalid publisher_id '{}': path traversal or invalid component not allowed",
                isolate_type.publisher_id
            ),
        ));
    }
    if is_invalid_component(&isolate_type.isolate_name) {
        return Err(EzManagementError::LoadIsolatesFailed(
            LoadIsolatesError::ManifestParsingFailure,
            format!(
                "Invalid isolate_name '{}': path traversal or invalid component not allowed",
                isolate_type.isolate_name
            ),
        ));
    }
    if package_filename.is_empty() || package_filename == "." || package_filename.contains("..") {
        return Err(EzManagementError::LoadIsolatesFailed(
            LoadIsolatesError::ManifestParsingFailure,
            format!("Invalid package_filename '{package_filename}': path traversal not allowed"),
        ));
    }
    Ok(())
}

/// Registers the package metadata of a single manifest descriptor, keyed by its IsolateType.
fn insert_package_metadata(
    package_metadata: &mut HashMap<IsolateType, PackageMetadata>,
    isolate_type: Option<&IsolateType>,
    package_filename: &str,
    is_ratified: bool,
) -> Result<(), EzManagementError> {
    let Some(isolate_type) =
        isolate_type.cloned().filter(|t| !t.publisher_id.is_empty() && !t.isolate_name.is_empty())
    else {
        return Err(EzManagementError::LoadIsolatesFailed(
            LoadIsolatesError::ManifestParsingFailure,
            "Manifest descriptor is missing valid IsolateType".to_string(),
        ));
    };
    if package_metadata.contains_key(&isolate_type) {
        return Err(EzManagementError::LoadIsolatesFailed(
            LoadIsolatesError::ManifestParsingFailure,
            format!("Duplicate Isolate '{isolate_type:?}' found across manifests"),
        ));
    }
    package_metadata.insert(
        isolate_type,
        PackageMetadata { package_filename: package_filename.to_string(), is_ratified },
    );
    Ok(())
}

/// Creates a mapping from IsolateType to its package metadata from the manifests.
fn get_isolate_type_to_package_metadata(
    manifests: &WorkloadManifests,
) -> Result<HashMap<IsolateType, PackageMetadata>, EzManagementError> {
    let mut package_metadata: HashMap<IsolateType, PackageMetadata> = HashMap::new();
    if let Some(ref ratified) = manifests.ratified_isolate_manifest {
        for d in &ratified.ratified_isolate_descriptors {
            insert_package_metadata(
                &mut package_metadata,
                d.isolate_type.as_ref(),
                &d.package_filename,
                true,
            )?;
        }
    }
    if let Some(ref opaque) = manifests.opaque_isolate_manifest {
        for d in &opaque.opaque_isolate_descriptors {
            insert_package_metadata(
                &mut package_metadata,
                d.isolate_type.as_ref(),
                &d.package_filename,
                false,
            )?;
        }
    }
    Ok(package_metadata)
}
