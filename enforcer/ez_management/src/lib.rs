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

use binary_manifest_proto::enforcer::v2::{binary_manifest, BinaryManifest, IsolateDigest};
use common_proto::enforcer::v2::IsolateType;
use container_manager_requester::{
    ContainerManagerRequester, LoadWorkloadIsolatesRequest, LoadWorkloadManifestsRequest,
};
use ez_management_proto::enforcer::v2::ez_management_service_client::EzManagementServiceClient;
use ez_management_proto::enforcer::v2::load_isolates_request::Request as LoadIsolatesRequestType;
use ez_management_proto::enforcer::v2::load_isolates_response::Response as LoadIsolatesResponseType;
use ez_management_proto::enforcer::v2::{
    ez_manifest, BundleManifest, EndorsedBinaryManifest, FetchIsolateStartupParametersRequest,
    FetchOperatorInfoRequest, LoadIsolatesRequest, LoadIsolatesResponse, LoadIsolatesResult,
    ReadyToLoadPackagesRequest,
};
pub use ez_management_proto::enforcer::v2::{LoadIsolatesError, OperatorInfo};
use grpc_connector::GrpcChannelPool;
use isolate_endorsement_proto::enforcer::v1::{ValidateIsolateEndorsementResponse, Validity};
use isolate_info::{get_binary_services_index, BinaryServicesIndex};
pub use isolate_startup_parameters_proto::enforcer::v2::IsolateStartupParameters;
use manifest_parser::v2::{validate_isolate_digest, WorkloadManifests};
use manifest_parser::ParsedIsolate;
use package_utils::{AssembledPackage, PackageAccumulator};
use setup_isolate_proto::enforcer::v2::{ExpectedClaims, ValidateIsolateEndorsementRequest};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Streaming;
pub use types::EzManagementError;

const DEFAULT_REQUEST_CHANNEL_BUFFER_SIZE: usize = 512;
const ENV_PACKAGE_OUTPUT_DIR: &str = "EZ_PACKAGE_OUTPUT_DIR";
const DEFAULT_PACKAGE_OUTPUT_DIR: &str = "/tmp";
const FETCH_OPERATOR_INFO_TIMEOUT: Duration = Duration::from_secs(10);
const FETCH_ISOLATE_STARTUP_PARAMETERS_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ISOLATE_COUNT: i32 = 64;
const BINARY_MANIFEST_FILENAME: &str = "binary_manifest.json";
const PACKAGE_FILENAME: &str = "package.tar";

/// Client for communicating with the EzManagementService.
#[derive(Clone)]
pub struct EzManagementClient {
    client: EzManagementServiceClient<Channel>,
    container_manager_requester: ContainerManagerRequester,
    // The directory where the Isolate packages are stored.
    package_output_dir: PathBuf,
    // Sender for sending LoadIsolatesRequests to the EzManagementService.
    req_tx: Option<Sender<LoadIsolatesRequest>>,
    // Map from IsolateType to the isolate_digest of its BinaryManifest.
    package_metadata: HashMap<IsolateType, IsolateDigest>,
    // Map from BinaryServicesIndex to the path of the Isolate package file.
    isolate_packages: HashMap<BinaryServicesIndex, String>,
    // The handle to the temporary directory. Keeps the directory alive.
    _package_temp_dir: Option<Arc<tempfile::TempDir>>,
}

/// A workload BinaryManifest that passed the checks in parse_binary_manifest.
struct ParsedBinaryManifest {
    manifest: BinaryManifest,
    calculated_manifest_digest_sha256: String,
    isolate_type: IsolateType,
    isolate_digest: IsolateDigest,
    binary_manifest_endorsements: Vec<u8>,
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

    /// Fetches and validates the node-level [`OperatorInfo`].
    pub async fn fetch_operator_info(&self) -> Result<OperatorInfo, EzManagementError> {
        let mut client = self.client.clone();
        let response = tokio::time::timeout(
            FETCH_OPERATOR_INFO_TIMEOUT,
            client.fetch_operator_info(FetchOperatorInfoRequest {}),
        )
        .await
        .map_err(|_| {
            EzManagementError::FetchOperatorInfoFailed(format!(
                "timed out after {FETCH_OPERATOR_INFO_TIMEOUT:?}"
            ))
        })?
        .map_err(|e| EzManagementError::FetchOperatorInfoFailed(e.to_string()))?;
        let operator_info = response.into_inner().operator_info.ok_or_else(|| {
            EzManagementError::InvalidOperatorInfo("operator_info is missing".to_string())
        })?;
        if operator_info.operator_domain.is_empty() || operator_info.operator_role.is_empty() {
            return Err(EzManagementError::InvalidOperatorInfo(format!(
                "operator_domain and operator_role must be non-empty: {operator_info:?}"
            )));
        }
        Ok(operator_info)
    }

    /// Fetches and validates the [`IsolateStartupParameters`] of every Isolate.
    pub async fn fetch_isolate_startup_parameters(
        &self,
    ) -> Result<HashMap<IsolateType, IsolateStartupParameters>, EzManagementError> {
        let mut client = self.client.clone();
        let response = tokio::time::timeout(
            FETCH_ISOLATE_STARTUP_PARAMETERS_TIMEOUT,
            client.fetch_isolate_startup_parameters(FetchIsolateStartupParametersRequest {}),
        )
        .await
        .map_err(|_| {
            EzManagementError::FetchIsolateStartupParametersFailed(format!(
                "timed out after {FETCH_ISOLATE_STARTUP_PARAMETERS_TIMEOUT:?}"
            ))
        })?
        .map_err(|e| EzManagementError::FetchIsolateStartupParametersFailed(e.to_string()))?;
        let mut startup_parameters = HashMap::new();
        for parameters in response.into_inner().startup_parameters {
            let identifier = parameters.identifier.as_ref().ok_or_else(|| {
                EzManagementError::InvalidIsolateStartupParameters(format!(
                    "missing identifier in startup parameters: {parameters:?}"
                ))
            })?;
            if identifier.publisher_id.is_empty() || identifier.isolate_name.is_empty() {
                return Err(EzManagementError::InvalidIsolateStartupParameters(format!(
                    "identifier must have non-empty publisher_id and isolate_name: {identifier:?}"
                )));
            }
            if parameters.number_of_isolates <= 0
                || parameters.number_of_isolates > MAX_ISOLATE_COUNT
            {
                return Err(EzManagementError::InvalidIsolateStartupParameters(format!(
                    "number_of_isolates must be between 1 and {MAX_ISOLATE_COUNT}: {parameters:?}"
                )));
            }
            if let Some(prev) = startup_parameters.insert(identifier.clone(), parameters) {
                return Err(EzManagementError::InvalidIsolateStartupParameters(format!(
                    "duplicate entries for Isolate {:?}",
                    prev.identifier
                )));
            }
        }
        Ok(startup_parameters)
    }

    /// Loads manifests and streams isolate packages from EzManagementService, launching
    /// workloads with the [`ContainerManagerRequester`]. This should be done after the EZ Node
    /// has acquired its Identity using the setup_isolate.
    pub async fn load_packages(&mut self) -> Result<Vec<BinaryServicesIndex>, EzManagementError> {
        let (req_tx, req_rx) =
            mpsc::channel::<LoadIsolatesRequest>(DEFAULT_REQUEST_CHANNEL_BUFFER_SIZE);
        self.req_tx = Some(req_tx);

        // TODO(b/553281475): Send ready signal only after setup_isolate is done with EZ Node identity.
        self.send_ready_signal().await?;
        let response = self
            .client
            .load_isolates(ReceiverStream::new(req_rx))
            .await
            .map_err(|e| EzManagementError::StreamError(e.to_string()))?;
        let mut stream = response.into_inner();

        let manifests = self.receive_bundle_manifest(&mut stream).await?;
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

    /// Reads the BundleManifest from the stream, validates its BinaryManifests, and fills
    /// package_metadata.
    async fn receive_bundle_manifest(
        &mut self,
        stream: &mut Streaming<LoadIsolatesResponse>,
    ) -> Result<WorkloadManifests, EzManagementError> {
        let msg = stream
            .message()
            .await
            .map_err(|e| EzManagementError::StreamError(e.to_string()))?
            .ok_or_else(|| {
                EzManagementError::StreamError(
                    "Stream closed prematurely while waiting for the BundleManifest".to_string(),
                )
            })?;
        let bundle_manifest = match msg.response {
            Some(LoadIsolatesResponseType::BundleManifest(bundle_manifest)) => bundle_manifest,
            Some(LoadIsolatesResponseType::IsolatePackageChunk(_))
            | Some(LoadIsolatesResponseType::AllPackagesLoaded(_)) => {
                return Err(EzManagementError::UnexpectedMessage(
                    "Received package chunk or completion before the BundleManifest".to_string(),
                ));
            }
            Some(LoadIsolatesResponseType::RatifiedIsolateManifest(_))
            | Some(LoadIsolatesResponseType::OpaqueIsolateManifest(_)) => {
                return Err(EzManagementError::UnexpectedMessage(
                    "Received legacy isolate manifest; only BundleManifest is supported"
                        .to_string(),
                ));
            }
            None => {
                return Err(EzManagementError::UnexpectedMessage(
                    "Received empty LoadIsolatesResponse".to_string(),
                ));
            }
        };

        let mut endorsed_manifests = Vec::new();
        flatten_bundle_manifest(bundle_manifest, &mut endorsed_manifests)?;
        let mut isolate_types = HashSet::new();
        let mut parsed_manifests = Vec::new();
        for endorsed_manifest in endorsed_manifests {
            let parsed = parse_binary_manifest(endorsed_manifest)?;
            if !isolate_types.insert(parsed.isolate_type.clone()) {
                return Err(EzManagementError::LoadIsolatesFailed(
                    LoadIsolatesError::ManifestParsingFailure,
                    format!("Duplicate Isolate '{:?}' in the BundleManifest", parsed.isolate_type),
                ));
            }
            parsed_manifests.push(parsed);
        }

        let mut binary_manifests = Vec::with_capacity(parsed_manifests.len());
        let mut package_metadata = HashMap::with_capacity(parsed_manifests.len());
        for parsed in parsed_manifests {
            self.validate_manifest_endorsement(
                &parsed.isolate_type,
                parsed.calculated_manifest_digest_sha256,
                &parsed.binary_manifest_endorsements,
            )
            .await?;
            package_metadata.insert(parsed.isolate_type, parsed.isolate_digest);
            binary_manifests.push(parsed.manifest);
        }
        self.package_metadata = package_metadata;
        Ok(WorkloadManifests::new(binary_manifests))
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
                | Some(LoadIsolatesResponseType::OpaqueIsolateManifest(_))
                | Some(LoadIsolatesResponseType::BundleManifest(_)) => {
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
    ) -> Result<setup_isolate_client::SetupIsolateClient, EzManagementError> {
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

    /// Validates a BinaryManifest endorsement via the Setup Isolate and sends the validation
    /// response in a LoadIsolatesResult.
    async fn validate_manifest_endorsement(
        &self,
        isolate_type: &IsolateType,
        manifest_digest_sha256: String,
        manifest_endorsements: &[u8],
    ) -> Result<(), EzManagementError> {
        let client = self.get_setup_isolate_client(isolate_type).await?;

        let expected_claims = ExpectedClaims {
            publisher_id: isolate_type.publisher_id.clone(),
            isolate_name: isolate_type.isolate_name.clone(),
            package_filename: BINARY_MANIFEST_FILENAME.to_string(),
        };
        let req = ValidateIsolateEndorsementRequest {
            expected_claims: Some(expected_claims),
            isolate_package_endorsement: manifest_endorsements.to_vec(),
            package_digest_sha256: manifest_digest_sha256,
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

        self.send_load_result(isolate_type, true, None, Some(validate_response)).await;
        Ok(())
    }

    /// Saves package to disk, associates target path with registered binary index, and sends result back to server.
    async fn process_package(
        &mut self,
        package: &AssembledPackage,
    ) -> Result<(), EzManagementError> {
        let isolate_type = &package.isolate_type;
        let isolate_digest = match self.package_metadata.get(isolate_type) {
            Some(isolate_digest) => isolate_digest.clone(),
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
        let target_path = self.resolve_package_path(isolate_type).await?;
        let binary_index = self.get_package_binary_index(isolate_type).await?;

        let package_sha256 = package_utils::calculate_sha256(&package.package_bytes);
        if package_sha256 != isolate_digest.package_sha256 {
            self.send_load_result(
                isolate_type,
                false,
                Some(LoadIsolatesError::PackageDigestMismatch),
                None,
            )
            .await;
            return Err(EzManagementError::LoadIsolatesFailed(
                LoadIsolatesError::PackageDigestMismatch,
                format!(
                    "Package digest {package_sha256} does not match isolate_digest.package_sha256 \
                     '{}' of Isolate '{isolate_type:?}'",
                    isolate_digest.package_sha256
                ),
            ));
        }

        self.write_package_to_disk(isolate_type, &package.package_bytes).await?;
        let target_path_str = target_path.to_string_lossy().to_string();
        self.isolate_packages.insert(binary_index, target_path_str);
        self.send_load_result(isolate_type, true, None, None).await;
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

    /// Validates `isolate_type` path components and returns
    /// `<package_output_dir>/<publisher_id>/<isolate_name>/package.tar`.
    async fn resolve_package_path(
        &self,
        isolate_type: &IsolateType,
    ) -> Result<PathBuf, EzManagementError> {
        if let Err(e) = validate_package_path_components(isolate_type) {
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
        Ok(self
            .package_output_dir
            .join(&isolate_type.publisher_id)
            .join(&isolate_type.isolate_name)
            .join(PACKAGE_FILENAME))
    }

    /// Writes package bytes to disk, creating parent directories if necessary.
    async fn write_package_to_disk(
        &self,
        isolate_type: &IsolateType,
        bytes: &[u8],
    ) -> Result<PathBuf, EzManagementError> {
        let target_path = self.resolve_package_path(isolate_type).await?;
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

/// Flattens a [`BundleManifest`] and its nested [`BundleManifest`]s into a list of
/// [`EndorsedBinaryManifest`]s.
fn flatten_bundle_manifest(
    bundle_manifest: BundleManifest,
    output: &mut Vec<EndorsedBinaryManifest>,
) -> Result<(), EzManagementError> {
    for entry in bundle_manifest.manifests {
        match entry.manifest {
            Some(ez_manifest::Manifest::BinaryManifest(endorsed_manifest)) => {
                output.push(endorsed_manifest);
            }
            Some(ez_manifest::Manifest::BundleManifest(nested_bundle)) => {
                flatten_bundle_manifest(nested_bundle, output)?;
            }
            None => {
                return Err(EzManagementError::LoadIsolatesFailed(
                    LoadIsolatesError::ManifestParsingFailure,
                    "BundleManifest entry has no manifest".to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// Parses and checks a workload EndorsedBinaryManifest before any endorsement RPC, so that an
/// incorrect manifest gives a manifest error, not an endorsement error.
fn parse_binary_manifest(
    endorsed_manifest: EndorsedBinaryManifest,
) -> Result<ParsedBinaryManifest, EzManagementError> {
    let manifest =
        manifest_parser::v2::parse_binary_manifest(&endorsed_manifest.binary_manifest_json)
            .map_err(|e| EzManagementError::ManifestParsingFailed(format!("{e:#}")))?;
    let isolate_type = manifest
        .isolate_type
        .clone()
        .filter(|t| !t.publisher_id.is_empty() && !t.isolate_name.is_empty())
        .ok_or_else(|| {
            EzManagementError::LoadIsolatesFailed(
                LoadIsolatesError::ManifestParsingFailure,
                "BinaryManifest is missing a valid isolate_type".to_string(),
            )
        })?;
    match manifest.isolate_kind {
        Some(binary_manifest::IsolateKind::Ratified(_))
        | Some(binary_manifest::IsolateKind::Opaque(_)) => {}
        Some(binary_manifest::IsolateKind::Setup(_)) => {
            return Err(EzManagementError::LoadIsolatesFailed(
                LoadIsolatesError::ManifestParsingFailure,
                format!("Setup Isolate '{isolate_type:?}' is not allowed in the BundleManifest"),
            ));
        }
        None => {
            return Err(EzManagementError::LoadIsolatesFailed(
                LoadIsolatesError::ManifestParsingFailure,
                format!("BinaryManifest for '{isolate_type:?}' is missing isolate_kind"),
            ));
        }
    }
    validate_isolate_digest(manifest.isolate_digest.as_ref()).map_err(|e| {
        EzManagementError::LoadIsolatesFailed(
            LoadIsolatesError::ManifestParsingFailure,
            format!(
                "BinaryManifest for '{isolate_type:?}' isolate_digest field parsing failure: \
                 {e:#}"
            ),
        )
    })?;
    let isolate_digest = manifest.isolate_digest.clone().unwrap_or_default();
    // Create ParsedIsolate to validate the BinaryManifest.
    ParsedIsolate::try_from(manifest.clone()).map_err(|e| {
        EzManagementError::LoadIsolatesFailed(
            LoadIsolatesError::ManifestParsingFailure,
            format!("BinaryManifest for '{isolate_type:?}': {e:#}"),
        )
    })?;
    let calculated_manifest_digest_sha256 =
        package_utils::calculate_sha256(&endorsed_manifest.binary_manifest_json);
    Ok(ParsedBinaryManifest {
        manifest,
        calculated_manifest_digest_sha256,
        isolate_type,
        isolate_digest,
        binary_manifest_endorsements: endorsed_manifest.binary_manifest_endorsements,
    })
}

/// Validates that the isolate identity does not contain path traversal or invalid components.
fn validate_package_path_components(isolate_type: &IsolateType) -> Result<(), EzManagementError> {
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
    Ok(())
}
