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

use common_proto::enforcer::v2::IsolateType;
use container_manager::{ContainerManager, ContainerManagerArgs, ManifestSource};
use container_manager_request::ContainerManagerRequest;
use container_manager_requester::{
    ContainerManagerRequester, LoadWorkloadIsolatesResponse, LoadWorkloadManifestsResponse,
};
use container_test_utils::FakeContainer;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::requester::DataScopeRequester;
use enforcer_proto::enforcer::v1::{InvokeIsolateRequest, InvokeIsolateResponse};
use ez_error::EzError;
use ez_management::{EzManagementClient, EzManagementError, LoadIsolatesError};
use ez_management_proto::enforcer::v2::ez_management_service_server::{
    EzManagementService, EzManagementServiceServer,
};
use ez_management_proto::enforcer::v2::{
    load_isolates_request, load_isolates_response, AllPackagesLoadedResponse, IsolatePackageChunk,
    LoadIsolatesRequest, LoadIsolatesResponse, RatifiedIsolateManifestPayload,
};
use fileshare_manager::FileshareManager;
use interceptor::Interceptor;
use isolate_ez_service_manager::{IsolateEzServiceManager, IsolateEzServiceManagerDependencies};
use isolate_info::{BinaryServicesIndex, IsolateId};
use isolate_service_mapper::IsolateServiceMapper;
use junction_test_utils::FakeJunction;
use junction_trait::Junction;
use manifest_proto::enforcer::v1::IsolateRuntimeConfigs;
use opaque_isolate_manifest_proto::enforcer::v2::{OpaqueIsolateDescriptor, OpaqueIsolateManifest};
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use prost::Message;
use ratified_isolate_manifest_proto::enforcer::v2::{
    RatifiedIsolateDescriptor, RatifiedIsolateManifest,
};
use setup_isolate_client::SetupIsolateClient;
use setup_isolate_proto::enforcer::v2::{
    ValidateIsolateEndorsementRequest, ValidateIsolateEndorsementResponse as SetupValidateResponse,
    Validity as SetupValidity,
};
use setup_isolate_proto::timestamp_proto::google::protobuf::Timestamp;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

pub const MAX_DECODING_SIZE: usize = 4 * 1024 * 1024;
pub const CHANNEL_SIZE: usize = 128;
pub const SHM_NUM_SLOTS: u64 = 10;
pub const SHM_SLOT_SIZE: u64 = 1024;
pub const SHM_PAYLOAD_THRESHOLD: u64 = 512;
pub const VALID_TAR: &[u8] = &[0u8; 1024];

pub struct TestContext {
    pub client: EzManagementClient,
    pub received_requests: Arc<Mutex<Vec<LoadIsolatesRequest>>>,
    pub server_handle: tokio::task::JoinHandle<()>,
    pub _server_temp_dir: tempfile::TempDir,
    pub _cm_temp_dir: tempfile::TempDir,
    pub pkg_temp_dir: tempfile::TempDir,
}

#[derive(Clone)]
pub struct MockSetupIsolateJunction {
    pub response: Arc<Mutex<Result<SetupValidateResponse, tonic::Status>>>,
    pub invoked_requests: Arc<Mutex<Vec<ValidateIsolateEndorsementRequest>>>,
}

pub fn assert_unexpected_msg(
    result: Result<Vec<BinaryServicesIndex>, EzManagementError>,
    expected: &str,
) {
    match result {
        Err(EzManagementError::UnexpectedMessage(msg)) => {
            assert!(msg.contains(expected), "Expected '{expected}' in UnexpectedMessage: '{msg}'")
        }
        other => panic!("Expected UnexpectedMessage containing '{expected}', got {:?}", other),
    }
}

pub fn assert_manifest_err(
    result: Result<Vec<BinaryServicesIndex>, EzManagementError>,
    expected: &str,
) {
    match result {
        Err(EzManagementError::ManifestParsingFailed(msg)) => assert!(
            msg.contains(expected),
            "Expected '{expected}' in ManifestParsingFailed: '{msg}'"
        ),
        other => panic!("Expected ManifestParsingFailed containing '{expected}', got {:?}", other),
    }
}

pub fn assert_stream_err(
    result: Result<Vec<BinaryServicesIndex>, EzManagementError>,
    expected: &str,
) {
    match result {
        Err(EzManagementError::StreamError(msg)) => {
            assert!(msg.contains(expected), "Expected '{expected}' in StreamError: '{msg}'")
        }
        other => panic!("Expected StreamError containing '{expected}', got {:?}", other),
    }
}

pub fn assert_load_failed(
    result: Result<Vec<BinaryServicesIndex>, EzManagementError>,
    expected_err: LoadIsolatesError,
    expected_msg: &str,
) {
    match result {
        Err(EzManagementError::LoadIsolatesFailed(err, msg)) => {
            assert_eq!(err, expected_err);
            assert!(
                msg.contains(expected_msg),
                "Expected '{expected_msg}' in LoadIsolatesFailed: '{msg}'"
            );
        }
        other => panic!(
            "Expected LoadIsolatesFailed({:?}) containing '{}', got {:?}",
            expected_err, expected_msg, other
        ),
    }
}

impl TestContext {
    pub fn pkg_dir(&self) -> &Path {
        self.pkg_temp_dir.path()
    }

    pub async fn get_last_load_result(
        &self,
    ) -> ez_management_proto::enforcer::v2::LoadIsolatesResult {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let reqs = self.received_requests.lock().unwrap().clone();
        for req in reqs.into_iter().rev() {
            if let Some(load_isolates_request::Request::LoadIsolatesResult(res)) = req.request {
                return res;
            }
        }
        panic!("No LoadIsolatesResult found in received requests");
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.server_handle.abort();
    }
}

pub struct MockManagementService {
    pub responses_to_send: Arc<Mutex<Vec<LoadIsolatesResponse>>>,
    pub received_requests: Arc<Mutex<Vec<LoadIsolatesRequest>>>,
}

#[tonic::async_trait]
impl EzManagementService for MockManagementService {
    type LoadIsolatesStream =
        Pin<Box<dyn Stream<Item = Result<LoadIsolatesResponse, Status>> + Send + 'static>>;

    async fn load_isolates(
        &self,
        request: Request<Streaming<LoadIsolatesRequest>>,
    ) -> Result<Response<Self::LoadIsolatesStream>, Status> {
        let mut in_stream = request.into_inner();
        let received_requests = self.received_requests.clone();

        tokio::spawn(async move {
            while let Ok(Some(req)) = in_stream.message().await {
                received_requests.lock().unwrap().push(req);
            }
        });

        let responses = self.responses_to_send.lock().unwrap().clone();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            for resp in responses {
                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

impl MockSetupIsolateJunction {
    pub fn new_success() -> Self {
        Self::new_with_result(Ok(SetupValidateResponse {
            validity: Some(SetupValidity {
                not_before: Some(Timestamp { seconds: 1000, nanos: 0 }),
                not_after: Some(Timestamp { seconds: 2000, nanos: 0 }),
            }),
            validation_error: 0,
            error_message: String::new(),
        }))
    }

    pub fn new_failure(validation_error: i32, error_message: &str) -> Self {
        Self::new_with_result(Ok(SetupValidateResponse {
            validity: None,
            validation_error,
            error_message: error_message.to_string(),
        }))
    }

    pub fn new_failure_with_validity(validation_error: i32, error_message: &str) -> Self {
        Self::new_with_result(Ok(SetupValidateResponse {
            validity: Some(SetupValidity {
                not_before: Some(Timestamp { seconds: 1000, nanos: 0 }),
                not_after: Some(Timestamp { seconds: 2000, nanos: 0 }),
            }),
            validation_error,
            error_message: error_message.to_string(),
        }))
    }

    pub fn new_rpc_error(error_message: &str) -> Self {
        Self::new_with_result(Err(tonic::Status::internal(error_message)))
    }

    pub fn new_with_result(result: Result<SetupValidateResponse, tonic::Status>) -> Self {
        Self {
            response: Arc::new(Mutex::new(result)),
            invoked_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_invoked_requests(&self) -> Vec<ValidateIsolateEndorsementRequest> {
        self.invoked_requests.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl Junction for MockSetupIsolateJunction {
    async fn invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        invoke_isolate_request: InvokeIsolateRequest,
        _is_from_public_api: bool,
        _deadline: Option<std::time::Instant>,
    ) -> Result<InvokeIsolateResponse, EzError> {
        if let Some(isolate_input) = invoke_isolate_request.isolate_input {
            if let Some(DeliveryMethod::InlineData(data)) = isolate_input.delivery_method {
                if let Some(bytes) = data.datagrams.into_iter().next() {
                    if let Ok(req) = ValidateIsolateEndorsementRequest::decode(&*bytes) {
                        self.invoked_requests.lock().unwrap().push(req);
                    }
                }
            }
        }

        match self.response.lock().unwrap().clone() {
            Ok(validate_resp) => {
                let bytes = validate_resp.encode_to_vec();
                Ok(InvokeIsolateResponse {
                    isolate_output: Some(EzHybridPayload {
                        delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                            datagrams: vec![bytes],
                        })),
                    }),
                    ..Default::default()
                })
            }
            Err(status) => Err(EzError::Status(status)),
        }
    }

    async fn stream_invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        _is_from_public_api: bool,
        _timeout: Option<std::time::Duration>,
    ) -> junction_trait::JunctionChannels {
        unimplemented!("stream_invoke_isolate not supported in mock setup junction")
    }

    async fn connect_isolate(
        &self,
        _isolate_id: IsolateId,
        _isolate_address: String,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub async fn create_fake_container_manager(
    setup_client: Option<Arc<SetupIsolateClient>>,
) -> (ContainerManagerRequester, tempfile::TempDir) {
    FakeContainer::clear_tracker();

    let temp_dir = tempdir().unwrap();
    let manifest_file = temp_dir.path().join("empty_manifest.json");
    tokio::fs::write(&manifest_file, r#"{"bundle_manifest": {"manifests": []}}"#).await.unwrap();

    let (cm_tx, cm_rx) = mpsc::channel::<ContainerManagerRequest>(CHANNEL_SIZE);
    let (client_tx, mut client_rx) = mpsc::channel::<ContainerManagerRequest>(CHANNEL_SIZE);

    tokio::spawn(async move {
        while let Some(req) = client_rx.recv().await {
            match req {
                ContainerManagerRequest::GetSetupIsolateClient { resp } => {
                    let _ =
                        resp.send(Ok(container_manager_request::GetSetupIsolateClientResponse {
                            client: setup_client.clone(),
                        }));
                }
                other => {
                    let _ = cm_tx.send(other).await;
                }
            }
        }
    });

    let container_manager_requester = ContainerManagerRequester::new(client_tx);
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let isolate_service_mapper = IsolateServiceMapper::default();
    let manifest_validator = ManifestValidator::default();
    let shared_memory_manager =
        SharedMemManager::new(container_manager_requester.clone(), SHM_NUM_SLOTS, SHM_SLOT_SIZE);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
    let isolate_junction = FakeJunction::default();
    let interceptor = Interceptor::new(isolate_service_mapper.clone());
    let isolate_ez_service_manager =
        IsolateEzServiceManager::new(IsolateEzServiceManagerDependencies {
            isolate_junction: Box::new(isolate_junction.clone()),
            isolate_state_manager: isolate_state_manager.clone(),
            shared_memory_manager: shared_memory_manager.clone(),
            fileshare_manager: fileshare_manager.clone(),
            external_proxy_connector: None,
            isolate_service_mapper: isolate_service_mapper.clone(),
            data_scope_requester: data_scope_requester.clone(),
            manifest_validator: manifest_validator.clone(),
            ez_to_ez_outbound_handler: None,
            max_decoding_message_size: MAX_DECODING_SIZE,
            interceptor: interceptor.clone(),
            otel_endpoint: None,
            disable_metrics_filtering: false,
            shm_payload_threshold: SHM_PAYLOAD_THRESHOLD,
        });
    let container_manager_args = ContainerManagerArgs {
        isolate_junction: Box::new(isolate_junction.clone()),
        container_manager_request_rx: cm_rx,
        isolate_state_manager: isolate_state_manager.clone(),
        isolate_service_mapper: isolate_service_mapper.clone(),
        isolate_ez_service_mngr: isolate_ez_service_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shared_mem_manager: shared_memory_manager.clone(),
        fileshare_manager: fileshare_manager.clone(),
        manifest_source: ManifestSource::V1 {
            manifest_path: manifest_file.to_str().unwrap().to_string(),
        },
        common_bind_mounts: vec![],
        max_decoding_message_size: MAX_DECODING_SIZE,
        isolate_runtime_configs: IsolateRuntimeConfigs::default(),
        interceptor: interceptor.clone(),
        otel_traces_endpoint: None,
        run_isolate_as_unprivileged: false,
        enable_syscall_filtering: false,
        shm_num_slots: SHM_NUM_SLOTS,
        shm_slot_size: SHM_SLOT_SIZE,
        shm_payload_threshold: SHM_PAYLOAD_THRESHOLD,
        operator_role: "PRIMARY".to_string(),
    };
    let _cm = ContainerManager::<FakeContainer>::start(container_manager_args)
        .await
        .expect("ContainerManager should start");
    (container_manager_requester, temp_dir)
}

pub async fn setup_test_context_with_requester(
    responses: Vec<LoadIsolatesResponse>,
    requester: ContainerManagerRequester,
) -> TestContext {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());
    let server_temp_dir = tempdir().unwrap();
    let uds_path = server_temp_dir.path().join("ez_mgmt_test.sock");
    let uds = UnixListener::bind(&uds_path).unwrap();
    let uds_stream = UnixListenerStream::new(uds);
    let received_requests = Arc::new(Mutex::new(Vec::new()));
    let service = MockManagementService {
        responses_to_send: Arc::new(Mutex::new(responses)),
        received_requests: received_requests.clone(),
    };

    let server_handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(EzManagementServiceServer::new(service))
            .serve_with_incoming(uds_stream)
            .await;
    });

    let address = format!("unix:{}", uds_path.to_str().unwrap());
    let client = EzManagementClient::new(&address, requester, MAX_DECODING_SIZE)
        .await
        .expect("Client should connect");
    TestContext {
        client,
        received_requests,
        server_handle,
        _server_temp_dir: server_temp_dir,
        _cm_temp_dir: tempdir().unwrap(),
        pkg_temp_dir,
    }
}

pub async fn get_ez_packages_dir(base_dir: &Path) -> PathBuf {
    let mut entries = tokio::fs::read_dir(base_dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.file_name().to_string_lossy().starts_with("ez_packages_") {
            return entry.path();
        }
    }
    panic!("ez_packages_ directory not found in {:?}", base_dir);
}

pub async fn setup_test_context(responses: Vec<LoadIsolatesResponse>) -> TestContext {
    setup_test_context_with_junction(responses, MockSetupIsolateJunction::new_success()).await
}

pub async fn setup_test_context_with_junction(
    responses: Vec<LoadIsolatesResponse>,
    junction: MockSetupIsolateJunction,
) -> TestContext {
    let setup_client = Arc::new(SetupIsolateClient::new(
        Box::new(junction),
        "EZ_Trusted".to_string(),
        "setup".to_string(),
        "SetupService".to_string(),
    ));
    setup_test_context_with_client(responses, Some(setup_client)).await
}

pub async fn setup_test_context_without_setup_client(
    responses: Vec<LoadIsolatesResponse>,
) -> TestContext {
    setup_test_context_with_client(responses, None).await
}

pub async fn setup_test_context_with_client(
    responses: Vec<LoadIsolatesResponse>,
    setup_client: Option<Arc<SetupIsolateClient>>,
) -> TestContext {
    let (container_manager_requester, cm_temp_dir) =
        create_fake_container_manager(setup_client).await;
    let mut ctx = setup_test_context_with_requester(responses, container_manager_requester).await;
    ctx._cm_temp_dir = cm_temp_dir;
    ctx
}

pub const RATIFIED_PUBLISHER_ID: &str = "EZ_Trusted";
pub const OPAQUE_PUBLISHER_ID: &str = "adtech.com";

/// Identity of an Isolate published in the ratified manifest.
pub fn ratified_isolate_type(isolate_name: &str) -> IsolateType {
    IsolateType {
        isolate_name: isolate_name.to_string(),
        publisher_id: RATIFIED_PUBLISHER_ID.to_string(),
    }
}

/// Identity of an Isolate published in the opaque manifest.
pub fn opaque_isolate_type(isolate_name: &str) -> IsolateType {
    IsolateType {
        isolate_name: isolate_name.to_string(),
        publisher_id: OPAQUE_PUBLISHER_ID.to_string(),
    }
}

pub fn create_ratified_manifest(
    isolate_name: &str,
    package_filename: &str,
) -> RatifiedIsolateManifest {
    RatifiedIsolateManifest {
        ratified_isolate_descriptors: vec![RatifiedIsolateDescriptor {
            isolate_type: Some(ratified_isolate_type(isolate_name)),
            package_filename: package_filename.to_string(),
            binary_filename: format!("main_{isolate_name}"),
            ..Default::default()
        }],
    }
}

pub fn create_opaque_manifest(isolate_name: &str, package_filename: &str) -> OpaqueIsolateManifest {
    OpaqueIsolateManifest {
        opaque_isolate_descriptors: vec![OpaqueIsolateDescriptor {
            isolate_type: Some(opaque_isolate_type(isolate_name)),
            package_filename: package_filename.to_string(),
            binary_filename: format!("main_{isolate_name}"),
            ..Default::default()
        }],
    }
}

pub fn create_ratified_response(manifest: RatifiedIsolateManifest) -> LoadIsolatesResponse {
    LoadIsolatesResponse {
        response: Some(load_isolates_response::Response::RatifiedIsolateManifest(
            RatifiedIsolateManifestPayload {
                manifest: Some(manifest),
                manifest_endorsements: vec![],
            },
        )),
    }
}

pub fn create_opaque_response(manifest: OpaqueIsolateManifest) -> LoadIsolatesResponse {
    LoadIsolatesResponse {
        response: Some(load_isolates_response::Response::OpaqueIsolateManifest(manifest)),
    }
}

pub fn create_chunk_response(
    isolate_type: Option<IsolateType>,
    chunk_sequence: i32,
    chunk_bytes: &[u8],
    is_last_chunk: bool,
    endorsements: &[u8],
) -> LoadIsolatesResponse {
    LoadIsolatesResponse {
        response: Some(load_isolates_response::Response::IsolatePackageChunk(
            IsolatePackageChunk {
                isolate_package_endorsements: endorsements.to_vec(),
                isolate_type,
                chunk_sequence,
                package_tar_chunk: chunk_bytes.to_vec(),
                is_last_chunk,
            },
        )),
    }
}

pub fn create_single_chunk_response(
    isolate_type: IsolateType,
    chunk_bytes: &[u8],
) -> LoadIsolatesResponse {
    create_chunk_response(Some(isolate_type), 0, chunk_bytes, true, &[])
}

pub fn create_all_packages_loaded_response() -> LoadIsolatesResponse {
    LoadIsolatesResponse {
        response: Some(load_isolates_response::Response::AllPackagesLoaded(
            AllPackagesLoadedResponse {},
        )),
    }
}

pub fn create_mock_container_manager_requester(
    manifest_result: Result<(), &'static str>,
    isolates_result: Result<(), &'static str>,
) -> ContainerManagerRequester {
    let (container_manager_request_tx, mut rx) =
        mpsc::channel::<ContainerManagerRequest>(CHANNEL_SIZE);
    tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            match req {
                ContainerManagerRequest::LoadWorkloadManifests { resp, .. } => {
                    let res = manifest_result
                        .map_err(|e| anyhow::anyhow!(e))
                        .map(|_| LoadWorkloadManifestsResponse { registered_indices: vec![] });
                    let _ = resp.send(res);
                }
                ContainerManagerRequest::LoadWorkloadIsolates { resp, .. } => {
                    let res = isolates_result
                        .map_err(|e| anyhow::anyhow!(e))
                        .map(|_| LoadWorkloadIsolatesResponse { loaded_indices: vec![] });
                    let _ = resp.send(res);
                }
                ContainerManagerRequest::GetSetupIsolateClient { resp } => {
                    let _ =
                        resp.send(Ok(container_manager_request::GetSetupIsolateClientResponse {
                            client: None,
                        }));
                }
                _ => {}
            }
        }
    });
    ContainerManagerRequester::new(container_manager_request_tx)
}

pub fn create_single_ratified_manifest_responses(
    isolate_name: &str,
    package_filename: &str,
) -> Vec<LoadIsolatesResponse> {
    vec![
        create_ratified_response(create_ratified_manifest(isolate_name, package_filename)),
        create_opaque_response(OpaqueIsolateManifest::default()),
    ]
}
