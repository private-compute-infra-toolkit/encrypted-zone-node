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

use anyhow::{ensure, Context, Result};
use container_manager::{ContainerManager, ContainerManagerArgs, ManifestSource};
use container_manager_request::ContainerManagerRequest;
use container_manager_requester::ContainerManagerRequester;
use container_test_utils::{FakeContainer, Status};
use data_scope::error::DataScopeError;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::request::{ValidateManifestInputScopeRequest, ValidateManifestOutputScopeRequest};
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    isolate_ez_bridge_client::IsolateEzBridgeClient, ControlPlaneMetadata, EzPayloadIsolateScope,
    InvokeEzRequest, InvokeEzResponse, IsolateDataScope, IsolateState, NotifyIsolateStateRequest,
    NotifyIsolateStateResponse,
};
use ez_service_proto::enforcer::v1::CallRequest;
use fileshare_manager::FileshareManager;
use hyper_util::rt::TokioIo;
use interceptor::Interceptor;
use isolate_ez_service_manager::{IsolateEzServiceManager, IsolateEzServiceManagerDependencies};
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceIndex, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use junction_test_utils::FakeJunction;
use manifest_parser_test_utils::load_workload_manifests_from_paths;
use manifest_proto::enforcer::v1::IsolateRuntimeConfigs;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use shared_memory_manager::SharedMemManager;
use simple_tonic_stream::SimpleStreamingWrapper;
use state_manager::{IsolateStateManager, IsolateStateManagerError};
use std::fs::OpenOptions;
use tokio::net::{TcpListener, UnixStream};
use tokio::sync::mpsc::channel;
use tokio::task::spawn_blocking;
use tokio::time::{sleep, timeout, Duration};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

pub const CHANNEL_SIZE: usize = 64;
pub const MAX_DECODING_SIZE: usize = 4 * 1024 * 1024;
pub const SHM_NUM_SLOTS: u64 = 10;
pub const SHM_SLOT_SIZE: u64 = 64;
pub const SHM_PAYLOAD_THRESHOLD: u64 = 100 * 1024 * 1024;
pub const JSON_MANIFEST_PATH_ONE_ISOLATE: &str =
    "enforcer/container/manager/test/testdata/test_manifest_one_isolate.json";
pub const JSON_MANIFEST_PATH_MULTIPLE_ISOLATE: &str =
    "enforcer/container/manager/test/testdata/test_manifest_multiple_isolates.json";
pub const JSON_MANIFEST_PATH_RATIFIED_ISOLATE: &str =
    "enforcer/container/manager/test/testdata/test_manifest_ratified_isolate.json";
pub const JSON_MANIFEST_PATH_WRONG_DOMAIN: &str =
    "enforcer/container/manager/test/testdata/test_manifest_wrong_domain.json";
pub const JSON_MANIFEST_PATH_INTERCEPTOR: &str =
    "enforcer/container/manager/test/testdata/test_manifest_interceptor.json";
pub const LOCALHOST_OTLP_ENDPOINT: &str = "http://localhost:4317";
pub const JSON_MANIFEST_PATH_OTEL: &str =
    "enforcer/container/manager/test/testdata/test_manifest_otel.json";

// The following constants are from the test manifests.
// From v2_setup.json
pub const SETUP_BINARY: &str = "/usr/local/bin/setup";
pub const SETUP_ISOLATE_DOMAIN: &str = "EZ_Trusted";
pub const SETUP_SERVICE: &str = "SetupService";

// From test_manifest_one_isolate.json
pub const HELLOWORLD_BINARY: &str = "/usr/local/bin/main";
pub const HELLOWORLD_DOMAIN: &str = "helloworld_domain";
pub const GREETER_SERVICE: &str = "Greeter";
pub const AUTH_SERVICE: &str = "AuthService";
pub const SAY_HELLO_METHOD: &str = "SayHello";
pub const RATIFIED_ISOLATE_DOMAIN: &str = "EZ_Trusted";
pub const ENTRYPOINT_BINARY: &str = "/usr/local/bin/entrypoint.sh";

// From test_manifest_multiple_isolates.json
pub const SUMMATION_BINARY: &str = "/usr/local/bin/summation_by_lookup_table_with_backend";
pub const PRECOMPUTED_BACKEND_BINARY: &str = "/usr/local/bin/summation_precomputed_backend";
pub const PLAYGROUND_EXAMPLE_DOMAIN: &str = "playground_example";
pub const SUMMATION_SERVICE: &str = "SimpleAdd";
pub const PRECOMPUTED_BACKEND_SERVICE: &str = "PrecomputedBackend";

pub const TEST_ENFORCER_WRITE_FILE: &str = "temp-test-enforcer-write";
pub const TEST_CONTAINER_WRITE_FILE: &str = "temp-test-container-write";
pub const TEST_ENFORCER_READ_FILE: &str = "temp-test-enforcer-read";
pub const TEST_CONTAINER_READ_FILE: &str = "temp-test-container-read";
pub const DEV_SHM_PATH: &str = "/dev/shm/";

#[derive(Debug)]
pub struct TestHarness {
    pub isolate_junction: FakeJunction,
    pub isolate_state_manager: IsolateStateManager,
    pub isolate_service_mapper: IsolateServiceMapper,
    pub isolate_ez_service_manager: IsolateEzServiceManager,
    pub manifest_validator: ManifestValidator,
    pub container_manager_requester: ContainerManagerRequester,
    pub data_scope_requester: DataScopeRequester,
    pub shared_memory_manager: SharedMemManager,
    pub container_manager: ContainerManager<FakeContainer>,
    pub interceptor: Interceptor,
}

impl TestHarness {
    pub async fn new(
        manifest_path: &str,
        isolate_runtime_configs: &IsolateRuntimeConfigs,
        otel_endpoint: Option<String>,
        operator_role: String,
    ) -> Result<Self> {
        Self::new_impl(
            ManifestSource::V1 { manifest_path: manifest_path.to_string() },
            isolate_runtime_configs,
            otel_endpoint,
            None,
            operator_role,
        )
        .await
    }

    pub async fn new_v2(
        setup_isolate_manifest_path: &str,
        isolate_runtime_configs: &IsolateRuntimeConfigs,
        operator_role: String,
    ) -> Result<Self> {
        Self::new_impl(
            ManifestSource::V2 {
                setup_isolate_manifest_path: setup_isolate_manifest_path.to_string(),
            },
            isolate_runtime_configs,
            None,
            None,
            operator_role,
        )
        .await
    }

    pub async fn new_with_otel_traces(
        manifest_path: &str,
        isolate_runtime_configs: &IsolateRuntimeConfigs,
        otel_endpoint: Option<String>,
        otel_traces_endpoint: Option<String>,
        operator_role: String,
    ) -> Result<Self> {
        Self::new_impl(
            ManifestSource::V1 { manifest_path: manifest_path.to_string() },
            isolate_runtime_configs,
            otel_endpoint,
            otel_traces_endpoint,
            operator_role,
        )
        .await
    }

    pub async fn load_workload_manifests_from_paths(
        &self,
        ratified_path: Option<&str>,
        opaque_path: Option<&str>,
    ) -> Result<()> {
        let workload_manifests = load_workload_manifests_from_paths(ratified_path, opaque_path)?;
        self.container_manager.load_workload_isolates(workload_manifests).await?;
        Ok(())
    }

    async fn new_impl(
        manifest_source: ManifestSource,
        isolate_runtime_configs: &IsolateRuntimeConfigs,
        otel_endpoint: Option<String>,
        otel_traces_endpoint: Option<String>,
        operator_role: String,
    ) -> Result<Self> {
        // Clear the tracker for test isolation.
        FakeContainer::clear_tracker();

        let (container_manager_request_tx, container_manager_request_rx) =
            channel::<ContainerManagerRequest>(CHANNEL_SIZE);

        let container_manager_requester =
            ContainerManagerRequester::new(container_manager_request_tx);
        let data_scope_requester = DataScopeRequester::new(u64::MAX);
        let isolate_state_manager = IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        );
        let isolate_service_mapper = IsolateServiceMapper::default();
        let manifest_validator = ManifestValidator::default();
        let shared_memory_manager = SharedMemManager::new(
            container_manager_requester.clone(),
            SHM_NUM_SLOTS,
            SHM_SLOT_SIZE,
        );
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
                otel_endpoint: otel_endpoint.clone(),
                disable_metrics_filtering: false,
                shm_payload_threshold: SHM_PAYLOAD_THRESHOLD,
            });

        let container_manager_args = ContainerManagerArgs {
            isolate_junction: Box::new(isolate_junction.clone()),
            container_manager_request_rx,
            isolate_state_manager: isolate_state_manager.clone(),
            isolate_service_mapper: isolate_service_mapper.clone(),
            isolate_ez_service_mngr: isolate_ez_service_manager.clone(),
            manifest_validator: manifest_validator.clone(),
            shared_mem_manager: shared_memory_manager.clone(),
            fileshare_manager: fileshare_manager.clone(),
            manifest_source,
            common_bind_mounts: vec![],
            max_decoding_message_size: MAX_DECODING_SIZE,
            isolate_runtime_configs: isolate_runtime_configs.clone(),
            interceptor: interceptor.clone(),
            otel_traces_endpoint,
            run_isolate_as_unprivileged: false,
            enable_syscall_filtering: false,
            shm_num_slots: SHM_NUM_SLOTS,
            shm_slot_size: SHM_SLOT_SIZE,
            shm_payload_threshold: SHM_PAYLOAD_THRESHOLD,
            operator_role,
        };

        let container_manager = ContainerManager::<FakeContainer>::start(container_manager_args)
            .await
            .context("Container Manager should start")?;

        let harness = Self {
            isolate_junction,
            isolate_state_manager,
            isolate_service_mapper,
            isolate_ez_service_manager,
            manifest_validator,
            container_manager_requester,
            data_scope_requester,
            shared_memory_manager,
            container_manager,
            interceptor,
        };

        // Wait for the Isolate to be connected by the ContainerManager.
        timeout(Duration::from_secs(30), async {
            while harness.isolate_junction.connected_isolates.is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("Isolate did not connect within the timeout")?;

        Ok(harness)
    }

    pub async fn stop(&mut self) {
        let mut isolate_ids = Vec::with_capacity(3);
        self.isolate_junction
            .connected_isolates
            .iter()
            .for_each(|entry| isolate_ids.push(*entry.key()));
        for isolate_id in isolate_ids.iter() {
            self.isolate_ez_service_manager.stop_isolate_servers(*isolate_id).await
        }
        self.container_manager.stop().await;
    }
}

// Helper function to create a gRPC client for the IsolateEzBridge service
// over a Unix Domain Socket.
pub async fn start_isolate_ez_bridge(
    isolate_ez_bridge_enforcer_side_uds_path: &str,
) -> Result<IsolateEzBridgeClient<Channel>> {
    // Wait for the Isolate EZ server to start up
    let uds_path = isolate_ez_bridge_enforcer_side_uds_path.to_string();
    let open_future = spawn_blocking(move || {
        let isolate_fifo_path = format!("{}.ready", uds_path);
        OpenOptions::new().read(true).open(isolate_fifo_path).unwrap();
    });
    timeout(Duration::from_millis(100), open_future)
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for Isolate EZ server to be ready."))?
        .map_err(|e| anyhow::anyhow!("Failed to open ready pipe: {}", e))?;

    let endpoint = Endpoint::from_shared(
        "http://localhost:".to_owned() + isolate_ez_bridge_enforcer_side_uds_path,
    )
    .context("Invalid endpoint")?;
    let channel = endpoint
        .connect_with_connector(service_fn(|uri: Uri| async move {
            let path = uri.path();
            Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(path).await?))
        }))
        .await
        .context("Invalid channel")?;
    Ok(IsolateEzBridgeClient::new(channel))
}

// Helper function to notify the IsolateEzBridge service that an Isolate is ready.
// It sends a `NotifyIsolateStateRequest` with the `Ready` state and waits for
// a `NotifyIsolateStateResponse`.
// CAUTION: Use this function only to mark an Isolate ready so that the read end of
// the ready pipe is opened and the test doesn't gets stalled.
pub async fn notify_ready(
    client: &mut IsolateEzBridgeClient<Channel>,
) -> Result<NotifyIsolateStateResponse> {
    let (notify_isolate_state_tx, notify_isolate_state_rx) =
        channel::<NotifyIsolateStateRequest>(CHANNEL_SIZE);
    let outbound_stream = ReceiverStream::new(notify_isolate_state_rx);
    let inbound = client
        .notify_isolate_state(outbound_stream)
        .await
        .context("Failed to get inbound channel")?;
    notify_isolate_state_tx
        .send(NotifyIsolateStateRequest { new_isolate_state: IsolateState::Ready.into() })
        .await
        .context("Failed to get notify isolate sender")?;

    let mut invoke_isolate_response_stream: SimpleStreamingWrapper<NotifyIsolateStateResponse> =
        inbound.into_inner().into();
    let response = invoke_isolate_response_stream.message().await;
    ensure!(response.is_some(), "Failed to receive Notify Isolate State Response");
    Ok(response.unwrap())
}

// Helper function to send a unary `InvokeEzRequest` to the IsolateEzBridge service
// and return the `InvokeEzResponse`.
pub async fn invoke_ez(
    client: &mut IsolateEzBridgeClient<Channel>,
    invoke_ez_request: InvokeEzRequest,
) -> Result<InvokeEzResponse> {
    Ok(client.invoke_ez(invoke_ez_request).await?.into_inner())
}

// Helper function to create a sample `InvokeEzRequest` with a random payload for testing.
pub fn create_random_request(isolate_service_info: &IsolateServiceInfo) -> InvokeEzRequest {
    // Generate 8 random bytes for request
    let random_request_data =
        [0u8; 8].into_iter().map(|_| rand::random::<u8>()).collect::<Vec<_>>();
    let original_msg_id = rand::random();

    InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            ipc_message_id: original_msg_id,
            requester_spiffe: String::new(),
            requester_is_local: false,
            responder_is_local: true,
            destination_operator_domain: isolate_service_info.operator_domain.clone(),
            destination_service_name: isolate_service_info.service_name.clone(),
            destination_method_name: SAY_HELLO_METHOD.to_string(),
            shared_memory_handles: Vec::new(),
            fileshare_handles: Vec::new(),
            destination_ez_instance_id: "".to_string(),
            ..Default::default()
        }),
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                mapped_scope_owner: None,
            }],
        }),
        isolate_request_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![random_request_data.to_vec()],
            })),
        }),
    }
}

// Helper function to create an `InvokeEzResponse` that echoes the data from an
// `InvokeEzRequest`, simulating an echo service for tests.
pub fn create_echo_invoke_ez_response(invoke_isolate_request: InvokeEzRequest) -> InvokeEzResponse {
    InvokeEzResponse {
        control_plane_metadata: invoke_isolate_request.control_plane_metadata.clone(),
        ez_response_iscope: invoke_isolate_request.isolate_request_iscope,
        ez_response_payload: invoke_isolate_request.isolate_request_payload,
        response_extensions: invoke_isolate_request
            .control_plane_metadata
            .map(|m| m.extensions)
            .unwrap_or_default(),
    }
}

// Helper function to verify that containers have been started correctly. It checks that the
// number of tracked containers matches the expected count and that their status is `Started`.
// It returns a vector of tuples, each containing a container's ID and its UDS path.
pub async fn check_container_started(binary_file_names: Vec<&str>) -> Result<Vec<(u64, String)>> {
    // Assert the container was created and tracked.
    let tracker = FakeContainer::get_tracker();
    ensure!(
        tracker.len() == binary_file_names.len(),
        "Tracker length mismatched. Expected: {}, Got: {}",
        binary_file_names.len(),
        tracker.len()
    );

    let mut results: Vec<(u64, String)> = vec![];
    for entry in tracker.iter() {
        let container_data = entry.value();
        let actual_binary_filename = container_data
            .binary_filename
            .as_ref()
            .context("Expected a valid binary filename, but it was None")?;
        ensure!(
            binary_file_names.contains(&actual_binary_filename.as_str()),
            "Binary Filename not found: Expected from {:?}, Got: {}",
            binary_file_names,
            actual_binary_filename
        );
        ensure!(
            container_data.status == Status::Started,
            "Status mismatched. Expected: {:?}, Got: {:?}",
            Status::Started,
            container_data.status
        );

        let isolate_ez_bridge_enforcer_side_uds_path = container_data
            .isolate_ez_bridge_enforcer_side_uds_path
            .as_ref()
            .context("Expected a valid UDS path for isolate-ez-bridge, but it was None")?
            .clone();
        results.push((*entry.key(), isolate_ez_bridge_enforcer_side_uds_path));
    }

    Ok(results)
}

// Helper function to wait for containers to start and notify them ready in dependency order.
pub async fn start_and_ready_containers_in_dependency_order(
    binary_file_names: Vec<&str>,
) -> Result<Vec<(u64, String)>> {
    let mut results: Vec<(u64, String)> = vec![];
    let mut readied_container_ids = std::collections::HashSet::new();

    timeout(Duration::from_secs(10), async {
        loop {
            let tracker = FakeContainer::get_tracker();
            for entry in tracker.iter() {
                let container_id = *entry.key();
                if readied_container_ids.contains(&container_id) {
                    continue;
                }
                let container_data = entry.value();
                if let Some(actual_binary_filename) = &container_data.binary_filename {
                    if binary_file_names.contains(&actual_binary_filename.as_str())
                        && container_data.status == Status::Started
                    {
                        if let Some(uds_path) =
                            &container_data.isolate_ez_bridge_enforcer_side_uds_path
                        {
                            let _client = notify_isolate_ready(uds_path.clone()).await?;
                            readied_container_ids.insert(container_id);
                            results.push((container_id, uds_path.clone()));
                        }
                    }
                }
            }
            if results.len() == binary_file_names.len() {
                return Ok(results);
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("Timed out waiting for containers to start and be readied")?
}

// Helper function that establishes a client connection to an Isolate's gRPC server
// and sends a `Ready` state notification.
pub async fn notify_isolate_ready(
    isolate_ez_bridge_enforcer_side_uds_path: String,
) -> Result<IsolateEzBridgeClient<Channel>> {
    // Verify that the server is started by IsolateEzServiceManager
    let mut client = start_isolate_ez_bridge(&isolate_ez_bridge_enforcer_side_uds_path).await?;
    // Notify Isolate Ready
    notify_ready(&mut client).await.context("Should notify ready")?;
    Ok(client)
}

// Helper function to ensure an Isolate is fully ready for testing. It notifies the
// Isolate that it is ready and then verifies that the `IsolateStateManager` correctly
// handles the state update.
pub async fn ensure_isolate_ready(
    harness: &mut TestHarness,
    isolate_ez_bridge_enforcer_side_uds_path: String,
    isolate_id: IsolateId,
) -> Result<IsolateEzBridgeClient<Channel>> {
    let client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path).await?;

    // Assert that Isolate is added in Isolate State Manager via IsolateEZ Bridge
    ensure!(
        harness.isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.is_err(),
        "Expected state update to fail with IsolateStateManagerError::DuplicateStateUpdate."
    );

    let err = harness
        .isolate_state_manager
        .update_state(isolate_id, IsolateState::Ready)
        .await
        .err()
        .unwrap();
    ensure!(
        matches!(err.downcast().unwrap(), IsolateStateManagerError::DuplicateStateUpdate),
        "Expected IsolateStateManagerError::DuplicateStateUpdate error"
    );

    Ok(client)
}

// Helper function to retrieve the `BinaryServicesIndex` for a given service,
// abstracting the interaction with the `IsolateServiceMapper`.
pub async fn get_binary_service_index(
    harness: &mut TestHarness,
    operator_domain: String,
    service_name: String,
) -> Result<BinaryServicesIndex> {
    // Assert that Isolate is added in Isolate Service Mapper
    harness
        .isolate_service_mapper
        .get_binary_index(&IsolateServiceInfo {
            operator_domain,
            service_name,
            ..Default::default()
        })
        .await
        .context("Should be valid binary index")
}

// Helper function to ensure that a container has been stopped. It checks the
// container's status in the `FakeContainer` tracker.
pub async fn ensure_isolate_stopped(fake_container_id: u64) -> Result<()> {
    // Assert the container was stopped.
    let tracker = FakeContainer::get_tracker();
    let tracked_container = tracker.get(&fake_container_id).context("Should have isolate")?;
    ensure!(
        tracked_container.value().status == Status::Stopped,
        "Expected container status to be Stopped, but it was {:?}",
        tracked_container.value().status
    );
    Ok(())
}

// Helper function to verify that the manifest's data scopes are correctly validated.
pub async fn check_valid_manifest_scopes(
    harness: &mut TestHarness,
    binary_services_index: BinaryServicesIndex,
    pass_input_scope: DataScopeType,
    pass_output_scope: DataScopeType,
) -> Result<()> {
    harness
        .manifest_validator
        .validate_input_scope(ValidateManifestInputScopeRequest {
            binary_services_index,
            requested_scope: pass_input_scope,
        })
        .await
        .context("Validation of input manifest scope should pass")?;
    harness
        .manifest_validator
        .validate_output_scope(ValidateManifestOutputScopeRequest {
            binary_services_index,
            emitted_scope: pass_output_scope,
        })
        .await
        .context("Validation of output manifest scope should pass")?;
    Ok(())
}

pub async fn check_invalid_manifest_scopes(
    harness: &mut TestHarness,
    binary_services_index: BinaryServicesIndex,
    fail_input_scope: DataScopeType,
    fail_output_scope: DataScopeType,
) -> Result<()> {
    let input_scope_result = harness
        .manifest_validator
        .validate_input_scope(ValidateManifestInputScopeRequest {
            binary_services_index,
            requested_scope: fail_input_scope,
        })
        .await;
    ensure!(
        matches!(input_scope_result, Err(DataScopeError::DisallowedByManifest)),
        "Validation of input manifest scope should fail with DisallowedByManifest, but got {:?}",
        input_scope_result
    );

    let output_scope_result = harness
        .manifest_validator
        .validate_output_scope(ValidateManifestOutputScopeRequest {
            binary_services_index,
            emitted_scope: fail_output_scope,
        })
        .await;
    ensure!(
        matches!(output_scope_result, Err(DataScopeError::DisallowedByManifest)),
        "Validation of output manifest scope should fail with DisallowedByManifest, but got {:?}",
        output_scope_result
    );

    Ok(())
}

pub fn create_call_request_for_interceptor() -> CallRequest {
    CallRequest {
        operator_domain: "playground_example".to_string(),
        service_name: "SimpleAdd".to_string(),
        method_name: "StreamingIntegerSequence".to_string(),
        ..Default::default()
    }
}

pub struct MockMetricsService {
    pub tx: tokio::sync::mpsc::Sender<ExportMetricsServiceRequest>,
}

#[tonic::async_trait]
impl MetricsService for MockMetricsService {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        let _ = self.tx.send(request.into_inner()).await;
        Ok(tonic::Response::new(ExportMetricsServiceResponse { partial_success: None }))
    }
}

pub async fn start_mock_otel_server(
) -> (String, tokio::sync::mpsc::Receiver<ExportMetricsServiceRequest>, tokio::task::JoinHandle<()>)
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let endpoint_url = format!("http://127.0.0.1:{}", port);
    let (tx, rx) = channel(10);
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(MetricsServiceServer::new(MockMetricsService { tx }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (endpoint_url, rx, server_handle)
}

pub async fn connect_metrics_uds_client(
    socket_path: std::path::PathBuf,
) -> opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient<
    Channel,
>{
    let endpoint = Endpoint::from_shared("http://127.0.0.1".to_string()).unwrap();
    let channel = endpoint
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let socket_path = socket_path.clone();
            async move {
                Ok::<_, std::io::Error>(TokioIo::new(
                    tokio::net::UnixStream::connect(socket_path).await?,
                ))
            }
        }))
        .await
        .unwrap();
    opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient::new(channel)
}

pub async fn setup_isolate_metrics_client(
    container_data: &container_test_utils::FakeContainerData,
) -> opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient<
    Channel,
>{
    let isolate_ez_bridge_enforcer_side_uds_path =
        container_data.isolate_ez_bridge_enforcer_side_uds_path.clone().unwrap();
    let _ = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path).await;

    let sharing_dir_path = &container_data.boot_mounts[0].source;
    let otlp_metrics_uds_path = sharing_dir_path.join("otlp-metrics.sock");

    let mut attempts = 0;
    while !otlp_metrics_uds_path.exists() && attempts < 100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        attempts += 1;
    }
    assert!(otlp_metrics_uds_path.exists(), "Metrics UDS path was never created");

    connect_metrics_uds_client(otlp_metrics_uds_path).await
}

pub fn create_test_export_metrics_request(metric_name: &str) -> ExportMetricsServiceRequest {
    let metric = opentelemetry_proto::tonic::metrics::v1::Metric {
        name: metric_name.to_string(),
        description: "".to_string(),
        unit: "".to_string(),
        data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(
            opentelemetry_proto::tonic::metrics::v1::Gauge {
                data_points: vec![opentelemetry_proto::tonic::metrics::v1::NumberDataPoint {
                    attributes: vec![],
                    value: Some(
                        opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(
                            42,
                        ),
                    ),
                    ..Default::default()
                }],
            },
        )),
        ..Default::default()
    };

    ExportMetricsServiceRequest {
        resource_metrics: vec![opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
            resource: Some(Default::default()),
            scope_metrics: vec![opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                scope: None,
                metrics: vec![metric],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    }
}

pub fn service_info_has_valid_binary_index(index: &IsolateServiceIndex) -> bool {
    index.get_binary_services_index().is_some()
}
