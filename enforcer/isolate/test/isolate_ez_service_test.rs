// Copyright 2025 Google LLC
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

use anyhow::{Context, Result};
use container_manager_request::ContainerManagerRequest;
use container_manager_requester::ContainerManagerRequester;
use data_scope::{
    manifest_validator::ManifestValidator,
    request::{
        AddBackendDependenciesRequest, AddIsolateRequest, GetIsolateScopeRequest,
        ValidateIsolateRequest,
    },
    requester::DataScopeRequester,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    isolate_ez_bridge_client::IsolateEzBridgeClient,
    isolate_ez_bridge_server::IsolateEzBridgeServer, ControlPlaneMetadata, CreateMemshareRequest,
    EzPayloadIsolateScope, InvokeEzRequest, InvokeEzResponse, IsolateDataScope, IsolateState,
    IsolateStatus, NotifyIsolateStateRequest, NotifyIsolateStateResponse, PollIsolateStateRequest,
};
use external_proxy_connector::{ExternalProxyChannel, ExternalProxyConnectorError};
use external_proxy_connector_constants::EXTERNAL_PREFIX;
use hyper_util::rt::tokio::TokioIo;
use isolate_ez_service::{IsolateEzBridgeDependencies, IsolateEzBridgeService};
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use isolate_test_utils::{DefaultEchoIsolate, ScopeDragInstruction};
use junction_test_utils::FakeJunction;
use manifest_proto::enforcer::ez_backend_dependency::RouteType;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use payload_proto::enforcer::v1::EzPayloadData;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{sleep, timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::Streaming;
use tonic::transport::{Channel, Endpoint, Server};
use tower::service_fn;

const CHANNEL_SIZE: usize = 10;
const REGION_SIZE: i64 = 4096;

const TEST_REMOTE_OPERATOR_DOMAIN: &str = "some.remote.service";
const TEST_SERVICE_NAME: &str = "TestService";
const TEST_INTERNAL_OPERATOR_DOMAIN: &str = "internal";
const TEST_UNKNOWN_DOMAIN: &str = "unknown.domain";
const TEST_UNKNOWN_SERVICE_NAME: &str = "UnknownService";
const TEST_SOME_DOMAIN: &str = "some.domain";
const TEST_SOME_SERVICE_NAME: &str = "some_service";
const RATIFIED_INTERCEPTOR_DOMAIN: &str = "ratified.interceptor.domain";
const RATIFIED_INTERCEPTOR_SERVICE: &str = "RatifiedInterceptorService";
const TEST_INTERNAL_ROUTE_TYPE: RouteType = RouteType::Internal;
const TEST_EXTERNAL_ROUTE_TYPE: RouteType = RouteType::External;
const TEST_REMOTE_ROUTE_TYPE: RouteType = RouteType::Remote;

// Creates a Mock Proxy Component for testing Ez-to-External external streaming calls.
#[derive(Clone, Debug, Default)]
struct MockExternalProxy {
    call_count: Arc<AtomicUsize>,
    received_from_bridge_count: Arc<AtomicUsize>,
    delay: Arc<Mutex<Duration>>,
}

#[tonic::async_trait]
impl ExternalProxyChannel for MockExternalProxy {
    async fn proxy_external(
        &self,
        _: IsolateId,
        _: InvokeEzRequest,
        timeout: Option<std::time::Duration>,
    ) -> Result<InvokeEzResponse, ExternalProxyConnectorError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        if let Some(t) = timeout {
            let delay = *self.delay.lock().unwrap();
            if delay > t {
                return Err(ExternalProxyConnectorError::ConnectionFailed(
                    "Mock proxy timeout".to_string(),
                ));
            }
        }
        let delay = *self.delay.lock().unwrap();
        sleep(delay).await;
        Ok(get_sample_invoke_ez_response())
    }
    async fn stream_proxy_external(
        &self,
        _: IsolateId,
        mut from_bridge_rx: Receiver<InvokeEzRequest>,
    ) -> Result<Receiver<InvokeEzResponse>, ExternalProxyConnectorError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let received_from_bridge_count = self.received_from_bridge_count.clone();

        let (to_bridge_tx, to_bridge_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            while from_bridge_rx.recv().await.is_some() {
                received_from_bridge_count.fetch_add(1, Ordering::SeqCst);
                to_bridge_tx
                    .send(get_sample_invoke_ez_response())
                    .await
                    .expect("Should be able to send response");
            }
        });
        Ok(to_bridge_rx)
    }
    fn clone_channel(&self) -> Box<dyn ExternalProxyChannel> {
        Box::new(self.clone())
    }
}

// Mock for EzToEzOutboundHandler for testing remote routing.
#[derive(Clone, Debug, Default)]
struct MockEzToEzOutboundHandler {
    call_count: Arc<AtomicUsize>,
    delay: Arc<Mutex<Duration>>,
}

#[tonic::async_trait]
impl OutboundEzToEzClient for MockEzToEzOutboundHandler {
    async fn remote_invoke(
        &self,
        _request: InvokeEzRequest,
        timeout: Option<std::time::Duration>,
    ) -> anyhow::Result<InvokeEzResponse> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        if let Some(t) = timeout {
            let delay = *self.delay.lock().unwrap();
            if delay > t {
                return Err(anyhow::anyhow!("Mock remote_invoke timeout"));
            }
        }
        let delay = *self.delay.lock().unwrap();
        sleep(delay).await;
        Ok(InvokeEzResponse::default())
    }

    async fn remote_streaming_connect(
        &self,
        mut from_local_rx: Receiver<InvokeEzRequest>,
    ) -> anyhow::Result<Receiver<Result<InvokeEzResponse>>> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let (to_caller_tx, to_caller_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            while from_local_rx.recv().await.is_some() {
                to_caller_tx
                    .send(Ok(get_sample_invoke_ez_response()))
                    .await
                    .expect("Should be able to send response");
            }
        });
        Ok(to_caller_rx)
    }
}

#[derive(Debug)]
struct TestHarness {
    isolate_id: IsolateId,
    mock_junction: FakeJunction,
    mock_proxy: MockExternalProxy,
    mock_ez_to_ez: MockEzToEzOutboundHandler,
    mapper: IsolateServiceMapper,
    isolate_state_manager: IsolateStateManager,
    data_scope_requester: DataScopeRequester,
    manifest_validator: ManifestValidator,
    container_manager_rx: Receiver<ContainerManagerRequest>,
    client: IsolateEzBridgeClient<Channel>,
    interceptor: interceptor::Interceptor,
}

impl TestHarness {
    async fn new_with_arguments(
        is_ratified: bool,
        instruction: ScopeDragInstruction,
    ) -> Result<Self> {
        let mock_proxy = MockExternalProxy::default();
        let mut mock_junction = FakeJunction::default();
        mock_junction.set_fake_isolate(Box::new(DefaultEchoIsolate::new(instruction, None)));

        let mock_ez_to_ez = MockEzToEzOutboundHandler::default();
        let service_mapper = IsolateServiceMapper::default();
        let isolate_id = IsolateId::new(BinaryServicesIndex::new(is_ratified));

        let (tx, container_manager_rx) = mpsc::channel(1);
        let container_manager_requester = ContainerManagerRequester::new(tx);
        let data_scope_requester = DataScopeRequester::new(0);

        let manifest_validator = ManifestValidator::default();
        let isolate_state_manager = IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        );
        let shared_memory_manager = SharedMemManager::new(container_manager_requester);
        let interceptor_instance = interceptor::Interceptor::new(service_mapper.clone());

        let deps = IsolateEzBridgeDependencies {
            isolate_id,
            isolate_junction: Box::new(mock_junction.clone()),
            isolate_state_manager: isolate_state_manager.clone(),
            shared_memory_manager,
            external_proxy_connector: Some(Box::new(mock_proxy.clone())),
            isolate_service_mapper: service_mapper.clone(),
            data_scope_requester: data_scope_requester.clone(),
            manifest_validator: manifest_validator.clone(),
            ez_to_ez_outbound_handler: Some(Box::new(mock_ez_to_ez.clone())),
            interceptor: interceptor_instance.clone(),
        };
        let isolate_ez_bridge_service = IsolateEzBridgeService::new(deps);
        let client = spawn_test_server(isolate_ez_bridge_service).await;

        Ok(Self {
            isolate_id,
            mock_junction,
            mock_proxy,
            mock_ez_to_ez,
            mapper: service_mapper,
            isolate_state_manager,
            data_scope_requester,
            manifest_validator,
            container_manager_rx,
            client,
            interceptor: interceptor_instance,
        })
    }

    async fn new() -> Result<Self> {
        Self::new_with_arguments(false, ScopeDragInstruction::KeepSame).await
    }
}

/// Test case for Isolate to find an internal service using Junction Component.
#[tokio::test]
async fn stream_routes_to_internal_junction() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&service_info.operator_domain, &service_info.service_name))
        .await
        .expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn stream_routes_to_external_proxy() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let proxy_calls = harness.mock_proxy.call_count.clone();
    let bridge_to_proxy_messages = harness.mock_proxy.received_from_bridge_count.clone();
    let domain = format!("{}some.service", EXTERNAL_PREFIX);

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: domain.clone(),
            service_name: TEST_SERVICE_NAME.to_string(),
        },
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&domain, TEST_SERVICE_NAME))
        .await
        .expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1, "External proxy should have been called");
    assert_eq!(
        bridge_to_proxy_messages.load(Ordering::SeqCst),
        1,
        "Bridge should have had a message for the connector."
    );
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_routes_to_internal_junction() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let response = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(response.get_ref().status.as_ref().expect("Response should have a status").code, 0);
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn unary_routes_to_external_proxy() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let proxy_calls = harness.mock_proxy.call_count.clone();
    let domain = format!("{}some.service", EXTERNAL_PREFIX);

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: domain.clone(),
            service_name: TEST_SERVICE_NAME.to_string(),
        },
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request(&domain, TEST_SERVICE_NAME);
    let response = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(response.get_ref().status.as_ref().expect("Response should have a status").code, 0);
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1, "External proxy should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn stream_routes_to_remote_handler() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
            service_name: TEST_SERVICE_NAME.to_string(),
        },
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME))
        .await
        .expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;
    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_routes_to_remote_handler() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
            service_name: TEST_SERVICE_NAME.to_string(),
        },
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    let response = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(response.get_ref().status, None); // Mock returns default response.
    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_routes_to_remote_handler_with_ez_instance_id() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let mut request = create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    request.control_plane_metadata.as_mut().unwrap().destination_ez_instance_id =
        "some_instance_id".to_string();
    let response = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(response.get_ref().status, None); // Mock returns default response.
    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn notify_isolate_state_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: harness.isolate_id,
        })
        .await;

    let (tx, mut rx) = setup_notify_state_stream(&mut harness.client)
        .await
        .expect("Should be able to setup notify state stream");

    tx.send(NotifyIsolateStateRequest { new_isolate_state: IsolateState::Ready.into() })
        .await
        .expect("Should be able to send state update");

    let response = rx.message().await;
    assert!(response.is_ok(), "Expected a successful state update response");
}

#[tokio::test]
async fn notify_isolate_state_failure() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: harness.isolate_id,
        })
        .await;

    let (tx, mut rx) = setup_notify_state_stream(&mut harness.client)
        .await
        .expect("Should be able to setup notify state stream");

    // Send the same state twice to trigger a DuplicateStateUpdate error.
    tx.send(NotifyIsolateStateRequest { new_isolate_state: IsolateState::Ready.into() })
        .await
        .expect("Should be able to send state update");
    let response = rx.message().await;
    assert!(response.is_ok(), "Expected a successful state update response");

    tx.send(NotifyIsolateStateRequest { new_isolate_state: IsolateState::Ready.into() })
        .await
        .expect("Should be able to send state update");

    // The service sends an error, then a success. We check for the error.
    let response = rx.message().await;
    assert!(response.is_err(), "Expected a state update failure");
    assert_eq!(response.expect_err("Expected an error response").code(), tonic::Code::Aborted);
}

#[tokio::test]
async fn poll_isolate_state_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: harness.isolate_id,
        })
        .await;

    harness
        .isolate_state_manager
        .update_state(harness.isolate_id, IsolateState::Ready)
        .await
        .expect("Should be able to update state");

    let response = harness
        .client
        .poll_isolate_state(PollIsolateStateRequest {})
        .await
        .expect("Should succeed");

    assert_eq!(response.get_ref().isolate_state, IsolateState::Ready.into());
}

#[tokio::test]
async fn create_memshare_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: harness.isolate_id,
        })
        .await;

    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .create_memshare(ReceiverStream::new(rx))
        .await
        .expect("create_memshare should succeed")
        .into_inner();

    tx.send(CreateMemshareRequest { region_size: REGION_SIZE })
        .await
        .expect("Should be able to send memshare request");

    // Check that ContainerManager receives a MountWritableFile request.
    let cm_request = timeout(Duration::from_secs(1), harness.container_manager_rx.recv())
        .await
        .expect("ContainerManager did not receive request in time")
        .expect("ContainerManager request channel should not be closed while harness is in scope");

    let (mount_req, resp_tx) = match cm_request {
        ContainerManagerRequest::MountWritableFile { req, resp } => (req, resp),
        _ => panic!("Expected MountWritableFile request"),
    };
    assert_eq!(mount_req.isolate_id, harness.isolate_id);
    assert_eq!(mount_req.region_size, REGION_SIZE);

    // Simulate successful mount by ContainerManager.
    resp_tx
        .send(Ok(container_manager_request::MountFileResponse {}))
        .expect("Should be able to send MountFileResponse");

    // Check for a successful response from create_memshare.
    let response = response_stream
        .message()
        .await
        .expect("Stream should have a response")
        .expect("Response should be Ok");

    assert!(response.status.is_none());
    assert!(!response.shared_memory_handle.is_empty());
}

#[tokio::test]
async fn create_memshare_freeze_scope_failure() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    // Do not add the Isolate to the state manager to cause freeze_scope to fail.

    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .create_memshare(ReceiverStream::new(rx))
        .await
        .expect("create_memshare should succeed")
        .into_inner();

    tx.send(enforcer_proto::enforcer::v1::CreateMemshareRequest { region_size: REGION_SIZE })
        .await
        .expect("Should be able to send memshare request");

    // Check for a failure response from create_memshare.
    let response = response_stream
        .message()
        .await
        .expect("Stream should have a response")
        .expect("Response should be Ok");

    let status = response.status.expect("Response should have a status");
    assert_eq!(status.code, 7); // PERMISSION_DENIED
    assert!(response.shared_memory_handle.is_empty());

    // Ensure ContainerManager is not called.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), harness.container_manager_rx.recv())
            .await
            .is_err(),
        "ContainerManager should not have been called"
    );
}

#[tokio::test]
async fn unary_invoke_ez_fails_without_control_plane_metadata() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let request = InvokeEzRequest::default(); // No metadata
    let response = harness.client.invoke_ez(request).await;
    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::Internal);
}

#[tokio::test]
async fn stream_invoke_ez_fails_without_control_plane_metadata() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx.send(InvokeEzRequest::default()).await.expect("Should send request successfully");
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::Internal);
}

#[tokio::test]
async fn unary_invoke_ez_fails_for_unknown_service() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let request = create_test_request(TEST_UNKNOWN_DOMAIN, TEST_UNKNOWN_SERVICE_NAME);
    let response = harness.client.invoke_ez(request).await;
    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn stream_invoke_ez_fails_for_unknown_service() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(TEST_UNKNOWN_DOMAIN, TEST_UNKNOWN_SERVICE_NAME))
        .await
        .expect("Should send request successfully");
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn unary_invoke_ez_fails_manifest_validation() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_SOME_DOMAIN.to_string(),
        service_name: TEST_SOME_SERVICE_NAME.to_string(),
    };
    // Add to mapper but not to manifest validator.
    harness
        .mapper
        .add_backend_dependency_service(&service_info, TEST_REMOTE_ROUTE_TYPE)
        .await
        .expect("Failed to add backend dependency");
    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let response = harness.client.invoke_ez(request).await;
    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn stream_invoke_ez_fails_manifest_validation() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_SOME_DOMAIN.to_string(),
        service_name: TEST_SOME_SERVICE_NAME.to_string(),
    };
    // Add to mapper but not to manifest validator.
    harness
        .mapper
        .add_backend_dependency_service(&service_info, TEST_REMOTE_ROUTE_TYPE)
        .await
        .expect("Failed to add backend dependency");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&service_info.operator_domain, &service_info.service_name))
        .await
        .expect("Should send request successfully");
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn unary_invoke_ez_fails_scope_enforcement() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let mut request = create_test_request(TEST_SOME_DOMAIN, TEST_SOME_SERVICE_NAME);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);

    harness
        .data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Request with public scope fails
    let response = harness.client.invoke_ez(request).await;
    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn stream_invoke_ez_fails_scope_enforcement() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let mut request = create_test_request(TEST_SOME_DOMAIN, TEST_SOME_SERVICE_NAME);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);

    harness
        .data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Request with public scope fails
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx.send(request).await.expect("Should send request successfully");
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn stream_invoke_ez_fails_scope_enforcement_on_second_request() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let junction_steam_calls = harness.mock_junction.stream_call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");

    // Initial scope is Public.
    // Set up the Isolate to only allow upto User Private scope.
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();

    // First request with Public scope, should pass.
    let mut request1 =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request1
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);
    req_tx.send(request1).await.expect("Should send request successfully");
    let response = response_stream.message().await;
    assert!(response.is_ok());
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(junction_steam_calls.load(Ordering::SeqCst), 1);

    // Update scope to UserPrivate.
    harness
        .data_scope_requester
        .validate_isolate_scope(ValidateIsolateRequest {
            requested_scope: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Second request with Public scope, should fail because current scope is now UserPrivate.
    // Request enforcements prevent emission of Public Scope while being in UserPrivate.
    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);
    req_tx.send(request).await.expect("Should send request successfully");

    // The error from the second request should be received.
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::PermissionDenied);
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(junction_steam_calls.load(Ordering::SeqCst), 1);

    // Third request should fail to send
    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    req_tx.send(request).await.expect_err("Should fail to send 3rd request");
}

#[tokio::test]
async fn stream_invoke_ez_ratified_isolate_skips_scope_enforcement_on_second_request() {
    let mut harness = TestHarness::new_with_arguments(true, ScopeDragInstruction::KeepSame)
        .await
        .expect("Harness should start"); // Ratified Isolate

    let junction_calls = harness.mock_junction.call_count.clone();
    let junction_steam_calls = harness.mock_junction.stream_call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");

    // Initial scope is Public.
    // Set up the Isolate to only allow upto User Private scope.
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();

    // First request with Public scope.
    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);
    req_tx.send(request).await.expect("Should send request successfully");

    let response = response_stream.message().await;
    assert!(response.is_ok());
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(junction_steam_calls.load(Ordering::SeqCst), 1);

    // Update scope to UserPrivate.
    harness
        .data_scope_requester
        .validate_isolate_scope(ValidateIsolateRequest {
            requested_scope: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Second request with Public scope. For Ratified Isolate, this should still pass.
    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);
    req_tx.send(request).await.expect("Should send request successfully");

    // There should be no error message on the stream.
    let response = response_stream.message().await;
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(junction_steam_calls.load(Ordering::SeqCst), 2);
    assert!(response.is_ok(), "Stream should not have any error messages");
}

#[tokio::test]
async fn unary_invoke_ez_fails_response_scope_enforcement() {
    // Create the test harness that increases response scope by one
    let mut harness = TestHarness::new_with_arguments(false, ScopeDragInstruction::IncreaseByOne)
        .await
        .expect("Harness should start");

    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");

    // Set up the Isolate to only allow upto User Private scope.
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    // Create a request with Private scope. The mock junction will echo this back with MultiUserPrivate.
    let request = create_test_request_with_scope(
        &service_info.operator_domain,
        &service_info.service_name,
        DataScopeType::UserPrivate,
    );
    let response = harness.client.invoke_ez(request).await;

    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::PermissionDenied);
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn stream_invoke_ez_fails_response_scope_enforcement() {
    // Create the test harness that increases response scope by one
    let mut harness = TestHarness::new_with_arguments(false, ScopeDragInstruction::IncreaseByOne)
        .await
        .expect("Harness should start");

    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        RouteType::Internal,
    )
    .await
    .expect("Failed to add backend dependency");

    // Set up the Isolate to be in UserPrivate scope.
    harness
        .data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request_with_scope(
            &service_info.operator_domain,
            &service_info.service_name,
            DataScopeType::Public,
        ))
        .await
        .expect("Should send request successfully");

    // Response fails because it is in MultiUserPrivate Scope
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::PermissionDenied);

    // Ensure that the Isolate remains in User Private
    let current_scope = harness
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id: harness.isolate_id })
        .await
        .expect("Valid GetIsolateScopeResponse")
        .current_scope;
    assert_eq!(current_scope, DataScopeType::UserPrivate);
}

#[tokio::test]
async fn unary_invoke_ez_succeeds_for_less_strict_response_scope() {
    // Create the test harness that decreases response scope by one
    let mut harness = TestHarness::new_with_arguments(false, ScopeDragInstruction::DecreaseByOne)
        .await
        .expect("Harness should start");

    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");

    // Set up the Isolate to be in UserPrivate scope.
    harness
        .data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Create a request with UserPrivate scope. The mock junction will echo this back with Domain Scope.
    // This should succeed because DomainOwned is less strict than UserPrivate.
    let request = create_test_request_with_scope(
        &service_info.operator_domain,
        &service_info.service_name,
        DataScopeType::UserPrivate,
    );
    assert!(harness.client.invoke_ez(request).await.is_ok(), "Request should succeed");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn stream_invoke_ez_fails_response_scope_enforcement_on_second_request() {
    // Create the test harness that decreases response scope by one
    let mut harness = TestHarness::new_with_arguments(false, ScopeDragInstruction::IncreaseByOne)
        .await
        .expect("Harness should start");

    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");

    // Set up the Isolate to only allow up to UserPrivate scope.
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();

    // First request with a valid scope should pass.
    let first_request = create_test_request_with_scope(
        &service_info.operator_domain,
        &service_info.service_name,
        DataScopeType::DomainOwned,
    );
    req_tx.send(first_request).await.expect("Should send request successfully");
    assert!(response_stream.message().await.is_ok());
    // Ensure that the Isolate is now in UserPrivateScope
    let current_scope = harness
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id: harness.isolate_id })
        .await
        .expect("Valid GetIsolateScopeResponse")
        .current_scope;
    assert_eq!(current_scope, DataScopeType::UserPrivate);

    // Second request with a scope that will be rejected on response.
    // Response enforcements fail to scope-drag Isolate to MultiUserPrivate.
    let second_request = create_test_request_with_scope(
        &service_info.operator_domain,
        &service_info.service_name,
        DataScopeType::UserPrivate,
    );

    // Maximum permissible scope is UserPrivate but response scope is MultiUserPrivate
    req_tx.send(second_request).await.expect("Should send request successfully");
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::PermissionDenied);

    // Ensure that the Isolate remains in UserPrivateScope
    let current_scope = harness
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id: harness.isolate_id })
        .await
        .expect("Valid GetIsolateScopeResponse")
        .current_scope;
    assert_eq!(current_scope, DataScopeType::UserPrivate);
}

#[tokio::test]
async fn verify_all_backend_dependency_routing() {
    let mut harness = TestHarness::new().await.expect("Harness should start");

    // INTERNAL Setup
    let internal_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "InternalService".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &internal_info,
        true, // is_internal = true
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add INTERNAL dependency");

    // EXTERNAL Setup
    let external_domain = format!("{}external.service", EXTERNAL_PREFIX);
    let external_info = IsolateServiceInfo {
        operator_domain: external_domain.clone(),
        service_name: "ExternalService".to_string(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &external_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add EXTERNAL dependency");

    // REMOTE Setup
    let remote_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: "RemoteService".to_string(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &remote_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add REMOTE dependency");

    // Add Isolate to Data Scope Manager (DSM)
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    harness.mock_junction.call_count.store(0, Ordering::SeqCst);
    harness.mock_proxy.call_count.store(0, Ordering::SeqCst);
    harness.mock_ez_to_ez.call_count.store(0, Ordering::SeqCst);

    // Test INTERNAL Routing (Junction)
    let internal_request =
        create_test_request(&internal_info.operator_domain, &internal_info.service_name);
    let _ =
        harness.client.invoke_ez(internal_request).await.expect("INTERNAL invoke should succeed");

    // Check counts after INTERNAL call
    assert_eq!(
        harness.mock_junction.call_count.load(Ordering::SeqCst),
        1,
        "INTERNAL call: Junction should be called once."
    );
    assert_eq!(
        harness.mock_proxy.call_count.load(Ordering::SeqCst),
        0,
        "INTERNAL call: Proxy should NOT be called."
    );
    assert_eq!(
        harness.mock_ez_to_ez.call_count.load(Ordering::SeqCst),
        0,
        "INTERNAL call: Remote handler should NOT be called."
    );

    // Test EXTERNAL Routing (Proxy)
    let external_request =
        create_test_request(&external_info.operator_domain, &external_info.service_name);
    let _ =
        harness.client.invoke_ez(external_request).await.expect("EXTERNAL invoke should succeed");

    // Check counts after EXTERNAL call
    assert_eq!(
        harness.mock_junction.call_count.load(Ordering::SeqCst),
        1,
        "EXTERNAL call: Junction call count unchanged."
    );
    assert_eq!(
        harness.mock_proxy.call_count.load(Ordering::SeqCst),
        1,
        "EXTERNAL call: Proxy should be called once."
    );
    assert_eq!(
        harness.mock_ez_to_ez.call_count.load(Ordering::SeqCst),
        0,
        "EXTERNAL call: Remote handler call count unchanged."
    );

    // Test REMOTE Routing (Ez-to-Ez Handler)
    let remote_request =
        create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, &remote_info.service_name);
    let _ = harness.client.invoke_ez(remote_request).await.expect("REMOTE invoke should succeed");

    // Check counts after REMOTE call
    assert_eq!(
        harness.mock_junction.call_count.load(Ordering::SeqCst),
        1,
        "REMOTE call: Junction call count unchanged."
    );
    assert_eq!(
        harness.mock_proxy.call_count.load(Ordering::SeqCst),
        1,
        "REMOTE call: Proxy call count unchanged."
    );
    assert_eq!(
        harness.mock_ez_to_ez.call_count.load(Ordering::SeqCst),
        1,
        "REMOTE call: Remote handler should be called once."
    );
}

/// Spawns test Bridge Server to talk with external connector
async fn spawn_test_server(service: IsolateEzBridgeService) -> IsolateEzBridgeClient<Channel> {
    let (client_io, server_io) = tokio::io::duplex(1024);

    tokio::spawn(async move {
        Server::builder()
            .add_service(IsolateEzBridgeServer::new(service))
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await
    });

    let mut client_conn = Some(client_io);
    Endpoint::try_from("http://in-memory.test")
        .expect("Endpoint should be valid")
        .connect_with_connector(service_fn(move |_| {
            let client = client_conn.take();
            async move {
                if let Some(client) = client {
                    Ok(TokioIo::new(client))
                } else {
                    Err(std::io::Error::other("Client already taken"))
                }
            }
        }))
        .await
        .map(IsolateEzBridgeClient::new)
        .expect("Should connect client")
}

/// Creates test request for Isolates.
fn create_test_request(domain: &str, name: &str) -> InvokeEzRequest {
    InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            destination_operator_domain: domain.to_string(),
            destination_service_name: name.to_string(),
            ..Default::default()
        }),
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Unspecified.into(),
                ..Default::default()
            }],
        }),
        isolate_request_payload: Some(EzPayloadData {
            datagrams: vec!["hello world".as_bytes().to_vec()],
        }),
    }
}

/// Creates test request for Isolates with a specific scope.
fn create_test_request_with_scope(
    domain: &str,
    name: &str,
    scope: DataScopeType,
) -> InvokeEzRequest {
    let mut request = create_test_request(domain, name);
    request.isolate_request_iscope = Some(EzPayloadIsolateScope {
        datagram_iscopes: vec![IsolateDataScope { scope_type: scope.into(), ..Default::default() }],
    });
    request
}

async fn setup_notify_state_stream(
    client: &mut IsolateEzBridgeClient<Channel>,
) -> Result<(Sender<NotifyIsolateStateRequest>, Streaming<NotifyIsolateStateResponse>)> {
    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    let inbound = client
        .notify_isolate_state(ReceiverStream::new(rx))
        .await
        .context("Failed to get inbound channel")?;
    let response_stream = inbound.into_inner();
    Ok((tx, response_stream))
}

async fn add_backend_dependencies(
    isolate_id: IsolateId,
    isolate_service_mapper: IsolateServiceMapper,
    manifest_validator: ManifestValidator,
    isolate_service_info: &IsolateServiceInfo,
    is_internal: bool,
    route_type: RouteType,
) -> Result<()> {
    if is_internal {
        isolate_service_mapper
            .new_binary_index(vec![isolate_service_info.clone()], false)
            .await
            .context("Should be a valid binary services index")?;
    }

    let dependency_index = isolate_service_mapper
        .add_backend_dependency_service(isolate_service_info, route_type)
        .await
        .context("Should be able to add external isolate service")?;
    manifest_validator
        .add_backend_dependencies(AddBackendDependenciesRequest {
            binary_services_index: isolate_id.get_binary_services_index(),
            dependency_index,
        })
        .await
        .context("Should be able to add backend dependency in manifest")?;
    Ok(())
}

async fn add_to_data_scope_requester(
    data_scope_requester: DataScopeRequester,
    isolate_id: IsolateId,
) -> Result<()> {
    data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id,
        })
        .await?;
    Ok(())
}

fn get_sample_invoke_ez_response() -> InvokeEzResponse {
    InvokeEzResponse {
        status: Some(IsolateStatus { code: 0, ..Default::default() }),
        ez_response_payload: Some(EzPayloadData {
            datagrams: vec!["hello world".as_bytes().to_vec()],
        }),
        ez_response_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                mapped_scope_owner: None,
            }],
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_unary_routes_via_interceptor() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();

    // The backend service we want to route to (interceptor)
    let interceptor_info = IsolateServiceInfo {
        operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
    };

    harness
        .mapper
        .new_binary_index(vec![interceptor_info.clone()], true)
        .await
        .expect("Failed to index ratified service");

    harness
        .manifest_validator
        .add_backend_dependencies(AddBackendDependenciesRequest {
            binary_services_index: harness.isolate_id.get_binary_services_index(),
            dependency_index: harness
                .mapper
                .get_service_index(&interceptor_info)
                .await
                .expect("Failed to get service index"),
        })
        .await
        .expect("Should be able to add backend dependency in manifest");

    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    // The fake target the request will try to hit initially
    let target_info = IsolateServiceInfo {
        operator_domain: TEST_SOME_DOMAIN.to_string(),
        service_name: TEST_SOME_SERVICE_NAME.to_string(),
    };
    harness
        .mapper
        .new_binary_index(vec![target_info.clone()], false)
        .await
        .expect("Failed to index opaque service");

    let intercepting_services = manifest_proto::enforcer::InterceptingServices {
        intercepting_operator_domain: target_info.operator_domain.clone(),
        intercepting_service_name: target_info.service_name.clone(),
        interceptor_operator_domain: interceptor_info.operator_domain.clone(),
        interceptor_service_name: interceptor_info.service_name.clone(),
        interceptor_method_for_unary: "interceptor_unary".to_string(),
        ..Default::default()
    };
    harness.interceptor.add_interceptor(intercepting_services).await.unwrap();

    let request = create_test_request(&target_info.operator_domain, &target_info.service_name);
    let _response = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
    let received_request =
        harness.mock_junction.invoked_isolate_requests.lock().unwrap().last().unwrap().clone();
    assert_eq!(
        received_request.control_plane_metadata.unwrap().destination_method_name,
        "interceptor_unary",
        "Request should be rerouted to interceptor's method"
    );
}

#[tokio::test]
async fn unary_external_timeout_expires() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let domain = format!("{}some.service", EXTERNAL_PREFIX);
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: domain.clone(),
            service_name: TEST_SERVICE_NAME.to_string(),
        },
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    *harness.mock_proxy.delay.lock().unwrap() = Duration::from_millis(100);

    let request = create_test_request(&domain, TEST_SERVICE_NAME);
    let mut tonic_request = tonic::Request::new(request);
    tonic_request.metadata_mut().insert("grpc-timeout", "10000u".parse().unwrap());

    let response = harness.client.invoke_ez(tonic_request).await;
    assert!(response.is_err());
    assert!(response.unwrap_err().to_string().contains("Mock proxy timeout"));
}

#[tokio::test]
async fn unary_external_timeout_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let domain = format!("{}some.service", EXTERNAL_PREFIX);
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: domain.clone(),
            service_name: TEST_SERVICE_NAME.to_string(),
        },
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    *harness.mock_proxy.delay.lock().unwrap() = Duration::from_millis(50);

    let request = create_test_request(&domain, TEST_SERVICE_NAME);
    let mut tonic_request = tonic::Request::new(request);
    tonic_request.metadata_mut().insert("grpc-timeout", "1S".parse().unwrap());

    let response = harness.client.invoke_ez(tonic_request).await;
    assert!(response.is_ok());
}

#[tokio::test]
async fn unary_remote_timeout_expires() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
            service_name: TEST_SERVICE_NAME.to_string(),
        },
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    *harness.mock_ez_to_ez.delay.lock().unwrap() = Duration::from_millis(100);

    let request = create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    let mut tonic_request = tonic::Request::new(request);
    tonic_request.metadata_mut().insert("grpc-timeout", "10000u".parse().unwrap());

    let response = harness.client.invoke_ez(tonic_request).await;
    assert!(response.is_err());
    let err_msg = response.unwrap_err().to_string();
    assert!(err_msg.contains("Error from remote enforcer: Mock remote_invoke timeout"));
}

#[tokio::test]
async fn unary_remote_timeout_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
            service_name: TEST_SERVICE_NAME.to_string(),
        },
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    *harness.mock_ez_to_ez.delay.lock().unwrap() = Duration::from_millis(50);

    let request = create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    let mut tonic_request = tonic::Request::new(request);
    tonic_request.metadata_mut().insert("grpc-timeout", "1S".parse().unwrap());

    let response = harness.client.invoke_ez(tonic_request).await;
    assert!(response.is_ok());
}
