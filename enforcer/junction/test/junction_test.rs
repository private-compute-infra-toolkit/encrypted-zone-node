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
use container_manager_request::{ContainerManagerRequest, ResetIsolateResponse};
use container_manager_requester::ContainerManagerRequester;
use data_scope::error::DataScopeError;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::request::AddManifestScopeRequest;
use data_scope::{request::AddIsolateRequest, requester::DataScopeRequester};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::ez_isolate_bridge_server::{
    EzIsolateBridge, EzIsolateBridgeServer,
};
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, EzPayloadIsolateScope, InvokeIsolateRequest, InvokeIsolateResponse,
    IsolateDataScope, IsolateState,
};
use isolate_info::{IsolateId, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use isolate_test_utils::{
    create_echo_invoke_isolate_response, create_error_invoke_isolate_response,
    start_fake_isolate_server, ScopeDragInstruction, DEFAULT_ISOLATE_UNIX_SOCKET,
    ECHO_ISOLATE_METHOD_NAME, ECHO_ISOLATE_OPERATOR_DOMAIN, ECHO_ISOLATE_SERVICE_NAME,
    ERROR_ISOLATE_SERVICE_NAME,
};
use junction::IsolateJunction;
use junction_test_utils::JUNCTION_TEST_CHANNEL_SIZE;
use junction_trait::Junction;
use payload_proto::enforcer::v1::EzPayloadData;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::collections::{HashMap, HashSet};
use tokio::net::UnixListener;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const CLONE_ECHO_ISOLATE_SERVICE_NAME: &str = "clone_echo_isolate_service";

#[derive(Clone)]
struct MetadataCheckingIsolate {
    expected_header_key: String,
    expected_header_value: String,
}

#[tonic::async_trait]
impl EzIsolateBridge for MetadataCheckingIsolate {
    type StreamInvokeIsolateStream =
        tokio_stream::wrappers::ReceiverStream<Result<InvokeIsolateResponse, Status>>;

    async fn stream_invoke_isolate(
        &self,
        _request: Request<Streaming<InvokeIsolateRequest>>,
    ) -> Result<Response<Self::StreamInvokeIsolateStream>, Status> {
        unimplemented!()
    }

    async fn invoke_isolate(
        &self,
        request: Request<InvokeIsolateRequest>,
    ) -> Result<Response<InvokeIsolateResponse>, Status> {
        let metadata = request.metadata();
        if let Some(val) = metadata.get(&self.expected_header_key) {
            if val == self.expected_header_value.as_str() {
                let response = create_echo_invoke_isolate_response(request.into_inner());
                return Ok(Response::new(response));
            }
        }
        Err(Status::invalid_argument("Missing or incorrect header"))
    }

    async fn update_isolate_state(
        &self,
        _request: Request<enforcer_proto::enforcer::v1::UpdateIsolateStateRequest>,
    ) -> Result<Response<enforcer_proto::enforcer::v1::UpdateIsolateStateResponse>, Status> {
        unimplemented!()
    }
}

async fn start_metadata_checking_isolate_server(
    test_isolate_id: IsolateId,
    expected_key: String,
    expected_value: String,
) -> oneshot::Sender<()> {
    let fake_isolate = MetadataCheckingIsolate {
        expected_header_key: expected_key,
        expected_header_value: expected_value,
    };

    let unix_listener =
        UnixListener::bind(format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, test_isolate_id)).unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(EzIsolateBridgeServer::new(fake_isolate))
            .serve_with_incoming_shutdown(UnixListenerStream::new(unix_listener), async {
                shutdown_rx.await.ok();
            })
            .await
    });
    shutdown_tx
}

struct TestHarness {
    isolate_junction: IsolateJunction,
    isolate_service_info_map: HashMap<String, IsolateServiceInfo>,
    isolate_server_shutdown_tx: oneshot::Sender<()>,
    container_manager_request_rx: Option<Receiver<ContainerManagerRequest>>,
}

impl TestHarness {
    // Create a real IsolateJunction connected to a single fake Isolate (DefaultEchoIsolate)
    async fn new_with_arguments(
        retirement_threshold: u64,
        scope_drag_instruction: ScopeDragInstruction,
        is_ratified: bool,
        manifest_input_scope: DataScopeType,
        delay: Option<Duration>,
    ) -> Self {
        // Using real implementations of all IsolateJunction deps (except Isolate)
        let isolate_service_mapper = IsolateServiceMapper::default();
        let (container_manager_request_tx, container_manager_request_rx) =
            tokio::sync::mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
        let container_manager_requester =
            ContainerManagerRequester::new(container_manager_request_tx);
        let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone());
        let data_scope_requester = DataScopeRequester::new(retirement_threshold);
        let manifest_validator = ManifestValidator::default();
        let isolate_state_manager = IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        );
        let isolate_junction = IsolateJunction::new(
            data_scope_requester.clone(),
            isolate_service_mapper.clone(),
            shared_memory_manager.clone(),
            isolate_state_manager.clone(),
            manifest_validator.clone(),
        );

        // Start fake Isolate server
        let isolate_service_info = IsolateServiceInfo {
            operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
            service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
        };
        let clone_isolate_service_info = IsolateServiceInfo {
            operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
            service_name: CLONE_ECHO_ISOLATE_SERVICE_NAME.to_string(),
        };
        let error_isolate_service_info = IsolateServiceInfo {
            operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
            service_name: ERROR_ISOLATE_SERVICE_NAME.to_string(),
        };

        // Add fake Isolate to IsolateServiceMapper and save the mapped IsolateServiceIndex
        // This is usually done by ContainerManager when we parse ez manifest
        let isolate_services_vec = vec![
            isolate_service_info.clone(),
            clone_isolate_service_info.clone(),
            error_isolate_service_info.clone(),
        ];
        let binary_services_index = isolate_service_mapper
            .new_binary_index(isolate_services_vec, is_ratified)
            .await
            .expect("Should be a valid binary services index");

        manifest_validator
            .add_scope_info(AddManifestScopeRequest {
                binary_services_index,
                max_input_scope: manifest_input_scope,
                max_output_scope: DataScopeType::UserPrivate,
            })
            .await
            .expect("Should succeed to add input output scopes in manifest validator");

        let isolate_id = IsolateId::new(binary_services_index);
        let isolate_server_shutdown_tx =
            start_fake_isolate_server(isolate_id, scope_drag_instruction, delay).await;

        // Add fake Isolate to DataScopeRequester
        // This is usually done by ContainerManager calling IsolateStateManager
        let add_isolate_request = create_add_isolate_request(isolate_id);
        isolate_state_manager.add_isolate(add_isolate_request).await;
        // The Isolate must be ready before it can be used.
        isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

        // Connect fake Isolate to IsolateJunction
        assert!(isolate_junction
            .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id),)
            .await
            .is_ok());

        let mut isolate_service_info_map = HashMap::new();
        isolate_service_info_map
            .insert(ECHO_ISOLATE_SERVICE_NAME.to_string(), isolate_service_info);
        isolate_service_info_map
            .insert(CLONE_ECHO_ISOLATE_SERVICE_NAME.to_string(), clone_isolate_service_info);
        isolate_service_info_map
            .insert(ERROR_ISOLATE_SERVICE_NAME.to_string(), error_isolate_service_info);

        Self {
            isolate_junction,
            isolate_service_info_map,
            isolate_server_shutdown_tx,
            container_manager_request_rx: Some(container_manager_request_rx),
        }
    }

    async fn new() -> Self {
        TestHarness::new_with_arguments(
            u64::MAX,
            ScopeDragInstruction::KeepUnspecified,
            false,
            DataScopeType::UserPrivate,
            None,
        )
        .await
    }
}

#[tokio::test]
async fn test_metadata_propagation_to_isolate() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let (container_manager_request_tx, _container_manager_request_rx) =
        tokio::sync::mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_request_tx);
    let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone());
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let manifest_validator = ManifestValidator::default();
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let isolate_junction = IsolateJunction::new(
        data_scope_requester.clone(),
        isolate_service_mapper.clone(),
        shared_memory_manager.clone(),
        isolate_state_manager.clone(),
        manifest_validator.clone(),
    );

    let isolate_service_info = IsolateServiceInfo {
        operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
        service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
    };

    let binary_services_index = isolate_service_mapper
        .new_binary_index(vec![isolate_service_info.clone()], false)
        .await
        .expect("Should be a valid binary services index");

    manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index,
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope: DataScopeType::UserPrivate,
        })
        .await
        .expect("Should succeed to add input output scopes in manifest validator");

    let isolate_id = IsolateId::new(binary_services_index);

    // Start our custom isolate server
    let header_key = "x-custom-header";
    let header_value = "custom-value";
    let isolate_server_shutdown_tx = start_metadata_checking_isolate_server(
        isolate_id,
        header_key.to_string(),
        header_value.to_string(),
    )
    .await;

    let add_isolate_request = create_add_isolate_request(isolate_id);
    isolate_state_manager.add_isolate(add_isolate_request).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

    assert!(isolate_junction
        .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id),)
        .await
        .is_ok());

    let mut invoke_isolate_request = create_random_request(&isolate_service_info);

    // Add the header to metadata
    let mut metadata_headers = HashMap::new();
    metadata_headers.insert(header_key.to_string(), header_value.to_string());
    if let Some(cp_metadata) = invoke_isolate_request.control_plane_metadata.as_mut() {
        cp_metadata.metadata_headers = metadata_headers;
    }

    let invoke_result =
        isolate_junction.invoke_isolate(None, invoke_isolate_request, false, None).await;

    let _ = isolate_server_shutdown_tx.send(());

    assert!(invoke_result.is_ok(), "Invoke isolate should succeed if header matches");
}

#[tokio::test]
async fn test_junction_unary_flow() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let invoke_isolate_response = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), false, None)
        .await
        .unwrap();

    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    assert_eq!(invoke_isolate_response, expected_invoke_isolate_response)
}

#[tokio::test]
async fn test_junction_application_error_unary_flow() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ERROR_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let invoke_isolate_response = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), false, None)
        .await
        .unwrap();

    let expected_invoke_isolate_response =
        create_error_invoke_isolate_response(invoke_isolate_request);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    assert_eq!(invoke_isolate_response, expected_invoke_isolate_response)
}

#[tokio::test]
async fn test_junction_unary_flow_manifest_validation_failure() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create an InvokeIsolateRequest with a scope that is not allowed by the manifest.
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    // The manifest in TestHarness allows up to UserPrivate. Sealed is stricter.
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::Sealed.into();

    let invoke_result = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), false, None)
        .await;

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    let err = invoke_result.unwrap_err();
    assert_eq!(err.to_string(), DataScopeError::DisallowedByManifest.to_string());
}

#[tokio::test]
async fn test_validate_streaming_request_scope_failure() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());

    // Send InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive first expected InvokeIsolateResponse
    let invoke_isolate_response_result =
        client_junction_channels.junction_to_client.recv().await.unwrap();

    assert_eq!(invoke_isolate_response_result.unwrap(), expected_invoke_isolate_response);

    // for subsequent requests don't populate Isolate targeting fields
    let empty_isolate_info =
        IsolateServiceInfo { operator_domain: "".to_string(), service_name: "".to_string() };

    let mut invoke_isolate_request = create_random_request(&empty_isolate_info);
    // Change DataScopeType to Sealed which is more private than strictest (UserPrivate)
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::Sealed.into();
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let validation_error =
        client_junction_channels.junction_to_client.recv().await.unwrap().unwrap_err();
    assert_eq!(validation_error.to_string(), DataScopeError::DisallowedByManifest.to_string());

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_streaming_subsequent_request_manifest_validation_failure() {
    // Create IsolateJunction w/ fake echo Isolate.
    // DSM will have maximum scope as Userprivate because max-output scope = USER_PRIVATE and
    // max permissible scope is max(max-input scope, max-output scope)
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::DomainOwned,
        None,
    )
    .await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;

    // Send a valid initial request to establish the stream.
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    // Change DataScopeType to Public so that first request can go through
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::Public.into();
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive the successful response for the first request.
    assert!(client_junction_channels.junction_to_client.recv().await.unwrap().is_ok());

    let empty_isolate_info =
        IsolateServiceInfo { operator_domain: "".to_string(), service_name: "".to_string() };

    // This request should fail because even though Validate Isolate can scope-drag to USER_PRIVATE scope
    // manifest validation will block this request.
    let invoke_isolate_request = create_random_request(&empty_isolate_info);
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let validation_error =
        client_junction_channels.junction_to_client.recv().await.unwrap().unwrap_err();
    assert_eq!(validation_error.to_string(), DataScopeError::DisallowedByManifest.to_string());

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_validate_streaming_request_scope_success() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let first_ipc_message_id =
        invoke_isolate_request.control_plane_metadata.as_ref().unwrap().ipc_message_id;
    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());

    // Send InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive first expected InvokeIsolateResponse
    let invoke_isolate_response_result =
        client_junction_channels.junction_to_client.recv().await.unwrap();

    assert_eq!(invoke_isolate_response_result.unwrap(), expected_invoke_isolate_response);

    // for subsequent requests don't populate Isolate targeting fields
    let empty_isolate_info =
        IsolateServiceInfo { operator_domain: "".to_string(), service_name: "".to_string() };

    let invoke_isolate_request = create_random_request(&empty_isolate_info);
    let mut expected_invoke_isolate_response_2 =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());
    // The junction reuses the ipc_message_id from the initial request for all streaming responses.
    expected_invoke_isolate_response_2.control_plane_metadata.as_mut().unwrap().ipc_message_id =
        first_ipc_message_id;
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let invoke_isolate_response =
        client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();

    assert_eq!(expected_invoke_isolate_response_2, invoke_isolate_response);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

// TODO: Support scope retrieval for retiring Isolates.
#[tokio::test]
#[ignore]
async fn test_isolate_retires_after_sensitive_session_and_resets_when_idle() {
    // Set a sensitive session threshold of 1.
    let mut test_harness = TestHarness::new_with_arguments(
        1,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a sensitive scope (UserPrivate).
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());

    // Create a listener for the reset request.
    let mut rx = test_harness.container_manager_request_rx.take().unwrap();
    let reset_listener = tokio::spawn(async move {
        if let Some(ContainerManagerRequest::ResetIsolateRequest { resp, .. }) = rx.recv().await {
            let _ = resp.send(Ok(ResetIsolateResponse {}));
            return true;
        }
        false
    });

    // Create junction client channels, send the sensitive request, which increments the counter.
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;
    assert!(client_junction_channels
        .client_to_junction
        .send(invoke_isolate_request.clone())
        .await
        .is_ok());

    // The first request should succeed. The response will trigger the state update to Retiring.
    let invoke_isolate_response_result =
        client_junction_channels.junction_to_client.recv().await.unwrap();
    assert_eq!(invoke_isolate_response_result.unwrap(), expected_invoke_isolate_response);

    // The Isolate is now Retiring with 1 in-flight request (the stream).
    // Close the stream, which will decrement the counter, transition the Isolate from
    // Retiring to Idle, and trigger the reset.
    drop(client_junction_channels);

    // Wait for the reset listener to confirm the reset request was received.
    assert!(reset_listener.await.unwrap(), "Container reset was not triggered");

    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let invoke_isolate_response_result_2 =
        client_junction_channels.junction_to_client.recv().await.unwrap();
    let err = invoke_isolate_response_result_2.unwrap_err();
    assert_eq!(err.to_string(), DataScopeError::NoMatchingIsolates.to_string());

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_junction_streaming_flow() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());

    // Send InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive first expected InvokeIsolateResponse
    let invoke_isolate_response_result =
        client_junction_channels.junction_to_client.recv().await.unwrap();

    assert_eq!(invoke_isolate_response_result.unwrap(), expected_invoke_isolate_response);

    // for subsequent requests don't populate Isolate targeting fields
    let empty_isolate_info =
        IsolateServiceInfo { operator_domain: "".to_string(), service_name: "".to_string() };

    let mut response_set = HashSet::new();
    // spawn 20 requests concurrently and save the random data in a vec
    for _ in 0..20 {
        let invoke_isolate_request = create_random_request(&empty_isolate_info);
        response_set
            .insert(invoke_isolate_request.clone().isolate_input.unwrap().datagrams[0].clone());
        let sender_clone = client_junction_channels.client_to_junction.clone();
        tokio::spawn(async move {
            assert!(sender_clone.send(invoke_isolate_request).await.is_ok());
        });
    }

    // assert that the random data in the response came from one of the requests
    // the rest of the response is empty since we didn't populate any routing fields
    for _ in 0..20 {
        let invoke_isolate_response =
            client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();
        assert!(
            response_set.contains(&invoke_isolate_response.isolate_output.unwrap().datagrams[0])
        );
    }

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_junction_application_error_streaming_flow() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ERROR_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let expected_invoke_isolate_response =
        create_error_invoke_isolate_response(invoke_isolate_request.clone());

    // Send InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive first expected InvokeIsolateResponse
    let invoke_isolate_response_result =
        client_junction_channels.junction_to_client.recv().await.unwrap();

    assert_eq!(invoke_isolate_response_result.unwrap(), expected_invoke_isolate_response);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_junction_supports_multiple_services() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let clone_invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(CLONE_ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let invoke_isolate_response = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), false, None)
        .await
        .unwrap();

    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request);

    assert_eq!(invoke_isolate_response, expected_invoke_isolate_response);

    let clone_invoke_isolate_response = test_harness
        .isolate_junction
        .invoke_isolate(None, clone_invoke_isolate_request.clone(), false, None)
        .await
        .unwrap();

    let expected_clone_invoke_isolate_response =
        create_echo_invoke_isolate_response(clone_invoke_isolate_request);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    assert_eq!(clone_invoke_isolate_response, expected_clone_invoke_isolate_response);
}

// TODO: Support scope retrieval for retiring Isolates.
#[tokio::test]
#[ignore]
async fn test_junction_unary_flow_inflight_counter() {
    // Create IsolateJunction w/ fake echo Isolate
    let mut test_harness = TestHarness::new_with_arguments(
        1,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Listen for the reset request from IsolateStateManager
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut cm_rx = test_harness.container_manager_request_rx.take().unwrap();
    let listener = tokio::spawn(async move {
        if let Some(ContainerManagerRequest::ResetIsolateRequest { resp, .. }) = cm_rx.recv().await
        {
            let _ = resp.send(Ok(ResetIsolateResponse {}));
            tx.send(()).await.unwrap();
        }
    });

    // Send InvokeIsolateRequest to IsolateJunction, which should increment and then decrement the counter
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    // This request should trigger retiring of the Isolate
    let _ = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, None)
        .await
        .expect("Invoke Isolate should succeed");

    // The counter should now be 0, triggering a state change to Idle and a reset request
    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Reset request was not sent after the in-flight request completed");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
    listener.await.unwrap();
}

// TODO: Support scope retrieval for retiring Isolates.
#[tokio::test]
#[ignore]
async fn test_junction_streaming_flow_inflight_counter() {
    // Create IsolateJunction w/ fake echo Isolate
    let mut test_harness = TestHarness::new_with_arguments(
        1,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Listen for the reset request from IsolateStateManager
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut cm_rx = test_harness.container_manager_request_rx.take().unwrap();
    let listener = tokio::spawn(async move {
        if let Some(ContainerManagerRequest::ResetIsolateRequest { resp, .. }) = cm_rx.recv().await
        {
            let _ = resp.send(Ok(ResetIsolateResponse {}));
            tx.send(()).await.unwrap();
        }
    });

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;

    // Send first InvokeIsolateRequest to IsolateJunction, incrementing counter to 1
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive the response. The counter is still 1 because the stream is open.
    client_junction_channels
        .junction_to_client
        .recv()
        .await
        .expect("Should receive valid response")
        .expect("Should be a valid InvokeIsolateResponse");

    // The reset should not have happened yet.
    assert!(
        timeout(Duration::from_millis(100), rx.recv()).await.is_err(),
        "Reset request was sent prematurely before stream was closed"
    );

    // Close the client side of the stream, which will trigger teardown and decrement the counter.
    drop(client_junction_channels);

    // The counter should now be 0, triggering a state change to Idle and a reset request.
    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Reset request was not sent after the stream was closed");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
    listener.await.unwrap();
}

#[tokio::test]
async fn test_unary_scope_enforcement_failure_public_api() {
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepSame,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a UserPrivate scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Make the call from the "public API". The junction should reject a response with a UserPrivate scope.
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, true, None)
        .await
        .expect_err("Unary request should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}
#[tokio::test]
async fn test_unary_scope_enforcement_failure_public_api_sensitive_scope() {
    let test_harness = TestHarness::new().await;

    // Create a request with a UserPrivate scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Make the call from the "public API". The junction should reject a response with a UserPrivate scope.
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, true, None)
        .await
        .expect_err("Unary request should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unary_scope_enforcement_failure_ratified_isolate_public_api() {
    // Create a TestHarness with a Ratified Isolate.
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepSame,
        true,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a UserPrivate scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Make the call from the "public API". For a Ratified Isolate, this should fail
    // as public API egress checks are still enforced for sensitive scopes.
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), true, None)
        .await
        .expect_err(
            "Ratified isolate should not be allowed to egress sensitive scope via public API",
        );

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unary_scope_enforcement_failure_manifest_output_scope() {
    // Create a TestHarness where the fake Isolate will "drag" the scope up by one level.
    // The manifest is configured in TestHarness::new_with_retirement to allow
    // max_output_scope: DataScopeType::UserPrivate.
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::IncreaseByOne,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a UserPrivate scope.
    // The fake Isolate will attempt to respond with MultiUserPrivate (UserPrivate + 1).
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // The Isolate will try to respond with MultiUserPrivate, which is stricter than the
    // manifest's allowed max_output_scope (UserPrivate). This should be rejected.
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, None)
        .await
        .expect_err("Unary call shouldn't be allowed");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unary_scope_enforcement_failure_scope_downgrade() {
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::DecreaseByOne,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a DomainOwned scope.
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::DomainOwned.into();

    // The Isolate will respond with Public (DomainOwned - 1)
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, None)
        .await
        .expect_err("Unary call shouldn't be allowed");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_streaming_scope_enforcement_failure_public_api() {
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels from the "public API".
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, true).await;

    // Create a request with a UserPrivate scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send the request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // The junction should reject a response with a UserPrivate scope from the public API.
    client_junction_channels
        .junction_to_client
        .recv()
        .await
        .expect("Should receive error response")
        .expect_err("Streaming call should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_streaming_manifest_output_scope_failure_initial_request() {
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::IncreaseByOne,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create streaming junction client channels.
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;

    // Create a request with a UserPrivate scope.
    // The fake Isolate will attempt to respond with MultiUserPrivate (UserPrivate + 1).
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send the request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // The Isolate will try to respond with MultiUserPrivate, which is stricter than the
    // manifest's allowed max_output_scope (UserPrivate). This should be rejected.
    client_junction_channels
        .junction_to_client
        .recv()
        .await
        .expect("Should receive error response")
        .expect_err("Streaming call should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_enforcement_failure_scope_downgrade() {
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::DecreaseByOne,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create streaming junction client channels.
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;

    // Create a request with a UserPrivate scope.
    // The fake Isolate will attempt to respond with Public (DomainOwned + 1).
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::DomainOwned.into();

    // Send the request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // The Isolate will try to respond with DomainOwned, which is stricter than the
    // manifest's allowed max_output_scope (UserPrivate). This should be rejected.
    client_junction_channels
        .junction_to_client
        .recv()
        .await
        .expect("Should receive error response")
        .expect_err("Streaming call should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unary_unspecified_scope_is_overridden() {
    // Create a TestHarness where the fake Isolate will respond with an Unspecified scope.
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a UserPrivate scope. This will set the Isolate's current scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // The fake Isolate will respond with an Unspecified scope, but the junction should
    // override it with the Isolate's current scope (UserPrivate).
    let response = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, None)
        .await
        .expect("Unary call should be allowed");

    let output_scope = response.isolate_output_iscope.unwrap().datagram_iscopes[0].scope_type();
    assert_eq!(
        output_scope,
        DataScopeType::UserPrivate,
        "Expected output scope to be overridden to the isolate's current scope"
    );

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_streaming_unspecified_scope_is_overridden() {
    // Create a TestHarness where the fake Isolate will respond with an Unspecified scope.
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create streaming junction client channels.
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false).await;

    // Create a request with a UserPrivate scope. This will set the Isolate's current scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send the request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());
    // The fake Isolate will respond with Unspecified, but the junction should override it.
    let response = client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();
    let output_scope = response.isolate_output_iscope.unwrap().datagram_iscopes[0].scope_type();
    assert_eq!(output_scope, DataScopeType::UserPrivate);

    // Create a second request with a UserPrivate scope. This will set the Isolate's current scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send the second request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());
    // The fake Isolate will respond with Unspecified, but the junction should override it.
    let response = client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();
    let output_scope = response.isolate_output_iscope.unwrap().datagram_iscopes[0].scope_type();
    assert_eq!(output_scope, DataScopeType::UserPrivate);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

fn create_random_request(isolate_service_info: &IsolateServiceInfo) -> InvokeIsolateRequest {
    // Generate 8 random bytes for request
    let random_request_data =
        [0u8; 8].into_iter().map(|_| rand::random::<u8>()).collect::<Vec<_>>();
    let original_msg_id = rand::random();

    InvokeIsolateRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            ipc_message_id: original_msg_id,
            requester_spiffe: String::new(),
            requester_is_local: false,
            responder_is_local: true,
            destination_operator_domain: isolate_service_info.operator_domain.clone(),
            destination_service_name: isolate_service_info.service_name.clone(),
            destination_method_name: ECHO_ISOLATE_METHOD_NAME.to_string(),
            shared_memory_handles: Vec::new(),
            destination_ez_instance_id: "".to_string(),
            ..Default::default()
        }),
        isolate_input_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::UserPrivate.into(),
                mapped_scope_owner: None,
            }],
        }),
        isolate_input: Some(EzPayloadData { datagrams: vec![random_request_data.to_vec()] }),
    }
}

#[tokio::test]
async fn test_junction_unary_timeout() {
    // Create IsolateJunction w/ fake echo Isolate that has a delay
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepSame,
        false,
        DataScopeType::UserPrivate,
        Some(Duration::from_secs(5)),
    )
    .await;

    // Create InvokeIsolateRequest
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Invoke the isolate with a short timeout
    let invoke_result = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, Some(Duration::from_millis(100)))
        .await;

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    // Assert that the call timed out
    assert!(invoke_result.is_err());
    let err = invoke_result.unwrap_err();
    assert!(matches!(
        err,
        ez_error::EzError::Status(status) if status.code() == tonic::Code::Cancelled
    ));
}

#[tokio::test]
async fn test_junction_unary_timeout_internal_route() {
    // Create IsolateJunction w/ fake echo Isolate that has a delay
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepSame,
        false,
        DataScopeType::UserPrivate,
        Some(Duration::from_secs(5)),
    )
    .await;

    // Create InvokeIsolateRequest
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Mark the request as an internal route
    if let Some(cp_metadata) = invoke_isolate_request.control_plane_metadata.as_mut() {
        cp_metadata.requester_is_local = true;
    }

    // Invoke the isolate with a short timeout
    let invoke_result = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, Some(Duration::from_millis(100)))
        .await;

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    // Assert that the call timed out
    assert!(invoke_result.is_err());
    let err = invoke_result.unwrap_err();
    assert!(matches!(
        err,
        ez_error::EzError::Status(status) if status.code() == tonic::Code::Cancelled
    ));
}

fn create_add_isolate_request(isolate_id: IsolateId) -> AddIsolateRequest {
    AddIsolateRequest {
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::UserPrivate,
        isolate_id,
    }
}

// TODO: Add metric tests
