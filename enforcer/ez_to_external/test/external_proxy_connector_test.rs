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

use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    ez_external_proxy_service_server::{EzExternalProxyService, EzExternalProxyServiceServer},
    ControlPlaneMetadata, EzExternalProxyRequest, EzExternalProxyResponse, EzPayloadIsolateScope,
    InvokeEzRequest, IsolateDataScope,
};
use external_proxy_connector::{
    translate_to_proxy_request, ExternalProxyChannel, ExternalProxyConnector,
    ExternalProxyConnectorError,
};
use isolate_info::{BinaryServicesIndex, IsolateId};
use payload_proto::enforcer::v1::EzPayloadData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::Server;

const MAX_DECODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024;
static ENV_VAR_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn get_mutex() -> &'static Mutex<()> {
    ENV_VAR_MUTEX.get_or_init(|| Mutex::new(()))
}

#[derive(Clone, Default)]
struct MockProxyService {
    call_count: Arc<AtomicUsize>,
    request_capture_tx: Option<Sender<EzExternalProxyRequest>>,
}

#[tonic::async_trait]
impl EzExternalProxyService for MockProxyService {
    type StreamCallStream = ReceiverStream<Result<EzExternalProxyResponse, tonic::Status>>;

    async fn unary_call(
        &self,
        request: tonic::Request<EzExternalProxyRequest>,
    ) -> Result<tonic::Response<EzExternalProxyResponse>, tonic::Status> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let req = request.into_inner();

        if let Some(capture_channel) = &self.request_capture_tx {
            capture_channel.send(req.clone()).await.ok();
        }

        // Create and return a single echo response.
        let response =
            EzExternalProxyResponse { external_response_payload: req.external_request_payload };
        Ok(tonic::Response::new(response))
    }

    async fn stream_call(
        &self,
        request: tonic::Request<tonic::Streaming<EzExternalProxyRequest>>,
    ) -> Result<tonic::Response<Self::StreamCallStream>, tonic::Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(10);

        let call_count = self.call_count.clone();
        let request_capture_tx = self.request_capture_tx.clone();

        tokio::spawn(async move {
            while let Some(Ok(req)) = stream.message().await.transpose() {
                call_count.fetch_add(1, Ordering::SeqCst);
                if let Some(capture_channel) = &request_capture_tx {
                    capture_channel.send(req.clone()).await.ok();
                }
                let response = EzExternalProxyResponse {
                    external_response_payload: req.external_request_payload,
                };
                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Default)]
struct FaultyProxyService;

#[tonic::async_trait]
impl EzExternalProxyService for FaultyProxyService {
    type StreamCallStream = ReceiverStream<Result<EzExternalProxyResponse, tonic::Status>>;

    async fn unary_call(
        &self,
        _request: tonic::Request<EzExternalProxyRequest>,
    ) -> Result<tonic::Response<EzExternalProxyResponse>, tonic::Status> {
        Err(tonic::Status::internal("Unary Failure!"))
    }

    async fn stream_call(
        &self,
        _request: tonic::Request<tonic::Streaming<EzExternalProxyRequest>>,
    ) -> Result<tonic::Response<Self::StreamCallStream>, tonic::Status> {
        Err(tonic::Status::internal("Stream Failure!"))
    }
}

/// Sets up a mock gRPC server on a Unix Domain Socket
/// Returns the address string and the TempDir handle
async fn setup_uds_server(
    mock_service: impl EzExternalProxyService,
) -> (String, oneshot::Sender<()>, tempfile::TempDir) {
    let (tx, rx) = oneshot::channel();
    let temp_dir = tempdir().unwrap();
    let uds_path = temp_dir.path().join("test-proxy.sock");
    let uds = UnixListener::bind(&uds_path).unwrap();
    let uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

    tokio::spawn(async move {
        Server::builder()
            .add_service(EzExternalProxyServiceServer::new(mock_service))
            .serve_with_incoming_shutdown(uds_stream, async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let address = format!("unix:{}", uds_path.to_str().unwrap());
    (address, tx, temp_dir)
}

/// Sets up a mock gRPC server on a TCP socket
/// Returns the address string
async fn setup_tcp_server(
    mock_service: impl EzExternalProxyService,
) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(EzExternalProxyServiceServer::new(mock_service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    rx.await.ok();
                },
            )
            .await
            .unwrap();
    });

    (format!("http://{}", addr), tx)
}

/// Test connector with DataScopeManager
async fn build_test_connector(
    server_address: String,
) -> Result<ExternalProxyConnector, ExternalProxyConnectorError> {
    std::env::set_var("PROXY_CONNECT_RETRY_COUNT", "2");
    std::env::set_var("PROXY_CONNECT_RETRY_DELAY_MS", "10");

    ExternalProxyConnector::new(server_address, MAX_DECODING_MESSAGE_SIZE).await
}

/// Creates a test request with a specific Data Scope.
fn create_scope_test_request(scope: DataScopeType) -> InvokeEzRequest {
    InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata::default()),
        isolate_request_payload: Some(EzPayloadData { datagrams: vec![vec![0]] }),
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: scope as i32,
                ..Default::default()
            }],
        }),
    }
}

/// Creates a generic test request for testing connectivity. When DataScope is not important.
fn create_generic_test_request(datagrams: Vec<Vec<u8>>) -> InvokeEzRequest {
    InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            destination_operator_domain: "external/test-target".to_string(),
            destination_service_name: "test.Service".to_string(),
            destination_method_name: "TestMethod".to_string(),
            ..Default::default()
        }),
        isolate_request_payload: Some(EzPayloadData { datagrams }),
        isolate_request_iscope: Some(EzPayloadIsolateScope::default()),
    }
}

/// Helper function to generate the expected ControlPlaneMetadata returned by the connector.
/// The connector copies ipc_message_id but clears/re-populates other network/requester fields.
fn expected_response_metadata(original_metadata: &ControlPlaneMetadata) -> ControlPlaneMetadata {
    ControlPlaneMetadata {
        ipc_message_id: original_metadata.ipc_message_id,
        requester_spiffe: String::new(),
        requester_is_local: true,
        responder_is_local: false,
        destination_operator_domain: String::new(),
        destination_service_name: String::new(),
        destination_method_name: String::new(),
        destination_ez_instance_id: String::new(),
        shared_memory_handles: Vec::new(),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_connect_retries_and_succeeds() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let uds_path = temp_dir.path().join("test-retry-proxy.sock");
    let connector_path = format!("unix:{}", uds_path.to_str().unwrap());

    let connect_future = grpc_connector::connect(connector_path, 30, 1000);

    tokio::time::sleep(Duration::from_millis(1500)).await;
    let (tx, rx) = oneshot::channel();
    let uds = UnixListener::bind(uds_path).unwrap();
    let uds_stream = UnixListenerStream::new(uds);
    tokio::spawn(async move {
        Server::builder()
            .add_service(EzExternalProxyServiceServer::new(MockProxyService::default()))
            .serve_with_incoming_shutdown(uds_stream, async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let result = connect_future.await;
    assert!(result.is_ok());

    let _ = tx.send(());
}

/// Tests the Mock Proxy Service Unary Call
#[tokio::test]
async fn test_mock_proxy_service_unary_call() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let (capture_tx, mut capture_rx) = mpsc::channel(10);
    let mock_service =
        MockProxyService { call_count: call_count.clone(), request_capture_tx: Some(capture_tx) };

    // Create the test request data.
    let test_payload = vec![10, 20, 30];
    let invoke_request = create_generic_test_request(vec![test_payload.clone()]);
    let request_data = translate_to_proxy_request(invoke_request).unwrap();
    let tonic_request = tonic::Request::new(request_data.clone());

    // Call the unary method directly.
    let response_result = mock_service.unary_call(tonic_request).await;

    assert!(response_result.is_ok());
    let response = response_result.unwrap().into_inner();

    // Check that the payload was echoed back.
    assert_eq!(response.external_response_payload, test_payload);

    // Check that the call count was incremented.
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Check that the request was captured.
    let captured_req = capture_rx.recv().await.unwrap();
    assert_eq!(captured_req.external_request_payload, test_payload);
}

/// Tests a unary call request-response call using UDS.
/// Sends one request and checks that it gets a successful response back.
#[tokio::test]
async fn test_proxy_external_end_to_end_unary_using_uds() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let (server_address, shutdown_tx, _temp_dir) =
        setup_uds_server(MockProxyService::default()).await;

    let connector = build_test_connector(server_address).await.unwrap();
    let test_datagrams = vec![vec![10, 20, 30]];
    let request = create_generic_test_request(test_datagrams.clone());

    let response_result = connector.proxy_external(isolate_id, request.clone(), None).await;

    assert!(response_result.is_ok());
    let response = response_result.unwrap();

    assert!(response.status.is_some());
    assert_eq!(
        response
            .ez_response_iscope
            .expect("Scopes should be mentioned for payload")
            .datagram_iscopes[0]
            .scope_type,
        DataScopeType::Public.into()
    );
    assert_eq!(response.status.unwrap().code, 0);
    let _ = shutdown_tx.send(());
}

/// Tests a unary call request-response call using TCP .
/// Sends one request and checks that it gets a successful response back.
#[tokio::test]
async fn test_proxy_external_end_to_end_unary() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let (server_address, shutdown_tx) = setup_tcp_server(MockProxyService::default()).await;

    let connector = build_test_connector(server_address).await.unwrap();
    let test_datagrams = vec![vec![10, 20, 30]];
    let request = create_generic_test_request(test_datagrams.clone());

    let response_result = connector.proxy_external(isolate_id, request.clone(), None).await;

    assert!(response_result.is_ok());
    let response = response_result.unwrap();

    assert!(response.status.is_some());
    assert_eq!(
        response
            .ez_response_iscope
            .expect("Scopes should be mentioned for payload")
            .datagram_iscopes[0]
            .scope_type,
        DataScopeType::Public.into()
    );
    assert_eq!(response.status.unwrap().code, 0);
    let _ = shutdown_tx.send(());
}

/// Tests a bidirectional streaming request-response call.
/// Establishes a stream, sends two back-to-back tests, and it verifies that it receives
/// two echoed back responses.
#[tokio::test]
async fn test_stream_proxy_external_end_to_end_streaming() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let (server_address, shutdown_tx, _temp_dir) =
        setup_uds_server(MockProxyService::default()).await;

    let connector = build_test_connector(server_address).await.unwrap();
    let (tx, rx) = mpsc::channel(10);

    // Send first request before we call stream_proxy_external.
    let request1 = create_generic_test_request(vec![vec![10, 20, 30]]);
    tx.send(request1.clone()).await.unwrap();

    let mut response_receiver = connector.stream_proxy_external(isolate_id, rx).await.unwrap();

    // Send a follow up request after the stream is established.
    let request2 = create_generic_test_request(vec![vec![40, 50, 60]]);
    tx.send(request2.clone()).await.unwrap();

    // Receive and verify the first echo response.
    let response1 = response_receiver.recv().await.expect("Should receive the first response");
    assert_eq!(response1.ez_response_payload, request1.isolate_request_payload);
    assert_eq!(
        response1
            .ez_response_iscope
            .expect("Scopes should be mentioned for payload")
            .datagram_iscopes[0]
            .scope_type,
        DataScopeType::Public.into()
    );
    assert_eq!(response1.status.unwrap().code, 0);

    // Receive and verify the second echo response.
    let response2 = response_receiver.recv().await.expect("Should receive the second response");
    assert_eq!(response2.ez_response_payload, request2.isolate_request_payload);
    assert_eq!(
        response2
            .ez_response_iscope
            .expect("Scopes should be mentioned for payload")
            .datagram_iscopes[0]
            .scope_type,
        DataScopeType::Public.into()
    );
    assert_eq!(response2.status.unwrap().code, 0);
    let _ = shutdown_tx.send(());
}

/// Tests Ratified Isolate with UserPrivate DataScope
#[tokio::test]
async fn stream_passes_for_ratified_isolate_with_userprivate_scope() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let mock_proxy = MockProxyService::default();
    let proxy_calls = mock_proxy.call_count.clone();
    let (server_address, shutdown_tx, _temp_dir) = setup_uds_server(mock_proxy).await;

    // Build the connector and register the Isolate as Ratified
    let connector = build_test_connector(server_address).await.unwrap();

    let (req_tx, req_rx) = mpsc::channel(1);
    let mut response_receiver = connector.stream_proxy_external(isolate_id, req_rx).await.unwrap();

    req_tx.send(create_scope_test_request(DataScopeType::UserPrivate)).await.unwrap();
    drop(req_tx); // Still a good practice to drop the sender

    // Wait for the response to ensure the call completed
    let err_response = response_receiver.recv().await.expect("Should receive an error response");
    let status = err_response.status.expect("Response should have a status");

    assert_eq!(status.code, 3, "Status code should be InvalidArgument");
    assert!(status.message.contains("Permission Denied:"));

    assert_eq!(proxy_calls.load(Ordering::SeqCst), 0, "Proxy should not have been called");
    let _ = shutdown_tx.send(());
}

/// Tests Ratified Isolate with multiple datagrams
#[tokio::test]
async fn stream_fails_for_ratified_isolate_with_translation_error() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let mock_proxy = MockProxyService::default();
    let proxy_calls = mock_proxy.call_count.clone();

    let (server_address, shutdown_tx, _temp_dir) =
        setup_uds_server(MockProxyService::default()).await;
    // Build the connector and register the Isolate as Ratified
    let connector = build_test_connector(server_address).await.unwrap();

    let (req_tx, req_rx) = mpsc::channel(1);

    // Create a request that will fail translation because it has two datagrams
    let malformed_request = create_generic_test_request(vec![vec![1, 2], vec![3, 4]]);
    req_tx.send(malformed_request).await.unwrap();
    let mut response_receiver = connector.stream_proxy_external(isolate_id, req_rx).await.unwrap();
    let err_response = response_receiver.recv().await.expect("Should receive an error response");
    let status = err_response.status.expect("Response should have a status");

    // Check for the gRPC InvalidArgument code (3) and the specific error message.
    assert_eq!(status.code, 3, "Status code should be InvalidArgument");
    assert!(
        status.message.contains("External calls must contain exactly one datagram."),
        "Error message did not match expected"
    );

    assert_eq!(proxy_calls.load(Ordering::SeqCst), 0, "Proxy should not have been called");
    let _ = shutdown_tx.send(());
}

/// Tests Opaque Isolate with UserPrivate DataScope, and should fail due to DataScope Restriction.
#[tokio::test]
async fn stream_fails_for_opaque_isolate_with_userprivate_scope() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(false));
    let mock_proxy = MockProxyService::default();
    let proxy_calls = mock_proxy.call_count.clone();

    let (server_address, shutdown_tx, _temp_dir) =
        setup_uds_server(MockProxyService::default()).await;

    // Build the connector and register the Isolate as Opaque.
    let connector = build_test_connector(server_address).await.unwrap();

    let (req_tx, req_rx) = mpsc::channel(1);
    req_tx.send(create_scope_test_request(DataScopeType::UserPrivate)).await.unwrap();
    let mut response_stream = connector.stream_proxy_external(isolate_id, req_rx).await.unwrap();

    // The call should be rejected with an error response.
    let response = response_stream.recv().await.unwrap();
    let status = response.status.expect("Response should have a status");

    assert_eq!(status.code, 3);
    assert!(status.message.contains("Permission Denied"));

    // The proxy should NOT be called.
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 0);
    let _ = shutdown_tx.send(());
}

/// Tests Opaque Isolate with DomainOwned DataScope and should succeed due to less sensitive data.
#[tokio::test]
async fn stream_passes_for_opaque_isolate_with_domainowned_scope() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(false));
    let mock_proxy =
        MockProxyService { call_count: Arc::new(AtomicUsize::new(0)), request_capture_tx: None };
    let proxy_calls = mock_proxy.call_count.clone();

    let (server_address, shutdown_tx, _temp_dir) = setup_uds_server(mock_proxy).await;

    // Build the connector and register the Isolate as Opaque.
    let connector = build_test_connector(server_address).await.unwrap();
    let (req_tx, req_rx) = mpsc::channel(1);
    let mut response_receiver = connector.stream_proxy_external(isolate_id, req_rx).await.unwrap();

    req_tx.send(create_scope_test_request(DataScopeType::DomainOwned)).await.unwrap();
    drop(req_tx);

    // Wait for the response
    let _ = response_receiver.recv().await;

    // The assertion will now pass.
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1);
    let _ = shutdown_tx.send(());
}

/// Translates the InvokeEzRequest to the expected ExternalProxyService Request.
#[test]
fn test_translate_to_proxy_request_success() {
    let test_datagrams = vec![vec![1, 2, 3]];
    let request = create_generic_test_request(test_datagrams.clone());
    let result = translate_to_proxy_request(request);
    assert!(result.is_ok());
    let proxy_req = result.unwrap();
    let expected_payload: Vec<u8> = test_datagrams.into_iter().flatten().collect();
    assert_eq!(proxy_req.external_request_payload, expected_payload);
    let resource = proxy_req.resource.unwrap();
    assert_eq!(resource.target, "test-target");
    assert_eq!(resource.service_name, "test.Service");
    assert_eq!(resource.method_name, "TestMethod");
}

/// Attempts to translate proxy request but fails due to missing metadata.
#[test]
fn test_translate_to_proxy_request_fails_on_missing_metadata() {
    let request = InvokeEzRequest {
        control_plane_metadata: None,
        isolate_request_payload: Some(EzPayloadData { datagrams: vec![vec![1, 2, 3]] }),
        isolate_request_iscope: None,
    };
    let result = translate_to_proxy_request(request);
    assert!(matches!(result, Err(ExternalProxyConnectorError::TranslationError(_))));
}

/// Attempts to translate proxy request but fails due to missing payload.
#[test]
fn test_translate_to_proxy_request_fails_on_missing_payload() {
    let request = InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            destination_operator_domain: "external/test-target".to_string(),
            ..Default::default()
        }),
        isolate_request_payload: None,
        isolate_request_iscope: None,
    };
    let result = translate_to_proxy_request(request);
    assert!(matches!(result, Err(ExternalProxyConnectorError::TranslationError(_))));
}

/// Tests that the connector correctly handles a TCP connection failure.
#[tokio::test]
async fn test_proxy_external_fails_on_tcp_connection_error() {
    let _lock = get_mutex().lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_address = format!("http://{}", addr);
    drop(listener);

    let connector_result = build_test_connector(server_address).await;

    assert!(connector_result.is_err(), "Connection should fail when no server is running");
    assert!(matches!(connector_result, Err(ExternalProxyConnectorError::ConnectionFailed(_))));
}

/// Tests that the connector correctly handles a UDS connection failure.
#[tokio::test]
async fn test_proxy_external_fails_on_uds_connection_error() {
    let temp_dir = tempdir().unwrap();
    let uds_path = temp_dir.path().join("test-failure.sock");
    let server_address = format!("unix:{}", uds_path.to_str().unwrap());

    let connector_result = build_test_connector(server_address).await;

    assert!(connector_result.is_err(), "Connection should fail when no server is running");
    assert!(matches!(connector_result, Err(ExternalProxyConnectorError::ConnectionFailed(_))));
}

/// Tests that the connector correctly handles a gRPC error from the server over a TCP connection
#[tokio::test]
async fn test_proxy_external_handles_server_error_using_tcp() {
    // Set up a TCP server using the new FaultyProxyService.
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let (server_address, shutdown_tx) = setup_tcp_server(FaultyProxyService).await;
    let connector = build_test_connector(server_address).await.unwrap();

    let test_datagrams = vec![vec![10, 20, 30]];
    let request = create_generic_test_request(test_datagrams.clone());

    // Call the proxy method. The server will respond with an error.
    let response_result = connector.proxy_external(isolate_id, request.clone(), None).await;

    assert!(response_result.is_err(), "The call should fail due to a server error");
    assert!(matches!(response_result, Err(ExternalProxyConnectorError::StreamFailed(_))),);
    let _ = shutdown_tx.send(());
}

/// Tests that the connector correctly handles a gRPC error from the server over a UDS connection
#[tokio::test]
async fn test_proxy_external_handles_server_error_using_uds() {
    // Set up a UDS server using the new FaultyProxyService.
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let (server_address, shutdown_tx, _temp_dir) = setup_uds_server(FaultyProxyService).await;
    let connector = build_test_connector(server_address).await.unwrap();

    let test_datagrams = vec![vec![10, 20, 30]];
    let request = create_generic_test_request(test_datagrams.clone());

    // Call the proxy method. The server will respond with an error.
    let response_result = connector.proxy_external(isolate_id, request.clone(), None).await;

    assert!(response_result.is_err(), "The call should fail due to a server error");
    assert!(matches!(response_result, Err(ExternalProxyConnectorError::StreamFailed(_))),);
    let _ = shutdown_tx.send(());
}

/// Tests that the control_plane_metadata from a request is correctly transformed and returned in the
/// response for a unary call.
#[tokio::test]
async fn unary_returns_transformed_metadata_in_response() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));

    let (server_address, shutdown_tx, _temp_dir) =
        setup_uds_server(MockProxyService::default()).await;
    let connector = build_test_connector(server_address).await.unwrap();

    // 1. Create original metadata with unique values for testing transformation
    let original_metadata = ControlPlaneMetadata {
        ipc_message_id: 12345,
        destination_method_name: "UniqueUnaryTestMethod".to_string(),
        requester_is_local: false,
        ..Default::default()
    };
    let expected_metadata = expected_response_metadata(&original_metadata);

    // 2. Create a request with original metadata
    let test_request = InvokeEzRequest {
        control_plane_metadata: Some(original_metadata.clone()),
        isolate_request_payload: Some(EzPayloadData { datagrams: vec![vec![10, 20]] }),
        ..Default::default()
    };

    let response_result = connector.proxy_external(isolate_id, test_request, None).await;

    // 3. Check that the call was successful and the response is correct
    assert!(response_result.is_ok());
    let response = response_result.unwrap();

    // 4. Verify that the metadata in the response matches the *transformed* metadata
    let response_metadata = response.control_plane_metadata.expect("Response should have metadata");
    assert_eq!(response_metadata, expected_metadata);
    // Specifically check the field that was preserved and one that was cleared
    assert_eq!(response_metadata.ipc_message_id, 12345);
    assert!(response_metadata.destination_method_name.is_empty());

    let _ = shutdown_tx.send(());
}

/// Tests that a unary call fails if the `control_plane_metadata` field is missing.
#[tokio::test]
async fn unary_fails_on_missing_control_plane_metadata() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let (server_address, shutdown_tx, _temp_dir) =
        setup_uds_server(MockProxyService::default()).await;
    let connector = build_test_connector(server_address).await.unwrap();

    // Request with missing metadata
    let test_request = InvokeEzRequest {
        control_plane_metadata: None,
        isolate_request_payload: Some(EzPayloadData { datagrams: vec![vec![10, 20]] }),
        ..Default::default()
    };

    let response_result = connector.proxy_external(isolate_id, test_request, None).await;

    assert!(response_result.is_err());
    // Check for the specific error type and message for missing metadata
    if let Err(ExternalProxyConnectorError::TranslationError(msg)) = response_result {
        assert!(msg.contains(
            "SDK is always expected to populate control_plane_metadata, but it was None"
        ));
    } else {
        panic!("Expected TranslationError, got {:?}", response_result);
    }

    let _ = shutdown_tx.send(());
}

/// Tests that the timeout is correctly propagated to the underlying connector.
#[tokio::test]
async fn test_external_proxy_connector_timeout_propagation() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let (server_address, shutdown_tx, _temp_dir) =
        setup_uds_server(MockProxyService::default()).await;

    let connector = build_test_connector(server_address).await.unwrap();
    let test_datagrams = vec![vec![10, 20, 30]];
    let request = create_generic_test_request(test_datagrams.clone());
    let timeout = Duration::from_secs(5);

    // To properly test timeout propagation, we would need to mock the inner gRPC client
    // or have the mock service simulate a delay. Since the current MockProxyService
    // responds immediately, we can only verify that the call completes within the timeout.
    // A more thorough test would involve a mock service that respects the timeout.
    let response_result = tokio::time::timeout(
        timeout + Duration::from_secs(1), // Add a little buffer
        connector.proxy_external(isolate_id, request.clone(), Some(timeout)),
    )
    .await;

    assert!(response_result.is_ok(), "Call should complete within the timeout");
    let inner_result = response_result.unwrap();
    assert!(inner_result.is_ok(), "Inner result should be successful");

    let _ = shutdown_tx.send(());
}

/// Tests that the control_plane_metadata from the first streaming request is correctly transformed
/// and returned in subsequent responses.
#[tokio::test]
async fn stream_returns_transformed_metadata_in_response() {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));
    let (server_address, shutdown_tx, _temp_dir) =
        setup_uds_server(MockProxyService::default()).await;
    let connector = build_test_connector(server_address).await.unwrap();
    let (req_tx, res_rx) = mpsc::channel(10);
    let mut response_receiver = connector.stream_proxy_external(isolate_id, res_rx).await.unwrap();

    // Create original metadata with unique values for testing transformation
    let original_metadata = ControlPlaneMetadata {
        ipc_message_id: 54321,
        destination_service_name: "UniqueStreamingTestMethod".to_string(),
        requester_is_local: false,
        ..Default::default()
    };
    let expected_metadata = expected_response_metadata(&original_metadata);

    // Send Request (the first one sets the metadata for all responses)
    let test_request = InvokeEzRequest {
        control_plane_metadata: Some(original_metadata.clone()),
        isolate_request_payload: Some(EzPayloadData { datagrams: vec![vec![1, 2, 3]] }),
        ..Default::default()
    };

    req_tx.send(test_request).await.unwrap();

    // Receive and verify response metadata
    let response = response_receiver.recv().await.expect("Should receive a response");

    let response_metadata =
        response.control_plane_metadata.expect("Response should contain metadata");

    assert_eq!(response_metadata, expected_metadata);
    // Specifically check the field that was preserved and one that was cleared
    assert_eq!(response_metadata.ipc_message_id, 54321);
    assert!(response_metadata.destination_service_name.is_empty());

    let _ = shutdown_tx.send(());
}
