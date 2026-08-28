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
use data_scope_proto::enforcer::v1::{EzDataScope, EzStaticScopeInfo};
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, InvokeIsolateRequest, InvokeIsolateResponse,
};
use ez_to_ez_service_proto::enforcer::v1::ez_to_ez_api_client::EzToEzApiClient;
use ez_to_ez_service_proto::enforcer::v1::EzCallRequest;
use grpc_connector::{
    GrpcChannelPool, DEFAULT_CONNECT_RETRY_COUNT, DEFAULT_CONNECT_RETRY_DELAY_MS,
    DEFAULT_CONNECT_RETRY_SCALING, DEFAULT_POOL_SIZE,
};
use inbound_ez_to_ez_handler::InboundEzToEzHandler;
use isolate_test_utils::{TEST_ERROR_CODE, TEST_ERROR_SERVICE_NAME};
use junction_test_utils::FakeJunction;
use payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod;
use payload_proto::enforcer::v1::{EzHybridPayload, EzPayloadData, EzPayloadScope, ShmSlotData};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Request;

#[derive(Clone)]
struct ShmResponseIsolate;

#[tonic::async_trait]
impl isolate_test_utils::FakeIsolate for ShmResponseIsolate {
    async fn process_isolate_requests(
        &self,
        mut client_to_junction_rx: mpsc::Receiver<InvokeIsolateRequest>,
        junction_to_client_tx: mpsc::Sender<Result<InvokeIsolateResponse, ez_error::EzError>>,
        _stream_call_count: Arc<AtomicUsize>,
        _invoke_isolate_requests: Arc<Mutex<Vec<InvokeIsolateRequest>>>,
    ) {
        while client_to_junction_rx.recv().await.is_some() {
            let response = InvokeIsolateResponse {
                isolate_output: Some(EzHybridPayload {
                    delivery_method: Some(DeliveryMethod::ShmData(ShmSlotData::default())),
                }),
                ..Default::default()
            };
            let _ = junction_to_client_tx.send(Ok(response)).await;
        }
    }

    async fn create_isolate_response(
        &self,
        _request: &InvokeIsolateRequest,
    ) -> InvokeIsolateResponse {
        InvokeIsolateResponse {
            isolate_output: Some(EzHybridPayload {
                delivery_method: Some(DeliveryMethod::ShmData(ShmSlotData::default())),
            }),
            ..Default::default()
        }
    }
}

#[derive(Clone)]
struct DroppingIsolate;

#[tonic::async_trait]
impl isolate_test_utils::FakeIsolate for DroppingIsolate {
    async fn process_isolate_requests(
        &self,
        _client_to_junction_rx: mpsc::Receiver<InvokeIsolateRequest>,
        _junction_to_client_tx: mpsc::Sender<Result<InvokeIsolateResponse, ez_error::EzError>>,
        _stream_call_count: Arc<AtomicUsize>,
        _invoke_isolate_requests: Arc<Mutex<Vec<InvokeIsolateRequest>>>,
    ) {
        // Immediately exit so client_to_junction_rx is dropped
    }

    async fn create_isolate_response(
        &self,
        _request: &InvokeIsolateRequest,
    ) -> InvokeIsolateResponse {
        InvokeIsolateResponse::default()
    }
}

fn create_test_request(input_payload: &str) -> EzCallRequest {
    EzCallRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            destination_operator_domain: "test_domain".to_string(),
            destination_service_name: "test_service".to_string(),
            destination_method_name: "test_method".to_string(),
            ipc_message_id: 123,
            ..Default::default()
        }),
        payload_scope: Some(EzPayloadScope {
            datagram_scopes: vec![EzDataScope {
                static_info: Some(EzStaticScopeInfo {
                    scope_type: DataScopeType::Public.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        }),
        payload_data: Some(EzPayloadData { datagrams: vec![input_payload.as_bytes().to_vec()] }),
    }
}

#[tokio::test]
async fn test_inbound_unary_flow() {
    let temp_dir = tempfile::Builder::new().prefix("inbound_unary_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let expected_payload = "hello world";
    let request = create_test_request(expected_payload);

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);

    let response = client.ez_call(Request::new(request.clone())).await.unwrap().into_inner();

    // Assert
    let output_payload = response.payload_data.unwrap().datagrams[0].clone();
    assert_eq!(output_payload, expected_payload.as_bytes());

    server_handle.abort();
}

// Ensure that response fields are None, as opposed to empty Some, to align with expected behavior
// downstream
#[tokio::test]
async fn test_inbound_unary_error_flow() {
    let temp_dir = tempfile::Builder::new().prefix("inbound_unary_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    // Send empty EzCallRequest to invoke error path from junction
    let request = EzCallRequest::default();

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);

    let response = client.ez_call(Request::new(request.clone())).await.unwrap().into_inner();

    // Assert
    assert!(response.payload_data.is_none());
    assert!(response.payload_scope.is_none());

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_streaming_flow() {
    let temp_dir = tempfile::Builder::new().prefix("inbound_streaming_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let (to_handler_tx, to_handler_rx) = mpsc::channel(10);
    let request_stream = ReceiverStream::new(to_handler_rx);

    let first_payload = "hello";
    let second_payload = "world";

    let first_request = create_test_request(first_payload);
    let mut second_request = create_test_request(second_payload);
    // Subsequent requests in a stream don't need routing info.
    if let Some(metadata) = second_request.control_plane_metadata.as_mut() {
        metadata.destination_operator_domain = "".to_string();
        metadata.destination_service_name = "".to_string();
        metadata.destination_method_name = "".to_string();
    }

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);
    let mut response_stream =
        client.ez_streaming_call(Request::new(request_stream)).await.unwrap().into_inner();

    // Send first request and assert response
    to_handler_tx.send(first_request.clone()).await.unwrap();
    let first_response = response_stream.next().await.unwrap().unwrap();

    let first_output_payload = first_response.payload_data.unwrap().datagrams[0].clone();
    assert_eq!(first_output_payload, first_payload.as_bytes());

    // Send second request and assert response
    to_handler_tx.send(second_request).await.unwrap();

    let second_response = response_stream.next().await.unwrap().unwrap();

    let second_output_payload = second_response.payload_data.unwrap().datagrams[0].clone();
    assert_eq!(second_output_payload, second_payload.as_bytes());

    // Ensure stream closes gracefully
    drop(to_handler_tx);
    assert!(response_stream.next().await.is_none());

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_unary_flow_with_timeout() {
    let temp_dir = tempfile::Builder::new().prefix("inbound_unary_timeout_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let expected_payload = "hello world with timeout";
    let request = create_test_request(expected_payload);

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);

    let mut grpc_req = Request::new(request.clone());
    grpc_req.metadata_mut().insert("grpc-timeout", "5S".parse().unwrap());

    let response = client.ez_call(grpc_req).await.unwrap().into_inner();

    // Assert
    let output_payload = response.payload_data.unwrap().datagrams[0].clone();
    assert_eq!(output_payload, expected_payload.as_bytes());

    let deadline =
        fake_junction.last_deadline.lock().unwrap().take().expect("Deadline should be set");
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    assert!(remaining <= Duration::from_secs(5) && remaining >= Duration::from_secs(3));

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_streaming_flow_with_timeout() {
    let temp_dir =
        tempfile::Builder::new().prefix("inbound_streaming_timeout_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let (to_handler_tx, to_handler_rx) = mpsc::channel(10);
    let request_stream = ReceiverStream::new(to_handler_rx);

    let first_payload = "hello";
    let first_request = create_test_request(first_payload);

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);
    let mut grpc_req = Request::new(request_stream);
    grpc_req.metadata_mut().insert("grpc-timeout", "10S".parse().unwrap());

    let mut response_stream = client.ez_streaming_call(grpc_req).await.unwrap().into_inner();

    // Assert timeout passed to junction
    assert_eq!(*fake_junction.last_stream_timeout.lock().unwrap(), Some(Duration::from_secs(10)));

    // Send request and assert response
    to_handler_tx.send(first_request).await.unwrap();
    let first_response = response_stream.next().await.unwrap().unwrap();
    let first_output_payload = first_response.payload_data.unwrap().datagrams[0].clone();
    assert_eq!(first_output_payload, first_payload.as_bytes());

    drop(to_handler_tx);
    assert!(response_stream.next().await.is_none());

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_unary_malformed_timeout_header() {
    let temp_dir =
        tempfile::Builder::new().prefix("inbound_unary_malformed_timeout_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let expected_payload = "hello world";
    let request = create_test_request(expected_payload);

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);

    let mut grpc_req = Request::new(request);
    grpc_req.metadata_mut().insert("grpc-timeout", "invalid_timeout".parse().unwrap());

    let response = client.ez_call(grpc_req).await.unwrap().into_inner();
    let output_payload = response.payload_data.unwrap().datagrams[0].clone();
    assert_eq!(output_payload, expected_payload.as_bytes());

    // Deadline should be None because the header was malformed
    assert!(fake_junction.last_deadline.lock().unwrap().is_none());

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_streaming_malformed_timeout_header() {
    let temp_dir = tempfile::Builder::new()
        .prefix("inbound_streaming_malformed_timeout_test")
        .tempdir()
        .unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let (to_handler_tx, to_handler_rx) = mpsc::channel(10);
    let request_stream = ReceiverStream::new(to_handler_rx);

    let first_payload = "hello";
    let first_request = create_test_request(first_payload);

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);
    let mut grpc_req = Request::new(request_stream);
    grpc_req.metadata_mut().insert("grpc-timeout", "invalid_timeout".parse().unwrap());

    let mut response_stream = client.ez_streaming_call(grpc_req).await.unwrap().into_inner();

    // Timeout should be None because the header was malformed
    assert_eq!(*fake_junction.last_stream_timeout.lock().unwrap(), None);

    to_handler_tx.send(first_request).await.unwrap();
    let first_response = response_stream.next().await.unwrap().unwrap();
    let first_output_payload = first_response.payload_data.unwrap().datagrams[0].clone();
    assert_eq!(first_output_payload, first_payload.as_bytes());

    drop(to_handler_tx);
    assert!(response_stream.next().await.is_none());

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_unary_junction_error() {
    let temp_dir = tempfile::Builder::new().prefix("inbound_unary_error_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let mut request = create_test_request("hello");
    request.control_plane_metadata.as_mut().unwrap().destination_service_name =
        TEST_ERROR_SERVICE_NAME.to_string();

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);

    let result = client.ez_call(Request::new(request)).await;
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), TEST_ERROR_CODE.into());

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_streaming_junction_error() {
    let temp_dir =
        tempfile::Builder::new().prefix("inbound_streaming_error_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let (to_handler_tx, to_handler_rx) = mpsc::channel(10);
    let request_stream = ReceiverStream::new(to_handler_rx);

    let mut error_request = create_test_request("hello");
    error_request.control_plane_metadata.as_mut().unwrap().destination_service_name =
        TEST_ERROR_SERVICE_NAME.to_string();

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);
    let mut response_stream =
        client.ez_streaming_call(Request::new(request_stream)).await.unwrap().into_inner();

    to_handler_tx.send(error_request).await.unwrap();

    let response = response_stream.next().await.unwrap();
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), TEST_ERROR_CODE.into());

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_streaming_client_disconnect() {
    let temp_dir =
        tempfile::Builder::new().prefix("inbound_streaming_disconnect_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let (to_handler_tx, to_handler_rx) = mpsc::channel(10);
    let request_stream = ReceiverStream::new(to_handler_rx);

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);
    let response_stream =
        client.ez_streaming_call(Request::new(request_stream)).await.unwrap().into_inner();

    // Drop the response stream and client connection so the server stream receiver is closed
    drop(response_stream);
    drop(client);
    drop(channel_pool);
    sleep(Duration::from_millis(100)).await;

    // Send requests to trigger junction response which will fail to send to closed client channel
    for _ in 0..10 {
        let _ = to_handler_tx.send(create_test_request("hello")).await;
    }
    sleep(Duration::from_millis(100)).await;

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_streaming_junction_channel_closed() {
    let temp_dir =
        tempfile::Builder::new().prefix("inbound_streaming_dropping_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let mut fake_junction = FakeJunction::default();
    fake_junction.set_fake_isolate(Box::new(DroppingIsolate));
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let (to_handler_tx, to_handler_rx) = mpsc::channel(10);
    let request_stream = ReceiverStream::new(to_handler_rx);

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);
    let _response_stream =
        client.ez_streaming_call(Request::new(request_stream)).await.unwrap().into_inner();

    // Send request after receiver dropped in DroppingIsolate
    let _ = to_handler_tx.send(create_test_request("hello")).await;
    sleep(Duration::from_millis(50)).await;

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_payload_scope_without_static_info() {
    let temp_dir = tempfile::Builder::new().prefix("inbound_scope_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let mut request = create_test_request("test");
    request.payload_scope = Some(EzPayloadScope {
        datagram_scopes: vec![EzDataScope { static_info: None, ..Default::default() }],
    });

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);

    let response = client.ez_call(Request::new(request)).await.unwrap().into_inner();
    assert_eq!(response.payload_data.unwrap().datagrams[0], "test".as_bytes());

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_unary_shm_delivery_method() {
    let temp_dir = tempfile::Builder::new().prefix("inbound_shm_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    let mut fake_junction = FakeJunction::default();
    fake_junction.set_fake_isolate(Box::new(ShmResponseIsolate));
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction));

    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });
    let request = create_test_request("test_shm");

    sleep(Duration::from_millis(50)).await;
    let channel_pool = GrpcChannelPool::new(
        uds_address,
        DEFAULT_POOL_SIZE,
        DEFAULT_CONNECT_RETRY_COUNT,
        DEFAULT_CONNECT_RETRY_DELAY_MS,
        DEFAULT_CONNECT_RETRY_SCALING,
    )
    .await
    .unwrap();
    let channel = channel_pool.next_channel();

    let mut client = EzToEzApiClient::new(channel);

    let response = client.ez_call(Request::new(request)).await.unwrap().into_inner();
    // Delivery method is ShmData, so payload_data should be None
    assert!(response.payload_data.is_none());

    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_launch_server_existing_socket_file() {
    let temp_dir = tempfile::Builder::new().prefix("inbound_existing_file_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());

    // Pre-create the file to exercise the remove_file Ok branch
    std::fs::File::create(&uds_path).unwrap();

    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction));

    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &uds_address,
            /*max_decoding_message_size=*/ 4 * 1024 * 1024,
            /*tls_config=*/ None,
        )
        .await;
    });

    sleep(Duration::from_millis(50)).await;
    server_handle.abort();
}

#[tokio::test]
async fn test_inbound_launch_server_invalid_address() {
    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction));

    // Address that doesn't start with 'unix:' should return immediately
    inbound_ez_to_ez_handler::launch_server(
        handler,
        "tcp://127.0.0.1:8080",
        /*max_decoding_message_size=*/ 4 * 1024 * 1024,
        /*tls_config=*/ None,
    )
    .await;
}

#[tokio::test]
async fn test_inbound_launch_server_bind_failure() {
    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction));

    // Invalid path where directory does not exist
    inbound_ez_to_ez_handler::launch_server(
        handler,
        "unix:///nonexistent/path/that/cannot/be/bound/socket.sock",
        /*max_decoding_message_size=*/ 4 * 1024 * 1024,
        /*tls_config=*/ None,
    )
    .await;
}
