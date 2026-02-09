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
use enforcer_proto::enforcer::v1::ControlPlaneMetadata;
use ez_to_ez_service_proto::enforcer::v1::ez_to_ez_api_client::EzToEzApiClient;
use ez_to_ez_service_proto::enforcer::v1::EzCallRequest;
use hyper_util::rt::TokioIo;
use inbound_ez_to_ez_handler::InboundEzToEzHandler;
use junction_test_utils::FakeJunction;
use payload_proto::enforcer::v1::{EzPayloadData, EzPayloadScope};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Request;

/// Helper function to connect to the UDS server for tests.
async fn connect_to_server(uds_address: &str) -> Channel {
    let path = uds_address
        .strip_prefix("unix://")
        .or_else(|| uds_address.strip_prefix("unix:"))
        .unwrap()
        .to_string();
    // The URI is a dummy, the path is what's used by the connector.
    Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let path = path.clone();
            async move { Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(path).await?)) }
        }))
        .await
        .unwrap()
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
        inbound_ez_to_ez_handler::launch_server(handler, &server_address, 4 * 1024 * 1024).await;
    });
    let expected_payload = "hello world";
    let request = create_test_request(expected_payload);

    sleep(Duration::from_millis(50)).await;
    let channel = connect_to_server(&uds_address).await;

    let mut client = EzToEzApiClient::new(channel);

    let response = client.ez_call(Request::new(request.clone())).await.unwrap().into_inner();

    // Assert
    assert_eq!(response.status.unwrap().code, 0);
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
        inbound_ez_to_ez_handler::launch_server(handler, &server_address, 4 * 1024 * 1024).await;
    });
    // Send empty EzCallRequest to invoke error path from junction
    let request = EzCallRequest::default();

    sleep(Duration::from_millis(50)).await;
    let channel = connect_to_server(&uds_address).await;

    let mut client = EzToEzApiClient::new(channel);

    let response = client.ez_call(Request::new(request.clone())).await.unwrap().into_inner();

    // Assert
    assert_eq!(response.status.unwrap().code, 0);
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
        inbound_ez_to_ez_handler::launch_server(handler, &server_address, 4 * 1024 * 1024).await;
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
    let channel = connect_to_server(&uds_address).await;

    let mut client = EzToEzApiClient::new(channel);
    let mut response_stream =
        client.ez_streaming_call(Request::new(request_stream)).await.unwrap().into_inner();

    // Send first request and assert response
    to_handler_tx.send(first_request.clone()).await.unwrap();
    let first_response = response_stream.next().await.unwrap().unwrap();

    assert_eq!(first_response.status.unwrap().code, 0);
    let first_output_payload = first_response.payload_data.unwrap().datagrams[0].clone();
    assert_eq!(first_output_payload, first_payload.as_bytes());

    // Send second request and assert response
    to_handler_tx.send(second_request).await.unwrap();

    let second_response = response_stream.next().await.unwrap().unwrap();

    assert_eq!(second_response.status.unwrap().code, 0);
    let second_output_payload = second_response.payload_data.unwrap().datagrams[0].clone();
    assert_eq!(second_output_payload, second_payload.as_bytes());

    // Ensure stream closes gracefully
    drop(to_handler_tx);
    assert!(response_stream.next().await.is_none());

    server_handle.abort();
}
