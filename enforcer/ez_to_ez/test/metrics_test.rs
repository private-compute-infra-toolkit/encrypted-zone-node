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

use data_scope_proto::enforcer::v1::{DataScopeType, EzDataScope, EzStaticScopeInfo};
use enforcer_proto::enforcer::v1::ControlPlaneMetadata;
use ez_to_ez_service_proto::enforcer::v1::ez_to_ez_api_client::EzToEzApiClient;
use ez_to_ez_service_proto::enforcer::v1::EzCallRequest;
use fake_opentelemetry_collector::FakeCollectorServer;
use inbound_ez_to_ez_handler::InboundEzToEzHandler;
use junction_test_utils::FakeJunction;

use metrics::setup_otel_metrics;
use metrics_test_utils::MetricsVerifier;

use payload_proto::enforcer::v1::{EzPayloadData, EzPayloadScope};
use std::time::Duration;
use tokio::time::sleep;
use tonic::Request;

use grpc_connector::{connect, DEFAULT_CONNECT_RETRY_COUNT, DEFAULT_CONNECT_RETRY_DELAY_MS};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_inbound_ez_to_ez_metrics() {
    let mut collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    let providers = setup_otel_metrics(Some(collector.endpoint()), Some(collector.endpoint()))
        .await
        .expect("Failed to setup OTel metrics");

    let temp_dir = tempfile::Builder::new().prefix("inbound_metrics_test").tempdir().unwrap();
    let uds_path = temp_dir.path().join("ez_to_ez.sock");
    let uds_address = format!("unix://{}", uds_path.to_str().unwrap());
    let fake_junction = FakeJunction::default();
    let handler = InboundEzToEzHandler::new(Box::new(fake_junction.clone()));
    let server_address = uds_address.clone();
    let server_handle = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(handler, &server_address, 4 * 1024 * 1024).await;
    });

    let channel = connect(uds_address, DEFAULT_CONNECT_RETRY_COUNT, DEFAULT_CONNECT_RETRY_DELAY_MS)
        .await
        .expect("Failed to connect to UDS server");
    let mut client = EzToEzApiClient::new(channel);

    let req1 = create_test_request("request1");
    let _resp1 = client.ez_call(Request::new(req1)).await.unwrap();

    let req2 = create_test_request("request2");
    let _resp2 = client.ez_call(Request::new(req2)).await.unwrap();

    // Make Streaming Request (2 stream messages)
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let req_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    let mut response_stream =
        client.ez_streaming_call(Request::new(req_stream)).await.unwrap().into_inner();

    tx.send(create_test_request("stream1")).await.unwrap();
    tx.send(create_test_request("stream2")).await.unwrap();

    drop(tx);
    while response_stream.message().await.unwrap_or(None).is_some() {}
    server_handle.abort();
    sleep(Duration::from_secs(1)).await;

    if let Some(ref provider) = providers.safe {
        let _res = provider.force_flush();
    }
    if let Some(ref provider) = providers.unsafe_metrics {
        let _res = provider.force_flush();
    }
    sleep(Duration::from_secs(1)).await;

    let exported = collector.exported_metrics(5, Duration::from_secs(2)).await;
    let verifier = MetricsVerifier::new(exported);

    // Verify Request Count (2 unary + 1 stream connection = 3)
    let request_counts = verifier.get_counter_points("enforcer.ez_to_ez.inbound.request");
    let total_requests: u64 = request_counts
        .iter()
        .filter(|(_, attrs)| {
            attrs.get("service_name").map(|s| s.contains("test_service")).unwrap_or(false)
        })
        .map(|(cnt, _)| *cnt)
        .sum();

    assert_eq!(
        total_requests, 3,
        "Expected exactly 3 requests recorded (2 unary, 1 streaming), got {}. Captured: {:?}",
        total_requests, request_counts
    );

    let duration_points =
        verifier.get_histogram_points("enforcer.ez_to_ez.inbound.request.duration");
    let total_duration_count: u64 = duration_points
        .iter()
        .filter(|(_, attrs)| {
            attrs.get("service_name").map(|s| s.contains("test_service")).unwrap_or(false)
        })
        .map(|(cnt, _)| *cnt)
        .sum();
    assert_eq!(
        total_duration_count, 3,
        "Expected exactly 3 duration observations (2 unary, 1 streaming)"
    );

    // Verify Message Size (Histogram)
    // Unary: 2 requests * (1 req msg + 1 resp msg) = 4 observations
    // Streaming: 1 request stream * (2 req msg + 2 resp msg from FakeJunction) = 4 observations
    // Total = 8 observations
    let msg_size_points = verifier.get_histogram_points("enforcer.ez_to_ez.inbound.message_size");
    let total_msg_size_count: u64 = msg_size_points
        .iter()
        .filter(|(_, attrs)| {
            attrs.get("service_name").map(|s| s.contains("test_service")).unwrap_or(false)
        })
        .map(|(cnt, _)| *cnt)
        .sum();
    assert_eq!(
        total_msg_size_count, 8,
        "Expected 8 message size observations (4 from unary + 4 from streaming)"
    );
}
