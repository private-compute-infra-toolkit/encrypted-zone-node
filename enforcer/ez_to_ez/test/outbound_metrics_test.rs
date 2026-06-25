// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use fake_opentelemetry_collector::FakeCollectorServer;
use metrics::ez_to_ez_outbound::EzToEzOutboundMetrics;
use metrics::setup_otel_metrics;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use outbound_ez_to_ez_handler::OutboundEzToEzHandler;
use std::time::Duration;
use tokio::sync::mpsc;

#[path = "outbound_test.rs"]
mod outbound_test;
use outbound_test::{create_test_request, start_fake_proxy_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_outbound_stream_metrics() {
    let mut collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    let providers = setup_otel_metrics(Some(collector.endpoint()), Some(collector.endpoint()))
        .await
        .expect("Failed to setup OTel metrics");

    let (port, shutdown_tx) = start_fake_proxy_server(None).await;
    let server_address = format!("http://localhost:{}", port);

    let metrics = EzToEzOutboundMetrics::default();
    let handler = OutboundEzToEzHandler::new(server_address, metrics, None).await.unwrap();

    let first_payload = "hello metrics 1";
    let second_payload = "hello metrics 2";

    let (local_to_outbound, from_local_rx) = mpsc::channel(10);
    let mut outbound_to_local =
        handler.remote_streaming_connect(None, from_local_rx).await.unwrap();

    let initial_request = create_test_request(Some(first_payload));
    local_to_outbound.send(initial_request).await.unwrap();
    let _ = outbound_to_local.recv().await.unwrap().unwrap();

    let second_request = create_test_request(Some(second_payload));
    local_to_outbound.send(second_request).await.unwrap();
    let _ = outbound_to_local.recv().await.unwrap().unwrap();

    drop(local_to_outbound);
    assert!(outbound_to_local.recv().await.is_none());

    let _ = shutdown_tx.send(());

    tokio::time::sleep(Duration::from_millis(200)).await;
    if let Some(ref provider) = providers.safe {
        let _ = provider.force_flush();
    }
    if let Some(ref provider) = providers.unsafe_metrics {
        let _ = provider.force_flush();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Some(provider) = providers.safe {
        let _ = provider.shutdown();
    }
    if let Some(provider) = providers.unsafe_metrics {
        let _ = provider.shutdown();
    }

    let exported = collector.exported_metrics(5, Duration::from_secs(2)).await;

    let verifier = metrics_test_utils::MetricsVerifier::new(exported);
    let points = verifier.get_histogram_points("enforcer.ez_to_ez.outbound.message_size");

    let mut request_message_count: u64 = 0;
    let mut response_message_count: u64 = 0;

    for (count, attributes) in points {
        if let Some(direction) = attributes.get("direction") {
            if direction.contains("request") {
                request_message_count = request_message_count.max(count);
            } else if direction.contains("response") {
                response_message_count = response_message_count.max(count);
            }
        }
    }

    assert_eq!(request_message_count, 2, "Expected exactly 2 outbound request size observations");
    assert_eq!(response_message_count, 2, "Expected exactly 2 outbound response size observations");
}
