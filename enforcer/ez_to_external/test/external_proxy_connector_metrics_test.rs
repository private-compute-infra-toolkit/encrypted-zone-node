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

use external_proxy_connector::ExternalProxyChannel;
use fake_opentelemetry_collector::FakeCollectorServer;
use isolate_info::{BinaryServicesIndex, IsolateId};
use metrics::setup_otel_metrics;
use std::time::Duration;
use tokio::sync::mpsc;

use std::collections::HashMap;

#[path = "external_proxy_connector_test.rs"]
mod external_proxy_connector_test;
use external_proxy_connector_test::{
    build_test_connector, create_generic_test_request, setup_uds_server, MockProxyService,
};

fn max_cumulative_sum_u64(points: &[(u64, HashMap<String, String>)]) -> u64 {
    let mut map: std::collections::HashMap<Vec<(&String, &String)>, u64> =
        std::collections::HashMap::new();
    for (count, attrs) in points {
        let mut sorted_attrs: Vec<_> = attrs.iter().collect();
        sorted_attrs.sort_by_key(|&(k, _)| k);
        let entry = map.entry(sorted_attrs).or_insert(0);
        *entry = (*entry).max(*count);
    }
    map.values().sum()
}

#[tokio::test]
async fn test_external_proxy_metrics() {
    let mut collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    let _providers = setup_otel_metrics(Some(collector.endpoint()), Some(collector.endpoint()))
        .await
        .expect("Failed to setup OTel metrics");

    let isolate_id = IsolateId::new(BinaryServicesIndex::new(true));

    let (server_address, shutdown_tx, _temp_dir) =
        setup_uds_server(MockProxyService::default()).await;
    let connector = build_test_connector(server_address).await.unwrap();

    let (tx, rx) = mpsc::channel(10);

    let request1 = create_generic_test_request(vec![vec![10, 20, 30]]);
    tx.send(request1.clone()).await.unwrap();

    let mut response_receiver = connector.stream_proxy_external(isolate_id, rx).await.unwrap();

    let request2 = create_generic_test_request(vec![vec![40, 50, 60]]);
    tx.send(request2.clone()).await.unwrap();

    let _ = response_receiver.recv().await.expect("Should receive the first response");
    let _ = response_receiver.recv().await.expect("Should receive the second response");

    let request3 = create_generic_test_request(vec![vec![70, 80, 90]]);
    let _ = connector.proxy_external(isolate_id, request3.clone(), None).await.unwrap();

    drop(tx);
    let _ = shutdown_tx.send(());

    // Wait for the receiver to close, which guarantees handle_requests and handle_responses
    // have fully exited and dropped their `req_tx_guard`s, triggering the CallTracker's Drop.
    // We add a timeout just in case it deadlocks, so we don't hang the entire CI.
    let _ = tokio::time::timeout(Duration::from_secs(2), response_receiver.recv()).await;

    let p_safe = _providers.safe.clone();
    let p_unsafe = _providers.unsafe_metrics.clone();

    let _ = tokio::task::spawn_blocking(move || {
        if let Some(ref provider) = p_safe {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
        if let Some(ref provider) = p_unsafe {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
    })
    .await;

    let exported = collector.exported_metrics(5, Duration::from_secs(2)).await;

    let verifier = metrics_test_utils::MetricsVerifier::new(exported);

    let requests = verifier.get_counter_points("enforcer.external.request");
    assert!(!requests.is_empty(), "enforcer.external.request metric is missing");
    let request_count = max_cumulative_sum_u64(&requests);
    assert_eq!(request_count, 2, "Expected 2 total external proxy request executions");

    let active_requests = verifier.get_counter_points("enforcer.external.active_requests");
    assert!(!active_requests.is_empty(), "enforcer.external.active_requests metric is missing");

    let msg_sizes = verifier.get_histogram_points("enforcer.external.message_size");
    assert!(!msg_sizes.is_empty(), "enforcer.external.message_size metric is missing");

    let req_msg_sizes: Vec<_> = msg_sizes
        .iter()
        .filter(|(_, attrs)| attrs.get("direction").is_some_and(|v| v.contains("request")))
        .cloned()
        .collect();
    let req_size_count = max_cumulative_sum_u64(&req_msg_sizes);

    let resp_msg_sizes: Vec<_> = msg_sizes
        .iter()
        .filter(|(_, attrs)| attrs.get("direction").is_some_and(|v| v.contains("response")))
        .cloned()
        .collect();
    let resp_size_count = max_cumulative_sum_u64(&resp_msg_sizes);
    assert_eq!(req_size_count, 3, "Expected exactly 3 external request size observations");
    assert_eq!(resp_size_count, 3, "Expected exactly 3 external response size observations");

    let durations = verifier.get_histogram_points("enforcer.external.request.duration");
    assert!(!durations.is_empty(), "enforcer.external.request.duration is missing");
    // Histograms are usually exported as Delta, but just in case we need the sum of the maximum occurrences over each export
    // Wait, get_histogram_points gets the COUNT of items in the histogram, not the sum of values.
    // If it's cumulative, we need max count. If delta, we need sum.
    // FakeCollector for open telemetry histograms returns points based on the underlying SDK format.
    // If it fails with sum > 2, we will change it to max. But for now delta sum:
    let _duration_count: u64 = durations.iter().map(|(cnt, _)| *cnt).sum();
    // In other observed tests, count is max. So let's use max_cumulative_sum_u64 since counts are cumulative in OTLP.
    let duration_count = max_cumulative_sum_u64(&durations);
    assert_eq!(duration_count, 2, "Expected exactly 2 duration observations (1 stream, 1 unary)");

    let processing_durations =
        verifier.get_histogram_points("enforcer.external.message.processing_duration");
    assert!(
        !processing_durations.is_empty(),
        "enforcer.external.message.processing_duration is missing"
    );
    let proc_duration_count = max_cumulative_sum_u64(&processing_durations);
    assert!(proc_duration_count >= 3, "Expected at least 3 processing duration observations");
}
