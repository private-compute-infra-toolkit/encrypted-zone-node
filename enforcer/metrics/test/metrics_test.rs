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

use std::time::Duration;

use fake_opentelemetry_collector::{ExportedMetric, FakeCollectorServer};
use futures::StreamExt;
use metrics::common::{MetricAttributes, ServiceMetrics};
use metrics::public_api::PublicApiMetrics;
use metrics::setup_otel_metrics;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::sync::Arc;

use ez_service_proto::enforcer::v1::CallRequest;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Clone, PartialEq, prost::Message)]
struct TestMessage {
    #[prost(string, tag = "1")]
    pub content: String,
}

#[derive(Clone)]
struct TestMetrics {
    errors: Counter<u64>,
    active_requests: UpDownCounter<i64>,
    duration_sec: Histogram<f64>,
    message_processing_duration: Histogram<f64>,
    message_size_bytes: Histogram<u64>,
}

impl ServiceMetrics for TestMetrics {
    fn errors(&self) -> Option<&Counter<u64>> {
        Some(&self.errors)
    }

    fn active_requests(&self) -> Option<&UpDownCounter<i64>> {
        Some(&self.active_requests)
    }

    fn duration_sec(&self) -> Option<&Histogram<f64>> {
        Some(&self.duration_sec)
    }

    fn message_processing_duration(&self) -> Option<&Histogram<f64>> {
        Some(&self.message_processing_duration)
    }

    fn message_size_bytes(&self) -> Option<&Histogram<u64>> {
        Some(&self.message_size_bytes)
    }
}

// Returns (Collector, Metrics, SafeProvider, UnsafeProvider)
async fn setup_test() -> (FakeCollectorServer, PublicApiMetrics, SdkMeterProvider, SdkMeterProvider)
{
    let collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    // Point both safe and unsafe to the SAME fake collector for easier assertion
    let providers = setup_otel_metrics(Some(collector.endpoint()), Some(collector.endpoint()))
        .await
        .expect("Failed to setup OTel metrics");

    let safe_provider = providers.safe.expect("Safe provider missing");
    let unsafe_provider = providers.unsafe_metrics.expect("Unsafe provider missing");

    let safe_meter = safe_provider.meter("enforcer.public_api");
    let unsafe_meter = unsafe_provider.meter("enforcer.public_api");

    let metrics = PublicApiMetrics::from_meters(safe_meter, unsafe_meter);

    (collector, metrics, safe_provider, unsafe_provider)
}

async fn setup_common_test() -> (FakeCollectorServer, TestMetrics, SdkMeterProvider) {
    let collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    let providers = setup_otel_metrics(Some(collector.endpoint()), None)
        .await
        .expect("Failed to setup OTel metrics");
    let provider = providers.safe.expect("Safe provider missing");
    let meter = provider.meter("test_meter");
    let metrics = TestMetrics {
        errors: meter.u64_counter("test.errors").build(),
        active_requests: meter.i64_up_down_counter("test.active_requests").build(),
        duration_sec: meter.f64_histogram("test.duration").build(),
        message_processing_duration: meter
            .f64_histogram("test.message_processing_duration")
            .build(),
        message_size_bytes: meter.u64_histogram("test.message_size_bytes").build(),
    };
    (collector, metrics, provider)
}

#[tokio::test]
async fn test_setup_otel_metrics_uds_waiting() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let socket_path = temp_dir.path().join("otel_waiting.sock");
    let socket_path_str = socket_path.to_str().expect("Invalid socket path").to_string();
    let socket_path_arg = format!("unix:{}", socket_path_str);

    // Spawn a task that will start listening after a short delay
    let socket_path_clone = socket_path_str.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _listener =
            tokio::net::UnixListener::bind(&socket_path_clone).expect("Failed to bind UDS");
        // Keep it alive long enough for the connection to be established
        tokio::time::sleep(Duration::from_secs(2)).await;
    });

    // The setup_otel_metrics call should now wait for the listener to start
    let providers_result = setup_otel_metrics(Some(socket_path_arg), None).await;

    assert!(
        providers_result.is_ok(),
        "Failed to setup OTel metrics over UDS with waiting: {:?}",
        providers_result.err()
    );

    let providers = providers_result.unwrap();
    assert!(providers.safe.is_some());

    // Clean up
    let _ = providers.safe.unwrap().shutdown();
}

#[tokio::test]
async fn test_record_error() {
    let (mut collector, metrics, provider) = setup_common_test().await;

    let attributes = [KeyValue::new("method", "test_method")];
    metrics.record_error(&attributes, "test_error");

    let _ = provider.shutdown();

    let exported_metrics: Vec<ExportedMetric> =
        collector.exported_metrics(1, Duration::from_secs(1)).await;

    assert_eq!(exported_metrics.len(), 1);
    assert_eq!(exported_metrics[0].metrics.len(), 1);

    let metric = &exported_metrics[0].metrics[0];
    assert_eq!(metric.name, "test.errors");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_tracker_lifecycle() {
    let (mut collector, metrics, provider) = setup_common_test().await;

    let attributes = Arc::new(vec![KeyValue::new("method", "scoped_method")]);

    {
        let _call_tracker = metrics.track_call(attributes);

        let _ = provider.force_flush();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let _ = provider.force_flush();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let _ = provider.shutdown();

    let exported_metrics: Vec<ExportedMetric> =
        collector.exported_metrics(2, Duration::from_secs(2)).await;

    assert!(exported_metrics.len() >= 2);

    let get_val = |batch: &ExportedMetric| -> Option<i64> {
        let metric = batch.metrics.iter().find(|m| m.name == "test.active_requests")?;
        let json = serde_json::to_value(metric).ok()?;
        json.get("data")
            .and_then(|d| d.get("Sum"))
            .and_then(|s| s.get("data_points"))
            .and_then(|dp| dp.get(0))
            .and_then(|p| p.get("value"))
            .and_then(|v| v.get("AsInt"))
            .and_then(|val| val.as_i64())
    };

    assert_eq!(get_val(&exported_metrics[0]), Some(1));
    assert_eq!(get_val(&exported_metrics[1]), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_tracker_duration_logging() {
    let (mut collector, metrics, provider) = setup_common_test().await;

    let attributes = Arc::new(vec![KeyValue::new("method", "duration_method")]);
    let expected_duration_ms = 300;

    {
        let _call_tracker = metrics.track_call(attributes);
        let _ = provider.force_flush();
        tokio::time::sleep(Duration::from_millis(expected_duration_ms)).await;
    }
    let _ = provider.force_flush();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let _ = provider.shutdown();

    let exported_metrics: Vec<ExportedMetric> =
        collector.exported_metrics(2, Duration::from_secs(2)).await;

    assert!(exported_metrics.len() >= 2);

    let get_histogram_val = |batch: &ExportedMetric| -> Option<f64> {
        let metric = batch.metrics.iter().find(|m| m.name == "test.duration")?;
        let json = serde_json::to_value(metric).ok()?;
        json.get("data")
            .and_then(|d| d.get("Histogram"))
            .and_then(|h| h.get("data_points"))
            .and_then(|dp| dp.get(0))
            .and_then(|p| p.get("sum"))
            .and_then(|v| v.as_f64())
    };

    // We expect the second metric batch to contain the duration histogram
    let duration_sum = get_histogram_val(&exported_metrics[1]);
    assert!(duration_sum.is_some());
    let duration = duration_sum.unwrap();

    // Allow for some small variance due to scheduler and test environment
    let lower_bound = (expected_duration_ms as f64 / 1000.0) * 0.9; // 90% of expected
    let upper_bound = (expected_duration_ms as f64 / 1000.0) * 1.5; // 150% of expected

    assert!(
        duration >= lower_bound && duration <= upper_bound,
        "Duration {} not within [{}, {}]",
        duration,
        lower_bound,
        upper_bound
    );
}

#[tokio::test]
async fn test_public_api_metrics_record_message_size_bytes() {
    let (mut collector, metrics, provider, _) = setup_test().await;

    let attributes =
        [KeyValue::new("method", "test_method"), KeyValue::new("direction", "incoming")];
    metrics.record_message_size_bytes(&attributes, 123);

    let _ = provider.shutdown();

    let exported_metrics: Vec<ExportedMetric> =
        collector.exported_metrics(1, Duration::from_secs(1)).await;
    assert_eq!(exported_metrics.len(), 1);
    assert_eq!(exported_metrics[0].metrics.len(), 1);

    let metric = &exported_metrics[0].metrics[0];
    assert_eq!(metric.name, "enforcer.public_api.message_size");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_message_processing_duration() {
    let (mut collector, metrics, provider) = setup_common_test().await;

    let attributes =
        [KeyValue::new("method", "process_msg"), KeyValue::new("direction", "request")];
    let expected_duration_ms = 100;

    {
        // RAII guard tracks duration until scope exit
        let _message_timer_guard = metrics.track_message_processing(&attributes);
        tokio::time::sleep(Duration::from_millis(expected_duration_ms)).await;
    }

    let _ = provider.shutdown();

    let exported_metrics: Vec<ExportedMetric> =
        collector.exported_metrics(1, Duration::from_secs(1)).await;

    assert_eq!(exported_metrics.len(), 1);
    let metric_batch = &exported_metrics[0];
    assert_eq!(metric_batch.metrics.len(), 1);

    let metric = &metric_batch.metrics[0];
    assert_eq!(metric.name, "test.message_processing_duration");

    let get_histogram_sum = |metric: &_| -> Option<f64> {
        let json = serde_json::to_value(metric).ok()?;
        json.get("data")
            .and_then(|d| d.get("Histogram"))
            .and_then(|h| h.get("data_points"))
            .and_then(|dp| dp.get(0))
            .and_then(|p| p.get("sum"))
            .and_then(|v| v.as_f64())
    };

    let duration = get_histogram_sum(metric).expect("Failed to get duration sum");

    // Allow for variance
    let lower_bound = (expected_duration_ms as f64 / 1000.0) * 0.9;
    let upper_bound = (expected_duration_ms as f64 / 1000.0) * 2.0;

    assert!(
        duration >= lower_bound && duration <= upper_bound,
        "Duration {} not within [{}, {}]",
        duration,
        lower_bound,
        upper_bound
    );
}

#[test]
fn test_metric_attributes_basic() {
    // Setup
    let attr = MetricAttributes::new("test.domain", "TestService", "TestMethod");

    // Test Builder Pattern
    // Add an extra attribute (e.g., simulating a route decision)
    let attr_modified = attr.with_attribute("route_type", "external");

    // Helper to check if a key/value pair exists
    let has_pair = |attrs: &[opentelemetry::KeyValue], key: &str, val: &str| {
        attrs.iter().any(|kv| kv.key.as_str() == key && kv.value.as_str() == val)
    };

    // Verify Base Attributes
    // Should contain Identity + Extra Attribute
    let base = attr_modified.base();
    assert!(has_pair(&base, "operator_domain", "test.domain"));
    assert!(has_pair(&base, "service_name", "TestService"));
    assert!(has_pair(&base, "method_name", "TestMethod"));
    assert!(has_pair(&base, "route_type", "external"));
    assert!(!has_pair(&base, "direction", "request")); // Base should NOT have direction

    // Verify Request Attributes
    // Should contain Base + Direction=Request
    let req = attr_modified.request();
    assert!(has_pair(req, "operator_domain", "test.domain"));
    assert!(has_pair(req, "route_type", "external"));
    assert!(has_pair(req, "direction", "request"));

    // Verify Response Attributes
    // Should contain Base + Direction=Response
    let resp = attr_modified.response();
    assert!(has_pair(resp, "operator_domain", "test.domain"));
    assert!(has_pair(resp, "route_type", "external"));
    assert!(has_pair(resp, "direction", "response"));
}

#[test]
fn test_metric_attribute_caching() {
    // Verify Basic Construction
    let attr = MetricAttributes::new("test_domain", "test_service", "test_method");

    // Verify Zero-Allocation Caching
    // First call: Allocates and caches the vector internally
    let req_1 = attr.request();
    // Second call: Should return reference to the SAME memory location
    let req_2 = attr.request();

    assert_eq!(req_1.len(), 4, "Should have 4 attributes: domain, service, method, direction");
    assert_eq!(
        req_1.as_ptr(),
        req_2.as_ptr(),
        "Pointer mismatch! request() is allocating on every call, breaking O(1) optimization."
    );

    // Verify Builder Pattern & Arc Copy-on-Write
    let attr_base = MetricAttributes::new("d", "s", "m");

    // 'with_attribute' should modify in-place if the Arc is exclusive (ref_count == 1)
    let attr_extended = attr_base.with_attribute("extra_key", "extra_val");

    let base_attrs = attr_extended.base();
    assert_eq!(base_attrs.len(), 4); // 3 original + 1 new
    assert!(base_attrs
        .iter()
        .any(|kv| kv.key.as_str() == "extra_key" && kv.value.as_str() == "extra_val"));

    // Verify Lazy Cache Invalidation in Builder
    // The previous 'request()' cache would be invalid for the new struct, so it must regenerate
    let req_extended = attr_extended.request();
    assert!(req_extended.iter().any(|kv| kv.key.as_str() == "extra_key"));
    assert!(req_extended
        .iter()
        .any(|kv| kv.key.as_str() == "direction" && kv.value.as_str() == "request"));
}

// ------------------------------------------------------------------------------------------------
//  ObservedStream Linked Pair Tests
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_observed_stream_pair_lifecycle() {
    let (mut collector, metrics, provider) = setup_common_test().await;

    let (req_tx, req_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);
    let (res_tx, res_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);

    let (mut request_wrapper, mut response_wrapper) = metrics::observed_stream::pair(
        ReceiverStream::new(req_rx),
        ReceiverStream::new(res_rx),
        metrics.clone(),
    );

    let response_handle = tokio::spawn(async move {
        while response_wrapper.next().await.is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let request_handle = tokio::spawn(async move {
        req_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();
        let _ = request_wrapper.next().await.unwrap();

        req_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();
        let _ = request_wrapper.next().await.unwrap();
    });

    // Send Responses concurrently
    tokio::time::sleep(Duration::from_millis(50)).await; // Give time for wakers to register
    res_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();
    res_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();

    // Wait for Request Stream to finish
    request_handle.await.unwrap();

    // Close Response Stream
    drop(res_tx);
    response_handle.await.unwrap();

    // Verify Metrics
    let _ = provider.shutdown();
    let exported = collector.exported_metrics(1, Duration::from_secs(1)).await;

    let find_metric = |name: &str| {
        exported[0]
            .metrics
            .iter()
            .find(|m| m.name == name)
            .unwrap_or_else(|| panic!("Metric {} not found", name))
    };

    // Verify Size Metrics: 2 Requests + 2 Responses = 4 Total
    let size_metric = find_metric("test.message_size_bytes");

    // Sum counts across all data points (OTel splits req/res attributes into separate points)
    let json = serde_json::to_value(size_metric).unwrap();
    let total_count: u64 = json
        .pointer("/data/Histogram/data_points")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|dp| dp.get("count").and_then(|c| c.as_u64())).sum())
        .unwrap_or(0);

    assert_eq!(total_count, 4, "Should record size for 2 req + 2 res");

    // Verify Duration Metric: 1 Total (The RequestTracker covers the whole lifecycle)
    let duration_metric = find_metric("test.duration");
    let duration_count = serde_json::to_value(duration_metric)
        .unwrap()
        .pointer("/data/Histogram/data_points/0/count")
        .unwrap()
        .as_u64()
        .unwrap();
    assert_eq!(duration_count, 1, "Should record exactly 1 lifecycle duration metric");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_observed_stream_pair_early_drop() {
    let (_collector, metrics, _provider) = setup_common_test().await;

    let (req_tx, req_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);
    let (res_tx, res_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);

    let (mut request_wrapper, mut response_wrapper) = metrics::observed_stream::pair(
        ReceiverStream::new(req_rx),
        ReceiverStream::new(res_rx),
        metrics.clone(),
    );

    // Simulate Request Stream Failure (Client Disconnects / Empty Stream)
    drop(req_tx);
    let req_result = request_wrapper.next().await;
    assert!(req_result.is_none());
    drop(request_wrapper); // Explicit drop to trigger Drop impl

    // Verify Traffic Flow (No Deadlock)
    res_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();

    // Expect the response to be received within a timeout
    let res_result = timeout(Duration::from_millis(100), response_wrapper.next()).await;
    assert!(res_result.is_ok(), "Response stream should not block/deadlock");
    let msg = res_result.unwrap().unwrap().unwrap();
    assert_eq!(msg, CallRequest { ..Default::default() });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_observed_stream_response_blocks_on_request() {
    let (_collector, metrics, _provider) = setup_common_test().await;

    let (req_tx, req_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);
    let (res_tx, res_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);

    let (mut request_wrapper, mut response_wrapper) = metrics::observed_stream::pair(
        ReceiverStream::new(req_rx),
        ReceiverStream::new(res_rx),
        metrics.clone(),
    );

    // Attempt to poll the response stream, expecting it to block.
    let result = timeout(Duration::from_millis(100), response_wrapper.next()).await;
    assert!(result.is_err(), "Response stream should block before first request");

    // Send the first request message to unblock the response stream side.
    req_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();
    // Poll the request stream to process the message and trigger the wake-up.
    let _ = request_wrapper.next().await;

    // Allow a very short time for the waker to notify the response stream.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Now, send a response. This should not block the send.
    res_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();

    // Poll the response stream again, this time it should not block.
    let result2 = timeout(Duration::from_millis(100), response_wrapper.next()).await;
    dbg!(&result2);
    assert!(result2.is_ok(), "Response stream should not block after first request");
    if let Ok(Some(Ok(msg_res))) = result2 {
        assert_eq!(msg_res, CallRequest { ..Default::default() });
    } else {
        panic!("Failed to receive response message: {:?}", result2);
    }

    drop(req_tx);
    drop(res_tx);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_observed_stream_request_error_handling() {
    let (_collector, metrics, _provider) = setup_common_test().await;

    let (req_tx, req_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);
    let (_res_tx, res_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);

    let (mut request_wrapper, _response_wrapper) = metrics::observed_stream::pair(
        ReceiverStream::new(req_rx),
        ReceiverStream::new(res_rx),
        metrics.clone(),
    );

    // Send an error
    req_tx.send(Err(tonic::Status::internal("test error"))).await.unwrap();

    // stream should return None (swallowing the error)
    let result = request_wrapper.next().await;
    assert!(result.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_observed_stream_pair_deferred() {
    let (mut collector, metrics, provider) = setup_common_test().await;

    let (req_tx, req_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);
    let (res_tx, res_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);

    let (mut request_wrapper, response_wrapper) = metrics::observed_stream::pair_deferred(
        ReceiverStream::new(req_rx),
        ReceiverStream::new(res_rx),
        metrics.clone(),
    );

    // Send a request
    req_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();

    // Consume the request
    let _ = request_wrapper.next().await.unwrap();

    // At this point, message size should NOT be recorded yet because it is deferred
    // We check this by verifying no metrics are exported yet for message size.
    // Note: collector.exported_metrics waits for metrics. If none come, it might time out or return empty if we wait short enough.
    // But ForceFlush should flush what we have.
    let _ = provider.force_flush();
    let exported_early = collector.exported_metrics(1, Duration::from_millis(200)).await;

    // Check if we have any message size metrics
    let size_metric_early = exported_early
        .iter()
        .flat_map(|b| b.metrics.iter())
        .find(|m| m.name == "test.message_size_bytes");

    if let Some(metric) = size_metric_early {
        let count: u64 = serde_json::to_value(metric)
            .unwrap()
            .pointer("/data/Histogram/data_points")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|dp| dp.get("count").and_then(|c| c.as_u64())).sum())
            .unwrap_or(0);
        assert_eq!(
            count, 0,
            "Should not record message size before finalize, but found count {}",
            count
        );
    }

    let attrs = MetricAttributes::new("test.domain", "TestService", "TestMethod");
    request_wrapper.finalize_attributes(attrs);

    // Now verification of metrics
    let _ = provider.force_flush();
    let exported_late = collector.exported_metrics(1, Duration::from_secs(1)).await;

    let size_metric_late = exported_late
        .iter()
        .flat_map(|b| b.metrics.iter())
        .find(|m| m.name == "test.message_size_bytes")
        .expect("Message size metric should be present after finalize");

    let count: u64 = serde_json::to_value(size_metric_late)
        .unwrap()
        .pointer("/data/Histogram/data_points")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|dp| dp.get("count").and_then(|c| c.as_u64())).sum())
        .unwrap_or(0);

    assert!(count >= 1, "Should record message size after finalize");

    // Clean up
    drop(req_tx);
    drop(res_tx);
    drop(response_wrapper);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_observed_stream_pair_with_attributes() {
    let (mut collector, metrics, provider) = setup_common_test().await;

    let (req_tx, req_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);
    let (res_tx, res_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);

    let attrs = MetricAttributes::new("test.domain", "TestService", "TestMethod");
    let (mut request_wrapper, mut response_wrapper) =
        metrics::observed_stream::pair_with_attributes(
            ReceiverStream::new(req_rx),
            ReceiverStream::new(res_rx),
            metrics.clone(),
            attrs,
        );

    // Response stream should be unblocked IMMEDIATELY, even before request is sent
    res_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();

    // This expects the response to be available immediately
    let result = timeout(Duration::from_millis(200), response_wrapper.next()).await;
    assert!(
        result.is_ok(),
        "Response stream should be unblocked immediately with pre-defined attributes"
    );
    let msg = result.unwrap().unwrap().unwrap();
    assert_eq!(msg, CallRequest { ..Default::default() });

    // Send a request and verify it's recorded immediately
    req_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();
    let _ = request_wrapper.next().await.unwrap();

    let _ = provider.shutdown();
    let exported = collector.exported_metrics(1, Duration::from_secs(1)).await;

    let size_metric = exported
        .iter()
        .flat_map(|b| b.metrics.iter())
        .find(|m| m.name == "test.message_size_bytes")
        .expect("Message size metric should be present");

    let count: u64 = serde_json::to_value(size_metric)
        .unwrap()
        .pointer("/data/Histogram/data_points")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|dp| dp.get("count").and_then(|c| c.as_u64())).sum())
        .unwrap_or(0);

    assert!(count >= 2, "Should record size for both req and res (2 total)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_observed_stream_memory_cap() {
    let (mut collector, metrics, provider) = setup_common_test().await;

    let (req_tx, req_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(2000);
    let (res_tx, res_rx) =
        mpsc::channel::<Result<ez_service_proto::enforcer::v1::CallRequest, tonic::Status>>(10);

    let (mut request_wrapper, response_wrapper) = metrics::observed_stream::pair_deferred(
        ReceiverStream::new(req_rx),
        ReceiverStream::new(res_rx),
        metrics.clone(),
    );

    // Send more than MAX_PENDING_SIZES (1000) messages
    let max_size = 1000;
    let overflow = 100;
    for _ in 0..(max_size + overflow) {
        req_tx.send(Ok(CallRequest { ..Default::default() })).await.unwrap();
    }

    // Process them to ensure they hit the buffer
    for _ in 0..(max_size + overflow) {
        let _ = request_wrapper.next().await.unwrap();
    }

    let attrs = MetricAttributes::new("test.domain", "TestService", "TestMethod");
    request_wrapper.finalize_attributes(attrs);

    // Verify metrics count
    let _ = provider.force_flush();
    let exported = collector.exported_metrics(1, Duration::from_secs(1)).await;

    let size_metric = exported
        .iter()
        .flat_map(|b| b.metrics.iter())
        .find(|m| m.name == "test.message_size_bytes")
        .expect("Message size metric should be present");

    let count: u64 = serde_json::to_value(size_metric)
        .unwrap()
        .pointer("/data/Histogram/data_points")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|dp| dp.get("count").and_then(|c| c.as_u64())).sum())
        .unwrap_or(0);

    // Expect exactly MAX_PENDING_SIZES metrics, as the overflow should have been dropped
    assert_eq!(
        count, max_size as u64,
        "Should record exactly MAX_PENDING_SIZES metrics, dropping the overflow"
    );

    drop(req_tx);
    drop(res_tx);
    drop(response_wrapper);
}
