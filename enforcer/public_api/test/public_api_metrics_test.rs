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
use container_manager_requester::ContainerManagerRequester;
use data_scope::requester::DataScopeRequester;
use ez_service_proto::enforcer::v1::{
    ez_public_api_client::EzPublicApiClient, ez_public_api_server::EzPublicApiServer,
};
use ez_service_proto::enforcer::v1::{CallParameters, CallRequest, SessionMetadata};
use fake_opentelemetry_collector::FakeCollectorServer;
use health_manager::HealthManager;
use interceptor::Interceptor;
use isolate_info::IsolateServiceInfo;
use isolate_service_mapper::IsolateServiceMapper;
use junction_test_utils::FakeJunction;
use manifest_proto::enforcer::InterceptingServices;
use metrics::setup_otel_metrics;
use public_api::EzPublicApiService;
use state_manager::IsolateStateManager;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::TcpListener;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE: usize = 10;
const OPAQUE_DOMAIN: &str = "opaque.domain";
const OPAQUE_SERVICE: &str = "OpaqueService";
const RATIFIED_INTERCEPTOR_DOMAIN: &str = "ratified.interceptor.domain";
const RATIFIED_INTERCEPTOR_SERVICE: &str = "RatifiedInterceptorService";
const RATIFIED_INTERCEPTOR_UNARY: &str = "unary";
const RATIFIED_INTERCEPTOR_STREAMING: &str = "streaming";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stream_call_metrics() {
    let mut collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    let providers = setup_otel_metrics(Some(collector.endpoint()), Some(collector.endpoint()))
        .await
        .expect("Failed to setup OTel metrics");

    let fake_junction = FakeJunction::default();
    let interceptor = setup_interceptor().await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let cm_req = ContainerManagerRequester::new(tx);
    let ds_req = DataScopeRequester::new(0);
    let ism = IsolateStateManager::new(ds_req.clone(), cm_req.clone());
    let health_manager = HealthManager::new(ism, cm_req, IsolateServiceMapper::default(), ds_req);

    let test_service =
        EzPublicApiService::new(Box::new(fake_junction.clone()), interceptor, health_manager).await;

    let sockaddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let listener = TcpListener::bind(sockaddr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(EzPublicApiServer::new(test_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                shutdown_rx.await.ok();
            })
            .await
    });

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };
    let req1_bytes = vec![1, 2, 3];
    let req2_bytes = vec![4, 5];

    let req1 = CallRequest {
        operator_domain: "test_domain".to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters {
            public_input: req1_bytes.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let req2 = CallRequest {
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters {
            public_input: req2_bytes.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let (client_tx, client_rx) =
        tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);

    let client_handle = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(client_rx);
        let mut response_stream = client.stream_call(request_stream).await.unwrap().into_inner();

        let _ = client_tx.send(req1).await;
        // Wait for first response
        if let Ok(Some(_)) = response_stream.message().await {
            // Success
        }

        let _ = client_tx.send(req2).await;
        // Wait for second response
        if let Ok(Some(_)) = response_stream.message().await {
            // Success
        }

        drop(client_tx);

        // Drain remaining messages to ensure clean shutdown without infinite loop
        while let Ok(Some(_)) = response_stream.message().await {}
    });

    client_handle.await.unwrap();
    let _ = shutdown_tx.send(());

    // Give time for metrics to be processed and sent
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Some(ref provider) = providers.safe {
        let _ = provider.force_flush();
    }
    if let Some(ref provider) = providers.unsafe_metrics {
        let _ = provider.force_flush();
    }
    // Small delay to ensure flush completes and collector receives data
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Some(provider) = providers.safe {
        let _ = provider.shutdown();
    }
    if let Some(provider) = providers.unsafe_metrics {
        let _ = provider.shutdown();
    }

    // Fetch metrics from the fake collector
    let exported = collector.exported_metrics(5, Duration::from_secs(2)).await;

    let mut request_message_sizes: Vec<usize> = Vec::new();
    let mut request_message_count: u64 = 0;
    let mut response_message_sizes: Vec<usize> = Vec::new();
    let mut response_message_count: u64 = 0;
    let mut processing_durations: Vec<f64> = Vec::new();
    let mut processing_duration_count: u64 = 0;
    let mut stream_durations: Vec<f64> = Vec::new();
    let mut stream_duration_count: u64 = 0;
    let mut active_requests_values: Vec<i64> = Vec::new();

    // Note: With cumulative temporality, each export contains the cumulative count.
    // We track the max count seen for each direction (the final cumulative value).

    for batch in exported {
        for metric in batch.metrics {
            match metric.name.as_str() {
                "enforcer.public_api.message_size" => {
                    let json = serde_json::to_value(&metric).unwrap();

                    if let Some(data_points) =
                        json.pointer("/data/Histogram/data_points").and_then(|v| v.as_array())
                    {
                        for point in data_points {
                            let sum = point.pointer("/sum").and_then(|v| v.as_f64());
                            let count = point.pointer("/count").and_then(|v| v.as_u64());
                            let attributes =
                                point.pointer("/attributes").and_then(|v| v.as_object());

                            if let (Some(sum), Some(cnt), Some(attrs)) = (sum, count, attributes) {
                                let direction =
                                    attrs.get("direction").and_then(|v| v.as_str()).map(|s| {
                                        if s.contains("response") {
                                            "response"
                                        } else if s.contains("request") {
                                            "request"
                                        } else {
                                            "unknown"
                                        }
                                    });

                                if let Some(dir) = direction {
                                    if dir == "request" {
                                        request_message_sizes.push(sum as usize);
                                        request_message_count = request_message_count.max(cnt);
                                    } else if dir == "response" {
                                        response_message_sizes.push(sum as usize);
                                        response_message_count = response_message_count.max(cnt);
                                    }
                                }
                            }
                        }
                    }
                }
                "enforcer.public_api.message.processing_duration" => {
                    let json = serde_json::to_value(&metric).unwrap();
                    if let Some(data_points) =
                        json.pointer("/data/Histogram/data_points").and_then(|v| v.as_array())
                    {
                        for point in data_points {
                            if let Some(sum) = point.pointer("/sum").and_then(|v| v.as_f64()) {
                                processing_durations.push(sum);
                            }
                            if let Some(cnt) = point.pointer("/count").and_then(|v| v.as_u64()) {
                                processing_duration_count = processing_duration_count.max(cnt);
                            }
                        }
                    }
                }
                "enforcer.public_api.request.duration" => {
                    let json = serde_json::to_value(&metric).unwrap();
                    if let Some(data_points) =
                        json.pointer("/data/Histogram/data_points").and_then(|v| v.as_array())
                    {
                        for point in data_points {
                            if let Some(sum) = point.pointer("/sum").and_then(|v| v.as_f64()) {
                                stream_durations.push(sum);
                            }
                            if let Some(cnt) = point.pointer("/count").and_then(|v| v.as_u64()) {
                                stream_duration_count = stream_duration_count.max(cnt);
                            }
                        }
                    }
                }
                "enforcer.public_api.active_requests" => {
                    let json = serde_json::to_value(&metric).unwrap();
                    if let Some(data_points) =
                        json.pointer("/data/Sum/data_points").and_then(|v| v.as_array())
                    {
                        for point in data_points {
                            if let Some(val) = point.get("as_int").and_then(|v| v.as_i64()) {
                                active_requests_values.push(val);
                            } else if let Some(val) =
                                point.pointer("/value/as_int").and_then(|v| v.as_i64())
                            {
                                active_requests_values.push(val);
                            } else if let Some(val) = point.get("AsInt").and_then(|v| v.as_i64()) {
                                active_requests_values.push(val);
                            } else if let Some(val) =
                                point.pointer("/value/AsInt").and_then(|v| v.as_i64())
                            {
                                active_requests_values.push(val);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Validate message sizes
    // Test sends 2 request messages, receives 2 response messages
    let total_request_bytes = req1_bytes.len() + req2_bytes.len();
    assert!(!request_message_sizes.is_empty(), "Expected at least one request message size metric");
    assert_eq!(
        request_message_count, 2,
        "Expected exactly 2 request message size observations (req1 + req2)"
    );
    // The sum should include all request bytes sent
    let max_request_sum = *request_message_sizes.iter().max().unwrap();
    assert!(
        max_request_sum >= total_request_bytes,
        "Request message size sum ({}) should be >= total request bytes ({})",
        max_request_sum,
        total_request_bytes
    );

    assert!(
        !response_message_sizes.is_empty(),
        "Expected at least one response message size metric"
    );
    assert_eq!(
        response_message_count, 2,
        "Expected exactly 2 response message size observations (resp1 + resp2)"
    );
    // Response sizes should be positive (we received responses)
    let max_response_sum = *response_message_sizes.iter().max().unwrap();
    assert!(
        max_response_sum > 0,
        "Response message size sum should be positive, got {}",
        max_response_sum
    );

    // Validate processing durations
    // Processing duration is recorded per direction (request + response = 2)
    assert!(!processing_durations.is_empty(), "Expected at least one processing duration metric");
    assert_eq!(
        processing_duration_count, 2,
        "Expected exactly 2 processing duration observations (1 req + 1 resp direction)"
    );
    for duration in &processing_durations {
        assert!(*duration >= 0.0, "Processing duration should be non-negative, got {}", duration);
    }

    // Validate stream duration is positive and reasonable
    // 1 stream call = 1 duration observation
    assert!(!stream_durations.is_empty(), "Expected at least one stream duration metric");
    assert_eq!(
        stream_duration_count, 1,
        "Expected exactly 1 stream duration observation (1 stream call)"
    );
    for duration in &stream_durations {
        assert!(*duration > 0.0, "Stream duration should be positive, got {}", duration);
        assert!(
            *duration < 10.0,
            "Stream duration should be < 10s for this test, got {}",
            duration
        );
    }

    // Validate active_requests ends at 0 (all streams completed)
    // For 1 stream call: CallTracker increments +1 on start, -1 on drop = 0 final value
    assert!(!active_requests_values.is_empty(), "Expected at least one active_requests metric");

    // All exported values should be 0 since the stream completed before metrics export
    // (we wait for client_handle.await and flush before collecting metrics)
    assert!(
        active_requests_values.iter().all(|v| *v == 0),
        "active_requests should be 0 after stream completion (stream started and ended before export), got {:?}",
        active_requests_values
    );
}

async fn setup_interceptor() -> Interceptor {
    let mapper = IsolateServiceMapper::default();
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: OPAQUE_DOMAIN.to_string(),
                service_name: OPAQUE_SERVICE.to_string(),
            }],
            false, // is_ratified
        )
        .await
        .unwrap();
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
                service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
            }],
            true, // is_ratified
        )
        .await
        .unwrap();
    let interceptor = Interceptor::new(mapper);
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: OPAQUE_SERVICE.to_string(),
        interceptor_operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        interceptor_service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        interceptor_method_for_unary: RATIFIED_INTERCEPTOR_UNARY.to_string(),
        interceptor_method_for_streaming: RATIFIED_INTERCEPTOR_STREAMING.to_string(),
        ..Default::default()
    };
    interceptor.add_interceptor(intercepting_services).await.unwrap();
    interceptor
}
