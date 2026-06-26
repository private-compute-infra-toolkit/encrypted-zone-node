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

use container::ContainerRunStatus as ContainerRunStatusEnum;
use container_manager_request::ContainerManagerRequest;
use container_manager_requester::ContainerManagerRequester;
use data_scope::request::AddIsolateRequest;
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::IsolateState;
use fake_opentelemetry_collector::FakeCollectorServer;
use health_manager::HealthManager;
use isolate_info::IsolateServiceInfo;
use isolate_service_mapper::IsolateServiceMapper;
use metrics::setup_otel_metrics;
use metrics_test_utils::MetricsVerifier;
use state_manager::IsolateStateManager;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_health_manager_metrics_scenarios() {
    let mut collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    let providers = setup_otel_metrics(Some(collector.endpoint()), Some(collector.endpoint()))
        .await
        .expect("Failed to setup OTel metrics");

    let (cm_tx, mut cm_rx) = tokio::sync::mpsc::channel(100);
    let cm_req = ContainerManagerRequester::new(cm_tx);
    let ds_req = DataScopeRequester::new(0);
    let ism = IsolateStateManager::new(ds_req.clone(), cm_req.clone());

    let mapper = IsolateServiceMapper::default();

    // Register Starting isolate
    let starting_binary_index = mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: "starting_domain".to_string(),
                service_name: "starting_service".to_string(),
                isolate_name: "starting_isolate".to_string(),
                publisher_id: "starting_publisher".to_string(),
            }],
            false,
            "starting_publisher".to_string(),
            "starting_isolate".to_string(),
        )
        .await
        .expect("Failed to add binary index");
    let starting_isolate_id = isolate_info::IsolateId::new(starting_binary_index);
    ism.add_isolate(AddIsolateRequest {
        isolate_id: starting_isolate_id,
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::Public,
    })
    .await;

    // Register Ready isolate
    let ready_binary_index = mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: "ready_domain".to_string(),
                service_name: "ready_service".to_string(),
                isolate_name: "ready_isolate".to_string(),
                publisher_id: "ready_publisher".to_string(),
            }],
            false,
            "ready_publisher".to_string(),
            "ready_isolate".to_string(),
        )
        .await
        .expect("Failed to add binary index");
    let ready_isolate_id = isolate_info::IsolateId::new(ready_binary_index);
    ism.add_isolate(AddIsolateRequest {
        isolate_id: ready_isolate_id,
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::Public,
    })
    .await;
    ism.update_state(ready_isolate_id, IsolateState::Ready).await.expect("Failed to update state");

    let health_manager = HealthManager::new(ism.clone(), cm_req, mapper.clone(), ds_req);

    let starting_should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ready_should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let starting_should_exit_clone = starting_should_exit.clone();
    let ready_should_exit_clone = ready_should_exit.clone();

    tokio::spawn(async move {
        while let Some(req) = cm_rx.recv().await {
            match req {
                ContainerManagerRequest::GetRunStatus { req, resp } => {
                    let isolate_id = req.isolate_id;
                    let should_exit = (isolate_id == starting_isolate_id
                        && starting_should_exit_clone.load(std::sync::atomic::Ordering::Relaxed))
                        || (isolate_id == ready_isolate_id
                            && ready_should_exit_clone.load(std::sync::atomic::Ordering::Relaxed));
                    let status = if should_exit {
                        ContainerRunStatusEnum::Exited(1)
                    } else {
                        ContainerRunStatusEnum::Running
                    };
                    let (rss_bytes, peak_rss_bytes, virt_bytes, shared_bytes, data_bytes) =
                        if status == ContainerRunStatusEnum::Running {
                            (
                                Some(100_000_000),
                                Some(200_000_000),
                                Some(500_000_000),
                                Some(50_000_000),
                                Some(10_000_000),
                            )
                        } else {
                            (None, None, None, None, None)
                        };
                    let _ = resp.send(Ok(container_manager_request::GetRunStatusResponse {
                        status,
                        rss_bytes,
                        peak_rss_bytes,
                        virt_bytes,
                        shared_bytes,
                        data_bytes,
                        restart_count: 0,
                    }));
                }
                ContainerManagerRequest::ResetIsolateRequest { resp, .. } => {
                    let _ = resp.send(Ok(container_manager_request::ResetIsolateResponse {}));
                }
                _ => {}
            }
        }
    });

    // Run 1: both running. Verify state metrics.
    health_manager.run().await;
    if let Some(ref provider) = providers.safe {
        let _ = provider.force_flush();
    }

    let exported = collector.exported_metrics(5, Duration::from_secs(5)).await;
    let verifier = MetricsVerifier::new(exported);

    let state_points =
        verifier.get_gauge_points("encrypted_zone.enforcer.health.isolate.state", false);
    assert!(
        !state_points.is_empty(),
        "Expected encrypted_zone.enforcer.health.isolate.state metric"
    );

    let assert_gauge_point = |points: &[(f64, std::collections::HashMap<String, String>)],
                              expected_val: f64,
                              isolate_name: &str,
                              publisher_id: &str| {
        let found = points.iter().any(|(val, attrs)| {
            *val == expected_val
                && attrs.get("ez_isolate_name").map(|s| s.as_str()) == Some(isolate_name)
                && attrs.get("ez_publisher_id").map(|s| s.as_str()) == Some(publisher_id)
                && attrs.get("ez_component_name").map(|s| s.as_str()) == Some("isolate")
                && attrs.get("ez_isolate_type").map(|s| s.as_str()) == Some("opaque")
        });
        assert!(
            found,
            "Expected point with value {} for isolate {}/{}",
            expected_val, publisher_id, isolate_name
        );
    };

    assert_gauge_point(&state_points, 1.0, "starting_isolate", "starting_publisher");
    assert_gauge_point(&state_points, 7.0, "ready_isolate", "ready_publisher");

    let mem_isolate_points =
        verifier.get_gauge_points("encrypted_zone.enforcer.isolate.memory.resident_size", false);
    assert_gauge_point(
        &mem_isolate_points,
        100_000_000.0,
        "starting_isolate",
        "starting_publisher",
    );
    assert_gauge_point(&mem_isolate_points, 100_000_000.0, "ready_isolate", "ready_publisher");

    let peak_mem_isolate_points = verifier
        .get_gauge_points("encrypted_zone.enforcer.isolate.memory.peak_resident_size", false);
    assert_gauge_point(
        &peak_mem_isolate_points,
        200_000_000.0,
        "starting_isolate",
        "starting_publisher",
    );
    assert_gauge_point(&peak_mem_isolate_points, 200_000_000.0, "ready_isolate", "ready_publisher");

    let virt_mem_isolate_points =
        verifier.get_gauge_points("encrypted_zone.enforcer.isolate.memory.virtual_size", false);
    assert_gauge_point(
        &virt_mem_isolate_points,
        500_000_000.0,
        "starting_isolate",
        "starting_publisher",
    );
    assert_gauge_point(&virt_mem_isolate_points, 500_000_000.0, "ready_isolate", "ready_publisher");

    let private_mem_isolate_points =
        verifier.get_gauge_points("encrypted_zone.enforcer.isolate.memory.private_size", false);
    assert_gauge_point(
        &private_mem_isolate_points,
        50_000_000.0,
        "starting_isolate",
        "starting_publisher",
    );
    assert_gauge_point(
        &private_mem_isolate_points,
        50_000_000.0,
        "ready_isolate",
        "ready_publisher",
    );

    let data_mem_isolate_points =
        verifier.get_gauge_points("encrypted_zone.enforcer.isolate.memory.data_size", false);
    assert_gauge_point(
        &data_mem_isolate_points,
        10_000_000.0,
        "starting_isolate",
        "starting_publisher",
    );
    assert_gauge_point(&data_mem_isolate_points, 10_000_000.0, "ready_isolate", "ready_publisher");

    // Run 2: both exit/crash.
    starting_should_exit.store(true, std::sync::atomic::Ordering::Relaxed);
    ready_should_exit.store(true, std::sync::atomic::Ordering::Relaxed);

    let mut found_cpu_metric = false;
    for _ in 0..5 {
        health_manager.run().await;
        if let Some(ref provider) = providers.safe {
            let _ = provider.force_flush();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let exported_2 = collector.exported_metrics(10, Duration::from_secs(5)).await;
        let verifier_2 = MetricsVerifier::new(exported_2);

        let cpu_points =
            verifier_2.get_gauge_points("encrypted_zone.enforcer.health.system.cpu.usage", true);
        if cpu_points.is_empty() {
            continue;
        }
        found_cpu_metric = true;

        // Verify pre_ready_exit is recorded for Starting isolate
        let pre_ready_exit_points =
            verifier_2.get_counter_points("encrypted_zone.enforcer.health.isolate.pre_ready_exit");
        assert_eq!(pre_ready_exit_points.len(), 1, "Expected exactly 1 pre_ready_exit point");
        let (val, attrs) = &pre_ready_exit_points[0];
        assert!(*val >= 1, "Expected pre_ready_exit count to be >= 1, found: {}", val);
        assert_eq!(attrs.get("ez_isolate_name").map(|v| v.as_str()), Some("starting_isolate"));
        assert_eq!(attrs.get("ez_publisher_id").map(|v| v.as_str()), Some("starting_publisher"));
        assert_eq!(attrs.get("exit_reason").map(|v| v.as_str()), Some("exited"));
        assert_eq!(attrs.get("pre_ready_state").map(|v| v.as_str()), Some("starting"));

        // Verify pre_ready_exit has NO points for Ready isolate
        let has_ready_pre_ready_exit = pre_ready_exit_points.iter().any(|(_, attrs)| {
            attrs.get("ez_isolate_name").map(|v| v.as_str()) == Some("ready_isolate")
        });
        assert!(!has_ready_pre_ready_exit, "Ready isolate should not record pre-ready exit");

        // Verify reset metric is recorded for Ready isolate
        let reset_points = verifier_2.get_counter_points("encrypted_zone.enforcer.reset");
        let found_ready_reset = reset_points.iter().any(|(val, attrs)| {
            *val >= 1 && attrs.get("service").map(|v| v.as_str()) == Some("ready_service")
        });
        assert!(found_ready_reset, "Ready isolate should record reset");

        break;
    }
    assert!(found_cpu_metric, "CPU metric should be recorded after retries");

    if let Some(provider) = providers.safe {
        let _ = provider.shutdown();
    }
}
