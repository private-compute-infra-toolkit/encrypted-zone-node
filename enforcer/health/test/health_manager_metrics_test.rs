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

    let binary_index = mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: "test_domain".to_string(),
                service_name: "test_service".to_string(),
                ..Default::default()
            }],
            false,
        )
        .await
        .expect("Failed to add binary index");

    let isolate_id = isolate_info::IsolateId::new(binary_index);

    let add_isolate_req = AddIsolateRequest {
        isolate_id,
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::Public,
    };
    ism.add_isolate(add_isolate_req).await;
    ism.update_state(isolate_id, IsolateState::Ready).await.expect("Failed to update state");

    let health_manager = HealthManager::new(ism.clone(), cm_req, mapper.clone(), ds_req);

    let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let should_exit_clone = should_exit.clone();

    tokio::spawn(async move {
        while let Some(req) = cm_rx.recv().await {
            match req {
                ContainerManagerRequest::GetRunStatus { resp, .. } => {
                    let status = if should_exit_clone.load(std::sync::atomic::Ordering::Relaxed) {
                        ContainerRunStatusEnum::Exited(1)
                    } else {
                        ContainerRunStatusEnum::Running
                    };
                    let _ =
                        resp.send(Ok(container_manager_request::GetRunStatusResponse { status }));
                }
                ContainerManagerRequest::ResetIsolateRequest { resp, .. } => {
                    let _ = resp.send(Ok(container_manager_request::ResetIsolateResponse {}));
                }
                _ => {}
            }
        }
    });

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
    assert_eq!(state_points[0].0, 7.0, "State should be 7 (Ready)");

    // Start Scenario 2
    should_exit.store(true, std::sync::atomic::Ordering::Relaxed);

    let mut found_cpu_metric = false;
    // Retry up to 5 times to allow for system time difference to be significant enough for CPU usage calculation
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

        let found_cpu = cpu_points.iter().any(|(val, _)| *val >= 0.0);
        assert!(found_cpu, "CPU percent should be >= 0.0. Found: {:?}", cpu_points);

        let container_status_points_2 = verifier_2
            .get_gauge_points("encrypted_zone.enforcer.health.isolate.container_run_status", false);
        assert!(
            !container_status_points_2.is_empty(),
            "Expected enforcer.health.isolate.container_run_status metric"
        );
        let found_container_exited = container_status_points_2.iter().any(|(val, _)| *val == 3.0);
        assert!(
            found_container_exited,
            "Container run status should eventually be 3 (Exited). Found: {:?}",
            container_status_points_2
        );

        let state_points_2 =
            verifier_2.get_gauge_points("encrypted_zone.enforcer.health.isolate.state", false);
        assert!(
            !state_points_2.is_empty(),
            "Expected encrypted_zone.enforcer.health.isolate.state metric"
        );
        // State remains Ready (7) as HealthManager doesn't update IsolateState on container exit.
        let found_state_ready = state_points_2.iter().any(|(val, _)| *val == 7.0);
        assert!(found_state_ready, "State should remain 7 (Ready). Found: {:?}", state_points_2);

        let reset_points = verifier_2.get_counter_points("encrypted_zone.enforcer.reset");
        assert!(!reset_points.is_empty(), "Expected encrypted_zone.enforcer.reset metric");
        let found_reset = reset_points.iter().any(|(val, _)| *val >= 1);
        assert!(
            found_reset,
            "Reset count should be >= 1 after a container mismatch. Found: {:?}",
            reset_points
        );
        let fd_points = verifier_2
            .get_gauge_points("encrypted_zone.enforcer.health.system.file_descriptors", false);
        assert!(
            !fd_points.is_empty(),
            "Expected encrypted_zone.enforcer.health.system.file_descriptors metric"
        );
        let found_fd = fd_points.iter().any(|(val, _)| *val > 0.0);
        assert!(found_fd, "FD count should be > 0. Found: {:?}", fd_points);

        let mem_points =
            verifier_2.get_gauge_points("encrypted_zone.enforcer.health.system.memory_rss", false);
        assert!(
            !mem_points.is_empty(),
            "Expected encrypted_zone.enforcer.health.system.memory_rss metric"
        );
        let found_mem = mem_points.iter().any(|(val, _)| *val > 0.0);
        assert!(found_mem, "Memory RSS should be > 0. Found: {:?}", mem_points);

        break;
    }
    assert!(
        found_cpu_metric,
        "Expected encrypted_zone.enforcer.health.system.cpu.usage metric after retries"
    );

    if let Some(provider) = providers.safe {
        let _ = provider.shutdown();
    }
}
