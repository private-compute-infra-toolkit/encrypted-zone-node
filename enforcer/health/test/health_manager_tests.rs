// Copyright 2025 Google LLC
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

use container::ContainerRunStatus;
use container_manager_request::{
    ContainerManagerRequest, GetRunStatusResponse, ResetIsolateResponse,
};
use container_manager_requester::ContainerManagerRequester;
use data_scope::request::{AddIsolateRequest, GetIsolateRequest};
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::ez_isolate_health::{
    container_run_status::Status as RunStatus, ContainerRunStatus as ProtoContainerRunStatus,
};
use enforcer_proto::enforcer::v1::{
    EzIsolateHealth, IsolateServiceInfo as ProtoIsolateServiceInfo, IsolateState,
};
use health_manager::{format_isolate_health, isolates_equal, HealthManager};
use health_ops::get_ops_for_state;
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use state_manager::IsolateStateManager;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

async fn add_isolate_with_state(isr: &IsolateStateManager, state: IsolateState) -> IsolateId {
    let binary_services_index = BinaryServicesIndex::new(false);
    let isolate_id = IsolateId::new(binary_services_index);
    let add_isolate_request = AddIsolateRequest {
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::Public,
        isolate_id,
    };
    isr.add_isolate(add_isolate_request).await;
    isr.mark_channel_connected(isolate_id).await.unwrap();
    if state != IsolateState::Starting {
        isr.update_state(isolate_id, state).await.unwrap();
    }
    isolate_id
}

#[tokio::test]
async fn test_health_manager() {
    let _ = logging::logger::setup_logging();
    let (tx, mut rx) = mpsc::channel(1);
    let container_manager_requester = ContainerManagerRequester::new(tx);
    let data_scope_requester = DataScopeRequester::new(0);
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());

    let isolate_id_1 = add_isolate_with_state(&isolate_state_manager, IsolateState::Ready).await;
    let isolate_id_2 = add_isolate_with_state(&isolate_state_manager, IsolateState::Starting).await;
    let isolate_id_3 = add_isolate_with_state(&isolate_state_manager, IsolateState::Ready).await;
    let isolate_id_4 = add_isolate_with_state(&isolate_state_manager, IsolateState::Ready).await;

    let health_manager = HealthManager::new(
        isolate_state_manager,
        container_manager_requester,
        IsolateServiceMapper::default(),
        data_scope_requester,
    );

    let reset_counts = Arc::new(Mutex::new(HashMap::new()));
    let reset_counts_clone = reset_counts.clone();
    tokio::spawn(async move {
        while let Some(request) = rx.recv().await {
            match request {
                ContainerManagerRequest::GetRunStatus { req, resp } => {
                    let status = if req.isolate_id == isolate_id_1 {
                        ContainerRunStatus::Running
                    } else if req.isolate_id == isolate_id_2 {
                        ContainerRunStatus::Exited(1)
                    } else if req.isolate_id == isolate_id_3 {
                        ContainerRunStatus::Signaled(9)
                    } else {
                        ContainerRunStatus::NotFound
                    };
                    let (rss_bytes, peak_rss_bytes, virt_bytes, shared_bytes, data_bytes) =
                        if status == ContainerRunStatus::Running {
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
                    let _ = resp.send(Ok(GetRunStatusResponse {
                        status,
                        rss_bytes,
                        peak_rss_bytes,
                        virt_bytes,
                        shared_bytes,
                        data_bytes,
                        restart_count: if status == ContainerRunStatus::Running { 0 } else { 3 },
                    }));
                }
                ContainerManagerRequest::ResetIsolateRequest { req, resp } => {
                    let mut counts = reset_counts_clone.lock().unwrap();
                    *counts.entry(req.isolate_id).or_insert(0) += 1;
                    let _ = resp.send(Ok(ResetIsolateResponse {}));
                }
                _ => {}
            }
        }
    });

    health_manager.run_in_background(Duration::from_millis(100));
    // This should give the background task time to run once.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let report = health_manager.get_report().await;
    // Make sure the timestamps are as expected.
    assert!(report.end_timestamp >= report.start_timestamp);
    // Make sure the report has the expected Isolates.
    assert_eq!(report.isolates.len(), 4);
    assert!(!report.are_isolates_ready);
    // Make sure the Isolates in the report have expected values.
    let isolates_by_id: HashMap<String, _> =
        report.isolates.into_iter().map(|i| (i.isolate_id.clone(), i)).collect();

    let expected_isolates = vec![
        (
            isolate_id_1,
            IsolateState::Ready,
            RunStatus::Running,
            None,
            None,
            false,
            Some(100_000_000),
            Some(200_000_000),
            Some(500_000_000),
            Some(50_000_000),
            Some(10_000_000),
            Some(0),
        ),
        (
            isolate_id_2,
            IsolateState::Starting,
            RunStatus::Exited,
            Some(1),
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            Some(3),
        ),
        (
            isolate_id_3,
            IsolateState::Ready,
            RunStatus::Signaled,
            None,
            Some(9),
            true,
            None,
            None,
            None,
            None,
            None,
            Some(3),
        ),
        (
            isolate_id_4,
            IsolateState::Ready,
            RunStatus::NotFound,
            None,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            Some(3),
        ),
    ];

    let counts = reset_counts.lock().unwrap();
    for (
        id,
        state,
        status,
        exit_code,
        signal,
        reset_requested,
        rss_memory,
        peak_rss_memory,
        virt_memory,
        shared_memory,
        data_memory,
        restart_count,
    ) in &expected_isolates
    {
        let isolate = isolates_by_id.get(&id.to_string()).unwrap();
        assert_eq!(isolate.state, Some(*state as i32));
        assert_eq!(isolate.container_run_status.as_ref().unwrap().status, *status as i32);
        assert_eq!(isolate.container_run_status.as_ref().unwrap().exit_code, *exit_code);
        assert_eq!(isolate.container_run_status.as_ref().unwrap().signal, *signal);
        assert_eq!(isolate.container_reset_requested, *reset_requested);
        assert_eq!(isolate.container_rss_memory_bytes, *rss_memory);
        assert_eq!(isolate.container_peak_rss_memory_bytes, *peak_rss_memory);
        assert_eq!(isolate.container_virt_memory_bytes, *virt_memory);
        assert_eq!(isolate.container_shared_memory_bytes, *shared_memory);
        assert_eq!(isolate.container_data_memory_bytes, *data_memory);
        assert_eq!(isolate.container_restart_count, *restart_count);
        if *reset_requested {
            assert_eq!(counts.get(id), Some(&1));
        } else {
            assert_eq!(counts.get(id), None);
        }
    }
}

#[tokio::test]
async fn test_get_ops_for_state() {
    let ops_ready = get_ops_for_state(IsolateState::Ready);
    assert_eq!(ops_ready.len(), 1);
    assert_eq!(format!("{:?}", ops_ready[0]), "CheckContainerRunStatus");
    let ops_starting = get_ops_for_state(IsolateState::Starting);
    assert_eq!(ops_starting.len(), 1);
    assert_eq!(format!("{:?}", ops_starting[0]), "CheckContainerRunStatus");
}

#[tokio::test]
async fn test_health_manager_exposes_services() {
    let (tx, mut rx) = mpsc::channel(1);
    let container_manager_requester = ContainerManagerRequester::new(tx);
    let isolate_state_manager =
        IsolateStateManager::new(DataScopeRequester::new(0), container_manager_requester.clone());
    // Setup Mapper with one service
    let mapper = IsolateServiceMapper::default();
    let service_info = IsolateServiceInfo {
        operator_domain: "example.com".to_string(),
        service_name: "myservice".to_string(),
        ..Default::default()
    };
    let binary_index = mapper
        .new_binary_index(
            vec![service_info.clone()],
            false,
            service_info.publisher_id.clone(),
            service_info.isolate_name.clone(),
        )
        .await
        .unwrap();
    let isolate_id = IsolateId::new(binary_index);
    // Add Isolate
    let add_req = AddIsolateRequest {
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::Public,
        isolate_id,
    };
    isolate_state_manager.add_isolate(add_req).await;
    let health_manager = HealthManager::new(
        isolate_state_manager,
        container_manager_requester,
        mapper,
        DataScopeRequester::new(0),
    );
    // Spawn mock container manager
    tokio::spawn(async move {
        while let Some(request) = rx.recv().await {
            if let ContainerManagerRequest::GetRunStatus { req: _, resp } = request {
                let _ = resp.send(Ok(GetRunStatusResponse {
                    status: ContainerRunStatus::Running,
                    rss_bytes: Some(100_000_000),
                    peak_rss_bytes: None,
                    virt_bytes: None,
                    shared_bytes: None,
                    data_bytes: None,
                    restart_count: 0,
                }));
            }
        }
    });
    health_manager.run().await;
    let report = health_manager.get_report().await;
    assert_eq!(report.isolates.len(), 1);
    let health = &report.isolates[0];
    assert_eq!(health.isolate_id, isolate_id.to_string());
    assert_eq!(health.services.len(), 1);
    assert_eq!(health.services[0].operator_domain, "example.com");
    assert_eq!(health.services[0].service_name, "myservice");
}

#[tokio::test]
async fn test_health_report_includes_current_scope() {
    let (tx, mut rx) = mpsc::channel(1);
    let container_manager_requester = ContainerManagerRequester::new(tx);
    let data_scope_requester = DataScopeRequester::new(0);
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let binary_services_index = BinaryServicesIndex::new(false);
    let isolate_id = IsolateId::new(binary_services_index);
    let add_req = AddIsolateRequest {
        current_data_scope_type: DataScopeType::DomainOwned,
        allowed_data_scope_type: DataScopeType::DomainOwned,
        isolate_id,
    };
    isolate_state_manager.add_isolate(add_req).await;
    isolate_state_manager.mark_channel_connected(isolate_id).await.unwrap();
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();
    let health_manager = HealthManager::new(
        isolate_state_manager,
        container_manager_requester,
        IsolateServiceMapper::default(),
        data_scope_requester,
    );
    // Spawn mock container manager
    tokio::spawn(async move {
        while let Some(request) = rx.recv().await {
            if let ContainerManagerRequest::GetRunStatus { req: _, resp } = request {
                let _ = resp.send(Ok(GetRunStatusResponse {
                    status: ContainerRunStatus::Running,
                    rss_bytes: Some(100_000_000),
                    peak_rss_bytes: None,
                    virt_bytes: None,
                    shared_bytes: None,
                    data_bytes: None,
                    restart_count: 0,
                }));
            }
        }
    });
    health_manager.run().await;
    let report = health_manager.get_report().await;
    assert_eq!(report.isolates.len(), 1);
    let health = &report.isolates[0];
    assert_eq!(health.isolate_id, isolate_id.to_string());
    assert_eq!(health.current_scope, Some(DataScopeType::DomainOwned as i32));
    assert_eq!(health.sensitive_session_count, Some(0));
}

#[tokio::test]
async fn test_health_report_includes_sensitive_session_count() {
    let (tx, mut rx) = mpsc::channel(1);
    let container_manager_requester = ContainerManagerRequester::new(tx);
    let data_scope_requester = DataScopeRequester::new(5);
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let binary_services_index = BinaryServicesIndex::new(false);
    let isolate_id = IsolateId::new(binary_services_index);
    let add_req = AddIsolateRequest {
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::UserPrivate,
        isolate_id,
    };
    isolate_state_manager.add_isolate(add_req).await;
    isolate_state_manager.mark_channel_connected(isolate_id).await.unwrap();
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

    // Trigger sensitive session
    let get_req =
        GetIsolateRequest { binary_services_index, data_scope_type: DataScopeType::UserPrivate };
    let get_resp = data_scope_requester.get_isolate(get_req).await.unwrap();
    assert_eq!(get_resp.isolate_id, isolate_id);

    let health_manager = HealthManager::new(
        isolate_state_manager,
        container_manager_requester,
        IsolateServiceMapper::default(),
        data_scope_requester,
    );
    // Spawn mock container manager
    tokio::spawn(async move {
        while let Some(request) = rx.recv().await {
            if let ContainerManagerRequest::GetRunStatus { req: _, resp } = request {
                let _ = resp.send(Ok(GetRunStatusResponse {
                    status: ContainerRunStatus::Running,
                    rss_bytes: Some(100_000_000),
                    peak_rss_bytes: None,
                    virt_bytes: None,
                    shared_bytes: None,
                    data_bytes: None,
                    restart_count: 0,
                }));
            }
        }
    });
    health_manager.run().await;
    let report = health_manager.get_report().await;
    assert_eq!(report.isolates.len(), 1);
    let health = &report.isolates[0];
    assert_eq!(health.isolate_id, isolate_id.to_string());
    assert_eq!(health.current_scope, Some(DataScopeType::UserPrivate as i32));
    assert_eq!(health.sensitive_session_count, Some(1));
}

#[tokio::test]
async fn test_health_manager_are_isolates_ready_conditions() {
    let (tx, mut rx) = mpsc::channel(1);
    let container_manager_requester = ContainerManagerRequester::new(tx);
    let data_scope_requester = DataScopeRequester::new(0);
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());

    let mapper = IsolateServiceMapper::default();
    let service_info_1 = IsolateServiceInfo {
        operator_domain: "example.com".to_string(),
        service_name: "service1".to_string(),
        publisher_id: "publisher1".to_string(),
        isolate_name: "isolate1".to_string(),
    };
    let binary_index_1 = mapper
        .new_binary_index(
            vec![service_info_1],
            false,
            "publisher1".to_string(),
            "isolate1".to_string(),
        )
        .await
        .unwrap();
    let isolate_id_1a = IsolateId::new(binary_index_1);
    let isolate_id_1b = IsolateId::new(binary_index_1);

    let service_info_2 = IsolateServiceInfo {
        operator_domain: "example.com".to_string(),
        service_name: "service2".to_string(),
        publisher_id: "publisher2".to_string(),
        isolate_name: "isolate2".to_string(),
    };
    let binary_index_2 = mapper
        .new_binary_index(
            vec![service_info_2],
            false,
            "publisher2".to_string(),
            "isolate2".to_string(),
        )
        .await
        .unwrap();
    let isolate_id_2a = IsolateId::new(binary_index_2);
    let isolate_id_2b = IsolateId::new(binary_index_2);

    let health_manager = HealthManager::new(
        isolate_state_manager.clone(),
        container_manager_requester,
        mapper.clone(),
        data_scope_requester.clone(),
    );

    let container_statuses = Arc::new(Mutex::new(HashMap::<IsolateId, ContainerRunStatus>::new()));
    let container_statuses_clone = container_statuses.clone();

    // Mock container manager
    tokio::spawn(async move {
        while let Some(request) = rx.recv().await {
            match request {
                ContainerManagerRequest::GetRunStatus { req, resp } => {
                    let status = container_statuses_clone
                        .lock()
                        .unwrap()
                        .get(&req.isolate_id)
                        .cloned()
                        .unwrap_or(ContainerRunStatus::Running);
                    let (rss_bytes, peak_rss_bytes, virt_bytes, shared_bytes, data_bytes) =
                        if status == ContainerRunStatus::Running {
                            (Some(100_000_000), None, None, None, None)
                        } else {
                            (None, None, None, None, None)
                        };
                    let _ = resp.send(Ok(GetRunStatusResponse {
                        status,
                        rss_bytes,
                        peak_rss_bytes,
                        virt_bytes,
                        shared_bytes,
                        data_bytes,
                        restart_count: if status == ContainerRunStatus::Running { 0 } else { 1 },
                    }));
                }
                ContainerManagerRequest::ResetIsolateRequest { req: _, resp } => {
                    let _ = resp.send(Ok(ResetIsolateResponse {}));
                }
                _ => {}
            }
        }
    });

    // 1. Condition: Empty registered binary indices -> are_isolates_ready is false even if isolates are registered
    {
        let (empty_tx, _empty_rx) = mpsc::channel(1);
        let empty_cm_req = ContainerManagerRequester::new(empty_tx);
        let empty_ism =
            IsolateStateManager::new(data_scope_requester.clone(), empty_cm_req.clone());
        let empty_hm = HealthManager::new(
            empty_ism.clone(),
            empty_cm_req,
            IsolateServiceMapper::default(),
            data_scope_requester.clone(),
        );
        empty_ism.set_isolates_registered();
        empty_hm.run().await;
        assert!(!empty_hm.get_report().await.are_isolates_ready);
    }

    // 2. Condition: Isolates not registered and no isolates added -> false
    health_manager.run().await;
    assert!(!health_manager.get_report().await.are_isolates_ready);

    // --- Build up to fully ready baseline ---
    // Add isolate_1a for binary_index_1: Ready and Running
    isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: isolate_id_1a,
        })
        .await;
    isolate_state_manager.mark_channel_connected(isolate_id_1a).await.unwrap();
    isolate_state_manager.update_state(isolate_id_1a, IsolateState::Ready).await.unwrap();

    // Add isolate_2a for binary_index_2: Ready and Running
    isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: isolate_id_2a,
        })
        .await;
    isolate_state_manager.mark_channel_connected(isolate_id_2a).await.unwrap();
    isolate_state_manager.update_state(isolate_id_2a, IsolateState::Ready).await.unwrap();

    // Still not ready because isolates are not registered
    health_manager.run().await;
    assert!(!health_manager.get_report().await.are_isolates_ready);

    // Set isolates_registered -> Baseline Ready State reached!
    isolate_state_manager.set_isolates_registered();
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);

    // --- Test each condition independently by mutating from ready -> verifying false -> restoring -> verifying true ---

    // 3. Condition: Container run status must be Running (testing Exited, Signaled, NotFound)
    container_statuses.lock().unwrap().insert(isolate_id_1a, ContainerRunStatus::Exited(1));
    health_manager.run().await;
    assert!(!health_manager.get_report().await.are_isolates_ready);
    container_statuses.lock().unwrap().insert(isolate_id_1a, ContainerRunStatus::Running);
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);

    container_statuses.lock().unwrap().insert(isolate_id_1a, ContainerRunStatus::Signaled(9));
    health_manager.run().await;
    assert!(!health_manager.get_report().await.are_isolates_ready);
    container_statuses.lock().unwrap().insert(isolate_id_1a, ContainerRunStatus::Running);
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);

    container_statuses.lock().unwrap().insert(isolate_id_1a, ContainerRunStatus::NotFound);
    health_manager.run().await;
    assert!(!health_manager.get_report().await.are_isolates_ready);
    container_statuses.lock().unwrap().insert(isolate_id_1a, ContainerRunStatus::Running);
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);

    // 4. Condition: Isolate state must be Ready (testing Retiring and Starting)
    isolate_state_manager.update_state(isolate_id_2a, IsolateState::Retiring).await.unwrap();
    health_manager.run().await;
    assert!(!health_manager.get_report().await.are_isolates_ready);

    // Adding isolate_2b in Starting state does not yet make it ready
    isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: isolate_id_2b,
        })
        .await;
    health_manager.run().await;
    assert!(!health_manager.get_report().await.are_isolates_ready);

    // Update isolate_2b to Ready -> system returns to ready
    isolate_state_manager.mark_channel_connected(isolate_id_2b).await.unwrap();
    isolate_state_manager.update_state(isolate_id_2b, IsolateState::Ready).await.unwrap();
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);

    // 5. Condition: Multi-type requirement (all registered binary indices must be satisfied)
    // Failing isolate_2b leaves binary_index_2 unsatisfied
    container_statuses.lock().unwrap().insert(isolate_id_2b, ContainerRunStatus::Exited(1));
    health_manager.run().await;
    assert!(!health_manager.get_report().await.are_isolates_ready);
    container_statuses.lock().unwrap().insert(isolate_id_2b, ContainerRunStatus::Running);
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);

    // 6. Condition: Multi-replica redundancy for the same binary index
    // Add isolate_1b in Ready and Running state for binary_index_1
    isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: isolate_id_1b,
        })
        .await;
    isolate_state_manager.mark_channel_connected(isolate_id_1b).await.unwrap();
    isolate_state_manager.update_state(isolate_id_1b, IsolateState::Ready).await.unwrap();
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);

    // If isolate_1a fails (Exited), the manager remains ready because isolate_1b is ready
    container_statuses.lock().unwrap().insert(isolate_id_1a, ContainerRunStatus::Exited(137));
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);

    // If isolate_1b ALSO fails (Exited), now binary_index_1 has no ready replica -> false
    container_statuses.lock().unwrap().insert(isolate_id_1b, ContainerRunStatus::Exited(1));
    health_manager.run().await;
    assert!(!health_manager.get_report().await.are_isolates_ready);

    // Restoring isolate_1b makes it ready again
    container_statuses.lock().unwrap().insert(isolate_id_1b, ContainerRunStatus::Running);
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);

    // Restoring isolate_1a as well
    container_statuses.lock().unwrap().insert(isolate_id_1a, ContainerRunStatus::Running);
    health_manager.run().await;
    assert!(health_manager.get_report().await.are_isolates_ready);
}

#[test]
fn test_isolates_equal() {
    let isolate1 = EzIsolateHealth {
        isolate_id: "iso1".to_string(),
        state: Some(IsolateState::Ready as i32),
        container_rss_memory_bytes: Some(100),
        container_peak_rss_memory_bytes: Some(200),
        ..Default::default()
    };

    let mut isolate2 = isolate1.clone();
    // Varying memory bytes, restart count, and sensitive session count should still be considered equal
    isolate2.container_rss_memory_bytes = Some(150);
    isolate2.container_peak_rss_memory_bytes = Some(250);
    isolate2.container_virt_memory_bytes = Some(500);
    isolate2.container_shared_memory_bytes = Some(50);
    isolate2.container_data_memory_bytes = Some(10);
    isolate2.container_restart_count = Some(5);
    isolate2.sensitive_session_count = Some(3);

    assert!(isolates_equal(std::slice::from_ref(&isolate1), std::slice::from_ref(&isolate2)));

    // Changing state should make them not equal
    let mut isolate3 = isolate1.clone();
    isolate3.state = Some(IsolateState::Starting as i32);

    assert!(!isolates_equal(std::slice::from_ref(&isolate1), std::slice::from_ref(&isolate3)));
}

#[test]
fn test_isolates_equal_field_variations() {
    let base = EzIsolateHealth {
        isolate_id: "iso1".to_string(),
        state: Some(IsolateState::Ready as i32),
        container_run_status: Some(ProtoContainerRunStatus {
            status: RunStatus::Running as i32,
            exit_code: None,
            signal: None,
        }),
        container_reset_requested: false,
        container_rss_memory_bytes: Some(100),
        services: vec![ProtoIsolateServiceInfo {
            service_name: "svc".to_string(),
            operator_domain: "example.com".to_string(),
            publisher_id: "pub".to_string(),
            isolate_name: "iso".to_string(),
        }],
        current_scope: Some(DataScopeType::Public as i32),
        container_restart_count: Some(0),
        ..Default::default()
    };

    // Different lengths
    assert!(!isolates_equal(&[], std::slice::from_ref(&base)));
    assert!(!isolates_equal(&[base.clone(), base.clone()], std::slice::from_ref(&base)));

    // Different isolate_id
    let mut diff_id = base.clone();
    diff_id.isolate_id = "iso2".to_string();
    assert!(!isolates_equal(std::slice::from_ref(&base), std::slice::from_ref(&diff_id)));

    // Different run status
    let mut diff_status = base.clone();
    diff_status.container_run_status = Some(ProtoContainerRunStatus {
        status: RunStatus::Exited as i32,
        exit_code: Some(1),
        signal: None,
    });
    assert!(!isolates_equal(std::slice::from_ref(&base), std::slice::from_ref(&diff_status)));

    // Different reset requested
    let mut diff_reset = base.clone();
    diff_reset.container_reset_requested = true;
    assert!(!isolates_equal(std::slice::from_ref(&base), std::slice::from_ref(&diff_reset)));

    // Different services
    let mut diff_services = base.clone();
    diff_services.services.clear();
    assert!(!isolates_equal(std::slice::from_ref(&base), std::slice::from_ref(&diff_services)));

    // Different scope
    let mut diff_scope = base.clone();
    diff_scope.current_scope = Some(DataScopeType::DomainOwned as i32);
    assert!(!isolates_equal(std::slice::from_ref(&base), std::slice::from_ref(&diff_scope)));

    // Varying restart count should still be equal
    let mut diff_restarts = base.clone();
    diff_restarts.container_restart_count = Some(2);
    assert!(isolates_equal(std::slice::from_ref(&base), std::slice::from_ref(&diff_restarts)));

    // Varying sensitive session count should still be equal
    let mut diff_sensitive = base.clone();
    diff_sensitive.sensitive_session_count = Some(1);
    assert!(isolates_equal(std::slice::from_ref(&base), std::slice::from_ref(&diff_sensitive)));
}

#[test]
fn test_format_isolate_health() {
    // Default / Minimal isolate
    let minimal = EzIsolateHealth { isolate_id: "iso1".to_string(), ..Default::default() };
    assert_eq!(
        format_isolate_health(&minimal),
        "iso1 | state: Unknown | container: None | scope: Unspecified | services: [none]"
    );

    // Ready isolate with running container and public scope
    let ready = EzIsolateHealth {
        isolate_id: "iso2".to_string(),
        state: Some(IsolateState::Ready as i32),
        container_run_status: Some(ProtoContainerRunStatus {
            status: RunStatus::Running as i32,
            exit_code: None,
            signal: None,
        }),
        current_scope: Some(DataScopeType::Public as i32),
        services: vec![ProtoIsolateServiceInfo {
            service_name: "myservice".to_string(),
            operator_domain: "example.com".to_string(),
            publisher_id: "pub1".to_string(),
            isolate_name: "iso_name".to_string(),
        }],
        container_reset_requested: false,
        ..Default::default()
    };
    assert_eq!(
        format_isolate_health(&ready),
        "iso2 | state: Ready | container: Running | scope: Public | services: [myservice (operator_domain: example.com, publisher_id: pub1, isolate_name: iso_name)]"
    );

    // Exited isolate with exit code, restarts, reset requested, multiple services
    let exited = EzIsolateHealth {
        isolate_id: "iso3".to_string(),
        state: Some(IsolateState::Starting as i32),
        container_run_status: Some(ProtoContainerRunStatus {
            status: RunStatus::Exited as i32,
            exit_code: Some(137),
            signal: None,
        }),
        container_restart_count: Some(3),
        container_reset_requested: true,
        current_scope: Some(DataScopeType::DomainOwned as i32),
        services: vec![
            ProtoIsolateServiceInfo {
                service_name: "svc1".to_string(),
                operator_domain: "d1.com".to_string(),
                publisher_id: "p1".to_string(),
                isolate_name: "in1".to_string(),
            },
            ProtoIsolateServiceInfo {
                service_name: "svc2".to_string(),
                operator_domain: "d2.com".to_string(),
                publisher_id: "p2".to_string(),
                isolate_name: "in2".to_string(),
            },
        ],
        ..Default::default()
    };
    assert_eq!(
        format_isolate_health(&exited),
        "iso3 | state: Starting | container: Exited(137) (restarts: 3) | scope: DomainOwned | services: [svc1 (operator_domain: d1.com, publisher_id: p1, isolate_name: in1); svc2 (operator_domain: d2.com, publisher_id: p2, isolate_name: in2)] | reset_requested: true"
    );

    // Signaled isolate
    let signaled = EzIsolateHealth {
        isolate_id: "iso4".to_string(),
        state: Some(IsolateState::Retiring as i32),
        container_run_status: Some(ProtoContainerRunStatus {
            status: RunStatus::Signaled as i32,
            exit_code: None,
            signal: Some(9),
        }),
        ..Default::default()
    };
    assert_eq!(
        format_isolate_health(&signaled),
        "iso4 | state: Retiring | container: Signaled(9) | scope: Unspecified | services: [none]"
    );

    // Sensitive isolate with sensitive sessions
    let sensitive = EzIsolateHealth {
        isolate_id: "iso5".to_string(),
        state: Some(IsolateState::Ready as i32),
        container_run_status: Some(ProtoContainerRunStatus {
            status: RunStatus::Running as i32,
            exit_code: None,
            signal: None,
        }),
        current_scope: Some(DataScopeType::UserPrivate as i32),
        sensitive_session_count: Some(2),
        ..Default::default()
    };
    assert_eq!(
        format_isolate_health(&sensitive),
        "iso5 | state: Ready | container: Running | scope: UserPrivate (sensitive_sessions: 2) | services: [none]"
    );
}
