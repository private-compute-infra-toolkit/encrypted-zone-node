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
use data_scope::request::AddIsolateRequest;
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::ez_isolate_health::container_run_status::Status as RunStatus;
use enforcer_proto::enforcer::v1::IsolateState;
use health_manager::HealthManager;
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
                    let _ = resp.send(Ok(GetRunStatusResponse { status }));
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
    // Make sure the Isolates in the report have expected values.
    let isolates_by_id: HashMap<String, _> =
        report.isolates.into_iter().map(|i| (i.isolate_id.clone(), i)).collect();

    let expected_isolates = vec![
        (isolate_id_1, IsolateState::Ready, RunStatus::Running, None, None, false),
        (isolate_id_2, IsolateState::Starting, RunStatus::Exited, Some(1), None, true),
        (isolate_id_3, IsolateState::Ready, RunStatus::Signaled, None, Some(9), true),
        (isolate_id_4, IsolateState::Ready, RunStatus::NotFound, None, None, true),
    ];

    let counts = reset_counts.lock().unwrap();
    for (id, state, status, exit_code, signal, reset_requested) in &expected_isolates {
        let isolate = isolates_by_id.get(&id.to_string()).unwrap();
        assert_eq!(isolate.state, Some(*state as i32));
        assert_eq!(isolate.container_run_status.as_ref().unwrap().status, *status as i32);
        assert_eq!(isolate.container_run_status.as_ref().unwrap().exit_code, *exit_code);
        assert_eq!(isolate.container_run_status.as_ref().unwrap().signal, *signal);
        assert_eq!(isolate.container_reset_requested, *reset_requested);
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
    };
    let binary_index = mapper.new_binary_index(vec![service_info], false).await.unwrap();
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
                let _ = resp.send(Ok(GetRunStatusResponse { status: ContainerRunStatus::Running }));
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
                let _ = resp.send(Ok(GetRunStatusResponse { status: ContainerRunStatus::Running }));
            }
        }
    });
    health_manager.run().await;
    let report = health_manager.get_report().await;
    assert_eq!(report.isolates.len(), 1);
    let health = &report.isolates[0];
    assert_eq!(health.isolate_id, isolate_id.to_string());
    assert_eq!(health.current_scope, Some(DataScopeType::DomainOwned as i32));
}
