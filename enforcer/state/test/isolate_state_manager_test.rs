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

use container_manager_request::{ContainerManagerRequest, ResetIsolateResponse};
use container_manager_requester::ContainerManagerRequester;
use data_scope::request::{
    AddIsolateRequest, FreezeIsolateScopeRequest, FreezeIsolateScopeResponse, GetIsolateRequest,
    RemoveIsolateRequest,
};
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::IsolateState;
use isolate_info::{BinaryServicesIndex, IsolateId};
use once_cell::sync::Lazy;
use state_manager::{IsolateStateManager, IsolateStateManagerError};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

static TEST_BINARY_SERVICES_INDEX: Lazy<BinaryServicesIndex> =
    Lazy::new(|| BinaryServicesIndex::new(false));

struct TestHarness {
    state_manager: IsolateStateManager,
    data_scope_requester: DataScopeRequester,
    isolate_id: IsolateId,
    container_manager_request_rx: Option<Receiver<ContainerManagerRequest>>,
}

impl TestHarness {
    async fn new() -> Self {
        let data_scope_requester = DataScopeRequester::new(u64::MAX);
        let (container_manager_request_tx, container_manager_request_rx) =
            tokio::sync::mpsc::channel(128);
        let container_manager_requester =
            ContainerManagerRequester::new(container_manager_request_tx);
        let state_manager =
            IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester);
        let isolate_id = IsolateId::new(*TEST_BINARY_SERVICES_INDEX);
        state_manager.add_isolate(create_add_isolate_request(isolate_id)).await;
        Self {
            state_manager,
            data_scope_requester,
            isolate_id,
            container_manager_request_rx: Some(container_manager_request_rx),
        }
    }

    async fn advance_to_state(&mut self, state: IsolateState) {
        if state == IsolateState::Ready
            || state == IsolateState::Retiring
            || state == IsolateState::Idle
        {
            self.state_manager.update_state(self.isolate_id, IsolateState::Ready).await.unwrap();
            self.data_scope_requester.get_isolate(create_get_isolate_request()).await.ok();
        }
        if state == IsolateState::Retiring || state == IsolateState::Idle {
            if state == IsolateState::Idle {
                let mut rx = self.container_manager_request_rx.take().unwrap();
                let listener = tokio::spawn(async move {
                    if let Some(ContainerManagerRequest::ResetIsolateRequest { resp, .. }) =
                        rx.recv().await
                    {
                        let _ = resp.send(Ok(ResetIsolateResponse {}));
                    }
                });
                self.state_manager
                    .update_state(self.isolate_id, IsolateState::Retiring)
                    .await
                    .unwrap();
                self.state_manager.increment_inflight_counter(self.isolate_id).await;
                self.state_manager.decrement_inflight_counter(self.isolate_id).await;
                listener.await.unwrap();
            } else {
                self.state_manager
                    .update_state(self.isolate_id, IsolateState::Retiring)
                    .await
                    .unwrap();
            }
        }
    }

    fn spawn_reset_listener(&mut self) -> JoinHandle<()> {
        let mut rx = self.container_manager_request_rx.take().unwrap();
        tokio::spawn(async move {
            if let Some(ContainerManagerRequest::ResetIsolateRequest { resp, .. }) = rx.recv().await
            {
                let _ = resp.send(Ok(ResetIsolateResponse {}));
            } else {
                panic!("Did not receive reset request");
            }
        })
    }
}

#[tokio::test]
async fn test_isolate_not_in_datascope_before_ready() -> Result<(), Box<dyn std::error::Error>> {
    let harness = TestHarness::new().await;

    assert!(
        harness.data_scope_requester.get_isolate(create_get_isolate_request()).await.is_err(),
        "get_isolate should fail before isolate is ready"
    );

    Ok(())
}

#[tokio::test]
async fn test_isolate_added_to_datascope_when_ready() -> Result<(), Box<dyn std::error::Error>> {
    let harness = TestHarness::new().await;

    harness.state_manager.update_state(harness.isolate_id, IsolateState::Ready).await?;

    let get_isolate_result =
        harness.data_scope_requester.get_isolate(create_get_isolate_request()).await?;
    assert_eq!(get_isolate_result.isolate_id, harness.isolate_id);

    Ok(())
}

#[tokio::test]
async fn test_valid_transition_starting_to_ready() -> Result<(), Box<dyn std::error::Error>> {
    let harness = TestHarness::new().await;

    let result = harness.state_manager.update_state(harness.isolate_id, IsolateState::Ready).await;

    assert!(result.is_ok(), "Transition Starting -> Ready should be valid");
    Ok(())
}

#[tokio::test]
async fn test_invalid_transition_ready_to_starting() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Ready).await;

    let result =
        harness.state_manager.update_state(harness.isolate_id, IsolateState::Starting).await;

    assert!(result.is_err(), "Transition Ready -> Starting should be invalid");
    assert!(matches!(
        result.unwrap_err().downcast_ref::<IsolateStateManagerError>(),
        Some(IsolateStateManagerError::InvalidStateTransition)
    ));
    Ok(())
}

#[tokio::test]
async fn test_inflight_decrement_does_not_idle_if_not_retiring(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Ready).await;

    harness.state_manager.increment_inflight_counter(harness.isolate_id).await;
    harness.state_manager.decrement_inflight_counter(harness.isolate_id).await;

    let result =
        harness.state_manager.update_state(harness.isolate_id, IsolateState::Retiring).await;
    assert!(
        result.is_ok(),
        "State should not have changed to Idle, so Retiring transition should be valid"
    );
    Ok(())
}

#[tokio::test]
async fn test_final_inflight_decrement_triggers_idle_state(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Retiring).await;
    harness.state_manager.increment_inflight_counter(harness.isolate_id).await;

    let listener_task = harness.spawn_reset_listener();

    harness.state_manager.decrement_inflight_counter(harness.isolate_id).await;

    let result = harness.state_manager.update_state(harness.isolate_id, IsolateState::Idle).await;
    assert!(result.is_err(), "Transition from auto-Idle -> Idle should fail");
    assert!(matches!(
        result.unwrap_err().downcast_ref::<IsolateStateManagerError>(),
        Some(IsolateStateManagerError::DuplicateStateUpdate)
    ));

    listener_task.await?;
    Ok(())
}

#[tokio::test]
async fn test_remove_isolate_cleans_up_all_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Ready).await;

    let remove_req = RemoveIsolateRequest { isolate_id: harness.isolate_id };
    harness.state_manager.remove_isolate(remove_req).await?;

    let result = harness.state_manager.update_state(harness.isolate_id, IsolateState::Ready).await;
    assert!(result.is_err(), "Updating a removed isolate should fail");
    assert!(
        result.unwrap_err().to_string().contains("Unrecognized IsolateId"),
        "Error should indicate isolate is unrecognized"
    );

    assert!(
        harness.data_scope_requester.get_isolate(create_get_isolate_request()).await.is_err(),
        "get_isolate should fail after isolate is removed"
    );

    Ok(())
}

#[tokio::test]
async fn test_freeze_scope_succeeds_for_unready_isolate() -> Result<(), Box<dyn std::error::Error>>
{
    let harness = TestHarness::new().await;

    let freeze_req = FreezeIsolateScopeRequest { isolate_id: harness.isolate_id };
    let result = harness.state_manager.freeze_scope(freeze_req).await;

    assert!(result.is_ok());

    let update_result =
        harness.state_manager.update_state(harness.isolate_id, IsolateState::Ready).await;
    assert!(update_result.is_ok());

    assert!(harness.data_scope_requester.get_isolate(create_get_isolate_request()).await.is_ok());

    Ok(())
}

#[tokio::test]
async fn test_freeze_scope_succeeds_for_ready_isolate() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Ready).await;

    let freeze_req = FreezeIsolateScopeRequest { isolate_id: harness.isolate_id };
    let result: Result<FreezeIsolateScopeResponse, _> =
        harness.state_manager.freeze_scope(freeze_req).await;

    assert!(result.is_ok());

    Ok(())
}

#[tokio::test]
async fn test_valid_transition_ready_to_idle() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Ready).await;

    let listener_task = harness.spawn_reset_listener();

    let result = harness.state_manager.update_state(harness.isolate_id, IsolateState::Idle).await;

    listener_task.await?;

    assert!(result.is_ok(), "Transition Ready -> Idle should be valid");
    Ok(())
}

#[tokio::test]
async fn test_valid_transition_retiring_to_idle() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Retiring).await;

    assert!(
        harness.data_scope_requester.get_isolate(create_get_isolate_request()).await.is_err(),
        "get_isolate should fail after isolate is retiring"
    );

    let listener_task = harness.spawn_reset_listener();

    harness.state_manager.increment_inflight_counter(harness.isolate_id).await;
    // This should trigger the Isolate reset
    harness.state_manager.decrement_inflight_counter(harness.isolate_id).await;

    listener_task.await?;

    let remove_req = RemoveIsolateRequest { isolate_id: harness.isolate_id };
    assert!(
        harness.state_manager.remove_isolate(remove_req).await.is_ok(),
        "remove_isolate should not fail even if isolate was already removed from data scope"
    );
    Ok(())
}

#[tokio::test]
async fn test_invalid_transition_starting_to_idle() -> Result<(), Box<dyn std::error::Error>> {
    let harness = TestHarness::new().await;

    let result = harness.state_manager.update_state(harness.isolate_id, IsolateState::Idle).await;

    assert!(result.is_err(), "Transition Starting -> Idle should be invalid");
    assert!(matches!(
        result.unwrap_err().downcast_ref::<IsolateStateManagerError>(),
        Some(IsolateStateManagerError::InvalidStateTransition)
    ));

    Ok(())
}

#[tokio::test]
async fn test_error_on_duplicate_state_update() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Ready).await;

    let result = harness.state_manager.update_state(harness.isolate_id, IsolateState::Ready).await;

    assert!(result.is_err(), "Duplicate state update should result in an error");
    assert!(matches!(
        result.unwrap_err().downcast_ref::<IsolateStateManagerError>(),
        Some(IsolateStateManagerError::DuplicateStateUpdate)
    ));
    Ok(())
}

#[tokio::test]
async fn test_error_on_update_for_non_existent_isolate() -> Result<(), Box<dyn std::error::Error>> {
    let harness = TestHarness::new().await;
    let unknown_isolate_id = IsolateId::new(*TEST_BINARY_SERVICES_INDEX);

    let result = harness.state_manager.update_state(unknown_isolate_id, IsolateState::Ready).await;

    assert!(result.is_err(), "Updating a non-existent isolate should fail");
    assert!(result.unwrap_err().to_string().contains("Unrecognized IsolateId"));
    Ok(())
}

#[tokio::test]
async fn test_intermediate_inflight_decrement_does_not_trigger_idle(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Retiring).await;

    harness.state_manager.increment_inflight_counter(harness.isolate_id).await;
    harness.state_manager.increment_inflight_counter(harness.isolate_id).await;

    harness.state_manager.decrement_inflight_counter(harness.isolate_id).await;

    let listener_task = harness.spawn_reset_listener();

    let result = harness.state_manager.update_state(harness.isolate_id, IsolateState::Idle).await;

    listener_task.await?;

    assert!(
        result.is_ok(),
        "Manual transition to Idle should succeed because auto-transition has not happened yet"
    );

    Ok(())
}

#[tokio::test]
async fn test_concurrent_increments_are_handled_safely() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Ready).await;

    let state_manager = harness.state_manager.clone();
    let isolate_id = harness.isolate_id;
    let mut tasks = Vec::new();

    let num_concurrent_tasks = 10;
    for _ in 0..num_concurrent_tasks {
        let sm_clone = state_manager.clone();
        let task = tokio::spawn(async move {
            sm_clone.increment_inflight_counter(isolate_id).await;
        });
        tasks.push(task);
    }

    for task in tasks {
        task.await?;
    }

    let listener_task = harness.spawn_reset_listener();

    harness.state_manager.update_state(isolate_id, IsolateState::Retiring).await?;

    for _ in 0..num_concurrent_tasks {
        harness.state_manager.decrement_inflight_counter(isolate_id).await;
    }

    let result = harness.state_manager.update_state(isolate_id, IsolateState::Idle).await;
    assert!(result.is_err(), "Transition to Idle should fail as it's already Idle");
    assert!(matches!(
        result.unwrap_err().downcast_ref::<IsolateStateManagerError>(),
        Some(IsolateStateManagerError::DuplicateStateUpdate)
    ));

    listener_task.await?;
    Ok(())
}

#[tokio::test]
async fn test_update_to_idle_triggers_container_reset() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Ready).await;

    let mut rx = harness.container_manager_request_rx.take().unwrap();
    let isolate_id = harness.isolate_id;
    let task = tokio::spawn(async move {
        match rx.recv().await {
            Some(ContainerManagerRequest::ResetIsolateRequest { req, resp }) => {
                assert_eq!(req.isolate_id, isolate_id);
                assert_eq!(req.isolate_id.get_binary_services_index(), *TEST_BINARY_SERVICES_INDEX);
                let _ = resp.send(Ok(ResetIsolateResponse {}));
            }
            Some(_) => panic!("Expected ResetIsolateRequest, but got a different request."),
            None => panic!("Expected ResetIsolateRequest, but channel was empty."),
        }
    });

    harness.state_manager.update_state(harness.isolate_id, IsolateState::Idle).await?;

    task.await?;

    Ok(())
}

#[tokio::test]
async fn test_get_all_isolate_states() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = TestHarness::new().await;
    harness.advance_to_state(IsolateState::Ready).await;

    let states = harness.state_manager.get_all_isolate_states();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].0, harness.isolate_id);
    assert_eq!(states[0].1, IsolateState::Ready);

    Ok(())
}

// --- Helper Functions ---

fn create_add_isolate_request(isolate_id: IsolateId) -> AddIsolateRequest {
    AddIsolateRequest {
        isolate_id,
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::Public,
    }
}

fn create_get_isolate_request() -> GetIsolateRequest {
    GetIsolateRequest {
        binary_services_index: *TEST_BINARY_SERVICES_INDEX,
        data_scope_type: DataScopeType::Public,
    }
}
