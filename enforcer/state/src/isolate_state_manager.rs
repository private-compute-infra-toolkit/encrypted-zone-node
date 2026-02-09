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

use anyhow::{Context, Result};
use container_manager_request::ResetIsolateRequest;
use container_manager_requester::ContainerManagerRequester;
use dashmap::DashMap;
use data_scope::error::DataScopeError;
use data_scope::request::{
    AddIsolateRequest, DataScopeManagerResponse, FreezeIsolateScopeRequest,
    FreezeIsolateScopeResponse, RemoveIsolateRequest, RemoveIsolateResponse,
};
use data_scope::requester::DataScopeRequester;
use enforcer_proto::enforcer::v1::IsolateState;
use isolate_info::IsolateId;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;

/// Maintains [IsolateState] for each Isolate and validates the state transitions.
/// It also delays the Isolates being added to DataScopeManager before they are ready.
/// Once an Isolate is ready, it is added to DataScopeManager after which it can start
/// receiving requests.
#[derive(Clone, Debug)]
pub struct IsolateStateManager {
    isolate_state_map: Arc<DashMap<IsolateId, IsolateState>>,
    unready_isolate_map: Arc<DashMap<IsolateId, AddIsolateRequest>>,
    data_scope_requester: DataScopeRequester,
    container_manager_requester: ContainerManagerRequester,
    in_flight_request_counts: Arc<DashMap<IsolateId, AtomicUsize>>,
    // TODO: Add another map here to store the Isolates that are in MULTI-USER scope.
}

#[derive(Copy, Clone, Debug, Error)]
pub enum IsolateStateManagerError {
    #[error("Same State was already received by the Enforcer")]
    DuplicateStateUpdate,
    #[error("The state transition is not valid")]
    InvalidStateTransition,
}

impl IsolateStateManager {
    /// Creates a new `IsolateStateManager`.
    ///
    /// # Arguments
    ///
    /// * `data_scope_requester` - A requester for communicating with the `DataScopeManager`.
    /// * `container_manager_requester` - A requester for communicating with the `ContainerManager`.
    pub fn new(
        data_scope_requester: DataScopeRequester,
        container_manager_requester: ContainerManagerRequester,
    ) -> Self {
        Self {
            isolate_state_map: Arc::new(DashMap::new()),
            unready_isolate_map: Arc::new(DashMap::new()),
            data_scope_requester,
            container_manager_requester,
            in_flight_request_counts: Arc::new(DashMap::new()),
        }
    }

    /// Registers a new Isolate, initializing its state to `IsolateState::Starting`.
    ///
    /// The Isolate is held in a "pending" state and is not yet active in the `DataScopeManager`.
    /// It will be fully added to the `DataScopeManager` only after its state is updated to
    /// `IsolateState::Ready` via the `update_state` method.
    ///
    /// # Arguments
    ///
    /// * `add_isolate_request` - The request containing the details of the Isolate to add.
    pub async fn add_isolate(&self, add_isolate_request: AddIsolateRequest) {
        self.isolate_state_map.insert(add_isolate_request.isolate_id, IsolateState::Starting);
        self.in_flight_request_counts.insert(add_isolate_request.isolate_id, AtomicUsize::new(0));
        self.unready_isolate_map.insert(add_isolate_request.isolate_id, add_isolate_request);
    }

    /// Increments the in-flight request counter for a given Isolate.
    ///
    /// This is called on the request path before forwarding a request to an Isolate,
    /// ensuring that the system can track active work.
    ///
    /// # Arguments
    ///
    /// * `isolate_id` - The ID of the Isolate handling the request.
    pub async fn increment_inflight_counter(&self, isolate_id: IsolateId) {
        if let Some(counter) = self.in_flight_request_counts.get(&isolate_id) {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Decrements the in-flight request counter for a given Isolate.
    ///
    /// This is called on the response path after an Isolate has finished processing a request.
    ///
    /// If an Isolate is in the `IsolateState::Retiring` state and its in-flight request count
    /// drops to zero, this method will transition its state to `IsolateState::Idle`.
    ///
    /// # Arguments
    ///
    /// * `isolate_id` - The ID of the Isolate that handled the request.
    pub async fn decrement_inflight_counter(&self, isolate_id: IsolateId) {
        let mut should_set_idle = false;
        if let Some(counter) = self.in_flight_request_counts.get(&isolate_id) {
            if counter.fetch_sub(1, Ordering::SeqCst) == 1 {
                if let Some(state_entry) = self.isolate_state_map.get(&isolate_id) {
                    if *state_entry.value() == IsolateState::Retiring {
                        should_set_idle = true;
                    }
                }
            }
        }
        if should_set_idle {
            // Note: Error is ignored because if update_state fails, it's a non-critical logic error.
            // The Isolate is already effectively idle.
            let _ = self.update_state(isolate_id, IsolateState::Idle).await.map_err(|err| {
                log::error!("Failed to update state to idle {:?}", err);
            });
            self.in_flight_request_counts.remove(&isolate_id);
        }
    }

    /// Removes an Isolate from all internal tracking and requests its removal from the
    /// `DataScopeManager`.
    ///
    /// This method cleans up all state associated with the Isolate within the `IsolateStateManager`.
    ///
    /// # Arguments
    ///
    /// * `remove_isolate_request` - The request containing the ID of the Isolate to remove.
    ///
    /// # Returns
    ///
    /// The response from the `DataScopeManager` after processing the removal request.
    pub async fn remove_isolate(
        &self,
        remove_isolate_request: RemoveIsolateRequest,
    ) -> DataScopeManagerResponse<RemoveIsolateResponse> {
        let isolate_id = remove_isolate_request.isolate_id;
        self.isolate_state_map.remove(&isolate_id);
        self.unready_isolate_map.remove(&isolate_id);
        self.in_flight_request_counts.remove(&isolate_id);

        match self.data_scope_requester.remove_isolate(remove_isolate_request).await {
            Ok(response) => Ok(response),
            Err(err) => {
                // It's possible the Isolate was already removed (e.g., due to sensitive session retirement).
                // In that case, we can safely ignore the error and treat it as a success.
                if matches!(err, DataScopeError::UnknownIsolateId) {
                    Ok(RemoveIsolateResponse { isolate_id })
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Updates the state of an Isolate and performs actions based on the new state.
    ///
    /// This method enforces valid state transitions. If the new state is `IsolateState::Ready`,
    /// it moves the Isolate from the "pending" map to the active `DataScopeManager`.
    /// If the new state is `IsolateState::Idle`, it triggers a container reset.
    ///
    /// # Arguments
    ///
    /// * `isolate_id` - The ID of the Isolate to update.
    /// * `isolate_state` - The new state for the Isolate.
    ///
    /// # Errors
    ///
    /// Returns an error if the Isolate ID is not recognized or if the state transition is invalid
    /// (e.g., duplicate update or an illegal transition like `Ready` -> `Starting`).
    pub async fn update_state(
        &self,
        isolate_id: IsolateId,
        isolate_state: IsolateState,
    ) -> Result<()> {
        let mut isolate_id_current_state_ref_mut = self
            .isolate_state_map
            .get_mut(&isolate_id)
            .context("Unrecognized IsolateId received for update_state")?;

        validate_state_transition(*isolate_id_current_state_ref_mut.value(), isolate_state)?;
        *isolate_id_current_state_ref_mut.value_mut() = isolate_state;
        drop(isolate_id_current_state_ref_mut); // drop ref to minimize contention for DashMap

        match isolate_state {
            IsolateState::Idle => {
                let _ = self
                    .container_manager_requester
                    .reset_container(ResetIsolateRequest { isolate_id })
                    .await
                    .context(format!("Failed to reset container for isolate {:?} ", isolate_id))?;
                Ok(())
            }
            IsolateState::Ready => {
                let (_isolate_id, add_isolate_request) = self
                    .unready_isolate_map
                    .remove(&isolate_id)
                    .context("Unrecognized IsolateId received for update_state, InternalError")?;

                self.data_scope_requester.add_isolate(add_isolate_request).await?;
                Ok(())
            }
            IsolateState::Retiring => {
                // Remove the Isolate Id if it is marked retiring so that Junction doesn't assign it new requests
                let result = self
                    .data_scope_requester
                    .remove_isolate(RemoveIsolateRequest { isolate_id })
                    .await;
                if let Some(err) = result.err() {
                    // It's possible the Isolate was already removed (e.g., due to sensitive session retirement).
                    // In that case, we can safely ignore the error.
                    if !matches!(err, DataScopeError::UnknownIsolateId) {
                        return Err(err.into());
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Freezes the DataScope for an Isolate, preventing future changes to its data access permissions.
    ///
    /// The behavior depends on the Isolate's current state:
    /// - If the Isolate is not yet `Ready` (i.e., it's in the pending `unready_isolate_map`),
    ///   this method locks its `allowed_data_scope_type` to its `current_data_scope_type` locally.
    /// - If the Isolate is already active (`Ready`), the request is forwarded to the `DataScopeManager`
    ///   to be frozen there.
    ///
    /// # Arguments
    ///
    /// * `freeze_isolate_request` - The request containing the ID of the Isolate whose scope should be frozen.
    ///
    /// # Returns
    ///
    /// The response from the `DataScopeManager` or an immediate success response if handled locally.
    pub async fn freeze_scope(
        &self,
        freeze_isolate_request: FreezeIsolateScopeRequest,
    ) -> DataScopeManagerResponse<FreezeIsolateScopeResponse> {
        let add_isolate_req_option =
            self.unready_isolate_map.remove(&freeze_isolate_request.isolate_id);

        match add_isolate_req_option {
            Some((_isolate_id, mut add_isolate_req)) => {
                add_isolate_req.allowed_data_scope_type = add_isolate_req.current_data_scope_type;
                self.unready_isolate_map.insert(freeze_isolate_request.isolate_id, add_isolate_req);
                Ok(FreezeIsolateScopeResponse {})
            }
            None => self.data_scope_requester.freeze_isolate_scope(freeze_isolate_request).await,
        }
    }

    /// Returns a list of all Isolates and their current states.
    ///
    /// This is used by the health manager to monitor the health of all Isolates.
    pub fn get_all_isolate_states(&self) -> Vec<(IsolateId, IsolateState)> {
        self.isolate_state_map.iter().map(|entry| (*entry.key(), *entry.value())).collect()
    }

    /// Returns the current state of a specific Isolate.
    ///
    /// # Arguments
    ///
    /// * `isolate_id` - The ID of the Isolate to query.
    pub fn get_isolate_state(&self, isolate_id: IsolateId) -> Option<IsolateState> {
        self.isolate_state_map.get(&isolate_id).map(|state| *state.value())
    }
}

fn validate_state_transition(current_state: IsolateState, new_state: IsolateState) -> Result<()> {
    if new_state == current_state {
        log::warn!(
            "Invalid state transition detected. Already in following state: {:?}",
            new_state
        );
        return Err(IsolateStateManagerError::DuplicateStateUpdate.into());
    }
    // TODO Convert this into a static Map defining allowed transitions
    match new_state {
        // TODO Support all IsolateStates
        IsolateState::Ready => {
            if current_state != IsolateState::Starting {
                return Err(IsolateStateManagerError::InvalidStateTransition.into());
            }
        }
        IsolateState::Idle => {
            if current_state != IsolateState::Ready && current_state != IsolateState::Retiring {
                return Err(IsolateStateManagerError::InvalidStateTransition.into());
            }
        }
        IsolateState::Retiring => {
            if current_state != IsolateState::Ready {
                return Err(IsolateStateManagerError::InvalidStateTransition.into());
            }
        }
        _ => {
            return Err(IsolateStateManagerError::InvalidStateTransition.into());
        }
    }

    Ok(())
}
