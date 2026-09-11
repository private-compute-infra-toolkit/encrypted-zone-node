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
    AddIsolateRequest, DataScopeManagerResponse, FreezeIsolateScopeRequest, RemoveIsolateRequest,
    RemoveIsolateResponse,
};
use data_scope::requester::DataScopeRequester;
use enforcer_proto::enforcer::v1::IsolateState;
use isolate_info::{BinaryServicesIndex, IsolateId};
use std::collections::HashSet;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;

use tokio::sync::broadcast;

/// Maintains [IsolateState] for each Isolate and validates the state transitions.
/// It also delays the Isolates being activated in DataScopeManager before they are ready.
/// Once an Isolate is ready and its channel connected, it is activated in DataScopeManager
/// after which it can start receiving requests.
#[derive(Clone, Debug)]
pub struct IsolateStateManager {
    isolate_state_map: Arc<DashMap<IsolateId, IsolateState>>,
    data_scope_requester: DataScopeRequester,
    container_manager_requester: ContainerManagerRequester,
    in_flight_request_counts: Arc<DashMap<IsolateId, AtomicUsize>>,
    sdk_ready_map: Arc<DashMap<IsolateId, bool>>,
    channel_ready_map: Arc<DashMap<IsolateId, bool>>,
    ready_notifier: Arc<broadcast::Sender<BinaryServicesIndex>>,
    ready_binary_services_map: Arc<DashMap<BinaryServicesIndex, HashSet<IsolateId>>>,
    isolates_registered: Arc<AtomicBool>,
    // TODO: Add another map here to store the Isolates that are in MULTI-USER scope.
}

#[derive(Copy, Clone, Debug, Error)]
pub enum IsolateStateManagerError {
    #[error("Same State was already received by the Enforcer")]
    DuplicateStateUpdate,
    #[error("The state transition is not valid")]
    InvalidStateTransition,
}

/// RAII guard that holds an in-flight request count for an Isolate.
/// Decrements the in-flight counter when it is dropped.
#[derive(Debug)]
pub struct InflightGuard {
    isolate_id: IsolateId,
    state_manager: IsolateStateManager,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if self.state_manager.decrement_inflight_counter(self.isolate_id) {
            let state_manager = self.state_manager.clone();
            let isolate_id = self.isolate_id;
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = state_manager
                        .update_state(isolate_id, IsolateState::Idle)
                        .await
                        .map_err(|err| {
                            log::error!("Failed to update state to idle {:?}", err);
                        });
                });
            } else {
                log::error!(
                    "InflightGuard dropped without an active Tokio runtime for retiring isolate {:?}",
                    isolate_id
                );
            }
        }
    }
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
        let (ready_notifier, _) = broadcast::channel(128);
        Self {
            isolate_state_map: Arc::new(DashMap::new()),
            data_scope_requester,
            container_manager_requester,
            in_flight_request_counts: Arc::new(DashMap::new()),
            sdk_ready_map: Arc::new(DashMap::new()),
            channel_ready_map: Arc::new(DashMap::new()),
            ready_notifier: Arc::new(ready_notifier),
            ready_binary_services_map: Arc::new(DashMap::new()),
            isolates_registered: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Registers a new Isolate, initializing its state to `IsolateState::Starting`.
    ///
    /// The Isolate scope is registered with `DataScopeManager` immediately so that outbound
    /// requests from the starting container succeed, but it will only be activated for inbound
    /// traffic routing once both the SDK reports `IsolateState::Ready` and the Junction channel is connected.
    ///
    /// # Arguments
    ///
    /// * `add_isolate_request` - The request containing the details of the Isolate to add.
    pub async fn add_isolate(&self, add_isolate_request: AddIsolateRequest) {
        let isolate_id = add_isolate_request.isolate_id;
        self.isolate_state_map.insert(isolate_id, IsolateState::Starting);
        self.in_flight_request_counts.insert(isolate_id, AtomicUsize::new(0));
        self.sdk_ready_map.insert(isolate_id, false);
        self.channel_ready_map.insert(isolate_id, false);
        if let Err(e) = self.data_scope_requester.add_isolate(add_isolate_request).await {
            log::error!("Failed to register isolate with DataScopeRequester: {:?}", e);
        }
    }

    /// Pre-registers an Isolate's `BinaryServicesIndex` and maximum allowed data scope.
    pub async fn register_isolate_scope(
        &self,
        binary_services_index: BinaryServicesIndex,
        allowed_data_scope_type: data_scope_proto::enforcer::v1::DataScopeType,
    ) {
        self.data_scope_requester
            .register_isolate_scope(binary_services_index, allowed_data_scope_type)
            .await;
    }

    /// Acquires an RAII in-flight request guard for an Isolate, incrementing its
    /// in-flight counter immediately. When the guard is dropped, it asynchronously
    /// decrements the in-flight counter.
    pub async fn acquire_inflight_guard(&self, isolate_id: IsolateId) -> InflightGuard {
        self.increment_inflight_counter(isolate_id).await;
        InflightGuard { isolate_id, state_manager: self.clone() }
    }

    /// Increments the in-flight request counter for a given Isolate.
    ///
    /// Called internally when acquiring an `InflightGuard`.
    async fn increment_inflight_counter(&self, isolate_id: IsolateId) {
        if let Some(counter) = self.in_flight_request_counts.get(&isolate_id) {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Decrements the in-flight request counter for a given Isolate.
    ///
    /// Called internally when an `InflightGuard` is dropped.
    ///
    /// If an Isolate is in the `IsolateState::Retiring` state and its in-flight request count
    /// drops to zero, this method returns `true` indicating that the caller should transition
    /// its state to `IsolateState::Idle`.
    fn decrement_inflight_counter(&self, isolate_id: IsolateId) -> bool {
        let mut should_set_idle = false;
        if let Some(counter) = self.in_flight_request_counts.get(&isolate_id) {
            let prev = counter.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| {
                Some(val.saturating_sub(1))
            });
            if prev == Ok(1) {
                if let Some(state_entry) = self.isolate_state_map.get(&isolate_id) {
                    if *state_entry.value() == IsolateState::Retiring {
                        should_set_idle = true;
                    }
                }
            }
        }
        should_set_idle
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
        self.in_flight_request_counts.remove(&isolate_id);
        self.sdk_ready_map.remove(&isolate_id);
        self.channel_ready_map.remove(&isolate_id);
        let binary_index = isolate_id.get_binary_services_index();
        if let Some(mut set) = self.ready_binary_services_map.get_mut(&binary_index) {
            set.remove(&isolate_id);
        }

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

    /// Marks the gRPC channel connection to the Isolate as established.
    pub async fn mark_channel_connected(&self, isolate_id: IsolateId) -> Result<()> {
        self.channel_ready_map.insert(isolate_id, true);
        self.check_and_promote_to_ready(isolate_id).await
    }

    async fn check_and_promote_to_ready(&self, isolate_id: IsolateId) -> Result<()> {
        let sdk_ready = self.sdk_ready_map.get(&isolate_id).map(|v| *v).unwrap_or(false);
        let channel_ready = self.channel_ready_map.get(&isolate_id).map(|v| *v).unwrap_or(false);

        if sdk_ready && channel_ready {
            let mut promote = false;
            if let Some(mut state_ref) = self.isolate_state_map.get_mut(&isolate_id) {
                if *state_ref.value() == IsolateState::Starting {
                    *state_ref.value_mut() = IsolateState::Ready;
                    promote = true;
                }
            }
            if promote {
                self.data_scope_requester.activate_isolate(isolate_id).await?;
                let binary_index = isolate_id.get_binary_services_index();
                self.ready_binary_services_map.entry(binary_index).or_default().insert(isolate_id);
                let _ = self.ready_notifier.send(binary_index);
            }
        }
        Ok(())
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

        let old_state = *isolate_id_current_state_ref_mut.value();
        validate_state_transition(old_state, isolate_state)?;

        if isolate_state != IsolateState::Ready {
            *isolate_id_current_state_ref_mut.value_mut() = isolate_state;
        }
        drop(isolate_id_current_state_ref_mut); // drop ref to minimize contention for DashMap

        let binary_index = isolate_id.get_binary_services_index();
        if old_state == IsolateState::Ready && isolate_state != IsolateState::Ready {
            if let Some(mut set) = self.ready_binary_services_map.get_mut(&binary_index) {
                set.remove(&isolate_id);
            }
        }

        match isolate_state {
            IsolateState::Idle => {
                self.sdk_ready_map.insert(isolate_id, false);
                self.channel_ready_map.insert(isolate_id, false);
                let _ = self
                    .container_manager_requester
                    .reset_container(ResetIsolateRequest { isolate_id })
                    .await
                    .context(format!("Failed to reset container for isolate {:?} ", isolate_id))?;
                Ok(())
            }
            IsolateState::Ready => {
                self.sdk_ready_map.insert(isolate_id, true);
                self.check_and_promote_to_ready(isolate_id).await
            }
            IsolateState::Retiring => {
                self.sdk_ready_map.insert(isolate_id, false);
                self.channel_ready_map.insert(isolate_id, false);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Freezes the DataScope for an Isolate, preventing future changes to its data access permissions.
    ///
    /// # Arguments
    ///
    /// * `freeze_isolate_request` - The request containing the ID of the Isolate whose scope should be frozen.
    ///
    /// # Returns
    ///
    /// The response from the `DataScopeManager`.
    pub async fn freeze_scope(
        &self,
        freeze_isolate_request: FreezeIsolateScopeRequest,
    ) -> DataScopeManagerResponse<()> {
        self.data_scope_requester.freeze_isolate_scope(freeze_isolate_request).await
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

    /// Returns the current in-flight request count for a specific Isolate.
    ///
    /// # Arguments
    ///
    /// * `isolate_id` - The ID of the Isolate to query.
    pub fn get_inflight_count(&self, isolate_id: IsolateId) -> usize {
        self.in_flight_request_counts
            .get(&isolate_id)
            .map(|counter| counter.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Returns true if at least one instance for the given BinaryServicesIndex is Ready.
    pub fn is_isolate_ready(&self, target: BinaryServicesIndex) -> bool {
        self.ready_binary_services_map.get(&target).is_some_and(|set| !set.is_empty())
    }

    /// Waits asynchronously until at least one instance of the target BinaryServicesIndex reports Ready.
    pub async fn wait_for_isolate_ready(&self, target: BinaryServicesIndex) -> Result<()> {
        let mut rx = self.ready_notifier.subscribe();
        if self.is_isolate_ready(target) {
            return Ok(());
        }
        loop {
            match rx.recv().await {
                Ok(ready_index) if ready_index == target => return Ok(()),
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    if self.is_isolate_ready(target) {
                        return Ok(());
                    }
                }
                Err(e) => {
                    anyhow::bail!("Failed while waiting for isolate ready notification: {:?}", e)
                }
            }
        }
    }

    /// Returns whether all initial isolates have been registered.
    pub fn are_isolates_registered(&self) -> bool {
        self.isolates_registered.load(Ordering::Relaxed)
    }

    /// Marks that all initial Isolates have been registered.
    pub fn set_isolates_registered(&self) {
        self.isolates_registered.store(true, Ordering::Relaxed);
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
