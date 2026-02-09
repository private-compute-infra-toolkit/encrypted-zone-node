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

use crate::error::DataScopeError;

use crate::request::{
    AddIsolateRequest, AddIsolateResponse, FreezeIsolateScopeRequest, FreezeIsolateScopeResponse,
    GetIsolateRequest, GetIsolateResponse, GetIsolateScopeRequest, GetIsolateScopeResponse,
    RemoveIsolateRequest, RemoveIsolateResponse, ValidateIsolateRequest, ValidateIsolateResponse,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::IsolateState;
use indexmap::set::IndexSet;
use isolate_info::{BinaryServicesIndex, IsolateId};
use metrics::histogram;
use rand::Rng;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::Mutex;

const DATA_SCOPES: [DataScopeType; 3] =
    [DataScopeType::Public, DataScopeType::DomainOwned, DataScopeType::UserPrivate];

/// The [DataScopeManager] handles tracking of data scope propagation for Isolate instances.
/// It operates with internal shared state protected by a Mutex, allowing concurrent access
/// from multiple requesters.
#[derive(Debug)]
pub struct DataScopeManager {
    state: Mutex<DataScopeManagerState>,
    sensitive_session_threshold: u64,
}

#[derive(Debug)]
struct DataScopeManagerState {
    // Map of Possible DataScope to Isolates
    available_scope_isolate_index:
        HashMap<BinaryServicesIndex, HashMap<DataScopeType, IndexSet<IsolateId>>>,
    // Map from Isolate instances to their current scope
    // TODO Support owner based indexing in this index
    isolate_scope_index: HashMap<IsolateId, DataScopeType>,
    // Index which maintains the strictest scope that is allowed for the Isolate
    // TODO Replace IsolateId with BinaryServicesIndex
    isolate_max_scope_index: HashMap<IsolateId, DataScopeType>,
    // Tracks how many times an Isolate has been assigned to a sensitive session.
    sensitive_session_counts: HashMap<IsolateId, u64>,
}

impl DataScopeManager {
    /// Creates a new [DataScopeManager].
    pub fn new(sensitive_session_threshold: u64) -> Self {
        DataScopeManager {
            state: Mutex::new(DataScopeManagerState {
                available_scope_isolate_index: HashMap::new(),
                isolate_scope_index: HashMap::new(),
                isolate_max_scope_index: HashMap::new(),
                sensitive_session_counts: HashMap::new(),
            }),
            sensitive_session_threshold,
        }
    }

    /// Adds a new Isolate to the [DataScopeManager].
    ///
    /// This function registers the Isolate with its current and allowed data scopes.
    /// It returns an error if the Isolate ID already exists or if the current scope
    /// exceeds the allowed scope.
    pub async fn add_isolate(
        &self,
        add_isolate_request: AddIsolateRequest,
    ) -> Result<AddIsolateResponse, DataScopeError> {
        let AddIsolateRequest { current_data_scope_type, allowed_data_scope_type, isolate_id } =
            add_isolate_request;

        if isolate_id.is_ratified_isolate() {
            // This should never happen
            log::error!("Trying to insert ratified isolate in DSM {:?}", isolate_id);
            return Err(DataScopeError::InternalError(
                "Trying to insert ratified isolate in DSM".to_string(),
            ));
        }

        let mut state = self.state.lock().await;

        if state.isolate_scope_index.contains_key(&isolate_id) {
            return Err(DataScopeError::InternalError(format!(
                "Isolate already exists: {:?}",
                isolate_id
            )));
        }

        let _ = state.isolate_max_scope_index.insert(isolate_id, allowed_data_scope_type);
        let _ = state.isolate_scope_index.insert(isolate_id, current_data_scope_type);

        let binary_services_index = isolate_id.get_binary_services_index();
        let scope_index =
            state.available_scope_isolate_index.entry(binary_services_index).or_default();

        if current_data_scope_type == allowed_data_scope_type {
            add_to_single_scope_index(scope_index, isolate_id, current_data_scope_type);
        } else if current_data_scope_type <= allowed_data_scope_type {
            add_from_to_data_scope_index(
                scope_index,
                isolate_id,
                current_data_scope_type,
                allowed_data_scope_type,
            );
        } else {
            // current_data_scope_type > allowed_data_scope_type
            return Err(DataScopeError::DisallowedByManifest);
        }

        Ok(AddIsolateResponse {})
    }

    /// Removes an Isolate from the [DataScopeManager].
    ///
    /// This function removes all state associated with the Isolate, including its
    /// scope tracking and sensitive session counts.
    pub async fn remove_isolate(
        &self,
        remove_isolate_request: RemoveIsolateRequest,
    ) -> Result<RemoveIsolateResponse, DataScopeError> {
        let RemoveIsolateRequest { isolate_id } = remove_isolate_request;

        let mut state_guard = self.state.lock().await;
        let state = &mut *state_guard;

        Self::remove_isolate_internal(state, isolate_id)?;
        Ok(RemoveIsolateResponse { isolate_id })
    }

    fn remove_isolate_internal(
        state: &mut DataScopeManagerState,
        isolate_id: IsolateId,
    ) -> Result<(), DataScopeError> {
        if !state.isolate_scope_index.contains_key(&isolate_id) {
            return Err(DataScopeError::UnknownIsolateId);
        }

        let binary_services_index = isolate_id.get_binary_services_index();
        let scope_index_option =
            state.available_scope_isolate_index.get_mut(&binary_services_index);
        let Some(scope_index) = scope_index_option else {
            // Ideally, should never happen.
            return internal_error(isolate_id);
        };

        let strictest_allowed_scope_option = state.isolate_max_scope_index.remove(&isolate_id);
        let Some(strictest_allowed_scope) = strictest_allowed_scope_option else {
            // Ideally, should never happen.
            return internal_error(isolate_id);
        };

        let current_data_scope_option = state.isolate_scope_index.remove(&isolate_id);
        let Some(current_data_scope) = current_data_scope_option else {
            // Ideally, should never happen.
            return internal_error(isolate_id);
        };

        if current_data_scope == strictest_allowed_scope {
            remove_from_single_scope_index(scope_index, isolate_id, current_data_scope);
        } else if current_data_scope <= strictest_allowed_scope {
            remove_from_to_scope_index(
                scope_index,
                isolate_id,
                current_data_scope,
                strictest_allowed_scope,
            );
        } else {
            // Ideally, will never happen
            // current_data_scope_type > allowed_data_scope_type
            return internal_error(isolate_id);
        }

        state.sensitive_session_counts.remove(&isolate_id);
        Ok(())
    }

    /// Retrieves an Isolate for a specific service and data scope.
    ///
    /// This function attempts to find an available Isolate for the given service index
    /// that matches the requested data scope. If necessary, it may upgrade the Isolate's
    /// scope (if allowed). It also checks if the Isolate has reached its sensitive
    /// session threshold and should be retired.
    pub async fn get_isolate(
        &self,
        get_isolate_request: GetIsolateRequest,
    ) -> Result<GetIsolateResponse, DataScopeError> {
        let start_time = Instant::now();
        let service_index = get_isolate_request.binary_services_index;
        let requested_data_scope = get_isolate_request.data_scope_type;

        let mut state_guard = self.state.lock().await;
        let state = &mut *state_guard;

        let isolate_id = {
            let scope_matched_isolate_option =
                find_compatible_isolate(state, service_index, requested_data_scope)?;
            scope_matched_isolate_option.ok_or(DataScopeError::NoMatchingIsolates)?
        };

        let DataScopeManagerState {
            available_scope_isolate_index,
            isolate_scope_index,
            isolate_max_scope_index,
            ..
        } = state;

        let Some(scope_index) = available_scope_isolate_index.get_mut(&service_index) else {
            return Err(DataScopeError::InternalError("Scope index not found".to_string()));
        };

        let current_data_scope = isolate_scope_index.get(&isolate_id).copied().unwrap_or_default();
        let strictest_allowed_scope =
            isolate_max_scope_index.get(&isolate_id).copied().unwrap_or_default();

        if requested_data_scope != current_data_scope {
            change_isolate_scope(
                scope_index,
                isolate_scope_index,
                isolate_id,
                requested_data_scope,
                current_data_scope,
                strictest_allowed_scope,
            );
        }

        if let Some(new_state) =
            self.handle_sensitive_session(requested_data_scope, state, isolate_id)?
        {
            log::info!(
                "Isolate {isolate_id} reached sensitive session threshold. Marking as retiring."
            );
            return Ok(GetIsolateResponse { isolate_id, new_state: Some(new_state) });
        }

        histogram!("ez_data_scope_manager_inner_get_isolate_latency").record(start_time.elapsed());
        Ok(GetIsolateResponse { isolate_id, new_state: None })
    }

    /// Checks if an Isolate has reached its sensitive session threshold and should be retired.
    /// If so, it removes the Isolate from the available pool and returns `Some(IsolateState::Retiring)`.
    fn handle_sensitive_session(
        &self,
        requested_data_scope: DataScopeType,
        state: &mut DataScopeManagerState,
        isolate_id: IsolateId,
    ) -> Result<Option<IsolateState>, DataScopeError> {
        // A threshold of 0 is a special value that disables retirement.
        if self.sensitive_session_threshold > 0
            && requested_data_scope >= DataScopeType::UserPrivate
        {
            let count_entry = state.sensitive_session_counts.entry(isolate_id).or_insert(0);
            *count_entry += 1;

            if *count_entry >= self.sensitive_session_threshold {
                // Propagate internal errors up the chain
                // Ideally remove Isolate should never fail because Get Isolate should not
                // choose an Isolate that is already removed
                Self::remove_isolate_internal(state, isolate_id)?;
                return Ok(Some(IsolateState::Retiring));
            }
        }
        Ok(None)
    }

    /// Validates and updates an Isolate's scope for a request.
    ///
    /// This function checks if the Isolate can operate at the requested scope.
    /// If the requested scope is higher than the current scope but within the allowed limits,
    /// the Isolate's scope is upgraded.
    pub async fn validate_isolate(
        &self,
        validate_isolate_request: ValidateIsolateRequest,
    ) -> Result<ValidateIsolateResponse, DataScopeError> {
        let isolate_id = validate_isolate_request.isolate_id;
        let requested_scope = validate_isolate_request.requested_scope;
        if requested_scope == DataScopeType::Unspecified {
            return Err(DataScopeError::InvalidDataScopeType);
        }

        let mut state_guard = self.state.lock().await;
        let state = &mut *state_guard;

        let DataScopeManagerState {
            available_scope_isolate_index,
            isolate_scope_index,
            isolate_max_scope_index,
            ..
        } = state;

        let binary_services_index = isolate_id.get_binary_services_index();
        let scope_index = available_scope_isolate_index
            .get_mut(&binary_services_index)
            .ok_or(DataScopeError::InvalidIsolateServiceIndex)?;

        let Some(current_data_scope) = isolate_scope_index.get(&isolate_id).copied() else {
            return Err(DataScopeError::UnknownIsolateId);
        };
        let Some(strictest_allowed_scope) = isolate_max_scope_index.get(&isolate_id).copied()
        else {
            return internal_error(isolate_id);
        };

        if requested_scope > strictest_allowed_scope {
            return Err(DataScopeError::DisallowedByManifest);
        }

        if requested_scope >= current_data_scope {
            change_isolate_scope(
                scope_index,
                isolate_scope_index,
                isolate_id,
                requested_scope,
                current_data_scope,
                strictest_allowed_scope,
            );
        }
        Ok(ValidateIsolateResponse {})
    }

    /// Freezes the Isolate's scope at its current level.
    ///
    /// This prevents the Isolate from taking on requests with a stricter data scope
    /// than its current scope.
    pub async fn freeze_isolate_scope(
        &self,
        req: FreezeIsolateScopeRequest,
    ) -> Result<FreezeIsolateScopeResponse, DataScopeError> {
        let mut state_guard = self.state.lock().await;
        let state = &mut *state_guard;

        let Some(current_data_scope) = state.isolate_max_scope_index.get(&req.isolate_id).copied()
        else {
            return Err(DataScopeError::UnknownIsolateId);
        };

        let binary_services_index = req.isolate_id.get_binary_services_index();
        let scope_index = state
            .available_scope_isolate_index
            .get_mut(&binary_services_index)
            .ok_or(DataScopeError::InternalError("Scope index not found".to_string()))?;

        state.isolate_max_scope_index.insert(req.isolate_id, current_data_scope);
        freeze_data_scope_index(scope_index, req.isolate_id, current_data_scope);
        Ok(FreezeIsolateScopeResponse {})
    }

    /// Retrieves the current data scope of the specified Isolate.
    pub async fn get_isolate_scope(
        &self,
        req: GetIsolateScopeRequest,
    ) -> Result<GetIsolateScopeResponse, DataScopeError> {
        let state = self.state.lock().await;

        // TODO: Add support for retiring Isolates
        if let Some(current_scope) = state.isolate_scope_index.get(&req.isolate_id) {
            return Ok(GetIsolateScopeResponse { current_scope: *current_scope });
        }
        Err(DataScopeError::UnknownIsolateId)
    }
}

/// Adds Isolate to [from_data_scope, to_data_scope] in ScopeIndex.
fn add_from_to_data_scope_index(
    scope_index: &mut HashMap<DataScopeType, IndexSet<IsolateId>>,
    isolate_id: IsolateId,
    from_data_scope: DataScopeType,
    to_data_scope: DataScopeType,
) {
    for data_scope in DATA_SCOPES {
        if data_scope >= from_data_scope && data_scope <= to_data_scope {
            add_to_single_scope_index(scope_index, isolate_id, data_scope);
        }
    }
}

fn add_to_single_scope_index(
    scope_index: &mut HashMap<DataScopeType, IndexSet<IsolateId>>,
    isolate_id: IsolateId,
    data_scope: DataScopeType,
) {
    let _ = scope_index.entry(data_scope).or_default().insert(isolate_id);
}

fn remove_from_all_scope_index(
    scope_index: &mut HashMap<DataScopeType, IndexSet<IsolateId>>,
    isolate_id: IsolateId,
) {
    for data_scope in DATA_SCOPES {
        remove_from_single_scope_index(scope_index, isolate_id, data_scope);
    }
}

/// Removes Isolate to [from_data_scope, to_data_scope] in ScopeIndex.
fn remove_from_to_scope_index(
    scope_index: &mut HashMap<DataScopeType, IndexSet<IsolateId>>,
    isolate_id: IsolateId,
    from_data_scope: DataScopeType,
    to_data_scope: DataScopeType,
) {
    for data_scope in DATA_SCOPES {
        if data_scope >= from_data_scope && data_scope <= to_data_scope {
            remove_from_single_scope_index(scope_index, isolate_id, data_scope);
        }
    }
}

fn remove_from_single_scope_index(
    scope_index: &mut HashMap<DataScopeType, IndexSet<IsolateId>>,
    isolate_id: IsolateId,
    data_scope: DataScopeType,
) {
    scope_index.get_mut(&data_scope).unwrap_or(&mut IndexSet::new()).swap_remove(&isolate_id);
}

fn get_isolate_from_index(
    scope_index_option: &Option<&HashMap<DataScopeType, IndexSet<IsolateId>>>,
    data_scope: DataScopeType,
) -> Option<IsolateId> {
    match scope_index_option {
        Some(scope_index) => {
            let isolate_set_option = scope_index.get(&data_scope);
            match isolate_set_option {
                // Select a Random Isolate from the Set (uniformly distribute traffic)
                Some(isolate_set) => {
                    // TODO Maybe create a separate module for LB to reuse ThreadRng
                    if isolate_set.is_empty() {
                        return None;
                    }
                    let random_isolate_index = rand::rng().random_range(0..isolate_set.len());
                    isolate_set.get_index(random_isolate_index).copied()
                }
                None => None,
            }
        }
        None => None,
    }
}

/// Update the internal Scope Index for the given Isolate. Assumes that
/// current_data_scope <= requested_data_scope <= strictest_allowed_scope.
fn change_isolate_scope(
    scope_isolate_index: &mut HashMap<DataScopeType, IndexSet<IsolateId>>,
    isolate_scope_index: &mut HashMap<IsolateId, DataScopeType>,
    isolate_id: IsolateId,
    requested_data_scope: DataScopeType,
    current_data_scope: DataScopeType,
    strictest_allowed_scope: DataScopeType,
) {
    isolate_scope_index.insert(isolate_id, requested_data_scope);

    if requested_data_scope == strictest_allowed_scope {
        freeze_data_scope_index(scope_isolate_index, isolate_id, requested_data_scope);
    } else {
        // current_scope <= requested_scope < strictest_allowed_scope
        // Remove Isolate from scope [current_scope, requested_scope)
        remove_from_to_scope_index(
            scope_isolate_index,
            isolate_id,
            current_data_scope,
            requested_data_scope,
        );
        add_to_single_scope_index(scope_isolate_index, isolate_id, requested_data_scope);
    }
}

/// Removes Isolates from all DataScopes except the provided one
fn freeze_data_scope_index(
    scope_index: &mut HashMap<DataScopeType, IndexSet<IsolateId>>,
    isolate_id: IsolateId,
    data_scope: DataScopeType,
) {
    remove_from_all_scope_index(scope_index, isolate_id);
    add_to_single_scope_index(scope_index, isolate_id, data_scope);
}

fn internal_error<T>(isolate_id: IsolateId) -> Result<T, DataScopeError> {
    log::error!("Inconsistent Internal state for DataScopeManager. Isolate: {isolate_id}");
    Err::<T, DataScopeError>(DataScopeError::InternalError(
        "Inconsistent Internal state for DataScopeManager".to_string(),
    ))
}

fn find_compatible_isolate(
    state: &DataScopeManagerState,
    service_index: BinaryServicesIndex,
    requested_data_scope: DataScopeType,
) -> Result<Option<IsolateId>, DataScopeError> {
    let scoped_index_option = state.available_scope_isolate_index.get(&service_index);

    match requested_data_scope {
        DataScopeType::Unspecified => {
            log::error!("Unknown data scope type: {:?}", requested_data_scope);
            Err(DataScopeError::InvalidDataScopeType)
        }
        DataScopeType::Public | DataScopeType::DomainOwned | DataScopeType::UserPrivate => {
            Ok(get_isolate_from_index(&scoped_index_option, requested_data_scope))
        }
        _ => {
            log::warn!("Unsupported DataScopeType: {:?}", requested_data_scope);
            Err(DataScopeError::InvalidDataScopeType)
        }
    }
}
