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

use crate::request::{
    AddIsolateRequest, AddIsolateResponse, DataScopeManagerResponse, FreezeIsolateScopeRequest,
    FreezeIsolateScopeResponse, GetIsolateRequest, GetIsolateResponse, GetIsolateScopeRequest,
    GetIsolateScopeResponse, RemoveIsolateRequest, RemoveIsolateResponse, ValidateIsolateRequest,
    ValidateIsolateResponse,
};
use crate::{
    data_scope_manager::DataScopeManager, ratified_isolate_manager::RatifiedIsolateManager,
};
use metrics::histogram;
use std::time::Instant;

/// Requester to send requests to DataScopeManager. Consumers of this Requester as
/// expected to be async i.e. they will not block the thread while waiting for a response
/// from DataScopeManager. Consumers are encouraged to clone the requester.
#[derive(Debug, Clone)]
pub struct DataScopeRequester {
    // We use Arc to share the DataScopeManager across multiple Requesters/threads.
    data_scope_manager: std::sync::Arc<DataScopeManager>,
    ratified_isolate_manager: RatifiedIsolateManager,
}

impl DataScopeRequester {
    /// Creates a new `DataScopeRequester`.
    ///
    /// The `sensitive_session_threshold` determines how many times an Isolate can be used
    /// for a sensitive session before it is retired.
    pub fn new(sensitive_session_threshold: u64) -> Self {
        Self {
            data_scope_manager: std::sync::Arc::new(DataScopeManager::new(
                sensitive_session_threshold,
            )),
            ratified_isolate_manager: RatifiedIsolateManager::default(),
        }
    }

    /// Adds a new Isolate to DataScopeManager. Returns
    /// [DataScopeError::IsolateAlreadyExists] if Isolate already is registered
    /// with DataScopeManager.
    pub async fn add_isolate(
        &self,
        add_isolate_request: AddIsolateRequest,
    ) -> DataScopeManagerResponse<AddIsolateResponse> {
        if add_isolate_request.isolate_id.is_ratified_isolate() {
            self.ratified_isolate_manager.add_isolate(add_isolate_request).await
        } else {
            self.data_scope_manager.add_isolate(add_isolate_request).await
        }
    }

    /// Removes the Isolate from DataScopeManager. Returns
    /// [DataScopeError::UnknownIsolateId] if Isolate is not registered with DataScopeManager.
    pub async fn remove_isolate(
        &self,
        remove_isolate_request: RemoveIsolateRequest,
    ) -> DataScopeManagerResponse<RemoveIsolateResponse> {
        if remove_isolate_request.isolate_id.is_ratified_isolate() {
            self.ratified_isolate_manager.remove_isolate(remove_isolate_request).await
        } else {
            self.data_scope_manager.remove_isolate(remove_isolate_request).await
        }
    }

    /// Find an Isolate matching request requirements. If required, DataScopeManager will
    /// perform a scope-drag (bring the Isolate & its dependent Isolates into requested data-scope).
    pub async fn get_isolate(
        &self,
        get_isolate_request: GetIsolateRequest,
    ) -> DataScopeManagerResponse<GetIsolateResponse> {
        let start_time = Instant::now();
        let result = if get_isolate_request.binary_services_index.is_ratified_binary() {
            self.ratified_isolate_manager.get_isolate(get_isolate_request).await
        } else {
            self.data_scope_manager.get_isolate(get_isolate_request).await
        };
        histogram!("ez_data_scope_manager_get_isolate_latency").record(start_time.elapsed());
        result
    }

    /// Freeze Isolate DataScope when it makes CreateMemshareRequest to Enforcer. No
    /// further DataScope changes will be allowed for this Isolate. It is expected that
    /// this call is made after [Self::add_isolate] call.
    pub async fn freeze_isolate_scope(
        &self,
        freeze_isolate_request: FreezeIsolateScopeRequest,
    ) -> DataScopeManagerResponse<FreezeIsolateScopeResponse> {
        self.data_scope_manager.freeze_isolate_scope(freeze_isolate_request).await
    }

    /// Validates if an Isolate can handle the requested data scope. If the requested scope is
    /// stricter than the Isolate's current scope but within its allowed limits, the Isolate's
    /// scope will be updated.
    pub async fn validate_isolate_scope(
        &self,
        validate_isolate_request: ValidateIsolateRequest,
    ) -> DataScopeManagerResponse<ValidateIsolateResponse> {
        if validate_isolate_request.isolate_id.is_ratified_isolate() {
            self.ratified_isolate_manager.validate_isolate_scope(validate_isolate_request).await
        } else {
            self.data_scope_manager.validate_isolate(validate_isolate_request).await
        }
    }

    /// Gets the current data scope of an Isolate.
    /// This operation is not supported for Ratified Isolates.
    pub async fn get_isolate_scope(
        &self,
        get_isolate_scope_request: GetIsolateScopeRequest,
    ) -> DataScopeManagerResponse<GetIsolateScopeResponse> {
        if get_isolate_scope_request.isolate_id.is_ratified_isolate() {
            self.ratified_isolate_manager.get_isolate_scope(get_isolate_scope_request).await
        } else {
            self.data_scope_manager.get_isolate_scope(get_isolate_scope_request).await
        }
    }
}
