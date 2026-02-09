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
    AddIsolateRequest, AddIsolateResponse, GetIsolateRequest, GetIsolateResponse,
    GetIsolateScopeRequest, GetIsolateScopeResponse, RemoveIsolateRequest, RemoveIsolateResponse,
    ValidateIsolateRequest, ValidateIsolateResponse,
};
use dashmap::{mapref::one::Ref, DashMap};
use data_scope_proto::enforcer::v1::DataScopeType;
use derivative::Derivative;
use indexmap::set::IndexSet;
use isolate_info::{BinaryServicesIndex, IsolateId};
use rand::Rng;
use std::{borrow::Borrow, sync::Arc};

/// Contains scope information for a Ratified Isolate.
#[derive(Derivative, Clone, Debug)]
#[derivative(PartialEq, Eq, Hash)]
struct IsolateScopeinfo {
    isolate_id: IsolateId,
    /// Maximum permissible scope of the Ratified Isolate.
    /// This field is not part of the hash or equality checks because the `isolate_id`
    /// uniquely identifies the Isolate.
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    max_data_scope: DataScopeType,
}

impl Borrow<IsolateId> for IsolateScopeinfo {
    /// Borrows the `IsolateId` for lookups in collections.
    fn borrow(&self) -> &IsolateId {
        &self.isolate_id
    }
}

/// The [RatifiedIsolateManager] handles tracking of Ratified Isolates. Ratified Isolates
/// have a fixed maximum data scope and are not subject to the same dynamic scope management
/// as regular Isolates.
///
/// This manager is designed for concurrent access, using a [DashMap] to store Isolate
/// information. This allows multiple threads to interact with the manager simultaneously
/// without the need for a dedicated processing thread and message passing.
#[derive(Debug, Default, Clone)]
pub struct RatifiedIsolateManager {
    // Using DashMap for concurrent access to the Isolate index. The key is the
    // BinaryServicesIndex, and the value is a set of IsolateScopeinfo structs.
    isolates_map: Arc<DashMap<BinaryServicesIndex, IndexSet<IsolateScopeinfo>>>,
}

impl RatifiedIsolateManager {
    /// Adds a Ratified Isolate to the manager.
    ///
    /// The `current_data_scope_type` from the request is ignored for Ratified Isolates, as their
    /// scope is determined by their `allowed_data_scope_type` which is considered their maximum
    /// permissible scope.
    pub async fn add_isolate(
        &self,
        add_isolate_request: AddIsolateRequest,
    ) -> Result<AddIsolateResponse, DataScopeError> {
        let AddIsolateRequest {
            current_data_scope_type: _, // The `current_data_scope_type` is ignored.
            allowed_data_scope_type,
            isolate_id,
        } = add_isolate_request;

        if !isolate_id.is_ratified_isolate() {
            // This should never happen
            log::error!("Trying to insert opaque isolate in RIM {:?}", isolate_id);
            return Err(DataScopeError::InternalError(
                "Trying to insert opaque isolate in RIM".to_string(),
            ));
        }

        let binary_services_index = isolate_id.get_binary_services_index();
        let mut isolate_set = self.isolates_map.entry(binary_services_index).or_default();

        if isolate_set.contains(&isolate_id) {
            return Err(DataScopeError::InternalError(format!(
                "Isolate already exists: {:?}",
                isolate_id
            )));
        }

        isolate_set
            .insert(IsolateScopeinfo { isolate_id, max_data_scope: allowed_data_scope_type });
        Ok(AddIsolateResponse {})
    }

    /// Removes a Ratified Isolate from the manager.
    pub async fn remove_isolate(
        &self,
        remove_isolate_request: RemoveIsolateRequest,
    ) -> Result<RemoveIsolateResponse, DataScopeError> {
        let RemoveIsolateRequest { isolate_id } = remove_isolate_request;
        let binary_services_index = isolate_id.get_binary_services_index();
        let Some(mut isolate_set) = self.isolates_map.get_mut(&binary_services_index) else {
            return Err(DataScopeError::InvalidIsolateServiceIndex);
        };

        if !isolate_set.contains(&isolate_id) {
            return Err(DataScopeError::UnknownIsolateId);
        }

        isolate_set.swap_remove(&isolate_id);
        Ok(RemoveIsolateResponse { isolate_id })
    }

    /// Retrieves a Ratified Isolate that can handle the requested data scope.
    ///
    /// It selects an available Isolate for the given `BinaryServicesIndex`.
    /// It then verifies that the requested data scope does not
    /// exceed the maximum allowed scope for the selected Isolate.
    pub async fn get_isolate(
        &self,
        get_isolate_request: GetIsolateRequest,
    ) -> Result<GetIsolateResponse, DataScopeError> {
        let GetIsolateRequest { binary_services_index, data_scope_type } = get_isolate_request;

        match data_scope_type {
            DataScopeType::Unspecified => {
                log::warn!("Unknown data scope type: {:?}", data_scope_type);
                Err(DataScopeError::InvalidDataScopeType)
            }
            DataScopeType::Public | DataScopeType::DomainOwned | DataScopeType::UserPrivate => {
                let isolate_set = self.get_isolate_set(&binary_services_index)?;
                let random_isolate_index = rand::rng().random_range(0..isolate_set.len());
                let Some(isolate_info) = isolate_set.get_index(random_isolate_index) else {
                    return Err(DataScopeError::NoMatchingIsolates);
                };

                if data_scope_type > isolate_info.max_data_scope {
                    return Err(DataScopeError::NoMatchingIsolates);
                }
                Ok(GetIsolateResponse { isolate_id: isolate_info.isolate_id, new_state: None })
            }
            _ => {
                // TODO Support other DataScopeTypes
                log::warn!("Unsupported DataScopeType: {:?}", data_scope_type);
                Err(DataScopeError::InvalidDataScopeType)
            }
        }
    }

    /// Validates that a given Ratified Isolate can handle the requested data scope.
    ///
    /// This method checks if the `requested_scope` is within the `max_data_scope`
    /// of the specified Isolate. It does not modify the state of the Isolate.
    pub async fn validate_isolate_scope(
        &self,
        validate_isolate_request: ValidateIsolateRequest,
    ) -> Result<ValidateIsolateResponse, DataScopeError> {
        let ValidateIsolateRequest { isolate_id, requested_scope } = validate_isolate_request;

        if requested_scope == DataScopeType::Unspecified {
            log::warn!("Unknown data scope type: {:?}", requested_scope);
            return Err(DataScopeError::InvalidDataScopeType);
        }
        let binary_services_index = isolate_id.get_binary_services_index();
        let isolate_set = self.get_isolate_set(&binary_services_index)?;
        let Some(isolate_info) = isolate_set.get(&isolate_id) else {
            return Err(DataScopeError::UnknownIsolateId);
        };
        if requested_scope > isolate_info.max_data_scope {
            return Err(DataScopeError::DisallowedByManifest);
        }
        Ok(ValidateIsolateResponse {})
    }

    /// Retrieves the scope of a Ratified Isolate.
    ///
    /// Ratified Isolates do not have a dynamic "current" scope like Opaque Isolates.
    /// Returns `DataScopeType::Unspecified` as a placeholder.
    pub async fn get_isolate_scope(
        &self,
        _: GetIsolateScopeRequest,
    ) -> Result<GetIsolateScopeResponse, DataScopeError> {
        Ok(GetIsolateScopeResponse { current_scope: DataScopeType::Unspecified })
    }

    /// Retrieves a reference to the set of Isolates for a given binary index.
    /// This function holds a read lock on the underlying map shard, which is released
    /// when the returned `Ref` goes out of scope.
    fn get_isolate_set<'a>(
        &'a self,
        binary_services_index: &BinaryServicesIndex,
    ) -> Result<Ref<'a, BinaryServicesIndex, IndexSet<IsolateScopeinfo>>, DataScopeError> {
        let Some(isolate_set) = self.isolates_map.get(binary_services_index) else {
            return Err(DataScopeError::InvalidIsolateServiceIndex);
        };
        if isolate_set.is_empty() {
            return Err(DataScopeError::NoMatchingIsolates);
        }
        Ok(isolate_set)
    }
}
