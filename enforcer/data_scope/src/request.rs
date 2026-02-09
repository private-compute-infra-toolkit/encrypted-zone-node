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
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::IsolateState;
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceIndex};

/// A response type for DataScopeManager operations
pub type DataScopeManagerResponse<T> = Result<T, DataScopeError>;

/// A request to add a new Isolate to be tracked by the `DataScopeManager`.
#[derive(Debug)]
pub struct AddIsolateRequest {
    /// The initial data scope of the Isolate.
    pub current_data_scope_type: DataScopeType,
    /// The strictest data scope that the Isolate is allowed to be in.
    pub allowed_data_scope_type: DataScopeType,
    /// The unique identifier for the Isolate.
    pub isolate_id: IsolateId,
}

/// A response for an `AddIsolateRequest`, indicating the operation was successful.
#[derive(Debug)]
pub struct AddIsolateResponse {}

/// A request to remove an Isolate from the `DataScopeManager` and `RatifiedIsolateManager`.
#[derive(Debug)]
pub struct RemoveIsolateRequest {
    /// The unique identifier for the Isolate to be removed.
    pub isolate_id: IsolateId,
}

/// A response for a `RemoveIsolateRequest`.
#[derive(Debug)]
pub struct RemoveIsolateResponse {
    /// The ID of the Isolate that was removed.
    pub isolate_id: IsolateId,
}

/// A request to find an available Isolate that can handle a specific data scope.
#[derive(Debug)]
pub struct GetIsolateRequest {
    /// The index of the binary's services for which an Isolate is requested.
    pub binary_services_index: BinaryServicesIndex,
    /// The required data scope for the Isolate.
    pub data_scope_type: DataScopeType,
}

/// A response for a `GetIsolateRequest`.
#[derive(Debug)]
pub struct GetIsolateResponse {
    /// The ID of the selected Isolate.
    pub isolate_id: IsolateId,
    /// An optional new state for the Isolate, e.g., if it's being retired.
    pub new_state: Option<IsolateState>,
}

/// A request to validate that an Isolate can be used for a given data scope.
#[derive(Debug)]
pub struct ValidateIsolateRequest {
    /// The ID of the Isolate to validate.
    pub isolate_id: IsolateId,
    /// The data scope to validate against.
    pub requested_scope: DataScopeType,
}

/// A response for a `ValidateIsolateRequest`, indicating the validation was successful.
#[derive(Debug)]
pub struct ValidateIsolateResponse {}

/// A request to freeze the data scope of an Isolate, preventing it from being changed further.
#[derive(Debug)]
pub struct FreezeIsolateScopeRequest {
    /// The ID of the Isolate whose scope is to be frozen.
    pub isolate_id: IsolateId,
}

/// A response for a `FreezeIsolateScopeRequest`, indicating the operation was successful.
#[derive(Debug)]
pub struct FreezeIsolateScopeResponse {}

/// A request to get the current data scope of an Isolate.
#[derive(Debug)]
pub struct GetIsolateScopeRequest {
    /// The ID of the Isolate to query.
    pub isolate_id: IsolateId,
}

/// A response for a `GetIsolateScopeRequest`.
#[derive(Debug)]
pub struct GetIsolateScopeResponse {
    /// The current data scope of the Isolate.
    pub current_scope: DataScopeType,
}

/// A request to add the data scope information from a manifest for a binary.
#[derive(Debug)]
pub struct AddManifestScopeRequest {
    /// The index of the binary's services.
    pub binary_services_index: BinaryServicesIndex,
    /// The maximum input data scope allowed for this binary.
    pub max_input_scope: DataScopeType,
    /// The maximum output data scope allowed for this binary.
    pub max_output_scope: DataScopeType,
}

/// A request to add backend dependencies for a binary from a manifest.
#[derive(Debug)]
pub struct AddBackendDependenciesRequest {
    /// The index of the binary's services.
    pub binary_services_index: BinaryServicesIndex,
    /// The index of the dependency service.
    pub dependency_index: IsolateServiceIndex,
}

/// A request to validate if a binary is allowed to call a backend dependency.
#[derive(Debug)]
pub struct ValidateBackendDependencyRequest {
    /// The index of the binary's services making the call.
    pub binary_services_index: BinaryServicesIndex,
    /// The index of the destination service being called.
    pub destination_isolate_service: IsolateServiceIndex,
}

/// A request to validate if a requested input scope is allowed by the manifest.
#[derive(Debug)]
pub struct ValidateManifestInputScopeRequest {
    /// The index of the binary's services.
    pub binary_services_index: BinaryServicesIndex,
    /// The input data scope being requested.
    pub requested_scope: DataScopeType,
}

/// A request to validate if an emitted output scope is allowed by the manifest.
#[derive(Debug)]
pub struct ValidateManifestOutputScopeRequest {
    /// The index of the binary's services.
    pub binary_services_index: BinaryServicesIndex,
    /// The output data scope being emitted.
    pub emitted_scope: DataScopeType,
}

/// A response for a `ValidateBackendDependencyRequest`, indicating the validation was successful.
#[derive(Debug)]
pub struct ValidateBackendDependencyResponse {}

/// A response for an `AddBackendDependenciesRequest`, indicating the operation was successful.
#[derive(Debug)]
pub struct AddBackendDependenciesResponse {}

/// A response for an `AddManifestScopeRequest`, indicating the operation was successful.
#[derive(Debug)]
pub struct AddManifestScopeResponse {}

/// A response for a `ValidateManifestInputScopeRequest`, indicating the validation was successful.
#[derive(Debug)]
pub struct ValidateManifestInputScopeResponse {}

/// A response for a `ValidateManifestOutputScopeRequest`, indicating the validation was successful.
#[derive(Debug)]
pub struct ValidateManifestOutputScopeResponse {}
