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
    AddBackendDependenciesRequest, AddManifestScopeRequest, ValidateBackendDependencyRequest,
    ValidateBackendDependencyResponse, ValidateManifestInputScopeRequest,
    ValidateManifestOutputScopeRequest,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use isolate_info::{BinaryServicesIndex, IsolateServiceIndex};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy)]
struct ManifestScopeInfo {
    max_input_scope: DataScopeType,
    max_output_scope: DataScopeType,
}

/// Maps each allowed backend dependency service index to the optional `target_label`
/// configured for that dependency in the manifest.
type BackendDependencyMap = HashMap<IsolateServiceIndex, Option<String>>;

/// Minimum and maximum allowed length (in bytes) for a non-empty `target_label`.
const MIN_TARGET_LABEL_LEN: usize = 2;
const MAX_TARGET_LABEL_LEN: usize = 63;

fn is_valid_target_label(label: &str) -> bool {
    label.is_empty()
        || ((MIN_TARGET_LABEL_LEN..=MAX_TARGET_LABEL_LEN).contains(&label.len())
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label.bytes().all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'-')))
}

/// Validates data scopes and backend dependencies based on the information provided in the manifest.
///
/// This validator is responsible for ensuring that Isolates operate within their declared
/// data scope boundaries and only communicate with their explicitly allowed backend dependencies.
/// Clients are encouraged to clone [ManifestValidator].
#[derive(Debug, Default, Clone)]
pub struct ManifestValidator {
    scope_info: Arc<RwLock<HashMap<BinaryServicesIndex, ManifestScopeInfo>>>,
    backend_dependencies: Arc<RwLock<HashMap<BinaryServicesIndex, BackendDependencyMap>>>,
}

impl ManifestValidator {
    /// Adds the maximum allowed input and output scopes for a given binary.
    ///
    /// This information is extracted from the manifest and used for subsequent validation.
    /// Returns [DataScopeError::DuplicateBinaryServiceIndex] if the binary is already registered.
    /// Returns [DataScopeError::InvalidDataScopeType] if either scope is `Unspecified`.
    pub async fn add_scope_info(&self, req: AddManifestScopeRequest) -> Result<(), DataScopeError> {
        let mut scope_info = self.scope_info.write().await;
        if let Entry::Vacant(entry) = scope_info.entry(req.binary_services_index) {
            if req.max_input_scope == DataScopeType::Unspecified
                || req.max_output_scope == DataScopeType::Unspecified
            {
                return Err(DataScopeError::InvalidDataScopeType);
            }
            entry.insert(ManifestScopeInfo {
                max_input_scope: req.max_input_scope,
                max_output_scope: req.max_output_scope,
            });
        } else {
            return Err(DataScopeError::DuplicateBinaryServiceIndex);
        }

        Ok(())
    }

    /// Registers a backend dependency and its optional `target_label` for a given binary.
    ///
    /// This allows the binary to make calls to the specified dependency.
    /// Returns [`DataScopeError::InvalidTargetLabel`] if `target_label` is not 2-63 characters of
    /// `[a-z0-9-]` starting and ending with `[a-z0-9]`.
    /// Returns [`DataScopeError::ConflictingTargetLabel`] if the same destination dependency is
    /// already registered for the binary with a different `target_label`.
    pub async fn add_backend_dependencies(
        &self,
        req: AddBackendDependenciesRequest,
    ) -> Result<(), DataScopeError> {
        if let Some(ref label) = req.target_label {
            if !is_valid_target_label(label) {
                return Err(DataScopeError::InvalidTargetLabel);
            }
        }

        let mut backend_dependencies = self.backend_dependencies.write().await;
        let dependencies =
            backend_dependencies.entry(req.binary_services_index).or_insert_with(HashMap::new);
        match dependencies.entry(req.dependency_index) {
            Entry::Occupied(existing) => {
                if existing.get().as_deref().unwrap_or("")
                    != req.target_label.as_deref().unwrap_or("")
                {
                    return Err(DataScopeError::ConflictingTargetLabel);
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(req.target_label);
            }
        }

        Ok(())
    }

    /// Removes all registered backend dependencies for a given binary.
    ///
    /// Isolates are only loaded once during manifest registration (with a fresh
    /// [`BinaryServicesIndex`]) before any replicas run, so clearing on registration failure
    /// cannot affect live replicas.
    pub async fn clear_backend_dependencies(&self, binary_services_index: BinaryServicesIndex) {
        let mut backend_dependencies = self.backend_dependencies.write().await;
        backend_dependencies.remove(&binary_services_index);
    }

    /// Validates if a binary is allowed to call a specific backend dependency and returns the
    /// response containing the optional `target_label` configured in the manifest.
    ///
    /// Returns [DataScopeError::BackendDependencyNotAllowed] if the dependency is not registered for the binary.
    /// Returns [DataScopeError::InvalidIsolateServiceIndex] if the binary is not recognized.
    pub async fn validate_backend_dependency(
        &self,
        req: ValidateBackendDependencyRequest,
    ) -> Result<ValidateBackendDependencyResponse, DataScopeError> {
        let backend_dependencies = self.backend_dependencies.read().await;
        if let Some(dependencies) = backend_dependencies.get(&req.binary_services_index) {
            if let Some(target_label) = dependencies.get(&req.destination_isolate_service) {
                return Ok(ValidateBackendDependencyResponse {
                    target_label: target_label.clone(),
                });
            } else {
                return Err(DataScopeError::BackendDependencyNotAllowed);
            }
        }
        Err(DataScopeError::InvalidIsolateServiceIndex)
    }

    /// Validates if a requested input data scope is within the allowed limits for a binary.
    ///
    /// Returns [DataScopeError::DataScopeNotAllowed] if the requested scope is stricter than the
    /// maximum input scope defined in the manifest.
    /// Returns [DataScopeError::InvalidIsolateServiceIndex] if the binary is not recognized.
    pub async fn validate_input_scope(
        &self,
        req: ValidateManifestInputScopeRequest,
    ) -> Result<(), DataScopeError> {
        let scope_info = self.scope_info.read().await;
        if let Some(manifest_scope_info) = scope_info.get(&req.binary_services_index) {
            if req.requested_scope != DataScopeType::Unspecified
                && req.requested_scope <= manifest_scope_info.max_input_scope
            {
                return Ok(());
            } else {
                return Err(DataScopeError::DisallowedByManifest);
            }
        }
        Err(DataScopeError::InvalidIsolateServiceIndex)
    }

    /// Validates if an emitted output data scope is within the allowed limits for a binary.
    ///
    /// Returns [DataScopeError::DataScopeNotAllowed] if the emitted scope is stricter than the
    /// maximum output scope defined in the manifest.
    /// Returns [DataScopeError::InvalidIsolateServiceIndex] if the binary is not recognized.
    pub async fn validate_output_scope(
        &self,
        req: ValidateManifestOutputScopeRequest,
    ) -> Result<(), DataScopeError> {
        let scope_info = self.scope_info.read().await;
        if let Some(manifest_scope_info) = scope_info.get(&req.binary_services_index) {
            if req.emitted_scope != DataScopeType::Unspecified
                && req.emitted_scope <= manifest_scope_info.max_output_scope
            {
                return Ok(());
            } else {
                return Err(DataScopeError::DisallowedByManifest);
            }
        }
        Err(DataScopeError::InvalidIsolateServiceIndex)
    }
}
