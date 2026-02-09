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
use anyhow::Result;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{EzPayloadIsolateScope, InvokeEzRequest, InvokeIsolateResponse};
use tonic::Status;

/// Validates and modifies the output scopes of an `InvokeIsolateResponse`. This should only be
/// called for Opaque Isolates.
///
/// This function performs several checks and modifications on the `isolate_output_iscope`
/// of a response from an Isolate:
///
/// 1.  **Scope Dragging**: If an output datagram has an `Unspecified` scope, it is updated
///     to match the Isolate's `current_scope`.
/// 2.  **Scope Escalation Check**: It ensures that an Isolate does not emit a scope that is
///     less strict than its `current_scope`. Doing so would be a security violation.
/// 3.  **Public API Restriction**: If the request originated from the public API, it prevents
///     the Isolate from emitting scopes of `UserPrivate` or higher to avoid leaking
///     sensitive data scopes externally.
///
/// # Arguments
/// * `invoke_isolate_resp` - A mutable reference to the response to validate and modify.
/// * `current_scope` - The data scope the Isolate was operating in when it generated the response.
/// * `is_from_public_api` - A boolean indicating if the original request came from the public API.
pub fn replace_and_enforce_invoke_isolate_resp_scopes(
    invoke_isolate_resp: &mut InvokeIsolateResponse,
    current_scope: DataScopeType,
    is_from_public_api: bool,
) -> Result<(), DataScopeError> {
    if let Some(ref mut iscopes) = &mut invoke_isolate_resp.isolate_output_iscope {
        for iscope in iscopes.datagram_iscopes.iter_mut() {
            if iscope.scope_type == DataScopeType::Unspecified.into() {
                iscope.scope_type = current_scope.into();
            } else if iscope.scope_type < current_scope.into() {
                return Err(crate::error::DataScopeError::PayloadExceedsCurrentDataScope {
                    emitted: DataScopeType::try_from(iscope.scope_type)
                        .unwrap_or(DataScopeType::Unspecified)
                        .as_str_name()
                        .to_string(),
                    current: current_scope.as_str_name().to_string(),
                });
            }
            if is_from_public_api && iscope.scope_type >= DataScopeType::UserPrivate.into() {
                return Err(crate::error::DataScopeError::PublicApiScopeViolation {
                    emitted: DataScopeType::try_from(iscope.scope_type)
                        .unwrap_or(DataScopeType::Unspecified)
                        .as_str_name()
                        .to_string(),
                });
            }
        }
    }

    Ok(())
}

/// Enforces data scope restrictions on `InvokeIsolateResponse` for public API calls.
///
/// This function ensures that an Isolate responding to a public API call does not emit
/// any data scopes that are `UserPrivate` or higher. This prevents sensitive data from
/// being leaked through the public API.
///
/// # Arguments
/// * `invoke_isolate_resp` - A mutable reference to the response to validate.
///
/// # Returns
/// A `Result` which is `Ok` if validation passes, or an `Error` if a scope violation is detected.
pub fn enforce_public_api_invoke_isolate_resp_scopes(
    invoke_isolate_resp: &mut InvokeIsolateResponse,
) -> Result<(), DataScopeError> {
    if let Some(ref mut iscopes) = &mut invoke_isolate_resp.isolate_output_iscope {
        for iscope in iscopes.datagram_iscopes.iter() {
            if iscope.scope_type >= DataScopeType::UserPrivate.into() {
                return Err(crate::error::DataScopeError::PublicApiScopeViolation {
                    emitted: DataScopeType::try_from(iscope.scope_type)
                        .unwrap_or(DataScopeType::Unspecified)
                        .as_str_name()
                        .to_string(),
                });
            }
        }
    }

    Ok(())
}

/// Validates and modifies the input scopes of an `InvokeEzRequest`. This should only be
/// called for Opaque Isolates.
///
/// This function is called when one EZ Isolate makes a request to another. It performs
/// several checks and modifications on the `isolate_request_iscope` of the request:
///
/// 1.  **Scope Dragging**: If an input datagram has an `Unspecified` scope, it is updated
///     to match the calling Isolate's `current_scope`.
/// 2.  **Scope De-escalation Check**: It ensures that an Isolate does not send data with a
///     scope that is less strict than its own `current_scope`.
///
/// # Arguments
/// * `invoke_ez_req` - A mutable reference to the request to validate and modify.
/// * `current_scope` - The data scope of the calling Isolate.
///
/// # Returns
/// A `tonic::Status` which is `Ok` if validation passes, or `PermissionDenied` if a scope
/// violation is detected.
/// replace_and_validate_invoke_ez_request_scopes
pub fn replace_and_validate_invoke_ez_request_scopes(
    invoke_ez_req: &mut InvokeEzRequest,
    current_scope: DataScopeType,
) -> Status {
    if let Some(ref mut iscopes) = &mut invoke_ez_req.isolate_request_iscope {
        for iscope in iscopes.datagram_iscopes.iter_mut() {
            if iscope.scope_type == DataScopeType::Unspecified.into() {
                iscope.scope_type = current_scope.into();
            } else if iscope.scope_type < current_scope.into() {
                return Status::permission_denied(format!(
                    "Cannot emit scope {} while being in scope {}",
                    DataScopeType::try_from(iscope.scope_type)
                        .unwrap_or(DataScopeType::Unspecified)
                        .as_str_name(),
                    current_scope.as_str_name()
                ));
            }
        }
    }

    Status::ok("")
}

/// Calculates the strictest (highest) data scope from a collection of datagram scopes.
///
/// This function iterates through all the datagram scopes within the provided `EzPayloadIsolateScope`
/// and determines the one with the highest value. If any of the scopes are `Unspecified`, this
/// function will immediately return `DataScopeType::Unspecified`. Otherwise, it returns the
/// strictest (highest) scope found.
///
/// # Arguments
/// * `iscopes` - A reference to an `EzPayloadIsolateScope` containing the datagram scopes to evaluate.
///
/// # Returns
/// The `DataScopeType` representing the strictest scope. Returns `DataScopeType::Unspecified`
/// if the input contains no scopes or if any scope is `Unspecified`.
pub fn get_strictest_scope(iscopes: &EzPayloadIsolateScope) -> DataScopeType {
    if iscopes.datagram_iscopes.iter().any(|s| s.scope_type() == DataScopeType::Unspecified) {
        return DataScopeType::Unspecified;
    }
    iscopes
        .datagram_iscopes
        .iter()
        .map(|s| s.scope_type())
        .max()
        .unwrap_or(DataScopeType::Unspecified)
}

/// Validates external service invocation requests. This should only be called for Opaque Isolates.
///
/// This function checks if an Isolate is allowed to invoke an external service based on the
/// data scopes attached to the request.
///
/// 1.  **Unspecified Scope Check**: If any datagram has an `Unspecified` scope, the request is denied.
/// 2.  **External Call Restriction**: It ensures that an Isolate does not invoke external services
///     while holding data with scope `UserPrivate` or higher. External calls are only permitted
///     for less sensitive scopes to prevent data leakage.
///
/// # Arguments
/// * `req` - A reference to the `InvokeEzRequest` to validate.
///
/// # Returns
/// A `Result` which is `Ok` if validation passes, or an `Error` if a scope violation is detected.
pub fn validate_external_call(req: &InvokeEzRequest) -> Result<()> {
    if let Some(ref iscopes) = &req.isolate_request_iscope {
        for iscope in iscopes.datagram_iscopes.iter() {
            if iscope.scope_type == DataScopeType::Unspecified.into() {
                anyhow::bail!("Cannot emit scope Unspecified while being in scope Unspecified");
            } else if iscope.scope_type >= DataScopeType::UserPrivate.into() {
                anyhow::bail!(format!(
                    "Cannot invoke external service while being on {} scope",
                    DataScopeType::try_from(iscope.scope_type)
                        .unwrap_or(DataScopeType::Unspecified)
                        .as_str_name()
                ));
            }
        }
    }

    Ok(())
}
