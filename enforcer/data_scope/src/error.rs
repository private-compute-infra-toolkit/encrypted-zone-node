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

use error_detail_proto::enforcer::v1::ez_error_detail::{ErrorReason, ErrorSource};
use error_details_proto::google::rpc::BadRequest;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum DataScopeError {
    // Retryable Errors
    #[error("No Isolates meet the requested DataScope criteria")]
    NoMatchingIsolates,

    // Non-retryable Errors
    #[error("Could not find a service matching the provided operator_domain & service_name")]
    InvalidIsolateServiceIndex,
    #[error("Internal Error: Provided Binary Service is duplicate")]
    DuplicateBinaryServiceIndex,
    #[error("Missing ControlPlaneMetadata in the InvokeIsolateRequest")]
    MissingControlPlaneMetadata,
    #[error("Unknown or unsupported DataScopeType provided")]
    InvalidDataScopeType,
    #[error("Current Isolate DataScope cannot exceed the maximum allowed DataScope defined in the EzManifest")]
    DisallowedByManifest,
    #[error("Communication to this Backend Dependency is not allowed as it is not defined in the EzManifest")]
    BackendDependencyNotAllowed,
    #[error("Isolate attempted to escalate scope. Emitted: {emitted}, Current: {current}")]
    PayloadExceedsCurrentDataScope { emitted: String, current: String },
    #[error("Private DataScope emitted via public API. Emitted DataScope: {emitted}")]
    PublicApiScopeViolation { emitted: String },
    #[error("Internal Error: Isolate not recognized")]
    UnknownIsolateId,
    #[error("Internal Error: {0}")]
    InternalError(String),
}

use ez_error_trait::ToEzError;

impl ToEzError for DataScopeError {
    fn to_ez_error(&self) -> ez_error::EzError {
        let (code, error_reason, bad_request) = match self {
            // ResourceExhausted Errors (Retryable)
            DataScopeError::NoMatchingIsolates => {
                let error_reason = ErrorReason::NoIsolatesWithRequestedDataScope;
                (tonic::Code::ResourceExhausted, Some(error_reason), None)
            }

            // InvalidArgument Errors (Non-retryable)
            DataScopeError::InvalidIsolateServiceIndex => {
                let bad_request = create_invalid_isolate_service_bad_request(vec![
                    "operator_domain".to_string(),
                    "service_name".to_string(),
                ]);
                (tonic::Code::InvalidArgument, None, Some(bad_request))
            }
            DataScopeError::DuplicateBinaryServiceIndex => {
                let bad_request = create_invalid_isolate_service_bad_request(vec![
                    "enforcer.EzManifest.binary_manifest.ez_backend_dependencies".to_string(),
                ]);
                (tonic::Code::InvalidArgument, None, Some(bad_request))
            }
            DataScopeError::MissingControlPlaneMetadata => {
                let bad_request = create_invalid_isolate_service_bad_request(vec![
                    "control_plane_metadata".to_string(),
                ]);
                (tonic::Code::InvalidArgument, None, Some(bad_request))
            }
            DataScopeError::InvalidDataScopeType => {
                let bad_request = create_invalid_isolate_service_bad_request(vec![
                    "enforcer.v1.DataScopeType".to_string(),
                ]);
                let error_reason = ErrorReason::InvalidDataScope;
                (tonic::Code::InvalidArgument, Some(error_reason), Some(bad_request))
            }

            // PermissionDenied Errors (Non-retryable)
            DataScopeError::DisallowedByManifest => {
                let error_reason = ErrorReason::DisallowedByManifest;
                (tonic::Code::PermissionDenied, Some(error_reason), None)
            }
            DataScopeError::BackendDependencyNotAllowed => {
                let error_reason = ErrorReason::CommunicationNotAllowed;
                (tonic::Code::PermissionDenied, Some(error_reason), None)
            }
            DataScopeError::PayloadExceedsCurrentDataScope { .. } => {
                let error_reason = ErrorReason::RequestedPayloadDataScopeUnsafe;
                (tonic::Code::PermissionDenied, Some(error_reason), None)
            }
            DataScopeError::PublicApiScopeViolation { .. } => {
                let error_reason = ErrorReason::DisallowedEgress;
                (tonic::Code::PermissionDenied, Some(error_reason), None)
            }

            // Internal Errors (Non-Retryable). Ideally these are never returned to clients.
            DataScopeError::InternalError(_) => (tonic::Code::Internal, None, None),
            DataScopeError::UnknownIsolateId => (tonic::Code::Internal, None, None),
        };

        let enforcer_error = ez_error::EnforcerError {
            message: self.to_string(),
            error_code: code,
            source: ErrorSource::Enforcer,
            error_reason,
            bad_request,
        };
        ez_error::EzError::EnforcerError(enforcer_error)
    }
}

fn create_invalid_isolate_service_bad_request(fields: Vec<String>) -> BadRequest {
    let field_violations = fields
        .into_iter()
        .map(|field| error_details_proto::google::rpc::bad_request::FieldViolation {
            field,
            description: "Invalid IsolateService Info".to_string(),
            reason: "".to_string(), // High level error msg already contains reason
            localized_message: None,
        })
        .collect();

    BadRequest { field_violations }
}
