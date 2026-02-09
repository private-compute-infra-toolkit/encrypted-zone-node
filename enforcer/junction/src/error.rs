// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use error_detail_proto::enforcer::v1::ez_error_detail::ErrorSource;
use error_details_proto::google::rpc::BadRequest;
use ez_error_trait::ToEzError;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum IsolateStatusCode {
    // Retryable Errors
    #[error("Destination Isolate server unreachable")]
    DestinationChannelClosed,
    #[error("Failed to receive initial request")]
    EmptyStream,

    // Non-retryable Errors
    #[error("Request missing required ControlPlaneMetadata")]
    MissingControlPlaneMetadata,
    #[error("Request missing required field {0}")]
    MissingField(String),
    #[error("MemShare operation failed")]
    MemShareFailed,
    #[error("Enforcer to Isolate IPC failed")]
    EnforcerBridgeError,
    #[error("Internal error")]
    InternalError,
}

impl ToEzError for IsolateStatusCode {
    fn to_ez_error(&self) -> ez_error::EzError {
        let (code, error_reason, bad_request) = match self {
            // Unavailable Errors (Retryable)
            IsolateStatusCode::DestinationChannelClosed => (tonic::Code::Unavailable, None, None),
            IsolateStatusCode::EnforcerBridgeError => (tonic::Code::Unavailable, None, None),
            IsolateStatusCode::EmptyStream => (tonic::Code::Unavailable, None, None),

            IsolateStatusCode::MissingControlPlaneMetadata => {
                let bad_request = create_invalid_isolate_service_bad_request(vec![
                    // Can't use fully qualified field name here as it could come from EzPublicApi / EzToEz / InvokeIsolateRequest
                    "control_plane_metadata".to_string(),
                ]);
                (tonic::Code::InvalidArgument, None, Some(bad_request))
            }
            IsolateStatusCode::MissingField(field) => {
                let bad_request = create_invalid_isolate_service_bad_request(vec![field.clone()]);
                (tonic::Code::InvalidArgument, None, Some(bad_request))
            }

            // Internal Errors (Non-retryable)
            // Note:  MemShareFailure happens after Enforcer gets InvokeIsolateResponse,
            // this error goes to the client and we should not expose any information here.
            IsolateStatusCode::MemShareFailed => (tonic::Code::Internal, None, None),
            IsolateStatusCode::InternalError => (tonic::Code::Internal, None, None),
        };

        let source = match self {
            IsolateStatusCode::DestinationChannelClosed => ErrorSource::Isolate,
            IsolateStatusCode::MissingControlPlaneMetadata => ErrorSource::Isolate,
            IsolateStatusCode::MissingField(_) => ErrorSource::Enforcer,
            IsolateStatusCode::MemShareFailed => ErrorSource::Enforcer,
            IsolateStatusCode::EnforcerBridgeError => ErrorSource::EnforcerBridge,
            IsolateStatusCode::InternalError => ErrorSource::Enforcer,
            IsolateStatusCode::EmptyStream => ErrorSource::Enforcer,
        };

        let enforcer_error = ez_error::EnforcerError {
            message: self.to_string(),
            error_code: code,
            source,
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
            // Error msg already provides description and reason
            description: "".to_string(),
            reason: "".to_string(),
            localized_message: None,
        })
        .collect();

    BadRequest { field_violations }
}
