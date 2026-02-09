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

use error_detail_proto::enforcer::v1::ez_error_detail::{ErrorReason, ErrorSource};
use error_detail_proto::enforcer::v1::EzErrorDetail;
use error_details_proto::google::rpc::BadRequest;
pub use googleapis_tonic_google_rpc::google::rpc::Status as RpcStatus;
use prost::bytes::Bytes;
use prost::Message;
use prost_types::Any;
use std::fmt;

/// EzError enum that either holds an Enforcer generated error or a tonic::Status
/// coming from IsolateBridge. This enum helps unify both types. For example: This is used
/// by Junction to return errors to the clients.
#[derive(Debug)]
pub enum EzError {
    EnforcerError(EnforcerError),
    Status(tonic::Status),
}

/// Enforcer Error struct that can be use by Enforcer Rust modules. This will
/// eventually be converted to a tonic::Status when sending back to the clients
/// (over the wire). Whenever the error is generated, correct error code (implying whether the error
/// is retryable or not) and source should be set.
#[derive(Debug)]
pub struct EnforcerError {
    pub message: String,
    pub error_code: tonic::Code,
    pub source: ErrorSource,
    pub error_reason: Option<ErrorReason>,
    pub bad_request: Option<BadRequest>,
}

impl EnforcerError {
    /// Convert to tonic::Status from EzError. EZ specific error details are incorporated into
    /// [EzErrorDetail](error_detail_proto::enforcer::v1::EzErrorDetail) and added to Status.details.
    /// Other non-EZ specified error details are also added to Status.details array
    /// (for instance google.rpc.BadRequest).
    pub fn to_tonic_status(self) -> tonic::Status {
        let mut details = Vec::new();

        let error_detail = EzErrorDetail {
            source: self.source.into(),
            error_reason: self.error_reason.map(|r| r as i32).unwrap_or(0),
        };
        let error_detail_encoded = error_detail.encode_to_vec();
        let any_detail = Any {
            type_url: "type.googleapis.com/enforcer.v1.EzErrorDetail".to_string(),
            value: error_detail_encoded,
        };
        details.push(any_detail);

        if let Some(bad_req) = self.bad_request {
            let bad_req_encoded = bad_req.encode_to_vec();
            let any_detail = Any {
                type_url: "type.googleapis.com/google.rpc.BadRequest".to_string(),
                value: bad_req_encoded,
            };
            details.push(any_detail);
        }

        let rpc_status =
            RpcStatus { code: i32::from(self.error_code), message: self.message.clone(), details };
        let status_bytes = rpc_status.encode_to_vec();

        tonic::Status::with_details(self.error_code, self.message, Bytes::from(status_bytes))
    }
}

impl EzError {
    /// Convert to tonic::Status from EzError.
    pub fn to_tonic_status(self) -> tonic::Status {
        match self {
            EzError::EnforcerError(e) => e.to_tonic_status(),
            EzError::Status(s) => s,
        }
    }
}

impl std::error::Error for EnforcerError {}
impl std::error::Error for EzError {}

impl fmt::Display for EnforcerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Display for EzError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EzError::EnforcerError(e) => write!(f, "{}", e),
            EzError::Status(s) => write!(f, "{}", s),
        }
    }
}
