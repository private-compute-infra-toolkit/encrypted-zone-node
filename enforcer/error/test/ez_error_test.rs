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
use error_detail_proto::enforcer::v1::{ez_error_detail::ErrorReason, EzErrorDetail};
use error_details_proto::google::rpc::{bad_request::FieldViolation, BadRequest};
use ez_error::EzError;
use ez_error::RpcStatus;
use prost::Message;
use tonic::Code;

#[test]
fn test_to_tonic_status() {
    let message = "Bad Request Error".to_string();
    let code = Code::InvalidArgument;
    let source = ErrorSource::Enforcer;
    let error_reason = ErrorReason::Unspecified;

    let field = "field1".to_string();
    let description = "description1".to_string();
    let bad_request = BadRequest {
        field_violations: vec![FieldViolation {
            field: field.clone(),
            description: description.clone(),
            ..Default::default()
        }],
    };
    let error = EzError::EnforcerError(ez_error::EnforcerError {
        message: message.clone(),
        error_code: code,
        source,
        error_reason: Some(error_reason),
        bad_request: Some(bad_request),
    });
    let status = error.to_tonic_status();

    assert_eq!(status.message(), message);
    assert_eq!(status.code(), code);

    // Verify EzErrorDetail
    let status_details_bytes = status.details();
    let rpc_status = RpcStatus::decode(status_details_bytes).expect("Should decode RpcStatus");

    let ez_detail_any = rpc_status
        .details
        .iter()
        .find(|any| any.type_url == "type.googleapis.com/enforcer.v1.EzErrorDetail")
        .expect("Should find EzErrorDetail");
    let ez_detail =
        EzErrorDetail::decode(&ez_detail_any.value[..]).expect("Should decode EzErrorDetail");
    assert_eq!(ez_detail.source, source as i32);
    assert_eq!(ez_detail.error_reason, error_reason as i32);

    // Verify BadRequest
    let bad_req_any = rpc_status
        .details
        .iter()
        .find(|any| any.type_url == "type.googleapis.com/google.rpc.BadRequest")
        .expect("Should find BadRequest");
    let bad_req = BadRequest::decode(&bad_req_any.value[..]).expect("Should decode BadRequest");
    assert_eq!(bad_req.field_violations.len(), 1);
    assert_eq!(bad_req.field_violations[0].field, field);
    assert_eq!(bad_req.field_violations[0].description, description);
}
