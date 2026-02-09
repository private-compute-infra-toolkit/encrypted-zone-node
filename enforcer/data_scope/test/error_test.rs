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

use data_scope::error::DataScopeError;
use ez_error_trait::ToEzError;

// use error_detail_proto::enforcer::v1::ez_error_detail::Error as EzDetailError;
use error_detail_proto::enforcer::v1::ez_error_detail::ErrorReason;
use error_detail_proto::enforcer::v1::ez_error_detail::ErrorSource;

use tonic::Code;

#[test]
fn test_to_ez_error_conversion() {
    // Test InvalidArgument conversion
    let err = DataScopeError::InvalidDataScopeType;
    let ez_err = err.to_ez_error();
    if let ez_error::EzError::EnforcerError(ez_err) = ez_err {
        assert_eq!(ez_err.error_code, Code::InvalidArgument);
        assert_eq!(ez_err.source, ErrorSource::Enforcer);
        if let Some(bad_request) = ez_err.bad_request {
            assert_eq!(bad_request.field_violations.len(), 1);
            assert_eq!(bad_request.field_violations[0].field, "enforcer.v1.DataScopeType");
        } else {
            panic!("Expected BadRequest details for InvalidDataScopeType");
        }
        if let Some(reason) = ez_err.error_reason {
            assert_eq!(reason, ErrorReason::InvalidDataScope);
        } else {
            panic!("Expected ErrorReason details for InvalidDataScopeType");
        }
    } else {
        panic!("Expected EnforcerError");
    }

    // Test NotFound conversion
    let err = DataScopeError::NoMatchingIsolates;
    let ez_err = err.to_ez_error();
    if let ez_error::EzError::EnforcerError(ez_err) = ez_err {
        assert_eq!(ez_err.error_code, Code::ResourceExhausted);
        assert_eq!(ez_err.source, ErrorSource::Enforcer);
        if let Some(reason) = ez_err.error_reason {
            assert_eq!(reason, ErrorReason::NoIsolatesWithRequestedDataScope);
        } else {
            panic!("Expected ErrorReason details for NoMatchingIsolates");
        }
    } else {
        panic!("Expected EnforcerError");
    }

    // Test Internal conversion
    let err = DataScopeError::InternalError("Test internal error".to_string());
    let ez_err = err.to_ez_error();
    if let ez_error::EzError::EnforcerError(ez_err) = ez_err {
        assert_eq!(ez_err.error_code, Code::Internal);
        assert_eq!(ez_err.source, ErrorSource::Enforcer);
    } else {
        panic!("Expected EnforcerError");
    }

    // Test UnknownIsolateId conversion
    let err = DataScopeError::UnknownIsolateId;
    let ez_err = err.to_ez_error();
    if let ez_error::EzError::EnforcerError(ez_err) = ez_err {
        assert_eq!(ez_err.error_code, Code::Internal);
        assert_eq!(ez_err.source, ErrorSource::Enforcer);
    } else {
        panic!("Expected EnforcerError");
    }

    // Test DuplicateBinaryServiceIndex conversion
    let err = DataScopeError::DuplicateBinaryServiceIndex;
    let ez_err = err.to_ez_error();
    if let ez_error::EzError::EnforcerError(ez_err) = ez_err {
        assert_eq!(ez_err.error_code, Code::InvalidArgument);
        if let Some(bad_request) = ez_err.bad_request {
            assert_eq!(bad_request.field_violations.len(), 1);
            assert_eq!(
                bad_request.field_violations[0].field,
                "enforcer.EzManifest.binary_manifest.ez_backend_dependencies"
            );
        } else {
            panic!("Expected BadRequest details for DuplicateBinaryServiceIndex");
        }
    } else {
        panic!("Expected EnforcerError");
    }

    // Test MissingControlPlaneMetadata conversion
    let err = DataScopeError::MissingControlPlaneMetadata;
    let ez_err = err.to_ez_error();
    if let ez_error::EzError::EnforcerError(ez_err) = ez_err {
        assert_eq!(ez_err.error_code, Code::InvalidArgument);
        if let Some(bad_request) = ez_err.bad_request {
            assert_eq!(bad_request.field_violations.len(), 1);
            assert_eq!(bad_request.field_violations[0].field, "control_plane_metadata");
        } else {
            panic!("Expected BadRequest details for MissingControlPlaneMetadata");
        }
    } else {
        panic!("Expected EnforcerError");
    }
}
