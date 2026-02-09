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

use data_scope::data_scope_validator::{
    get_strictest_scope, replace_and_enforce_invoke_isolate_resp_scopes,
    replace_and_validate_invoke_ez_request_scopes, validate_external_call,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    EzPayloadIsolateScope, InvokeEzRequest, InvokeIsolateResponse, IsolateDataScope,
};

#[tokio::test]
async fn test_get_strictest_scope() {
    let mut iscopes = EzPayloadIsolateScope::default();
    assert_eq!(get_strictest_scope(&iscopes), DataScopeType::Unspecified);

    iscopes
        .datagram_iscopes
        .push(IsolateDataScope { scope_type: DataScopeType::Public.into(), ..Default::default() });
    assert_eq!(get_strictest_scope(&iscopes), DataScopeType::Public);

    iscopes.datagram_iscopes.push(IsolateDataScope {
        scope_type: DataScopeType::UserPrivate.into(),
        ..Default::default()
    });
    assert_eq!(get_strictest_scope(&iscopes), DataScopeType::UserPrivate);

    iscopes.datagram_iscopes.push(IsolateDataScope {
        scope_type: DataScopeType::Unspecified.into(),
        ..Default::default()
    });
    assert_eq!(get_strictest_scope(&iscopes), DataScopeType::Unspecified);
}

#[tokio::test]
async fn test_replace_and_enforce_invoke_isolate_resp_scopes() {
    // 1. Scope Dragging: Unspecified -> Current (UserPrivate)
    let mut resp = InvokeIsolateResponse {
        isolate_output_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Unspecified.into(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    replace_and_enforce_invoke_isolate_resp_scopes(&mut resp, DataScopeType::UserPrivate, false)
        .unwrap();
    assert_eq!(
        resp.isolate_output_iscope.unwrap().datagram_iscopes[0].scope_type,
        DataScopeType::UserPrivate as i32
    );

    // 2. Scope Escalation Check: Less strict than current -> Error
    let mut resp = InvokeIsolateResponse {
        isolate_output_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let result = replace_and_enforce_invoke_isolate_resp_scopes(
        &mut resp,
        DataScopeType::UserPrivate,
        false,
    );
    assert!(result.is_err());

    // 3. Public API Restriction: UserPrivate -> Error if from public API
    let mut resp = InvokeIsolateResponse {
        isolate_output_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::UserPrivate.into(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let result = replace_and_enforce_invoke_isolate_resp_scopes(
        &mut resp,
        DataScopeType::UserPrivate,
        true, // is_from_public_api
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn test_replace_and_validate_invoke_ez_request_scopes() {
    // 1. Scope Dragging: Unspecified -> Current (UserPrivate)
    let mut req = InvokeEzRequest {
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Unspecified.into(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    replace_and_validate_invoke_ez_request_scopes(&mut req, DataScopeType::UserPrivate);
    assert_eq!(
        req.isolate_request_iscope.unwrap().datagram_iscopes[0].scope_type,
        DataScopeType::UserPrivate as i32
    );

    // 2. Scope De-escalation Check: Less strict than current -> Error
    let mut req = InvokeEzRequest {
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let status =
        replace_and_validate_invoke_ez_request_scopes(&mut req, DataScopeType::UserPrivate);
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn test_validate_external_call() {
    // 1. Unspecified Scope -> Error
    let req = InvokeEzRequest {
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Unspecified.into(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let result = validate_external_call(&req);
    assert!(result.is_err());

    // 2. UserPrivate Scope -> Error
    let req = InvokeEzRequest {
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::UserPrivate.into(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let result = validate_external_call(&req);
    assert!(result.is_err());

    // 3. Public Scope -> OK
    let req = InvokeEzRequest {
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let result = validate_external_call(&req);
    assert!(result.is_ok());
}
