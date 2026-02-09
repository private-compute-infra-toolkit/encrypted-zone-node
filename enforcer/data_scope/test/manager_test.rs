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
use data_scope::manifest_validator::ManifestValidator;
use data_scope::request::{
    AddBackendDependenciesRequest, AddIsolateRequest, AddManifestScopeRequest,
    FreezeIsolateScopeRequest, GetIsolateRequest, GetIsolateScopeRequest, RemoveIsolateRequest,
    ValidateBackendDependencyRequest, ValidateIsolateRequest, ValidateManifestInputScopeRequest,
    ValidateManifestOutputScopeRequest,
};
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::IsolateState;
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceIndex};
use once_cell::sync::Lazy;

static TEST_BINARY_SERVICES_INDEX: Lazy<BinaryServicesIndex> =
    Lazy::new(|| BinaryServicesIndex::new(false));
static TEST_RATIFIED_BINARY_SERVICES_INDEX: Lazy<BinaryServicesIndex> =
    Lazy::new(|| BinaryServicesIndex::new(true));
static TEST_REMOTE_ISOLATE_SERVICE_INDEX: Lazy<IsolateServiceIndex> = Lazy::new(|| {
    IsolateServiceIndex::new(None, "remote.service", false)
        .expect("Expect a valid isolate service index")
});

const TEST_SENSITIVE_SESSION_THRESHOLD: u64 = 2;

#[tokio::test]
async fn test_add_isolate() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    for is_ratified in [true, false] {
        let add_isolate_request = create_add_isolate_request(is_ratified);
        let old_isolate_id = add_isolate_request.isolate_id;
        data_scope_requester.add_isolate(add_isolate_request).await?;

        let mut add_isolate_request_2 = create_add_isolate_request(is_ratified);
        add_isolate_request_2.isolate_id = old_isolate_id;

        assert!(matches!(
            data_scope_requester.add_isolate(add_isolate_request_2).await,
            Err(DataScopeError::InternalError(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn test_remove_isolate() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    for is_ratified in [true, false] {
        let add_isolate_request = create_add_isolate_request(is_ratified);
        let isolate_id = add_isolate_request.isolate_id;
        let _ = data_scope_requester.add_isolate(add_isolate_request).await?;

        let remove_isolate_request = create_remove_isolate_request(isolate_id);
        let response = data_scope_requester.remove_isolate(remove_isolate_request).await?;
        assert_eq!(response.isolate_id, isolate_id);

        let remove_isolate_request = create_remove_isolate_request(isolate_id);
        assert!(matches!(
            data_scope_requester.remove_isolate(remove_isolate_request).await,
            Err(DataScopeError::UnknownIsolateId)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn test_get_isolate_success() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    for is_ratified in [true, false] {
        let mut add_isolate_request: AddIsolateRequest = create_add_isolate_request(is_ratified);
        add_isolate_request.allowed_data_scope_type = DataScopeType::Public;
        let isolate_id = add_isolate_request.isolate_id;
        data_scope_requester.add_isolate(add_isolate_request).await?;
        let get_isolate_request = create_get_isolate_request(is_ratified);
        let get_isolate_result = data_scope_requester.get_isolate(get_isolate_request).await?;
        assert_eq!(get_isolate_result.isolate_id, isolate_id);
    }
    Ok(())
}

#[tokio::test]
async fn test_get_isolate_invalid_service_index() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let random_binary_service_index = BinaryServicesIndex::new(true);
    for is_ratified in [true, false] {
        let mut add_isolate_request: AddIsolateRequest = create_add_isolate_request(is_ratified);
        add_isolate_request.allowed_data_scope_type = DataScopeType::Public;
        data_scope_requester.add_isolate(add_isolate_request).await?;
        let mut get_isolate_request = create_get_isolate_request(is_ratified);
        get_isolate_request.binary_services_index = random_binary_service_index;
        assert!(matches!(
            data_scope_requester.get_isolate(get_isolate_request).await,
            Err(DataScopeError::InvalidIsolateServiceIndex)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn test_get_isolate_no_matching_isolates() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    for is_ratified in [true, false] {
        let mut add_isolate_request: AddIsolateRequest = create_add_isolate_request(is_ratified);
        add_isolate_request.allowed_data_scope_type = DataScopeType::Public;
        data_scope_requester.add_isolate(add_isolate_request).await?;
        let mut get_isolate_request = create_get_isolate_request(is_ratified);
        get_isolate_request.data_scope_type = DataScopeType::UserPrivate;
        assert!(matches!(
            data_scope_requester.get_isolate(get_isolate_request).await,
            Err(DataScopeError::NoMatchingIsolates)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn test_get_isolate_invalid_data_scope_type() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    for is_ratified in [true, false] {
        let mut add_isolate_request: AddIsolateRequest = create_add_isolate_request(is_ratified);
        add_isolate_request.allowed_data_scope_type = DataScopeType::Public;
        data_scope_requester.add_isolate(add_isolate_request).await?;
        let mut get_isolate_request = create_get_isolate_request(is_ratified);
        get_isolate_request.data_scope_type = DataScopeType::MultiUserPrivate;
        assert!(matches!(
            data_scope_requester.get_isolate(get_isolate_request).await,
            Err(DataScopeError::InvalidDataScopeType)
        ));
        let mut get_isolate_request = create_get_isolate_request(is_ratified);
        get_isolate_request.data_scope_type = DataScopeType::Unspecified;
        assert!(matches!(
            data_scope_requester.get_isolate(get_isolate_request).await,
            Err(DataScopeError::InvalidDataScopeType)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn test_freeze_isolate() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    let add_isolate_request: AddIsolateRequest = create_add_isolate_request(false);
    let isolate_id = add_isolate_request.isolate_id;
    let _ = data_scope_requester.add_isolate(add_isolate_request).await?;

    let freeze_isolate_request = create_freeze_isolate_request(isolate_id);
    let _ = data_scope_requester.freeze_isolate_scope(freeze_isolate_request).await?;
    Ok(())
}

#[tokio::test]
async fn test_add_isolate_error_too_strict_scope() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    let mut add_isolate_request: AddIsolateRequest = create_add_isolate_request(false);
    // Request too strict scope
    add_isolate_request.current_data_scope_type = DataScopeType::Sealed;
    let result = data_scope_requester.add_isolate(add_isolate_request).await;
    assert!(result.is_err());
    assert!(matches!(result, Err(DataScopeError::DisallowedByManifest)));
    Ok(())
}

// Requests with UnknownDataScopeType should be errors
#[tokio::test]
async fn test_validate_isolate_scope_unknown_scope() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    for is_ratified in [true, false] {
        let add_isolate_request = create_add_isolate_request(is_ratified);
        let isolate_id = add_isolate_request.isolate_id;
        assert!(data_scope_requester.add_isolate(add_isolate_request).await.is_ok());

        let validate_request =
            create_validate_isolate_request(isolate_id, DataScopeType::Unspecified);
        let result = data_scope_requester.validate_isolate_scope(validate_request).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DataScopeError::InvalidDataScopeType)));
    }
    Ok(())
}

// We should error if the requested scope is stricter than our maximum allowed scope
#[tokio::test]
async fn test_validate_isolate_scope_too_strict() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    for is_ratified in [true, false] {
        let mut add_isolate_request = create_add_isolate_request(is_ratified);
        add_isolate_request.allowed_data_scope_type = DataScopeType::DomainOwned;
        let isolate_id = add_isolate_request.isolate_id;
        assert!(data_scope_requester.add_isolate(add_isolate_request).await.is_ok());

        let validate_request =
            create_validate_isolate_request(isolate_id, DataScopeType::UserPrivate);
        let validation_result = data_scope_requester.validate_isolate_scope(validate_request).await;
        assert!(matches!(validation_result, Err(DataScopeError::DisallowedByManifest)));
    }
    Ok(())
}

#[tokio::test]
async fn test_validate_isolate_scope_unknown_isolate() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    for is_ratified in [true, false] {
        let add_isolate_request = create_add_isolate_request(is_ratified);
        assert!(data_scope_requester.add_isolate(add_isolate_request).await.is_ok());

        let binary_services_index = if is_ratified {
            *TEST_RATIFIED_BINARY_SERVICES_INDEX
        } else {
            *TEST_BINARY_SERVICES_INDEX
        };

        let validate_request = create_validate_isolate_request(
            IsolateId::new(binary_services_index),
            DataScopeType::DomainOwned,
        );
        let validation_result = data_scope_requester.validate_isolate_scope(validate_request).await;
        assert!(matches!(validation_result, Err(DataScopeError::UnknownIsolateId)));
    }
    Ok(())
}

// If requested scope is less strict than current, don't modify current scope
#[tokio::test]
async fn test_validate_isolate_scope_less_strict_than_current(
) -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    let mut add_isolate_request = create_add_isolate_request(false);
    add_isolate_request.current_data_scope_type = DataScopeType::DomainOwned;
    add_isolate_request.allowed_data_scope_type = DataScopeType::UserPrivate;
    let isolate_id = add_isolate_request.isolate_id;
    assert!(data_scope_requester.add_isolate(add_isolate_request).await.is_ok());

    // Validate with a less strict scope
    let validate_request = create_validate_isolate_request(isolate_id, DataScopeType::Public);
    assert!(data_scope_requester.validate_isolate_scope(validate_request).await.is_ok());

    // The scope should not have changed. We can still get it for DomainOwned.
    let mut get_isolate_request = create_get_isolate_request(false);
    get_isolate_request.data_scope_type = DataScopeType::DomainOwned;
    let get_isolate_result = data_scope_requester.get_isolate(get_isolate_request).await?;
    assert_eq!(get_isolate_result.isolate_id, isolate_id);

    Ok(())
}

// When we process a DataScope > current, we should update our scope to the stricter requested scope
// Here we verify that the current scope was updated with a stricter validate
#[tokio::test]
async fn test_validate_isolate_scope_valid_change() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    let add_isolate_request = create_add_isolate_request(false); //[PUBLIC, USER]
    let isolate_id = add_isolate_request.isolate_id;
    assert!(data_scope_requester.add_isolate(add_isolate_request).await.is_ok());

    // Validate with a stricter scope that is allowed
    let validate_request = create_validate_isolate_request(isolate_id, DataScopeType::DomainOwned);
    assert!(data_scope_requester.validate_isolate_scope(validate_request).await.is_ok());

    // The current scope should have changed. We can no longer get it for Public.
    let mut get_isolate_request = create_get_isolate_request(false);
    get_isolate_request.data_scope_type = DataScopeType::Public;
    let get_isolate_result = data_scope_requester.get_isolate(get_isolate_request).await;
    assert!(get_isolate_result.is_err());
    assert!(matches!(get_isolate_result, Err(DataScopeError::NoMatchingIsolates)));

    // We can get it for DomainOwned.
    let mut get_isolate_request_2: GetIsolateRequest = create_get_isolate_request(false);
    get_isolate_request_2.data_scope_type = DataScopeType::DomainOwned;
    let get_isolate_result = data_scope_requester.get_isolate(get_isolate_request_2).await?;
    assert_eq!(get_isolate_result.isolate_id, isolate_id);

    Ok(())
}

#[tokio::test]
async fn test_validate_isolate_scope_ratified() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);

    let mut add_isolate_request = create_add_isolate_request(true);
    add_isolate_request.allowed_data_scope_type = DataScopeType::DomainOwned;
    let isolate_id = add_isolate_request.isolate_id;
    assert!(data_scope_requester.add_isolate(add_isolate_request).await.is_ok());

    let validate_request = create_validate_isolate_request(isolate_id, DataScopeType::DomainOwned);
    let _ = data_scope_requester.validate_isolate_scope(validate_request).await?;

    Ok(())
}

#[test]
// Simple unit test to verify that direct comparison works with DataScopeType
fn test_data_scope_comparison() {
    assert!(DataScopeType::Public < DataScopeType::DomainOwned);
    assert!(DataScopeType::MultiUserPrivate > DataScopeType::UserPrivate);
    assert!(DataScopeType::Unspecified <= DataScopeType::DomainOwned);
    assert!(DataScopeType::Sealed == DataScopeType::Sealed);
}

#[tokio::test]
async fn test_isolate_retires_when_sensitive_session_threshold_is_reached(
) -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(TEST_SENSITIVE_SESSION_THRESHOLD);

    // Add an Isolate that can handle up to UserPrivate scope.
    let add_isolate_request = create_add_isolate_request(false);
    let isolate_id = add_isolate_request.isolate_id;
    data_scope_requester.add_isolate(add_isolate_request).await?;

    let get_sensitive_isolate_request = || GetIsolateRequest {
        binary_services_index: *TEST_BINARY_SERVICES_INDEX,
        data_scope_type: DataScopeType::UserPrivate,
    };

    // Use the Isolate for N-1 sensitive sessions, which should not trigger retirement.
    for i in 1..TEST_SENSITIVE_SESSION_THRESHOLD {
        let get_isolate_result =
            data_scope_requester.get_isolate(get_sensitive_isolate_request()).await?;
        assert_eq!(get_isolate_result.isolate_id, isolate_id);
        assert!(
            get_isolate_result.new_state.is_none(),
            "Isolate should not be retiring on sensitive use #{}",
            i
        );
    }

    // The Nth sensitive request (where N is the threshold) should succeed but mark the Isolate for retirement.
    let get_isolate_result_final =
        data_scope_requester.get_isolate(get_sensitive_isolate_request()).await?;
    assert_eq!(get_isolate_result_final.isolate_id, isolate_id);
    assert_eq!(
        get_isolate_result_final.new_state,
        Some(IsolateState::Retiring),
        "Isolate should be retiring on its final allowed sensitive use"
    );

    // After being retired, the Isolate should no longer be available for any new requests.
    let get_isolate_result_3 =
        data_scope_requester.get_isolate(get_sensitive_isolate_request()).await;
    assert!(matches!(get_isolate_result_3, Err(DataScopeError::NoMatchingIsolates)));
    Ok(())
}

#[tokio::test]
async fn test_isolate_retires_when_already_removed() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(1);

    // Add an Isolate that can handle up to UserPrivate scope.
    let add_isolate_request = create_add_isolate_request(false);
    let isolate_id = add_isolate_request.isolate_id;
    data_scope_requester.add_isolate(add_isolate_request).await?;

    let get_sensitive_isolate_request = || GetIsolateRequest {
        binary_services_index: *TEST_BINARY_SERVICES_INDEX,
        data_scope_type: DataScopeType::UserPrivate,
    };

    // Manually remove the Isolate before the sensitive use that would trigger retirement.
    data_scope_requester.remove_isolate(RemoveIsolateRequest { isolate_id }).await?;

    // Now, try to get the Isolate for a sensitive session. This should fail to find a matching
    // Isolate because we just removed it. The test here is that `get_isolate` doesn't panic
    // or return an unexpected error when `handle_sensitive_session` tries to remove an
    // already-removed Isolate.
    let get_isolate_result =
        data_scope_requester.get_isolate(get_sensitive_isolate_request()).await;
    assert!(matches!(get_isolate_result, Err(DataScopeError::NoMatchingIsolates)));
    Ok(())
}

#[tokio::test]
async fn test_non_sensitive_requests_do_not_retire_isolate(
) -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(TEST_SENSITIVE_SESSION_THRESHOLD);

    let add_isolate_request = create_add_isolate_request(false);
    let isolate_id = add_isolate_request.isolate_id;
    data_scope_requester.add_isolate(add_isolate_request).await?;

    // Call it multiple times with a non-sensitive scope, more than the sensitive threshold.
    for _ in 0..TEST_SENSITIVE_SESSION_THRESHOLD + 5 {
        let result = data_scope_requester.get_isolate(create_get_isolate_request(false)).await?;
        assert_eq!(result.isolate_id, isolate_id);
        assert!(
            result.new_state.is_none(),
            "Isolate should not have a new state from non-sensitive requests"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_add_scope_info_success() {
    let validator = ManifestValidator::default();
    let request = AddManifestScopeRequest {
        binary_services_index: BinaryServicesIndex::new(false),
        max_input_scope: DataScopeType::Public,
        max_output_scope: DataScopeType::Public,
    };
    let result = validator.add_scope_info(request).await;
    assert!(result.is_ok());
}

// Tests that adding scope info for an already registered binary fails with `DuplicateBinaryServiceIndex`.
#[tokio::test]
async fn test_add_scope_info_duplicate() {
    let validator = ManifestValidator::default();
    let binary_services_index = BinaryServicesIndex::new(false);
    let request = AddManifestScopeRequest {
        binary_services_index,
        max_input_scope: DataScopeType::Public,
        max_output_scope: DataScopeType::Public,
    };
    validator.add_scope_info(request).await.unwrap();
    let duplicate_request = AddManifestScopeRequest {
        binary_services_index,
        max_input_scope: DataScopeType::Public,
        max_output_scope: DataScopeType::Public,
    };
    let result = validator.add_scope_info(duplicate_request).await;
    assert!(result.is_err());
    assert!(matches!(result, Err(DataScopeError::DuplicateBinaryServiceIndex)));
}

// Tests that adding a backend dependency for a binary succeeds.
#[tokio::test]
async fn test_add_backend_dependencies() {
    let validator = ManifestValidator::default();
    let request = AddBackendDependenciesRequest {
        binary_services_index: BinaryServicesIndex::new(false),
        dependency_index: *TEST_REMOTE_ISOLATE_SERVICE_INDEX,
    };
    let result = validator.add_backend_dependencies(request).await;
    assert!(result.is_ok());
}

// Tests that validating a backend dependency that has been correctly registered succeeds.
#[tokio::test]
async fn test_validate_backend_dependency_success() {
    let validator = ManifestValidator::default();
    let binary_services_index = BinaryServicesIndex::new(false);
    let add_request = AddBackendDependenciesRequest {
        binary_services_index,
        dependency_index: *TEST_REMOTE_ISOLATE_SERVICE_INDEX,
    };
    validator.add_backend_dependencies(add_request).await.unwrap();
    let validate_request = ValidateBackendDependencyRequest {
        binary_services_index,
        destination_isolate_service: *TEST_REMOTE_ISOLATE_SERVICE_INDEX,
    };
    let result = validator.validate_backend_dependency(validate_request).await;
    assert!(result.is_ok());
}

// Tests that validating a backend dependency that is not added fails with `BackendDependencyNotAllowed`.
#[tokio::test]
async fn test_validate_backend_dependency_not_allowed() {
    let validator = ManifestValidator::default();
    let binary_services_index = BinaryServicesIndex::new(false);
    let add_request = AddBackendDependenciesRequest {
        binary_services_index,
        dependency_index: *TEST_REMOTE_ISOLATE_SERVICE_INDEX,
    };
    validator
        .add_backend_dependencies(add_request)
        .await
        .expect("Should be able to add backend dependency");

    let unknown_isolate_service_index =
        IsolateServiceIndex::new(None, "remote.service.unknown", false)
            .expect("Should be a valid isolate service index");
    let validate_request = ValidateBackendDependencyRequest {
        binary_services_index,
        destination_isolate_service: unknown_isolate_service_index,
    };
    let result = validator.validate_backend_dependency(validate_request).await;
    assert!(matches!(result, Err(DataScopeError::BackendDependencyNotAllowed)));
}

// Tests that validating a backend dependency for an unknown binary fails with `InvalidIsolateServiceIndex`.
#[tokio::test]
async fn test_validate_backend_dependency_invalid_index() {
    let validator = ManifestValidator::default();
    let validate_request = ValidateBackendDependencyRequest {
        binary_services_index: BinaryServicesIndex::new(false),
        destination_isolate_service: *TEST_REMOTE_ISOLATE_SERVICE_INDEX,
    };
    let result = validator.validate_backend_dependency(validate_request).await;
    assert!(matches!(result, Err(DataScopeError::InvalidIsolateServiceIndex)));
}

// Tests that validating an input scope that is within the allowed manifest scope succeeds.
#[tokio::test]
async fn test_validate_input_scope_success() {
    let validator = ManifestValidator::default();
    let binary_services_index = BinaryServicesIndex::new(false);
    let add_request = AddManifestScopeRequest {
        binary_services_index,
        max_input_scope: DataScopeType::DomainOwned,
        max_output_scope: DataScopeType::DomainOwned,
    };
    validator.add_scope_info(add_request).await.unwrap();
    let validate_request = ValidateManifestInputScopeRequest {
        binary_services_index,
        requested_scope: DataScopeType::Public,
    };
    let result = validator.validate_input_scope(validate_request).await;
    assert!(result.is_ok());
}

// Tests that validating an input scope stricter than the allowed manifest scope fails with `DataScopeNotAllowed`.
#[tokio::test]
async fn test_validate_input_scope_not_allowed() {
    let validator = ManifestValidator::default();
    let binary_services_index = BinaryServicesIndex::new(false);
    let add_request = AddManifestScopeRequest {
        binary_services_index,
        max_input_scope: DataScopeType::Public,
        max_output_scope: DataScopeType::Public,
    };
    validator.add_scope_info(add_request).await.unwrap();
    let validate_request = ValidateManifestInputScopeRequest {
        binary_services_index,
        requested_scope: DataScopeType::DomainOwned,
    };
    let result = validator.validate_input_scope(validate_request).await;
    assert!(matches!(result, Err(DataScopeError::DisallowedByManifest)));
}

// Tests that validating an input scope for an unknown binary fails with `InvalidIsolateServiceIndex`.
#[tokio::test]
async fn test_validate_input_scope_invalid_index() {
    let validator = ManifestValidator::default();
    let validate_request = ValidateManifestInputScopeRequest {
        binary_services_index: BinaryServicesIndex::new(false),
        requested_scope: DataScopeType::Public,
    };
    let result = validator.validate_input_scope(validate_request).await;
    assert!(matches!(result, Err(DataScopeError::InvalidIsolateServiceIndex)));
}

// Tests that validating an output scope that is within the allowed manifest scope succeeds.
#[tokio::test]
async fn test_validate_output_scope_success() {
    let validator = ManifestValidator::default();
    let binary_services_index = BinaryServicesIndex::new(false);
    let add_request = AddManifestScopeRequest {
        binary_services_index,
        max_input_scope: DataScopeType::DomainOwned,
        max_output_scope: DataScopeType::DomainOwned,
    };
    validator.add_scope_info(add_request).await.unwrap();
    let validate_request = ValidateManifestOutputScopeRequest {
        binary_services_index,
        emitted_scope: DataScopeType::Public,
    };
    let result = validator.validate_output_scope(validate_request).await;
    assert!(result.is_ok());
}

// Tests that validating an output scope stricter than the allowed manifest scope fails with `DataScopeNotAllowed`.
#[tokio::test]
async fn test_validate_output_scope_not_allowed() {
    let validator = ManifestValidator::default();
    let binary_services_index = BinaryServicesIndex::new(false);
    let add_request = AddManifestScopeRequest {
        binary_services_index,
        max_input_scope: DataScopeType::Public,
        max_output_scope: DataScopeType::Public,
    };
    validator.add_scope_info(add_request).await.unwrap();
    let validate_request = ValidateManifestOutputScopeRequest {
        binary_services_index,
        emitted_scope: DataScopeType::DomainOwned,
    };
    let result = validator.validate_output_scope(validate_request).await;
    assert!(matches!(result, Err(DataScopeError::DisallowedByManifest)));
}

// Tests that validating an output scope for an unknown binary fails with `InvalidIsolateServiceIndex`.
#[tokio::test]
async fn test_validate_output_scope_invalid_index() {
    let validator = ManifestValidator::default();
    let validate_request = ValidateManifestOutputScopeRequest {
        binary_services_index: BinaryServicesIndex::new(false),
        emitted_scope: DataScopeType::Public,
    };
    let result = validator.validate_output_scope(validate_request).await;
    assert!(matches!(result, Err(DataScopeError::InvalidIsolateServiceIndex)));
}

#[tokio::test]
async fn test_get_isolate_scope_opaque_success() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let add_isolate_request = create_add_isolate_request(false);
    let isolate_id = add_isolate_request.isolate_id;
    data_scope_requester.add_isolate(add_isolate_request).await?;
    let get_scope_request = GetIsolateScopeRequest { isolate_id };
    let response = data_scope_requester.get_isolate_scope(get_scope_request).await?;
    assert_eq!(response.current_scope, DataScopeType::Public);
    Ok(())
}

#[tokio::test]
async fn test_get_isolate_scope_ratified_unsupported() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let add_ratified_isolate_request = create_add_isolate_request(true);
    let ratified_isolate_id = add_ratified_isolate_request.isolate_id;
    data_scope_requester.add_isolate(add_ratified_isolate_request).await?;
    let get_ratified_scope_request = GetIsolateScopeRequest { isolate_id: ratified_isolate_id };
    let response = data_scope_requester.get_isolate_scope(get_ratified_scope_request).await?;
    assert_eq!(response.current_scope, DataScopeType::Unspecified);
    Ok(())
}

#[tokio::test]
async fn test_get_isolate_scope_unknown_isolate() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let unknown_isolate_id = IsolateId::new(*TEST_BINARY_SERVICES_INDEX);
    let get_unknown_scope_request = GetIsolateScopeRequest { isolate_id: unknown_isolate_id };
    let result = data_scope_requester.get_isolate_scope(get_unknown_scope_request).await;
    assert!(matches!(result, Err(DataScopeError::UnknownIsolateId)));
    Ok(())
}

#[tokio::test]
async fn test_parallel_get_isolate_requests() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    // Add multiple Isolates to ensure we test contention.
    let add_isolate_request = create_add_isolate_request(false);
    let isolate_id = add_isolate_request.isolate_id;
    data_scope_requester.add_isolate(add_isolate_request).await?;

    let num_requests = 100;
    let mut tasks = Vec::with_capacity(num_requests);

    for _ in 0..num_requests {
        let requester = data_scope_requester.clone();
        let request = create_get_isolate_request(false);
        tasks.push(tokio::spawn(async move { requester.get_isolate(request).await }));
    }

    for task in tasks {
        let get_isolate_result = task.await??;
        assert_eq!(get_isolate_result.isolate_id, isolate_id);
    }
    Ok(())
}

#[tokio::test]
async fn test_parallel_validate_isolate_requests() -> Result<(), Box<dyn std::error::Error>> {
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let add_isolate_request = create_add_isolate_request(false);
    let isolate_id = add_isolate_request.isolate_id;
    data_scope_requester.add_isolate(add_isolate_request).await?;

    let num_requests = 100;
    let mut tasks = Vec::with_capacity(num_requests);

    // Concurrency is dependent on how the tokio tasks are scheduled.
    for i in 0..num_requests {
        let requester = data_scope_requester.clone();
        // Mix of valid and invalid requests to stress test read/write locks?
        // Or just valid ones. Let's do valid ones to check for data races mainly.
        // Maybe some upgrades?
        let request = if i % 2 == 0 {
            create_validate_isolate_request(isolate_id, DataScopeType::Public)
        } else {
            create_validate_isolate_request(isolate_id, DataScopeType::UserPrivate)
        };
        tasks.push(tokio::spawn(async move { requester.validate_isolate_scope(request).await }));
    }

    for task in tasks {
        let _ = task.await??;
        // All should succeed as UserPrivate is allowed
    }
    Ok(())
}

fn create_add_isolate_request(is_ratified_isolate: bool) -> AddIsolateRequest {
    let binary_services_index = if is_ratified_isolate {
        *TEST_RATIFIED_BINARY_SERVICES_INDEX
    } else {
        *TEST_BINARY_SERVICES_INDEX
    };
    let isolate_id = IsolateId::new(binary_services_index);
    assert_eq!(isolate_id.is_ratified_isolate(), is_ratified_isolate);

    AddIsolateRequest {
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::UserPrivate,
        isolate_id,
    }
}

fn create_remove_isolate_request(isolate_id: IsolateId) -> RemoveIsolateRequest {
    RemoveIsolateRequest { isolate_id }
}

fn create_get_isolate_request(is_ratified_isolate: bool) -> GetIsolateRequest {
    let binary_services_index = if is_ratified_isolate {
        *TEST_RATIFIED_BINARY_SERVICES_INDEX
    } else {
        *TEST_BINARY_SERVICES_INDEX
    };
    GetIsolateRequest { binary_services_index, data_scope_type: DataScopeType::Public }
}

fn create_freeze_isolate_request(isolate_id: IsolateId) -> FreezeIsolateScopeRequest {
    assert!(!isolate_id.is_ratified_isolate());
    FreezeIsolateScopeRequest { isolate_id }
}

fn create_validate_isolate_request(
    isolate_id: IsolateId,
    requested_scope: DataScopeType,
) -> ValidateIsolateRequest {
    ValidateIsolateRequest { isolate_id, requested_scope }
}
