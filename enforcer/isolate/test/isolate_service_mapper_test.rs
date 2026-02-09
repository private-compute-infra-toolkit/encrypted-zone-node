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

use isolate_info::{IsolateServiceInfo, Route};
use isolate_service_mapper::IsolateServiceMapper;
use manifest_proto::enforcer::ez_backend_dependency::RouteType;

const TEST_OPERATOR_DOMAIN: &str = "test_operator_domain";
const TEST_SERVICE_NAME: &str = "test_service_name";

#[tokio::test]
async fn test_new_binary_index_and_get_service_index_successful() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let test_isolate_service_info = IsolateServiceInfo {
        operator_domain: TEST_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
    };
    let binary_index = isolate_service_mapper
        .new_binary_index(vec![test_isolate_service_info.clone()], false)
        .await
        .expect("new_binary_index should succeed");
    let mapped_isolate_service_index = isolate_service_mapper
        .get_service_index(&test_isolate_service_info)
        .await
        .expect("get_service_index should succeed");
    assert_eq!(
        isolate_service_mapper
            .get_service_index(&test_isolate_service_info)
            .await
            .expect("get_service_index should succeed"),
        mapped_isolate_service_index,
        "get_service_index should return the same index for the same service info"
    );
    assert_eq!(
        mapped_isolate_service_index
            .get_binary_services_index()
            .expect("Should be a valid binary index"),
        binary_index
    );
    assert_eq!(mapped_isolate_service_index.get_request_route(), Route::Internal);
}

#[tokio::test]
async fn test_new_binary_index_fails_on_duplicate_service() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let test_isolate_service_info = IsolateServiceInfo {
        operator_domain: TEST_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
    };
    let _binary_index = isolate_service_mapper
        .new_binary_index(vec![test_isolate_service_info.clone()], false)
        .await
        .expect("first new_binary_index should succeed");
    assert!(isolate_service_mapper
        .new_binary_index(vec![test_isolate_service_info.clone()], false)
        .await
        .is_err());
    // Marking it ratified also fail
    assert!(isolate_service_mapper
        .new_binary_index(vec![test_isolate_service_info], true)
        .await
        .is_err());
}

#[tokio::test]
async fn test_unregistered_service_returns_none() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let test_isolate_service_info = IsolateServiceInfo {
        operator_domain: TEST_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
    };
    assert!(isolate_service_mapper.get_service_index(&test_isolate_service_info).await.is_none())
}

#[tokio::test]
async fn test_multiple_services_binary_mapping_successful() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let test_info = IsolateServiceInfo {
        operator_domain: "domain1".to_string(),
        service_name: "service1".to_string(),
    };
    let secondary_test_info = IsolateServiceInfo {
        operator_domain: "domain2".to_string(),
        service_name: "service2".to_string(),
    };
    let services_vec = vec![test_info.clone(), secondary_test_info.clone()];
    let binary_index = isolate_service_mapper
        .new_binary_index(services_vec, false)
        .await
        .expect("new_binary_index should succeed");
    assert!(!binary_index.is_ratified_binary());
    assert_eq!(
        isolate_service_mapper
            .get_binary_index(&test_info)
            .await
            .expect("get_binary_index should succeed"),
        binary_index
    );
    assert_eq!(
        isolate_service_mapper
            .get_binary_index(&secondary_test_info)
            .await
            .expect("get_binary_index should succeed"),
        binary_index
    );
}

#[tokio::test]
async fn test_multiple_binary_mapping_successful() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let test_info = IsolateServiceInfo {
        operator_domain: "domain1".to_string(),
        service_name: "service1".to_string(),
    };
    let services_vec = vec![test_info.clone()];
    let secondary_test_info = IsolateServiceInfo {
        operator_domain: "domain2".to_string(),
        service_name: "service2".to_string(),
    };
    let secondary_services_vec = vec![secondary_test_info.clone()];
    let binary_index = isolate_service_mapper
        .new_binary_index(services_vec, false)
        .await
        .expect("new_binary_index should succeed");
    assert!(!binary_index.is_ratified_binary());
    let secondary_binary_index = isolate_service_mapper
        .new_binary_index(secondary_services_vec, true)
        .await
        .expect("new_binary_index should succeed for secondary");
    assert!(secondary_binary_index.is_ratified_binary());
    assert_eq!(
        isolate_service_mapper
            .get_binary_index(&test_info)
            .await
            .expect("get_binary_index should succeed"),
        binary_index
    );
    assert_eq!(
        isolate_service_mapper
            .get_binary_index(&secondary_test_info)
            .await
            .expect("get_binary_index for secondary should succeed"),
        secondary_binary_index
    );
}

#[tokio::test]
async fn test_unregistered_service_returns_none_for_binary_index() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let test_info = IsolateServiceInfo::default();
    assert!(isolate_service_mapper.get_binary_index(&test_info).await.is_none());
}

#[tokio::test]
async fn test_add_backend_dependency_service_passes_on_duplicate() {
    let mapper = IsolateServiceMapper::default();
    let service_info = IsolateServiceInfo {
        operator_domain: "some.service".to_string(),
        service_name: "TestService".to_string(),
    };
    mapper
        .add_backend_dependency_service(&service_info, RouteType::Remote)
        .await
        .expect("first add_backend_dependency_service should succeed");
    let backend_dependency_index =
        mapper.get_service_index(&service_info).await.expect("get_service_index should succeed");
    assert_eq!(
        mapper
            .add_backend_dependency_service(&service_info, RouteType::Remote)
            .await
            .expect("Should be a valid isolate service index"),
        backend_dependency_index
    );
    assert!(backend_dependency_index.get_binary_services_index().is_none())
}

#[tokio::test]
async fn test_add_backend_dependency_service_passes_if_service_in_binary_index() {
    let mapper = IsolateServiceMapper::default();
    let service_info = IsolateServiceInfo {
        operator_domain: "some.service".to_string(),
        service_name: "TestService".to_string(),
    };
    mapper
        .new_binary_index(vec![service_info.clone()], false)
        .await
        .expect("new_binary_index should succeed");
    let backend_dependency_index =
        mapper.get_service_index(&service_info).await.expect("get_service_index should succeed");
    assert_eq!(
        mapper
            .add_backend_dependency_service(&service_info, RouteType::Remote)
            .await
            .expect("Should be a valid isolate service index"),
        backend_dependency_index
    );
    assert!(backend_dependency_index.get_binary_services_index().is_some())
}

#[tokio::test]
async fn test_new_binary_index_fails_if_service_is_backend_dependency() {
    let mapper = IsolateServiceMapper::default();
    let service_info = IsolateServiceInfo {
        operator_domain: "some.service".to_string(),
        service_name: "TestService".to_string(),
    };
    mapper
        .add_backend_dependency_service(&service_info, RouteType::Remote)
        .await
        .expect("add_backend_dependency_service should succeed");
    assert!(
        mapper.new_binary_index(vec![service_info], false).await.is_err(),
        "new_binary_index should fail for existing backend dependency"
    );
}

#[tokio::test]
async fn test_get_isolate_service_infos_successful() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let info1 = IsolateServiceInfo {
        operator_domain: "domain1".to_string(),
        service_name: "service1".to_string(),
    };
    let info2 = IsolateServiceInfo {
        operator_domain: "domain2".to_string(),
        service_name: "service2".to_string(),
    };
    let services_vec = vec![info1.clone(), info2.clone()];
    let binary_index = isolate_service_mapper
        .new_binary_index(services_vec.clone(), false)
        .await
        .expect("new_binary_index should succeed");
    let retrieved_services = isolate_service_mapper
        .get_isolate_service_infos(&binary_index)
        .await
        .expect("get_isolate_service_infos should return Some");
    assert_eq!(retrieved_services.len(), 2);
    assert!(retrieved_services.contains(&info1));
    assert!(retrieved_services.contains(&info2));
}
