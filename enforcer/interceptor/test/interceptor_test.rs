// Copyright 2025 Google LLC
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

use anyhow::Result;
use ez_service_proto::enforcer::v1::CallRequest;
use interceptor::{Interceptor, RequestType};
use isolate_info::IsolateServiceInfo;
use isolate_service_mapper::IsolateServiceMapper;
use manifest_proto::enforcer::InterceptingServices;

const OPAQUE_DOMAIN: &str = "opaque.domain";
const OPAQUE_SERVICE: &str = "OpaqueService";
const RATIFIED_INTERCEPTOR_DOMAIN: &str = "ratified.interceptor.domain";
const RATIFIED_INTERCEPTOR_SERVICE: &str = "RatifiedInterceptorService";
const ANOTHER_RATIFIED_DOMAIN: &str = "another.ratified.domain";
const ANOTHER_RATIFIED_SERVICE: &str = "AnotherRatifiedService";
const UNARY: &str = "unary";
const STREAMING: &str = "streaming";

async fn setup_interceptor_with_populated_mapper() -> Result<Interceptor> {
    let mapper = IsolateServiceMapper::default();
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: OPAQUE_DOMAIN.to_string(),
                service_name: OPAQUE_SERVICE.to_string(),
            }],
            false, // is_ratified
        )
        .await?;
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
                service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
            }],
            true, // is_ratified
        )
        .await?;
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: ANOTHER_RATIFIED_DOMAIN.to_string(),
                service_name: ANOTHER_RATIFIED_SERVICE.to_string(),
            }],
            true, // is_ratified
        )
        .await?;

    Ok(Interceptor::new(mapper))
}

#[tokio::test]
async fn test_add_interceptor_success() {
    let interceptor =
        setup_interceptor_with_populated_mapper().await.expect("Failed to setup Interceptor");
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: OPAQUE_SERVICE.to_string(),
        interceptor_operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        interceptor_service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        interceptor_method_for_unary: UNARY.to_string(),
        interceptor_method_for_streaming: STREAMING.to_string(),
        ..Default::default()
    };

    let result = interceptor.add_interceptor(intercepting_services).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_add_interceptor_unrecognized_opaque_service() {
    let interceptor =
        setup_interceptor_with_populated_mapper().await.expect("Failed to setup Interceptor");
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: "unrecognized.domain".to_string(),
        intercepting_service_name: "UnrecognizedService".to_string(),
        interceptor_operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        interceptor_service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        ..Default::default()
    };

    let result = interceptor
        .add_interceptor(intercepting_services)
        .await
        .expect_err("Expected error while adding unrecognized Opaque service");
    assert_eq!(result.to_string(), "Unrecognized Opaque Service provided in InterceptingServices");
}

#[tokio::test]
async fn test_intercepting_ratified_service_not_allowed() {
    let interceptor =
        setup_interceptor_with_populated_mapper().await.expect("Failed to setup Interceptor");
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        intercepting_service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        interceptor_operator_domain: ANOTHER_RATIFIED_DOMAIN.to_string(),
        interceptor_service_name: ANOTHER_RATIFIED_SERVICE.to_string(),
        ..Default::default()
    };

    let result = interceptor
        .add_interceptor(intercepting_services)
        .await
        .expect_err("Expected error while adding interceptor for Ratified Isolate");
    assert_eq!(result.to_string(), "Intercepting Ratified Isolate services is not allowed");
}

#[tokio::test]
async fn test_unrecognized_ratified_service() {
    let interceptor =
        setup_interceptor_with_populated_mapper().await.expect("Failed to setup Interceptor");
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: OPAQUE_SERVICE.to_string(),
        interceptor_operator_domain: "unrecognized.ratified.domain".to_string(),
        interceptor_service_name: "UnrecognizedRatifiedService".to_string(),
        ..Default::default()
    };

    let result = interceptor
        .add_interceptor(intercepting_services)
        .await
        .expect_err("Expected error as Unrecognized Ratified Isolate provided");
    assert_eq!(
        result.to_string(),
        "Unrecognized Ratified Service provided in InterceptingServices"
    );
}

#[tokio::test]
async fn test_interceptor_is_not_ratified() {
    let interceptor =
        setup_interceptor_with_populated_mapper().await.expect("Failed to setup Interceptor");
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: OPAQUE_SERVICE.to_string(),
        interceptor_operator_domain: OPAQUE_DOMAIN.to_string(),
        interceptor_service_name: OPAQUE_SERVICE.to_string(),
        ..Default::default()
    };

    let result = interceptor
        .add_interceptor(intercepting_services)
        .await
        .expect_err("Expected error when Opaque Service provided as Interceptor");
    assert_eq!(result.to_string(), "Only Ratified Isolates Services can be set as interceptors");
}

#[tokio::test]
async fn test_replace_with_interceptor_unary() {
    let interceptor =
        setup_interceptor_with_populated_mapper().await.expect("Failed to setup Interceptor");
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: OPAQUE_SERVICE.to_string(),
        interceptor_operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        interceptor_service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        interceptor_method_for_unary: UNARY.to_string(),
        interceptor_method_for_streaming: STREAMING.to_string(),
        ..Default::default()
    };
    interceptor.add_interceptor(intercepting_services).await.unwrap();
    let mut call_request = CallRequest {
        operator_domain: OPAQUE_DOMAIN.to_string(),
        service_name: OPAQUE_SERVICE.to_string(),
        method_name: "OriginalUnaryMethod".to_string(),
        ..Default::default()
    };

    interceptor.replace_with_interceptor(&mut call_request, RequestType::Unary).await;
    assert_eq!(call_request.operator_domain, RATIFIED_INTERCEPTOR_DOMAIN);
    assert_eq!(call_request.service_name, RATIFIED_INTERCEPTOR_SERVICE);
    assert_eq!(call_request.method_name, UNARY);
}

#[tokio::test]
async fn test_replace_with_interceptor_streaming() {
    let interceptor =
        setup_interceptor_with_populated_mapper().await.expect("Failed to setup Interceptor");
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: OPAQUE_SERVICE.to_string(),
        interceptor_operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        interceptor_service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        interceptor_method_for_unary: UNARY.to_string(),
        interceptor_method_for_streaming: STREAMING.to_string(),
        ..Default::default()
    };
    interceptor.add_interceptor(intercepting_services).await.unwrap();
    let mut call_request = CallRequest {
        operator_domain: OPAQUE_DOMAIN.to_string(),
        service_name: OPAQUE_SERVICE.to_string(),
        method_name: "OriginalStreamingMethod".to_string(),
        ..Default::default()
    };

    interceptor.replace_with_interceptor(&mut call_request, RequestType::Streaming).await;
    assert_eq!(call_request.operator_domain, RATIFIED_INTERCEPTOR_DOMAIN);
    assert_eq!(call_request.service_name, RATIFIED_INTERCEPTOR_SERVICE);
    assert_eq!(call_request.method_name, STREAMING);
}

#[tokio::test]
async fn test_replace_with_interceptor_no_interceptor_configured() {
    let interceptor =
        setup_interceptor_with_populated_mapper().await.expect("Failed to setup Interceptor");

    let mut call_request = CallRequest {
        operator_domain: OPAQUE_DOMAIN.to_string(),
        service_name: OPAQUE_SERVICE.to_string(),
        method_name: "OriginalMethod".to_string(),
        ..Default::default()
    };
    let original_request = call_request.clone();

    interceptor.replace_with_interceptor(&mut call_request, RequestType::Unary).await;
    assert_eq!(call_request.operator_domain, original_request.operator_domain);
    assert_eq!(call_request.service_name, original_request.service_name);
    assert_eq!(call_request.method_name, original_request.method_name);
}

#[tokio::test]
async fn test_add_interceptor_already_configured() {
    let interceptor =
        setup_interceptor_with_populated_mapper().await.expect("Failed to setup Interceptor");
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: OPAQUE_SERVICE.to_string(),
        interceptor_operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        interceptor_service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        interceptor_method_for_unary: UNARY.to_string(),
        interceptor_method_for_streaming: STREAMING.to_string(),
        ..Default::default()
    };
    interceptor.add_interceptor(intercepting_services).await.unwrap();

    let duplicate_intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: OPAQUE_SERVICE.to_string(),
        interceptor_operator_domain: ANOTHER_RATIFIED_DOMAIN.to_string(),
        interceptor_service_name: ANOTHER_RATIFIED_SERVICE.to_string(),
        interceptor_method_for_unary: "AnotherUnaryMethod".to_string(),
        interceptor_method_for_streaming: "AnotherStreamingMethod".to_string(),
        ..Default::default()
    };
    let result = interceptor
        .add_interceptor(duplicate_intercepting_services)
        .await
        .expect_err("Expecting error as an Interceptor was already configured");
    assert_eq!(result.to_string(), "Interceptor already configured for this Opaque Service");
}
