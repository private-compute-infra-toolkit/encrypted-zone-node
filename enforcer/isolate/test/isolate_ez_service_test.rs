// Copyright 2026 Google LLC
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

mod isolate_ez_service_test_common;

use anyhow::Result;
use data_scope::request::AddBackendDependenciesRequest;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::InvokeEzRequest;
use isolate_ez_service_test_common::{
    add_backend_dependencies, add_to_data_scope_requester, create_test_request,
    create_test_request_with_scope, get_sample_invoke_ez_response, TestHarness, TestHarnessConfig,
    CHANNEL_SIZE, SHM_PAYLOAD_LARGE_THRESHOLD, TEST_EXTERNAL_OPERATOR_DOMAIN,
    TEST_EXTERNAL_ROUTE_TYPE, TEST_INTERNAL_OPERATOR_DOMAIN, TEST_INTERNAL_ROUTE_TYPE,
    TEST_REMOTE_OPERATOR_DOMAIN, TEST_REMOTE_ROUTE_TYPE, TEST_SERVICE_NAME, TEST_SOME_DOMAIN,
    TEST_SOME_SERVICE_NAME,
};
use isolate_info::IsolateServiceInfo;
use isolate_test_utils::{
    ScopeDragInstruction, TEST_ERROR_CODE, TEST_ERROR_MESSAGE, TEST_ERROR_SERVICE_NAME,
};
use manifest_proto::enforcer::v1::ez_backend_dependency::RouteType;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

const TEST_UNKNOWN_DOMAIN: &str = "unknown.domain";
const TEST_UNKNOWN_SERVICE_NAME: &str = "UnknownService";
const RATIFIED_INTERCEPTOR_DOMAIN: &str = "ratified.interceptor.domain";
const RATIFIED_INTERCEPTOR_SERVICE: &str = "RatifiedInterceptorService";

/// Test case for Isolate to find an internal service using Junction Component.
#[tokio::test]
async fn stream_routes_to_internal_junction() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&service_info.operator_domain, &service_info.service_name))
        .await
        .expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn stream_external_error_response_from_first_request() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let proxy_calls = harness.mock_proxy.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_EXTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_ERROR_SERVICE_NAME.into(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&service_info.operator_domain, &service_info.service_name))
        .await
        .expect("Should be able to send request");

    let expected_status = Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE);
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("We should be able to setup the stream")
        .into_inner();
    let status = response_stream.message().await.expect_err("First request should fail the stream");
    assert_eq!(status.code(), expected_status.code());
    assert!(status.message().contains(expected_status.message()));

    sleep(Duration::from_millis(20)).await;
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1, "External proxy should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn stream_internal_error_response_from_first_request() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_ERROR_SERVICE_NAME.into(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&service_info.operator_domain, &service_info.service_name))
        .await
        .expect("Should be able to send request");

    let expected_status = Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE);
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("We should be able to setup the stream")
        .into_inner();
    let status = response_stream.message().await.expect_err("First request should fail the stream");
    assert_eq!(status.code(), expected_status.code());
    assert!(status.message().contains(expected_status.message()));

    sleep(Duration::from_millis(20)).await;
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn stream_remote_error_response_from_first_request() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_ERROR_SERVICE_NAME.into(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&service_info.operator_domain, &service_info.service_name))
        .await
        .expect("Should be able to send request");

    let expected_status = Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE);
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("We should be able to setup the stream")
        .into_inner();
    let status = response_stream.message().await.expect_err("First request should fail the stream");
    assert_eq!(status.code(), expected_status.code());
    assert!(status.message().contains(expected_status.message()));

    sleep(Duration::from_millis(20)).await;
    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn stream_routes_to_external_proxy() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let proxy_calls = harness.mock_proxy.call_count.clone();
    let bridge_to_proxy_messages = harness.mock_proxy.received_from_bridge_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_EXTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&service_info.operator_domain, &service_info.service_name))
        .await
        .expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1, "External proxy should have been called");
    assert_eq!(
        bridge_to_proxy_messages.load(Ordering::SeqCst),
        1,
        "Bridge should have had a message for the connector."
    );
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_routes_to_internal_junction() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let _ = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn unary_external_error_response() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let proxy_calls = harness.mock_proxy.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_ERROR_SERVICE_NAME.into(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let expected_status = Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE);
    let status = harness.client.invoke_ez(request).await.expect_err("Invoke should fail");
    assert_eq!(status.code(), expected_status.code());
    assert!(status.message().contains(expected_status.message()));
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1, "External proxy should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_internal_error_response() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_ERROR_SERVICE_NAME.into(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let expected_status = Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE);
    let status = harness.client.invoke_ez(request).await.expect_err("Invoke should fail");
    assert_eq!(status.code(), expected_status.code());
    assert!(status.message().contains(expected_status.message()));
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn unary_remote_error_response() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_ERROR_SERVICE_NAME.into(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let expected_status = Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE);
    let status = harness.client.invoke_ez(request).await.expect_err("Invoke should fail");
    assert_eq!(status.code(), expected_status.code());
    assert!(status.message().contains(expected_status.message()));
    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_routes_to_external_proxy() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let proxy_calls = harness.mock_proxy.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_EXTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let _ = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1, "External proxy should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn stream_routes_to_remote_handler() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&service_info.operator_domain, &service_info.service_name))
        .await
        .expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;
    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_routes_to_remote_handler() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let _ = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_routes_to_remote_handler_with_declared_ez_instance_id() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let mut request = create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    request.control_plane_metadata.as_mut().unwrap().destination_ez_instance_id =
        "some_instance_id".to_string();
    let _ = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_rejects_ez_instance_id_for_internal_route() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        RouteType::Internal,
    )
    .await
    .expect("Failed to add internal backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let mut request = create_test_request(TEST_INTERNAL_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    request.control_plane_metadata.as_mut().unwrap().destination_ez_instance_id =
        "some_instance_id".to_string();
    let response = harness.client.invoke_ez(request).await;

    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        remote_calls.load(Ordering::SeqCst),
        0,
        "Remote handler should NOT have been called"
    );
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_allows_ez_instance_id_for_self_hosted_peer_service() {
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    let mut harness = TestHarness::new_with_self_hosted_services(vec![service_info.clone()])
        .await
        .expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();

    // Also register the service as an approved backend dependency for this isolate
    let dependency_index = harness
        .mapper
        .add_backend_dependency_service(&service_info, RouteType::Remote)
        .await
        .expect("Should be able to add backend dependency");
    harness
        .manifest_validator
        .add_backend_dependencies(AddBackendDependenciesRequest {
            binary_services_index: harness.isolate_id.get_binary_services_index(),
            dependency_index,
        })
        .await
        .expect("Should be able to add backend dependency in manifest");

    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let mut request = create_test_request(TEST_INTERNAL_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    request.control_plane_metadata.as_mut().unwrap().destination_ez_instance_id =
        "peer_replica_1".to_string();
    let _ = harness.client.invoke_ez(request).await.expect("Peer invoke should succeed");

    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

// `destination_ez_instance_id` selects *where* a request is delivered, never
// *whether* it is allowed. No backend dependency is registered here, so the
// request must be rejected even though an instance id is pinned.
#[tokio::test]
async fn unary_rejects_undeclared_destination_with_ez_instance_id() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let mut request = create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    request.control_plane_metadata.as_mut().unwrap().destination_ez_instance_id =
        "some_instance_id".to_string();
    let response = harness.client.invoke_ez(request).await;

    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        remote_calls.load(Ordering::SeqCst),
        0,
        "Remote handler should NOT have been called"
    );
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

// `destination_ez_instance_id` selects *where* a request is delivered, never
// *whether* it is allowed. No backend dependency is registered here, so the
// request must be rejected even though an instance id is pinned.
#[tokio::test]
async fn stream_rejects_undeclared_destination_with_ez_instance_id() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let mut request = create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    request.control_plane_metadata.as_mut().unwrap().destination_ez_instance_id =
        "some_instance_id".to_string();
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx.send(request).await.expect("Should send request successfully");
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;

    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        remote_calls.load(Ordering::SeqCst),
        0,
        "Remote handler should NOT have been called"
    );
}

#[tokio::test]
async fn stream_routes_to_internal_junction_with_qualifiers() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
        isolate_name: "internal_iso".into(),
        publisher_id: "internal_pub".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request_with_qualifiers(
            &service_info.operator_domain,
            &service_info.service_name,
            &service_info.isolate_name,
            &service_info.publisher_id,
        ))
        .await
        .expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn stream_routes_to_external_proxy_with_qualifiers() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let proxy_calls = harness.mock_proxy.call_count.clone();
    let bridge_to_proxy_messages = harness.mock_proxy.received_from_bridge_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_EXTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        isolate_name: "external_iso".into(),
        publisher_id: "external_pub".into(),
    };

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request_with_qualifiers(
            &service_info.operator_domain,
            &service_info.service_name,
            &service_info.isolate_name,
            &service_info.publisher_id,
        ))
        .await
        .expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1, "External proxy should have been called");
    assert_eq!(
        bridge_to_proxy_messages.load(Ordering::SeqCst),
        1,
        "Bridge should have had a message for the connector."
    );
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn stream_routes_to_remote_handler_with_qualifiers() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        isolate_name: "remote_iso".into(),
        publisher_id: "remote_pub".into(),
    };

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request_with_qualifiers(
            &service_info.operator_domain,
            &service_info.service_name,
            &service_info.isolate_name,
            &service_info.publisher_id,
        ))
        .await
        .expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;
    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_routes_to_internal_junction_with_qualifiers() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        isolate_name: "internal_iso".into(),
        publisher_id: "internal_pub".into(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request_with_qualifiers(
        &service_info.operator_domain,
        &service_info.service_name,
        &service_info.isolate_name,
        &service_info.publisher_id,
    );
    let _ = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn unary_routes_to_external_proxy_with_qualifiers() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let proxy_calls = harness.mock_proxy.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_EXTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        isolate_name: "external_iso".into(),
        publisher_id: "external_pub".into(),
    };

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request_with_qualifiers(
        &service_info.operator_domain,
        &service_info.service_name,
        &service_info.isolate_name,
        &service_info.publisher_id,
    );
    let _ = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1, "External proxy should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_routes_to_remote_handler_with_qualifiers() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let remote_calls = harness.mock_ez_to_ez.call_count.clone();
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        isolate_name: "remote_iso".into(),
        publisher_id: "remote_pub".into(),
    };

    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let request = create_test_request_with_qualifiers(
        &service_info.operator_domain,
        &service_info.service_name,
        &service_info.isolate_name,
        &service_info.publisher_id,
    );
    let _ = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(remote_calls.load(Ordering::SeqCst), 1, "Remote handler should have been called");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn unary_invoke_ez_fails_without_control_plane_metadata() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let request = InvokeEzRequest::default(); // No metadata
    let response = harness.client.invoke_ez(request).await;
    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::Internal);
}

#[tokio::test]
async fn stream_invoke_ez_fails_without_control_plane_metadata() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx.send(InvokeEzRequest::default()).await.expect("Should send request successfully");
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::Internal);
}

#[tokio::test]
async fn unary_invoke_ez_fails_for_unknown_service() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let request = create_test_request(TEST_UNKNOWN_DOMAIN, TEST_UNKNOWN_SERVICE_NAME);
    let response = harness.client.invoke_ez(request).await;
    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn stream_invoke_ez_fails_for_unknown_service() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(TEST_UNKNOWN_DOMAIN, TEST_UNKNOWN_SERVICE_NAME))
        .await
        .expect("Should send request successfully");
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn verify_all_backend_dependency_routing() {
    let mut harness = TestHarness::new().await.expect("Harness should start");

    // INTERNAL Setup
    let internal_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "InternalService".into(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &internal_info,
        true, // is_internal = true
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add INTERNAL dependency");

    // EXTERNAL Setup
    let external_domain = "external.service".to_string();
    let external_info = IsolateServiceInfo {
        operator_domain: external_domain.clone(),
        service_name: "ExternalService".to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &external_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add EXTERNAL dependency");

    // REMOTE Setup
    let remote_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: "RemoteService".to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &remote_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add REMOTE dependency");

    // Add Isolate to Data Scope Manager (DSM)
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    harness.mock_junction.call_count.store(0, Ordering::SeqCst);
    harness.mock_proxy.call_count.store(0, Ordering::SeqCst);
    harness.mock_ez_to_ez.call_count.store(0, Ordering::SeqCst);

    // Test INTERNAL Routing (Junction)
    let internal_request =
        create_test_request(&internal_info.operator_domain, &internal_info.service_name);
    let _ =
        harness.client.invoke_ez(internal_request).await.expect("INTERNAL invoke should succeed");

    // Check counts after INTERNAL call
    assert_eq!(
        harness.mock_junction.call_count.load(Ordering::SeqCst),
        1,
        "INTERNAL call: Junction should be called once."
    );
    assert_eq!(
        harness.mock_proxy.call_count.load(Ordering::SeqCst),
        0,
        "INTERNAL call: Proxy should NOT be called."
    );
    assert_eq!(
        harness.mock_ez_to_ez.call_count.load(Ordering::SeqCst),
        0,
        "INTERNAL call: Remote handler should NOT be called."
    );

    // Test EXTERNAL Routing (Proxy)
    let external_request =
        create_test_request(&external_info.operator_domain, &external_info.service_name);
    let _ =
        harness.client.invoke_ez(external_request).await.expect("EXTERNAL invoke should succeed");

    // Check counts after EXTERNAL call
    assert_eq!(
        harness.mock_junction.call_count.load(Ordering::SeqCst),
        1,
        "EXTERNAL call: Junction call count unchanged."
    );
    assert_eq!(
        harness.mock_proxy.call_count.load(Ordering::SeqCst),
        1,
        "EXTERNAL call: Proxy should be called once."
    );
    assert_eq!(
        harness.mock_ez_to_ez.call_count.load(Ordering::SeqCst),
        0,
        "EXTERNAL call: Remote handler call count unchanged."
    );

    // Test REMOTE Routing (Ez-to-Ez Handler)
    let remote_request =
        create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, &remote_info.service_name);
    let _ = harness.client.invoke_ez(remote_request).await.expect("REMOTE invoke should succeed");

    // Check counts after REMOTE call
    assert_eq!(
        harness.mock_junction.call_count.load(Ordering::SeqCst),
        1,
        "REMOTE call: Junction call count unchanged."
    );
    assert_eq!(
        harness.mock_proxy.call_count.load(Ordering::SeqCst),
        1,
        "REMOTE call: Proxy call count unchanged."
    );
    assert_eq!(
        harness.mock_ez_to_ez.call_count.load(Ordering::SeqCst),
        1,
        "REMOTE call: Remote handler should be called once."
    );
}

#[tokio::test]
async fn test_unary_routes_via_interceptor() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();

    // The backend service we want to route to (interceptor)
    let interceptor_info = IsolateServiceInfo {
        operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        ..Default::default()
    };

    harness
        .mapper
        .new_binary_index(
            vec![interceptor_info.clone()],
            true,
            interceptor_info.publisher_id.clone(),
            interceptor_info.isolate_name.clone(),
        )
        .await
        .expect("Failed to index ratified service");

    harness
        .manifest_validator
        .add_backend_dependencies(AddBackendDependenciesRequest {
            binary_services_index: harness.isolate_id.get_binary_services_index(),
            dependency_index: harness
                .mapper
                .get_service_index(&interceptor_info)
                .await
                .expect("Failed to get service index"),
        })
        .await
        .expect("Should be able to add backend dependency in manifest");

    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    // The fake target the request will try to hit initially
    let target_info = IsolateServiceInfo {
        operator_domain: TEST_SOME_DOMAIN.to_string(),
        service_name: TEST_SOME_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    harness
        .mapper
        .new_binary_index(
            vec![target_info.clone()],
            false,
            target_info.publisher_id.clone(),
            target_info.isolate_name.clone(),
        )
        .await
        .expect("Failed to index opaque service");

    let intercepting_services = manifest_proto::enforcer::v1::InterceptingServices {
        intercepting_operator_domain: target_info.operator_domain.clone(),
        intercepting_service_name: target_info.service_name.clone(),
        interceptor_operator_domain: interceptor_info.operator_domain.clone(),
        interceptor_service_name: interceptor_info.service_name.clone(),
        interceptor_method_for_unary: "interceptor_unary".to_string(),
        ..Default::default()
    };
    harness.interceptor.add_interceptor(intercepting_services).await.unwrap();

    let request = create_test_request(&target_info.operator_domain, &target_info.service_name);
    let _response = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
    let received_request =
        harness.mock_junction.invoked_isolate_requests.lock().unwrap().last().unwrap().clone();
    assert_eq!(
        received_request.control_plane_metadata.unwrap().destination_method_name,
        "interceptor_unary",
        "Request should be rerouted to interceptor's method"
    );
}

#[tokio::test]
async fn unary_external_timeout_expires() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_EXTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    *harness.mock_proxy.delay.lock().unwrap() = Duration::from_millis(100);

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let mut tonic_request = tonic::Request::new(request);
    tonic_request.metadata_mut().insert("grpc-timeout", "10000u".parse().unwrap());

    let response = harness.client.invoke_ez(tonic_request).await;
    assert!(response.is_err());
    assert!(response.unwrap_err().to_string().contains("Mock proxy timeout"));
}

#[tokio::test]
async fn unary_external_timeout_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_EXTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    *harness.mock_proxy.delay.lock().unwrap() = Duration::from_millis(50);

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let mut tonic_request = tonic::Request::new(request);
    tonic_request.metadata_mut().insert("grpc-timeout", "1S".parse().unwrap());

    let response = harness.client.invoke_ez(tonic_request).await;
    assert!(response.is_ok());
}

#[tokio::test]
async fn unary_remote_timeout_expires() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
            service_name: TEST_SERVICE_NAME.to_string(),
            ..Default::default()
        },
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    *harness.mock_ez_to_ez.delay.lock().unwrap() = Duration::from_millis(100);

    let request = create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    let mut tonic_request = tonic::Request::new(request);
    tonic_request.metadata_mut().insert("grpc-timeout", "10000u".parse().unwrap());

    let response = harness.client.invoke_ez(tonic_request).await;
    assert!(response.is_err());
    let err_msg = response.unwrap_err().to_string();
    assert!(err_msg.contains("Error from remote enforcer: Mock remote_invoke timeout"));
}

#[tokio::test]
async fn unary_remote_timeout_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &IsolateServiceInfo {
            operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
            service_name: TEST_SERVICE_NAME.to_string(),
            ..Default::default()
        },
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    *harness.mock_ez_to_ez.delay.lock().unwrap() = Duration::from_millis(50);

    let request = create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    let mut tonic_request = tonic::Request::new(request);
    tonic_request.metadata_mut().insert("grpc-timeout", "1S".parse().unwrap());

    let response = harness.client.invoke_ez(tonic_request).await;
    assert!(response.is_ok());
}

#[tokio::test]
async fn test_interceptor_replacement_skipped_for_ratified_isolate() {
    // Create harness with is_ratified = true
    let mut harness = TestHarness::new_with_arguments(
        true,
        ScopeDragInstruction::KeepSame,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();

    // The backend service we want to route to (interceptor)
    let interceptor_info = IsolateServiceInfo {
        operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        ..Default::default()
    };

    harness
        .mapper
        .new_binary_index(
            vec![interceptor_info.clone()],
            true,
            interceptor_info.publisher_id.clone(),
            interceptor_info.isolate_name.clone(),
        )
        .await
        .expect("Failed to index ratified service");

    // The target service the request will try to hit
    let target_info = IsolateServiceInfo {
        operator_domain: "original.target.domain".to_string(),
        service_name: "OriginalService".to_string(),
        ..Default::default()
    };
    harness
        .mapper
        .new_binary_index(
            vec![target_info.clone()],
            false,
            target_info.publisher_id.clone(),
            target_info.isolate_name.clone(),
        )
        .await
        .expect("Failed to index opaque service");

    // Add target_info as a backend dependency for the ratified isolate
    harness
        .manifest_validator
        .add_backend_dependencies(AddBackendDependenciesRequest {
            binary_services_index: harness.isolate_id.get_binary_services_index(),
            dependency_index: harness
                .mapper
                .get_service_index(&target_info)
                .await
                .expect("Failed to get service index"),
        })
        .await
        .expect("Should be able to add backend dependency in manifest");

    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to RIM");

    // Configure an interceptor from target_info to interceptor_info
    let intercepting_services = manifest_proto::enforcer::v1::InterceptingServices {
        intercepting_operator_domain: target_info.operator_domain.clone(),
        intercepting_service_name: target_info.service_name.clone(),
        interceptor_operator_domain: interceptor_info.operator_domain.clone(),
        interceptor_service_name: interceptor_info.service_name.clone(),
        interceptor_method_for_unary: "interceptor_unary".to_string(),
        ..Default::default()
    };
    harness.interceptor.add_interceptor(intercepting_services).await.unwrap();

    let request = create_test_request_with_scope(
        &target_info.operator_domain,
        &target_info.service_name,
        DataScopeType::Public,
    );
    let _response = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    // Verify that the junction receives a request for target_info, NOT the interceptor
    assert_eq!(
        junction_calls.load(Ordering::SeqCst),
        1,
        "Junction should have been called (unary)"
    );
    let received_request =
        harness.mock_junction.invoked_isolate_requests.lock().unwrap().last().unwrap().clone();

    let meta = received_request.control_plane_metadata.unwrap();
    assert_eq!(
        meta.destination_operator_domain, target_info.operator_domain,
        "Request (unary) should NOT be rerouted to interceptor's domain"
    );
    assert_eq!(
        meta.destination_service_name, target_info.service_name,
        "Request (unary) should NOT be rerouted to interceptor's service"
    );
    assert_ne!(
        meta.destination_method_name, "interceptor_unary",
        "Request (unary) should NOT be rerouted to interceptor's method"
    );

    // Invoke target_info via Streaming
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    let request_streaming = create_test_request_with_scope(
        &target_info.operator_domain,
        &target_info.service_name,
        DataScopeType::Public,
    );
    req_tx.send(request_streaming).await.expect("Should be able to send streaming request");

    let mut _response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed")
        .into_inner();

    sleep(Duration::from_millis(20)).await;

    // Verify that the junction receives a streaming request for target_info, NOT the interceptor
    assert_eq!(
        junction_calls.load(Ordering::SeqCst),
        2,
        "Junction should have been called (streaming)"
    );
    let received_request_streaming =
        harness.mock_junction.invoked_isolate_requests.lock().unwrap().last().unwrap().clone();

    let meta_streaming = received_request_streaming.control_plane_metadata.unwrap();
    assert_eq!(
        meta_streaming.destination_operator_domain, target_info.operator_domain,
        "Request (streaming) should NOT be rerouted to interceptor's domain"
    );
    assert_eq!(
        meta_streaming.destination_service_name, target_info.service_name,
        "Request (streaming) should NOT be rerouted to interceptor's service"
    );
    assert_ne!(
        meta_streaming.destination_method_name, "interceptor_unary",
        "Request (streaming) should NOT be rerouted to interceptor's method"
    );
}

#[tokio::test]
async fn unary_routes_to_internal_junction_with_granular_qualifiers() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        isolate_name: "specific_isolate".to_string(),
        publisher_id: "specific_publisher".to_string(),
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &service_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add backend dependency");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    if let Some(meta) = request.control_plane_metadata.as_mut() {
        meta.destination_isolate_name = "specific_isolate".to_string();
        meta.destination_publisher_id = "specific_publisher".to_string();
    }
    let _ = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn stream_rejects_midstream_route_mismatch_remote_to_internal() {
    let mut harness = TestHarness::new().await.expect("Harness should start");

    let expected_remote = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &expected_remote,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add component");

    let mismatched_internal = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &mismatched_internal,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add component");

    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&expected_remote.operator_domain, &expected_remote.service_name))
        .await
        .expect("Should be able to send request");

    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("We should be able to setup the stream")
        .into_inner();

    req_tx
        .send(create_test_request(
            &mismatched_internal.operator_domain,
            &mismatched_internal.service_name,
        ))
        .await
        .expect("Should be able to send mismatched request");

    // Wait for the first response
    let first_resp = response_stream.message().await;
    assert!(first_resp.is_ok(), "First request should succeed");

    // Pull second response which should fail due to midstream route mismatch
    let second_resp = response_stream.message().await;
    let err = second_resp.expect_err("Second request should error out");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Mid-stream route mismatch"));
}

#[tokio::test]
async fn stream_rejects_midstream_route_mismatch_internal_to_remote() {
    let mut harness = TestHarness::new().await.expect("Harness should start");

    let expected_internal = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &expected_internal,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add component");

    let mismatched_remote = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &mismatched_remote,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("Failed to add component");

    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(
            &expected_internal.operator_domain,
            &expected_internal.service_name,
        ))
        .await
        .expect("Should be able to send request");

    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("We should be able to setup the stream")
        .into_inner();

    req_tx
        .send(create_test_request(
            &mismatched_remote.operator_domain,
            &mismatched_remote.service_name,
        ))
        .await
        .expect("Should be able to send mismatched request");

    let first_resp = response_stream.message().await;
    assert!(first_resp.is_ok(), "First request should succeed");

    let second_resp = response_stream.message().await;
    let err = second_resp.expect_err("Second request should error out");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Mid-stream route mismatch"));
}

#[tokio::test]
async fn test_unconfigured_external_and_remote_handlers_and_empty_stream() {
    let mut harness =
        TestHarness::new_without_outbound_handlers().await.expect("Harness should start");
    let external_info = IsolateServiceInfo {
        operator_domain: TEST_EXTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &external_info,
        false,
        TEST_EXTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("add external dep");
    let remote_info = IsolateServiceInfo {
        operator_domain: TEST_REMOTE_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &remote_info,
        false,
        TEST_REMOTE_ROUTE_TYPE,
    )
    .await
    .expect("add remote dep");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("add isolate scope");
    let (empty_tx, empty_rx) = mpsc::channel::<InvokeEzRequest>(1);
    drop(empty_tx);
    let mut empty_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(empty_rx))
        .await
        .expect("empty stream setup")
        .into_inner();
    let eof = timeout(Duration::from_secs(5), empty_stream.message())
        .await
        .expect("empty stream should finish promptly")
        .expect("empty stream should close cleanly");
    assert!(eof.is_none());
    let ext_unary_err = harness
        .client
        .invoke_ez(create_test_request(TEST_EXTERNAL_OPERATOR_DOMAIN, TEST_SERVICE_NAME))
        .await
        .expect_err("unary external without connector must fail");
    assert_eq!(ext_unary_err.code(), tonic::Code::FailedPrecondition);
    let (ext_tx, ext_rx) = mpsc::channel(CHANNEL_SIZE);
    ext_tx
        .send(create_test_request(TEST_EXTERNAL_OPERATOR_DOMAIN, TEST_SERVICE_NAME))
        .await
        .unwrap();
    drop(ext_tx);
    let mut ext_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(ext_rx))
        .await
        .expect("external stream setup")
        .into_inner();
    let ext_stream_err = timeout(Duration::from_secs(5), ext_stream.message())
        .await
        .expect("external stream error should arrive")
        .expect_err("streaming external without connector must fail");
    assert_eq!(ext_stream_err.code(), tonic::Code::FailedPrecondition);
    let rem_unary_err = harness
        .client
        .invoke_ez(create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME))
        .await
        .expect_err("unary remote without handler must fail");
    assert_eq!(rem_unary_err.code(), tonic::Code::FailedPrecondition);
    let (rem_tx, rem_rx) = mpsc::channel(CHANNEL_SIZE);
    rem_tx.send(create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME)).await.unwrap();
    drop(rem_tx);
    let mut rem_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(rem_rx))
        .await
        .expect("remote stream setup")
        .into_inner();
    let rem_stream_err = timeout(Duration::from_secs(5), rem_stream.message())
        .await
        .expect("remote stream error should arrive")
        .expect_err("streaming remote without handler must fail");
    assert_eq!(rem_stream_err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn test_missing_metadata_and_midstream_route_mismatch_rejected() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    for (domain, is_internal, route_type) in [
        (TEST_INTERNAL_OPERATOR_DOMAIN, true, TEST_INTERNAL_ROUTE_TYPE),
        (TEST_EXTERNAL_OPERATOR_DOMAIN, false, TEST_EXTERNAL_ROUTE_TYPE),
        (TEST_REMOTE_OPERATOR_DOMAIN, false, TEST_REMOTE_ROUTE_TYPE),
    ] {
        let info = IsolateServiceInfo {
            operator_domain: domain.to_string(),
            service_name: TEST_SERVICE_NAME.to_string(),
            ..Default::default()
        };
        add_backend_dependencies(
            harness.isolate_id,
            harness.mapper.clone(),
            harness.manifest_validator.clone(),
            &info,
            is_internal,
            route_type,
        )
        .await
        .expect("add route dependency");
    }
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("add isolate scope");
    let mut no_meta_req = create_test_request(TEST_INTERNAL_OPERATOR_DOMAIN, TEST_SERVICE_NAME);
    no_meta_req.control_plane_metadata = None;
    let no_meta_err = harness
        .client
        .invoke_ez(no_meta_req)
        .await
        .expect_err("unary request without metadata must fail");
    assert_eq!(no_meta_err.code(), tonic::Code::FailedPrecondition);
    let rem_ok = harness
        .client
        .invoke_ez(create_test_request(TEST_REMOTE_OPERATOR_DOMAIN, TEST_SERVICE_NAME))
        .await
        .expect("unary remote call with empty response scope should succeed")
        .into_inner();
    assert!(rem_ok.ez_response_payload.is_none());
    let (ext_tx, ext_rx) = mpsc::channel(CHANNEL_SIZE);
    ext_tx
        .send(create_test_request(TEST_EXTERNAL_OPERATOR_DOMAIN, TEST_SERVICE_NAME))
        .await
        .unwrap();
    ext_tx
        .send(create_test_request(TEST_EXTERNAL_OPERATOR_DOMAIN, TEST_SERVICE_NAME))
        .await
        .unwrap();
    let mut ext_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(ext_rx))
        .await
        .expect("external stream setup")
        .into_inner();
    let echoed = timeout(Duration::from_secs(5), ext_stream.message())
        .await
        .expect("external stream echo should arrive")
        .expect("external stream response should be Ok")
        .expect("external stream should emit echoed payload");
    assert_eq!(echoed.ez_response_payload, get_sample_invoke_ez_response().ez_response_payload);
    ext_tx
        .send(create_test_request(TEST_INTERNAL_OPERATOR_DOMAIN, TEST_SERVICE_NAME))
        .await
        .unwrap();
    let ext_mismatch = timeout(Duration::from_secs(5), ext_stream.message())
        .await
        .expect("external route mismatch error should arrive")
        .expect_err("switching external stream to internal route must fail");
    assert_eq!(ext_mismatch.code(), tonic::Code::FailedPrecondition);
    drop(ext_tx);
    for (initial_domain, switched_domain) in [
        (TEST_REMOTE_OPERATOR_DOMAIN, TEST_INTERNAL_OPERATOR_DOMAIN),
        (TEST_INTERNAL_OPERATOR_DOMAIN, TEST_EXTERNAL_OPERATOR_DOMAIN),
    ] {
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        tx.send(create_test_request(initial_domain, TEST_SERVICE_NAME)).await.unwrap();
        let mut stream = harness
            .client
            .stream_invoke_ez(ReceiverStream::new(rx))
            .await
            .expect("stream setup")
            .into_inner();
        let first = timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("initial stream response should arrive")
            .expect("initial stream response should be Ok");
        assert!(first.is_some());
        tx.send(create_test_request(switched_domain, TEST_SERVICE_NAME)).await.unwrap();
        drop(tx);
        let mismatch = timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("route mismatch error should arrive")
            .expect_err("mid-stream route switch must fail");
        assert_eq!(mismatch.code(), tonic::Code::FailedPrecondition);
    }
}

impl TestHarness {
    async fn new_without_outbound_handlers() -> Result<Self> {
        Self::with_config(TestHarnessConfig { with_outbound_handlers: false, ..Default::default() })
            .await
    }

    async fn new_with_self_hosted_services(service_infos: Vec<IsolateServiceInfo>) -> Result<Self> {
        Self::with_config(TestHarnessConfig {
            self_hosted_services: service_infos,
            ..Default::default()
        })
        .await
    }
}

/// Creates test request for Isolates with specific `isolate_name` and `publisher_id`.
fn create_test_request_with_qualifiers(
    domain: &str,
    name: &str,
    isolate_name: &str,
    publisher_id: &str,
) -> InvokeEzRequest {
    let mut req = create_test_request(domain, name);
    if let Some(meta) = req.control_plane_metadata.as_mut() {
        meta.destination_isolate_name = isolate_name.to_string();
        meta.destination_publisher_id = publisher_id.to_string();
    }
    req
}
