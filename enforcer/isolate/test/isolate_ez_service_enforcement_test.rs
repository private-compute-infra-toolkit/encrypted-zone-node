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

use anyhow::{Context, Result};
use container_manager_request::ContainerManagerRequest;
use data_scope::request::{
    AddIsolateRequest, AddManifestScopeRequest, GetIsolateScopeRequest, ValidateIsolateRequest,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    isolate_ez_bridge_client::IsolateEzBridgeClient,
    publish_event_for_request::Payload as PublishPayload, CreateFileshareRequest,
    CreateMemshareRequest, EventTopic, FileshareEvent, IsolateDataScope, IsolateState,
    NotifyIsolateStateRequest, NotifyIsolateStateResponse, PollIsolateStateRequest,
    PublishEventForRequest, StreamSubscribeToRequest,
};
use isolate_ez_service_test_common::{
    add_backend_dependencies, add_to_data_scope_requester, create_test_request,
    create_test_request_with_scope, TestHarness, CHANNEL_SIZE, SHM_PAYLOAD_LARGE_THRESHOLD,
    TEST_EXTERNAL_OPERATOR_DOMAIN, TEST_EXTERNAL_ROUTE_TYPE, TEST_INTERNAL_OPERATOR_DOMAIN,
    TEST_INTERNAL_ROUTE_TYPE, TEST_PAYLOAD, TEST_REMOTE_OPERATOR_DOMAIN, TEST_REMOTE_ROUTE_TYPE,
    TEST_SERVICE_NAME, TEST_SOME_DOMAIN, TEST_SOME_SERVICE_NAME,
};
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceInfo};
use isolate_test_utils::ScopeDragInstruction;
use manifest_proto::enforcer::v1::ez_backend_dependency::RouteType;
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData, ShmSlotData,
};
use shm_slab_pool::{ShmSlabPool, ShmSlabPoolOptions};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{sleep, timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::Streaming;
use tonic::transport::Channel;

const REGION_SIZE: i64 = 4096;
// Low payload threshold forces requests and responses through SHM conversion.
const SHM_PAYLOAD_LOW_THRESHOLD: u64 = 0;

#[tokio::test]
async fn unary_routes_to_external_proxy_bypasses_scopes() {
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

    // Simulate setting up the data scopes.
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    // Omit scopes completely!
    request.isolate_request_iscope = None;

    // Requests without datascopes are validated and blocked.
    let response = harness.client.invoke_ez(request).await;

    assert!(response.is_err(), "Invoke should be blocked");
    assert_eq!(response.unwrap_err().code(), tonic::Code::PermissionDenied);
    assert_eq!(
        proxy_calls.load(Ordering::SeqCst),
        0,
        "External proxy should have been called due to bypass"
    );
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should NOT have been called");
}

#[tokio::test]
async fn notify_isolate_state_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: harness.isolate_id,
        })
        .await;

    let (tx, mut rx) = setup_notify_state_stream(&mut harness.client)
        .await
        .expect("Should be able to setup notify state stream");

    tx.send(NotifyIsolateStateRequest { new_isolate_state: IsolateState::Ready.into() })
        .await
        .expect("Should be able to send state update");

    let response = rx.message().await;
    assert!(response.is_ok(), "Expected a successful state update response");
}

#[tokio::test]
async fn notify_isolate_state_failure() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: harness.isolate_id,
        })
        .await;
    harness.isolate_state_manager.mark_channel_connected(harness.isolate_id).await.unwrap();

    let (tx, mut rx) = setup_notify_state_stream(&mut harness.client)
        .await
        .expect("Should be able to setup notify state stream");

    // Send the same state twice to trigger a DuplicateStateUpdate error.
    tx.send(NotifyIsolateStateRequest { new_isolate_state: IsolateState::Ready.into() })
        .await
        .expect("Should be able to send state update");
    let response = rx.message().await;
    assert!(response.is_ok(), "Expected a successful state update response");

    tx.send(NotifyIsolateStateRequest { new_isolate_state: IsolateState::Ready.into() })
        .await
        .expect("Should be able to send state update");

    // The service sends an error, then a success. We check for the error.
    let response = rx.message().await;
    assert!(response.is_err(), "Expected a state update failure");
    assert_eq!(response.expect_err("Expected an error response").code(), tonic::Code::Aborted);
}

#[tokio::test]
async fn poll_isolate_state_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: harness.isolate_id,
        })
        .await;
    harness.isolate_state_manager.mark_channel_connected(harness.isolate_id).await.unwrap();

    harness
        .isolate_state_manager
        .update_state(harness.isolate_id, IsolateState::Ready)
        .await
        .expect("Should be able to update state");

    let response = harness
        .client
        .poll_isolate_state(PollIsolateStateRequest {})
        .await
        .expect("Should succeed");

    assert_eq!(response.get_ref().isolate_state, IsolateState::Ready as i32);
}

#[tokio::test]
async fn create_memshare_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::Public,
            isolate_id: harness.isolate_id,
        })
        .await;

    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .create_memshare(ReceiverStream::new(rx))
        .await
        .expect("create_memshare should succeed")
        .into_inner();

    tx.send(CreateMemshareRequest { region_size: REGION_SIZE, data_scope: None })
        .await
        .expect("Should be able to send memshare request");

    // Check that ContainerManager receives a MountWritableFile request.
    let cm_request = timeout(Duration::from_secs(1), harness.container_manager_rx.recv())
        .await
        .expect("ContainerManager did not receive request in time")
        .expect("ContainerManager request channel should not be closed while harness is in scope");

    let (mount_req, resp_tx) = match cm_request {
        ContainerManagerRequest::MountWritableFile { req, resp } => (req, resp),
        _ => panic!("Expected MountWritableFile request"),
    };
    assert_eq!(mount_req.isolate_id, harness.isolate_id);
    assert_eq!(mount_req.region_size, REGION_SIZE);

    // Simulate successful mount by ContainerManager.
    resp_tx
        .send(Ok(container_manager_request::MountFileResponse {}))
        .expect("Should be able to send MountFileResponse");

    // Check for a successful response from create_memshare.
    let response = response_stream
        .message()
        .await
        .expect("Stream should have a response")
        .expect("Response should be Ok");

    assert!(!response.shared_memory_handle.is_empty());
}

#[tokio::test]
async fn create_memshare_freeze_scope_failure() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    // Do not add the Isolate to the state manager to cause freeze_scope to fail.

    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .create_memshare(ReceiverStream::new(rx))
        .await
        .expect("create_memshare should succeed")
        .into_inner();

    tx.send(enforcer_proto::enforcer::v1::CreateMemshareRequest {
        region_size: REGION_SIZE,
        data_scope: None,
    })
    .await
    .expect("Should be able to send memshare request");

    // Check for a failure response from create_memshare.
    let status = response_stream.message().await.expect_err("Stream should return an error");

    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    // Ensure ContainerManager is not called.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), harness.container_manager_rx.recv())
            .await
            .is_err(),
        "ContainerManager should not have been called"
    );
}

#[tokio::test]
async fn create_memshare_rejects_scope_above_manifest() {
    // Manifest output scope is only enforced for Ratified Isolates.
    let mut harness = TestHarness::new_with_arguments(
        true,
        ScopeDragInstruction::KeepSame,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await;
    add_manifest_output_scope(&harness, DataScopeType::Public).await;

    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .create_memshare(ReceiverStream::new(rx))
        .await
        .expect("create_memshare should succeed")
        .into_inner();

    tx.send(CreateMemshareRequest {
        region_size: REGION_SIZE,
        data_scope: Some(IsolateDataScope {
            scope_type: DataScopeType::UserPrivate.into(),
            ..Default::default()
        }),
    })
    .await
    .expect("Should be able to send memshare request");

    let status = response_stream.message().await.expect_err("Stream should return an error");

    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), harness.container_manager_rx.recv())
            .await
            .is_err(),
        "ContainerManager should not have been called"
    );
}

#[tokio::test]
async fn create_fileshare_rejects_scope_above_manifest() {
    // Manifest output scope is only enforced for Ratified Isolates.
    let harness = TestHarness::new_with_arguments(
        true,
        ScopeDragInstruction::KeepSame,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await;
    add_manifest_output_scope(&harness, DataScopeType::Public).await;

    let mut client = harness.client.clone();
    let status = client
        .create_fileshare(CreateFileshareRequest {
            data_scope: Some(IsolateDataScope {
                scope_type: DataScopeType::UserPrivate.into(),
                ..Default::default()
            }),
        })
        .await
        .expect_err("CreateFileshare should fail");

    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn create_memshare_records_current_scope_of_opaque_isolate() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await;

    // An Opaque Isolate cannot declare its way out of the scope it is currently in.
    let handle = create_memshare_handle(&mut harness, DataScopeType::Public).await;

    assert_eq!(recorded_share_scope(&harness, handle), DataScopeType::UserPrivate);
}

#[tokio::test]
async fn create_memshare_ignores_stricter_declared_scope_of_opaque_isolate() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await;

    let handle = create_memshare_handle(&mut harness, DataScopeType::UserPrivate).await;

    assert_eq!(recorded_share_scope(&harness, handle), DataScopeType::Public);
}

#[tokio::test]
async fn create_memshare_records_declared_scope_of_ratified_isolate() {
    let mut harness = TestHarness::new_with_arguments(
        true,
        ScopeDragInstruction::KeepSame,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await;
    add_manifest_output_scope(&harness, DataScopeType::UserPrivate).await;

    let handle = create_memshare_handle(&mut harness, DataScopeType::Public).await;

    assert_eq!(recorded_share_scope(&harness, handle), DataScopeType::Public);
}

#[tokio::test]
async fn unary_invoke_ez_fails_manifest_validation() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_SOME_DOMAIN.to_string(),
        service_name: TEST_SOME_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    // Add to mapper but not to manifest validator.
    harness
        .mapper
        .add_backend_dependency_service(&service_info, TEST_REMOTE_ROUTE_TYPE)
        .await
        .expect("Failed to add backend dependency");
    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let response = harness.client.invoke_ez(request).await;
    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn stream_invoke_ez_fails_manifest_validation() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let service_info = IsolateServiceInfo {
        operator_domain: TEST_SOME_DOMAIN.to_string(),
        service_name: TEST_SOME_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    // Add to mapper but not to manifest validator.
    harness
        .mapper
        .add_backend_dependency_service(&service_info, TEST_REMOTE_ROUTE_TYPE)
        .await
        .expect("Failed to add backend dependency");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request(&service_info.operator_domain, &service_info.service_name))
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
async fn unary_invoke_ez_fails_scope_enforcement() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let mut request = create_test_request(TEST_SOME_DOMAIN, TEST_SOME_SERVICE_NAME);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);

    harness
        .data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Request with public scope fails
    let response = harness.client.invoke_ez(request).await;
    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn stream_invoke_ez_fails_scope_enforcement() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let mut request = create_test_request(TEST_SOME_DOMAIN, TEST_SOME_SERVICE_NAME);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);

    harness
        .data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Request with public scope fails
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx.send(request).await.expect("Should send request successfully");
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn stream_invoke_ez_fails_scope_enforcement_on_second_request() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
    let junction_calls = harness.mock_junction.call_count.clone();
    let junction_steam_calls = harness.mock_junction.stream_call_count.clone();
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

    // Initial scope is Public.
    // Set up the Isolate to only allow upto User Private scope.
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();

    // First request with Public scope, should pass.
    let mut request1 =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request1
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);
    req_tx.send(request1).await.expect("Should send request successfully");
    let response = response_stream.message().await;
    assert!(response.is_ok());
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(junction_steam_calls.load(Ordering::SeqCst), 1);

    // Update scope to UserPrivate.
    harness
        .data_scope_requester
        .validate_isolate_scope(ValidateIsolateRequest {
            requested_scope: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Second request with Public scope, should fail because current scope is now UserPrivate.
    // Request enforcements prevent emission of Public Scope while being in UserPrivate.
    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);
    req_tx.send(request).await.expect("Should send request successfully");

    // The error from the second request should be received.
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::PermissionDenied);
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(junction_steam_calls.load(Ordering::SeqCst), 1);

    // Third request should fail to send
    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    req_tx.send(request).await.expect_err("Should fail to send 3rd request");
}

#[tokio::test]
async fn stream_invoke_ez_ratified_isolate_skips_scope_enforcement_on_second_request() {
    let mut harness = TestHarness::new_with_arguments(
        true,
        ScopeDragInstruction::KeepSame,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Harness should start"); // Ratified Isolate

    let junction_calls = harness.mock_junction.call_count.clone();
    let junction_steam_calls = harness.mock_junction.stream_call_count.clone();
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

    // Initial scope is Public.
    // Set up the Isolate to only allow upto User Private scope.
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();

    // First request with Public scope.
    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);
    req_tx.send(request).await.expect("Should send request successfully");

    let response = response_stream.message().await;
    assert!(response.is_ok());
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(junction_steam_calls.load(Ordering::SeqCst), 1);

    // Update scope to UserPrivate.
    harness
        .data_scope_requester
        .validate_isolate_scope(ValidateIsolateRequest {
            requested_scope: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Second request with Public scope. For Ratified Isolate, this should still pass.
    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request
        .isolate_request_iscope
        .as_mut()
        .expect("Scope is mentioned in the request")
        .datagram_iscopes[0]
        .set_scope_type(DataScopeType::Public);
    req_tx.send(request).await.expect("Should send request successfully");

    // There should be no error message on the stream.
    let response = response_stream.message().await;
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(junction_steam_calls.load(Ordering::SeqCst), 2);
    assert!(response.is_ok(), "Stream should not have any error messages");
}

#[tokio::test]
async fn unary_invoke_ez_fails_response_scope_enforcement() {
    // Create the test harness that increases response scope by one
    let mut harness = TestHarness::new_with_arguments(
        false,
        ScopeDragInstruction::IncreaseByOne,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Harness should start");

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

    // Set up the Isolate to only allow upto User Private scope.
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");

    // Create a request with Private scope. The mock junction will echo this back with MultiUserPrivate.
    let request = create_test_request_with_scope(
        &service_info.operator_domain,
        &service_info.service_name,
        DataScopeType::UserPrivate,
    );
    let response = harness.client.invoke_ez(request).await;

    assert_eq!(response.expect_err("Request should fail").code(), tonic::Code::PermissionDenied);
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn stream_invoke_ez_fails_response_scope_enforcement() {
    // Create the test harness that increases response scope by one
    let mut harness = TestHarness::new_with_arguments(
        false,
        ScopeDragInstruction::IncreaseByOne,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Harness should start");

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
        RouteType::Internal,
    )
    .await
    .expect("Failed to add backend dependency");

    // Set up the Isolate to be in UserPrivate scope.
    harness
        .data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_test_request_with_scope(
            &service_info.operator_domain,
            &service_info.service_name,
            DataScopeType::Public,
        ))
        .await
        .expect("Should send request successfully");

    // Response fails because it is in MultiUserPrivate Scope
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::PermissionDenied);

    // Ensure that the Isolate remains in User Private
    let current_scope = harness
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id: harness.isolate_id })
        .await
        .expect("Valid GetIsolateScopeResponse")
        .current_scope;
    assert_eq!(current_scope, DataScopeType::UserPrivate);
}

#[tokio::test]
async fn unary_invoke_ez_succeeds_for_less_strict_response_scope() {
    // Create the test harness that decreases response scope by one
    let mut harness = TestHarness::new_with_arguments(
        false,
        ScopeDragInstruction::DecreaseByOne,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Harness should start");

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

    // Set up the Isolate to be in UserPrivate scope.
    harness
        .data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await
        .expect("Should be able to add to DSM/RIM");

    // Create a request with UserPrivate scope. The mock junction will echo this back with Domain Scope.
    // This should succeed because DomainOwned is less strict than UserPrivate.
    let request = create_test_request_with_scope(
        &service_info.operator_domain,
        &service_info.service_name,
        DataScopeType::UserPrivate,
    );
    assert!(harness.client.invoke_ez(request).await.is_ok(), "Request should succeed");
    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
}

#[tokio::test]
async fn stream_invoke_ez_fails_response_scope_enforcement_on_second_request() {
    // Create the test harness that decreases response scope by one
    let mut harness = TestHarness::new_with_arguments(
        false,
        ScopeDragInstruction::IncreaseByOne,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Harness should start");

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

    // Set up the Isolate to only allow up to UserPrivate scope.
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add to DSM/RIM");
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream should set up")
        .into_inner();

    // First request with a valid scope should pass.
    let first_request = create_test_request_with_scope(
        &service_info.operator_domain,
        &service_info.service_name,
        DataScopeType::DomainOwned,
    );
    req_tx.send(first_request).await.expect("Should send request successfully");
    assert!(response_stream.message().await.is_ok());
    // Ensure that the Isolate is now in UserPrivateScope
    let current_scope = harness
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id: harness.isolate_id })
        .await
        .expect("Valid GetIsolateScopeResponse")
        .current_scope;
    assert_eq!(current_scope, DataScopeType::UserPrivate);

    // Second request with a scope that will be rejected on response.
    // Response enforcements fail to scope-drag Isolate to MultiUserPrivate.
    let second_request = create_test_request_with_scope(
        &service_info.operator_domain,
        &service_info.service_name,
        DataScopeType::UserPrivate,
    );

    // Maximum permissible scope is UserPrivate but response scope is MultiUserPrivate
    req_tx.send(second_request).await.expect("Should send request successfully");
    let response = response_stream.message().await;
    assert_eq!(response.expect_err("Stream should fail").code(), tonic::Code::PermissionDenied);

    // Ensure that the Isolate remains in UserPrivateScope
    let current_scope = harness
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id: harness.isolate_id })
        .await
        .expect("Valid GetIsolateScopeResponse")
        .current_scope;
    assert_eq!(current_scope, DataScopeType::UserPrivate);
}

#[tokio::test]
async fn create_fileshare_success() {
    let mut harness = TestHarness::new().await.expect("Harness should start");

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

    let dependency_index =
        harness.mapper.get_service_index(&service_info).await.expect("Service should be indexed");

    // Setup receiver isolate in DSM
    let receiver_binary_index = dependency_index.get_binary_services_index().unwrap();
    let receiver_id = IsolateId::new(receiver_binary_index);

    harness
        .data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::UserPrivate,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: receiver_id,
        })
        .await
        .expect("Should add receiver to DSM");

    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("Should be able to add sender to DSM/RIM");

    // We must intercept ContainerManager requests because CreateFileshare waits for them.
    let mut rx = harness.container_manager_rx;
    tokio::spawn(async move {
        // Expect MountWritableDirectory (for sender)
        match rx.recv().await.expect("Sender mount request") {
            ContainerManagerRequest::MountWritableDirectory { req, resp } => {
                assert!(!req.container_dir_name.is_empty());
                resp.send(Ok(container_manager_request::MountDirectoryResponse {})).unwrap();
            }
            _ => panic!("Expected MountWritableDirectory"),
        }
        // Expect MountReadOnlyDirectory (for receiver)
        match rx.recv().await.expect("Receiver mount request") {
            ContainerManagerRequest::MountReadOnlyDirectory { req, resp } => {
                assert!(!req.container_dir_name.is_empty());
                resp.send(Ok(container_manager_request::MountDirectoryResponse {})).unwrap();
            }
            _ => panic!("Expected MountReadOnlyDirectory"),
        }
    });

    let response = harness
        .client
        .create_fileshare(CreateFileshareRequest { data_scope: None })
        .await
        .expect("CreateFileshare should succeed");
    assert!(!response.get_ref().fileshare_handle.is_empty());
}

#[tokio::test]
async fn unary_routes_shm_payload_to_inline_data() {
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

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();

    // Setup bridge communication buffers in the shared memory manager
    harness
        .shared_memory_manager
        .setup_bridge_communication_buffers(harness.isolate_id, &path)
        .await
        .expect("Failed to setup bridge communication buffers");

    // Create the isolate writer pool
    let isolate_pool = ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: format!("{path}/isolate-writes"),
        number_of_slots: 64,
        slot_size: 4,
        writer: true,
    })
    .expect("Failed to create isolate writer pool");

    let slot_refs =
        isolate_pool.write_to_pool(TEST_PAYLOAD).await.expect("Failed to write to pool");

    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request.isolate_request_payload = Some(EzHybridPayload {
        delivery_method: Some(DeliveryMethod::ShmData(payload_proto::enforcer::v1::ShmSlotData {
            slots: slot_refs,
        })),
    });

    let _ = harness.client.invoke_ez(request).await.expect("Invoke should succeed");

    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");

    let received_request =
        harness.mock_junction.invoked_isolate_requests.lock().unwrap().last().unwrap().clone();

    let payload = received_request.isolate_input.expect("Should have payload");
    let delivery = payload.delivery_method.expect("Should have delivery method");
    match delivery {
        DeliveryMethod::InlineData(data) => {
            assert_eq!(data.datagrams.len(), 1);
            assert_eq!(data.datagrams[0], TEST_PAYLOAD);
        }
        _ => panic!("Expected InlineData delivery method"),
    }
}

#[tokio::test]
async fn unary_routes_shm_payload_read_failure() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
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

    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request.isolate_request_payload = Some(EzHybridPayload {
        delivery_method: Some(DeliveryMethod::ShmData(payload_proto::enforcer::v1::ShmSlotData {
            slots: vec![payload_proto::enforcer::v1::ShmSlotReference { slot_index: 0, length: 4 }],
        })),
    });

    // We did not setup bridge communication buffers, so reading from SHM will fail
    let response = harness.client.invoke_ez(request).await;
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(status.message().contains("Failed to read from SHM"));
}

// Regression test for b/556034613. The SHM payload is read before any routing or policy
// validation runs, so a crafted ShmSlotReference used to drive the enforcer into an
// out-of-bounds atomic write. The bridge must reject the request and stay healthy.
#[tokio::test]
async fn unary_rejects_malicious_shm_slot_reference() {
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

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    harness
        .shared_memory_manager
        .setup_bridge_communication_buffers(harness.isolate_id, &path)
        .await
        .expect("Failed to setup bridge communication buffers");

    // slot_index = 2^44 was entirely unbounded before the fix: depending on the pool geometry it
    // either panicked while slicing the data mapping or drove free_slots into an atomic
    // read-modify-write ~2 TiB past the header mapping. Either outcome takes down the enforcer.
    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request.isolate_request_payload = Some(EzHybridPayload {
        delivery_method: Some(DeliveryMethod::ShmData(payload_proto::enforcer::v1::ShmSlotData {
            slots: vec![payload_proto::enforcer::v1::ShmSlotReference {
                slot_index: 1 << 44,
                length: 0,
            }],
        })),
    });

    let status =
        harness.client.invoke_ez(request).await.expect_err("Hostile reference must be rejected");
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(status.message().contains("Failed to read from SHM"), "got: {}", status.message());

    // The request is rejected while parsing the payload, so no routing decision is ever made.
    assert_eq!(junction_calls.load(Ordering::SeqCst), 0, "Junction should not have been called");

    // The bridge must survive the hostile message and keep serving legitimate SHM traffic.
    let isolate_pool = ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: format!("{path}/isolate-writes"),
        number_of_slots: 64,
        slot_size: 4,
        writer: true,
    })
    .expect("Failed to create isolate writer pool");
    let slot_refs =
        isolate_pool.write_to_pool(TEST_PAYLOAD).await.expect("Failed to write to pool");

    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request.isolate_request_payload = Some(EzHybridPayload {
        delivery_method: Some(DeliveryMethod::ShmData(payload_proto::enforcer::v1::ShmSlotData {
            slots: slot_refs,
        })),
    });
    let _ = harness.client.invoke_ez(request).await.expect("Valid invoke should still succeed");

    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");
    let received_request =
        harness.mock_junction.invoked_isolate_requests.lock().unwrap().last().unwrap().clone();
    let payload = received_request.isolate_input.expect("Should have payload");
    match payload.delivery_method.expect("Should have delivery method") {
        DeliveryMethod::InlineData(data) => {
            assert_eq!(data.datagrams.len(), 1);
            assert_eq!(data.datagrams[0], TEST_PAYLOAD);
        }
        _ => panic!("Expected InlineData delivery method"),
    }
}

#[tokio::test]
async fn stream_routes_shm_payload_to_inline_data() {
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

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();

    // Setup bridge communication buffers in the shared memory manager
    harness
        .shared_memory_manager
        .setup_bridge_communication_buffers(harness.isolate_id, &path)
        .await
        .expect("Failed to setup bridge communication buffers");

    // Create the isolate writer pool
    let isolate_pool = ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: format!("{path}/isolate-writes"),
        number_of_slots: 64,
        slot_size: 4,
        writer: true,
    })
    .expect("Failed to create isolate writer pool");

    let slot_refs =
        isolate_pool.write_to_pool(TEST_PAYLOAD).await.expect("Failed to write to pool");

    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request.isolate_request_payload = Some(EzHybridPayload {
        delivery_method: Some(DeliveryMethod::ShmData(payload_proto::enforcer::v1::ShmSlotData {
            slots: slot_refs,
        })),
    });

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx.send(request).await.expect("Should be able to send request");

    let _ = harness
        .client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Stream invoke should succeed");

    sleep(Duration::from_millis(20)).await;

    assert_eq!(junction_calls.load(Ordering::SeqCst), 1, "Junction should have been called");

    let received_request =
        harness.mock_junction.invoked_isolate_requests.lock().unwrap().last().unwrap().clone();

    let payload = received_request.isolate_input.expect("Should have payload");
    let delivery = payload.delivery_method.expect("Should have delivery method");
    match delivery {
        DeliveryMethod::InlineData(data) => {
            assert_eq!(data.datagrams.len(), 1);
            assert_eq!(data.datagrams[0], TEST_PAYLOAD);
        }
        _ => panic!("Expected InlineData delivery method"),
    }
}

#[tokio::test]
async fn stream_routes_shm_payload_read_failure() {
    let mut harness = TestHarness::new().await.expect("Harness should start");
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

    let mut request =
        create_test_request(&service_info.operator_domain, &service_info.service_name);
    request.isolate_request_payload = Some(EzHybridPayload {
        delivery_method: Some(DeliveryMethod::ShmData(payload_proto::enforcer::v1::ShmSlotData {
            slots: vec![payload_proto::enforcer::v1::ShmSlotReference { slot_index: 0, length: 4 }],
        })),
    });

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx.send(request).await.expect("Should be able to send request");

    // We did not setup bridge communication buffers, so reading from SHM will fail
    let response = harness.client.stream_invoke_ez(ReceiverStream::new(req_rx)).await;
    assert!(response.is_ok());
    let mut response_stream = response.unwrap().into_inner();
    let status =
        response_stream.message().await.expect_err("SHM read failure should fail the stream");
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(status.message().contains("Failed to read from SHM"));
}

#[tokio::test]
async fn unary_routes_shm_response_to_shm_data() {
    // Initialize with a low threshold, so b"hello world" always triggers SHM writing
    let mut harness = TestHarness::new_with_arguments(
        false,
        ScopeDragInstruction::KeepSame,
        SHM_PAYLOAD_LOW_THRESHOLD,
    )
    .await
    .expect("Harness should start");
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

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();

    // Setup bridge communication buffers in the shared memory manager
    harness
        .shared_memory_manager
        .setup_bridge_communication_buffers(harness.isolate_id, &path)
        .await
        .expect("Failed to setup bridge communication buffers");

    // Create the enforcer reader pool to verify written slots
    let enforcer_pool = ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: format!("{path}/enforcer-writes"),
        number_of_slots: 64,
        slot_size: 4,
        writer: false,
    })
    .expect("Failed to create enforcer reader pool");

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let response = harness.client.invoke_ez(request).await;
    assert!(response.is_ok());
    let response_msg = response.unwrap().into_inner();

    let payload = response_msg.ez_response_payload.expect("Should have payload");
    let delivery = payload.delivery_method.expect("Should have delivery method");
    match delivery {
        DeliveryMethod::ShmData(shm_data) => {
            let read_bytes =
                enforcer_pool.read_from_pool(&shm_data.slots).expect("Failed to read from pool");
            assert_eq!(read_bytes, TEST_PAYLOAD);
        }
        _ => panic!("Expected ShmData delivery method"),
    }
}

#[tokio::test]
async fn stream_routes_shm_response_to_shm_data() {
    // Initialize with a threshold of 1 byte, so b"hello world" always triggers SHM writing
    let mut harness = TestHarness::new_with_arguments(false, ScopeDragInstruction::KeepSame, 1)
        .await
        .expect("Harness should start");
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

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();

    // Setup bridge communication buffers in the shared memory manager
    harness
        .shared_memory_manager
        .setup_bridge_communication_buffers(harness.isolate_id, &path)
        .await
        .expect("Failed to setup bridge communication buffers");

    // Create the enforcer reader pool to verify written slots
    let enforcer_pool = ShmSlabPool::new(ShmSlabPoolOptions {
        file_name: format!("{path}/enforcer-writes"),
        number_of_slots: 64,
        slot_size: 4,
        writer: false,
    })
    .expect("Failed to create enforcer reader pool");

    let request = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx.send(request).await.expect("Should be able to send request");

    let response = harness.client.stream_invoke_ez(ReceiverStream::new(req_rx)).await;
    assert!(response.is_ok());
    let mut response_stream = response.unwrap().into_inner();
    let response_msg = response_stream
        .message()
        .await
        .expect("Should receive response message")
        .expect("Response message should be present");

    let payload = response_msg.ez_response_payload.expect("Should have payload");
    let delivery = payload.delivery_method.expect("Should have delivery method");
    match delivery {
        DeliveryMethod::ShmData(shm_data) => {
            let read_bytes =
                enforcer_pool.read_from_pool(&shm_data.slots).expect("Failed to read from pool");
            assert_eq!(read_bytes, TEST_PAYLOAD);
        }
        _ => panic!("Expected ShmData delivery method"),
    }
}

#[tokio::test]
async fn test_midstream_shm_read_failure_across_routes() {
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
    for domain in
        [TEST_INTERNAL_OPERATOR_DOMAIN, TEST_EXTERNAL_OPERATOR_DOMAIN, TEST_REMOTE_OPERATOR_DOMAIN]
    {
        let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
        req_tx.send(create_test_request(domain, TEST_SERVICE_NAME)).await.unwrap();
        let mut response_stream = harness
            .client
            .stream_invoke_ez(ReceiverStream::new(req_rx))
            .await
            .expect("stream setup")
            .into_inner();
        let mut bad_shm_req = create_test_request(domain, TEST_SERVICE_NAME);
        bad_shm_req.isolate_request_payload = Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::ShmData(ShmSlotData {
                slots: vec![payload_proto::enforcer::v1::ShmSlotReference {
                    slot_index: 999_999,
                    length: 8,
                }],
            })),
        });
        req_tx.send(bad_shm_req).await.unwrap();
        drop(req_tx);
        let first = timeout(Duration::from_secs(5), response_stream.message())
            .await
            .expect("stream response should not time out");
        let err = match first {
            Err(status) => status,
            Ok(_) => timeout(Duration::from_secs(5), response_stream.message())
                .await
                .expect("mid-stream SHM error should arrive")
                .expect_err("second message must fail SHM read"),
        };
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("Failed to read from SHM"));
    }
}

#[tokio::test]
async fn test_event_and_ratified_fileshare_validation() {
    let mut harness = TestHarness::new_with_arguments(
        true,
        ScopeDragInstruction::KeepSame,
        SHM_PAYLOAD_LARGE_THRESHOLD,
    )
    .await
    .expect("Ratified harness should start");
    harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id: harness.isolate_id,
        })
        .await;
    add_manifest_output_scope(&harness, DataScopeType::UserPrivate).await;
    let bad_topic_err = harness
        .client
        .publish_event_for(PublishEventForRequest {
            topic: EventTopic::Unspecified.into(),
            ..Default::default()
        })
        .await
        .expect_err("unsupported topic must fail");
    assert_eq!(bad_topic_err.code(), tonic::Code::InvalidArgument);
    let missing_payload_err = harness
        .client
        .publish_event_for(PublishEventForRequest {
            topic: EventTopic::Fileshare.into(),
            handle: "123".to_string(),
            payload: None,
        })
        .await
        .expect_err("missing payload must fail");
    assert_eq!(missing_payload_err.code(), tonic::Code::InvalidArgument);
    let bad_event_err = harness
        .client
        .publish_event_for(PublishEventForRequest {
            topic: EventTopic::Fileshare.into(),
            handle: "123".to_string(),
            payload: Some(PublishPayload::FileshareEvent(FileshareEvent { event_type: 999 })),
        })
        .await
        .expect_err("unknown event type must fail");
    assert_eq!(bad_event_err.code(), tonic::Code::InvalidArgument);
    let sub_err = harness
        .client
        .stream_subscribe_to(StreamSubscribeToRequest { topic: EventTopic::Unspecified.into() })
        .await
        .expect_err("unsupported subscribe topic must fail");
    assert_eq!(sub_err.code(), tonic::Code::InvalidArgument);
    let unspec_err = harness
        .client
        .create_fileshare(CreateFileshareRequest { data_scope: None })
        .await
        .expect_err("Ratified isolate without declared share scope must fail");
    assert_eq!(unspec_err.code(), tonic::Code::InvalidArgument);
    let cm_join = tokio::spawn(async move {
        if let Some(ContainerManagerRequest::MountWritableDirectory { resp, .. }) =
            harness.container_manager_rx.recv().await
        {
            let _ = resp.send(Ok(container_manager_request::MountDirectoryResponse {}));
        }
    });
    let created = harness
        .client
        .create_fileshare(CreateFileshareRequest {
            data_scope: Some(IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                ..Default::default()
            }),
        })
        .await
        .expect("Ratified create_fileshare with declared scope should succeed")
        .into_inner();
    assert!(!created.fileshare_handle.is_empty());
    cm_join.await.unwrap();
}

#[tokio::test]
async fn test_shm_response_fallback_to_inline_when_unallocated() {
    let mut harness = TestHarness::new_with_arguments(
        false,
        ScopeDragInstruction::KeepSame,
        SHM_PAYLOAD_LOW_THRESHOLD,
    )
    .await
    .expect("Harness with 0 SHM threshold should start");
    let internal_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: TEST_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    add_backend_dependencies(
        harness.isolate_id,
        harness.mapper.clone(),
        harness.manifest_validator.clone(),
        &internal_info,
        true,
        TEST_INTERNAL_ROUTE_TYPE,
    )
    .await
    .expect("add internal dep");
    add_to_data_scope_requester(harness.data_scope_requester.clone(), harness.isolate_id)
        .await
        .expect("add isolate scope");
    let unary_resp = harness
        .client
        .invoke_ez(create_test_request(TEST_INTERNAL_OPERATOR_DOMAIN, TEST_SERVICE_NAME))
        .await
        .expect("unary invoke should succeed with inline fallback")
        .into_inner();
    assert_eq!(
        unary_resp.ez_response_payload,
        Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![TEST_PAYLOAD.to_vec()],
            })),
        })
    );
}

/// Creates a memshare through the service, declaring `declared_scope`, and returns its handle.
async fn create_memshare_handle(
    harness: &mut TestHarness,
    declared_scope: DataScopeType,
) -> String {
    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    let mut response_stream = harness
        .client
        .create_memshare(ReceiverStream::new(rx))
        .await
        .expect("create_memshare should succeed")
        .into_inner();
    tx.send(CreateMemshareRequest {
        region_size: REGION_SIZE,
        data_scope: Some(IsolateDataScope {
            scope_type: declared_scope.into(),
            ..Default::default()
        }),
    })
    .await
    .expect("Should be able to send memshare request");
    match timeout(Duration::from_secs(1), harness.container_manager_rx.recv())
        .await
        .expect("ContainerManager did not receive request in time")
        .expect("ContainerManager request channel should not be closed")
    {
        ContainerManagerRequest::MountWritableFile { resp, .. } => resp
            .send(Ok(container_manager_request::MountFileResponse {}))
            .expect("Should be able to send MountFileResponse"),
        _ => panic!("Expected MountWritableFile request"),
    }
    response_stream
        .message()
        .await
        .expect("Stream should have a response")
        .expect("Response should be Ok")
        .shared_memory_handle
}

/// Returns the scope the `SharedMemManager` recorded for `handle`.
fn recorded_share_scope(harness: &TestHarness, handle: String) -> DataScopeType {
    let receiver_id = IsolateId::new(BinaryServicesIndex::new(false));
    harness
        .shared_memory_manager
        .share_scope(harness.isolate_id, receiver_id, &[handle])
        .expect("Share should be valid")
}

async fn add_manifest_output_scope(harness: &TestHarness, max_output_scope: DataScopeType) {
    harness
        .manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index: harness.isolate_id.get_binary_services_index(),
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope,
        })
        .await
        .expect("Should add manifest scope info");
}

async fn setup_notify_state_stream(
    client: &mut IsolateEzBridgeClient<Channel>,
) -> Result<(Sender<NotifyIsolateStateRequest>, Streaming<NotifyIsolateStateResponse>)> {
    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    let inbound = client
        .notify_isolate_state(ReceiverStream::new(rx))
        .await
        .context("Failed to get inbound channel")?;
    let response_stream = inbound.into_inner();
    Ok((tx, response_stream))
}
