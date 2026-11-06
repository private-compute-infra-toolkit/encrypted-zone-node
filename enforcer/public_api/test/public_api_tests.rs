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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use container_manager_requester::ContainerManagerRequester;
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::{DataScopeType, EzDataScope, EzStaticScopeInfo};
use error_detail_proto::enforcer::v1::ez_error_detail::ErrorSource;
use error_detail_proto::enforcer::v1::{ez_error_detail::ErrorReason, EzErrorDetail};
use error_details_proto::google::rpc::BadRequest;
use ez_service_proto::enforcer::v1::{
    ez_public_api_client::EzPublicApiClient, ez_public_api_server::EzPublicApiServer,
};
use ez_service_proto::enforcer::v1::{
    CallParameters, CallRequest, CallResponse, EncryptedField, GetHealthReportRequest,
    SessionMetadata,
};
use googleapis_tonic_google_rpc::google::rpc::Status as RpcStatus;
use health_manager::HealthManager;
use interceptor::Interceptor;
use isolate_info::IsolateServiceInfo;
use isolate_service_mapper::IsolateServiceMapper;
use isolate_test_utils::{TEST_ERROR_CODE, TEST_ERROR_MESSAGE, TEST_ERROR_SERVICE_NAME};
use junction_test_utils::FakeJunction;
use junction_test_utils::UNKNOWN_ISOLATE_DOMAIN;
use manifest_proto::enforcer::v1::InterceptingServices;
use prost::Message;
use public_api::EzPublicApiService;
use state_manager::IsolateStateManager;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::Code;
use tonic::Request;

const EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE: usize = 10;
const OPAQUE_DOMAIN: &str = "opaque.domain";
const OPAQUE_SERVICE: &str = "OpaqueService";
const RATIFIED_INTERCEPTOR_DOMAIN: &str = "ratified.interceptor.domain";
const RATIFIED_INTERCEPTOR_SERVICE: &str = "RatifiedInterceptorService";
const RATIFIED_INTERCEPTOR_UNARY: &str = "unary";
const RATIFIED_INTERCEPTOR_STREAMING: &str = "streaming";
const TEST_DOMAIN: &str = "test_domain";
const TEST_SERVICE: &str = "test_service";
const TEST_METHOD: &str = "test_method";

#[tokio::test]
async fn test_call_error_missing_session_metadata() {
    let (port, shutdown_tx, _) = start_api_server().await;

    let invalid_request = CallRequest {
        operator_domain: "test_domain".to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: None, // Missing session data
        input_params: Some(CallParameters::default()),
        ..Default::default()
    };

    let response_result = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(invalid_request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    verify_missing_field_error(response_result.unwrap_err(), "session_metadata");
}

#[tokio::test]
async fn test_call_error_no_matching_isolate() {
    let (port, shutdown_tx, _) = start_api_server().await;

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };

    let invalid_request = CallRequest {
        operator_domain: UNKNOWN_ISOLATE_DOMAIN.to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters::default()),
        ..Default::default()
    };

    let response_result = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(invalid_request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    match response_result {
        Ok(resp) => panic!("Expected an error, but got Ok: {:?}", resp),
        Err(status) => {
            assert_eq!(
                status.code(),
                Code::InvalidArgument,
                "Expected InvalidArgument, but got {:?}",
                status.code()
            );
        }
    }
}

#[tokio::test]
async fn test_call_propagates_timeout() {
    let (port, shutdown_tx, fake_junction) = start_api_server().await;

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };

    let valid_request = CallRequest {
        operator_domain: "test_domain".to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters::default()),
        ..Default::default()
    };

    let mut request = Request::new(valid_request);
    request.metadata_mut().insert("grpc-timeout", MetadataValue::from_static("5S"));

    let _response_result = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let captured_deadline = fake_junction.last_deadline.lock().unwrap();
    assert!(captured_deadline.is_some());
}

#[tokio::test]
async fn test_stream_call_propagates_timeout() {
    let (port, shutdown_tx, fake_junction) = start_api_server().await;

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };

    let valid_request = CallRequest {
        operator_domain: "test_domain".to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters::default()),
        ..Default::default()
    };

    let (client_tx, client_rx) =
        tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
    let request_stream = tokio_stream::wrappers::ReceiverStream::new(client_rx);
    let mut request = Request::new(request_stream);
    request.metadata_mut().insert("grpc-timeout", MetadataValue::from_static("5S"));

    tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        let mut stream = client.stream_call(request).await.unwrap().into_inner();
        let _ = client_tx.send(valid_request).await;
        let _ = stream.message().await;
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let captured_timeout = fake_junction.last_stream_timeout.lock().unwrap();
    assert_eq!(*captured_timeout, Some(Duration::from_secs(5)));
}

#[tokio::test]
async fn test_call_succeeds() {
    let (port, shutdown_tx, _) = start_api_server().await;

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };

    let valid_request = CallRequest {
        operator_domain: "test_domain".to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters::default()),
        ..Default::default()
    };

    let response_result = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(valid_request).await.unwrap().into_inner()
    })
    .await
    .unwrap();

    let expected_response =
        CallResponse { session_metadata: Some(session_metadata), ..Default::default() };

    let _ = shutdown_tx.send(());

    assert_eq!(response_result, expected_response);
}

#[tokio::test]
async fn test_stream_call_error_initial_request_incomplete() {
    let (port, shutdown_tx, _) = start_api_server().await;

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };

    let invalid_request = CallRequest {
        operator_domain: "".to_string(), // Missing operator_domain
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters::default()),
        ..Default::default()
    };
    let (client_tx, client_rx) =
        tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
    let mut response_stream = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(client_rx);
        let response_stream = client.stream_call(request_stream).await.unwrap().into_inner();
        let _ = client_tx.send(invalid_request).await;
        response_stream
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let response_result = response_stream.message().await;

    verify_missing_field_error(response_result.unwrap_err(), "operator_domain");
}
#[tokio::test]
async fn test_stream_call_succeeds() {
    let (port, shutdown_tx, _) = start_api_server().await;

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };
    let expected_initial_bytes = vec![1];
    let expected_second_request_bytes = vec![2];

    let valid_first_request = CallRequest {
        operator_domain: "test_domain".to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters {
            public_input: expected_initial_bytes.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Note for second request+ we don't have to specify Isolate targeting fields
    let valid_second_request = CallRequest {
        operator_domain: "".to_string(),
        service_name: "".to_string(),
        method_name: "".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters {
            public_input: expected_second_request_bytes.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let initial_expected_response =
        CallResponse { public_output: expected_initial_bytes.clone(), ..Default::default() };
    let second_expected_response =
        CallResponse { public_output: expected_second_request_bytes.clone(), ..Default::default() };
    let (client_tx, client_rx) =
        tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
    let mut response_stream = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(client_rx);
        let mut response_stream = client.stream_call(request_stream).await.unwrap().into_inner();
        let _ = client_tx.send(valid_first_request).await;
        let first_response = response_stream.message().await.unwrap();
        assert_eq!(first_response.unwrap(), initial_expected_response);
        let _ = client_tx.send(valid_second_request).await;
        response_stream
    })
    .await
    .unwrap();

    let response_result = response_stream.message().await.unwrap();

    assert_eq!(response_result.unwrap(), second_expected_response);

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_call_succeeds_with_empty_call_parameters() {
    let (port, shutdown_tx, _) = start_api_server().await;

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };

    let valid_request = CallRequest {
        operator_domain: "test_domain".to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters {
            public_input: vec![],
            encrypted_input: vec![],
            request_metadata: HashMap::new(),
            request_extensions: vec![],
        }),
        ..Default::default()
    };

    let response_result = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(valid_request).await.unwrap().into_inner()
    })
    .await
    .unwrap();

    let expected_response = CallResponse {
        session_metadata: Some(session_metadata),
        public_output: vec![],
        encrypted_output: vec![],
        response_metadata: HashMap::new(),
        response_extensions: vec![],
    };

    let _ = shutdown_tx.send(());

    assert_eq!(response_result, expected_response);
}

#[tokio::test]
async fn test_interceptor_unary() {
    let (port, shutdown_tx, fake_junction) = start_api_server().await;
    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };
    let call_request = CallRequest {
        operator_domain: OPAQUE_DOMAIN.to_string(),
        service_name: OPAQUE_SERVICE.to_string(),
        method_name: "OriginalUnaryMethod".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters::default()),
        ..Default::default()
    };
    let _ = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(call_request).await.unwrap().into_inner()
    })
    .await
    .unwrap();

    let invoke_isolate_req = fake_junction.invoked_isolate_requests.lock().unwrap()[0].clone();
    let cpm = invoke_isolate_req.control_plane_metadata.unwrap();

    let _ = shutdown_tx.send(());
    assert_eq!(cpm.destination_operator_domain, RATIFIED_INTERCEPTOR_DOMAIN);
    assert_eq!(cpm.destination_service_name, RATIFIED_INTERCEPTOR_SERVICE);
    assert_eq!(cpm.destination_method_name, RATIFIED_INTERCEPTOR_UNARY);
}

#[tokio::test]
async fn test_get_health_report() {
    let (port, shutdown_tx, _) = start_api_server().await;
    let response_result = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.get_health_report(GetHealthReportRequest {}).await.unwrap().into_inner()
    })
    .await
    .unwrap();
    let _ = shutdown_tx.send(());
    assert!(response_result.health_report.is_some());
    let report = response_result.health_report.unwrap();
    assert!(report.start_timestamp > 0);
    assert!(report.end_timestamp >= report.start_timestamp);
    assert!(report.isolates.is_empty());
}

#[tokio::test]
async fn test_interceptor_streaming() {
    let (port, shutdown_tx, fake_junction) = start_api_server().await;
    let valid_first_request = CallRequest {
        operator_domain: OPAQUE_DOMAIN.to_string(),
        service_name: OPAQUE_SERVICE.to_string(),
        method_name: "OriginalStreamingMethod".to_string(),
        session_metadata: Some(SessionMetadata { session_id: port as u64, ..Default::default() }),
        input_params: Some(CallParameters { public_input: vec![1], ..Default::default() }),
        ..Default::default()
    };

    let (client_tx, client_rx) =
        tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
    let mut response_stream = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(client_rx);
        let response_stream = client.stream_call(request_stream).await.unwrap().into_inner();
        let _ = client_tx.send(valid_first_request).await;
        drop(client_tx);
        response_stream
    })
    .await
    .unwrap();
    let _ = response_stream.message().await.unwrap();

    let invoke_isolate_req = fake_junction.invoked_isolate_requests.lock().unwrap()[0].clone();
    let cpm = invoke_isolate_req.control_plane_metadata.unwrap();

    let _ = shutdown_tx.send(());
    assert_eq!(cpm.destination_operator_domain, RATIFIED_INTERCEPTOR_DOMAIN);
    assert_eq!(cpm.destination_service_name, RATIFIED_INTERCEPTOR_SERVICE);
    assert_eq!(cpm.destination_method_name, RATIFIED_INTERCEPTOR_STREAMING);
}

#[tokio::test]
async fn test_interceptor_unary_with_qualifiers() {
    let (port, shutdown_tx, fake_junction) = start_api_server().await;
    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };
    let call_request = CallRequest {
        operator_domain: OPAQUE_DOMAIN.to_string(),
        service_name: "QualifiedOpaqueService".to_string(),
        isolate_name: "opaque_iso".to_string(),
        publisher_id: "opaque_pub".to_string(),
        method_name: "OriginalUnaryMethod".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters::default()),
    };
    let _ = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(call_request).await.unwrap().into_inner()
    })
    .await
    .unwrap();

    let invoke_isolate_req = fake_junction.invoked_isolate_requests.lock().unwrap()[0].clone();
    let cpm = invoke_isolate_req.control_plane_metadata.unwrap();

    let _ = shutdown_tx.send(());
    assert_eq!(cpm.destination_operator_domain, RATIFIED_INTERCEPTOR_DOMAIN);
    assert_eq!(cpm.destination_service_name, "QualifiedRatifiedService");
    assert_eq!(cpm.destination_isolate_name, "ratified_iso");
    assert_eq!(cpm.destination_publisher_id, "ratified_pub");
    assert_eq!(cpm.destination_method_name, RATIFIED_INTERCEPTOR_UNARY);
}

#[tokio::test]
async fn test_interceptor_streaming_with_qualifiers() {
    let (port, shutdown_tx, fake_junction) = start_api_server().await;
    let valid_first_request = CallRequest {
        operator_domain: OPAQUE_DOMAIN.to_string(),
        service_name: "QualifiedOpaqueService".to_string(),
        isolate_name: "opaque_iso".to_string(),
        publisher_id: "opaque_pub".to_string(),
        method_name: "OriginalStreamingMethod".to_string(),
        session_metadata: Some(SessionMetadata { session_id: port as u64, ..Default::default() }),
        input_params: Some(CallParameters { public_input: vec![1], ..Default::default() }),
    };

    let (client_tx, client_rx) =
        tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
    let mut response_stream = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(client_rx);
        let response_stream = client.stream_call(request_stream).await.unwrap().into_inner();
        let _ = client_tx.send(valid_first_request).await;
        drop(client_tx);
        response_stream
    })
    .await
    .unwrap();
    let _ = response_stream.message().await.unwrap();

    let invoke_isolate_req = fake_junction.invoked_isolate_requests.lock().unwrap()[0].clone();
    let cpm = invoke_isolate_req.control_plane_metadata.unwrap();

    let _ = shutdown_tx.send(());
    assert_eq!(cpm.destination_operator_domain, RATIFIED_INTERCEPTOR_DOMAIN);
    assert_eq!(cpm.destination_service_name, "QualifiedRatifiedService");
    assert_eq!(cpm.destination_isolate_name, "ratified_iso");
    assert_eq!(cpm.destination_publisher_id, "ratified_pub");
    assert_eq!(cpm.destination_method_name, RATIFIED_INTERCEPTOR_STREAMING);
}

fn verify_missing_field_error(status: tonic::Status, field_name: &str) {
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), format!("Request missing required field {}", field_name));

    let status_details_bytes = status.details();
    let rpc_status = RpcStatus::decode(status_details_bytes).expect("Should decode RpcStatus");

    let ez_detail_any = rpc_status
        .details
        .iter()
        .find(|any| any.type_url == "type.googleapis.com/enforcer.v1.EzErrorDetail")
        .expect("Should find EzErrorDetail");
    let ez_detail =
        EzErrorDetail::decode(&ez_detail_any.value[..]).expect("Should decode EzErrorDetail");
    assert_eq!(ez_detail.source, ErrorSource::Enforcer as i32);
    assert_eq!(ez_detail.error_reason, ErrorReason::Unspecified as i32);

    let bad_req_any = rpc_status
        .details
        .iter()
        .find(|any| any.type_url == "type.googleapis.com/google.rpc.BadRequest")
        .expect("Should find BadRequest");
    let bad_req = BadRequest::decode(&bad_req_any.value[..]).expect("Should decode BadRequest");
    assert_eq!(bad_req.field_violations.len(), 1);
    assert_eq!(bad_req.field_violations[0].field, field_name);
    assert_eq!(bad_req.field_violations[0].description, "");
}

/// Start PublicApi server. Returns (port, shutdown_channel, fake_junction).
async fn start_api_server() -> (u16, oneshot::Sender<()>, FakeJunction) {
    let fake_junction = FakeJunction::default();

    let interceptor = setup_interceptor().await;

    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let cm_req = ContainerManagerRequester::new(tx);
    let ds_req = DataScopeRequester::new(0);
    let ism = IsolateStateManager::new(ds_req.clone(), cm_req.clone());
    let health_manager = HealthManager::new(ism, cm_req, IsolateServiceMapper::default(), ds_req);
    // Run once to populate the latest report.
    health_manager.run().await;

    let test_service =
        EzPublicApiService::new(Box::new(fake_junction.clone()), interceptor, health_manager).await;

    let sockaddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let listener = TcpListener::bind(sockaddr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(EzPublicApiServer::new(test_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                shutdown_rx.await.ok();
            })
            .await
    });
    // Explicitly wait for readiness by polling the health report
    let mut ready = false;
    for _ in 0..50 {
        // Try for up to 2.5 seconds
        if let Ok(mut client) =
            EzPublicApiClient::connect(format!("http://localhost:{}", port)).await
        {
            if client.get_health_report(GetHealthReportRequest {}).await.is_ok() {
                ready = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "Server did not become ready in time");

    (port, shutdown_tx, fake_junction)
}

async fn setup_interceptor() -> Interceptor {
    let mapper = IsolateServiceMapper::default();
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: OPAQUE_DOMAIN.to_string(),
                service_name: OPAQUE_SERVICE.to_string(),
                ..Default::default()
            }],
            false, // is_ratified
            "publisher_id".to_string(),
            "isolate_name".to_string(),
        )
        .await
        .unwrap();
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
                service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
                ..Default::default()
            }],
            true, // is_ratified
            "ratified_publisher_id".to_string(),
            "ratified_isolate_name".to_string(),
        )
        .await
        .unwrap();
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: OPAQUE_DOMAIN.to_string(),
                service_name: "QualifiedOpaqueService".to_string(),
                isolate_name: "opaque_iso".to_string(),
                publisher_id: "opaque_pub".to_string(),
            }],
            false,
            "opaque_pub".to_string(),
            "opaque_iso".to_string(),
        )
        .await
        .unwrap();
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
                service_name: "QualifiedRatifiedService".to_string(),
                isolate_name: "ratified_iso".to_string(),
                publisher_id: "ratified_pub".to_string(),
            }],
            true,
            "ratified_pub".to_string(),
            "ratified_iso".to_string(),
        )
        .await
        .unwrap();
    let interceptor = Interceptor::new(mapper);
    let intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: OPAQUE_SERVICE.to_string(),
        interceptor_operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        interceptor_service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
        interceptor_method_for_unary: RATIFIED_INTERCEPTOR_UNARY.to_string(),
        interceptor_method_for_streaming: RATIFIED_INTERCEPTOR_STREAMING.to_string(),
        ..Default::default()
    };
    interceptor.add_interceptor(intercepting_services).await.unwrap();
    let qualified_intercepting_services = InterceptingServices {
        intercepting_operator_domain: OPAQUE_DOMAIN.to_string(),
        intercepting_service_name: "QualifiedOpaqueService".to_string(),
        intercepting_isolate_name: "opaque_iso".to_string(),
        intercepting_publisher_id: "opaque_pub".to_string(),
        interceptor_operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
        interceptor_service_name: "QualifiedRatifiedService".to_string(),
        interceptor_isolate_name: "ratified_iso".to_string(),
        interceptor_publisher_id: "ratified_pub".to_string(),
        interceptor_method_for_unary: RATIFIED_INTERCEPTOR_UNARY.to_string(),
        interceptor_method_for_streaming: RATIFIED_INTERCEPTOR_STREAMING.to_string(),
    };
    interceptor.add_interceptor(qualified_intercepting_services).await.unwrap();
    interceptor
}

#[tokio::test]
async fn test_request_metadata_propagated() {
    let (port, shutdown_tx, fake_junction) = start_api_server().await;

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };

    let mut request_metadata = HashMap::new();
    let key_for_string = "string-key";
    let valid_utf8 = "typical value";
    let key_for_bytes = "byte-key";
    let invalid_utf8 = vec![0, 159, 146, 150];
    request_metadata.insert(key_for_string.to_string(), valid_utf8.as_bytes().to_vec());
    request_metadata.insert(key_for_bytes.to_string(), invalid_utf8.clone());

    let valid_request = CallRequest {
        operator_domain: "test_domain".to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters { request_metadata, ..Default::default() }),
        ..Default::default()
    };

    let _response_result = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(valid_request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let invoke_isolate_req = fake_junction.invoked_isolate_requests.lock().unwrap()[0].clone();
    let cpm = invoke_isolate_req.control_plane_metadata.unwrap();

    assert_eq!(cpm.request_metadata.get(key_for_string).unwrap(), valid_utf8.as_bytes());
    assert_eq!(cpm.request_metadata.get(key_for_bytes).unwrap(), &invalid_utf8);
}

#[tokio::test]
async fn test_extensions_propagated() {
    let (port, shutdown_tx, fake_junction) = start_api_server().await;

    let session_metadata = SessionMetadata { session_id: port as u64, ..Default::default() };

    let ext_bytes = vec![1, 2, 3, 4];

    let valid_request = CallRequest {
        operator_domain: "test_domain".to_string(),
        service_name: "test_service".to_string(),
        method_name: "test_method".to_string(),
        session_metadata: Some(session_metadata.clone()),
        input_params: Some(CallParameters {
            request_extensions: ext_bytes.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let response_result = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(valid_request).await.unwrap().into_inner()
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    // In this test, the fake junction echo's the request back, including the extensions into response_extensions.
    let invoke_isolate_req = fake_junction.invoked_isolate_requests.lock().unwrap()[0].clone();
    let cpm = invoke_isolate_req.control_plane_metadata.unwrap();

    assert_eq!(cpm.extensions, ext_bytes);
    assert_eq!(response_result.response_extensions, ext_bytes);
}

#[tokio::test]
async fn test_call_error_missing_input_params() {
    let (port, shutdown_tx, _) = start_api_server().await;
    let invalid_request = CallRequest {
        operator_domain: TEST_DOMAIN.to_string(),
        service_name: TEST_SERVICE.to_string(),
        method_name: TEST_METHOD.to_string(),
        session_metadata: Some(SessionMetadata { session_id: port as u64, ..Default::default() }),
        input_params: None,
        ..Default::default()
    };
    let response_result = tokio::spawn(async move {
        let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
            .await
            .expect("failed to connect to EzPublicApi");
        client.call(invalid_request).await
    })
    .await
    .unwrap();
    let _ = shutdown_tx.send(());
    verify_missing_field_error(response_result.unwrap_err(), "input_params");
}

#[tokio::test]
async fn test_stream_call_validation_errors() {
    let (port, shutdown_tx, _) = start_api_server().await;
    let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
        .await
        .expect("failed to connect to EzPublicApi");
    // 1. Empty stream closed before initial request.
    let (empty_tx, empty_rx) = tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
    drop(empty_tx);
    let mut empty_stream = client
        .stream_call(tokio_stream::wrappers::ReceiverStream::new(empty_rx))
        .await
        .unwrap()
        .into_inner();
    let empty_err = timeout(Duration::from_secs(5), empty_stream.message())
        .await
        .expect("empty stream should not time out")
        .expect_err("empty stream should return error");
    assert_eq!(empty_err.code(), Code::Unavailable);
    // 2. Missing service_name, method_name, session_metadata, and initial input_params.
    let base_req = CallRequest {
        operator_domain: TEST_DOMAIN.to_string(),
        service_name: TEST_SERVICE.to_string(),
        method_name: TEST_METHOD.to_string(),
        session_metadata: Some(SessionMetadata { session_id: port as u64, ..Default::default() }),
        input_params: Some(CallParameters::default()),
        ..Default::default()
    };
    let cases = [
        (CallRequest { service_name: String::new(), ..base_req.clone() }, "service_name"),
        (CallRequest { method_name: String::new(), ..base_req.clone() }, "method_name"),
        (CallRequest { session_metadata: None, ..base_req.clone() }, "session_metadata"),
        (CallRequest { input_params: None, ..base_req.clone() }, "input_params"),
    ];
    for (bad_req, expected_field) in cases {
        let (tx, rx) = tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
        tx.send(bad_req).await.unwrap();
        drop(tx);
        let mut resp_stream = client
            .stream_call(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let status = timeout(Duration::from_secs(5), resp_stream.message())
            .await
            .expect("validation error should arrive within timeout")
            .expect_err("expected missing field status");
        verify_missing_field_error(status, expected_field);
    }
    // 3. Subsequent stream request missing input_params fails cleanly.
    let (tx, rx) = tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
    let mut resp_stream = client
        .stream_call(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    tx.send(base_req.clone()).await.unwrap();
    let first = timeout(Duration::from_secs(5), resp_stream.message())
        .await
        .expect("first message should arrive")
        .expect("first message should succeed");
    assert!(first.is_some());
    tx.send(CallRequest { input_params: None, ..Default::default() }).await.unwrap();
    drop(tx);
    let second_err = timeout(Duration::from_secs(5), resp_stream.message())
        .await
        .expect("second message error should arrive")
        .expect_err("second message without input_params should fail");
    verify_missing_field_error(second_err, "input_params");
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_stream_call_propagates_isolate_error() {
    let (port, shutdown_tx, _) = start_api_server().await;
    let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
        .await
        .expect("failed to connect to EzPublicApi");
    let (client_tx, client_rx) =
        tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
    let mut response_stream = client
        .stream_call(tokio_stream::wrappers::ReceiverStream::new(client_rx))
        .await
        .unwrap()
        .into_inner();
    client_tx
        .send(CallRequest {
            operator_domain: TEST_DOMAIN.to_string(),
            service_name: TEST_ERROR_SERVICE_NAME.to_string(),
            method_name: TEST_METHOD.to_string(),
            session_metadata: Some(SessionMetadata {
                session_id: port as u64,
                ..Default::default()
            }),
            input_params: Some(CallParameters::default()),
            ..Default::default()
        })
        .await
        .unwrap();
    drop(client_tx);
    let err_status = timeout(Duration::from_secs(5), response_stream.message())
        .await
        .expect("isolate error should arrive before timeout")
        .expect_err("expected isolate error status");
    assert_eq!(err_status.code(), TEST_ERROR_CODE.into());
    assert!(err_status.message().contains(TEST_ERROR_MESSAGE));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_call_and_stream_encrypted_input_and_trace_headers() {
    const DOMAIN_SECRET_CIPHERTEXT: &[u8] = b"domain-secret";
    const UNTYPED_SCOPE_CIPHERTEXT: &[u8] = b"untyped-scope";
    const MISSING_SCOPE_CIPHERTEXT: &[u8] = b"missing-scope";
    const UNARY_PUBLIC_PAYLOAD: &[u8] = b"unary-pub";
    const STREAM_PUBLIC_PAYLOAD_1: &[u8] = b"stream-pub-1";
    const STREAM_PUBLIC_PAYLOAD_2: &[u8] = b"stream-pub-2";
    const TRACEPARENT_HEADER: &str = "traceparent";
    const TRACEPARENT_VALUE: &[u8] = b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const TRACESTATE_HEADER: &str = "tracestate";
    const TRACESTATE_VALUE: &[u8] = b"vendor=state1";
    const CUSTOM_HEADER: &str = "x-custom";
    const CUSTOM_HEADER_VALUE: &[u8] = b"custom-val";

    let (port, shutdown_tx, fake_junction) = start_api_server().await;
    let mut client = EzPublicApiClient::connect(format!("http://localhost:{}", port))
        .await
        .expect("failed to connect to EzPublicApi");

    let make_params = |pub_bytes: &[u8]| CallParameters {
        public_input: pub_bytes.to_vec(),
        encrypted_input: vec![
            EncryptedField {
                scope: Some(EzDataScope {
                    static_info: Some(EzStaticScopeInfo {
                        scope_type: DataScopeType::DomainOwned.into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ciphertext: DOMAIN_SECRET_CIPHERTEXT.to_vec(),
            },
            EncryptedField {
                scope: Some(EzDataScope { static_info: None, ..Default::default() }),
                ciphertext: UNTYPED_SCOPE_CIPHERTEXT.to_vec(),
            },
            EncryptedField { scope: None, ciphertext: MISSING_SCOPE_CIPHERTEXT.to_vec() },
        ],
        request_metadata: HashMap::from([
            (TRACEPARENT_HEADER.to_string(), TRACEPARENT_VALUE.to_vec()),
            (TRACESTATE_HEADER.to_string(), TRACESTATE_VALUE.to_vec()),
            (CUSTOM_HEADER.to_string(), CUSTOM_HEADER_VALUE.to_vec()),
        ]),
        request_extensions: vec![],
    };

    let unary_resp = client
        .call(CallRequest {
            operator_domain: TEST_DOMAIN.to_string(),
            service_name: TEST_SERVICE.to_string(),
            method_name: TEST_METHOD.to_string(),
            session_metadata: Some(SessionMetadata {
                session_id: port as u64,
                ..Default::default()
            }),
            input_params: Some(make_params(UNARY_PUBLIC_PAYLOAD)),
            ..Default::default()
        })
        .await
        .expect("unary call should succeed")
        .into_inner();
    assert_eq!(unary_resp.public_output, UNARY_PUBLIC_PAYLOAD);
    assert_eq!(unary_resp.encrypted_output.len(), 3);
    assert_eq!(unary_resp.encrypted_output[0].ciphertext, DOMAIN_SECRET_CIPHERTEXT);
    assert_eq!(
        unary_resp.encrypted_output[0]
            .scope
            .as_ref()
            .unwrap()
            .static_info
            .as_ref()
            .unwrap()
            .scope_type,
        i32::from(DataScopeType::DomainOwned)
    );
    assert_eq!(unary_resp.encrypted_output[1].ciphertext, UNTYPED_SCOPE_CIPHERTEXT);
    assert_eq!(unary_resp.encrypted_output[2].ciphertext, MISSING_SCOPE_CIPHERTEXT);

    let (tx, rx) = tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE);
    let mut stream = client
        .stream_call(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    tx.send(CallRequest {
        operator_domain: TEST_DOMAIN.to_string(),
        service_name: TEST_SERVICE.to_string(),
        method_name: TEST_METHOD.to_string(),
        session_metadata: Some(SessionMetadata { session_id: port as u64, ..Default::default() }),
        input_params: Some(make_params(STREAM_PUBLIC_PAYLOAD_1)),
        ..Default::default()
    })
    .await
    .unwrap();
    tx.send(CallRequest {
        input_params: Some(make_params(STREAM_PUBLIC_PAYLOAD_2)),
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);

    let first_stream_resp =
        timeout(Duration::from_secs(5), stream.message()).await.unwrap().unwrap().unwrap();
    let second_stream_resp =
        timeout(Duration::from_secs(5), stream.message()).await.unwrap().unwrap().unwrap();
    assert_eq!(first_stream_resp.public_output, STREAM_PUBLIC_PAYLOAD_1);
    assert_eq!(first_stream_resp.encrypted_output.len(), 3);
    assert_eq!(second_stream_resp.public_output, STREAM_PUBLIC_PAYLOAD_2);
    assert_eq!(second_stream_resp.encrypted_output.len(), 3);

    let invoked_requests = fake_junction.invoked_isolate_requests.lock().unwrap();
    assert_eq!(invoked_requests.len(), 3);
    for req in invoked_requests.iter() {
        let cpm = req.control_plane_metadata.as_ref().unwrap();
        assert_eq!(cpm.request_metadata.get(TRACEPARENT_HEADER).unwrap(), TRACEPARENT_VALUE);
        assert_eq!(cpm.request_metadata.get(TRACESTATE_HEADER).unwrap(), TRACESTATE_VALUE);
        assert_eq!(cpm.request_metadata.get(CUSTOM_HEADER).unwrap(), CUSTOM_HEADER_VALUE);
    }

    let _ = shutdown_tx.send(());
}
