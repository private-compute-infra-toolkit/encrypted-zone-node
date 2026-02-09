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
use container_manager_requester::ContainerManagerRequester;
use data_scope::requester::DataScopeRequester;
use error_detail_proto::enforcer::v1::ez_error_detail::ErrorSource;
use error_detail_proto::enforcer::v1::{ez_error_detail::ErrorReason, EzErrorDetail};
use error_details_proto::google::rpc::BadRequest;
use ez_service_proto::enforcer::v1::{
    ez_public_api_client::EzPublicApiClient, ez_public_api_server::EzPublicApiServer,
};
use ez_service_proto::enforcer::v1::{
    CallParameters, CallRequest, CallResponse, GetHealthReportRequest, SessionMetadata,
};
use googleapis_tonic_google_rpc::google::rpc::Status as RpcStatus;
use health_manager::HealthManager;
use interceptor::Interceptor;
use isolate_info::IsolateServiceInfo;
use isolate_service_mapper::IsolateServiceMapper;
use junction_test_utils::FakeJunction;
use junction_test_utils::UNKNOWN_ISOLATE_DOMAIN;
use manifest_proto::enforcer::InterceptingServices;
use prost::Message;
use public_api::EzPublicApiService;
use state_manager::IsolateStateManager;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Code;

const EZ_PUBLIC_API_RESPONSE_TEST_CHANNEL_SIZE: usize = 10;
const OPAQUE_DOMAIN: &str = "opaque.domain";
const OPAQUE_SERVICE: &str = "OpaqueService";
const RATIFIED_INTERCEPTOR_DOMAIN: &str = "ratified.interceptor.domain";
const RATIFIED_INTERCEPTOR_SERVICE: &str = "RatifiedInterceptorService";
const RATIFIED_INTERCEPTOR_UNARY: &str = "unary";
const RATIFIED_INTERCEPTOR_STREAMING: &str = "streaming";

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

    let _ = shutdown_tx.send(());

    let response_result = response_stream.message().await.unwrap();

    assert_eq!(response_result.unwrap(), second_expected_response);
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
        input_params: Some(CallParameters { public_input: vec![], encrypted_input: vec![] }),
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
    (port, shutdown_tx, fake_junction)
}

async fn setup_interceptor() -> Interceptor {
    let mapper = IsolateServiceMapper::default();
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: OPAQUE_DOMAIN.to_string(),
                service_name: OPAQUE_SERVICE.to_string(),
            }],
            false, // is_ratified
        )
        .await
        .unwrap();
    mapper
        .new_binary_index(
            vec![IsolateServiceInfo {
                operator_domain: RATIFIED_INTERCEPTOR_DOMAIN.to_string(),
                service_name: RATIFIED_INTERCEPTOR_SERVICE.to_string(),
            }],
            true, // is_ratified
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
    interceptor
}
