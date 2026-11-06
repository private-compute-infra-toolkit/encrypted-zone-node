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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, InvokeIsolateRequest, InvokeIsolateResponse,
};
use ez_error::EzError;
use ez_mtls_proto::enforcer::v1::ez_mtls_service_server::{EzMtlsService, EzMtlsServiceServer};
use ez_mtls_proto::enforcer::v1::{
    GetCertificateRequest, GetCertificateResponse, ReportSniRequest, ReportSniResponse,
};
use ez_to_ez_service_proto::enforcer::v1::ez_to_ez_api_server::EzToEzApiServer;
use isolate_info::IsolateId;
use junction_trait::Junction;
use manifest_parser::v1::parse_manifest;
use metrics_test_utils::TestMetrics;
use mtls::mtls::{
    extract_sni_params, sni, EzMtlsManager, EzMtlsManagerConfig, IsolateIdentity, SpiffeUri,
};
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use outbound_ez_to_ez_handler::{OutboundEzToEzHandler, OutboundTlsConfig};
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use prost::Message;
use setup_isolate_client::SetupIsolateClient;
use setup_isolate_proto::enforcer::v2::{FetchTlsCertificateRequest, FetchTlsCertificateResponse};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct MockJunction {
    response: Result<InvokeIsolateResponse, String>,
    last_request: Arc<std::sync::Mutex<Option<InvokeIsolateRequest>>>,
}

impl MockJunction {
    fn new(response: Result<InvokeIsolateResponse, String>) -> Self {
        Self { response, last_request: Arc::new(std::sync::Mutex::new(None)) }
    }
}

#[tonic::async_trait]
impl Junction for MockJunction {
    async fn invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        invoke_isolate_request: InvokeIsolateRequest,
        _is_from_public_api: bool,
        _deadline: Option<Instant>,
    ) -> Result<InvokeIsolateResponse, EzError> {
        *self.last_request.lock().unwrap() = Some(invoke_isolate_request);
        match &self.response {
            Ok(res) => Ok(res.clone()),
            Err(e) => Err(EzError::Status(tonic::Status::internal(e.to_string()))),
        }
    }

    async fn stream_invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        _is_from_public_api: bool,
        _timeout: Option<std::time::Duration>,
    ) -> junction_trait::JunctionChannels {
        unimplemented!()
    }

    async fn connect_isolate(
        &self,
        _isolate_id: IsolateId,
        _isolate_address: String,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }
}

fn create_valid_invoke_response(payload: Vec<u8>) -> InvokeIsolateResponse {
    InvokeIsolateResponse {
        isolate_output: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![payload],
            })),
        }),
        ..Default::default()
    }
}

#[test]
fn test_sni() {
    let sni = sni(
        /*ez_instance_id=*/ "ez-instance-id",
        /*isolate_name=*/ "isolate_name",
        /*publisher_id=*/ "publisher@example.com",
        /*operator_domain=*/ "release@example.com",
        /*trust_domain=*/ "avs.goog.tca",
    );
    assert_eq!(sni, "ez-instance-id--00b5ac486086aa9eae630c1d511eee7c-a.avs.goog.tca");
}

#[test]
fn test_extract_sni_params_v1() {
    let manifest = parse_manifest("enforcer/manifest_parser/test/testdata/test_manifest.json")
        .expect("Failed to parse v1 manifest");
    let snis = extract_sni_params(&manifest);
    assert_eq!(snis.len(), 2);
    assert_eq!(
        snis[0],
        IsolateIdentity::new("ezpkg://playground.example.com", "playground_example")
    );
    assert_eq!(
        snis[1],
        IsolateIdentity::new("ezpkg://playground.example.com", "playground_example")
    );
}

#[derive(Clone)]
struct MockMtlsService {
    certs: Vec<Vec<u8>>,
    reported_snis: std::sync::Arc<tokio::sync::Mutex<Vec<Vec<String>>>>,
}

impl MockMtlsService {
    pub fn new() -> Self {
        Self {
            certs: vec![
                include_bytes!("testdata/leaf.der").to_vec(),
                include_bytes!("testdata/root.der").to_vec(),
            ],
            reported_snis: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    pub fn with_leaf_der(leaf_der: Vec<u8>) -> Self {
        Self {
            certs: vec![leaf_der, include_bytes!("testdata/root.der").to_vec()],
            reported_snis: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }
}

fn generate_leaf_der_with_spiffe_uri(spiffe_uri: &str) -> Vec<u8> {
    let root_cert = boring::x509::X509::from_der(include_bytes!("testdata/root.der")).unwrap();
    let root_key_bytes = std::fs::read("enforcer/ez_to_ez/test/testdata/root.key").unwrap();
    let root_key = boring::pkey::PKey::private_key_from_pem(&root_key_bytes).unwrap();
    let leaf_key_bytes = std::fs::read("enforcer/ez_to_ez/test/testdata/leaf.key").unwrap();
    let leaf_key = boring::pkey::PKey::private_key_from_pem(&leaf_key_bytes).unwrap();

    let mut subject_name = boring::x509::X509NameBuilder::new().unwrap();
    subject_name.append_entry_by_text("CN", "leaf").unwrap();
    let subject_name = subject_name.build();

    let mut builder = boring::x509::X509::builder().unwrap();
    builder.set_version(2).unwrap();
    builder.set_pubkey(&leaf_key).unwrap();
    builder.set_subject_name(&subject_name).unwrap();
    builder.set_issuer_name(root_cert.subject_name()).unwrap();
    let not_before = boring::asn1::Asn1Time::days_from_now(0).unwrap();
    let not_after = boring::asn1::Asn1Time::days_from_now(365).unwrap();
    builder.set_not_before(&not_before).unwrap();
    builder.set_not_after(&not_after).unwrap();
    let san = boring::x509::extension::SubjectAlternativeName::new()
        .uri(spiffe_uri)
        .build(&builder.x509v3_context(Some(&root_cert), None))
        .unwrap();
    builder.append_extension(&san).unwrap();
    builder.sign(&root_key, boring::hash::MessageDigest::sha256()).unwrap();
    builder.build().to_der().unwrap()
}

#[tonic::async_trait]
impl EzMtlsService for MockMtlsService {
    async fn get_certificate(
        &self,
        _request: Request<GetCertificateRequest>,
    ) -> Result<Response<GetCertificateResponse>, Status> {
        Ok(Response::new(GetCertificateResponse {
            certs: self.certs.clone(),
            ..Default::default()
        }))
    }

    async fn report_sni(
        &self,
        request: Request<ReportSniRequest>,
    ) -> Result<Response<ReportSniResponse>, Status> {
        self.reported_snis.lock().await.push(request.into_inner().sni);
        Ok(Response::new(ReportSniResponse::default()))
    }
}

#[tokio::test]
async fn test_connect_ez_mtls() {
    let (tx, rx) = oneshot::channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_addr = format!("http://{}", addr);

    let service = MockMtlsService::new();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let key_path = "enforcer/ez_to_ez/test/testdata/leaf.key".to_string();
    let csr_path = "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string();

    let config = EzMtlsManagerConfig::new_with_proxy(key_path, csr_path, server_addr, None);
    let manager_result = EzMtlsManager::build(config).await;
    assert!(
        manager_result.is_ok(),
        "Failed to connect to mock mTLS service: {:?}",
        manager_result.err()
    );
    let _manager = manager_result.unwrap();

    let _ = tx.send(());
}

#[test]
fn test_parse_spiffe_uri_valid() {
    let uri = "spiffe://avs.tca.fakeca/operator/example/some-job/publisher/release@google.com/workload/encrypted-zone";
    let parsed = SpiffeUri::new(uri).unwrap();
    assert_eq!(parsed.trust_domain, "avs.tca.fakeca");
    assert_eq!(parsed.operator_domain, "example/some-job");
    assert_eq!(parsed.publisher_id, "release@google.com");
    assert_eq!(parsed.workload_name, "encrypted-zone");
}

#[test]
fn test_parse_spiffe_uri_invalid() {
    assert_eq!(
        SpiffeUri::new("http://example.com/operator/op/publisher/pub/workload/wl")
            .unwrap_err()
            .to_string(),
        "Missing 'spiffe://' scheme"
    );
    assert_eq!(
        SpiffeUri::new("spiffe://avs.tca.fakeca/publisher/pub/workload/wl")
            .unwrap_err()
            .to_string(),
        "Missing '/operator/' tag"
    );
    assert_eq!(
        SpiffeUri::new("spiffe://avs.tca.fakeca/operator/op/workload/wl").unwrap_err().to_string(),
        "Missing '/publisher/' tag"
    );
    assert_eq!(
        SpiffeUri::new("spiffe://avs.tca.fakeca/publisher/pub/operator/op/workload/wl")
            .unwrap_err()
            .to_string(),
        "Tags are out of correct sequential order"
    );
}

/// Tests the end-to-end mTLS handshake between a simulated server and client.
/// It spawns a mock `EzMtlsService` to obtain test certificates, initializes
/// `EzMtlsManager`s for both ends, and configures an `SslAcceptor` and `SslConnector`
/// bound with an `EzToEzPolicy`. Two local Tokio tasks are spawned to negotiate
/// the TLS connection and verify the SPIFFE URI identity handling in `boring` SSL.
#[tokio::test]
async fn test_tls_acceptor_connector() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_addr = format!("http://{}", addr);

    let service = MockMtlsService::new();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                ez_mtls_proto::enforcer::v1::ez_mtls_service_server::EzMtlsServiceServer::new(
                    service,
                ),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let key_path = "enforcer/ez_to_ez/test/testdata/leaf.key".to_string();
    let csr_path = "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string();
    let config = EzMtlsManagerConfig::new_with_proxy(key_path, csr_path, server_addr, None);
    let server_manager =
        EzMtlsManager::build(config.clone()).await.expect("Failed to initialize server manager");
    let client_manager =
        EzMtlsManager::build(config).await.expect("Failed to initialize client manager");

    let acceptor =
        server_manager.create_tls_acceptor().await.expect("Failed to create TLS acceptor");

    let connector = client_manager
        .get_connector_factory()
        .create_connector()
        .await
        .expect("Failed to create TLS connector");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let tls_stream =
            tokio_boring::accept(&acceptor, stream).await.expect("Server failed to accept TLS");
        assert_eq!(tls_stream.ssl().selected_alpn_protocol(), Some(&b"h2"[..]));
    });

    let client_task = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        // Domain verification is disabled by configuration (replaced with SPIFFE policy matching)
        let tls_stream = tokio_boring::connect(
            connector.configure().unwrap().verify_hostname(false),
            "localhost",
            stream,
        )
        .await
        .expect("Client failed to connect TLS");
        assert_eq!(tls_stream.ssl().selected_alpn_protocol(), Some(&b"h2"[..]));
    });

    let (server_result, client_result) = tokio::join!(server_task, client_task);
    server_result.expect("Server task panicked");
    client_result.expect("Client task panicked");
}

#[tokio::test]
async fn test_boring_tls_stream_duplex() {
    let key_path = "enforcer/ez_to_ez/test/testdata/leaf.key".to_string();
    let csr_path = "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string();

    // Create a mock EzMtlsService server to provide certificates.
    let (tx, rx) = tokio::sync::oneshot::channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_addr = format!("http://{}", addr);

    let service = MockMtlsService::new();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let config = EzMtlsManagerConfig::new_with_proxy(key_path, csr_path, server_addr, None);
    // Create a mTLS manager to fetch the certificates from the mock server and load certificates from testdata.
    let server_manager =
        EzMtlsManager::build(config.clone()).await.expect("Failed to initialize server manager");

    // Create a client side mTLS manager.
    let client_manager =
        EzMtlsManager::build(config).await.expect("Failed to initialize client manager");

    let acceptor =
        server_manager.create_tls_acceptor().await.expect("Failed to create TLS acceptor");

    let connector = client_manager
        .get_connector_factory()
        .create_connector()
        .await
        .expect("Failed to create TLS connector");

    // Use duplex stream to test SSL behavior on byte stream.
    let (client_stream, server_stream) = tokio::io::duplex(1024);

    // Block on accept() until a TLS connection is established.
    let server_task = tokio::spawn(async move {
        // Accepted TLS stream is tokio_boring::TlsStream<tokio::io::DuplexStream>.
        let tls_stream = tokio_boring::accept(&acceptor, server_stream)
            .await
            .expect("Server failed to accept TLS");
        let mut wrapped_stream = mtls::mtls::BoringTlsStream::new(tls_stream);

        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 11];
        wrapped_stream.read_exact(&mut buf).await.expect("Failed to read from stream");
        assert_eq!(&buf[..], b"hello world");
    });

    let client_task = tokio::spawn(async move {
        // Disable hostname check because we ignore peer DNS name in certificate.
        // Connecting with the TLS connector will encrypt the traffic.
        let tls_stream = tokio_boring::connect(
            connector.configure().unwrap().verify_hostname(false),
            "dummy-sni-value",
            client_stream,
        )
        .await
        .expect("Client failed to connect TLS");

        use tokio::io::AsyncWriteExt;
        let mut tls_stream = tls_stream; // make mutable for write_all
        tls_stream.write_all(b"hello world").await.expect("Failed to write to stream");
        tls_stream.flush().await.expect("Failed to flush");
    });

    let (server_result, client_result) = tokio::join!(server_task, client_task);
    server_result.expect("Server task panicked");
    client_result.expect("Client task panicked");
    let _ = tx.send(()); // Stop mock service
}

struct TestContext {
    /// Keeps the temporary directory (and the UDS socket within it) alive.
    _dir: tempfile::TempDir,
    /// Sender to signal shutdown to the mock EzMtlsService.
    mock_service_tx: tokio::sync::oneshot::Sender<()>,
    /// Handle to the background task running the inbound server.
    server_task: tokio::task::JoinHandle<()>,
    /// Manager for the client's mTLS identities.
    client_manager: EzMtlsManager,
    /// TLS acceptor configured for the test server identity.
    acceptor: boring::ssl::SslAcceptor,
    /// Server socket address.
    server_address: String,
    /// Outbound TLS configuration.
    outbound_tls_config: OutboundTlsConfig,
    /// The outbound handler instance used to make calls.
    outbound_handler: OutboundEzToEzHandler<TestMetrics>,
}

/// Creates and initializes a complete test context, including the inbound server, mock service,
/// and outbound handler, all configured for mTLS communication.
async fn create_test_context() -> TestContext {
    let key_path = "enforcer/ez_to_ez/test/testdata/leaf.key".to_string();
    let csr_path = "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string();

    let (tx, rx) = tokio::sync::oneshot::channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_addr = format!("http://{}", addr);

    let service = MockMtlsService::new();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let config = EzMtlsManagerConfig::new_with_proxy(key_path, csr_path, server_addr, None);

    let server_manager = EzMtlsManager::build(config.clone()).await.unwrap();
    let client_manager = EzMtlsManager::build(config).await.unwrap();

    let acceptor = server_manager.create_tls_acceptor().await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let server_address = format!("unix:{}", socket_path.to_str().unwrap());

    let fake_junction = junction_test_utils::FakeJunction::default();
    let handler =
        inbound_ez_to_ez_handler::InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let tls_config = inbound_ez_to_ez_handler::InboundTlsConfig {
        acceptor: acceptor.clone(),
        handshake_timeout: std::time::Duration::from_secs(2),
        max_concurrent_handshakes: 5,
    };

    let server_addr_clone = server_address.clone();
    let server_task = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_addr_clone,
            4 * 1024 * 1024,
            Some(tls_config),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let outbound_tls_config = OutboundTlsConfig {
        factory: client_manager.get_connector_factory(),
        trust_domain: client_manager.self_identity().trust_domain.clone(),
    };

    let outbound_handler = OutboundEzToEzHandler::new(
        server_address.clone(),
        TestMetrics::default(),
        Some(outbound_tls_config.clone()),
        /* expect_tls_config= */ false,
        /* enable_per_unary_tls_channel= */ true,
        4 * 1024 * 1024,
    )
    .await
    .unwrap();

    TestContext {
        _dir: dir,
        mock_service_tx: tx,
        server_task,
        client_manager,
        acceptor,
        server_address,
        outbound_tls_config,
        outbound_handler,
    }
}

/// Tests the full e2e flow between OutboundEzToEzHandler and InboundEzToEzHandler over mTLS.
/// It verifies that:
/// 1. The outbound handler correctly creates a TLS connection using the TlsConnectorFactory.
/// 2. The inbound server correctly accepts the TLS connection using InboundTlsConfig.
/// 3. A gRPC call can be made over the established mTLS connection.
///
/// This test will fail if:
/// 1. The TLS handshake fails (e.g. certs don't match, trust domain mismatch).
/// 2. The gRPC call fails (e.g. protocol error, timeout).
/// 3. The response payload does not match the request payload.
#[tokio::test]
async fn test_e2e_mtls() {
    let ctx = create_test_context().await;

    // Verify OutboundTlsConfig Debug format.
    assert!(format!("{:?}", ctx.outbound_tls_config).contains("OutboundTlsConfig"));

    // Missing metadata when TLS is active returns an error.
    let req_no_meta = enforcer_proto::enforcer::v1::InvokeEzRequest {
        control_plane_metadata: None,
        ..Default::default()
    };
    let no_meta_err = ctx.outbound_handler.remote_invoke(req_no_meta, None).await.unwrap_err();
    assert!(no_meta_err.to_string().contains("Missing ControlPlaneMetadata for TLS routing"));

    let meta = ControlPlaneMetadata {
        destination_operator_domain: ctx.client_manager.self_identity().operator_domain.clone(),
        ..Default::default()
    };

    let request = enforcer_proto::enforcer::v1::InvokeEzRequest {
        control_plane_metadata: Some(meta.clone()),
        isolate_request_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![b"hello world".to_vec()],
            })),
        }),
        ..Default::default()
    };

    let response = ctx.outbound_handler.remote_invoke(request.clone(), None).await;

    assert!(response.is_ok(), "Remote invoke failed: {:?}", response.err());
    let response = response.unwrap();
    let output_payload = match response.ez_response_payload.unwrap().delivery_method.unwrap() {
        DeliveryMethod::InlineData(inline) => inline.datagrams[0].clone(),
        _ => panic!("Expected InlineData"),
    };
    assert_eq!(output_payload, b"hello world");

    // Subsequent unary invoke also succeeds over a fresh TLS channel.
    let response2 = ctx.outbound_handler.remote_invoke(request, None).await;
    assert!(response2.is_ok());

    // Payload with None delivery method.
    let req_none_delivery = enforcer_proto::enforcer::v1::InvokeEzRequest {
        control_plane_metadata: Some(meta.clone()),
        isolate_request_payload: Some(EzHybridPayload { delivery_method: None }),
        ..Default::default()
    };
    let response_none = ctx.outbound_handler.remote_invoke(req_none_delivery, None).await;
    assert!(response_none.is_ok());

    let _ = ctx.mock_service_tx.send(()); // Stop mock service
    ctx.server_task.abort(); // Stop server task
}

/// Tests the streaming invoke flow between OutboundEzToEzHandler and InboundEzToEzHandler over mTLS.
#[tokio::test]
async fn test_e2e_mtls_streaming() {
    let ctx = create_test_context().await;

    let meta = ControlPlaneMetadata {
        destination_operator_domain: ctx.client_manager.self_identity().operator_domain.clone(),
        ..Default::default()
    };

    let (req_tx, req_rx) = tokio::sync::mpsc::channel(10);
    let mut res_rx =
        ctx.outbound_handler.remote_streaming_connect(Some(&meta), req_rx, None).await.unwrap();

    let stream_request = enforcer_proto::enforcer::v1::InvokeEzRequest {
        control_plane_metadata: Some(meta),
        isolate_request_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![b"hello streaming".to_vec()],
            })),
        }),
        ..Default::default()
    };

    req_tx.send(stream_request).await.unwrap();
    let response = res_rx.recv().await.unwrap().unwrap();
    let output_payload = match response.ez_response_payload.unwrap().delivery_method.unwrap() {
        DeliveryMethod::InlineData(inline) => inline.datagrams[0].clone(),
        _ => panic!("Expected InlineData"),
    };
    assert_eq!(output_payload, b"hello streaming");

    let _ = ctx.mock_service_tx.send(()); // Stop mock service
    ctx.server_task.abort(); // Stop server task
}

/// Tests that the outbound handler fails to connect when the peer identity does not match
/// `self_identity` or `allowed_peer_identities`.
/// This verifies that SPIFFE identity verification is working.
#[tokio::test]
async fn test_e2e_mtls_wrong_identity() {
    // Set retry count to 1 to fail fast in test
    std::env::set_var("PROXY_CONNECT_RETRY_COUNT", "1");

    let key_path = "enforcer/ez_to_ez/test/testdata/leaf.key".to_string();
    let csr_path = "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string();

    // Server uses a certificate with a different SPIFFE URI than the client's self_identity.
    let (server_tx, server_rx) = tokio::sync::oneshot::channel();
    let server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_mtls_addr = format!("http://{}", server_listener.local_addr().unwrap());
    let wrong_leaf_der = generate_leaf_der_with_spiffe_uri(
        "spiffe://avs.tca.fakeca/operator/wrong_domain/publisher/release@google.com/workload/encrypted-zone",
    );
    let server_service = MockMtlsService::with_leaf_der(wrong_leaf_der);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(server_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(server_listener), async {
                server_rx.await.ok();
            })
            .await
            .unwrap();
    });

    let (client_tx, client_rx) = tokio::sync::oneshot::channel();
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_mtls_addr = format!("http://{}", client_listener.local_addr().unwrap());
    let client_service = MockMtlsService::new();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(client_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(client_listener), async {
                client_rx.await.ok();
            })
            .await
            .unwrap();
    });

    let server_config = EzMtlsManagerConfig::new_with_proxy(
        key_path.clone(),
        csr_path.clone(),
        server_mtls_addr,
        None,
    );
    let client_config = EzMtlsManagerConfig::new_with_proxy(
        key_path,
        csr_path,
        client_mtls_addr,
        Some(vec![
            "spiffe://avs.tca.fakeca/operator/allowed_domain/publisher/release@google.com/workload/encrypted-zone"
                .to_string(),
        ]),
    );

    let server_manager = EzMtlsManager::build(server_config).await.unwrap();
    let client_manager = EzMtlsManager::build(client_config).await.unwrap();

    let acceptor = server_manager.create_tls_acceptor().await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let server_address = format!("unix:{}", socket_path.to_str().unwrap());

    let fake_junction = junction_test_utils::FakeJunction::default();
    let handler =
        inbound_ez_to_ez_handler::InboundEzToEzHandler::new(Box::new(fake_junction.clone()));

    let tls_config = inbound_ez_to_ez_handler::InboundTlsConfig {
        acceptor,
        handshake_timeout: std::time::Duration::from_secs(2),
        max_concurrent_handshakes: 5,
    };

    let server_addr_clone = server_address.clone();
    let server_task = tokio::spawn(async move {
        inbound_ez_to_ez_handler::launch_server(
            handler,
            &server_addr_clone,
            4 * 1024 * 1024,
            Some(tls_config),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let outbound_tls_config = OutboundTlsConfig {
        factory: client_manager.get_connector_factory(),
        trust_domain: client_manager.self_identity().trust_domain.clone(),
    };

    let outbound_handler = OutboundEzToEzHandler::new(
        server_address,
        TestMetrics::default(),
        Some(outbound_tls_config),
        /* expect_tls_config= */ false,
        /* enable_per_unary_tls_channel= */ true,
        4 * 1024 * 1024,
    )
    .await
    .unwrap();

    let meta = ControlPlaneMetadata {
        destination_operator_domain: "wrong_domain".to_string(),
        ..Default::default()
    };

    let request = enforcer_proto::enforcer::v1::InvokeEzRequest {
        control_plane_metadata: Some(meta),
        isolate_request_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![b"hello world".to_vec()],
            })),
        }),
        ..Default::default()
    };

    let response = outbound_handler.remote_invoke(request, None).await;

    assert!(response.is_err(), "Expected remote invoke to fail due to identity mismatch");
    let err = response.unwrap_err();
    assert!(
        format!("{:?}", err).contains("CERTIFICATE_VERIFY_FAILED"),
        "Expected error to contain 'CERTIFICATE_VERIFY_FAILED', got: {:?}",
        err
    );

    let _ = server_tx.send(());
    let _ = client_tx.send(());
    server_task.abort();
}

/// Tests that the inbound handler correctly propagates errors from the junction back to the client.
/// This verifies that error handling is working across the mTLS boundary.
#[tokio::test]
async fn test_e2e_mtls_junction_error() {
    let ctx = create_test_context().await;

    let meta = ControlPlaneMetadata {
        destination_operator_domain: ctx.client_manager.self_identity().operator_domain.clone(),
        // "ErrorService" triggers FakeJunction to return an error, as defined in junction/test_utils.rs
        destination_service_name: "ErrorService".to_string(),
        ..Default::default()
    };

    let request = enforcer_proto::enforcer::v1::InvokeEzRequest {
        control_plane_metadata: Some(meta),
        isolate_request_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![b"hello world".to_vec()],
            })),
        }),
        ..Default::default()
    };

    let response = ctx.outbound_handler.remote_invoke(request, None).await;

    assert!(response.is_err(), "Expected remote invoke to fail due to junction error");
    let err = response.err().unwrap();
    let status = err.downcast_ref::<tonic::Status>().expect("Expected tonic::Status");
    assert_eq!(status.message(), "TEST_ERROR");

    let _ = ctx.mock_service_tx.send(()); // Stop mock service
    ctx.server_task.abort(); // Stop server task
}

/// Tests that an outbound handler initialized in pending mode (expect_tls_config=true, tls_config=None)
/// fails precondition, and once set_tls_config is called, calls succeed over mTLS.
#[tokio::test]
async fn test_outbound_pending_then_set_tls_config_works() {
    let ctx = create_test_context().await;

    let handler = OutboundEzToEzHandler::new(
        ctx.server_address.clone(),
        TestMetrics::default(),
        /* tls_config= */ None,
        /* expect_tls_config= */ true,
        /* enable_per_unary_tls_channel= */ true,
        4 * 1024 * 1024,
    )
    .await
    .unwrap();

    let meta = ControlPlaneMetadata {
        destination_operator_domain: ctx.client_manager.self_identity().operator_domain.clone(),
        ..Default::default()
    };
    let request = enforcer_proto::enforcer::v1::InvokeEzRequest {
        control_plane_metadata: Some(meta.clone()),
        isolate_request_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![b"hello dynamic mtls".to_vec()],
            })),
        }),
        ..Default::default()
    };

    // 1. Invoking before setting TLS config must fail with FailedPrecondition.
    let err = handler.remote_invoke(request.clone(), None).await.unwrap_err();
    let status = err.downcast_ref::<tonic::Status>().expect("Expected tonic::Status");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);

    // 2. Set the TLS config via the OutboundEzToEzClient trait.
    let duplicate_config = ctx.outbound_tls_config.clone();
    handler.set_tls_config(ctx.outbound_tls_config).expect("Failed to set TLS config");

    // 3. Now the outbound handler works and successfully sends the request over mTLS.
    let response = handler
        .remote_invoke(request, None)
        .await
        .expect("Remote invoke should succeed after TLS config is set");
    let output_payload = match response.ez_response_payload.unwrap().delivery_method.unwrap() {
        DeliveryMethod::InlineData(inline) => inline.datagrams[0].clone(),
        _ => panic!("Expected InlineData"),
    };
    assert_eq!(output_payload, b"hello dynamic mtls");

    // 4. Calling set_tls_config a second time must return an error (no panic).
    let err = handler.set_tls_config(duplicate_config).unwrap_err();
    assert!(err.to_string().contains("set_tls_config must be called only once"));

    let _ = ctx.mock_service_tx.send(());
    ctx.server_task.abort();
}

#[test]
fn test_generate_key_and_csr() {
    let (pkey, csr_der) =
        EzMtlsManager::generate_key_and_csr().expect("Failed to generate key and CSR");

    let ec_key = pkey.ec_key().expect("Generated private key should be an EC key");
    assert_eq!(
        ec_key.group().curve_name(),
        Some(boring::nid::Nid::X9_62_PRIME256V1),
        "Generated key must be on the P-256 curve"
    );

    let csr =
        boring::x509::X509Req::from_der(&csr_der).expect("CSR bytes should be valid DER X509Req");
    let csr_pubkey = csr.public_key().expect("CSR should contain a public key");
    assert!(csr_pubkey.public_eq(&pkey), "CSR public key must match the generated private key");
    assert!(
        csr.verify(&pkey).expect("CSR signature verification should not error"),
        "CSR signature must be valid for the generated key"
    );

    let (pkey2, _) =
        EzMtlsManager::generate_key_and_csr().expect("Failed to generate second key and CSR");
    assert!(
        !pkey.public_eq(&pkey2),
        "Subsequent calls to generate_key_and_csr must produce distinct key pairs"
    );
}

#[tokio::test]
async fn test_ez_mtls_manager_build_with_setup_isolate() {
    let expected_res = FetchTlsCertificateResponse {
        certificate_chain: vec![include_bytes!("testdata/leaf.der").to_vec()],
        trust_anchors: vec![include_bytes!("testdata/root.der").to_vec()],
    };
    let junction =
        MockJunction::new(Ok(create_valid_invoke_response(expected_res.encode_to_vec())));
    let last_request = junction.last_request.clone();
    let setup_client = SetupIsolateClient::new(
        Box::new(junction),
        "test-publisher".to_string(),
        "setup-isolate".to_string(),
        "SetupService".to_string(),
    );

    let config = EzMtlsManagerConfig::new_with_setup_isolate(setup_client, None);

    let manager = EzMtlsManager::build(config).await.expect("Failed to build EzMtlsManager");
    assert_eq!(manager.self_identity().trust_domain, "avs.tca.fakeca");

    let invoked_req = last_request.lock().unwrap().clone().expect("No request invoked");
    let cpm = invoked_req.control_plane_metadata.expect("Missing metadata");
    assert_eq!(cpm.destination_service_name, "SetupService");
    assert_eq!(cpm.destination_method_name, "FetchMtlsCertificate");

    // Verify the CSR generated in memory was passed in the request datagram.
    let payload = invoked_req.isolate_input.unwrap().delivery_method.unwrap();
    let req_bytes = match payload {
        DeliveryMethod::InlineData(data) => data.datagrams[0].clone(),
        _ => panic!("Expected InlineData delivery method"),
    };
    let parsed_req = FetchTlsCertificateRequest::decode(&*req_bytes).unwrap();
    assert!(!parsed_req.signed_certificate_signing_request.is_empty());
}

#[tokio::test]
async fn test_ez_mtls_manager_build_missing_key_file() {
    let config = EzMtlsManagerConfig::new_with_proxy(
        "nonexistent/leaf.key".to_string(),
        "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string(),
        "http://127.0.0.1:1".to_string(),
        None,
    );
    let err = EzMtlsManager::build(config).await.unwrap_err();
    assert!(err.to_string().contains("Failed to read leaf private key from path"));
}

#[tokio::test]
async fn test_ez_mtls_manager_build_missing_csr_file() {
    let config = EzMtlsManagerConfig::new_with_proxy(
        "enforcer/ez_to_ez/test/testdata/leaf.key".to_string(),
        "nonexistent/leaf.csr".to_string(),
        "http://127.0.0.1:1".to_string(),
        None,
    );
    let err = EzMtlsManager::build(config).await.unwrap_err();
    assert!(err.to_string().contains("Failed to read leaf CSR from path"));
}

#[tokio::test]
async fn test_ez_mtls_manager_build_setup_isolate_rpc_failure() {
    let junction = MockJunction::new(Err("RPC transport error".to_string()));
    let setup_client = SetupIsolateClient::new(
        Box::new(junction),
        "test-publisher".to_string(),
        "setup-isolate".to_string(),
        "SetupService".to_string(),
    );

    let config = EzMtlsManagerConfig::new_with_setup_isolate(setup_client, None);
    let err = EzMtlsManager::build(config).await.unwrap_err();
    assert!(err.to_string().contains("Failed to fetch certificate from Setup Isolate"));
}

#[tokio::test]
async fn test_ez_mtls_manager_build_with_setup_isolate_empty_trust_anchors_err() {
    let expected_res = FetchTlsCertificateResponse {
        certificate_chain: vec![include_bytes!("testdata/leaf.der").to_vec()],
        trust_anchors: vec![],
    };
    let junction =
        MockJunction::new(Ok(create_valid_invoke_response(expected_res.encode_to_vec())));
    let setup_client = SetupIsolateClient::new(
        Box::new(junction),
        "test-publisher".to_string(),
        "setup-isolate".to_string(),
        "SetupService".to_string(),
    );

    let config = EzMtlsManagerConfig::new_with_setup_isolate(setup_client, None);
    let err = EzMtlsManager::build(config).await.unwrap_err();
    assert!(err
        .to_string()
        .contains("Trust anchors returned from Setup Isolate must not be empty"));
}

#[tokio::test]
async fn test_ez_mtls_manager_build_missing_config_fields() {
    let config = EzMtlsManagerConfig::default();
    let err = EzMtlsManager::build(config).await.unwrap_err();
    assert!(err
        .to_string()
        .contains("mtls_key_path must be provided when setup_isolate_client is not set"));
}

#[tokio::test]
async fn test_ez_mtls_manager_build_invalid_allowed_peer_identity() {
    let config = EzMtlsManagerConfig::new_with_proxy(
        "enforcer/ez_to_ez/test/testdata/leaf.key".to_string(),
        "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string(),
        "http://127.0.0.1:1".to_string(),
        Some(vec!["invalid-spiffe-id".to_string()]),
    );
    let err = EzMtlsManager::build(config).await.unwrap_err();
    assert!(err.to_string().contains("Failed to parse allowed peer SPIFFE ID"));
}

#[tokio::test]
async fn test_tls_handshake_succeeds_with_allowed_peer_identity() {
    let server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = format!("http://{}", server_listener.local_addr().unwrap());

    let server_service = MockMtlsService::new();
    let (server_tx, server_rx) = oneshot::channel();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(server_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(server_listener), async {
                server_rx.await.ok();
            })
            .await
            .unwrap();
    });

    let client_spiffe_id =
        "spiffe://avs.tca.fakeca/operator/other-op/publisher/other-pub/workload/other-wl";
    let client_leaf_der = generate_leaf_der_with_spiffe_uri(client_spiffe_id);
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = format!("http://{}", client_listener.local_addr().unwrap());
    let client_service = MockMtlsService::with_leaf_der(client_leaf_der);
    let (client_tx, client_rx) = oneshot::channel();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(client_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(client_listener), async {
                client_rx.await.ok();
            })
            .await
            .unwrap();
    });

    let key_path = "enforcer/ez_to_ez/test/testdata/leaf.key".to_string();
    let csr_path = "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string();

    let server_config = EzMtlsManagerConfig::new_with_proxy(
        key_path.clone(),
        csr_path.clone(),
        server_addr,
        Some(vec![client_spiffe_id.to_string()]),
    );
    let server_manager = EzMtlsManager::build(server_config).await.unwrap();
    let server_spiffe = server_manager.self_identity();
    let server_spiffe_id = format!(
        "spiffe://{}/operator/{}/publisher/{}/workload/{}",
        server_spiffe.trust_domain,
        server_spiffe.operator_domain,
        server_spiffe.publisher_id,
        server_spiffe.workload_name
    );

    let client_config = EzMtlsManagerConfig::new_with_proxy(
        key_path,
        csr_path,
        client_addr,
        Some(vec![server_spiffe_id]),
    );
    let client_manager = EzMtlsManager::build(client_config).await.unwrap();

    let acceptor = server_manager.create_tls_acceptor().await.unwrap();
    let connector = client_manager.get_connector_factory().create_connector().await.unwrap();

    let (client_stream, server_stream) = tokio::io::duplex(1024);

    let server_task = tokio::spawn(async move {
        tokio_boring::accept(&acceptor, server_stream)
            .await
            .expect("Server should accept peer via allowed_peer_identities");
    });

    let client_task = tokio::spawn(async move {
        tokio_boring::connect(
            connector.configure().unwrap().verify_hostname(false),
            "localhost",
            client_stream,
        )
        .await
        .expect("Client should accept peer via allowed_peer_identities");
    });

    let (server_res, client_res) = tokio::join!(server_task, client_task);
    server_res.unwrap();
    client_res.unwrap();
    let _ = server_tx.send(());
    let _ = client_tx.send(());
}

#[tokio::test]
async fn test_tls_handshake_fails_open_when_allowed_peer_identities_empty() {
    let server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = format!("http://{}", server_listener.local_addr().unwrap());

    let server_service = MockMtlsService::new();
    let (server_tx, server_rx) = oneshot::channel();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(server_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(server_listener), async {
                server_rx.await.ok();
            })
            .await
            .unwrap();
    });

    let client_spiffe_id =
        "spiffe://avs.tca.fakeca/operator/other-op/publisher/other-pub/workload/other-wl";
    let client_leaf_der = generate_leaf_der_with_spiffe_uri(client_spiffe_id);
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = format!("http://{}", client_listener.local_addr().unwrap());
    let client_service = MockMtlsService::with_leaf_der(client_leaf_der);
    let (client_tx, client_rx) = oneshot::channel();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(client_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(client_listener), async {
                client_rx.await.ok();
            })
            .await
            .unwrap();
    });

    let key_path = "enforcer/ez_to_ez/test/testdata/leaf.key".to_string();
    let csr_path = "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string();

    let server_config =
        EzMtlsManagerConfig::new_with_proxy(key_path.clone(), csr_path.clone(), server_addr, None);
    let server_manager = EzMtlsManager::build(server_config).await.unwrap();

    let client_config = EzMtlsManagerConfig::new_with_proxy(key_path, csr_path, client_addr, None);
    let client_manager = EzMtlsManager::build(client_config).await.unwrap();

    let acceptor = server_manager.create_tls_acceptor().await.unwrap();
    let connector = client_manager.get_connector_factory().create_connector().await.unwrap();

    let (client_stream, server_stream) = tokio::io::duplex(1024);

    let server_task = tokio::spawn(async move {
        tokio_boring::accept(&acceptor, server_stream)
            .await
            .expect("Server should fail open when allowed_peer_identities is empty");
    });

    let client_task = tokio::spawn(async move {
        tokio_boring::connect(
            connector.configure().unwrap().verify_hostname(false),
            "localhost",
            client_stream,
        )
        .await
        .expect("Client should fail open when allowed_peer_identities is empty");
    });

    let (server_res, client_res) = tokio::join!(server_task, client_task);
    server_res.unwrap();
    client_res.unwrap();
    let _ = server_tx.send(());
    let _ = client_tx.send(());
}

#[tokio::test]
async fn test_outbound_per_unary_tls_channel_enabled_vs_disabled() {
    let ctx = create_test_context().await;
    let counting_sock = ctx._dir.path().join("counting.sock");
    let (addr, tls_handshakes, counting_task) =
        start_counting_tls_uds_server(&counting_sock, ctx.acceptor.clone());
    let meta = ControlPlaneMetadata {
        destination_operator_domain: ctx.client_manager.self_identity().operator_domain.clone(),
        ..Default::default()
    };
    let make_req = |msg: &str| enforcer_proto::enforcer::v1::InvokeEzRequest {
        control_plane_metadata: Some(meta.clone()),
        isolate_request_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![msg.as_bytes().to_vec()],
            })),
        }),
        ..Default::default()
    };

    // 1. Default (enabled): each unary RPC opens 1 fresh TLS channel; streams share the pool.
    let per_unary = OutboundEzToEzHandler::new(
        addr.clone(),
        TestMetrics::default(),
        Some(ctx.outbound_tls_config.clone()),
        false,
        /* enable_per_unary_tls_channel= */ true,
        4 * 1024 * 1024,
    )
    .await
    .unwrap();
    let wait = Duration::from_secs(2);
    assert_eq!(tls_handshakes.load(Ordering::SeqCst), 0);
    for expected in 1..=3 {
        let msg = format!("unary-{expected}");
        let res = tokio::time::timeout(wait, per_unary.remote_invoke(make_req(&msg), None))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(extract_first_datagram(res), msg.as_bytes());
        assert_eq!(tls_handshakes.load(Ordering::SeqCst), expected);
    }
    for idx in 1..=2 {
        let (req_tx, req_rx) = tokio::sync::mpsc::channel(4);
        let mut res_rx =
            per_unary.remote_streaming_connect(Some(&meta), req_rx, None).await.unwrap();
        let msg = format!("stream-{idx}");
        req_tx.send(make_req(&msg)).await.unwrap();
        let res = tokio::time::timeout(wait, res_rx.recv()).await.unwrap().unwrap().unwrap();
        assert_eq!(extract_first_datagram(res), msg.as_bytes());
        assert_eq!(tls_handshakes.load(Ordering::SeqCst), 3 + grpc_connector::DEFAULT_POOL_SIZE);
    }

    // 2. Disabled: unary RPCs initialize and reuse the channel pool.
    tls_handshakes.store(0, Ordering::SeqCst);
    let pooled = OutboundEzToEzHandler::new(
        addr,
        TestMetrics::default(),
        Some(ctx.outbound_tls_config),
        false,
        /* enable_per_unary_tls_channel= */ false,
        4 * 1024 * 1024,
    )
    .await
    .unwrap();
    for idx in 1..=3 {
        let msg = format!("pooled-{idx}");
        let res = tokio::time::timeout(wait, pooled.remote_invoke(make_req(&msg), None))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(extract_first_datagram(res), msg.as_bytes());
        assert_eq!(tls_handshakes.load(Ordering::SeqCst), grpc_connector::DEFAULT_POOL_SIZE);
    }

    counting_task.abort();
    let _ = ctx.mock_service_tx.send(());
    ctx.server_task.abort();
}

#[tokio::test]
async fn test_tls_acceptor_connector_deep_cert_chain() {
    let mut certs: Vec<Vec<u8>> =
        boring::x509::X509::stack_from_pem(DEEP_CERT_CHAIN_PEM.as_bytes())
            .expect("Failed to parse DEEP_CERT_CHAIN_PEM")
            .into_iter()
            .map(|c| c.to_der().expect("Failed to convert certificate to DER"))
            .collect();
    certs.push(include_bytes!("testdata/root.der").to_vec());

    let (tx, rx) = oneshot::channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = format!("http://{}", listener.local_addr().unwrap());
    let service = MockMtlsService { certs, ..MockMtlsService::new() };
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzMtlsServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let config = EzMtlsManagerConfig::new_with_proxy(
        "enforcer/ez_to_ez/test/testdata/leaf.key".to_string(),
        "enforcer/ez_to_ez/test/testdata/leaf.csr".to_string(),
        server_addr,
        None,
    );
    let manager = EzMtlsManager::build(config).await.expect("Failed to build EzMtlsManager");
    let acceptor = manager.create_tls_acceptor().await.expect("Failed to create TLS acceptor");
    let connector = manager
        .get_connector_factory()
        .create_connector()
        .await
        .expect("Failed to create TLS connector");

    let (client_stream, server_stream) = tokio::io::duplex(16 * 1024);
    let (server_res, client_res) = tokio::join!(
        tokio_boring::accept(&acceptor, server_stream),
        tokio_boring::connect(
            connector.configure().unwrap().verify_hostname(false),
            "localhost",
            client_stream,
        ),
    );
    let server_tls = server_res.expect("Server failed to accept TLS");
    let client_tls = client_res.expect("Client failed to connect TLS");
    assert_eq!(server_tls.ssl().selected_alpn_protocol(), Some(&b"h2"[..]));
    assert_eq!(client_tls.ssl().selected_alpn_protocol(), Some(&b"h2"[..]));
    let _ = tx.send(());
}

fn extract_first_datagram(res: enforcer_proto::enforcer::v1::InvokeEzResponse) -> Vec<u8> {
    match res.ez_response_payload.unwrap().delivery_method.unwrap() {
        DeliveryMethod::InlineData(inline) => inline.datagrams[0].clone(),
        _ => panic!("Expected InlineData"),
    }
}

fn start_counting_tls_uds_server(
    socket_path: &std::path::Path,
    acceptor: boring::ssl::SslAcceptor,
) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let addr = format!("unix:{}", socket_path.to_str().unwrap());
    let uds = tokio::net::UnixListener::bind(socket_path).unwrap();
    let acceptor = Arc::new(acceptor);
    let tls_handshakes = Arc::new(AtomicUsize::new(0));
    let handshakes = tls_handshakes.clone();
    let incoming = UnixListenerStream::new(uds)
        .then(move |conn| {
            let (acceptor, handshakes) = (acceptor.clone(), handshakes.clone());
            async move {
                let tls = tokio::time::timeout(
                    Duration::from_secs(1),
                    tokio_boring::accept(&acceptor, conn.ok()?),
                )
                .await
                .ok()?
                .ok()?;
                handshakes.fetch_add(1, Ordering::SeqCst);
                Some(Ok::<_, std::io::Error>(mtls::mtls::BoringTlsStream::new(tls)))
            }
        })
        .filter_map(|x| x);
    let junction = Box::new(junction_test_utils::FakeJunction::default());
    let handler = inbound_ez_to_ez_handler::InboundEzToEzHandler::new(junction);
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EzToEzApiServer::new(handler))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    (addr, tls_handshakes, task)
}

// Leaf (testdata/leaf.csr) + 3 intermediate CAs (int-3 -> int-2 -> int-1) signed by testdata/root.key.
const DEEP_CERT_CHAIN_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIICzjCCAnOgAwIBAgIBZDAKBggqhkjOPQQDAjAQMQ4wDAYDVQQDDAVpbnQtMzAe
Fw0yNTAxMDEwMDAwMDBaFw0zNjAxMDEwMDAwMDBaMFYxCzAJBgNVBAYTAlVTMQsw
CQYDVQQIDAJXQTEQMA4GA1UEBwwHU2VhdHRsZTEPMA0GA1UECgwGR29vZ2xlMRcw
FQYDVQQLDA5Qcml2YWN5U2FuZGJveDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAJ3TIWzi5XLZcaB6LkgrhBFUAU0634RScPQgs2Lkhpji6PDbklui4vfe
+JnLWPp1Yp3f9SXDomA8giasYNTRTf34Rnoug+NOESkJ6xK58I0+cb0YG3ZrvACZ
eut64+vlH2XpAiE74S3waFZZDpq+TWE8oedbnk+YJufFUIyOjZfrWczShmTAlegE
HADPF//Sq5lQPAvSWUF2w2+w/8cRY757jCjCx4By78hZCNKXfo9rGsQlmKDer4SR
RPuQvKAVKsdrLO7Z3Bf6/203pWs6EzApo171Xb4LkDGfhxz7D7lP/5Y3JL0/MlXs
nTk4ShAG+P4RH0svMEBYcFkJsLDfGncCAwEAAaOBrDCBqTCBpgYDVR0RBIGeMIGb
hoGGc3BpZmZlOi8vYXZzLnRjYS5mYWtlY2Evb3BlcmF0b3IvcHJpdmFjeS1zYW5k
Ym94LWRldi1qb2JzQHByb2QuZ29vZ2xlLmNvbS9wdWJsaXNoZXIvcmVsZWFzZUBn
b29nbGUuY29tL3dvcmtsb2FkL2VuY3J5cHRlZC16b25lL3YvMS4wLjCCECouYXZz
LnRjYS5mYWtlY2EwCgYIKoZIzj0EAwIDSQAwRgIhAKavf3xkDmpkJQ8Ou0pC/Vk7
HbEWwW6JklpPJ3p0XlurAiEA2BecJv5aK/5zoqIe3flR5nS/f6ueZfpjuHJ0LUn+
7S4=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIBMjCB2KADAgECAgEDMAoGCCqGSM49BAMCMBAxDjAMBgNVBAMMBWludC0yMB4X
DTI1MDEwMTAwMDAwMFoXDTM2MDEwMTAwMDAwMFowEDEOMAwGA1UEAwwFaW50LTMw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQcLJwgR54rVm4x1eg7BXSDTrPuvvy1
j+poC8hNZ0+bdHbdlLjQ2J3G12DXAa7QukLDpqOmMRhbQ9xvue1oS1aXoyMwITAP
BgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBBjAKBggqhkjOPQQDAgNJADBG
AiEA0dJDpMn5tJDIWv7DVq+EnxWLBFyhATfBcgmY3dIRkZYCIQCWM/48XehJr1p0
T0L8FTXpRc1hrKjHKH7jYEK8WtBmCQ==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIBMDCB2KADAgECAgECMAoGCCqGSM49BAMCMBAxDjAMBgNVBAMMBWludC0xMB4X
DTI1MDEwMTAwMDAwMFoXDTM2MDEwMTAwMDAwMFowEDEOMAwGA1UEAwwFaW50LTIw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQzlBZ8AhocNOGRv5AmJrNqKQn73WEh
vO6IQ8/GmK91tBdY/w/RdQMwmkAi0n54sXe24UeXKUB07js1pregDCqQoyMwITAP
BgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBBjAKBggqhkjOPQQDAgNHADBE
AiBD16kAZM3Zh1SP46Ympf2Zc+UzzVjTsMZfG7ITCufQ9QIgRs5UMH8yizu/ygQk
FdReYMP1Du2lwkQQHUDF4UwZ7rY=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIICOTCCASGgAwIBAgIBATANBgkqhkiG9w0BAQsFADBWMQswCQYDVQQGEwJVUzEL
MAkGA1UECAwCV0ExEDAOBgNVBAcMB1NlYXR0bGUxDzANBgNVBAoMBkdvb2dsZTEX
MBUGA1UECwwOUHJpdmFjeVNhbmRib3gwHhcNMjUwMTAxMDAwMDAwWhcNMzYwMTAx
MDAwMDAwWjAQMQ4wDAYDVQQDDAVpbnQtMTBZMBMGByqGSM49AgEGCCqGSM49AwEH
A0IABOFDoVkivvv+FkGWIFBJZMohfotkZG9SrdhuoFN4nt9Mj4tl4rZNxa6JQHDc
Sd7m0kAJ/2ZOEE4Z58JZK5vIAOSjIzAhMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0P
AQH/BAQDAgEGMA0GCSqGSIb3DQEBCwUAA4IBAQANyRRWo26E06KnFzOcxL8yjB/l
niiQlT1stYZM0KgYCYJbkNyUEHHR6dWlulyBNxPGgn/Z2Fl6JODJsM63ek48LQ0Z
TcApuIehi5lBPv0F+5u9Mmwbu7OQc9rSBwGHvi5MIJDtd5aUAvS9XhvcGGELa0zf
igcmzYlrKIPwDx7fAXo7OsdUMxptq8C310fkSCm0FEJRpd7eEfZZGZmvKDJZvaYV
Gy7q5kUanaBOds5cg9C6ByXu2p2dv/eJglQWBpmy6GgJgCAb68ujM/wfI4X2ApUu
If963vruIjbjEef6zDujoz7tVnN+4+mI54Sl+J+0J03gII/uR80XikgL7Gdp
-----END CERTIFICATE-----
";
