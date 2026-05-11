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

use ez_mtls_proto::enforcer::v1::ez_mtls_service_server::{EzMtlsService, EzMtlsServiceServer};
use ez_mtls_proto::enforcer::v1::{
    GetCertificateRequest, GetCertificateResponse, ReportSniRequest, ReportSniResponse,
};
use manifest_proto::enforcer::v1::EzManifest;
use mtls::mtls::{sni, EzMtlsManager, SpiffeUri};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use enforcer_proto::enforcer::v1::ControlPlaneMetadata;
use metrics_test_utils::TestMetrics;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use outbound_ez_to_ez_handler::{OutboundEzToEzHandler, OutboundTlsConfig};
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};

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

struct MockMtlsService {
    leaf_der: &'static [u8],
    root_der: &'static [u8],
}

impl MockMtlsService {
    pub fn new() -> Self {
        Self {
            leaf_der: include_bytes!("testdata/leaf.der"),
            root_der: include_bytes!("testdata/root.der"),
        }
    }
}

#[tonic::async_trait]
impl EzMtlsService for MockMtlsService {
    async fn get_certificate(
        &self,
        _request: Request<GetCertificateRequest>,
    ) -> Result<Response<GetCertificateResponse>, Status> {
        Ok(Response::new(GetCertificateResponse {
            certs: vec![self.leaf_der.to_vec(), self.root_der.to_vec()],
            ..Default::default()
        }))
    }

    async fn report_sni(
        &self,
        _request: Request<ReportSniRequest>,
    ) -> Result<Response<ReportSniResponse>, Status> {
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

    let ez_manifest = manifest_proto::enforcer::v1::EzManifest {
        isolate_name: "test-isolate".to_string(),
        publisher_id: "test-publisher".to_string(),
        ..Default::default()
    };
    let config = mtls::mtls::EzMtlsManagerConfig {
        mtls_key_path: key_path,
        csr_path,
        proxy_address: server_addr,
        ez_manifest,
    };
    let manager_result = EzMtlsManager::build(config).await;
    assert!(
        manager_result.is_ok(),
        "Failed to connect to mock mTLS service: {:?}",
        manager_result.err()
    );
    let manager = manager_result.unwrap();

    let fetch_result = manager.fetch_certificate().await;
    assert!(fetch_result.is_ok(), "Failed to fetch certificate");

    let report_result = manager.report_snis(vec!["test-sni".to_string()]).await;
    assert!(report_result.is_ok(), "Failed to report SNIs");

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
    let ez_manifest = EzManifest {
        isolate_name: "encrypted-zone".to_string(),
        publisher_id: "release@google.com".to_string(),
        ..Default::default()
    };
    let config = mtls::mtls::EzMtlsManagerConfig {
        mtls_key_path: key_path.clone(),
        csr_path: csr_path.clone(),
        proxy_address: server_addr.clone(),
        ez_manifest,
    };
    let server_manager =
        EzMtlsManager::build(config.clone()).await.expect("Failed to initialize server manager");
    let client_manager =
        EzMtlsManager::build(config).await.expect("Failed to initialize client manager");

    let acceptor =
        server_manager.create_tls_acceptor().await.expect("Failed to create TLS acceptor");

    let connector = client_manager
        .get_connector_factory()
        .create_connector(&client_manager.spiffe_identity().operator_domain)
        .await
        .expect("Failed to create TLS connector");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _tls_stream =
            tokio_boring::accept(&acceptor, stream).await.expect("Server failed to accept TLS");
        // Handshake succeeded
    });

    let client_task = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        // Domain verification is disabled by configuration (replaced with SPIFFE policy matching)
        let _tls_stream = tokio_boring::connect(
            connector.configure().unwrap().verify_hostname(false),
            "localhost",
            stream,
        )
        .await
        .expect("Client failed to connect TLS");
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

    // Create a fake EZ manifest.
    let ez_manifest = EzManifest {
        isolate_name: "encrypted-zone".to_string(),
        publisher_id: "release@google.com".to_string(),
        ..Default::default()
    };

    let config = mtls::mtls::EzMtlsManagerConfig {
        mtls_key_path: key_path.clone(),
        csr_path: csr_path.clone(),
        proxy_address: server_addr.clone(),
        ez_manifest,
    };
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
        .create_connector(&client_manager.spiffe_identity().operator_domain)
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

    let ez_manifest = manifest_proto::enforcer::v1::EzManifest {
        isolate_name: "encrypted-zone".to_string(),
        publisher_id: "release@google.com".to_string(),
        ..Default::default()
    };

    let config = mtls::mtls::EzMtlsManagerConfig {
        mtls_key_path: key_path.clone(),
        csr_path: csr_path.clone(),
        proxy_address: server_addr.clone(),
        ez_manifest,
    };

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
        trust_domain: client_manager.spiffe_identity().trust_domain.clone(),
    };

    let outbound_handler = OutboundEzToEzHandler::new(
        server_address.clone(),
        TestMetrics::default(),
        Some(outbound_tls_config),
    )
    .await
    .unwrap();

    TestContext { _dir: dir, mock_service_tx: tx, server_task, client_manager, outbound_handler }
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

    let meta = ControlPlaneMetadata {
        destination_operator_domain: ctx.client_manager.spiffe_identity().operator_domain.clone(),
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

    let response = ctx.outbound_handler.remote_invoke(request, None).await;

    assert!(response.is_ok(), "Remote invoke failed: {:?}", response.err());
    let response = response.unwrap();
    let output_payload = match response.ez_response_payload.unwrap().delivery_method.unwrap() {
        DeliveryMethod::InlineData(inline) => inline.datagrams[0].clone(),
        _ => panic!("Expected InlineData"),
    };
    assert_eq!(output_payload, b"hello world");
    let _ = ctx.mock_service_tx.send(()); // Stop mock service
    ctx.server_task.abort(); // Stop server task
}

/// Tests the streaming invoke flow between OutboundEzToEzHandler and InboundEzToEzHandler over mTLS.
#[tokio::test]
async fn test_e2e_mtls_streaming() {
    let ctx = create_test_context().await;

    let meta = ControlPlaneMetadata {
        destination_operator_domain: ctx.client_manager.spiffe_identity().operator_domain.clone(),
        ..Default::default()
    };

    let (req_tx, req_rx) = tokio::sync::mpsc::channel(10);
    let mut res_rx =
        ctx.outbound_handler.remote_streaming_connect(Some(&meta), req_rx).await.unwrap();

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

/// Tests that the outbound handler fails to connect when the peer identity does not match the expected identity.
/// This verifies that SPIFFE identity verification is working.
#[tokio::test]
async fn test_e2e_mtls_wrong_identity() {
    // Set retry count to 1 to fail fast in test
    std::env::set_var("PROXY_CONNECT_RETRY_COUNT", "1");

    let ctx = create_test_context().await;

    // The server side doesn't have "wrong_domain" as its operator domain in SPIFFE ID.
    // The server returned certificate must match the outbound request destination_operator_domain.
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

    let response = ctx.outbound_handler.remote_invoke(request, None).await;

    assert!(response.is_err(), "Expected remote invoke to fail due to identity mismatch");
    let err = response.err().unwrap();
    assert!(
        format!("{:?}", err).contains("CERTIFICATE_VERIFY_FAILED"),
        "Expected error to contain 'CERTIFICATE_VERIFY_FAILED', got: {:?}",
        err
    );

    let _ = ctx.mock_service_tx.send(()); // Stop mock service
    ctx.server_task.abort(); // Stop server task
}

/// Tests that the inbound handler correctly propagates errors from the junction back to the client.
/// This verifies that error handling is working across the mTLS boundary.
#[tokio::test]
async fn test_e2e_mtls_junction_error() {
    let ctx = create_test_context().await;

    let meta = ControlPlaneMetadata {
        destination_operator_domain: ctx.client_manager.spiffe_identity().operator_domain.clone(),
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
