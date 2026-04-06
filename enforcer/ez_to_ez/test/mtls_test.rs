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
use mtls::mtls::{sni, EzMtlsManager, EzToEzPolicy, SpiffeUri};
use std::collections::HashSet;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

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
    let policy = std::sync::Arc::new(EzToEzPolicy {
        allowed_trust_domains: HashSet::new(),
        allowed_operator_domains: HashSet::new(),
        allowed_publisher_ids: HashSet::new(),
        allowed_workload_names: HashSet::new(),
    });

    let result = EzMtlsManager::create(key_path, csr_path, server_addr, &ez_manifest, policy).await;
    assert!(result.is_ok(), "Failed to connect to mock mTLS service: {:?}", result.err());
    let manager = result.unwrap();

    let fetch_result = manager.fetch_certificate(vec![1, 2, 3]).await;
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

#[test]
fn test_spiffe_policy_verify() {
    let policy = EzToEzPolicy {
        allowed_trust_domains: HashSet::from(["allow.trust".to_string()]),
        allowed_operator_domains: HashSet::from(["allow.operator".to_string()]),
        allowed_publisher_ids: HashSet::from(["allow.publisher".to_string()]),
        allowed_workload_names: HashSet::from(["allow.workload".to_string()]),
    };

    let valid_uri = SpiffeUri {
        trust_domain: "allow.trust".to_string(),
        operator_domain: "allow.operator".to_string(),
        publisher_id: "allow.publisher".to_string(),
        workload_name: "allow.workload".to_string(),
    };

    assert!(policy.verify(&valid_uri));

    let invalid_uri = SpiffeUri {
        trust_domain: "deny.trust".to_string(),
        operator_domain: "allow.operator".to_string(),
        publisher_id: "allow.publisher".to_string(),
        workload_name: "allow.workload".to_string(),
    };

    assert!(!policy.verify(&invalid_uri));
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
    let policy = std::sync::Arc::new(EzToEzPolicy {
        allowed_trust_domains: HashSet::from(["avs.tca.fakeca".to_string()]),
        allowed_operator_domains: HashSet::from([
            "privacy-sandbox-dev-jobs@prod.google.com".to_string()
        ]),
        allowed_publisher_ids: HashSet::from(["release@google.com".to_string()]),
        // TODO: /v/ will not be part of the SPIFFE ID. Will remove in next CLs.
        allowed_workload_names: HashSet::from(["encrypted-zone/v/1.0.0".to_string()]),
    });

    let server_manager = EzMtlsManager::create(
        key_path.clone(),
        csr_path.clone(),
        server_addr.clone(),
        &ez_manifest,
        policy.clone(),
    )
    .await
    .expect("Failed to create server manager");
    let client_manager =
        EzMtlsManager::create(key_path, csr_path, server_addr, &ez_manifest, policy.clone())
            .await
            .expect("Failed to create client manager");

    let acceptor =
        server_manager.create_tls_acceptor().await.expect("Failed to create TLS acceptor");

    let connector =
        client_manager.create_tls_connector().await.expect("Failed to create TLS connector");

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

    // Create a mTLS manager to fetch the certificates from the mock server and load certificates from testdata.
    let policy = std::sync::Arc::new(EzToEzPolicy {
        allowed_trust_domains: HashSet::from(["avs.tca.fakeca".to_string()]),
        allowed_operator_domains: HashSet::from([
            "privacy-sandbox-dev-jobs@prod.google.com".to_string()
        ]),
        allowed_publisher_ids: HashSet::from(["release@google.com".to_string()]),
        allowed_workload_names: HashSet::from(["encrypted-zone/v/1.0.0".to_string()]),
    });

    let server_manager = EzMtlsManager::create(
        key_path.clone(),
        csr_path.clone(),
        server_addr.clone(),
        &ez_manifest,
        policy.clone(),
    )
    .await
    .expect("Failed to create server manager");

    // Create a client side mTLS manager.
    let client_manager =
        EzMtlsManager::create(key_path, csr_path, server_addr, &ez_manifest, policy.clone())
            .await
            .expect("Failed to create client manager");

    let acceptor =
        server_manager.create_tls_acceptor().await.expect("Failed to create TLS acceptor");

    let connector =
        client_manager.create_tls_connector().await.expect("Failed to create TLS connector");

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

#[tokio::test]
async fn test_tls_handshake_rejected_by_policy() {
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

    let ez_manifest = EzManifest {
        isolate_name: "encrypted-zone".to_string(),
        publisher_id: "release@google.com".to_string(),
        ..Default::default()
    };

    let valid_policy = std::sync::Arc::new(EzToEzPolicy {
        allowed_trust_domains: HashSet::from(["avs.tca.fakeca".to_string()]),
        allowed_operator_domains: HashSet::from([
            "privacy-sandbox-dev-jobs@prod.google.com".to_string()
        ]),
        allowed_publisher_ids: HashSet::from(["release@google.com".to_string()]),
        allowed_workload_names: HashSet::from(["encrypted-zone/v/1.0.0".to_string()]),
    });

    let invalid_policy = std::sync::Arc::new(EzToEzPolicy {
        allowed_trust_domains: HashSet::from(["avs.tca.fakeca".to_string()]),
        allowed_operator_domains: HashSet::from([
            "privacy-sandbox-dev-jobs@prod.google.com".to_string()
        ]),
        allowed_publisher_ids: HashSet::from(["release@example.com".to_string()]),
        allowed_workload_names: HashSet::from(["encrypted-zone/v/1.0.0".to_string()]),
    });

    // Server side policy is invalid, so the server should reject the client.
    let server_manager = EzMtlsManager::create(
        key_path.clone(),
        csr_path.clone(),
        server_addr.clone(),
        &ez_manifest,
        invalid_policy.clone(),
    )
    .await
    .expect("Failed to create server manager");

    let client_manager = EzMtlsManager::create(
        key_path.clone(),
        csr_path.clone(),
        server_addr,
        &ez_manifest,
        valid_policy.clone(),
    )
    .await
    .expect("Failed to create client manager");

    let acceptor =
        server_manager.create_tls_acceptor().await.expect("Failed to create TLS acceptor");

    let connector =
        client_manager.create_tls_connector().await.expect("Failed to create TLS connector");

    let (client_stream, server_stream) = tokio::io::duplex(1024);

    let server_task = tokio::spawn(async move {
        let result = tokio_boring::accept(&acceptor, server_stream).await;
        assert!(result.is_err(), "Server handshake should have failed");
    });

    let client_task = tokio::spawn(async move {
        let result = tokio_boring::connect(
            connector.configure().unwrap().verify_hostname(false),
            "dummy-sni-value",
            client_stream,
        )
        .await;

        match result {
            Ok(mut stream) => {
                // If connect returned Ok, try to read to detect that the server rejected us.
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 128];
                let read_result = stream.read(&mut buf).await;
                assert!(
                    read_result.is_err() || (read_result.is_ok() && read_result.unwrap() == 0),
                    "Client should have failed to read due to server rejection"
                );
            }
            Err(_) => {
                // Client handshake failed directly, which is also fine.
            }
        }
    });

    let (server_result, client_result) = tokio::join!(server_task, client_task);
    server_result.expect("Server task panicked");
    client_result.expect("Client task panicked");
    let _ = tx.send(());
}
