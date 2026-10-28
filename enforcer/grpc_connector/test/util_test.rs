// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// See the License for the specific language governing permissions and
// limitations under the License.

use grpc_connector::GrpcChannelPool;
use grpc_connector::{get_grpc_timeout_or_log, try_parse_grpc_timeout};
use grpc_connector_test_proto::enforcer::grpc_connector::test::{
    test_service_client::TestServiceClient,
    test_service_server::{TestService, TestServiceServer},
    TestRequest, TestResponse,
};
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_stream::StreamExt;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::{server::Connected, Server, Uri};

#[derive(Default, Clone)]
struct MockTestService {
    last_authority: Arc<Mutex<Option<String>>>,
}

#[tonic::async_trait]
impl TestService for MockTestService {
    async fn unary_call(
        &self,
        request: tonic::Request<TestRequest>,
    ) -> Result<tonic::Response<TestResponse>, tonic::Status> {
        if let Some(uri) = request.extensions().get::<Uri>() {
            *self.last_authority.lock().unwrap() =
                uri.authority().map(|authority| authority.to_string());
        }
        Ok(tonic::Response::new(TestResponse {}))
    }
}

struct TestTlsStream<S>(tokio_boring::SslStream<S>);

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for TestTlsStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for TestTlsStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl<S: Connected> Connected for TestTlsStream<S> {
    type ConnectInfo = S::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.0.get_ref().connect_info()
    }
}

#[derive(Clone)]
struct CaptureUriService<S> {
    inner: S,
}

impl<S, B> tower::Service<tonic::codegen::http::Request<B>> for CaptureUriService<S>
where
    S: tower::Service<tonic::codegen::http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: tonic::codegen::http::Request<B>) -> Self::Future {
        let uri = req.uri().clone();
        req.extensions_mut().insert(uri);
        self.inner.call(req)
    }
}

fn create_test_tls_acceptor_and_connector() -> (boring::ssl::SslAcceptor, boring::ssl::SslConnector)
{
    let group = boring::ec::EcGroup::from_curve_name(boring::nid::Nid::X9_62_PRIME256V1).unwrap();
    let ec_key = boring::ec::EcKey::generate(&group).unwrap();
    let pkey = boring::pkey::PKey::from_ec_key(ec_key).unwrap();

    let mut x509_builder = boring::x509::X509::builder().unwrap();
    x509_builder.set_version(2).unwrap();
    x509_builder.set_pubkey(&pkey).unwrap();
    let not_before = boring::asn1::Asn1Time::days_from_now(0).unwrap();
    let not_after = boring::asn1::Asn1Time::days_from_now(1).unwrap();
    x509_builder.set_not_before(&not_before).unwrap();
    x509_builder.set_not_after(&not_after).unwrap();
    x509_builder.sign(&pkey, boring::hash::MessageDigest::sha256()).unwrap();
    let cert = x509_builder.build();

    let mut acceptor =
        boring::ssl::SslAcceptor::mozilla_intermediate_v5(boring::ssl::SslMethod::tls()).unwrap();
    acceptor.set_private_key(&pkey).unwrap();
    acceptor.set_certificate(&cert).unwrap();
    acceptor.set_alpn_protos(b"\x02h2").unwrap();
    acceptor.set_alpn_select_callback(|_ssl, client_protos| {
        boring::ssl::select_next_proto(b"\x02h2", client_protos)
            .ok_or(boring::ssl::AlpnError::NOACK)
    });

    let mut connector = boring::ssl::SslConnector::builder(boring::ssl::SslMethod::tls()).unwrap();
    connector.set_verify(boring::ssl::SslVerifyMode::NONE);
    connector.set_alpn_protos(b"\x02h2").unwrap();

    (acceptor.build(), connector.build())
}

/// Sets up a TLS-enabled mock gRPC server on a Unix Domain Socket.
async fn setup_tls_uds_server(
    service: MockTestService,
    acceptor: boring::ssl::SslAcceptor,
) -> (String, oneshot::Sender<()>, tempfile::TempDir) {
    let (tx, rx) = oneshot::channel();
    let temp_dir = tempdir().unwrap();
    let uds_path = temp_dir.path().join("test-tls-connector.sock");
    let uds = UnixListener::bind(&uds_path).unwrap();
    let acceptor = Arc::new(acceptor);
    let tls_stream = UnixListenerStream::new(uds).then(move |conn| {
        let acceptor = acceptor.clone();
        async move {
            let stream = conn?;
            let tls =
                tokio_boring::accept(&acceptor, stream).await.map_err(std::io::Error::other)?;
            Ok::<_, std::io::Error>(TestTlsStream(tls))
        }
    });

    tokio::spawn(async move {
        Server::builder()
            .layer(tower::layer::layer_fn(|inner| CaptureUriService { inner }))
            .add_service(TestServiceServer::new(service))
            .serve_with_incoming_shutdown(tls_stream, async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let address = format!("unix:{}", uds_path.to_str().unwrap());
    (address, tx, temp_dir)
}

/// Sets up a mock gRPC server on a Unix Domain Socket.
async fn setup_uds_server() -> (String, oneshot::Sender<()>, tempfile::TempDir) {
    let (tx, rx) = oneshot::channel();
    let temp_dir = tempdir().unwrap();
    let uds_path = temp_dir.path().join("test-connector.sock");
    let uds = UnixListener::bind(&uds_path).unwrap();
    let uds_stream = UnixListenerStream::new(uds);

    tokio::spawn(async move {
        Server::builder()
            .add_service(TestServiceServer::new(MockTestService::default()))
            .serve_with_incoming_shutdown(uds_stream, async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let address = format!("unix:{}", uds_path.to_str().unwrap());
    (address, tx, temp_dir)
}

/// Sets up a mock gRPC server on a TCP socket.
async fn setup_tcp_server() -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(TestServiceServer::new(MockTestService::default()))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    rx.await.ok();
                },
            )
            .await
            .unwrap();
    });

    (format!("http://{}", addr), tx)
}

#[tokio::test]
async fn test_connect_tcp_success() {
    let (addr, shutdown_tx) = setup_tcp_server().await;
    let result = GrpcChannelPool::new(addr, 1, 1, 10, 2).await;
    assert!(result.is_ok());
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_connect_uds_success() {
    let (addr, shutdown_tx, _temp_dir) = setup_uds_server().await;
    let result = GrpcChannelPool::new(addr, 1, 1, 10, 2).await;
    assert!(result.is_ok());
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_connect_tcp_failure() {
    let result = GrpcChannelPool::new("http://127.0.0.1:1".to_string(), 1, 2, 10, 2).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_connect_uds_failure() {
    let result =
        GrpcChannelPool::new("unix:/tmp/nonexistent-socket-for-test.sock".to_string(), 1, 2, 10, 2)
            .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_new_tls_fails_with_non_tls_server() {
    // TODO(b/439073817): Add TLS enabled mock server for testing purpose.
    // The next CL will introduce acceptor and therefore making it possible to run mock
    // TLS server for testing purpose.
    let (addr, shutdown_tx, _temp_dir) = setup_uds_server().await;
    let connector =
        boring::ssl::SslConnector::builder(boring::ssl::SslMethod::tls()).unwrap().build();
    let result = GrpcChannelPool::new_tls(
        addr,
        /*pool_size=*/ 1,
        /*retry_count=*/ 1,
        /*retry_delay_ms=*/ 10,
        /*retry_scaling=*/ 2,
        connector,
        /*sni=*/ "localhost".to_string(),
    )
    .await;
    assert!(result.is_err());
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_new_tls_with_bns_sni_falls_back_to_default_authority() {
    let service = MockTestService::default();
    let (acceptor, connector) = create_test_tls_acceptor_and_connector();
    let (addr, shutdown_tx, _temp_dir) = setup_tls_uds_server(service.clone(), acceptor).await;
    let bns_sni = "/bns/mock-cell/borg/mock-cell/bns/mock-job--mock-hash.example.com";

    let pool = GrpcChannelPool::new_tls(
        addr,
        /*pool_size=*/ 1,
        /*retry_count=*/ 1,
        /*retry_delay_ms=*/ 10,
        /*retry_scaling=*/ 2,
        connector,
        /*sni=*/ bns_sni.to_string(),
    )
    .await
    .expect("Failed to connect to TLS UDS server with BNS SNI");

    let mut client = TestServiceClient::new(pool.next_channel());
    client.unary_call(TestRequest {}).await.expect("Unary call over TLS UDS with BNS SNI failed");

    assert_eq!(service.last_authority.lock().unwrap().as_deref(), Some("localhost"));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_new_tls_sets_sni_as_authority() {
    let service = MockTestService::default();
    let (acceptor, connector) = create_test_tls_acceptor_and_connector();
    let (addr, shutdown_tx, _temp_dir) = setup_tls_uds_server(service.clone(), acceptor).await;
    let expected_sni = "custom-sni.example.com";

    let pool = GrpcChannelPool::new_tls(
        addr,
        /*pool_size=*/ 1,
        /*retry_count=*/ 1,
        /*retry_delay_ms=*/ 10,
        /*retry_scaling=*/ 2,
        connector,
        /*sni=*/ expected_sni.to_string(),
    )
    .await
    .expect("Failed to connect to TLS UDS server");

    let mut client = TestServiceClient::new(pool.next_channel());
    client.unary_call(TestRequest {}).await.expect("Unary call over TLS UDS failed");

    assert_eq!(service.last_authority.lock().unwrap().as_deref(), Some(expected_sni));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_connect_tcp_retry_and_succeed() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_address = format!("http://{}", addr);
    drop(listener); // Close the listener to ensure the first connection fails.

    let connect_future = GrpcChannelPool::new(server_address, 1, 5, 100, 2);

    // Give it time to fail once.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Now start the real server.
    let (tx, rx) = oneshot::channel();
    let listener = TcpListener::bind(addr).await.unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(TestServiceServer::new(MockTestService::default()))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    rx.await.ok();
                },
            )
            .await
            .unwrap();
    });

    let result = connect_future.await;
    assert!(result.is_ok(), "Connection should eventually succeed with retries");

    let _ = tx.send(());
}

#[tokio::test]
async fn test_connect_uds_retry_and_succeed() {
    let temp_dir = tempdir().unwrap();
    let uds_path = temp_dir.path().join("test-retry.sock");
    let server_address = format!("unix:{}", uds_path.to_str().unwrap());

    // The file doesn't exist, so the first connection will fail.
    let connect_future = GrpcChannelPool::new(server_address, 1, 5, 100, 2);

    // Give it time to fail once.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Now start the real server.
    let (tx, rx) = oneshot::channel();
    let listener = UnixListener::bind(&uds_path).unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(TestServiceServer::new(MockTestService::default()))
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let result = connect_future.await;
    assert!(result.is_ok(), "Connection should eventually succeed with retries");

    let _ = tx.send(());
}

#[tokio::test]
async fn test_connect_uds_tls_retry_and_succeed() {
    let temp_dir = tempdir().unwrap();
    let uds_path = temp_dir.path().join("test-tls-retry.sock");
    let server_address = format!("unix:{}", uds_path.to_str().unwrap());
    let (acceptor, connector) = create_test_tls_acceptor_and_connector();

    let connect_future = GrpcChannelPool::new_tls(
        server_address,
        /*pool_size=*/ 1,
        /*retry_count=*/ 5,
        /*retry_multiplier=*/ 2,
        /*retry_initial_delay_ms=*/ 50,
        connector,
        /*sni=*/ "localhost".to_string(),
    );
    let connect_handle = tokio::spawn(connect_future);

    // Wait long enough for the initial attempt (0ms) and first retry (~100ms) to fail.
    tokio::time::sleep(Duration::from_millis(180)).await;

    let (tx, rx) = oneshot::channel();
    let listener = UnixListener::bind(&uds_path).unwrap();
    let acceptor = Arc::new(acceptor);
    let tls_stream = UnixListenerStream::new(listener).then(move |conn| {
        let acceptor = acceptor.clone();
        async move {
            let stream = conn?;
            let tls =
                tokio_boring::accept(&acceptor, stream).await.map_err(std::io::Error::other)?;
            Ok::<_, std::io::Error>(TestTlsStream(tls))
        }
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(TestServiceServer::new(MockTestService::default()))
            .serve_with_incoming_shutdown(tls_stream, async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    let result = tokio::time::timeout(Duration::from_secs(2), connect_handle)
        .await
        .expect("TLS UDS retry timed out (exponential backoff may be misconfigured)")
        .expect("connect task panicked");
    assert!(result.is_ok(), "TLS UDS connection should eventually succeed with retries");

    let _ = tx.send(());
}

// Tests for try_parse_grpc_timeout
fn make_meta(key: &'static str, value: &'static str) -> MetadataMap {
    let mut meta = MetadataMap::new();
    meta.insert(key, MetadataValue::from_static(value));
    meta
}

#[test]
fn test_parse_valid_timeouts() {
    assert_eq!(
        try_parse_grpc_timeout(&make_meta("grpc-timeout", "1H")).unwrap(),
        Some(Duration::from_secs(3600))
    );
    assert_eq!(
        try_parse_grpc_timeout(&make_meta("grpc-timeout", "2M")).unwrap(),
        Some(Duration::from_secs(120))
    );
    assert_eq!(
        try_parse_grpc_timeout(&make_meta("grpc-timeout", "3S")).unwrap(),
        Some(Duration::from_secs(3))
    );
    assert_eq!(
        try_parse_grpc_timeout(&make_meta("grpc-timeout", "4m")).unwrap(),
        Some(Duration::from_millis(4))
    );
    assert_eq!(
        try_parse_grpc_timeout(&make_meta("grpc-timeout", "5u")).unwrap(),
        Some(Duration::from_micros(5))
    );
    assert_eq!(
        try_parse_grpc_timeout(&make_meta("grpc-timeout", "6n")).unwrap(),
        Some(Duration::from_nanos(6))
    );
    assert_eq!(
        try_parse_grpc_timeout(&make_meta("grpc-timeout", "100S")).unwrap(),
        Some(Duration::from_secs(100))
    );
}

#[test]
fn test_parse_missing_header() {
    assert_eq!(try_parse_grpc_timeout(&MetadataMap::new()).unwrap(), None);
}

#[test]
fn test_parse_invalid_values() {
    assert!(try_parse_grpc_timeout(&make_meta("grpc-timeout", "")).is_err()); // Empty
    assert!(try_parse_grpc_timeout(&make_meta("grpc-timeout", "S")).is_err()); // No number
    assert!(try_parse_grpc_timeout(&make_meta("grpc-timeout", "1")).is_err()); // No unit
    assert!(try_parse_grpc_timeout(&make_meta("grpc-timeout", "abcS")).is_err()); // Invalid number
    assert!(try_parse_grpc_timeout(&make_meta("grpc-timeout", "1X")).is_err()); // Invalid unit
    assert!(try_parse_grpc_timeout(&make_meta("grpc-timeout", "1SS")).is_err());
    // Invalid format
}

#[test]
fn test_parse_large_values() {
    // Test near u64 max for seconds to check for overflow issues if not handled carefully.
    // Note: The function parses to u64, so very large numbers for smaller units are fine.
    assert_eq!(
        try_parse_grpc_timeout(&make_meta("grpc-timeout", "18446744073709551615S")).unwrap(),
        Some(Duration::from_secs(u64::MAX))
    );
}

#[test]
fn test_get_grpc_timeout_or_log() {
    assert_eq!(
        get_grpc_timeout_or_log(&make_meta("grpc-timeout", "3S")),
        Some(Duration::from_secs(3))
    );
    assert_eq!(get_grpc_timeout_or_log(&MetadataMap::new()), None);
    assert_eq!(get_grpc_timeout_or_log(&make_meta("grpc-timeout", "invalid")), None);
}
