// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// See the License for the specific language governing permissions and
// limitations under the License.

use grpc_connector::connect;
use grpc_connector::try_parse_grpc_timeout;
use grpc_connector_test_proto::enforcer::grpc_connector::test::{
    test_service_server::{TestService, TestServiceServer},
    TestRequest, TestResponse,
};
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::Server;

#[derive(Default, Clone)]
struct MockTestService {}

#[tonic::async_trait]
impl TestService for MockTestService {
    async fn unary_call(
        &self,
        _request: tonic::Request<TestRequest>,
    ) -> Result<tonic::Response<TestResponse>, tonic::Status> {
        Ok(tonic::Response::new(TestResponse {}))
    }
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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
    let result = connect(addr, 1, 10).await;
    assert!(result.is_ok());
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_connect_uds_success() {
    let (addr, shutdown_tx, _temp_dir) = setup_uds_server().await;
    let result = connect(addr, 1, 10).await;
    assert!(result.is_ok());
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_connect_tcp_failure() {
    let result = connect("http://127.0.0.1:1".to_string(), 2, 10).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_connect_uds_failure() {
    let result = connect("unix:/tmp/nonexistent-socket-for-test.sock".to_string(), 2, 10).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_connect_tcp_retry_and_succeed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_address = format!("http://{}", addr);
    drop(listener); // Close the listener to ensure the first connection fails.

    let connect_future = connect(server_address, 5, 100);

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
    let connect_future = connect(server_address, 5, 100);

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
