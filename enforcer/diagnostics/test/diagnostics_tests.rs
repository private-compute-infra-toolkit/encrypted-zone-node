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

use console_helper::ConsoleHelperService;
use diagnostics::{EnforcerDiagnosticService, EnforcerPprofService, TokioDiagnosticService};
use diagnostics_proto::enforcer::diagnostics::v1::diagnostic_service_client::DiagnosticServiceClient;
use diagnostics_proto::enforcer::diagnostics::v1::diagnostic_service_server::DiagnosticServiceServer;
use diagnostics_proto::enforcer::diagnostics::v1::{
    get_diagnostic_request, get_diagnostic_response, CpuProfileSpec, GetDiagnosticRequest,
    HeapProfileSpec, TokioProfileSpec,
};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Code;

const HOST: Ipv4Addr = Ipv4Addr::LOCALHOST;

async fn start_diagnostics_server() -> (u16, oneshot::Sender<()>) {
    let console_helper_endpoint = format!("http://{}:{}", HOST, 0);
    let console_helper = Arc::new(ConsoleHelperService::new(console_helper_endpoint));
    let tokio_service = Arc::new(TokioDiagnosticService::new(console_helper));
    let pprof_service = EnforcerPprofService::new();
    let diagnostics_service =
        EnforcerDiagnosticService::new(pprof_service.clone(), (*tokio_service).clone());

    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(HOST), 0))
        .await
        .expect("failed to bind to port");
    let port = listener.local_addr().expect("failed to get local address").port();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(DiagnosticServiceServer::new(diagnostics_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                shutdown_rx.await.ok();
            })
            .await
            .expect("failed to start server");
    });

    (port, shutdown_tx)
}

#[tokio::test]
async fn test_get_diagnostic_cpu_success() {
    let (port, shutdown_tx) = start_diagnostics_server().await;

    let request = GetDiagnosticRequest {
        profile_spec: Some(get_diagnostic_request::ProfileSpec::Cpu(CpuProfileSpec {
            duration_seconds: 1,
        })),
    };

    let response_result = tokio::spawn(async move {
        let mut client = DiagnosticServiceClient::connect(format!("http://{}:{}", HOST, port))
            .await
            .expect("failed to connect to DiagnosticService");
        client.get_diagnostic(request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let response = response_result.expect("RPC failed").into_inner();
    match response.result {
        Some(get_diagnostic_response::Result::Cpu(cpu_result)) => {
            assert!(!cpu_result.pprof_data.is_empty());
            assert_eq!(cpu_result.captured_duration_seconds, 1);
        }
        _ => panic!("Expected CPU profile result, got {:?}", response.result),
    }
}

#[tokio::test]
async fn test_get_diagnostic_cpu_with_duration() {
    let (port, shutdown_tx) = start_diagnostics_server().await;

    let request = GetDiagnosticRequest {
        profile_spec: Some(get_diagnostic_request::ProfileSpec::Cpu(CpuProfileSpec {
            duration_seconds: 2,
        })),
    };

    let response_result = tokio::spawn(async move {
        let mut client = DiagnosticServiceClient::connect(format!("http://{}:{}", HOST, port))
            .await
            .expect("failed to connect to DiagnosticService");
        client.get_diagnostic(request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let response = response_result.expect("RPC failed").into_inner();
    match response.result {
        Some(get_diagnostic_response::Result::Cpu(cpu_result)) => {
            assert!(!cpu_result.pprof_data.is_empty());
            assert_eq!(cpu_result.captured_duration_seconds, 2);
        }
        _ => panic!("Expected CPU profile result, got {:?}", response.result),
    }
}

#[tokio::test]
async fn test_get_diagnostic_unimplemented_heap() {
    let (port, shutdown_tx) = start_diagnostics_server().await;

    let request = GetDiagnosticRequest {
        profile_spec: Some(get_diagnostic_request::ProfileSpec::Heap(HeapProfileSpec {})),
    };

    let response_result = tokio::spawn(async move {
        let mut client = DiagnosticServiceClient::connect(format!("http://{}:{}", HOST, port))
            .await
            .expect("failed to connect to DiagnosticService");
        client.get_diagnostic(request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let status = response_result.unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(status.message().contains("Requested profiling type is not supported"));
}

#[tokio::test]
async fn test_get_diagnostic_tokio_success() {
    let (port, shutdown_tx) = start_diagnostics_server().await;

    let request = GetDiagnosticRequest {
        profile_spec: Some(get_diagnostic_request::ProfileSpec::Tokio(TokioProfileSpec {
            duration: None,
            top_n: 1,
            location_filter: String::new(),
        })),
    };

    let response_result = tokio::spawn(async move {
        let mut client = DiagnosticServiceClient::connect(format!("http://{}:{}", HOST, port))
            .await
            .expect("failed to connect to DiagnosticService");
        client.get_diagnostic(request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let response = response_result.expect("RPC failed").into_inner();
    match response.result {
        Some(get_diagnostic_response::Result::Tokio(tokio_result)) => {
            assert_eq!(tokio_result.text_summary, "Tokio task statistics collected");
        }
        _ => panic!("Expected Tokio profile result, got {:?}", response.result),
    }
}

#[tokio::test]
async fn test_get_diagnostic_tokio_invalid_top_n() {
    let (port, shutdown_tx) = start_diagnostics_server().await;

    let request = GetDiagnosticRequest {
        profile_spec: Some(get_diagnostic_request::ProfileSpec::Tokio(TokioProfileSpec {
            duration: None,
            top_n: 0,
            location_filter: String::new(),
        })),
    };

    let response_result = tokio::spawn(async move {
        let mut client = DiagnosticServiceClient::connect(format!("http://{}:{}", HOST, port))
            .await
            .expect("failed to connect to DiagnosticService");
        client.get_diagnostic(request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let status = response_result.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("top_n must be greater than 0"));
}

#[tokio::test]
async fn test_get_diagnostic_invalid_argument() {
    let (port, shutdown_tx) = start_diagnostics_server().await;

    let request = GetDiagnosticRequest { profile_spec: None };

    let response_result = tokio::spawn(async move {
        let mut client = DiagnosticServiceClient::connect(format!("http://{}:{}", HOST, port))
            .await
            .expect("failed to connect to DiagnosticService");
        client.get_diagnostic(request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let status = response_result.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("profile_spec is required"));
}

#[tokio::test]
async fn test_get_diagnostic_cpu_invalid_duration() {
    let (port, shutdown_tx) = start_diagnostics_server().await;

    let request = GetDiagnosticRequest {
        profile_spec: Some(get_diagnostic_request::ProfileSpec::Cpu(CpuProfileSpec {
            duration_seconds: -3,
        })),
    };

    let response_result = tokio::spawn(async move {
        let mut client = DiagnosticServiceClient::connect(format!("http://{}:{}", HOST, port))
            .await
            .expect("failed to connect to DiagnosticService");
        client.get_diagnostic(request).await
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());

    let error = response_result.expect_err("RPC should fail");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(error.message(), "invalid duration (ie. < 1)");
}

#[tokio::test]
async fn test_get_diagnostic_cpu_concurrency() {
    let (port, shutdown_tx) = start_diagnostics_server().await;

    // Send two simultaneous requests.
    // The internal implementation uses a Mutex, so they should be processed sequentially.
    let request1 = GetDiagnosticRequest {
        profile_spec: Some(get_diagnostic_request::ProfileSpec::Cpu(CpuProfileSpec {
            duration_seconds: 1,
        })),
    };
    let request2 = GetDiagnosticRequest {
        profile_spec: Some(get_diagnostic_request::ProfileSpec::Cpu(CpuProfileSpec {
            duration_seconds: 1,
        })),
    };

    let client_addr = format!("http://{}:{}", HOST, port);
    let client_addr_clone = client_addr.clone();

    let start_time = std::time::Instant::now();

    let handle1 = tokio::spawn(async move {
        let mut client =
            DiagnosticServiceClient::connect(client_addr).await.expect("failed to connect");
        client.get_diagnostic(request1).await
    });

    let handle2 = tokio::spawn(async move {
        let mut client =
            DiagnosticServiceClient::connect(client_addr_clone).await.expect("failed to connect");
        client.get_diagnostic(request2).await
    });

    let (res1, res2) = tokio::join!(handle1, handle2);
    let elapsed = start_time.elapsed();

    let _ = shutdown_tx.send(());

    // Check results first to get better error messages if they failed.
    let resp1 = res1.unwrap().expect("Request 1 failed").into_inner();
    let resp2 = res2.unwrap().expect("Request 2 failed").into_inner();

    // Each request takes 1 second. Since they are serialized, total time should be >= 2 seconds.
    assert!(
        elapsed >= std::time::Duration::from_secs(2),
        "Requests should be serialized, expected >= 2s elapsed, got {:?}",
        elapsed
    );

    match (resp1.result, resp2.result) {
        (
            Some(get_diagnostic_response::Result::Cpu(cpu1)),
            Some(get_diagnostic_response::Result::Cpu(cpu2)),
        ) => {
            assert!(!cpu1.pprof_data.is_empty());
            assert!(!cpu2.pprof_data.is_empty());
        }
        _ => panic!("Expected both results to be successful CPU profiles"),
    }
}
