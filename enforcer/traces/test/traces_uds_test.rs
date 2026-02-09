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

use fake_opentelemetry_collector::{ExportedSpan, FakeCollectorServer};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio::sync::Notify;
use traces::setup_telemetry;
use tracing::info_span;

#[tokio::test]
async fn test_trace_export_uds() {
    // 1. Start the fake collector (TCP)
    let mut collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    let collector_endpoint = collector.endpoint();
    // Remove http:// prefix to get the address for TcpStream::connect
    let collector_addr =
        collector_endpoint.strip_prefix("http://").unwrap_or(&collector_endpoint).to_string();

    // 2. Setup UDS Proxy
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let uds_path = temp_dir.path().join("traces.sock");
    let uds_path_str = uds_path.to_str().expect("Invalid path").to_string();

    let listener = UnixListener::bind(&uds_path).expect("Failed to bind UDS listener");
    let shutdown = Arc::new(Notify::new());
    let shutdown_rx = shutdown.clone();

    // Spawn proxy task to forward UDS traffic to the TCP collector
    let proxy_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                res = listener.accept() => {
                    if let Ok((mut ingress, _)) = res {
                        let collector_addr = collector_addr.clone();
                        tokio::spawn(async move {
                            if let Ok(mut egress) = tokio::net::TcpStream::connect(collector_addr).await {
                                let (mut ri, mut wi) = ingress.split();
                                let (mut ro, mut wo) = egress.split();
                                let _ = tokio::join!(
                                    tokio::io::copy(&mut ri, &mut wo),
                                    tokio::io::copy(&mut ro, &mut wi)
                                );
                            }
                        });
                    }
                }
                _ = shutdown_rx.notified() => {
                    break;
                }
            }
        }
    });

    // 3. Setup Telemetry with UDS endpoint
    let endpoint = Some(format!("unix:{}", uds_path_str));
    let provider = setup_telemetry(&endpoint).await.expect("Failed to setup telemetry");

    // 4. Generate Traces
    {
        let _span = info_span!("test_span_uds").entered();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Force flush to ensure spans are sent to the collector
    let _ = provider.force_flush();

    // 5. Verify Spans
    let exported_spans: Vec<ExportedSpan> =
        collector.exported_spans(1, Duration::from_secs(5)).await;

    assert!(!exported_spans.is_empty(), "Should have received at least one span batch via UDS");

    let found = exported_spans.iter().any(|span| span.name == "test_span_uds");
    assert!(found, "Did not find span with name 'test_span_uds'");

    let _ = provider.shutdown();
    shutdown.notify_one();
    let _ = proxy_handle.await;
}
