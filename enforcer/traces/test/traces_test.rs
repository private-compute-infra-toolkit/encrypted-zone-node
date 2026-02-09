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
use std::time::Duration;
use traces::setup_telemetry;
use tracing::info_span;

#[tokio::test]
async fn test_trace_export() {
    // 1. Start the fake collector
    let mut collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    let endpoint = Some(collector.endpoint());

    // 2. Setup Telemetry
    // Note: setup_telemetry sets the global tracing subscriber.
    // This means this test can only be run once per process and cannot run in parallel
    // with other tests that set the global subscriber.
    let provider = setup_telemetry(&endpoint).await.expect("Failed to setup telemetry");

    // 3. Generate Traces
    {
        let _span = info_span!("test_span").entered();
        // Simulate some work
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Force flush to ensure spans are sent to the collector
    let _ = provider.force_flush();

    // 4. Verify Spans
    // We expect at least 1 batch of spans.
    let exported_spans: Vec<ExportedSpan> =
        collector.exported_spans(1, Duration::from_secs(5)).await;

    assert!(!exported_spans.is_empty(), "Should have received at least one span batch");

    // Check if we can find our span
    let found = exported_spans.iter().any(|span| span.name == "test_span");
    assert!(found, "Did not find span with name 'test_span'");

    // Shutdown provider
    let _ = provider.shutdown();
}
