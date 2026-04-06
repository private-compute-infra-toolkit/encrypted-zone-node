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

use console_api::instrument::instrument_client::InstrumentClient;
use console_api::instrument::InstrumentRequest;
use std::time::Duration;
use tokio::net::TcpListener;
use traces::setup_telemetry;

async fn find_free_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

#[tokio::test]
async fn test_console_subscriber_connection() {
    // 1. Find a free port
    let port = find_free_port().await.expect("Failed to find free port");

    let console_endpoint = format!("[{}]:{}", std::net::Ipv6Addr::LOCALHOST, port);

    // 2. Setup Telemetry with Console Subscriber
    // Note: setup_telemetry sets the global tracing subscriber.
    // This means this test can only be run once per process and cannot run in parallel
    // with other tests that set the global subscriber.
    let _provider = setup_telemetry(&None, &Some(port)).await.expect("Failed to setup telemetry");

    // TODO: wait until the server starts, rather than a fixed duration
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 3. Connect as a Client
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", console_endpoint))
        .unwrap()
        .connect()
        .await
        .expect("Failed to connect to console subscriber");

    let mut client = InstrumentClient::new(channel);

    // 4. Send a Request (InstrumentRequest is usually empty for initial connection)
    let request = tonic::Request::new(InstrumentRequest {});
    let response = client.watch_updates(request).await;

    // We expect a successful stream response
    assert!(response.is_ok(), "Failed to establish watch stream: {:?}", response.err());

    // 5. Verify we get at least one update
    let mut stream = response.unwrap().into_inner();
    let update = tokio::time::timeout(Duration::from_secs(5), stream.message()).await;

    assert!(update.is_ok(), "Timed out waiting for update");
    let update_result = update.unwrap();
    assert!(update_result.is_ok(), "Stream error: {:?}", update_result.err());
    assert!(update_result.unwrap().is_some(), "Stream closed unexpectedly");
}
