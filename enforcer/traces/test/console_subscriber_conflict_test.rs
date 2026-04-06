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

use std::time::Duration;
use tokio::net::TcpListener;
use traces::setup_telemetry;

#[tokio::test]
async fn test_console_subscriber_bind_conflict() {
    // 1. Manually bind to a port to create a conflict
    let listener =
        TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, 0)).await.expect("Failed to bind a port");
    let port = listener.local_addr().unwrap().port();

    // 2. Setup telemetry with the same port.
    // This should NOT panic and should return Ok, even though it will log an error in the background.
    let setup_result = setup_telemetry(&None, &Some(port)).await;

    assert!(setup_result.is_ok(), "setup_telemetry should return Ok even if port is in use");

    // 3. Give the background task a moment to attempt binding and fail
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The test succeeds if it reaches this point without a panic.
    // The previous implementation would have panicked in a hidden thread at this point.
}
