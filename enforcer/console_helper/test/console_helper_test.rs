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

use console_helper::ConsoleHelperService;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use traces::setup_telemetry;

async fn find_free_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

#[tokio::test]
async fn test_console_helper_fetches_task_statistics() {
    // 1. Find a free port for the tokio-console backend
    let port = find_free_port().await.expect("Failed to find free port");

    let console_endpoint = format!("http://[{}]:{}", std::net::Ipv6Addr::LOCALHOST, port);

    // 2. Start the tokio-console publisher internally using our traces module
    let _provider = setup_telemetry(traces::ENFORCER_SERVICE_NAME, &None, &Some(port), 1.0)
        .await
        .expect("Failed to setup telemetry");

    // TODO: b/491604619 - Replace fixed sleep with dynamic readiness check for tokio-console
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 3. Instantiate our new ConsoleHelperService
    let helper = ConsoleHelperService::new(console_endpoint);

    // 4. Create some arbitrary async tasks so tokio-console has data to publish
    for _ in 0..10 {
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
        });
    }

    // Wait a brief moment for the tasks to register
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Query the task_update_stream
    let stream = helper.task_update_stream();
    tokio::pin!(stream);

    // 6. Assertions - collect at least one update
    let mut found = false;
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            update = stream.next() => {
                if let Some(u) = update {
                    println!("Received update for task {}: {}", u.task_id, u.location);
                    found = true;
                    break;
                }
            }
            _ = &mut timeout => {
                break;
            }
        }
    }

    assert!(found, "Failed to receive any task updates from the stream");
}
