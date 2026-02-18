// Copyright 2025 Google LLC
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

use anyhow::{Context, Result};
use hyper_util::rt::tokio::TokioIo;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

pub const DEFAULT_CONNECT_RETRY_DELAY_MS: u64 = 1000;
pub const DEFAULT_CONNECT_RETRY_COUNT: usize = 30;

/// Establishes a connection to a gRPC service and returns the channel.
///
/// This function supports both TCP and Unix Domain Socket (UDS) connections.
/// It includes a configurable retry mechanism with exponential backoff.
pub async fn connect(address: String, retry_count: usize, retry_delay_ms: u64) -> Result<Channel> {
    log::info!("Attempting to connect to gRPC service at {}...", address);

    let retry_strategy = ExponentialBackoff::from_millis(retry_delay_ms).take(retry_count);

    let channel = if let Some(path) = address.strip_prefix("unix:") {
        connect_uds(path, retry_strategy).await?
    } else {
        connect_tcp(&address, retry_strategy).await?
    };

    log::info!("Successfully connected to gRPC service at {}.", address);
    Ok(channel)
}

/// Handles connecting to a service via a Unix Domain Socket.
async fn connect_uds(
    path: &str,
    retry_strategy: impl Iterator<Item = Duration> + Clone,
) -> Result<Channel> {
    // Normalize path in case it starts with // (e.g. from unix:///path)
    let path = path.strip_prefix("//").unwrap_or(path);
    let socket_path = std::path::PathBuf::from(path);

    let connect_action = || {
        let socket_path = socket_path.clone();
        async move {
            // UDS connections in tonic require a URI, but the host part is ignored.
            // We use http://localhost as a base URI and use the captured socket_path for connecting.
            let endpoint = Endpoint::from_shared("http://localhost".to_string())
                .map_err(|e| anyhow::anyhow!("Invalid UDS URI: {}", e))?;

            // TODO: Make the limit configurable.
            endpoint
                .http2_max_header_list_size(32 * 1024 * 1024)
                .connect_with_connector(service_fn(move |_: Uri| {
                    let socket_path = socket_path.clone();
                    async move {
                        Ok::<_, std::io::Error>(TokioIo::new(
                            UnixStream::connect(socket_path).await?,
                        ))
                    }
                }))
                .await
                .context("Failed to connect to UDS endpoint")
        }
    };

    Retry::spawn(retry_strategy, connect_action).await
}

/// Handles connecting to a service via TCP.
async fn connect_tcp(
    address: &str,
    retry_strategy: impl Iterator<Item = Duration> + Clone,
) -> Result<Channel> {
    let endpoint = Channel::from_shared(address.to_owned())
        .with_context(|| format!("Invalid TCP address URI: {}", address))?;

    let connect_action =
        || async { endpoint.connect().await.context("Failed to connect to TCP endpoint") };

    Retry::spawn(retry_strategy, connect_action).await
}

/// Parses the `grpc-timeout` header from gRPC metadata.
/// Follows the gRPC HTTP/2 spec for timeout encodings.
pub fn try_parse_grpc_timeout(headers: &tonic::metadata::MetadataMap) -> Result<Option<Duration>> {
    match headers.get("grpc-timeout") {
        Some(val) => {
            let s = val.to_str().context("Invalid grpc-timeout header format")?;
            if s.is_empty() {
                anyhow::bail!("grpc-timeout header value is empty");
            }
            if s.len() < 2 {
                anyhow::bail!("Invalid grpc-timeout format: too short");
            }
            let (timeout_value, timeout_unit) = s.split_at(s.len() - 1);
            let parsed_value: u64 = timeout_value
                .parse()
                .with_context(|| format!("Invalid number in grpc-timeout: {timeout_value}"))?;

            match timeout_unit {
                "H" => Ok(Some(Duration::from_secs(parsed_value * 60 * 60))),
                "M" => Ok(Some(Duration::from_secs(parsed_value * 60))),
                "S" => Ok(Some(Duration::from_secs(parsed_value))),
                "m" => Ok(Some(Duration::from_millis(parsed_value))),
                "u" => Ok(Some(Duration::from_micros(parsed_value))),
                "n" => Ok(Some(Duration::from_nanos(parsed_value))),
                _ => anyhow::bail!("Invalid unit in grpc-timeout: {timeout_unit}"),
            }
        }
        None => {
            log::warn!("No grpc-timeout header found");
            Ok(None)
        }
    }
}
