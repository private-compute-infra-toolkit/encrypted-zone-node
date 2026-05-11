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
use boring::ssl::SslConnector;
use hyper_util::rt::tokio::TokioIo;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

pub const DEFAULT_CONNECT_RETRY_DELAY_MS: u64 = 1000;
pub const DEFAULT_CONNECT_RETRY_SCALING: u64 = 2;
pub const DEFAULT_CONNECT_RETRY_COUNT: usize = 30;
pub const DEFAULT_POOL_SIZE: usize = 8;
pub const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 32 * 1024 * 1024;

pub const ENV_PROXY_CONNECT_RETRY_DELAY_MS: &str = "PROXY_CONNECT_RETRY_DELAY_MS";
pub const ENV_PROXY_CONNECT_RETRY_COUNT: &str = "PROXY_CONNECT_RETRY_COUNT";
pub const ENV_PROXY_CONNECT_RETRY_SCALING: &str = "PROXY_CONNECT_RETRY_SCALING";

/// Holds a pool of tonic::Channels to a gRPC server.
/// Provides an interface for round-robin selection of channels.
#[derive(Clone, Debug)]
pub struct GrpcChannelPool {
    channels: Vec<Channel>,
    next_idx: Arc<AtomicUsize>,
}

impl GrpcChannelPool {
    /// Creates a new GrpcChannelPool, establishing a connection to the given address.
    ///
    /// This function supports both TCP and Unix Domain Socket (UDS) connections.
    /// It includes a configurable retry mechanism with exponential backoff.
    pub async fn new(
        address: String,
        pool_size: usize,
        retry_count: usize,
        retry_delay_ms: u64,
        retry_scaling: u64,
    ) -> Result<Self> {
        let mut channels = Vec::new();
        for _ in 0..pool_size {
            channels
                .push(connect(address.clone(), retry_count, retry_delay_ms, retry_scaling).await?);
        }
        Ok(Self { channels, next_idx: Arc::new(AtomicUsize::new(0)) })
    }

    /// Creates a new GrpcChannelPool by reading retry configuration from environment variables.
    pub async fn new_from_env(address: &str) -> Result<Self> {
        let retry_delay_ms = std::env::var(ENV_PROXY_CONNECT_RETRY_DELAY_MS)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECT_RETRY_DELAY_MS);
        let retry_count = std::env::var(ENV_PROXY_CONNECT_RETRY_COUNT)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECT_RETRY_COUNT);
        let retry_scaling = std::env::var(ENV_PROXY_CONNECT_RETRY_SCALING)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECT_RETRY_SCALING);
        Self::new(
            address.to_string(),
            DEFAULT_POOL_SIZE,
            retry_count,
            retry_delay_ms,
            retry_scaling,
        )
        .await
    }

    /// Creates a new GrpcChannelPool with TLS enabled, establishing a connection to the given address.
    ///
    /// This function supports UDS connections with TLS (and will fail for TCP connections with TLS).
    /// It includes a configurable retry mechanism with exponential backoff.
    pub async fn new_tls(
        address: String,
        pool_size: usize,
        retry_count: usize,
        retry_multiplier: u64,
        retry_initial_delay_ms: u64,
        ssl_connector: SslConnector,
        sni: String,
    ) -> Result<Self> {
        let mut channels = Vec::new();
        for _ in 0..pool_size {
            channels.push(
                connect_tls(
                    address.clone(),
                    retry_count,
                    retry_multiplier,
                    retry_initial_delay_ms,
                    ssl_connector.clone(),
                    sni.clone(),
                )
                .await?,
            );
        }
        Ok(Self { channels, next_idx: Arc::new(AtomicUsize::new(0)) })
    }

    /// Creates a new GrpcChannelPool with TLS enabled by reading retry configuration from environment variables.
    pub async fn new_tls_from_env(
        address: &str,
        ssl_connector: SslConnector,
        sni: String,
    ) -> Result<Self> {
        let retry_delay_ms = std::env::var(ENV_PROXY_CONNECT_RETRY_DELAY_MS)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECT_RETRY_DELAY_MS);
        let retry_count = std::env::var(ENV_PROXY_CONNECT_RETRY_COUNT)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECT_RETRY_COUNT);
        let retry_scaling = std::env::var(ENV_PROXY_CONNECT_RETRY_SCALING)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECT_RETRY_SCALING);
        Self::new_tls(
            address.to_string(),
            DEFAULT_POOL_SIZE,
            retry_count,
            retry_scaling,
            retry_delay_ms,
            ssl_connector,
            sni,
        )
        .await
    }

    /// Method to get the next channel in line.
    ///
    /// Utilizes round-robin for load balancing by default.
    pub fn next_channel(&self) -> Channel {
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        self.channels[idx].clone()
    }
}

/// Establishes a connection to a gRPC service and returns the channel.
///
/// This function supports both TCP and Unix Domain Socket (UDS) connections.
/// It includes a configurable retry mechanism with exponential backoff.
async fn connect(
    address: String,
    retry_count: usize,
    retry_delay_ms: u64,
    retry_scaling: u64,
) -> Result<Channel> {
    log::info!("Attempting to connect to gRPC service at {}...", address);

    // TokioRetry's ExponentialBackoff arguments swap the from and factor usage.
    let retry_strategy =
        ExponentialBackoff::from_millis(retry_scaling).factor(retry_delay_ms).take(retry_count);

    let channel = if let Some(path) = address.strip_prefix("unix:") {
        connect_uds(path, retry_strategy).await?
    } else {
        connect_tcp(&address, retry_strategy).await?
    };

    log::info!("Successfully connected to gRPC service at {}.", address);
    Ok(channel)
}

/// Establishes a TLS-enabled connection to a gRPC service and returns the channel.
async fn connect_tls(
    address: String,
    retry_count: usize,
    retry_multiplier: u64,
    initial_delay_ms: u64,
    ssl_connector: SslConnector,
    sni: String,
) -> Result<Channel> {
    log::info!("Attempting to connect to gRPC service (TLS) at {}...", address);

    let retry_strategy = ExponentialBackoff::from_millis(initial_delay_ms)
        .factor(retry_multiplier)
        .take(retry_count);

    let channel = if let Some(path) = address.strip_prefix("unix:") {
        connect_uds_tls(path, retry_strategy, ssl_connector, sni).await?
    } else {
        anyhow::bail!("TLS over TCP is not supported yet.");
    };

    log::info!("Successfully connected to gRPC service (TLS) at {}.", address);
    Ok(channel)
}

/// Creates an endpoint for a Unix Domain Socket with a specified header list size.
fn create_uds_endpoint(header_list_size: u32) -> Result<Endpoint> {
    // UDS connections in tonic require a URI, but the host part is ignored.
    // We use http://localhost as a base URI and use the captured socket_path for connecting.
    let endpoint = Endpoint::from_static("http://localhost");
    Ok(endpoint.http2_max_header_list_size(header_list_size))
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
            create_uds_endpoint(DEFAULT_MAX_HEADER_LIST_SIZE)?
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

/// Handles connecting to a service via a Unix Domain Socket with TLS.
async fn connect_uds_tls(
    path: &str,
    retry_strategy: impl Iterator<Item = Duration> + Clone,
    ssl_connector: SslConnector,
    sni: String,
) -> Result<Channel> {
    // Normalize path to prevent redundant slashes and enable the fallback option
    // (Unix domain socket behavior). Example: "unix:///tmp/socket" or "unix:/tmp/socket"
    let path = path.strip_prefix("//").unwrap_or(path);
    let socket_path = std::path::PathBuf::from(path);

    let connect_action = || {
        let socket_path = socket_path.clone();
        let ssl_connector = ssl_connector.clone();
        let sni = sni.clone();
        async move {
            // The endpoint URI is unused because we use a custom connector.
            // However, it still expects a valid HTTP URI.
            create_uds_endpoint(DEFAULT_MAX_HEADER_LIST_SIZE)?
                .connect_with_connector(service_fn(move |_uri: Uri| {
                    let socket_path = socket_path.clone();
                    let ssl_connector = ssl_connector.clone();
                    let sni = sni.clone();
                    async move {
                        let stream = UnixStream::connect(socket_path).await?;
                        // SNI carries the peer isolate information and not used for hostname verification.
                        // Disabling verify_hostname because SNI will not match the certificate.
                        // ssl_connector.config() is a single-use object and should be called per connection.
                        let tls_stream = tokio_boring::connect(
                            ssl_connector
                                .configure()
                                .map_err(std::io::Error::other)?
                                .verify_hostname(false),
                            &sni,
                            stream,
                        )
                        .await
                        .map_err(std::io::Error::other)?;
                        Ok::<_, std::io::Error>(TokioIo::new(tls_stream))
                    }
                }))
                .await
                .context("Failed to connect to UDS TLS endpoint")
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
