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

use anyhow::Context;
use clap::Parser;
use container_custom::ContainerCustom;
use container_manager::{ContainerManager, ContainerManagerArgs};
use container_manager_requester::ContainerManagerRequester;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::requester::DataScopeRequester;
use external_proxy_connector::ExternalProxyChannel;
use external_proxy_connector::ExternalProxyConnector;
use ez_service_proto::enforcer::v1::ez_public_api_server::EzPublicApiServer;
use health_manager::HealthManager;
use inbound_ez_to_ez_handler::InboundEzToEzHandler;
use interceptor::Interceptor;
use isolate_ez_service_manager::{IsolateEzServiceManager, IsolateEzServiceManagerDependencies};
use isolate_service_mapper::IsolateServiceMapper;
use junction::IsolateJunction;
use logging::logger;
use manifest_parser::parse_isolate_runtime_configs;
use metrics::setup_otel_metrics;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use outbound_ez_to_ez_handler::OutboundEzToEzHandler;
use public_api::EzPublicApiService;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::env;
use std::ffi::CStr;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

const PUBLIC_API_PORT_ENV_VAR: &str = "EZ_PUBLIC_API_PORT";
const ISOLATE_MNGR_REQUEST_CHANNEL_SIZE: usize = 1024;

#[derive(Parser, Debug)]
#[command(version, about)]
struct EnforcerInputs {
    /// Path to EZ Manifest Json file representing EzManifest proto
    #[arg(short = 'm', long, required = true)]
    manifest_path: String,
    /// Network host to listen on
    #[arg(short = 'a', long, default_value = "[::1]")]
    host: String,
    /// Port to listen on (overridden by EZ_PUBLIC_API_PORT env variable if set)
    #[arg(short = 'p', long, default_value_t = 53459)]
    port: u64,
    /// OTel Safe collector endpoint
    #[arg(long)]
    otel_safe_endpoint: Option<String>,
    /// OTel Unsafe collector endpoint
    #[arg(long)]
    otel_unsafe_endpoint: Option<String>,
    /// OTel traces endpoint
    #[arg(long)]
    otel_traces_endpoint: Option<String>,
    /// Bind mount for all Isolates
    #[arg(
        short = 'b',
        long,
        help = "Bind mount for all isolates with format 'host_path:isolate_path'. This may be replaced in the future."
    )]
    common_bind_mount: Vec<String>,
    #[arg(
        long,
        default_value_t = 0,
        help = "Threshold for retiring an isolate after sensitive sessions. A value of 0 means the isolate will never be retired. To be deprecated in the future. "
    )]
    sensitive_session_threshold: u64,
    #[arg(
        long,
        required = false,
        help = "Proxy address for outbound requests from EZ. Supports both UDS and network addresses. This should be a fully qualified gRPC URI."
    )]
    ez_to_external_address: Option<String>,
    #[arg(
        long,
        required = false,
        help = "UDS address for inbound requests from external (non-EZ) services. Must start with 'unix:'. If set, the 'port' flag is ignored."
    )]
    external_to_ez_address: Option<String>,
    #[arg(
        long,
        required = false,
        help = "UDS address for the inbound EZ-to-EZ service to listen on. Must start with 'unix:'."
    )]
    ez_to_ez_inbound_address: Option<String>,
    #[arg(
        long,
        required = false,
        help = "UDS address for the outbound handler to communicate with proxy for EZ-to-EZ service. Must start with 'unix:'."
    )]
    ez_to_ez_outbound_address: Option<String>,
    // Note: all services running in an EZ instance will use the same PublicApi
    // and IsolateEzBridge channels, set this value according to the largest
    // expected message size for all services in your EZ instance
    #[arg(
        short = 'd',
        long,
        default_value_t = 4 * 1024 * 1024, // 4MiB
        help = "Maximum expected message size in bytes for all services running in this EZ instance"
    )]
    max_decoding_message_size: usize,
    #[arg(
        long,
        help = "Sets the container arguments of selected isolates. Expect JSON format. See enforcer/ez_manifest.proto:IsolateRuntimeConfigs for details",
        default_value = ""
    )]
    isolate_runtime_configs: String,
    #[arg(
        long,
        default_value_t = 10,
        help = "Interval in seconds for the HealthManager to check isolates. A value of 0 means the health manager is disabled."
    )]
    health_manager_interval_secs: u64,
}

enum Endpoint {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

fn main() -> anyhow::Result<()> {
    setup_namespace()?;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let enforcer_inputs = EnforcerInputs::parse();
        logger::setup_logging()?;

        let _otel_providers = setup_otel_metrics(
            enforcer_inputs.otel_safe_endpoint,
            enforcer_inputs.otel_unsafe_endpoint,
        )
        .await?;

        let _otel_traces = traces::setup_telemetry(&enforcer_inputs.otel_traces_endpoint).await?;

        let data_scope_requester =
            DataScopeRequester::new(enforcer_inputs.sensitive_session_threshold);
        let isolate_service_mapper = IsolateServiceMapper::default();
        let interceptor: Interceptor = Interceptor::new(isolate_service_mapper.clone());
        let manifest_validator = ManifestValidator::default();

        let (container_manager_request_tx, container_manager_request_rx) =
            tokio::sync::mpsc::channel(ISOLATE_MNGR_REQUEST_CHANNEL_SIZE);
        let container_manager_requester =
            ContainerManagerRequester::new(container_manager_request_tx);

        let isolate_state_manager = IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        );
        let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone());
        let health_manager = HealthManager::new(
            isolate_state_manager.clone(),
            container_manager_requester.clone(),
            isolate_service_mapper.clone(),
            data_scope_requester.clone(),
        );
        if enforcer_inputs.health_manager_interval_secs > 0 {
            health_manager.run_in_background(tokio::time::Duration::from_secs(
                enforcer_inputs.health_manager_interval_secs,
            ));
        }

        let isolate_junction = IsolateJunction::new(
            data_scope_requester.clone(),
            isolate_service_mapper.clone(),
            shared_memory_manager.clone(),
            isolate_state_manager.clone(),
            manifest_validator.clone(),
        );
        let max_decoding_message_size = enforcer_inputs.max_decoding_message_size;

        // Start Public API server in background
        let ez_public_api = EzPublicApiService::new(
            Box::new(isolate_junction.clone()),
            interceptor.clone(),
            health_manager.clone(),
        )
        .await;
        let socket_addr_str = enforcer_inputs.external_to_ez_address.unwrap_or_else(|| {
            let host = enforcer_inputs.host;
            let port =
                env::var(PUBLIC_API_PORT_ENV_VAR).unwrap_or(enforcer_inputs.port.to_string());
            format!("{host}:{port}")
        });

        let public_api_handle = tokio::spawn(async move {
            launch_ez_public_api_server(ez_public_api, &socket_addr_str, max_decoding_message_size)
                .await;
        });

        // Start the inbound EZ-to-EZ gRPC server in the background.
        let inbound_ez_to_ez_handler =
            InboundEzToEzHandler::new(Box::new(isolate_junction.clone()));
        let inbound_ez_to_ez_api_handle = enforcer_inputs.ez_to_ez_inbound_address.map(|address| {
            tokio::spawn(async move {
                inbound_ez_to_ez_handler::launch_server(
                    inbound_ez_to_ez_handler,
                    &address,
                    max_decoding_message_size,
                )
                .await;
            })
        });

        let external_proxy_connector: Option<Box<dyn ExternalProxyChannel>> =
            if let Some(proxy_address) = enforcer_inputs.ez_to_external_address {
                Some(Box::new(
                    ExternalProxyConnector::new(proxy_address, max_decoding_message_size).await?,
                ))
            } else {
                None
            };

        let ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>> =
            if let Some(ez_to_ez_outbound_address) = enforcer_inputs.ez_to_ez_outbound_address {
                Some(Box::new(OutboundEzToEzHandler::new(ez_to_ez_outbound_address).await?))
            } else {
                None
            };

        let manager_deps = IsolateEzServiceManagerDependencies {
            isolate_junction: Box::new(isolate_junction.clone()),
            isolate_state_manager: isolate_state_manager.clone(),
            shared_memory_manager: shared_memory_manager.clone(),
            external_proxy_connector,
            isolate_service_mapper: isolate_service_mapper.clone(),
            manifest_validator: manifest_validator.clone(),
            data_scope_requester: data_scope_requester.clone(),
            ez_to_ez_outbound_handler,
            max_decoding_message_size,
        };

        let isolate_ez_service_mngr = IsolateEzServiceManager::new(manager_deps);

        let container_manager_args = ContainerManagerArgs {
            isolate_junction: Box::new(isolate_junction.clone()),
            container_manager_request_rx,
            isolate_state_manager,
            isolate_service_mapper,
            isolate_ez_service_mngr,
            manifest_validator,
            shared_mem_manager: shared_memory_manager,
            manifest_path: enforcer_inputs.manifest_path,
            common_bind_mounts: enforcer_inputs.common_bind_mount.clone(),
            max_decoding_message_size,
            isolate_runtime_configs: parse_isolate_runtime_configs(
                enforcer_inputs.isolate_runtime_configs,
            )
            .context("failed to parse isolate config configs")?,
            interceptor,
        };

        let mut container_manager =
            ContainerManager::<ContainerCustom>::start(container_manager_args)
                .await
                .context("container manager failed")?;

        // Clean-up when Enforcer receives SIGINT
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.expect("Could not add SIGINT handler");
            log::info!("ctrl-c received! Cleaning-up...");
            container_manager.stop().await;
            std::process::exit(0); // Without this, Enforcer will never end.
        });

        let _ = public_api_handle.await;
        if let Some(handle) = inbound_ez_to_ez_api_handle {
            let _ = handle.await;
        }
        let _ = _otel_traces.shutdown();
        Ok::<_, anyhow::Error>(())
    })?;
    Ok(())
}

async fn launch_ez_public_api_server(
    ez_public_api: EzPublicApiService,
    socket_addr_str: &str,
    max_decoding_message_size: usize,
) {
    let endpoint = if let Some(path_str) = socket_addr_str.strip_prefix("unix:") {
        Ok(Endpoint::Unix(PathBuf::from(path_str)))
    } else {
        socket_addr_str.parse().map(Endpoint::Tcp).map_err(|e| e.into())
    }
    .unwrap_or_else(|e: Box<dyn std::error::Error>| {
        panic!("Failed to parse PublicApi socket address '{socket_addr_str}': {e}")
    });

    let server_builder = Server::builder()
        .trace_fn(|req| tracing::info_span!("EzPublicApi", method = %req.uri().path()))
        .add_service(
            EzPublicApiServer::new(ez_public_api)
                .max_decoding_message_size(max_decoding_message_size),
        );

    let result = match endpoint {
        Endpoint::Tcp(addr) => {
            log::info!("PublicApi Server listening on TCP: {}", addr);
            server_builder.serve(addr).await
        }
        Endpoint::Unix(path) => {
            // Remove the socket file if it already exists
            // This is crucial for UDS to avoid "address in use" errors on restart
            let _ = std::fs::remove_file(&path);

            log::info!("PublicApi Server listening on UDS: {}", path.display());

            let uds = UnixListener::bind(&path)
                .unwrap_or_else(|e| panic!("Failed to bind UDS at '{}': {}", path.display(), e));

            let uds_stream = UnixListenerStream::new(uds);
            server_builder.serve_with_incoming(uds_stream).await
        }
    };

    if result.is_err() {
        panic!("PublicApi Server launch failed {:?}", result.err());
    }
}

fn setup_namespace() -> anyhow::Result<()> {
    // Get the `uid` and `gid` of the process before creating a new user
    // namespace.
    let uid = users::get_current_uid();
    let gid = users::get_current_gid();

    // Create a new user, mount and cgroup namespaces.
    unsafe {
        let r = libc::unshare(libc::CLONE_NEWUSER | libc::CLONE_NEWNS | libc::CLONE_NEWCGROUP);
        if r != 0 {
            anyhow::bail!(
                "unshare failed: {:#?}",
                CStr::from_ptr(libc::strerror(*libc::__errno_location()))
            );
        }
    };

    // Disable `setgroups` is required to set {uid|gid} mappings.
    let mut file = OpenOptions::new().write(true).open("/proc/self/setgroups")?;
    file.write_all(b"deny")?;

    // Map uid 0 - root - to the uid running the program.
    let mut file = OpenOptions::new().write(true).open("/proc/self/uid_map")?;
    file.write_all(format!("0 {uid} 1").as_bytes())?;

    // Map gid 0 to the gid running the program.
    let mut file = OpenOptions::new().write(true).open("/proc/self/gid_map")?;
    file.write_all(format!("0 {gid} 1").as_bytes())?;

    Ok(())
}
