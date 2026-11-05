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

use common_proto::enforcer::v2::IsolateType;
use dashmap::DashMap;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::requester::DataScopeRequester;
use enforcer_proto::enforcer::v1::isolate_ez_bridge_server::IsolateEzBridgeServer;
use external_proxy_connector::ExternalProxyChannel;
use fileshare_manager::FileshareManager;
use isolate_ez_service::{IsolateEzBridgeDependencies, IsolateEzBridgeService};
use isolate_info::IsolateId;
use isolate_service_mapper::IsolateServiceMapper;
use junction_trait::Junction;
use manifest_proto::enforcer::v1::IsolateMetricsPolicy;
use metrics::isolate_metrics_receiver::{IsolateMetricsReceiver, IsolateMetricsReceiverConfig};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::UnixListener;
use tokio::task::{self, JoinHandle};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

#[derive(Clone, Debug)]
pub struct IsolateEzServiceManagerDependencies {
    pub isolate_junction: Box<dyn Junction>,
    pub isolate_state_manager: IsolateStateManager,
    pub shared_memory_manager: SharedMemManager,
    pub fileshare_manager: FileshareManager,
    pub external_proxy_connector: Option<Box<dyn ExternalProxyChannel>>,
    pub isolate_service_mapper: IsolateServiceMapper,
    pub ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>>,
    pub manifest_validator: ManifestValidator,
    pub data_scope_requester: DataScopeRequester,
    pub max_decoding_message_size: usize,
    pub interceptor: interceptor::Interceptor,
    pub otel_endpoint: Option<String>,
    pub disable_metrics_filtering: bool,
    pub shm_payload_threshold: u64,
}

#[derive(Debug)]
struct IsolateServerHandles {
    isolate_ez_handle: JoinHandle<()>,
    otel_metrics_handle: JoinHandle<()>,
    isolate_address: String,
    otel_metrics_address: String,
    ready_fifo_cancelled: Arc<AtomicBool>,
    ready_fifo_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    isolate_fifo_path: String,
    fifo_opath_fd: Option<Arc<OwnedFd>>,
}

fn bind_uds_listener(address: &str) -> std::io::Result<UnixListenerStream> {
    match std::fs::remove_file(address) {
        Ok(()) => log::warn!("Removed pre-existing UDS socket file before bind: {address}"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => log::warn!("Failed to remove pre-existing UDS socket file {address}: {e:?}"),
    }
    Ok(UnixListenerStream::new(UnixListener::bind(address)?))
}

/// Manager to maintain IsolateEzService servers for each Isolate. Enforcer has
/// a unique UDS for each Isolate for this service and hence a unique server for each Isolate.
#[derive(Clone, Debug)]
pub struct IsolateEzServiceManager {
    isolate_server_map: Arc<DashMap<IsolateId, IsolateServerHandles>>,
    deps: IsolateEzServiceManagerDependencies,
}

pub struct StartIsolateEzServerArgs {
    pub isolate_id: IsolateId,
    pub isolate_address: String,
    pub isolate_fifo_path: String,
    pub otel_metrics_address: String,
    pub metrics_policy: IsolateMetricsPolicy,
    pub isolate_type: IsolateType,
    pub isolate_instance_id: String,
}

impl IsolateEzServiceManager {
    pub fn new(deps: IsolateEzServiceManagerDependencies) -> Self {
        Self { isolate_server_map: Arc::new(DashMap::new()), deps }
    }

    /// Start a new IsolateEzService on the provided UDS address.
    pub async fn start_isolate_ez_server(
        &self,
        args: StartIsolateEzServerArgs,
    ) -> std::io::Result<()> {
        let isolate_uds_stream = bind_uds_listener(&args.isolate_address)?;

        let metrics_uds_stream = match bind_uds_listener(&args.otel_metrics_address) {
            Ok(uds_stream) => uds_stream,
            Err(e) => {
                let _ = std::fs::remove_file(&args.isolate_address);
                return Err(e);
            }
        };

        let ready_fifo_cancelled = Arc::new(AtomicBool::new(false));
        let ready_fifo_handle = Arc::new(Mutex::new(None));
        let fifo_opath_fd = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_PATH | nix::libc::O_CLOEXEC)
            .open(&args.isolate_fifo_path)
            .ok()
            .map(|file| Arc::new(OwnedFd::from(file)));

        let service_mngr_clone = self.clone();
        let shm_payload_threshold = self.deps.shm_payload_threshold;
        let cancelled_for_task = Arc::clone(&ready_fifo_cancelled);
        let fifo_handle_for_task = Arc::clone(&ready_fifo_handle);
        let fifo_opath_for_task = fifo_opath_fd.clone();
        let fifo_path_for_task = args.isolate_fifo_path.clone();
        let isolate_ez_handle = tokio::spawn(async move {
            service_mngr_clone
                .create_isolate_ez_server(
                    args.isolate_id,
                    isolate_uds_stream,
                    fifo_path_for_task,
                    fifo_opath_for_task,
                    cancelled_for_task,
                    fifo_handle_for_task,
                    shm_payload_threshold,
                )
                .await;
        });

        let service_mngr_clone = self.clone();
        let is_ratified = args.isolate_id.is_ratified_isolate();
        let otel_metrics_handle = tokio::spawn(async move {
            service_mngr_clone
                .create_otel_metrics_server(
                    metrics_uds_stream,
                    args.metrics_policy,
                    args.isolate_type.isolate_name,
                    args.isolate_type.publisher_id,
                    is_ratified,
                    args.isolate_instance_id,
                )
                .await;
        });

        let handles = IsolateServerHandles {
            isolate_ez_handle,
            otel_metrics_handle,
            isolate_address: args.isolate_address,
            otel_metrics_address: args.otel_metrics_address,
            ready_fifo_cancelled,
            ready_fifo_handle,
            isolate_fifo_path: args.isolate_fifo_path,
            fifo_opath_fd,
        };
        self.isolate_server_map.insert(args.isolate_id, handles);
        Ok(())
    }

    /// Stops the IsolateEzService server for the provided Isolate.
    pub async fn stop_isolate_servers(&self, isolate_id: IsolateId) {
        let server_handles_option = self.isolate_server_map.remove(&isolate_id);
        if let Some((_isolate_id, server_handles)) = server_handles_option {
            server_handles.ready_fifo_cancelled.store(true, Ordering::SeqCst);
            server_handles.isolate_ez_handle.abort();
            let _ = server_handles.isolate_ez_handle.await;
            let _ = std::fs::remove_file(&server_handles.isolate_address);

            let fifo_handle_opt =
                server_handles.ready_fifo_handle.lock().unwrap_or_else(|e| e.into_inner()).take();
            if let Some(fifo_handle) = fifo_handle_opt {
                let _reader_guard = if !fifo_handle.is_finished() {
                    let open_target = server_handles
                        .fifo_opath_fd
                        .as_ref()
                        .map(|fd| format!("/proc/self/fd/{}", fd.as_raw_fd()))
                        .unwrap_or_else(|| server_handles.isolate_fifo_path.clone());
                    std::fs::OpenOptions::new()
                        .read(true)
                        .custom_flags(nix::libc::O_NONBLOCK)
                        .open(open_target)
                        .ok()
                } else {
                    None
                };
                let _ = fifo_handle.await;
            }

            server_handles.otel_metrics_handle.abort();
            let _ = server_handles.otel_metrics_handle.await;
            let _ = std::fs::remove_file(&server_handles.otel_metrics_address);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_isolate_ez_server(
        &self,
        isolate_id: IsolateId,
        uds_stream: UnixListenerStream,
        isolate_fifo_path: String,
        fifo_opath_fd: Option<Arc<OwnedFd>>,
        ready_fifo_cancelled: Arc<AtomicBool>,
        ready_fifo_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
        shm_payload_threshold: u64,
    ) {
        // Once the UDS server is listening, the client can connect. In a separate task,
        // open the fifo for write which blocks until the read end is opened by the Isolate.
        // No need to await on this blocking call because the server can continue to start-up.
        if !ready_fifo_cancelled.load(Ordering::SeqCst) {
            let handle = task::spawn_blocking(move || {
                if ready_fifo_cancelled.load(Ordering::SeqCst) {
                    return;
                }
                let open_target = fifo_opath_fd
                    .as_ref()
                    .map(|fd| format!("/proc/self/fd/{}", fd.as_raw_fd()))
                    .unwrap_or(isolate_fifo_path);
                if let Err(e) = std::fs::OpenOptions::new().write(true).open(open_target) {
                    log::error!("IsolateEzBridgeService Server signaling failed {:?}", e);
                }
            });
            *ready_fifo_handle.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
        }

        let bridge_deps = IsolateEzBridgeDependencies {
            isolate_id,
            isolate_junction: self.deps.isolate_junction.clone(),
            isolate_state_manager: self.deps.isolate_state_manager.clone(),
            shared_memory_manager: self.deps.shared_memory_manager.clone(),
            fileshare_manager: self.deps.fileshare_manager.clone(),
            external_proxy_connector: self.deps.external_proxy_connector.clone(),
            isolate_service_mapper: self.deps.isolate_service_mapper.clone(),
            manifest_validator: self.deps.manifest_validator.clone(),
            data_scope_requester: self.deps.data_scope_requester.clone(),
            ez_to_ez_outbound_handler: self.deps.ez_to_ez_outbound_handler.clone(),
            interceptor: self.deps.interceptor.clone(),
            shm_payload_threshold,
        };

        let isolate_ez_service = IsolateEzBridgeService::new(bridge_deps);
        let result = Server::builder()
            .add_service(
                IsolateEzBridgeServer::new(isolate_ez_service)
                    .max_decoding_message_size(self.deps.max_decoding_message_size),
            )
            .serve_with_incoming(uds_stream)
            .await;
        if let Err(e) = result {
            log::error!("IsolateEzBridgeService Server error: {:?}", e);
        }
    }

    async fn create_otel_metrics_server(
        &self,
        uds_stream: UnixListenerStream,
        policy: IsolateMetricsPolicy,
        isolate_name: String,
        publisher_id: String,
        is_ratified: bool,
        isolate_instance_id: String,
    ) {
        let receiver = IsolateMetricsReceiver::new(IsolateMetricsReceiverConfig {
            policy,
            isolate_name,
            publisher_id,
            is_ratified,
            isolate_instance_id,
            otel_endpoint: self.deps.otel_endpoint.clone(),
            max_decoding_message_size: self.deps.max_decoding_message_size,
            disable_filtering: self.deps.disable_metrics_filtering,
            ..Default::default()
        })
        .await
        .expect("Failed to create IsolateMetricsReceiver");
        let max_decoding_message_size = self.deps.max_decoding_message_size;
        log::info!("Starting OTel metrics server task");
        let result = Server::builder()
            .add_service(
                MetricsServiceServer::new(receiver)
                    .max_decoding_message_size(max_decoding_message_size),
            )
            .serve_with_incoming(uds_stream)
            .await;
        log::info!("OTel metrics server task finished with result: {:?}", result);
        if let Err(e) = result {
            log::error!("OTel Metrics Server error: {:?}", e);
        }
    }
}
