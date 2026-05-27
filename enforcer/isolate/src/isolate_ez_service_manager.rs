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
use metrics::receiver::IsolateMetricsReceiver;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::sync::Arc;
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
    pub isolate_name: String,
    pub publisher_id: String,
}

impl IsolateEzServiceManager {
    pub fn new(deps: IsolateEzServiceManagerDependencies) -> Self {
        Self { isolate_server_map: Arc::new(DashMap::new()), deps }
    }

    /// Start a new IsolateEzService on the provided UDS address.
    pub async fn start_isolate_ez_server(&self, args: StartIsolateEzServerArgs) {
        let service_mngr_clone = self.clone();
        let shm_payload_threshold = self.deps.shm_payload_threshold;
        let isolate_ez_handle = tokio::spawn(async move {
            service_mngr_clone
                .create_isolate_ez_server(
                    args.isolate_id,
                    args.isolate_address,
                    args.isolate_fifo_path,
                    shm_payload_threshold,
                )
                .await;
        });

        let service_mngr_clone = self.clone();
        let is_ratified = args.isolate_id.is_ratified_isolate();
        let otel_metrics_handle = tokio::spawn(async move {
            service_mngr_clone
                .create_otel_metrics_server(
                    args.otel_metrics_address,
                    args.metrics_policy,
                    args.isolate_name,
                    args.publisher_id,
                    is_ratified,
                )
                .await;
        });

        let handles = IsolateServerHandles { isolate_ez_handle, otel_metrics_handle };
        self.isolate_server_map.insert(args.isolate_id, handles);
    }

    /// Stops the IsolateEzService server for the provided Isolate.
    pub async fn stop_isolate_servers(&self, isolate_id: IsolateId) {
        let server_handles_option = self.isolate_server_map.remove(&isolate_id);
        if let Some((_isolate_id, server_handles)) = server_handles_option {
            server_handles.isolate_ez_handle.abort();
            let _ = server_handles.isolate_ez_handle.await;

            server_handles.otel_metrics_handle.abort();
            let _ = server_handles.otel_metrics_handle.await;
        }
    }

    async fn create_isolate_ez_server(
        &self,
        isolate_id: IsolateId,
        isolate_address: String,
        isolate_fifo_path: String,
        shm_payload_threshold: u64,
    ) {
        let uds_result = UnixListener::bind(&isolate_address);
        let uds = uds_result
            .unwrap_or_else(|_| panic!("Failed to bind to Isolate EZ UDS: {}", isolate_address));
        let uds_stream = UnixListenerStream::new(uds);

        // Once the UDS server is listening, the client can connect. In a separate task,
        // open the fifo for write which blocks until the read end is opened by the Isolate.
        // No need to await on this blocking call because the server can continue to start-up.
        task::spawn_blocking(move || {
            if let Err(e) = std::fs::OpenOptions::new().write(true).open(isolate_fifo_path) {
                log::error!("IsolateEzBridgeService Server signaling failed {:?}", e);
            }
        });

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
        address: String,
        policy: IsolateMetricsPolicy,
        isolate_name: String,
        publisher_id: String,
        is_ratified: bool,
    ) {
        let uds_result = UnixListener::bind(&address);
        let uds = uds_result.expect("Failed to bind to OTel metrics UDS");
        let uds_stream = UnixListenerStream::new(uds);

        let receiver = IsolateMetricsReceiver::new(
            policy,
            isolate_name,
            publisher_id,
            is_ratified,
            self.deps.otel_endpoint.clone(),
            self.deps.max_decoding_message_size,
            self.deps.disable_metrics_filtering,
        )
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
