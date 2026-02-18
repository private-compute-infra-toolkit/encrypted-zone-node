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

use dashmap::DashMap;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::requester::DataScopeRequester;
use enforcer_proto::enforcer::v1::isolate_ez_bridge_server::IsolateEzBridgeServer;
use external_proxy_connector::ExternalProxyChannel;
use isolate_ez_service::{IsolateEzBridgeDependencies, IsolateEzBridgeService};
use isolate_info::IsolateId;
use isolate_service_mapper::IsolateServiceMapper;
use junction_trait::Junction;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::fs::OpenOptions;
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
    pub external_proxy_connector: Option<Box<dyn ExternalProxyChannel>>,
    pub isolate_service_mapper: IsolateServiceMapper,
    pub ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>>,
    pub manifest_validator: ManifestValidator,
    pub data_scope_requester: DataScopeRequester,
    pub max_decoding_message_size: usize,
    pub interceptor: interceptor::Interceptor,
}

/// Manager to maintain IsolateEzService servers for each Isolate. Enforcer has
/// a unique UDS for each Isolate for this service and hence a unique server for each Isolate.
#[derive(Clone, Debug)]
pub struct IsolateEzServiceManager {
    isolate_server_map: Arc<DashMap<IsolateId, JoinHandle<()>>>,
    deps: IsolateEzServiceManagerDependencies,
}

impl IsolateEzServiceManager {
    pub fn new(deps: IsolateEzServiceManagerDependencies) -> Self {
        Self { isolate_server_map: Arc::new(DashMap::new()), deps }
    }
    /// Start a new IsolateEzService on the provided UDS address.
    pub async fn start_isolate_ez_server(
        &self,
        isolate_id: IsolateId,
        isolate_address: String,
        isolate_fifo_path: String,
    ) {
        let service_mngr_clone = self.clone();
        let server_handle = tokio::spawn(async move {
            service_mngr_clone
                .create_isolate_ez_server(isolate_id, isolate_address, isolate_fifo_path)
                .await;
        });
        self.isolate_server_map.insert(isolate_id, server_handle);
    }

    /// Stops the IsolateEzService server for the provided Isolate.
    pub async fn stop_isolate_ez_server(&self, isolate_id: IsolateId) {
        let server_handle_option = self.isolate_server_map.remove(&isolate_id);
        if let Some((_isolate_id, server_handle)) = server_handle_option {
            server_handle.abort();
        }
    }

    async fn create_isolate_ez_server(
        &self,
        isolate_id: IsolateId,
        isolate_address: String,
        isolate_fifo_path: String,
    ) {
        let uds_result = UnixListener::bind(&isolate_address);
        let Ok(uds) = uds_result else {
            log::error!("Couldn't bind to UDS {isolate_address}");
            return;
        };
        let uds_stream = UnixListenerStream::new(uds);

        // Once the UDS server is listening, the client can connect. In a separate task,
        // open the fifo for write which blocks until the read end is opened by the Isolate.
        // No need to await on this blocking call because the server can continue to start-up.
        task::spawn_blocking(|| {
            if let Err(e) = OpenOptions::new().write(true).open(isolate_fifo_path) {
                log::error!("IsolateEzBridgeService Server signaling failed {:?}", e);
            }
        });

        let bridge_deps = IsolateEzBridgeDependencies {
            isolate_id,
            isolate_junction: self.deps.isolate_junction.clone(),
            isolate_state_manager: self.deps.isolate_state_manager.clone(),
            shared_memory_manager: self.deps.shared_memory_manager.clone(),
            external_proxy_connector: self.deps.external_proxy_connector.clone(),
            isolate_service_mapper: self.deps.isolate_service_mapper.clone(),
            manifest_validator: self.deps.manifest_validator.clone(),
            data_scope_requester: self.deps.data_scope_requester.clone(),
            ez_to_ez_outbound_handler: self.deps.ez_to_ez_outbound_handler.clone(),
            interceptor: self.deps.interceptor.clone(),
        };

        let isolate_ez_service = IsolateEzBridgeService::new(bridge_deps);
        let result = Server::builder()
            .add_service(
                IsolateEzBridgeServer::new(isolate_ez_service)
                    .max_decoding_message_size(self.deps.max_decoding_message_size),
            )
            .serve_with_incoming(uds_stream)
            .await;
        if result.is_err() {
            log::error!("IsolateEzBridgeService Server launch failed {:?}", result.err());
        }
    }
}
