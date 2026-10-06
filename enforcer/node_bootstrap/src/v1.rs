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

use crate::common::{await_setup_isolate, build_tls_configs, launch_ez_to_ez};
use anyhow::{Context, Result};
use container_manager_requester::ContainerManagerRequester;
use inbound_ez_to_ez_handler::InboundEzToEzHandler;
use mtls::mtls::EzMtlsManagerConfig;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use state_manager::IsolateStateManager;
use std::time::Duration;

/// Inputs required to run the EZ Node bootstrap sequence for Manifest V1.
#[derive(Clone, Debug)]
pub struct NodeBootstrapV1Config {
    /// Whether mTLS is enabled for EZ-to-EZ communications.
    pub enable_mtls: bool,
    /// Whether to obtain the EZ-to-EZ mTLS certificate from a remote service through the Setup Isolate.
    pub enable_tls_cert_remote_fetch: bool,
    /// Path to the mTLS control plane UDS socket.
    pub mtls_control_plane_uds_path: Option<String>,
    /// Path to the mTLS private key.
    pub mtls_key_path: Option<String>,
    /// Path to the mTLS leaf CSR.
    pub mtls_leaf_csr_path: Option<String>,
    /// Handshake timeout for incoming TLS connections.
    pub ez_to_ez_handshake_timeout: Duration,
    /// Maximum concurrent incoming TLS handshakes.
    pub ez_to_ez_max_concurrent_handshakes: usize,
    /// Maximum expected gRPC message size for the inbound EZ-to-EZ server.
    pub max_decoding_message_size: usize,
}

/// EZ Node Bootstrapper for Manifest V1.
#[derive(Clone, Debug)]
pub struct NodeBootstrapV1 {
    isolate_state_manager: IsolateStateManager,
    container_manager_requester: ContainerManagerRequester,
    ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>>,
    ez_to_ez_inbound: Option<(InboundEzToEzHandler, String)>,
    config: NodeBootstrapV1Config,
}

impl NodeBootstrapV1 {
    /// Creates a new [`NodeBootstrapV1`].
    pub fn new(
        isolate_state_manager: IsolateStateManager,
        container_manager_requester: ContainerManagerRequester,
        ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>>,
        ez_to_ez_inbound: Option<(InboundEzToEzHandler, String)>,
        config: NodeBootstrapV1Config,
    ) -> Self {
        Self {
            isolate_state_manager,
            container_manager_requester,
            ez_to_ez_outbound_handler,
            ez_to_ez_inbound,
            config,
        }
    }

    /// Runs the bootstrap sequence to completion.
    pub async fn run(&self) -> Result<()> {
        let (inbound_tls_config, outbound_tls_config) = if self.config.enable_mtls {
            let config = if self.config.enable_tls_cert_remote_fetch {
                let setup_client = await_setup_isolate(
                    &self.isolate_state_manager,
                    &self.container_manager_requester,
                )
                .await?;
                EzMtlsManagerConfig::new_with_setup_isolate(setup_client)
            } else {
                let proxy_address = self.config.mtls_control_plane_uds_path.as_ref().context(
                    "mTLS enabled but mtls_control_plane_uds_path is missing. mTLS must be properly fetched.",
                )?;
                let key_path = self
                    .config
                    .mtls_key_path
                    .as_ref()
                    .context("mTLS enabled but mtls_key_path is missing.")?;
                let csr_path = self
                    .config
                    .mtls_leaf_csr_path
                    .as_ref()
                    .context("mTLS enabled but mtls_leaf_csr_path is missing.")?;
                EzMtlsManagerConfig::new_with_proxy(
                    key_path.clone(),
                    csr_path.clone(),
                    proxy_address.clone(),
                )
            };
            let (inbound_tls, outbound_tls) = build_tls_configs(
                config,
                self.config.ez_to_ez_handshake_timeout,
                self.config.ez_to_ez_max_concurrent_handshakes,
            )
            .await?;
            (Some(inbound_tls), Some(outbound_tls))
        } else {
            (None, None)
        };

        launch_ez_to_ez(
            &self.ez_to_ez_outbound_handler,
            self.ez_to_ez_inbound.clone(),
            self.config.max_decoding_message_size,
            inbound_tls_config,
            outbound_tls_config,
        )?;

        Ok(())
    }
}
