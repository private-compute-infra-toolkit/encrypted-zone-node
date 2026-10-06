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
use ez_management::{EzManagementClient, OperatorInfo};
use inbound_ez_to_ez_handler::InboundEzToEzHandler;
use isolate_info::BinaryServicesIndex;
use mtls::mtls::EzMtlsManagerConfig;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use state_manager::IsolateStateManager;
use std::time::Duration;

/// Inputs required to run the EZ Node bootstrap sequence for Manifest V2.
#[derive(Clone, Debug)]
pub struct NodeBootstrapV2Config {
    /// Address of the EzManagementService. Supports both UDS and network ports.
    pub ez_management_address: String,
    /// Maximum expected gRPC message size for the EzManagementService stream.
    pub max_decoding_message_size: usize,
    /// Whether mTLS is enabled for EZ-to-EZ communications.
    pub enable_mtls: bool,
    /// Handshake timeout for incoming TLS connections.
    pub ez_to_ez_handshake_timeout: Duration,
    /// Maximum concurrent incoming TLS handshakes.
    pub ez_to_ez_max_concurrent_handshakes: usize,
}

/// EZ Node Bootstrapper for Manifest V2.
#[derive(Clone)]
pub struct NodeBootstrapV2 {
    isolate_state_manager: IsolateStateManager,
    container_manager_requester: ContainerManagerRequester,
    ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>>,
    ez_to_ez_inbound: Option<(InboundEzToEzHandler, String)>,
    config: NodeBootstrapV2Config,
    ez_management_client: EzManagementClient,
}

impl NodeBootstrapV2 {
    /// Creates a new [`NodeBootstrapV2`], connecting to the EzManagementService.
    pub async fn new(
        isolate_state_manager: IsolateStateManager,
        container_manager_requester: ContainerManagerRequester,
        ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>>,
        ez_to_ez_inbound: Option<(InboundEzToEzHandler, String)>,
        config: NodeBootstrapV2Config,
    ) -> Result<Self> {
        let ez_management_client = EzManagementClient::new(
            &config.ez_management_address,
            container_manager_requester.clone(),
            config.max_decoding_message_size,
        )
        .await
        .context("Failed to create EzManagementClient")?;
        Ok(Self {
            isolate_state_manager,
            container_manager_requester,
            ez_to_ez_outbound_handler,
            ez_to_ez_inbound,
            config,
            ez_management_client,
        })
    }

    /// Fetches the node-level [`OperatorInfo`]. Must complete before the Setup Isolate is booted.
    pub async fn fetch_operator_info(&self) -> Result<OperatorInfo> {
        self.ez_management_client
            .fetch_operator_info()
            .await
            .context("Failed to fetch OperatorInfo from EzManagementService")
    }

    /// Runs the bootstrap sequence to completion.
    ///
    /// Returns the [`BinaryServicesIndex`] of every workload Isolate that was loaded.
    pub async fn run(self) -> Result<Vec<BinaryServicesIndex>> {
        let setup_client =
            await_setup_isolate(&self.isolate_state_manager, &self.container_manager_requester)
                .await?;

        let (inbound_tls_config, outbound_tls_config) = if self.config.enable_mtls {
            let (inbound_tls, outbound_tls) = build_tls_configs(
                EzMtlsManagerConfig::new_with_setup_isolate(setup_client),
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

        let loaded_indices = self.load_isolate_packages().await?;
        log::info!("Loaded {} workload Isolate package(s).", loaded_indices.len());
        Ok(loaded_indices)
    }

    /// Starts EzManagementService interactions to load workload isolates.
    async fn load_isolate_packages(mut self) -> Result<Vec<BinaryServicesIndex>> {
        self.ez_management_client
            .load_packages()
            .await
            .context("Failed to load Isolate packages from EzManagementService")
    }
}
