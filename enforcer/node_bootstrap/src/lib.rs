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

//! Orchestrates the runtime startup sequence of an EZ Node using v2 manifests.
//!
//! With v2 manifests the Enforcer boots only the Setup Isolate from disk. Everything else the
//! Node needs is acquired at runtime, in a strict order, by this crate.

use anyhow::{anyhow, Context, Result};
use container_manager_requester::ContainerManagerRequester;
use ez_management::EzManagementClient;
use isolate_info::BinaryServicesIndex;
use state_manager::IsolateStateManager;
use std::time::Duration;
use tokio::time::timeout;

const SETUP_ISOLATE_READY_TIMEOUT: Duration = Duration::from_secs(300);

/// Inputs required to run the EZ Node bootstrap sequence.
#[derive(Clone, Debug)]
pub struct NodeBootstrapConfig {
    /// Address of the EzManagementService. Supports both UDS and network ports.
    pub ez_management_address: String,
    /// Maximum expected gRPC message size for the EzManagementService stream.
    pub max_decoding_message_size: usize,
}

/// EZ Node Bootstrapper. Responsible to orchestrate
#[derive(Clone, Debug)]
pub struct NodeBootstrap {
    isolate_state_manager: IsolateStateManager,
    container_manager_requester: ContainerManagerRequester,
    config: NodeBootstrapConfig,
}

impl NodeBootstrap {
    /// Creates a new [`NodeBootstrap`].
    pub fn new(
        isolate_state_manager: IsolateStateManager,
        container_manager_requester: ContainerManagerRequester,
        config: NodeBootstrapConfig,
    ) -> Self {
        Self { isolate_state_manager, container_manager_requester, config }
    }

    /// Runs the bootstrap sequence to completion.
    ///
    /// Returns the [`BinaryServicesIndex`] of every workload Isolate that was loaded.
    pub async fn run(&self) -> Result<Vec<BinaryServicesIndex>> {
        self.await_setup_isolate().await?;

        // TODO: Acquire the EZ Node mTLS identity from the Setup Isolate here. It
        // must be obtained before any EzManagementService interaction begins.

        let loaded_indices = self.load_isolate_packages().await?;
        log::info!("Loaded {} workload Isolate package(s).", loaded_indices.len());
        Ok(loaded_indices)
    }

    /// Waits until the Setup Isolate can serve requests.
    async fn await_setup_isolate(&self) -> Result<()> {
        let setup_isolate_client = self
            .container_manager_requester
            .get_setup_isolate_client()
            .await
            .context("Failed to request the Setup Isolate client")?
            .context("No Setup Isolate is configured; a v2 manifest is required")?;
        let setup_isolate_index = setup_isolate_client
            .binary_services_index()
            .context("Setup Isolate services have not been registered")?;

        log::info!("Waiting for the Setup Isolate to become Ready.");
        timeout(
            SETUP_ISOLATE_READY_TIMEOUT,
            self.isolate_state_manager.wait_for_isolate_ready(setup_isolate_index),
        )
        .await
        .map_err(|_| {
            anyhow!("Setup Isolate did not become Ready within {SETUP_ISOLATE_READY_TIMEOUT:?}")
        })?
        .context("Failed while waiting for the Setup Isolate to become Ready")?;
        log::info!("Setup Isolate is Ready.");
        Ok(())
    }

    /// Starts EzManagementService interactions to load workload isolates.
    async fn load_isolate_packages(&self) -> Result<Vec<BinaryServicesIndex>> {
        let mut ez_management_client = EzManagementClient::new(
            &self.config.ez_management_address,
            self.container_manager_requester.clone(),
            self.config.max_decoding_message_size,
        )
        .await
        .context("Failed to create EzManagementClient")?;
        ez_management_client
            .load_packages()
            .await
            .context("Failed to load Isolate packages from EzManagementService")
    }
}
