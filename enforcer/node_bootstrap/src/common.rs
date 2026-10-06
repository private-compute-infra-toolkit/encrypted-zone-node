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

use anyhow::{anyhow, Context, Result};
use container_manager_requester::ContainerManagerRequester;
use inbound_ez_to_ez_handler::{InboundEzToEzHandler, InboundTlsConfig};
use mtls::mtls::{EzMtlsManager, EzMtlsManagerConfig};
use outbound_ez_to_ez_client::{OutboundEzToEzClient, OutboundTlsConfig};
use setup_isolate_client::SetupIsolateClient;
use state_manager::IsolateStateManager;
use std::time::Duration;
use tokio::time::timeout;

const SETUP_ISOLATE_READY_TIMEOUT: Duration = Duration::from_secs(300);

/// Waits until the Setup Isolate is configured and can serve requests.
pub(crate) async fn await_setup_isolate(
    isolate_state_manager: &IsolateStateManager,
    container_manager_requester: &ContainerManagerRequester,
) -> Result<SetupIsolateClient> {
    let setup_isolate_client = container_manager_requester
        .get_setup_isolate_client()
        .await
        .context("Failed to request the Setup Isolate client")?
        .context("No Setup Isolate is configured")?;
    let setup_isolate_index = setup_isolate_client
        .binary_services_index()
        .context("Setup Isolate services have not been registered")?;

    log::info!("Waiting for the Setup Isolate to become Ready.");
    timeout(
        SETUP_ISOLATE_READY_TIMEOUT,
        isolate_state_manager.wait_for_isolate_ready(setup_isolate_index),
    )
    .await
    .map_err(|_| {
        anyhow!("Setup Isolate did not become Ready within {SETUP_ISOLATE_READY_TIMEOUT:?}")
    })?
    .context("Failed while waiting for the Setup Isolate to become Ready")?;
    log::info!("Setup Isolate is Ready.");
    Ok(setup_isolate_client)
}

/// Bootstraps the [`EzMtlsManager`] and constructs the inbound and outbound TLS configurations.
pub(crate) async fn build_tls_configs(
    config: EzMtlsManagerConfig,
    ez_to_ez_handshake_timeout: Duration,
    ez_to_ez_max_concurrent_handshakes: usize,
) -> Result<(InboundTlsConfig, OutboundTlsConfig)> {
    let mtls_manager = EzMtlsManager::build(config)
        .await
        .context("Failed to bootstrap EzMtlsManager. mTLS connection must be successful.")?;
    let acceptor =
        mtls_manager.create_tls_acceptor().await.context("Failed to create TLS acceptor")?;
    let inbound_tls_config = InboundTlsConfig {
        acceptor,
        handshake_timeout: ez_to_ez_handshake_timeout,
        max_concurrent_handshakes: ez_to_ez_max_concurrent_handshakes,
    };
    let outbound_tls_config = OutboundTlsConfig {
        factory: mtls_manager.get_connector_factory(),
        trust_domain: mtls_manager.spiffe_identity().trust_domain.clone(),
    };
    log::info!("Successfully bootstrapped EzMtlsManager.");
    Ok((inbound_tls_config, outbound_tls_config))
}

/// Configures the outbound EZ-to-EZ client with TLS and launches the inbound EZ-to-EZ server
/// in the background if configured.
pub(crate) fn launch_ez_to_ez(
    ez_to_ez_outbound_handler: &Option<Box<dyn OutboundEzToEzClient>>,
    ez_to_ez_inbound: Option<(InboundEzToEzHandler, String)>,
    max_decoding_message_size: usize,
    inbound_tls_config: Option<InboundTlsConfig>,
    outbound_tls_config: Option<OutboundTlsConfig>,
) -> Result<()> {
    if let (Some(ref handler), Some(outbound_tls_config)) =
        (ez_to_ez_outbound_handler, outbound_tls_config)
    {
        handler
            .set_tls_config(outbound_tls_config)
            .context("Failed to set outbound TLS config on outbound handler")?;
    }

    if let Some((inbound_handler, address)) = ez_to_ez_inbound {
        tokio::spawn(async move {
            inbound_ez_to_ez_handler::launch_server(
                inbound_handler,
                &address,
                max_decoding_message_size,
                inbound_tls_config,
            )
            .await;
        });
    }

    Ok(())
}
