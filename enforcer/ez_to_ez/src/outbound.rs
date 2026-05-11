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

use anyhow::{anyhow, Result};
use data_scope_proto::enforcer::v1::{EzDataScope, EzStaticScopeInfo};
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, EzPayloadIsolateScope, InvokeEzRequest, InvokeEzResponse,
    IsolateDataScope,
};
use ez_to_ez_service_proto::enforcer::v1::{
    ez_to_ez_api_client::EzToEzApiClient, EzCallRequest, EzCallResponse,
};
use grpc_connector::GrpcChannelPool;
use metrics::common::ServiceMetrics;
use metrics::observed_proxy_channel::ObservedSender;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod;
use payload_proto::enforcer::v1::EzPayloadScope;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::OnceCell;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

const EZ_TO_EZ_CHANNEL_SIZE: usize = 256;
const EZ_TO_EZ_EMPTY_PAYLOAD_ERROR: &str = "Empty payload data from remote enforcer";

/// Configuration for the outbound EZ-to-EZ mTLS.
#[derive(Clone)]
pub struct OutboundTlsConfig {
    pub factory: mtls::mtls::TlsConnectorFactory,
    pub trust_domain: String,
}

/// Handles outbound requests from the local enforcer to remote enforcers.
#[derive(Clone)]
pub struct OutboundEzToEzHandler<MetricsImpl: ServiceMetrics> {
    proxy_address: String,
    client_channel_pool: GrpcChannelPool,
    metrics: MetricsImpl,

    // TLS Config is set to turn on mTLS between Ez-to-Ez.
    tls_config: Option<OutboundTlsConfig>,
    // Map from unique SNI to TLS client channel pool.
    // A connection represents the connection to a unique peer enforcer.
    // Since multiple threads may want to create GrpcChannelPool concurrently,
    // we use OnceCell to ensure that only one task initializes the channel pool for a given SNI.
    tls_channel_pool: Arc<RwLock<HashMap<String, Arc<OnceCell<GrpcChannelPool>>>>>,
}

impl<MetricsImpl: ServiceMetrics> OutboundEzToEzHandler<MetricsImpl> {
    pub async fn new(
        proxy_address: String,
        metrics: MetricsImpl,
        tls_config: Option<OutboundTlsConfig>,
    ) -> Result<Self> {
        let client_channel_pool = GrpcChannelPool::new_from_env(&proxy_address)
            .await
            .map_err(|e| anyhow!("Failed to connect to EzToEz proxy: {}", e))?;
        Ok(Self {
            proxy_address,
            client_channel_pool,
            metrics,
            tls_config,
            tls_channel_pool: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn handle_outbound_requests(
        mut from_local_rx: mpsc::Receiver<InvokeEzRequest>,
        to_remote_tx: ObservedSender<EzCallRequest, MetricsImpl>,
    ) {
        while let Some(request) = from_local_rx.recv().await {
            let ez_req = invoke_ez_request_to_ez_call_request(request);
            if let Err(e) = to_remote_tx.send_request(ez_req).await {
                // TODO: We should send back the error to the caller.
                log::debug!("OutboundEzToEz: Remote stream closed, cannot send request: {}", e);
                break;
            }
        }
        log::info!("Done transporting requests from local to remote enforcer.");
    }

    async fn handle_remote_responses(
        mut response_stream: tonic::codec::Streaming<EzCallResponse>,
        to_local_tx: ObservedSender<anyhow::Result<InvokeEzResponse>, MetricsImpl>,
    ) {
        while let Some(remote_response_result) = response_stream.message().await.transpose() {
            let response_to_send = match remote_response_result {
                Ok(remote_response) => {
                    ez_call_response_to_invoke_ez_response(remote_response, None)
                }
                Err(status) => {
                    log::error!("EzToEz outbound streaming call failed: {status:#?}");
                    Err(status.into())
                }
            };

            if to_local_tx.send_response(response_to_send).await.is_err() {
                log::debug!("OutboundEzToEz: Local stream closed, cannot send response.");
                break;
            }
        }
        log::info!("Done transporting responses from remote to local enforcer.");
    }

    // This function gets or creates a TLS channel based on the SNI constructed from
    // the Control Plane Metadata.
    async fn get_or_create_channel(&self, meta: Option<&ControlPlaneMetadata>) -> Result<Channel> {
        // Fallback to default pool if TLS is not configured.
        let tls_config = match &self.tls_config {
            Some(config) => config,
            None => return Ok(self.client_channel_pool.next_channel()),
        };
        // If TLS is configured, we need metadata to route the request (SNI).
        let meta = match meta {
            Some(m) => m,
            None => return Err(anyhow!("Missing ControlPlaneMetadata for TLS routing")),
        };

        // Construct the SNI for the destination peer.
        let sni = mtls::mtls::sni(
            &meta.destination_ez_instance_id,
            &meta.destination_isolate_name,
            &meta.destination_publisher_id,
            &meta.destination_operator_domain,
            &tls_config.trust_domain,
        );

        let cell = {
            let read_guard = self.tls_channel_pool.read().await;
            read_guard.get(&sni).cloned()
        };

        // Use OnceCell to ensure that only one task initializes the channel pool
        // for a given SNI, avoiding duplicate work and thundering herd issues
        // when multiple requests for the same SNI arrive concurrently.
        let cell = if let Some(c) = cell {
            c
        } else {
            let mut write_guard = self.tls_channel_pool.write().await;
            // Double check in case another task created it while we were awaiting.
            write_guard
                .entry(sni.clone())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };

        let tls_config_clone = tls_config.clone();
        let sni_clone = sni.clone();
        let operator_domain = meta.destination_operator_domain.clone();
        let proxy_address = self.proxy_address.clone();

        // When OnceCell is empty, it'll execute the closure and store the result in the OnceCell.
        // Otherwise, it'll directly return the stored result.
        // This prevents multiple concurrent channel creation calls from race condition.
        let pool = cell
            .get_or_try_init(|| async move {
                let connector = tls_config_clone.factory.create_connector(&operator_domain).await?;
                grpc_connector::GrpcChannelPool::new_tls_from_env(
                    &proxy_address,
                    connector,
                    sni_clone,
                )
                .await
            })
            .await?;

        Ok(pool.next_channel())
    }
}
#[tonic::async_trait]
impl<MetricsImpl: ServiceMetrics> OutboundEzToEzClient for OutboundEzToEzHandler<MetricsImpl> {
    async fn remote_invoke(
        &self,
        request: InvokeEzRequest,
        timeout: Option<std::time::Duration>,
    ) -> Result<InvokeEzResponse> {
        let original_metadata = request.control_plane_metadata.clone();
        let ez_call_request = invoke_ez_request_to_ez_call_request(request);
        let channel = self.get_or_create_channel(original_metadata.as_ref()).await?;
        let mut client = EzToEzApiClient::new(channel);
        let mut tonic_request = tonic::Request::new(ez_call_request);
        if let Some(duration) = timeout {
            tonic_request.set_timeout(duration);
        }
        let response = client
            .ez_call(tonic_request)
            .await
            .map_err(|e| {
                log::error!("EzToEz outbound unary call failed: {e:#?}");
                e
            })?
            .into_inner();

        ez_call_response_to_invoke_ez_response(response, original_metadata)
    }

    async fn remote_streaming_connect(
        &self,
        first_request_metadata: Option<&ControlPlaneMetadata>,
        from_local_rx: mpsc::Receiver<InvokeEzRequest>,
    ) -> Result<mpsc::Receiver<anyhow::Result<InvokeEzResponse>>> {
        let channel = self.get_or_create_channel(first_request_metadata).await?;
        let mut client = EzToEzApiClient::new(channel);

        let channels = metrics::observed_proxy_channel::create_proxy_channels(
            EZ_TO_EZ_CHANNEL_SIZE,
            self.metrics.clone(),
        );

        let request_stream = ReceiverStream::new(channels.req_rx);
        let request = tonic::Request::new(request_stream);

        tokio::spawn(Self::handle_outbound_requests(from_local_rx, channels.req_tx));

        match client.ez_streaming_call(request).await {
            Ok(response) => {
                log::info!("New gRPC stream to remote enforcer established.");
                let response_stream = response.into_inner();
                tokio::spawn(Self::handle_remote_responses(response_stream, channels.res_tx));
                Ok(channels.res_rx)
            }
            Err(e) => {
                log::error!("EzToEz outbound streaming call failed to connect: {e:#?}");
                Err(e.into())
            }
        }
    }
}

fn invoke_ez_request_to_ez_call_request(request: InvokeEzRequest) -> EzCallRequest {
    let payload_scope = request.isolate_request_iscope.map(|iscope| EzPayloadScope {
        datagram_scopes: iscope
            .datagram_iscopes
            .into_iter()
            .map(|isolate_data_scope| EzDataScope {
                static_info: Some(EzStaticScopeInfo {
                    scope_type: isolate_data_scope.scope_type,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect(),
    });

    EzCallRequest {
        control_plane_metadata: request.control_plane_metadata,
        payload_scope,
        payload_data: request.isolate_request_payload.and_then(|hp| match hp.delivery_method {
            Some(DeliveryMethod::InlineData(data)) => Some(data),
            _ => None,
        }),
    }
}

/// Converts an EzCallResponse to an InvokeEzResponse.
fn ez_call_response_to_invoke_ez_response(
    response: EzCallResponse,
    original_metadata: Option<ControlPlaneMetadata>,
) -> Result<InvokeEzResponse> {
    let ez_response_iscope = response.payload_scope.map(|payload_scope| EzPayloadIsolateScope {
        datagram_iscopes: payload_scope
            .datagram_scopes
            .into_iter()
            .map(|datagram_scope| {
                let scope_type = datagram_scope.static_info.map_or_else(
                    || {
                        log::warn!("EzToEz Outbound: received EzDataScope without static_info, defaulting to Unspecified scope type.");
                        data_scope_proto::enforcer::v1::DataScopeType::Unspecified.into()
                    },
                    |si| si.scope_type,
                );
                IsolateDataScope { scope_type, ..Default::default() }
            })
            .collect(),
    });
    if response.payload_data.is_none() {
        return Err(anyhow::anyhow!(EZ_TO_EZ_EMPTY_PAYLOAD_ERROR));
    }

    Ok(InvokeEzResponse {
        control_plane_metadata: original_metadata,
        ez_response_iscope,
        ez_response_payload: response.payload_data.map(|data| {
            payload_proto::enforcer::v1::EzHybridPayload {
                delivery_method: Some(DeliveryMethod::InlineData(data)),
            }
        }),
    })
}
