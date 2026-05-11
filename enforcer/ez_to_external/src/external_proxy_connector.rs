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

use crate::types::{ExternalProxyChannel, ExternalProxyConnectorError};
use data_scope::data_scope_validator::validate_external_call;
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, EzPayloadIsolateScope, InvokeEzRequest, InvokeEzResponse,
    IsolateDataScope,
};
use external_proxy_proto::enforcer::v1::{
    ez_external_proxy_service_client::EzExternalProxyServiceClient, EzExternalProxyRequest,
    EzExternalProxyResponse, EzExternalResource,
};
use grpc_connector::{
    GrpcChannelPool, DEFAULT_CONNECT_RETRY_COUNT, DEFAULT_CONNECT_RETRY_DELAY_MS,
    DEFAULT_CONNECT_RETRY_SCALING, DEFAULT_POOL_SIZE,
};
use isolate_info::IsolateId;
use metrics::common::{CallTracker, MessageTimerGuard, MetricAttributes, ServiceMetrics};
use metrics::external_proxy_connector::ExternalProxyConnectorMetrics;
use metrics::observed_proxy_channel::ObservedSender;
use payload_proto::data_scope_proto::enforcer::v1::DataScopeType;
use payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod;
use payload_proto::enforcer::v1::EzPayloadData;
use prost::Message;
use std::collections::HashMap;
use std::env;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::async_trait;
use tonic::Status;

const PROXY_CHANNEL_SIZE: usize = 1024;

type ProxyRequestSender = ObservedSender<EzExternalProxyRequest, ExternalProxyConnectorMetrics>;
type BridgeResponseSender =
    ObservedSender<Result<InvokeEzResponse, Status>, ExternalProxyConnectorMetrics>;

/// Forwards requests to external proxy.
#[derive(Clone, Debug)]
pub struct ExternalProxyConnector {
    /// The gRPC client for the EzExternalProxyService.
    client_channel_pool: GrpcChannelPool,
    max_decoding_message_size: usize,
    /// Metrics for the external proxy
    metrics: ExternalProxyConnectorMetrics,
}

impl ExternalProxyConnector {
    /// Creates a new ExternalProxyConnector, establishing a connection to the given address.
    pub async fn new(
        address: String,
        max_decoding_message_size: usize,
    ) -> Result<Self, ExternalProxyConnectorError> {
        let retry_delay_ms = env::var("PROXY_CONNECT_RETRY_DELAY_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECT_RETRY_DELAY_MS);

        let retry_count = env::var("PROXY_CONNECT_RETRY_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECT_RETRY_COUNT);

        let retry_scaling = env::var("PROXY_CONNECT_RETRY_SCALING")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CONNECT_RETRY_SCALING);

        let client_channel_pool = GrpcChannelPool::new(
            address.to_string(),
            DEFAULT_POOL_SIZE,
            retry_count,
            retry_delay_ms,
            retry_scaling,
        )
        .await
        .map_err(|e| ExternalProxyConnectorError::ConnectionFailed(e.to_string()))?;

        let metrics = ExternalProxyConnectorMetrics::new();

        Ok(Self { client_channel_pool, max_decoding_message_size, metrics })
    }

    /// Helper function to process the ControlPlaneMetadata from the first request.
    async fn process_first_metadata(
        response_metadata: &mut Option<ControlPlaneMetadata>,
        metadata_tx_opt: &mut Option<oneshot::Sender<ControlPlaneMetadata>>,
        invoke_request: &InvokeEzRequest,
        isolate_id: IsolateId,
        to_bridge_tx: &BridgeResponseSender,
    ) -> bool {
        // Only process if metadata hasn't been set yet.
        if response_metadata.is_none() {
            // Only process if we still have the one-shot sender.
            if let Some(tx) = metadata_tx_opt.take() {
                if let Some(metadata) = &invoke_request.control_plane_metadata {
                    // First request has metadata, create new metadata with ipc_message_id.
                    let new_metadata = ControlPlaneMetadata {
                        ipc_message_id: metadata.ipc_message_id,
                        requester_is_local: true,
                        ..ControlPlaneMetadata::default()
                    };

                    if tx.send(new_metadata.clone()).is_err() {
                        log::warn!("Failed to send metadata to response handler.");
                        return false;
                    }
                    *response_metadata = Some(new_metadata);
                } else {
                    // First request is missing metadata.
                    log::warn!(
                        "First InvokeEzRequest for isolate {} missing control_plane_metadata.",
                        isolate_id
                    );
                    let err_msg = "SDK is always expected to populate control_plane_metadata, but it was None".to_string();
                    if to_bridge_tx
                        .unobserved_send(Err(Status::invalid_argument(err_msg)))
                        .await
                        .is_err()
                    {
                        log::warn!("Failed to send metadata error to bridge: channel closed.");
                    }
                    return false;
                }
            }
        }
        true
    }

    // Handles the request stream: receives from bridge, validates, translates, and sends to proxy.
    // We use the oneshot channel to pass the `ControlPlaneMetadata` from the first `InvokeEzRequest`
    // received by `handle_requests` to the concurrent `handle_responses` task, which must wait for
    // this metadata before it can process and send any responses.
    async fn handle_requests(
        isolate_id: IsolateId,
        mut from_bridge_rx: Receiver<InvokeEzRequest>,
        to_proxy_tx: ProxyRequestSender,
        to_bridge_tx: BridgeResponseSender,
        metadata_tx: oneshot::Sender<ControlPlaneMetadata>,
    ) {
        let mut response_metadata: Option<ControlPlaneMetadata> = None;
        let mut metadata_tx_opt = Some(metadata_tx);
        while let Some(invoke_request) = from_bridge_rx.recv().await {
            // First Request Metadata Processing: True if successful, False if error.
            if !Self::process_first_metadata(
                &mut response_metadata,
                &mut metadata_tx_opt,
                &invoke_request,
                isolate_id,
                &to_bridge_tx,
            )
            .await
            {
                // First request had no metadata or metadata channel failed.
                break;
            }
            if let Err(e) = validate_external_call(&invoke_request) {
                log::warn!("External call rejected by policy for isolate {}: {}", isolate_id, e);
                let err_msg = format!("Permission Denied: {e:#?}");
                if to_bridge_tx
                    .unobserved_send(Err(Status::permission_denied(err_msg)))
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }

            match translate_to_proxy_request(invoke_request) {
                Ok(proxy_request) => {
                    if to_proxy_tx.send_request(proxy_request).await.is_err() {
                        log::warn!("Proxy request channel closed. Stopping forward task.");
                        break;
                    }
                }
                Err(e) => {
                    log::error!("Failed to translate request for proxy: {:?}", e);
                    if to_bridge_tx
                        .unobserved_send(Err(Status::invalid_argument(e.to_string())))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
        log::info!("Done transporting requests from bridge to proxy.");
    }

    /// Handles the response stream: receives from proxy, translates, and sends to bridge.
    async fn handle_responses(
        mut response_stream: tonic::codec::Streaming<EzExternalProxyResponse>,
        to_bridge_tx: BridgeResponseSender,
        metadata_rx: oneshot::Receiver<ControlPlaneMetadata>,
        metrics: ExternalProxyConnectorMetrics,
    ) {
        let response_metadata = match metadata_rx.await {
            Ok(m) => m,
            Err(_) => {
                log::error!("Failed to receive control plane metadata from request handler.");
                return;
            }
        };
        while let Some(proxy_response_result) = response_stream.next().await {
            let _msg_timer = MessageTimerGuard::new(&metrics, &[]);
            let response_to_send = match proxy_response_result {
                Ok(proxy_response) => {
                    Ok(translate_from_proxy_response(proxy_response, response_metadata.clone()))
                }
                Err(status) => Err(status),
            };

            if to_bridge_tx.send_response(response_to_send).await.is_err() {
                break;
            }
        }
        log::info!("gRPC stream from external proxy has ended.");
    }
}

#[async_trait]
impl ExternalProxyChannel for ExternalProxyConnector {
    /// Handles a unary request to the external proxy.
    async fn proxy_external(
        &self,
        isolate_id: IsolateId,
        request: InvokeEzRequest,
        timeout: Option<std::time::Duration>,
    ) -> Result<InvokeEzResponse, ExternalProxyConnectorError> {
        if let Err(e) = validate_external_call(&request) {
            log::warn!("External call rejected by policy for isolate {}: {}", isolate_id, e);
            let err =
                ExternalProxyConnectorError::TranslationError(format!("Permission Denied: {}", e));
            return Err(err);
        }

        let new_metadata = match &request.control_plane_metadata {
            Some(original_metadata) => ControlPlaneMetadata {
                ipc_message_id: original_metadata.ipc_message_id,
                requester_is_local: true,
                ..ControlPlaneMetadata::default()
            },
            None => {
                return Err(ExternalProxyConnectorError::TranslationError(
                    "SDK is always expected to populate control_plane_metadata, but it was None"
                        .to_string(),
                ));
            }
        };

        // Translate and send unary_call request
        let proxy_request = translate_to_proxy_request(request)?;
        let attrs = MetricAttributes::from(&proxy_request);
        self.metrics.record_message_size_bytes(attrs.request(), proxy_request.encoded_len() as u64);

        let _tracker = CallTracker::new(self.metrics.clone(), attrs.base());
        let base_attrs = attrs.base();
        let _msg_timer = MessageTimerGuard::new(&self.metrics, &base_attrs);

        let mut tonic_request = tonic::Request::new(proxy_request);
        if let Some(duration) = timeout {
            tonic_request.set_timeout(duration);
        }
        let mut client = EzExternalProxyServiceClient::new(self.client_channel_pool.next_channel())
            .max_decoding_message_size(self.max_decoding_message_size);
        let response = client
            .unary_call(tonic_request)
            .await
            .map_err(ExternalProxyConnectorError::UnaryCallFailed)?;

        let proxy_response = response.into_inner();
        self.metrics
            .record_message_size_bytes(attrs.response(), proxy_response.encoded_len() as u64);

        // Translate the proxy's response back and return.
        Ok(translate_from_proxy_response(proxy_response, new_metadata))
    }

    /// Proxies a bidirectional stream between an Isolate and the external proxy.
    ///
    /// # Deadlock Warning
    ///
    /// This function spawns a background task that waits for the first request from the
    /// `from_bridge_rx` receiver to extract `ControlPlaneMetadata` before establishing the
    /// connection to the external proxy. Callers **must** ensure that at least one request is sent
    /// to `from_bridge_rx` to avoid a deadlock where the stream is never established.
    ///
    /// # Error Handling
    ///
    /// Unlike typical gRPC methods, this function does *not* return `Err` if the connection
    /// to the external proxy fails initially. Instead, it returns a `Receiver` for responses,
    /// and any connection or stream errors are sent as `InvokeEzResponse` messages with a
    /// non-OK status code through that receiver.
    async fn stream_proxy_external(
        &self,
        isolate_id: IsolateId,
        from_bridge_rx: Receiver<InvokeEzRequest>,
    ) -> Result<Receiver<Result<InvokeEzResponse, Status>>, ExternalProxyConnectorError> {
        let mut client = EzExternalProxyServiceClient::new(self.client_channel_pool.next_channel())
            .max_decoding_message_size(self.max_decoding_message_size);

        let channels = metrics::observed_proxy_channel::create_proxy_channels::<
            EzExternalProxyRequest,
            Result<InvokeEzResponse, Status>,
            ExternalProxyConnectorMetrics,
        >(PROXY_CHANNEL_SIZE, self.metrics.clone());

        let (metadata_tx, metadata_rx) = oneshot::channel();

        let request_stream = ReceiverStream::new(channels.req_rx);
        let request = tonic::Request::new(request_stream);

        tokio::spawn(Self::handle_requests(
            isolate_id,
            from_bridge_rx,
            channels.req_tx,
            channels.res_tx.clone(),
            metadata_tx,
        ));

        match client.stream_call(request).await {
            Ok(response) => {
                log::info!(
                    "New gRPC stream to external proxy established for Isolate Id: {:?}",
                    isolate_id
                );
                let response_stream = response.into_inner();
                tokio::spawn(Self::handle_responses(
                    response_stream,
                    channels.res_tx.clone(),
                    metadata_rx,
                    self.metrics.clone(),
                ));
            }
            Err(e) => {
                let _ = channels.res_tx.unobserved_send(Err(e)).await;
            }
        }

        Ok(channels.res_rx)
    }

    // Allows ExternalProxyChannel to be cloned.
    fn clone_channel(&self) -> Box<dyn ExternalProxyChannel> {
        Box::new(self.clone())
    }
}

/// Translates an InvokeEzRequest into the format required by the ez_external_proxy.
pub fn translate_to_proxy_request(
    req: InvokeEzRequest,
) -> Result<EzExternalProxyRequest, ExternalProxyConnectorError> {
    // Error if Control Plane Metadata is missing
    let metadata = req.control_plane_metadata.ok_or_else(|| {
        ExternalProxyConnectorError::TranslationError("Missing control_plane_metadata".to_string())
    })?;
    // Error if payload is missing
    let payload = req.isolate_request_payload.ok_or_else(|| {
        ExternalProxyConnectorError::TranslationError("Missing isolate_request_payload".to_string())
    })?;

    let inline_data = match payload.delivery_method {
        Some(DeliveryMethod::InlineData(data)) => Ok(data),
        _ => Err(ExternalProxyConnectorError::TranslationError(
            "Can't use shared-memory for external comms.".to_string(),
        )),
    }?;

    // Check if the payload contains more than one datagram.
    if inline_data.datagrams.len() != 1 {
        return Err(ExternalProxyConnectorError::TranslationError(
            "External calls must contain exactly one datagram.".to_string(),
        ));
    }
    let payload_bytes = inline_data.datagrams.into_iter().next().unwrap();

    let target_domain = &metadata.destination_operator_domain;

    let resource = EzExternalResource {
        target: target_domain.to_string(),
        service_name: metadata.destination_service_name,
        method_name: metadata.destination_method_name,
    };

    Ok(EzExternalProxyRequest {
        resource: Some(resource),
        external_request_payload: payload_bytes,
        request_metadata: HashMap::new(),
    })
}

/// Translates a response from the external proxy back into the InvokeEzResponse format.
fn translate_from_proxy_response(
    res: EzExternalProxyResponse,
    response_metadata: ControlPlaneMetadata,
) -> InvokeEzResponse {
    let payload = EzPayloadData { datagrams: vec![res.external_response_payload] };
    let hybrid_payload = payload_proto::enforcer::v1::EzHybridPayload {
        delivery_method: Some(DeliveryMethod::InlineData(payload)),
    };

    InvokeEzResponse {
        ez_response_payload: Some(hybrid_payload),
        control_plane_metadata: Some(response_metadata),
        ez_response_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                mapped_scope_owner: None,
            }],
        }),
    }
}
