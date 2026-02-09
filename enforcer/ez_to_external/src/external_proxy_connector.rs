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
use enforcer_proto::enforcer::v1::ez_external_proxy_service_client::EzExternalProxyServiceClient;
use enforcer_proto::enforcer::v1::{ControlPlaneMetadata, EzPayloadIsolateScope, IsolateDataScope};
use enforcer_proto::enforcer::v1::{
    EzExternalProxyRequest, EzExternalProxyResponse, EzExternalResource, InvokeEzRequest,
    InvokeEzResponse, IsolateStatus,
};
/// Prefix to Route External Requests to the ExternalProxyConnector.
use external_proxy_connector_constants::EXTERNAL_PREFIX;
use grpc_connector::connect;
use isolate_info::IsolateId;
use payload_proto::data_scope_proto::enforcer::v1::DataScopeType;
use payload_proto::enforcer::v1::EzPayloadData;
use std::env;
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::async_trait;
use tonic::transport::Channel;

const PROXY_CHANNEL_SIZE: usize = 1024;

/// Forwards requests to external proxy.
#[derive(Clone, Debug)]
pub struct ExternalProxyConnector {
    /// The gRPC client for the EzExternalProxyService.
    client: EzExternalProxyServiceClient<Channel>,
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
            .unwrap_or(grpc_connector::DEFAULT_CONNECT_RETRY_DELAY_MS);

        let retry_count = env::var("PROXY_CONNECT_RETRY_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(grpc_connector::DEFAULT_CONNECT_RETRY_COUNT);

        let channel = connect(address, retry_count, retry_delay_ms)
            .await
            .map_err(|e| ExternalProxyConnectorError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            client: EzExternalProxyServiceClient::new(channel)
                .max_decoding_message_size(max_decoding_message_size),
        })
    }

    /// Helper function to process the ControlPlaneMetadata from the first request.
    async fn process_first_metadata(
        response_metadata: &mut Option<ControlPlaneMetadata>,
        metadata_tx_opt: &mut Option<oneshot::Sender<ControlPlaneMetadata>>,
        invoke_request: &InvokeEzRequest,
        isolate_id: IsolateId,
        to_bridge_tx: &mpsc::Sender<InvokeEzResponse>,
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
                    let err = ExternalProxyConnectorError::TranslationError(
                    "SDK is always expected to populate control_plane_metadata, but it was None"
                        .to_string(),
                );
                    if Self::send_to_bridge(to_bridge_tx, error_to_response(err, None))
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

    /// Handles the request stream: receives from bridge, validates, translates, and sends to proxy.
    /// We use the oneshot channel to pass the `ControlPlaneMetadata` from the first `InvokeEzRequest`
    // received by `handle_requests` to the concurrent `handle_responses` task, which must wait for
    // this metadata before it can process and send any responses.
    async fn handle_requests(
        isolate_id: IsolateId,
        mut from_bridge_rx: Receiver<InvokeEzRequest>,
        to_proxy_tx: mpsc::Sender<EzExternalProxyRequest>,
        to_bridge_tx: mpsc::Sender<InvokeEzResponse>,
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
                let err = ExternalProxyConnectorError::TranslationError(format!(
                    "Permission Denied: {}",
                    e
                ));
                if Self::send_to_bridge(
                    &to_bridge_tx,
                    error_to_response(err, response_metadata.clone()),
                )
                .await
                .is_err()
                {
                    break;
                }
                continue;
            }

            match translate_to_proxy_request(invoke_request) {
                Ok(proxy_request) => {
                    if to_proxy_tx.send(proxy_request).await.is_err() {
                        log::warn!("Proxy request channel closed. Stopping forward task.");
                        break;
                    }
                }
                Err(e) => {
                    log::error!("Failed to translate request for proxy: {:?}", e);
                    if Self::send_to_bridge(
                        &to_bridge_tx,
                        error_to_response(e, response_metadata.clone()),
                    )
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
        to_bridge_tx: mpsc::Sender<InvokeEzResponse>,
        metadata_rx: oneshot::Receiver<ControlPlaneMetadata>,
    ) {
        let response_metadata = match metadata_rx.await {
            Ok(m) => m,
            Err(_) => {
                log::error!("Failed to receive control plane metadata from request handler.");
                return;
            }
        };
        while let Some(proxy_response_result) = response_stream.next().await {
            let response_to_send = match proxy_response_result {
                Ok(proxy_response) => {
                    translate_from_proxy_response(proxy_response, response_metadata.clone())
                }
                Err(status) => Err(ExternalProxyConnectorError::from(status)),
            };

            let final_response = response_to_send
                .unwrap_or_else(|err| error_to_response(err, Some(response_metadata.clone())));

            if Self::send_to_bridge(&to_bridge_tx, final_response).await.is_err() {
                break;
            }
        }
        log::info!("gRPC stream from external proxy has ended.");
    }
    /// Sends a response to the bridge channel and logs a warning if the send fails.
    async fn send_to_bridge(
        to_bridge_tx: &mpsc::Sender<InvokeEzResponse>,
        response: InvokeEzResponse,
    ) -> Result<(), ()> {
        if let Err(e) = to_bridge_tx.send(response).await {
            log::warn!("Failed to send response to bridge: channel closed: {}", e);
        }
        Ok(())
    }
}

#[async_trait]
impl ExternalProxyChannel for ExternalProxyConnector {
    /// Handles a unary request to the external proxy.
    async fn proxy_external(
        &self,
        isolate_id: IsolateId,
        request: InvokeEzRequest,
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
        let response = self.client.clone().unary_call(proxy_request).await?;

        // Translate the proxy's response back and return.
        translate_from_proxy_response(response.into_inner(), new_metadata)
    }

    /// Proxies a bidirectional stream between an Isolate and the external proxy.
    async fn stream_proxy_external(
        &self,
        isolate_id: IsolateId,
        from_bridge_rx: Receiver<InvokeEzRequest>,
    ) -> Result<Receiver<InvokeEzResponse>, ExternalProxyConnectorError> {
        let mut client = self.client.clone();
        let (to_bridge_tx, to_bridge_rx) = mpsc::channel(PROXY_CHANNEL_SIZE);
        let (to_proxy_tx, to_proxy_rx) = mpsc::channel(PROXY_CHANNEL_SIZE);

        let (metadata_tx, metadata_rx) = oneshot::channel();

        let request_stream = ReceiverStream::new(to_proxy_rx);
        let request = tonic::Request::new(request_stream);

        tokio::spawn(Self::handle_requests(
            isolate_id,
            from_bridge_rx,
            to_proxy_tx,
            to_bridge_tx.clone(),
            metadata_tx,
        ));

        match client.stream_call(request).await {
            Ok(response) => {
                log::info!(
                    "New gRPC stream to external proxy established for Isolate Id: {:?}",
                    isolate_id
                );
                let response_stream = response.into_inner();
                tokio::spawn(Self::handle_responses(response_stream, to_bridge_tx, metadata_rx));
                Ok(to_bridge_rx)
            }
            Err(e) => {
                return Err(ExternalProxyConnectorError::StreamFailed(Box::new(e)));
            }
        }
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

    // Check if the payload contains more than one datagram.
    if payload.datagrams.len() != 1 {
        return Err(ExternalProxyConnectorError::TranslationError(
            "External calls must contain exactly one datagram.".to_string(),
        ));
    }
    let payload_bytes = payload.datagrams.into_iter().next().unwrap();

    let target_domain = metadata
        .destination_operator_domain
        .strip_prefix(EXTERNAL_PREFIX)
        .unwrap_or(&metadata.destination_operator_domain);

    let resource = EzExternalResource {
        target: target_domain.to_string(),
        service_name: metadata.destination_service_name,
        method_name: metadata.destination_method_name,
    };

    Ok(EzExternalProxyRequest { resource: Some(resource), external_request_payload: payload_bytes })
}

/// Translates a response from the external proxy back into the InvokeEzResponse format.
fn translate_from_proxy_response(
    res: EzExternalProxyResponse,
    response_metadata: ControlPlaneMetadata,
) -> Result<InvokeEzResponse, ExternalProxyConnectorError> {
    let payload = EzPayloadData { datagrams: vec![res.external_response_payload] };
    let status = IsolateStatus { code: 0, message: "OK".to_string() };

    Ok(InvokeEzResponse {
        ez_response_payload: Some(payload),
        status: Some(status),
        control_plane_metadata: Some(response_metadata),
        ez_response_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                mapped_scope_owner: None,
            }],
        }),
    })
}

// TODO: Have a common file for all gRPC Error Codes.
/// Converts a connector error into an InvokeEzResponse with a non-OK status.
fn error_to_response(
    err: ExternalProxyConnectorError,
    response_metadata: Option<ControlPlaneMetadata>,
) -> InvokeEzResponse {
    let status = match err {
        ExternalProxyConnectorError::ConnectionFailed(msg) => {
            IsolateStatus { code: 14, message: msg }
        }
        ExternalProxyConnectorError::StreamFailed(status) => {
            IsolateStatus { code: status.code() as i32, message: status.message().to_string() }
        }
        ExternalProxyConnectorError::TranslationError(msg) => {
            IsolateStatus { code: 3, message: msg }
        }
    };

    InvokeEzResponse {
        ez_response_payload: None,
        status: Some(status),
        control_plane_metadata: response_metadata,
        ez_response_iscope: None,
    }
}
