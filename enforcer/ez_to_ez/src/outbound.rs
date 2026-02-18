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
    IsolateDataScope, IsolateStatus,
};
use ez_to_ez_service_proto::enforcer::v1::{
    ez_to_ez_api_client::EzToEzApiClient, EzCallRequest, EzCallResponse,
};
use grpc_connector::connect;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use payload_proto::enforcer::v1::EzPayloadScope;
use std::env;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

const EZ_TO_EZ_CHANNEL_SIZE: usize = 256;

/// Handles outbound requests from the local enforcer to remote enforcers.
#[derive(Clone)]
pub struct OutboundEzToEzHandler {
    ez_to_ez_api_client: EzToEzApiClient<Channel>,
}

impl OutboundEzToEzHandler {
    pub async fn new(address: String) -> Result<Self> {
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
            .map_err(|e| anyhow!("Failed to connect to EzToEz proxy: {}", e))?;

        Ok(Self { ez_to_ez_api_client: EzToEzApiClient::new(channel) })
    }

    async fn handle_outbound_requests(
        mut from_local_rx: mpsc::Receiver<InvokeEzRequest>,
        to_remote_tx: mpsc::Sender<EzCallRequest>,
    ) {
        while let Some(request) = from_local_rx.recv().await {
            let ez_req = invoke_ez_request_to_ez_call_request(request);
            if let Err(e) = to_remote_tx.send(ez_req).await {
                // TODO: We should send back the error to the caller.
                log::warn!("OutboundEzToEz: Remote stream closed, cannot send request: {}", e);
                break;
            }
        }
        log::info!("Done transporting requests from local to remote enforcer.");
    }

    async fn handle_remote_responses(
        mut response_stream: tonic::codec::Streaming<EzCallResponse>,
        to_local_tx: mpsc::Sender<anyhow::Result<InvokeEzResponse>>,
    ) {
        while let Some(remote_response_result) = response_stream.message().await.transpose() {
            let response_to_send = match remote_response_result {
                Ok(remote_response) => {
                    let invoke_ez_response =
                        ez_call_response_to_invoke_ez_response(remote_response, None);
                    Ok(invoke_ez_response)
                }
                Err(status) => Err(anyhow!("EzToEz outbound streaming call failed: {}", status)),
            };

            if to_local_tx.send(response_to_send).await.is_err() {
                log::warn!("OutboundEzToEz: Local stream closed, cannot send response.");
                break;
            }
        }
        log::info!("gRPC stream from remote enforcer has ended.");
    }
}

#[tonic::async_trait]
impl OutboundEzToEzClient for OutboundEzToEzHandler {
    async fn remote_invoke(
        &self,
        request: InvokeEzRequest,
        timeout: Option<std::time::Duration>,
    ) -> Result<InvokeEzResponse> {
        // Extract metadata before the request is moved.
        let original_metadata = request.control_plane_metadata.clone();
        log::info!(
            "OutboundEzToEz: outbound EZ-to-EZ control_plane_metadata: {:?}",
            original_metadata
        );
        let ez_call_request = invoke_ez_request_to_ez_call_request(request);
        let mut client = self.ez_to_ez_api_client.clone();

        let mut tonic_request = tonic::Request::new(ez_call_request);
        if let Some(duration) = timeout {
            tonic_request.set_timeout(duration);
        }

        let response = client
            .ez_call(tonic_request)
            .await
            .map_err(|e| anyhow!("EzToEz outbound unary call failed: {}", e))?
            .into_inner();

        Ok(ez_call_response_to_invoke_ez_response(response, original_metadata))
    }

    async fn remote_streaming_connect(
        &self,
        from_local_rx: mpsc::Receiver<InvokeEzRequest>,
    ) -> Result<mpsc::Receiver<anyhow::Result<InvokeEzResponse>>> {
        let mut client = self.ez_to_ez_api_client.clone();
        let (to_local_tx, to_local_rx) = mpsc::channel(EZ_TO_EZ_CHANNEL_SIZE);
        let (to_remote_tx, to_remote_rx) = mpsc::channel(EZ_TO_EZ_CHANNEL_SIZE);

        let request_stream = ReceiverStream::new(to_remote_rx);
        let request = tonic::Request::new(request_stream);

        tokio::spawn(Self::handle_outbound_requests(from_local_rx, to_remote_tx));

        match client.ez_streaming_call(request).await {
            Ok(response) => {
                log::info!("New gRPC stream to remote enforcer established.");
                let response_stream = response.into_inner();
                tokio::spawn(Self::handle_remote_responses(response_stream, to_local_tx));
                Ok(to_local_rx)
            }
            Err(e) => Err(anyhow!("EzToEz outbound streaming call failed to connect: {}", e)),
        }
    }
}

/// Converts an InvokeEzRequest to an EzCallRequest.
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
        payload_data: request.isolate_request_payload,
    }
}

/// Converts an EzCallResponse to an InvokeEzResponse.
fn ez_call_response_to_invoke_ez_response(
    response: EzCallResponse,
    original_metadata: Option<ControlPlaneMetadata>,
) -> InvokeEzResponse {
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

    InvokeEzResponse {
        control_plane_metadata: original_metadata,
        ez_response_iscope,
        ez_response_payload: response.payload_data,
        status: response.status.map(|s| IsolateStatus { code: s.code, message: s.message }),
    }
}
