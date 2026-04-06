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

use data_scope_proto::enforcer::v1::{DataScopeType, EzStaticScopeInfo};
use enforcer_proto::enforcer::v1::{
    EzPayloadIsolateScope, InvokeIsolateRequest, InvokeIsolateResponse, IsolateDataScope,
};
use ez_error::EzError;
use ez_to_ez_service_proto::enforcer::v1::{
    ez_to_ez_api_server::{EzToEzApi, EzToEzApiServer},
    EzCallRequest, EzCallResponse,
};
use grpc_connector::try_parse_grpc_timeout;
use junction_trait::{Junction, JunctionChannels};
use metrics::{
    common::{MetricAttributes, ServiceMetrics},
    ez_to_ez_inbound::EzToEzInboundMetrics,
    observed_stream,
};
use opentelemetry::global;
use payload_proto::enforcer::v1::EzPayloadScope;
use prost::Message;
use tokio::{net::UnixListener, sync::mpsc};
use tokio_stream::{
    wrappers::{ReceiverStream, UnixListenerStream},
    StreamExt,
};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const EZ_TO_EZ_RESPONSE_CHANNEL_SIZE: usize = 256;

type EzToEzResponseStream = observed_stream::ObservedResponseStream<
    ReceiverStream<Result<EzCallResponse, Status>>,
    EzToEzInboundMetrics,
>;

/// Handles inbound requests from remote enforcers.
#[derive(Clone, Debug)]
pub struct InboundEzToEzHandler {
    isolate_junction: Box<dyn Junction>,
    metrics: EzToEzInboundMetrics,
}

impl InboundEzToEzHandler {
    pub fn new(isolate_junction: Box<dyn Junction>) -> Self {
        let metrics = EzToEzInboundMetrics::new();
        Self { isolate_junction, metrics }
    }
}

#[tonic::async_trait]
impl EzToEzApi for InboundEzToEzHandler {
    async fn ez_call(
        &self,
        request: Request<EzCallRequest>,
    ) -> Result<Response<EzCallResponse>, Status> {
        let timeout = try_parse_grpc_timeout(request.metadata()).unwrap_or(None);
        let ez_call_request = request.into_inner();
        let metric_attr = MetricAttributes::from(&ez_call_request);
        let _call_tracker = self.metrics.track_call(metric_attr.base());

        let span = tracing::info_span!("Enforcer.EzToEzApi.Inbound.ez_call");

        if let Some(ref metadata) = ez_call_request.control_plane_metadata {
            log::info!("EzCall Inbound Request headers: {:#?}", metadata.metadata_headers);
            let parent_context = global::get_text_map_propagator(|propagator| {
                propagator.extract(&trace_context::HashMapExtractor(&metadata.metadata_headers))
            });

            let _ = span.set_parent(parent_context);
        }

        async move {
            self.metrics.record_message_size_bytes(
                metric_attr.request(),
                ez_call_request.encoded_len() as u64,
            );
            let invoke_isolate_request = ez_call_request_to_invoke_isolate_request(ez_call_request);

            let invoke_isolate_response = self
                .isolate_junction
                .invoke_isolate(None, invoke_isolate_request, false, timeout)
                .await
                .map_err(|e| {
                    self.metrics.record_error(&metric_attr.base(), "junction_invocation_failed");
                    e.to_tonic_status()
                })?;

            let ez_call_response =
                invoke_isolate_response_to_ez_call_response(invoke_isolate_response);
            self.metrics.record_message_size_bytes(
                metric_attr.response(),
                ez_call_response.encoded_len() as u64,
            );
            Ok(Response::new(ez_call_response))
        }
        .instrument(span)
        .await
    }

    type EzStreamingCallStream = EzToEzResponseStream;

    async fn ez_streaming_call(
        &self,
        request: Request<Streaming<EzCallRequest>>,
    ) -> Result<Response<Self::EzStreamingCallStream>, Status> {
        let ez_call_request_stream = request.into_inner();

        // Create the response stream to send back to the remote enforcer.
        let (to_remote_response_tx, to_remote_response_rx) =
            mpsc::channel(EZ_TO_EZ_RESPONSE_CHANNEL_SIZE);
        let isolate_junction = self.isolate_junction.clone();

        let JunctionChannels { client_to_junction: client_to_junction_tx, mut junction_to_client } =
            isolate_junction.stream_invoke_isolate(None, false).await;

        let (mut ez_call_request_stream, to_remote_response_rx) = observed_stream::pair(
            ez_call_request_stream,
            ReceiverStream::new(to_remote_response_rx),
            self.metrics.clone(),
        );

        let metrics_clone_for_req = self.metrics.clone();

        // Task 1: Proxy requests from the remote enforcer to the local isolate.
        tokio::spawn(async move {
            while let Some(ez_call_request) = ez_call_request_stream.next().await {
                proxy_to_local_request(
                    ez_call_request,
                    ez_call_request_stream.attributes(),
                    &client_to_junction_tx,
                    &metrics_clone_for_req,
                )
                .await;
            }
        });

        // Task 2: Proxy responses from the local Isolate back to the remote enforcer.
        tokio::spawn(async move {
            while let Some(invoke_response_result) = junction_to_client.recv().await {
                proxy_to_remote_response(invoke_response_result, &to_remote_response_tx).await;
            }
        });

        Ok(Response::new(to_remote_response_rx))
    }
}

/// Proxies requests from a remote enforcer to a local Isolate.
async fn proxy_to_local_request(
    ez_call_request: EzCallRequest,
    attrs: &MetricAttributes,
    client_to_junction_tx: &mpsc::Sender<InvokeIsolateRequest>,
    metrics: &EzToEzInboundMetrics,
) {
    let _timer = metrics.track_message_processing(attrs.request());
    let invoke_request = ez_call_request_to_invoke_isolate_request(ez_call_request);
    if let Err(e) = client_to_junction_tx.send(invoke_request).await {
        metrics.record_error(attrs.base().as_ref(), "junction_invocation_failed");
        log::error!(
            "EzToEz Inbound: Failed to proxy request to local isolate. Junction channel closed: {e:?}"
        );
    }
}

/// Converts an EzCallRequest to an InvokeIsolateRequest.
fn ez_call_request_to_invoke_isolate_request(request: EzCallRequest) -> InvokeIsolateRequest {
    let mut metadata = request.control_plane_metadata;
    if let Some(m) = metadata.as_mut() {
        // The request is coming from a remote enforcer, so the requester is not local.
        m.requester_is_local = false;
        // The request is being handled by a local Isolate.
        m.responder_is_local = true;
    }

    InvokeIsolateRequest {
        control_plane_metadata: metadata,
        isolate_input_iscope: request
            .payload_scope
            .map(ez_payload_scope_to_ez_payload_isolate_scope),
        isolate_input: request.payload_data,
    }
}

/// Converts an EzPayloadScope to an EzPayloadIsolateScope.
fn ez_payload_scope_to_ez_payload_isolate_scope(scope: EzPayloadScope) -> EzPayloadIsolateScope {
    let datagram_iscopes = scope
        .datagram_scopes
        .into_iter()
        .map(|ez_data_scope| {
            // Grab only the scope_type from the static_info.
            let scope_type = ez_data_scope
                .static_info
                .as_ref()
                .map_or(DataScopeType::Unspecified.into(), |si| si.scope_type);
            IsolateDataScope { scope_type, mapped_scope_owner: None }
        })
        .collect();
    EzPayloadIsolateScope { datagram_iscopes }
}

/// Proxies responses from a local Isolate (via the junction) back to the remote enforcer.
async fn proxy_to_remote_response(
    invoke_response_result: Result<InvokeIsolateResponse, EzError>,
    to_remote_response_tx: &mpsc::Sender<Result<EzCallResponse, Status>>,
) {
    let response_to_send = match invoke_response_result {
        Ok(invoke_response) => {
            let ez_call_response = invoke_isolate_response_to_ez_call_response(invoke_response);
            Ok(ez_call_response)
        }
        Err(e) => Err(e.to_tonic_status()),
    };
    if let Err(e) = to_remote_response_tx.send(response_to_send).await {
        log::warn!(
            "EzToEz Inbound: Failed to proxy response to remote enforcer. Client channel closed: {e:?}"
        );
    }
}

/// Converts an InvokeIsolateResponse to an EzCallResponse.
#[tracing::instrument(
    name = "Enforcer.EzToEzApi.Inbound.invoke_isolate_response_to_ez_call_response",
    skip(response)
)]
fn invoke_isolate_response_to_ez_call_response(response: InvokeIsolateResponse) -> EzCallResponse {
    let payload_scope = response.isolate_output_iscope.map(|payload_iscope| {
        EzPayloadScope {
            datagram_scopes: payload_iscope
                .datagram_iscopes
                .into_iter()
                .map(|iscope| {
                    // Only the scope_type is populated; other fields are default.
                    data_scope_proto::enforcer::v1::EzDataScope {
                        static_info: Some(EzStaticScopeInfo {
                            scope_type: iscope.scope_type,
                            ..Default::default()
                        }),
                        dynamic_info: None,
                        local_info: None,
                    }
                })
                .collect(),
        }
    });

    EzCallResponse { payload_scope, payload_data: response.isolate_output }
}

/// Launches the gRPC server for the inbound EZ-to-EZ API.
pub async fn launch_server(
    handler: InboundEzToEzHandler,
    address: &str,
    max_decoding_message_size: usize,
) {
    log::info!("Starting inbound EZ-to-EZ server at {}...", address);
    let server_builder = Server::builder().add_service(
        EzToEzApiServer::new(handler).max_decoding_message_size(max_decoding_message_size),
    );

    let server_result = if let Some(path) = address.strip_prefix("unix:") {
        // Attempt to remove old socket file, logging a warning on failure.
        // This is not a critical error, as the subsequent bind will fail with a
        // more specific error if the path is still in use.
        if let Err(e) = std::fs::remove_file(path) {
            log::warn!("Could not remove old UDS socket at {path:?}: {e}. This may be ignored if the file did not exist.");
        }
        let uds_result = UnixListener::bind(path);
        let Ok(uds) = uds_result else {
            log::error!("Failed to bind to UDS at {path:?}: {:?}", uds_result.unwrap_err());
            return;
        };
        let uds_stream = UnixListenerStream::new(uds);
        server_builder.serve_with_incoming(uds_stream).await
    } else {
        log::error!("Invalid inbound EZ-to-EZ address: '{address}'. Address must be a UDS path starting with 'unix:'.");
        return;
    };

    if let Err(e) = server_result {
        log::error!("Inbound EZ-to-EZ Server launch failed: {:?}", e);
    }
}
