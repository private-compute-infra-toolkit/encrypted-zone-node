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

use data_scope::error::DataScopeError;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::request::{
    GetIsolateRequest, GetIsolateScopeRequest, ValidateIsolateRequest,
    ValidateManifestInputScopeRequest, ValidateManifestOutputScopeRequest,
};
use data_scope::requester::DataScopeRequester;
use enforcer_proto::enforcer::v1::ez_isolate_bridge_client::EzIsolateBridgeClient;
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, EzPayloadIsolateScope, InvokeIsolateRequest, InvokeIsolateResponse,
    IsolateState,
};
use ez_error::EzError;
use ez_error_trait::ToEzError;
use hyper_util::rt::TokioIo;
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use shared_memory_manager::SharedMemManager;
use simple_tonic_stream::SimpleStreamingWrapper;
use std::time::Instant;
use tokio::net::UnixStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
pub mod error;
use dashmap::DashMap;
use data_scope::data_scope_validator::{
    enforce_public_api_invoke_isolate_resp_scopes, get_strictest_scope,
    replace_and_enforce_invoke_isolate_resp_scopes,
};
use error::IsolateStatusCode;
use junction_trait::{Junction, JunctionChannels};
use metrics::common::{MetricAttributes, ServiceMetrics};
use metrics::junction::JunctionMetrics;
use opentelemetry::KeyValue;
use state_manager::IsolateStateManager;
use std::sync::Arc;
use tokio::sync::oneshot;

pub(crate) const CHANNEL_SIZE: usize = 128;
const RETRY_COUNT: usize = 60;
const RETRY_DELAY_MILLIS: u64 = 1000;
const LOCAL_HOST_URI_PREFIX: &str = "http://localhost:";

#[derive(Debug)]
struct DestinationIsolateInfo {
    id: IsolateId,
    new_state: Option<IsolateState>,
}

// Context struct for streaming request/response processing
struct RequestContext {
    isolate_id: IsolateId,
    request_source_isolate_id: Option<IsolateId>,
    original_msg_id: u64,
    is_from_public_api: bool,
}

#[derive(Clone, Debug)]
pub struct IsolateJunction {
    isolate_service_mapper: IsolateServiceMapper,
    data_scope_requester: DataScopeRequester,
    shared_mem_manager: SharedMemManager,
    isolate_client_map: Arc<DashMap<IsolateId, EzIsolateBridgeClient<Channel>>>,
    state_manager: IsolateStateManager,
    manifest_validator: ManifestValidator,
    metrics: JunctionMetrics,
}

#[tonic::async_trait]
impl Junction for IsolateJunction {
    // TODO Support session stickiness for each client based on their preference
    // (either at channel level or at request level)
    async fn invoke_isolate(
        &self,
        client_isolate_id_option: Option<IsolateId>,
        mut invoke_isolate_request: InvokeIsolateRequest,
        is_from_public_api: bool,
    ) -> Result<InvokeIsolateResponse, EzError> {
        let metric_attr =
            MetricAttributes::from(invoke_isolate_request.control_plane_metadata.as_ref());
        let _call_tracker = self.metrics.track_call(metric_attr.base());

        let destination_isolate_ids_result =
            self.get_isolate_id_based_on_scope(&invoke_isolate_request).await;
        match destination_isolate_ids_result {
            Ok(destination_isolate_info) => {
                // If the DataScopeManager determined this Isolate should be retired after this
                // request, spawn a task to update its state accordingly.
                self.spawn_isolate_state_update_task(
                    destination_isolate_info.id,
                    destination_isolate_info.new_state,
                );
                let destination_isolate_id = destination_isolate_info.id;
                let masked_id = rand::random::<u64>();
                let original_msg_id =
                    match mask_streaming_msg_id(masked_id, &mut invoke_isolate_request) {
                        Ok(ipc_msg_id) => ipc_msg_id,
                        Err(e) => {
                            self.metrics
                                .record_error(&metric_attr.base(), "mask_streaming_msg_id_error");
                            return Err(e.to_ez_error());
                        }
                    };
                let Some(isolate_client_ref) = self.isolate_client_map.get(&destination_isolate_id)
                else {
                    // TODO Retry connecting to the Isolate or remove it from DSM.
                    self.metrics.record_error(&metric_attr.base(), "invalid_destination_target");
                    log_destination_unavailable_error(invoke_isolate_request);
                    return Err(IsolateStatusCode::DestinationChannelClosed.to_ez_error());
                };
                let mut client = isolate_client_ref.value().clone();
                // Drop the key/value reference immediately after the clone() to release any locks.
                drop(isolate_client_ref);

                self.state_manager.increment_inflight_counter(destination_isolate_info.id).await;
                let isolate_rpc_start_time = Instant::now();
                let invoke_isolate_result = client.invoke_isolate(invoke_isolate_request).await;
                self.metrics
                    .isolate_rpc_duration_sec
                    .record(isolate_rpc_start_time.elapsed().as_secs_f64(), &metric_attr.base());
                self.state_manager.decrement_inflight_counter(destination_isolate_id).await;
                let Ok(invoke_isolate_response) = invoke_isolate_result else {
                    self.metrics.record_error(&metric_attr.base(), "invoke_isolate_error");
                    // TODO add retry send and reset
                    return Err(EzError::Status(invoke_isolate_result.unwrap_err()));
                };
                let mut invoke_isolate_response = invoke_isolate_response.into_inner();
                match invoke_isolate_response.control_plane_metadata.as_mut() {
                    Some(control_plane_metadata) => {
                        control_plane_metadata.ipc_message_id = original_msg_id;
                        if !control_plane_metadata.shared_memory_handles.is_empty() {
                            let mem_share_result = self
                                .handle_shared_memory(
                                    control_plane_metadata,
                                    destination_isolate_id,
                                    client_isolate_id_option,
                                )
                                .await;
                            if mem_share_result.is_err() {
                                log::warn!("MemShare failed: {:?}", mem_share_result);
                                self.metrics.record_error(&metric_attr.base(), "mem_share_failed");
                                return Err(IsolateStatusCode::MemShareFailed.to_ez_error());
                            }
                        }
                        self.validate_invoke_response(
                            destination_isolate_id,
                            &mut invoke_isolate_response,
                            is_from_public_api,
                        )
                        .await?;
                        Ok(invoke_isolate_response)
                    }
                    None => {
                        self.metrics.record_error(&metric_attr.base(), "invalid_request");
                        log::info!("InvokeIsolateResponse missing required ControlPlaneMetadata");
                        // TODO Redact error code if error is from Opaque Isolate having private data.
                        return Err(IsolateStatusCode::MissingControlPlaneMetadata.to_ez_error());
                    }
                }
            }
            Err(e) => {
                self.metrics
                    .record_error(&metric_attr.base(), "get_isolate_id_based_on_scope_error");
                return Err(e.to_ez_error());
            }
        }
    }

    async fn stream_invoke_isolate(
        &self,
        request_source_isolate_id_option: Option<IsolateId>,
        is_from_public_api: bool,
    ) -> JunctionChannels {
        let (client_to_junction_tx, mut client_to_junction_rx) = channel(CHANNEL_SIZE);
        let (junction_to_client_tx, junction_to_client_rx) = channel(CHANNEL_SIZE);

        let junction_channel = JunctionChannels {
            client_to_junction: client_to_junction_tx,
            junction_to_client: junction_to_client_rx,
        };

        let isolate_junction_clone = self.clone();
        let (metrics_context_tx, metrics_context_rx) = oneshot::channel();
        // Spawn proxy task to avoid blocking while we wait for first request
        tokio::spawn(async move {
            // receive initial request to setup long lived stream with Isolate
            let Some(mut initial_invoke_request) = client_to_junction_rx.recv().await else {
                log::info!("Missing initial invoke request");
                let _ = junction_to_client_tx
                    .send(Err(IsolateStatusCode::MissingControlPlaneMetadata.to_ez_error()))
                    .await;
                return;
            };

            let metric_attr =
                MetricAttributes::from(initial_invoke_request.control_plane_metadata.as_ref());
            let call_tracker = isolate_junction_clone.metrics.track_call(metric_attr.base());

            // Send base attributes (no direction) to the response task
            if metrics_context_tx.send((metric_attr.clone(), call_tracker)).is_err() {
                // The receiver was dropped, so the stream is already dead.
                return;
            }

            // use initial request to get IsolateId from DataScopeRequester
            let destination_isolate_info_result =
                isolate_junction_clone.get_isolate_id_based_on_scope(&initial_invoke_request).await;
            let Ok(destination_isolate_info) = destination_isolate_info_result else {
                let _ = junction_to_client_tx
                    .send(Err(destination_isolate_info_result.unwrap_err().to_ez_error()))
                    .await;
                return;
            };

            isolate_junction_clone.spawn_isolate_state_update_task(
                destination_isolate_info.id,
                destination_isolate_info.new_state,
            );

            // Mask the initial request msg_id
            let stream_id = rand::random::<u64>();
            let original_ipc_message_id =
                match mask_streaming_msg_id(stream_id, &mut initial_invoke_request) {
                    Ok(ipc_msg_id) => ipc_msg_id,
                    Err(e) => {
                        // Initial request must have an ipc_message_id
                        let _ = junction_to_client_tx.send(Err(e.to_ez_error())).await;
                        return;
                    }
                };
            // Todo: Potential optimization. This operation involves a lock and can be run in a separate task to avoid
            // blocking the critical path of setting up the stream.
            isolate_junction_clone
                .state_manager
                .increment_inflight_counter(destination_isolate_info.id)
                .await;
            // Using TonicClient, create a new streaming rpc to Isolate
            // and send initial request to Isolate
            let connect_result = isolate_junction_clone
                .establish_isolate_streaming_rpc(
                    destination_isolate_info.id,
                    initial_invoke_request,
                )
                .await;
            let Ok((isolate_tx_channel, invoke_isolate_response_stream)) = connect_result else {
                // If send fails, decrement the counter we just incremented.
                isolate_junction_clone
                    .state_manager
                    .decrement_inflight_counter(destination_isolate_info.id)
                    .await;
                let _ = junction_to_client_tx
                    .send(Err(IsolateStatusCode::DestinationChannelClosed.to_ez_error()))
                    .await;
                return;
            };

            let junction_clone = isolate_junction_clone.clone();
            let junction_to_client_clone = junction_to_client_tx.clone();
            let request_context = RequestContext {
                isolate_id: destination_isolate_info.id,
                request_source_isolate_id: request_source_isolate_id_option,
                original_msg_id: original_ipc_message_id,
                is_from_public_api,
            };
            // spawn response processing task
            tokio::spawn(async move {
                junction_clone
                    .proxy_streaming_isolate_responses(
                        invoke_isolate_response_stream,
                        junction_to_client_clone,
                        request_context,
                        metrics_context_rx,
                    )
                    .await;
            });

            // Proxy subsequent requests
            tokio::spawn(async move {
                isolate_junction_clone
                    .proxy_streaming_isolate_requests(
                        client_to_junction_rx,
                        isolate_tx_channel,
                        junction_to_client_tx,
                        destination_isolate_info.id,
                        stream_id,
                        metric_attr,
                    )
                    .await;
            });
        });

        junction_channel
    }

    async fn connect_isolate(
        &self,
        isolate_id: IsolateId,
        isolate_address: String,
    ) -> anyhow::Result<()> {
        // TODO Make the overall delay configurable
        let retry_strategy = FixedInterval::from_millis(RETRY_DELAY_MILLIS).take(RETRY_COUNT);
        let transport_channel = Retry::spawn(retry_strategy, move || {
            retryable_connect(LOCAL_HOST_URI_PREFIX.to_owned() + &isolate_address.clone())
        })
        .await?;

        let client = EzIsolateBridgeClient::new(transport_channel);
        // Store the `EzIsolateBridgeClient`, we will use this `TonicClient` to make rpc requests
        // to the `Isolate` for each connect_client/streaming_connect.
        self.isolate_client_map.insert(isolate_id, client);

        Ok(())
    }
}

impl IsolateJunction {
    async fn establish_isolate_streaming_rpc(
        &self,
        isolate_id: IsolateId,
        initial_request: InvokeIsolateRequest,
    ) -> anyhow::Result<(Sender<InvokeIsolateRequest>, SimpleStreamingWrapper<InvokeIsolateResponse>)>
    {
        let Some(isolate_client_ref) = self.isolate_client_map.get(&isolate_id) else {
            log::info!("Missing isolate connection from connection map for isolate {}", isolate_id);
            return Err(IsolateStatusCode::InternalError.into());
        };
        let mut client = isolate_client_ref.value().clone();
        // Drop the key/value reference immediately after the `clone()` to release any locks.
        drop(isolate_client_ref);

        let (junction_to_isolate_tx, junction_to_isolate_rx) =
            channel::<InvokeIsolateRequest>(CHANNEL_SIZE);
        let outbound_stream = ReceiverStream::new(junction_to_isolate_rx);

        junction_to_isolate_tx.send(initial_request).await?;

        let inbound = client.stream_invoke_isolate(outbound_stream).await?;
        let invoke_isolate_response_stream: SimpleStreamingWrapper<InvokeIsolateResponse> =
            inbound.into_inner().into();

        Ok((junction_to_isolate_tx, invoke_isolate_response_stream))
    }

    pub fn new(
        data_scope_requester: DataScopeRequester,
        isolate_service_mapper: IsolateServiceMapper,
        shared_mem_manager: SharedMemManager,
        state_manager: IsolateStateManager,
        manifest_validator: ManifestValidator,
    ) -> Self {
        let metrics = JunctionMetrics::default();
        Self {
            isolate_service_mapper,
            data_scope_requester,
            shared_mem_manager,
            isolate_client_map: Arc::new(DashMap::new()),
            state_manager,
            manifest_validator,
            metrics,
        }
    }

    fn spawn_isolate_state_update_task(
        &self,
        isolate_id: IsolateId,
        new_state: Option<IsolateState>,
    ) {
        if let Some(state) = new_state {
            let state_manager = self.state_manager.clone();
            tokio::spawn(async move {
                if let Err(e) = state_manager.update_state(isolate_id, state).await {
                    log::warn!(
                        "Failed to update isolate {} state to {:?}: {}",
                        isolate_id,
                        state,
                        e
                    );
                }
            });
        }
    }

    // Proxy InvokeIsolateRequests from client to Isolate for streaming rpc
    async fn validate_streaming_request_scope(
        &self,
        invoke_isolate_request: &InvokeIsolateRequest,
        isolate_id: IsolateId,
    ) -> Result<(), DataScopeError> {
        let Some(iscopes) = invoke_isolate_request.isolate_input_iscope.as_ref() else {
            return Err(DataScopeError::InvalidDataScopeType);
        };
        let validate_scope_request = create_validate_isolate_request(isolate_id, iscopes);
        self.manifest_validator
            .validate_input_scope(ValidateManifestInputScopeRequest {
                binary_services_index: validate_scope_request
                    .isolate_id
                    .get_binary_services_index(),
                requested_scope: validate_scope_request.requested_scope,
            })
            .await?;
        self.data_scope_requester.validate_isolate_scope(validate_scope_request).await?;
        Ok(())
    }

    // Proxy InvokeIsolateRequests from client to Isolate
    async fn proxy_streaming_isolate_requests(
        self,
        mut client_to_junction_rx: Receiver<InvokeIsolateRequest>,
        isolate_tx_channel: Sender<InvokeIsolateRequest>,
        junction_to_client_tx: Sender<Result<InvokeIsolateResponse, EzError>>,
        isolate_id: IsolateId,
        stream_id: u64,
        metric_attr: MetricAttributes,
    ) {
        while let Some(mut invoke_isolate_request) = client_to_junction_rx.recv().await {
            let _timer = self.metrics.track_message_processing(metric_attr.request());
            let validate_scope_result =
                self.validate_streaming_request_scope(&invoke_isolate_request, isolate_id).await;
            let Ok(_) = validate_scope_result else {
                if junction_to_client_tx
                    .send(Err(validate_scope_result.unwrap_err().to_ez_error()))
                    .await
                    .is_err()
                {
                    // Break the stream if junction fails to send the error response
                    log::warn!("Failed to send error response from the junction to the client");
                    break;
                }
                continue;
            };
            match mask_streaming_msg_id(stream_id, &mut invoke_isolate_request) {
                Ok(_) => {}
                Err(e) => {
                    // This case is only hit when ControlPlaneMetadata is empty which
                    // is expected on initial connect when we send an empty request
                    log::info!("failed to mask msg_id: {:?}", e);
                }
            }
            let send_result = isolate_tx_channel.send(invoke_isolate_request).await;
            // break if we are unable to route request to Isolate
            if send_result.is_err() {
                let _ = junction_to_client_tx
                    .send(Err(IsolateStatusCode::DestinationChannelClosed.to_ez_error()))
                    .await;
                // TODO add retry send and reset
                break;
            };
        }
    }

    // Proxy InvokeIsolateResponses from Isolate to client for streaming rpc
    async fn proxy_streaming_isolate_responses(
        self,
        mut invoke_isolate_response_stream: SimpleStreamingWrapper<InvokeIsolateResponse>,
        junction_to_client_tx: Sender<Result<InvokeIsolateResponse, EzError>>,
        request_context: RequestContext,
        metrics_context_rx: oneshot::Receiver<(
            MetricAttributes,
            metrics::common::CallTracker<JunctionMetrics>,
        )>,
    ) {
        let (metric_attr, _call_tracker) = match metrics_context_rx.await {
            Ok(data) => data,
            Err(_) => return,
        };

        while let Some(invoke_isolate_response) = invoke_isolate_response_stream.message().await {
            let _timer = self.metrics.track_message_processing(metric_attr.response());
            self.process_invoke_isolate_response(
                invoke_isolate_response,
                junction_to_client_tx.clone(),
                &request_context,
                &metric_attr.base(),
            )
            .await;
        }
        // Decrement the inflight counter once the streaming RPC completes.
        self.state_manager.decrement_inflight_counter(request_context.isolate_id).await;
    }

    async fn process_invoke_isolate_response(
        &self,
        mut invoke_isolate_response: InvokeIsolateResponse,
        junction_to_client_tx: Sender<Result<InvokeIsolateResponse, EzError>>,
        request_context: &RequestContext,
        attributes: &[KeyValue],
    ) {
        let isolate_id = request_context.isolate_id;
        let original_msg_id = request_context.original_msg_id;
        let request_source_isolate_id_option = request_context.request_source_isolate_id;
        let is_from_public_api = request_context.is_from_public_api;
        match invoke_isolate_response.control_plane_metadata.as_mut() {
            Some(control_plane_metadata) => {
                control_plane_metadata.ipc_message_id = original_msg_id;
                if !control_plane_metadata.shared_memory_handles.is_empty() {
                    let mem_share_result = self
                        .handle_shared_memory(
                            control_plane_metadata,
                            isolate_id,
                            request_source_isolate_id_option,
                        )
                        .await;
                    if mem_share_result.is_err() {
                        log::info!("MemShare failed: {:?}", mem_share_result);
                        self.metrics.record_error(attributes, "mem_share_failed");
                        // Note this error goes to the client, so we should not provide any details
                        // about the error.
                        let _ = junction_to_client_tx
                            .send(Err(IsolateStatusCode::MemShareFailed.to_ez_error()))
                            .await;
                    }
                }
                if let Err(e) = self
                    .validate_invoke_response(
                        isolate_id,
                        &mut invoke_isolate_response,
                        is_from_public_api,
                    )
                    .await
                {
                    self.metrics.record_error(attributes, "validate_invoke_response_error");
                    log::info!("Response validation failed: {:?}", e);
                    let _ = junction_to_client_tx.send(Err(e)).await;
                } else {
                    let _ = junction_to_client_tx.send(Ok(invoke_isolate_response)).await;
                }
            }
            None => {
                // This case only happens during the initial connect
                // when we send the Isolate an empty request
                log::info!("received response with no metadata");
            }
        }
    }

    async fn validate_invoke_response(
        &self,
        isolate_id: IsolateId,
        resp: &mut InvokeIsolateResponse,
        is_from_public_api: bool,
    ) -> Result<(), EzError> {
        if !isolate_id.is_ratified_isolate() {
            let current_scope = self
                .data_scope_requester
                .get_isolate_scope(GetIsolateScopeRequest { isolate_id })
                .await
                .map_err(|e| e.to_ez_error())?
                .current_scope;
            replace_and_enforce_invoke_isolate_resp_scopes(resp, current_scope, is_from_public_api)
                .map_err(|e| e.to_ez_error())?;
        } else if is_from_public_api {
            enforce_public_api_invoke_isolate_resp_scopes(resp).map_err(|e| e.to_ez_error())?;
        }
        if let Some(response_iscopes) = &resp.isolate_output_iscope {
            self.manifest_validator
                .validate_output_scope(ValidateManifestOutputScopeRequest {
                    binary_services_index: isolate_id.get_binary_services_index(),
                    emitted_scope: get_strictest_scope(response_iscopes),
                })
                .await
                .map_err(|e| e.to_ez_error())?;
        } else if resp.isolate_output.is_some() {
            // Only return an error if there are no scopes, but there is a payload
            return Err(DataScopeError::InvalidDataScopeType.to_ez_error());
        };
        Ok(())
    }

    /// Gets ([IsolateId], [IsolateServiceIndex]) by calling [IsolateServiceMapper] & [DataScopeRequester].
    async fn get_isolate_id_based_on_scope(
        &self,
        request: &InvokeIsolateRequest,
    ) -> Result<DestinationIsolateInfo, DataScopeError> {
        let Some(request_iscopes) = &request.isolate_input_iscope else {
            return Err(DataScopeError::InvalidDataScopeType);
        };
        let Some(control_plane_metadata) = &request.control_plane_metadata else {
            return Err(DataScopeError::MissingControlPlaneMetadata);
        };

        let isolate_service_info = create_isolate_service_info(control_plane_metadata);
        let Some(binary_services_index) =
            self.isolate_service_mapper.get_binary_index(&isolate_service_info).await
        else {
            return Err(DataScopeError::InvalidIsolateServiceIndex);
        };

        let get_isolate_request =
            create_get_isolate_request(binary_services_index, request_iscopes);
        self.manifest_validator
            .validate_input_scope(ValidateManifestInputScopeRequest {
                binary_services_index: get_isolate_request.binary_services_index,
                requested_scope: get_isolate_request.data_scope_type,
            })
            .await?;
        let get_isolate_response_result =
            self.data_scope_requester.get_isolate(get_isolate_request).await;
        match get_isolate_response_result {
            Ok(get_isolate_response) => Ok(DestinationIsolateInfo {
                id: get_isolate_response.isolate_id,
                new_state: get_isolate_response.new_state,
            }),
            Err(e) => Err(e),
        }
    }

    async fn handle_shared_memory(
        &self,
        control_plane_metadata: &ControlPlaneMetadata,
        source_isolate_id: IsolateId,
        destination_isolate_id_option: Option<IsolateId>,
    ) -> anyhow::Result<()> {
        let Some(destination_isolate_id) = destination_isolate_id_option else {
            return Err(anyhow::anyhow!("missing destination_isolate_id for file share"));
        };
        for file_handle in &control_plane_metadata.shared_memory_handles {
            self.shared_mem_manager
                .share_file(source_isolate_id, destination_isolate_id, file_handle.to_string())
                .await?;
        }
        Ok(())
    }
}

async fn retryable_connect(address: String) -> anyhow::Result<Channel, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(address)?;
    endpoint
        .connect_with_connector(service_fn(|uri: Uri| async move {
            let path = uri.path();
            Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(path).await?))
        }))
        .await
}

fn mask_streaming_msg_id(
    stream_id: u64,
    invoke_isolate_request: &mut InvokeIsolateRequest,
) -> Result<u64, IsolateStatusCode> {
    match invoke_isolate_request.control_plane_metadata.as_mut() {
        Some(control_plane_metadata) => {
            let original_msg_id = control_plane_metadata.ipc_message_id;
            control_plane_metadata.ipc_message_id = stream_id;
            Ok(original_msg_id)
        }
        None => Err(IsolateStatusCode::MissingControlPlaneMetadata),
    }
}

fn create_isolate_service_info(
    control_plane_metadata: &ControlPlaneMetadata,
) -> IsolateServiceInfo {
    IsolateServiceInfo {
        operator_domain: control_plane_metadata.destination_operator_domain.clone(),
        service_name: control_plane_metadata.destination_service_name.clone(),
    }
}

fn create_get_isolate_request(
    binary_services_index: BinaryServicesIndex,
    iscopes: &EzPayloadIsolateScope,
) -> GetIsolateRequest {
    let strictest_scope = get_strictest_scope(iscopes);
    GetIsolateRequest { binary_services_index, data_scope_type: strictest_scope }
}

fn create_validate_isolate_request(
    isolate_id: IsolateId,
    iscopes: &EzPayloadIsolateScope,
) -> ValidateIsolateRequest {
    let strictest_scope = get_strictest_scope(iscopes);
    ValidateIsolateRequest { isolate_id, requested_scope: strictest_scope }
}

fn log_destination_unavailable_error(invoke_isolate_request: InvokeIsolateRequest) {
    let Some(control_plane) = invoke_isolate_request.control_plane_metadata else {
        return;
    };
    log::info!(
        "Invalid destination isolate operator-domain {} service_name {}",
        control_plane.destination_operator_domain,
        control_plane.destination_service_name
    );
}
