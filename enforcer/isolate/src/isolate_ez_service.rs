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

use data_scope::data_scope_validator::{
    get_strictest_scope, replace_and_validate_invoke_ez_request_scopes,
};
use data_scope::manifest_validator::ManifestValidator;
use data_scope::request::{
    FreezeIsolateScopeRequest, GetIsolateScopeRequest, ValidateBackendDependencyRequest,
    ValidateIsolateRequest,
};
use data_scope::requester::DataScopeRequester;
use enforcer_proto::data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::isolate_ez_bridge_server::IsolateEzBridge;
use enforcer_proto::enforcer::v1::{
    CreateMemshareRequest, CreateMemshareResponse, InvokeEzRequest, InvokeEzResponse,
    InvokeIsolateRequest, InvokeIsolateResponse, IsolateState, IsolateStatus,
    NotifyIsolateStateRequest, NotifyIsolateStateResponse, PollIsolateStateRequest,
    PollIsolateStateResponse,
};
use external_proxy_connector::ExternalProxyChannel;
use futures::stream::{Stream, StreamExt};
use grpc_connector::try_parse_grpc_timeout;
use interceptor::{Interceptor, RequestType};
use isolate_info::{IsolateId, IsolateServiceInfo, Route};
use isolate_service_mapper::IsolateServiceMapper;
use junction_trait::Junction;
use metrics::common::{MetricAttributes, ServiceMetrics};
use metrics::isolate_ez_service::IsolateEzServiceMetrics;
use metrics::observed_stream;
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use shared_memory_manager::SharedMemManager;
use simple_tonic_stream::SimpleStreamingWrapper;
use state_manager::IsolateStateManager;
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

type InvokeEzResult = Result<Response<InvokeEzResponse>, Status>;

// Buffer size for each connection b/w Streaming API and each Isolate
const CHANNEL_SIZE: usize = 256;

pub struct IsolateEzBridgeDependencies {
    pub isolate_id: IsolateId,
    pub isolate_junction: Box<dyn Junction>,
    pub isolate_state_manager: IsolateStateManager,
    pub shared_memory_manager: SharedMemManager,
    pub external_proxy_connector: Option<Box<dyn ExternalProxyChannel>>,
    pub isolate_service_mapper: IsolateServiceMapper,
    pub manifest_validator: ManifestValidator,
    pub data_scope_requester: DataScopeRequester,
    pub ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>>,
    pub interceptor: Interceptor,
}
/// Service Impl for IsolateEzBridge.
#[derive(Debug)]
pub struct IsolateEzBridgeService {
    isolate_id: IsolateId,
    isolate_junction: Box<dyn Junction>,
    isolate_state_manager: IsolateStateManager,
    shared_memory_manager: SharedMemManager,
    external_proxy_connector: Option<Box<dyn ExternalProxyChannel>>,
    isolate_service_mapper: IsolateServiceMapper,
    manifest_validator: ManifestValidator,
    data_scope_requester: DataScopeRequester,
    ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>>,
    interceptor: Interceptor,
    metrics: IsolateEzServiceMetrics,
}

impl IsolateEzBridgeService {
    pub fn new(deps: IsolateEzBridgeDependencies) -> Self {
        let metrics = IsolateEzServiceMetrics::default();
        Self {
            isolate_id: deps.isolate_id,
            isolate_junction: deps.isolate_junction,
            isolate_state_manager: deps.isolate_state_manager,
            shared_memory_manager: deps.shared_memory_manager,
            external_proxy_connector: deps.external_proxy_connector,
            isolate_service_mapper: deps.isolate_service_mapper,
            manifest_validator: deps.manifest_validator,
            data_scope_requester: deps.data_scope_requester,
            ez_to_ez_outbound_handler: deps.ez_to_ez_outbound_handler,
            interceptor: deps.interceptor,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl IsolateEzBridge for IsolateEzBridgeService {
    type StreamInvokeEzStream = observed_stream::ObservedResponseStream<
        ReceiverStream<Result<InvokeEzResponse, Status>>,
        IsolateEzServiceMetrics,
    >;
    async fn stream_invoke_ez(
        &self,
        request: Request<Streaming<InvokeEzRequest>>,
    ) -> Result<Response<Self::StreamInvokeEzStream>, Status> {
        let (invoke_ez_response_tx, invoke_ez_response_rx) = channel(CHANNEL_SIZE);
        let invoke_ez_response_stream = ReceiverStream::new(invoke_ez_response_rx);

        let (invoke_ez_request_stream, invoke_ez_response_stream) = observed_stream::pair_deferred(
            request.into_inner(),
            invoke_ez_response_stream,
            self.metrics.clone(),
        );

        let stream_handler = StreamHandler::new(self, invoke_ez_response_tx);
        tokio::spawn(async move {
            stream_handler.process_invoke_ez_requests(invoke_ez_request_stream).await;
        });
        Ok(Response::new(invoke_ez_response_stream))
    }

    async fn invoke_ez(&self, request: Request<InvokeEzRequest>) -> InvokeEzResult {
        let timeout = try_parse_grpc_timeout(request.metadata()).unwrap_or(None);
        let mut req = request.into_inner();

        self.interceptor.replace_with_interceptor(&mut req, RequestType::Unary).await;

        let restrictions_enforcer = RestrictionsEnforcer {
            isolate_id: self.isolate_id,
            isolate_service_mapper: self.isolate_service_mapper.clone(),
            manifest_validator: self.manifest_validator.clone(),
            data_scope_requester: self.data_scope_requester.clone(),
            response_tx: None,
        };

        let route_result = restrictions_enforcer.validate_invoke_ez_req(&mut req).await;

        let metric_attr = MetricAttributes::from(req.control_plane_metadata.as_ref())
            .with_attribute(
                "route_type",
                route_result.as_ref().unwrap_or(&Route::Unknown).as_str_name(),
            );

        let _call_tracker = self.metrics.track_call(metric_attr.base());

        let result = match route_result {
            Ok(route) => {
                let mut result: InvokeEzResult = match route {
                    Route::External => {
                        self.handle_external_request(req, &metric_attr, timeout).await
                    }
                    Route::Internal => {
                        self.handle_internal_request(req, &metric_attr, timeout).await
                    }
                    Route::Remote => self.handle_remote_request(req, &metric_attr, timeout).await,
                    Route::Unknown => {
                        self.metrics.record_error(&metric_attr.base(), "unknown_route");
                        Err(Status::not_found(
                            "The requested service is not registered or the request is malformed.",
                        ))
                    }
                };
                if let Ok(response) = &result {
                    if let Err(e) =
                        restrictions_enforcer.validate_invoke_ez_resp(response.get_ref()).await
                    {
                        self.metrics
                            .record_error(&metric_attr.base(), "response_validation_failed");
                        result = Err(e);
                    }
                }
                result
            }
            Err(status) => {
                self.metrics.record_error(&metric_attr.base(), "validation_failed");
                Err(status)
            }
        };

        result
    }

    type CreateMemshareStream = ReceiverStream<Result<CreateMemshareResponse, Status>>;
    async fn create_memshare(
        &self,
        request: Request<Streaming<CreateMemshareRequest>>,
    ) -> Result<Response<Self::CreateMemshareStream>, Status> {
        let mut mem_share_request_simple_stream: SimpleStreamingWrapper<CreateMemshareRequest> =
            request.into_inner().into();
        let (mem_share_response_tx, mem_share_response_rx) = channel(CHANNEL_SIZE);
        let state_manager_clone = self.isolate_state_manager.clone();
        let mem_share_manager_clone = self.shared_memory_manager.clone();
        let isolate_id = self.isolate_id;

        tokio::spawn(async move {
            while let Some(mem_share_req) = mem_share_request_simple_stream.message().await {
                // Freeze the DataScope and then Create MemShare-able file
                let freeze_result = state_manager_clone
                    .freeze_scope(FreezeIsolateScopeRequest { isolate_id })
                    .await;

                let mem_share_response = if let Err(e) = freeze_result {
                    CreateMemshareResponse {
                        shared_memory_handle: "".to_string(),
                        status: Some(IsolateStatus {
                            code: 7, //grpc code for PERMISSION_DENIED
                            message: e.to_string(),
                        }),
                    }
                } else {
                    mem_share_manager_clone.create_shared_mem_file(isolate_id, mem_share_req).await
                };

                let send_result = mem_share_response_tx.send(Ok(mem_share_response)).await;
                if send_result.is_err() {
                    log::error!("Error while sending MemShareResponse to Isolate");
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(mem_share_response_rx)))
    }

    type NotifyIsolateStateStream = ReceiverStream<Result<NotifyIsolateStateResponse, Status>>;
    async fn notify_isolate_state(
        &self,
        request: Request<Streaming<NotifyIsolateStateRequest>>,
    ) -> Result<Response<Self::NotifyIsolateStateStream>, Status> {
        let mut notify_state_request_simple_stream: SimpleStreamingWrapper<
            NotifyIsolateStateRequest,
        > = request.into_inner().into();
        let (notify_state_response_tx, notify_state_response_rx) = channel(CHANNEL_SIZE);

        let state_manager_clone = self.isolate_state_manager.clone();
        let isolate_id = self.isolate_id;
        tokio::spawn(async move {
            while let Some(notify_state_req) = notify_state_request_simple_stream.message().await {
                let new_state = IsolateState::try_from(notify_state_req.new_isolate_state)
                    .unwrap_or(IsolateState::Unspecified);

                let state_update_result =
                    state_manager_clone.update_state(isolate_id, new_state).await;

                if state_update_result.is_err() {
                    log::warn!("Warning: {:?}", state_update_result.err());
                    let _ = notify_state_response_tx
                        .send(Err(Status::aborted("Failed to update isolate state")))
                        .await;
                }

                let _ = notify_state_response_tx.send(Ok(NotifyIsolateStateResponse {})).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(notify_state_response_rx)))
    }

    async fn poll_isolate_state(
        &self,
        _request: Request<PollIsolateStateRequest>,
    ) -> Result<Response<PollIsolateStateResponse>, Status> {
        let state = self
            .isolate_state_manager
            .get_isolate_state(self.isolate_id)
            .unwrap_or(IsolateState::Unspecified);
        Ok(Response::new(PollIsolateStateResponse { isolate_state: state.into() }))
    }
}

impl IsolateEzBridgeService {
    // Helper for Route::External
    async fn handle_external_request(
        &self,
        req: InvokeEzRequest,
        metric_attr: &MetricAttributes,
        timeout: Option<std::time::Duration>,
    ) -> InvokeEzResult {
        log::debug!("Routing external call for isolate {}", self.isolate_id);

        // Check if the connector is existent
        let connector = match get_connector(&self.external_proxy_connector, self.isolate_id) {
            Ok(c) => c,
            Err(boxed_status) => {
                let status = *boxed_status;
                log::error!("{}", status.message());
                self.metrics.record_error(&metric_attr.base(), "connector_not_found");
                return Err(status);
            }
        };

        // Forward to the request to the ExternalProxyConnector and to the proxy.
        match connector.proxy_external(self.isolate_id, req, timeout).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                log::error!("External unary call failed: {}", e);
                self.metrics.record_error(&metric_attr.base(), "proxy_external_failed");
                Err(Status::internal(format!("Error from proxy: {}", e)))
            }
        }
    }

    // Helper for Route::Internal
    async fn handle_internal_request(
        &self,
        req: InvokeEzRequest,
        metric_attr: &MetricAttributes,
        timeout: Option<std::time::Duration>,
    ) -> InvokeEzResult {
        log::debug!("Routing internal call for isolate {}", self.isolate_id);
        let invoke_isolate_result = self
            .isolate_junction
            .invoke_isolate(Some(self.isolate_id), convert_to_isolate_request(req), false, timeout)
            .await;

        match invoke_isolate_result {
            Ok(invoke_isolate_response) => {
                Ok(Response::new(convert_to_invoke_ez_response(invoke_isolate_response)))
            }
            Err(_) => {
                self.metrics.record_error(&metric_attr.base(), "junction_invoke_failed");
                Err(Status::internal("Failed to reach Junction"))
            }
        }
    }

    // Helper for Route::Remote
    async fn handle_remote_request(
        &self,
        req: InvokeEzRequest,
        metric_attr: &MetricAttributes,
        timeout: Option<std::time::Duration>,
    ) -> InvokeEzResult {
        log::debug!("Routing remote call for isolate {}", self.isolate_id);
        let Some(handler) = &self.ez_to_ez_outbound_handler else {
            self.metrics.record_error(&metric_attr.base(), "remote_handler_not_configured");
            return Err(Status::failed_precondition(
                "Remote calls are not configured for this enforcer.",
            ));
        };

        handler.remote_invoke(req, timeout).await.map(Response::new).map_err(|e| {
            self.metrics.record_error(&metric_attr.base(), "remote_invoke_failed");
            Status::internal(format!("Error from remote enforcer: {}", e))
        })
    }
}

#[derive(Debug, Clone)]
struct StreamHandler {
    isolate_id: IsolateId,
    isolate_junction: Box<dyn Junction>,
    external_proxy_connector: Option<Box<dyn ExternalProxyChannel>>,
    restrictions_enforcer: RestrictionsEnforcer,
    ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>>,
    response_tx: Sender<Result<InvokeEzResponse, Status>>,
    metrics: IsolateEzServiceMetrics,
    interceptor: Interceptor,
}

impl StreamHandler {
    fn new(
        service: &IsolateEzBridgeService,
        response_tx: Sender<Result<InvokeEzResponse, Status>>,
    ) -> Self {
        let restrictions_enforcer = RestrictionsEnforcer {
            isolate_id: service.isolate_id,
            isolate_service_mapper: service.isolate_service_mapper.clone(),
            manifest_validator: service.manifest_validator.clone(),
            data_scope_requester: service.data_scope_requester.clone(),
            response_tx: Some(response_tx.clone()),
        };

        Self {
            isolate_id: service.isolate_id,
            isolate_junction: service.isolate_junction.clone(),
            external_proxy_connector: service.external_proxy_connector.clone(),
            restrictions_enforcer,
            ez_to_ez_outbound_handler: service.ez_to_ez_outbound_handler.clone(),
            response_tx,
            metrics: service.metrics.clone(),
            interceptor: service.interceptor.clone(),
        }
    }

    async fn process_invoke_ez_requests<S>(
        &self,
        mut invoke_req_stream: observed_stream::ObservedRequestStream<S, IsolateEzServiceMetrics>,
    ) where
        S: Stream<Item = Result<InvokeEzRequest, Status>> + Unpin + Send + 'static,
    {
        let Some(mut first_req) = invoke_req_stream.next().await else {
            log::error!(
                "Failed to receive first request in StreamInvokeEz for isolate {}",
                self.isolate_id
            );
            self.metrics.record_error(&[], "missing_initial_request");
            return;
        };

        self.interceptor.replace_with_interceptor(&mut first_req, RequestType::Streaming).await;

        let route_result =
            self.restrictions_enforcer.stream_validate_invoke_ez_req(&mut first_req).await;

        let is_validation_error = route_result.is_err();
        let route = route_result.unwrap_or(Route::Unknown);

        // Complete activation with determined route
        let attributes = MetricAttributes::from(first_req.control_plane_metadata.as_ref())
            .with_attribute("route_type", route.as_str_name());
        invoke_req_stream.finalize_attributes(attributes);

        if is_validation_error {
            self.metrics
                .record_error(invoke_req_stream.attributes().base().as_ref(), "validation_failed");
        }

        match route {
            Route::External => {
                self.handle_external_stream(first_req, invoke_req_stream).await;
            }
            Route::Internal => {
                self.handle_internal_stream(first_req, invoke_req_stream).await;
            }
            Route::Remote => {
                self.handle_remote_stream(first_req, invoke_req_stream).await;
            }
            Route::Unknown => {
                self.handle_unknown_stream().await;
            }
        }
    }

    async fn handle_remote_stream<S>(
        &self,
        first_req: InvokeEzRequest,
        mut invoke_req_stream: observed_stream::ObservedRequestStream<S, IsolateEzServiceMetrics>,
    ) where
        S: Stream<Item = Result<InvokeEzRequest, Status>> + Unpin + Send + 'static,
    {
        let metric_attr = invoke_req_stream.attributes().clone();

        let handler = match self.ez_to_ez_outbound_handler {
            Some(ref h) => h,
            None => {
                let _ = self
                    .response_tx
                    .send(Err(Status::failed_precondition(
                        "Remote calls are not configured for this enforcer.",
                    )))
                    .await;
                log::error!(
                    "Received a remote request for isolate {}, but no remote handler is configured.",
                    self.isolate_id
                );
                return;
            }
        };

        // Create the channel to send requests to the outbound handler.
        let (to_outbound_tx, from_local_rx) = channel(CHANNEL_SIZE);

        // Task to proxy requests from the local Isolate to the remote enforcer.
        let restrictions_enforcer = self.restrictions_enforcer.clone();
        let metrics = self.metrics.clone();
        let metric_attr_req = metric_attr.clone();

        tokio::spawn(async move {
            {
                // Send the first request that we already consumed.
                let _timer = metrics.track_message_processing(metric_attr_req.request());
                if to_outbound_tx.send(first_req).await.is_err() {
                    log::error!("Remote enforcer request channel closed before first message.");
                    return;
                }
            }

            // Send the rest of the requests from the stream.
            while let Some(mut req) = invoke_req_stream.next().await {
                let _timer = metrics.track_message_processing(metric_attr_req.request());
                if restrictions_enforcer.stream_validate_invoke_ez_req(&mut req).await.is_err() {
                    log::error!("Remote enforcer request channel closed before first message.");
                    return;
                }
                if to_outbound_tx.send(req).await.is_err() {
                    log::error!("Remote enforcer request channel closed.");
                    break;
                }
            }
        });

        let Ok(mut from_outbound_rx) = handler.remote_streaming_connect(from_local_rx).await else {
            // TODO: We should send back the actual error to the caller.
            let _ =
                self.response_tx.send(Err(Status::internal("Remote RPC connection failed."))).await;
            return;
        };

        // Task to proxy responses from the remote enforcer to the local Isolate.
        let restrictions_enforcer = self.restrictions_enforcer.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            while let Some(response_result) = from_outbound_rx.recv().await {
                let _timer = metrics.track_message_processing(metric_attr.response());
                if restrictions_enforcer
                    .stream_validate_invoke_ez_resp(
                        response_result.map_err(|e| Status::internal(e.to_string())),
                    )
                    .await
                    .is_err()
                {
                    log::info!("Client response channel closed.");
                    break;
                }
            }
        });
    }

    async fn handle_external_stream<S>(
        &self,
        first_req: InvokeEzRequest,
        mut invoke_req_stream: observed_stream::ObservedRequestStream<S, IsolateEzServiceMetrics>,
    ) where
        S: Stream<Item = Result<InvokeEzRequest, Status>> + Unpin + Send + 'static,
    {
        let metric_attr = invoke_req_stream.attributes().clone();

        log::debug!("Routing external stream for isolate {}", self.isolate_id);

        let connector = match get_connector(&self.external_proxy_connector, self.isolate_id) {
            Ok(c) => c,
            Err(status) => {
                log::error!("{}", status.message());
                let _ = self.response_tx.send(Err(*status)).await;
                return;
            }
        };

        // Create a channel to pipe all requests to the connector.
        let (to_connector_tx, to_connector_rx) = channel(CHANNEL_SIZE);

        // Spawn a task to pipe ALL requests (including the first one) to the connector.
        let restrictions_enforcer = self.restrictions_enforcer.clone();
        let metrics = self.metrics.clone();
        let metric_attr_req = metric_attr.clone();

        tokio::spawn(async move {
            {
                // Send the first request that we already consumed.
                let _timer = metrics.track_message_processing(metric_attr_req.request());
                if let Err(e) = to_connector_tx.send(first_req).await {
                    log::error!(
                        "Failed to send the first request to the connector. Error: {:?} Isolate Id: {:?}",
                        e,
                        restrictions_enforcer.isolate_id
                    );
                    // The channel likely closed or encountered another issue, so further operations will fail.
                    return;
                }
            }

            // Send the rest of the requests from the stream.
            while let Some(mut req) = invoke_req_stream.next().await {
                let _timer = metrics.track_message_processing(metric_attr_req.request());
                if restrictions_enforcer.stream_validate_invoke_ez_req(&mut req).await.is_err() {
                    log::error!("Error while enforcing scopes InvokeEzRequest");
                    return;
                }
                if let Err(e) = to_connector_tx.send(req).await {
                    log::error!(
                        "Failed to send a subsequent request to the connector. Channel error: {:?} Isolate Id: {:?}",
                        e,
                        restrictions_enforcer.isolate_id
                    );
                    // The connector channel is closed or in an error state, so break the loop.
                    break;
                }
            }
            log::info!("Finished piping requests to the external connector stream.");
        });

        // Get the response stream from the connector.
        let mut from_connector_stream =
            match connector.stream_proxy_external(self.isolate_id, to_connector_rx).await {
                Ok(stream) => stream,
                Err(e) => {
                    let _ = self
                        .response_tx
                        .send(Err(Status::internal(format!("Proxy RPC failed: {}", e))))
                        .await;
                    return;
                }
            };

        // Spawn a task to pipe all responses from the connector back to the client.
        let restrictions_enforcer = self.restrictions_enforcer.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            while let Some(response) = from_connector_stream.recv().await {
                let _timer = metrics.track_message_processing(metric_attr.response());
                // The connector now sends InvokeEzResponse directly, with errors inside the status field.
                if restrictions_enforcer.stream_validate_invoke_ez_resp(Ok(response)).await.is_err()
                {
                    log::info!("Client response channel closed.");
                    break;
                }
            }
        });
    }

    async fn handle_internal_stream<S>(
        &self,
        first_req: InvokeEzRequest,
        mut invoke_req_stream: observed_stream::ObservedRequestStream<S, IsolateEzServiceMetrics>,
    ) where
        S: Stream<Item = Result<InvokeEzRequest, Status>> + Unpin + Send + 'static,
    {
        let metric_attr = invoke_req_stream.attributes().clone();

        let junction_channel =
            self.isolate_junction.stream_invoke_isolate(Some(self.isolate_id), false).await;
        let to_junction = junction_channel.client_to_junction;
        let mut from_junction = junction_channel.junction_to_client;

        let restrictions_enforcer = self.restrictions_enforcer.clone();
        let metrics = self.metrics.clone();
        let metric_attr_req = metric_attr.clone();

        tokio::spawn(async move {
            // First handle the initial request.
            {
                let _timer = metrics.track_message_processing(metric_attr_req.request());
                if to_junction.send(convert_to_isolate_request(first_req)).await.is_err() {
                    log::error!("Error while sending first InvokeEzRequest to junction");
                    return; // Exit this spawned task if sending the first request fails.
                }
            }

            // Handle the rest of the streaming requests.
            while let Some(mut invoke_ez_req) = invoke_req_stream.next().await {
                let _timer = metrics.track_message_processing(metric_attr_req.request());
                if restrictions_enforcer
                    .stream_validate_invoke_ez_req(&mut invoke_ez_req)
                    .await
                    .is_err()
                {
                    log::error!("Error while enforcing scopes InvokeEzRequest");
                    return;
                }
                if to_junction.send(convert_to_isolate_request(invoke_ez_req)).await.is_err() {
                    log::error!("Error while sending InvokeEzRequest to junction");
                    break;
                }
            }
        });

        let restrictions_enforcer = self.restrictions_enforcer.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            while let Some(invoke_isolate_response) = from_junction.recv().await {
                let _timer = metrics.track_message_processing(metric_attr.response());
                let response = invoke_isolate_response
                    .map(convert_to_invoke_ez_response)
                    .map_err(|e| Status::internal(e.to_string()));
                if restrictions_enforcer.stream_validate_invoke_ez_resp(response).await.is_err() {
                    log::warn!("Error while sending InvokeEzResponse to Isolate");
                    break;
                }
            }
        });
    }

    async fn handle_unknown_stream(&self) {
        let _ = self
            .response_tx
            .send(Err(Status::not_found(
                "The requested service is not registered or the request is malformed.",
            )))
            .await;
    }
}

#[derive(Debug, Clone)]
struct RestrictionsEnforcer {
    isolate_id: IsolateId,
    isolate_service_mapper: IsolateServiceMapper,
    data_scope_requester: DataScopeRequester,
    manifest_validator: ManifestValidator,
    response_tx: Option<Sender<Result<InvokeEzResponse, Status>>>,
}

impl RestrictionsEnforcer {
    async fn validate_invoke_ez_req(&self, req: &mut InvokeEzRequest) -> Result<Route, Status> {
        if !self.isolate_id.is_ratified_isolate() {
            let current_scope_result = self
                .data_scope_requester
                .get_isolate_scope(GetIsolateScopeRequest { isolate_id: self.isolate_id })
                .await;
            match current_scope_result {
                Ok(get_isolate_scope_response) => {
                    let status = replace_and_validate_invoke_ez_request_scopes(
                        req,
                        get_isolate_scope_response.current_scope,
                    );
                    if status.code() != tonic::Code::Ok {
                        return Err(status);
                    }
                }
                Err(err) => {
                    return Err(Status::internal(err.to_string()));
                }
            }
        }

        let metadata = match req.control_plane_metadata.as_ref() {
            Some(meta) => meta,
            None => {
                return Err(Status::failed_precondition(
                    "Failed to find control plane metadata in InvokeEZ Request",
                ));
            }
        };

        // When the ez instance is provided, we're routing it to the specific instance and we
        // don't need to do matching.
        if !metadata.destination_ez_instance_id.is_empty() {
            return Ok(Route::Remote);
        }

        let destination_isolate_service = match self
            .isolate_service_mapper
            .get_service_index(&IsolateServiceInfo {
                operator_domain: metadata.destination_operator_domain.clone(),
                service_name: metadata.destination_service_name.clone(),
            })
            .await
        {
            Some(destination_isolate_service) => destination_isolate_service,
            None => {
                return Err(Status::failed_precondition(
                    "Failed to find destination isolate service in InvokeEZ Request",
                ));
            }
        };

        if let Err(err) = self
            .manifest_validator
            .validate_backend_dependency(ValidateBackendDependencyRequest {
                binary_services_index: self.isolate_id.get_binary_services_index(),
                destination_isolate_service,
            })
            .await
        {
            return Err(Status::failed_precondition(err.to_string()));
        }

        let route = destination_isolate_service.get_request_route();
        Ok(route)
    }

    async fn stream_validate_invoke_ez_req(&self, req: &mut InvokeEzRequest) -> Result<Route, ()> {
        let Some(ref response_tx) = self.response_tx else {
            log::error!("Streaming enforcer invoked without response transmitter");
            return Err(());
        };

        match self.validate_invoke_ez_req(req).await {
            Ok(route) => Ok(route), // The route is returned on success.
            Err(status) => {
                log::error!(
                    "Failed to enforce scopes and manifest validation on Invoke EZ Request {:?}",
                    status,
                );
                let _ = response_tx.send(Err(status)).await;
                Err(())
            } // An empty error is returned, signaling that the error was sent.
        }
    }

    async fn validate_invoke_ez_resp(&self, resp: &InvokeEzResponse) -> Result<(), Status> {
        let mut strictest_scope = DataScopeType::Unspecified;
        if let Some(ez_response_scope) = &resp.ez_response_iscope {
            strictest_scope = get_strictest_scope(ez_response_scope);
            if strictest_scope == DataScopeType::Unspecified {
                return Err(Status::failed_precondition("Unspecified scope in InvokeEZ Response"));
            }
        } else if resp.ez_response_payload.is_some() {
            return Err(Status::failed_precondition("No scopes mentioned for the payload"));
        } else if resp.ez_response_payload.is_none() {
            log::warn!(
                "Received empty payload+scope in InvokeEzResponse, allowing with no scope checks"
            );
            return Ok(());
        }

        self.data_scope_requester
            .validate_isolate_scope(ValidateIsolateRequest {
                isolate_id: self.isolate_id,
                requested_scope: strictest_scope,
            })
            .await
            .map_err(|e| {
                Status::permission_denied(format!(
                    "Failed to validate isolate scope for InvokeEZ Response {}",
                    e
                ))
            })?;

        Ok(())
    }

    async fn stream_validate_invoke_ez_resp(
        &self,
        response: Result<InvokeEzResponse, Status>,
    ) -> Result<(), ()> {
        let Some(ref response_tx) = self.response_tx else {
            log::error!("Streaming enforcer invoked without response transmitter");
            return Err(());
        };

        let final_response = async move {
            let resp = response?;
            self.validate_invoke_ez_resp(&resp).await?;
            Ok(resp)
        }
        .await;
        response_tx.send(final_response).await.map_err(|_| ())
    }
}

fn convert_to_isolate_request(invoke_ez_req: InvokeEzRequest) -> InvokeIsolateRequest {
    InvokeIsolateRequest {
        control_plane_metadata: invoke_ez_req.control_plane_metadata,
        isolate_input_iscope: invoke_ez_req.isolate_request_iscope,
        isolate_input: invoke_ez_req.isolate_request_payload,
    }
}

fn convert_to_invoke_ez_response(
    invoke_isolate_response: InvokeIsolateResponse,
) -> InvokeEzResponse {
    InvokeEzResponse {
        control_plane_metadata: invoke_isolate_response.control_plane_metadata,
        ez_response_iscope: invoke_isolate_response.isolate_output_iscope,
        ez_response_payload: invoke_isolate_response.isolate_output,
        status: invoke_isolate_response.status,
    }
}

fn get_connector(
    connector_option: &Option<Box<dyn ExternalProxyChannel>>,
    isolate_id: IsolateId,
) -> Result<&dyn ExternalProxyChannel, Box<Status>> {
    match connector_option {
        Some(connector) => Ok(connector.as_ref()),
        None => Err(Box::new(Status::failed_precondition(format!(
            "External call for isolate {} failed: No external proxy is configured.",
            isolate_id
        )))),
    }
}
