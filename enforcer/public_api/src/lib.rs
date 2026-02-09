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

use anyhow::Result;
use data_scope_proto::enforcer::v1::{DataScopeType, EzDataScope, EzStaticScopeInfo};
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, EzPayloadIsolateScope, InvokeIsolateRequest, InvokeIsolateResponse,
    IsolateDataScope,
};
use ez_error::EzError;
use ez_error_trait::ToEzError;
use ez_service_proto::enforcer::v1::ez_public_api_server::EzPublicApi;
use ez_service_proto::enforcer::v1::{
    CallParameters, CallRequest, CallResponse, EncryptedField, GetHealthReportRequest,
    GetHealthReportResponse, SessionMetadata,
};
use health_manager::HealthManager;
use interceptor::{Interceptor, RequestType};
use junction::error::IsolateStatusCode;
use junction_trait::Junction;
use metrics::common::{MetricAttributes, ServiceMetrics};
use metrics::observed_stream;
use metrics::public_api::PublicApiMetrics;
use payload_proto::enforcer::v1::EzPayloadData;
use prost::Message;
use std::iter::zip;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

type CallResponseResult = Result<Response<CallResponse>, Status>;

type PublicApiRequestStream =
    observed_stream::ObservedRequestStream<Streaming<CallRequest>, PublicApiMetrics>;

type PublicApiResponseStream = observed_stream::ObservedResponseStream<
    ReceiverStream<Result<InvokeIsolateResponse, EzError>>,
    PublicApiMetrics,
>;

const EZ_PUBLIC_API_RESPONSE_CHANNEL_SIZE: usize = 128;

struct MetricContext<'a> {
    metrics: &'a PublicApiMetrics,
    metric_attr: &'a MetricAttributes,
}

#[derive(Clone, Debug)]
pub struct EzPublicApiService {
    // To create new Junction session for each stream_call
    isolate_junction: Box<dyn Junction>,
    // To intercept PublicApi requests
    interceptor: Interceptor,
    // To get health report
    health_manager: HealthManager,
    metrics: PublicApiMetrics,
}

impl EzPublicApiService {
    /// Connects to IsolateJunction and initiates stream processing in a parallel task
    pub async fn new(
        isolate_junction: Box<dyn Junction>,
        interceptor: Interceptor,
        health_manager: HealthManager,
    ) -> Self {
        let metrics = PublicApiMetrics::default();
        Self { isolate_junction, interceptor, health_manager, metrics }
    }
}

#[tonic::async_trait]
impl EzPublicApi for EzPublicApiService {
    type StreamCallStream = ReceiverStream<Result<CallResponse, Status>>;
    #[tracing::instrument]
    async fn call(&self, request: Request<CallRequest>) -> CallResponseResult {
        let mut call_request = request.into_inner();

        let metric_attr = MetricAttributes::from(&call_request);
        let _call_tracker = self.metrics.track_call(metric_attr.base());

        if call_request.session_metadata.is_none() {
            return Err(
                IsolateStatusCode::MissingField("session_metadata".to_string()).to_tonic_status()
            );
        }
        if call_request.input_params.is_none() {
            return Err(
                IsolateStatusCode::MissingField("input_params".to_string()).to_tonic_status()
            );
        }

        self.interceptor.replace_with_interceptor(&mut call_request, RequestType::Unary).await;

        let session_metadata = std::mem::take(&mut call_request.session_metadata).unwrap();
        let session_id = session_metadata.session_id;

        // Send request to IsolateJunction
        let invoke_isolate_request = create_invoke_isolate_request(call_request, session_id);
        self.metrics.record_message_size_bytes(
            metric_attr.request(),
            invoke_isolate_request.encoded_len() as u64,
        );
        let invoke_isolate_response_result =
            self.isolate_junction.invoke_isolate(None, invoke_isolate_request, true).await;

        match invoke_isolate_response_result {
            Ok(invoke_isolate_response) => {
                self.metrics.record_message_size_bytes(
                    metric_attr.response(),
                    invoke_isolate_response.encoded_len() as u64,
                );
                let call_response =
                    create_call_response(invoke_isolate_response, Some(session_metadata));
                Ok(Response::new(call_response))
            }
            Err(e) => {
                self.metrics.record_error(&metric_attr.base(), "unary_call_processing_error");
                Err(e.to_tonic_status())
            }
        }
    }

    async fn stream_call(
        &self,
        request: Request<Streaming<CallRequest>>,
    ) -> Result<Response<Self::StreamCallStream>, Status> {
        let call_request_stream = request.into_inner();

        let (api_to_client_response_tx, api_to_client_response_rx) =
            tokio::sync::mpsc::channel(EZ_PUBLIC_API_RESPONSE_CHANNEL_SIZE);
        let api_to_client_response_tx_clone = api_to_client_response_tx.clone();
        // new bi-di stream created for each stream_call
        let isolate_junction_channel =
            self.isolate_junction.stream_invoke_isolate(None, true).await;

        // Wrap the internal junction receiver to observe responses before processing
        let internal_response_stream =
            ReceiverStream::new(isolate_junction_channel.junction_to_client);

        let (observed_req_stream, observed_res_stream) = observed_stream::pair(
            call_request_stream,
            internal_response_stream,
            self.metrics.clone(),
        );

        let interceptor_clone = self.interceptor.clone();

        // start processing task async
        tokio::spawn(process_call_request_stream(
            api_to_client_response_tx,
            observed_req_stream,
            isolate_junction_channel.client_to_junction,
            interceptor_clone,
            self.metrics.clone(),
        ));

        tokio::spawn(process_invoke_isolate_response_stream(
            api_to_client_response_tx_clone,
            observed_res_stream,
            self.metrics.clone(),
        ));

        Ok(Response::new(ReceiverStream::new(api_to_client_response_rx)))
    }

    async fn get_health_report(
        &self,
        _request: Request<GetHealthReportRequest>,
    ) -> Result<Response<GetHealthReportResponse>, Status> {
        let report = self.health_manager.get_report().await;
        Ok(Response::new(GetHealthReportResponse { health_report: Some(report) }))
    }
}

async fn process_call_request_stream(
    api_to_client_response_tx: Sender<Result<CallResponse, Status>>,
    mut call_request_stream: PublicApiRequestStream,
    isolate_junction_channel: Sender<InvokeIsolateRequest>,
    interceptor: Interceptor,
    metrics: PublicApiMetrics,
) {
    let Some(mut initial_call_request) = call_request_stream.next().await else {
        let _ = api_to_client_response_tx
            .send(Err(IsolateStatusCode::EmptyStream.to_tonic_status()))
            .await;
        return;
    };

    let metric_attr = call_request_stream.attributes().clone();

    // Handle Initial Request
    {
        let _timer = metrics.track_message_processing(metric_attr.request());

        let request_validation_status: Status =
            validate_initial_stream_call_request(&initial_call_request);
        if request_validation_status.code() != tonic::Code::Ok {
            let _ = api_to_client_response_tx.send(Err(request_validation_status)).await;
            return;
        }
        let Some(session_metadata) = initial_call_request.session_metadata.clone() else {
            let _ = api_to_client_response_tx
                .send(Err(Status::internal("failed to read session metadata")))
                .await;
            return;
        };
        interceptor
            .replace_with_interceptor(&mut initial_call_request, RequestType::Streaming)
            .await;
        let session_id = session_metadata.session_id;

        let metric_ctx = MetricContext { metrics: &metrics, metric_attr: &metric_attr };

        let send_result = send_to_junction(
            initial_call_request,
            isolate_junction_channel.clone(),
            session_id,
            metric_ctx,
        )
        .await;

        if send_result.code() != tonic::Code::Ok {
            let _ = api_to_client_response_tx.send(Err(send_result)).await;
            return;
        }

        // Handle Subsequent Requests
        let session_id = session_metadata.session_id;
        while let Some(call_request) = call_request_stream.next().await {
            let _timer = metrics.track_message_processing(metric_attr.request());
            let metric_ctx = MetricContext { metrics: &metrics, metric_attr: &metric_attr };

            let send_result = send_to_junction(
                call_request,
                isolate_junction_channel.clone(),
                session_id,
                metric_ctx,
            )
            .await;
            if send_result.code() != tonic::Code::Ok {
                let _ = api_to_client_response_tx.send(Err(send_result)).await;
                return;
            }
        }
    }
}

async fn process_invoke_isolate_response_stream(
    api_to_client_response_tx: Sender<Result<CallResponse, Status>>,
    mut invoke_isolate_response_stream: PublicApiResponseStream,
    metrics: PublicApiMetrics,
) {
    while let Some(invoke_isolate_reponse_result) = invoke_isolate_response_stream.next().await {
        let metric_attr = invoke_isolate_response_stream.attributes();
        let _timer = metrics.track_message_processing(metric_attr.response());

        match invoke_isolate_reponse_result {
            Ok(invoke_isolate_response) => {
                let _ = api_to_client_response_tx
                    .send(Ok(create_call_response(invoke_isolate_response, None)))
                    .await;
            }
            Err(e) => {
                metrics.record_error(&metric_attr.base(), "invoke_isolate_error");
                let _ = api_to_client_response_tx.send(Err(e.to_tonic_status())).await;
            }
        }
    }
}

fn validate_initial_stream_call_request(initial_request: &CallRequest) -> Status {
    if initial_request.operator_domain.is_empty() {
        IsolateStatusCode::MissingField("operator_domain".to_string()).to_tonic_status()
    } else if initial_request.service_name.is_empty() {
        IsolateStatusCode::MissingField("service_name".to_string()).to_tonic_status()
    } else if initial_request.method_name.is_empty() {
        IsolateStatusCode::MissingField("method_name".to_string()).to_tonic_status()
    } else if initial_request.session_metadata.is_none() {
        IsolateStatusCode::MissingField("session_metadata".to_string()).to_tonic_status()
    } else {
        Status::ok("valid initial request")
    }
}

async fn send_to_junction(
    call_request: CallRequest,
    isolate_junction_channel: Sender<InvokeIsolateRequest>,
    session_id: u64,
    ctx: MetricContext<'_>,
) -> Status {
    if call_request.input_params.is_none() {
        return IsolateStatusCode::MissingField("input_params".to_string()).to_tonic_status();
    }
    let invoke_isolate_request = create_invoke_isolate_request(call_request, session_id);

    let isolate_bridge_send_result = isolate_junction_channel.send(invoke_isolate_request).await;
    if let Err(isolate_bridge_send_error) = isolate_bridge_send_result {
        ctx.metrics.record_error(&ctx.metric_attr.base(), "send_to_junction_error");
        return Status::from_error(Box::new(isolate_bridge_send_error));
    }
    Status::ok("call request sent")
}

fn create_invoke_isolate_request(
    call_request: CallRequest,
    session_id: u64,
) -> InvokeIsolateRequest {
    let (isolate_input_iscope, isolate_input) =
        convert_isolate_input_to_iscope_and_payload(call_request.input_params);

    InvokeIsolateRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            // Use the external session_id here to disambiguate traffic. This
            // ipc_message_id will not be sent to the Isolates, since it is only attached to
            // the caller's side of the IPC junction.
            ipc_message_id: session_id,
            // TODO The CallRequest should have this field
            requester_spiffe: String::new(),
            requester_is_local: false,
            responder_is_local: true,
            destination_operator_domain: call_request.operator_domain,
            // TODO: Add publisher_id when available.
            destination_publisher_id: String::new(),
            // TODO: Add isolate_name when available.
            destination_isolate_name: String::new(),
            destination_service_name: call_request.service_name,
            destination_method_name: call_request.method_name,
            // Call into EzInstance with public API shouldn't specify the ez_instance_id.
            destination_ez_instance_id: String::new(),
            // No shared memory handles from remote callers.
            shared_memory_handles: Vec::new(),
        }),
        isolate_input_iscope,
        isolate_input,
    }
}

fn create_call_response(
    invoke_isolate_response: InvokeIsolateResponse,
    session_metadata: Option<SessionMetadata>,
) -> CallResponse {
    let (public_output, encrypted_output) = convert_isolate_output_to_public_and_encrypted(
        invoke_isolate_response.isolate_output_iscope,
        invoke_isolate_response.isolate_output,
    );

    CallResponse { session_metadata, public_output, encrypted_output }
}

fn convert_isolate_output_to_public_and_encrypted(
    iscope_option: Option<EzPayloadIsolateScope>,
    isolate_output_option: Option<EzPayloadData>,
) -> (Vec<u8>, Vec<EncryptedField>) {
    let mut public_output = vec![];
    let mut encrypted_output = vec![];

    let (Some(iscope), Some(isolate_output)) = (iscope_option, isolate_output_option) else {
        log::warn!("Both required fields EzPayloadIsolateScope & EzPayloadData were not present");
        return (public_output, encrypted_output);
    };

    for (iscope, vec_of_bytes) in zip(iscope.datagram_iscopes, isolate_output.datagrams) {
        if iscope.scope_type() == DataScopeType::Public {
            public_output = vec_of_bytes;
        } else {
            encrypted_output.push(EncryptedField {
                scope: Some(EzDataScope {
                    static_info: Some(EzStaticScopeInfo {
                        scope_type: iscope.scope_type().into(),
                        scope_instance_id: u64::default(),
                        scope_owner: None,
                        subscope_instance_ids: vec![],
                    }),
                    dynamic_info: None,
                    local_info: None,
                }),
                // TODO Encrypt this once EZ supports encryption
                ciphertext: vec_of_bytes,
            });
        }
    }

    (public_output, encrypted_output)
}

fn convert_isolate_input_to_iscope_and_payload(
    call_params_option: Option<CallParameters>,
) -> (Option<EzPayloadIsolateScope>, Option<EzPayloadData>) {
    match call_params_option {
        Some(call_params) => {
            let mut datagram_iscopes = vec![];
            let mut datagrams = vec![];

            // Special case: When both public_input and the encrypted_input are empty, it's likely
            // a request with no field is received.
            if call_params.public_input.is_empty() && call_params.encrypted_input.is_empty() {
                datagram_iscopes.push(IsolateDataScope {
                    scope_type: DataScopeType::Public.into(),
                    mapped_scope_owner: None,
                });
                datagrams.push(vec![]);
            }

            if !call_params.public_input.is_empty() {
                datagram_iscopes.push(IsolateDataScope {
                    scope_type: DataScopeType::Public.into(),
                    mapped_scope_owner: None,
                });
                datagrams.push(call_params.public_input);
            }

            for encrypted_field in call_params.encrypted_input {
                datagram_iscopes.push(IsolateDataScope {
                    scope_type: convert_data_scope_to_iscope(encrypted_field.scope),
                    mapped_scope_owner: None,
                });
                // TODO Decrypt this once EZ supports encryption
                datagrams.push(encrypted_field.ciphertext);
            }

            (Some(EzPayloadIsolateScope { datagram_iscopes }), Some(EzPayloadData { datagrams }))
        }
        None => (None, None),
    }
}

/// Return i32 equivalent of DataScopeType to what is provided in EzStaticScopeInfo.
fn convert_data_scope_to_iscope(scope_option: Option<EzDataScope>) -> i32 {
    match scope_option {
        Some(scope) => {
            if let Some(static_info) = scope.static_info {
                static_info.scope_type
            } else {
                DataScopeType::Unspecified.into()
            }
        }
        None => DataScopeType::Unspecified.into(),
    }
}
