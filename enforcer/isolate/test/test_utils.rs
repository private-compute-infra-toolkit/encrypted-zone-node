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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use anyhow::Result;
use dyn_clone::DynClone;
use enforcer_proto::data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::ez_isolate_bridge_server::{
    EzIsolateBridge, EzIsolateBridgeServer,
};
use enforcer_proto::enforcer::v1::{
    InvokeIsolateRequest, InvokeIsolateResponse, UpdateIsolateStateRequest,
    UpdateIsolateStateResponse,
};
use ez_error::EzError;
use isolate_info::IsolateId;
use simple_tonic_stream::SimpleStreamingWrapper;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::UnixListener;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

type InvokeIsolateResult = Result<Response<InvokeIsolateResponse>, Status>;

const ISOLATE_TEST_CHANNEL_SIZE: usize = 128;
pub const TEST_ERROR_CODE: i32 = 99;
pub const TEST_ERROR_MESSAGE: &str = "TEST_ERROR";
pub const TEST_ERROR_SERVICE_NAME: &str = "ErrorService";
pub const DEFAULT_ISOLATE_UNIX_SOCKET: &str = "/tmp/default-isolate.sock";
pub const ECHO_ISOLATE_OPERATOR_DOMAIN: &str = "echo_isolate_service_test_domain";
pub const ECHO_ISOLATE_SERVICE_NAME: &str = "echo_isolate_service";
pub const ECHO_ISOLATE_METHOD_NAME: &str = "echo_isolate_method";
pub const ERROR_ISOLATE_SERVICE_NAME: &str = "error_isolate_service";

#[tonic::async_trait]
// Implement custom Isolate logic
pub trait FakeIsolate: Send + Sync + DynClone + std::fmt::Debug {
    /// Processes incoming Isolate requests from the client and sends responses back to the client.
    async fn process_isolate_requests(
        &self,
        client_to_junction_rx: Receiver<InvokeIsolateRequest>,
        junction_to_client_tx: Sender<Result<InvokeIsolateResponse, ez_error::EzError>>,
        stream_call_count: Arc<AtomicUsize>,
        invoke_isolate_requests: Arc<Mutex<Vec<InvokeIsolateRequest>>>,
    );

    async fn create_isolate_response(
        &self,
        request: &InvokeIsolateRequest,
    ) -> InvokeIsolateResponse;
}
dyn_clone::clone_trait_object!(FakeIsolate);

#[derive(Debug, Clone)]
pub enum ScopeDragInstruction {
    IncreaseByOne,
    DecreaseByOne,
    KeepSame,
    KeepUnspecified,
}

// Default Isolate used for testing
// Echos back InvokeIsolateResponse with Ok status
#[derive(Clone, Debug)]
pub struct DefaultEchoIsolate {
    scope_drag_instruction: ScopeDragInstruction,
    delay: Option<tokio::time::Duration>,
}

impl DefaultEchoIsolate {
    pub fn new(
        scope_drag_instruction: ScopeDragInstruction,
        delay: Option<tokio::time::Duration>,
    ) -> Self {
        let isolate = Self { scope_drag_instruction, delay };
        let _ = format!("{isolate:?}");
        isolate
    }
}

fn drag_scope_by_one(
    input_scope: DataScopeType,
    instruction: &ScopeDragInstruction,
) -> DataScopeType {
    if input_scope == DataScopeType::Unspecified {
        return DataScopeType::Unspecified;
    }
    let step = match instruction {
        ScopeDragInstruction::KeepSame => return input_scope,
        ScopeDragInstruction::KeepUnspecified => return DataScopeType::Unspecified,
        ScopeDragInstruction::IncreaseByOne => 1,
        ScopeDragInstruction::DecreaseByOne => -1,
    };
    let min_scope = DataScopeType::Public as i32;
    let max_scope = DataScopeType::Sealed as i32;
    DataScopeType::try_from((input_scope as i32 + step).clamp(min_scope, max_scope))
        .unwrap_or(DataScopeType::Unspecified)
}

fn adjust_scope(instruction: &ScopeDragInstruction, response: &mut InvokeIsolateResponse) {
    if let Some(scope) = &mut response.isolate_output_iscope {
        for datagram_scope in &mut scope.datagram_iscopes {
            let new_scope = drag_scope_by_one(datagram_scope.scope_type(), instruction);
            datagram_scope.set_scope_type(new_scope);
        }
    }
}

#[tonic::async_trait]
impl FakeIsolate for DefaultEchoIsolate {
    async fn process_isolate_requests(
        &self,
        mut client_to_junction_rx: Receiver<InvokeIsolateRequest>,
        junction_to_client_tx: Sender<Result<InvokeIsolateResponse, ez_error::EzError>>,
        stream_call_count: Arc<AtomicUsize>,
        invoke_isolate_requests: Arc<Mutex<Vec<InvokeIsolateRequest>>>,
    ) {
        let delay = self.delay;
        while let Some(invoke_isolate_request) = client_to_junction_rx.recv().await {
            if invoke_isolate_request
                .control_plane_metadata
                .as_ref()
                .unwrap()
                .destination_service_name
                == TEST_ERROR_SERVICE_NAME
            {
                let _ = junction_to_client_tx
                    .send(Err(EzError::Status(Status::new(
                        TEST_ERROR_CODE.into(),
                        TEST_ERROR_MESSAGE,
                    ))))
                    .await;
                return;
            }
            invoke_isolate_requests.lock().unwrap().push(invoke_isolate_request.clone());
            let mut invoke_isolate_response =
                create_echo_invoke_isolate_response(invoke_isolate_request.clone());
            adjust_scope(&self.scope_drag_instruction, &mut invoke_isolate_response);
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let _ = junction_to_client_tx.send(Ok(invoke_isolate_response)).await;
            stream_call_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn create_isolate_response(
        &self,
        request: &InvokeIsolateRequest,
    ) -> InvokeIsolateResponse {
        let mut invoke_isolate_response = create_echo_invoke_isolate_response(request.clone());
        adjust_scope(&self.scope_drag_instruction, &mut invoke_isolate_response);
        invoke_isolate_response
    }
}

#[tonic::async_trait]
impl EzIsolateBridge for DefaultEchoIsolate {
    type StreamInvokeIsolateStream = ReceiverStream<Result<InvokeIsolateResponse, Status>>;

    async fn stream_invoke_isolate(
        &self,
        request: Request<Streaming<InvokeIsolateRequest>>,
    ) -> Result<Response<Self::StreamInvokeIsolateStream>, Status> {
        let mut invoke_isolate_request_receiver: SimpleStreamingWrapper<InvokeIsolateRequest> =
            request.into_inner().into();
        let (invoke_isolate_response_tx, invoke_isolate_response_rx) =
            channel(ISOLATE_TEST_CHANNEL_SIZE);

        let scope_drag_instruction = self.scope_drag_instruction.clone();
        let delay = self.delay;
        tokio::spawn(async move {
            while let Some(invoke_isolate_request) = invoke_isolate_request_receiver.message().await
            {
                if let Some(ref metadata) = invoke_isolate_request.control_plane_metadata {
                    if metadata.destination_service_name == *ERROR_ISOLATE_SERVICE_NAME {
                        let _ = invoke_isolate_response_tx
                            .send(Err(Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE)))
                            .await;
                        continue;
                    }
                }
                let mut invoke_isolate_response =
                    create_echo_invoke_isolate_response(invoke_isolate_request.clone());
                adjust_scope(&scope_drag_instruction, &mut invoke_isolate_response);
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }
                let _ = invoke_isolate_response_tx.send(Ok(invoke_isolate_response)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(invoke_isolate_response_rx)))
    }

    async fn invoke_isolate(&self, request: Request<InvokeIsolateRequest>) -> InvokeIsolateResult {
        let request = request.into_inner();
        if let Some(ref metadata) = request.control_plane_metadata {
            if metadata.destination_service_name == *ERROR_ISOLATE_SERVICE_NAME {
                return Err(Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE));
            }
        }
        let mut invoke_isolate_response = create_echo_invoke_isolate_response(request);
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        adjust_scope(&self.scope_drag_instruction, &mut invoke_isolate_response);
        Ok(Response::new(invoke_isolate_response))
    }

    async fn update_isolate_state(
        &self,
        request: Request<UpdateIsolateStateRequest>,
    ) -> Result<Response<UpdateIsolateStateResponse>, Status> {
        let request = request.into_inner();
        // Since DefaultEchoIsolate is stateless, we just mock a successful transition
        // by returning the requested state as the current state.
        Ok(Response::new(UpdateIsolateStateResponse { current_state: request.move_to_state }))
    }
}

// we use a random IsolateId for tests, append that value to the socket path here
// so we don't have collisions when running multiple tests at a time
// Returns a oneshot sender to shutdown the Isolate server at the end of each test
pub async fn start_fake_isolate_server(
    test_isolate_id: IsolateId,
    scope_drag_instruction: ScopeDragInstruction,
    delay: Option<tokio::time::Duration>,
) -> oneshot::Sender<()> {
    let fake_isolate = DefaultEchoIsolate::new(scope_drag_instruction, delay);

    let unix_listener =
        UnixListener::bind(format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, test_isolate_id)).unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(EzIsolateBridgeServer::new(fake_isolate))
            .serve_with_incoming_shutdown(UnixListenerStream::new(unix_listener), async {
                shutdown_rx.await.ok();
            })
            .await
    });
    shutdown_tx
}

pub fn create_echo_invoke_isolate_response(
    invoke_isolate_request: InvokeIsolateRequest,
) -> InvokeIsolateResponse {
    InvokeIsolateResponse {
        control_plane_metadata: invoke_isolate_request.control_plane_metadata.clone(),
        isolate_output_iscope: invoke_isolate_request.isolate_input_iscope,
        isolate_output: invoke_isolate_request.isolate_input,
        response_extensions: invoke_isolate_request
            .control_plane_metadata
            .map(|m| m.extensions)
            .unwrap_or_default(),
    }
}
