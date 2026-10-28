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
use container_manager_request::{
    ContainerManagerRequest, MountDirectoryResponse, MountFileResponse, ResetIsolateResponse,
};
use container_manager_requester::ContainerManagerRequester;
use data_scope::error::DataScopeError;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::request::{AddIsolateRequest, AddManifestScopeRequest, GetIsolateScopeRequest};
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::ez_isolate_bridge_server::{
    EzIsolateBridge, EzIsolateBridgeServer,
};
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, EzPayloadIsolateScope, InvokeIsolateRequest, InvokeIsolateResponse,
    IsolateDataScope, IsolateState,
};
use fileshare_manager::FileshareManager;
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use isolate_test_utils::{
    create_echo_invoke_isolate_response, ScopeDragInstruction, DEFAULT_ISOLATE_UNIX_SOCKET,
    ECHO_ISOLATE_METHOD_NAME, ECHO_ISOLATE_OPERATOR_DOMAIN, ECHO_ISOLATE_SERVICE_NAME,
    ERROR_ISOLATE_SERVICE_NAME, TEST_ERROR_CODE, TEST_ERROR_MESSAGE,
};
use junction::{IsolateJunction, IsolateJunctionArgs};
use junction_test_utils::{
    create_add_isolate_request, TestHarness, CLONE_ECHO_ISOLATE_SERVICE_NAME,
    JUNCTION_TEST_CHANNEL_SIZE, QUALIFIED_ECHO_ISOLATE_SERVICE_NAME,
};
use junction_trait::Junction;
use payload_proto::enforcer::v1::EzPayloadData;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::net::UnixListener;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const DEFAULT_MAX_DECODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

const TEST_VALID_HEADER: &str = "x-valid-header";
const TEST_VALID_HEADER_VALUE: &str = "ok-val";

#[derive(Clone)]
struct MetadataCheckingIsolate {
    expected_header_key: String,
    expected_header_value: String,
}

#[tonic::async_trait]
impl EzIsolateBridge for MetadataCheckingIsolate {
    type StreamInvokeIsolateStream =
        tokio_stream::wrappers::ReceiverStream<Result<InvokeIsolateResponse, Status>>;

    async fn stream_invoke_isolate(
        &self,
        _request: Request<Streaming<InvokeIsolateRequest>>,
    ) -> Result<Response<Self::StreamInvokeIsolateStream>, Status> {
        unimplemented!()
    }

    async fn invoke_isolate(
        &self,
        request: Request<InvokeIsolateRequest>,
    ) -> Result<Response<InvokeIsolateResponse>, Status> {
        let metadata = request.metadata();
        if let Some(val) = metadata.get(&self.expected_header_key) {
            if val == self.expected_header_value.as_str() {
                let response = create_echo_invoke_isolate_response(request.into_inner());
                return Ok(Response::new(response));
            }
        }
        Err(Status::invalid_argument("Missing or incorrect header"))
    }

    async fn update_isolate_state(
        &self,
        _request: Request<enforcer_proto::enforcer::v1::UpdateIsolateStateRequest>,
    ) -> Result<Response<enforcer_proto::enforcer::v1::UpdateIsolateStateResponse>, Status> {
        unimplemented!()
    }
}

async fn start_metadata_checking_isolate_server(
    test_isolate_id: IsolateId,
    expected_key: String,
    expected_value: String,
) -> oneshot::Sender<()> {
    let fake_isolate = MetadataCheckingIsolate {
        expected_header_key: expected_key,
        expected_header_value: expected_value,
    };

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

fn create_random_request(isolate_service_info: &IsolateServiceInfo) -> InvokeIsolateRequest {
    // Generate 8 random bytes for request
    let random_request_data =
        [0u8; 8].into_iter().map(|_| rand::random::<u8>()).collect::<Vec<_>>();
    let original_msg_id = rand::random();

    InvokeIsolateRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            ipc_message_id: original_msg_id,
            requester_spiffe: String::new(),
            requester_is_local: false,
            responder_is_local: true,
            destination_operator_domain: isolate_service_info.operator_domain.clone(),
            destination_service_name: isolate_service_info.service_name.clone(),
            destination_isolate_name: isolate_service_info.isolate_name.clone(),
            destination_publisher_id: isolate_service_info.publisher_id.clone(),
            destination_method_name: ECHO_ISOLATE_METHOD_NAME.to_string(),
            shared_memory_handles: Vec::new(),
            fileshare_handles: Vec::new(),
            destination_ez_instance_id: "".to_string(),
            ..Default::default()
        }),
        isolate_input_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::UserPrivate.into(),
                mapped_scope_owner: None,
            }],
        }),
        isolate_input: Some(payload_proto::enforcer::v1::EzHybridPayload {
            delivery_method: Some(
                payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::InlineData(
                    EzPayloadData { datagrams: vec![random_request_data.to_vec()] },
                ),
            ),
        }),
    }
}

// TODO(b/469487499): Add metric tests

struct DataScopeTestIsolate;

#[tonic::async_trait]
impl EzIsolateBridge for DataScopeTestIsolate {
    type StreamInvokeIsolateStream =
        tokio_stream::wrappers::ReceiverStream<Result<InvokeIsolateResponse, Status>>;

    async fn stream_invoke_isolate(
        &self,
        _request: Request<Streaming<InvokeIsolateRequest>>,
    ) -> Result<Response<Self::StreamInvokeIsolateStream>, Status> {
        unimplemented!()
    }

    async fn invoke_isolate(
        &self,
        request: Request<InvokeIsolateRequest>,
    ) -> Result<Response<InvokeIsolateResponse>, Status> {
        let response = InvokeIsolateResponse {
            control_plane_metadata: request.into_inner().control_plane_metadata,
            isolate_output_iscope: None,           // Empty scope.
            isolate_output: None,                  // Empty payload.
            response_extensions: vec![1, 3, 3, 7], // Data in extensions.
        };
        Ok(Response::new(response))
    }

    async fn update_isolate_state(
        &self,
        _request: Request<enforcer_proto::enforcer::v1::UpdateIsolateStateRequest>,
    ) -> Result<Response<enforcer_proto::enforcer::v1::UpdateIsolateStateResponse>, Status> {
        unimplemented!()
    }
}

async fn start_datascope_test_isolate_server(
    test_isolate_id: IsolateId,
) -> tokio::sync::oneshot::Sender<()> {
    let fake_isolate = DataScopeTestIsolate;
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

#[derive(Clone)]
struct DeadlockTestingIsolate;

#[tonic::async_trait]
impl EzIsolateBridge for DeadlockTestingIsolate {
    type StreamInvokeIsolateStream =
        tokio_stream::wrappers::ReceiverStream<Result<InvokeIsolateResponse, Status>>;

    async fn stream_invoke_isolate(
        &self,
        request: Request<Streaming<InvokeIsolateRequest>>,
    ) -> Result<Response<Self::StreamInvokeIsolateStream>, Status> {
        let mut stream = request.into_inner();

        // Wait for two requests before returning Response::new()
        // Without the fix, the second request will never arrive because the junction
        // won't start proxying until Response::new() is returned.
        let req1 = stream.message().await.unwrap().unwrap();
        let req2 = stream.message().await.unwrap().unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(Ok(create_echo_invoke_isolate_response(req1))).await.unwrap();
        tx.send(Ok(create_echo_invoke_isolate_response(req2))).await.unwrap();

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn invoke_isolate(
        &self,
        _request: Request<InvokeIsolateRequest>,
    ) -> Result<Response<InvokeIsolateResponse>, Status> {
        unimplemented!()
    }

    async fn update_isolate_state(
        &self,
        _request: Request<enforcer_proto::enforcer::v1::UpdateIsolateStateRequest>,
    ) -> Result<Response<enforcer_proto::enforcer::v1::UpdateIsolateStateResponse>, Status> {
        unimplemented!()
    }
}

async fn start_deadlock_testing_isolate_server(test_isolate_id: IsolateId) -> oneshot::Sender<()> {
    let fake_isolate = DeadlockTestingIsolate;

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

struct ShmRequestCheckingIsolate {
    received_requests: Arc<Mutex<Vec<InvokeIsolateRequest>>>,
}

#[tonic::async_trait]
impl EzIsolateBridge for ShmRequestCheckingIsolate {
    type StreamInvokeIsolateStream =
        tokio_stream::wrappers::ReceiverStream<Result<InvokeIsolateResponse, Status>>;

    async fn stream_invoke_isolate(
        &self,
        request: Request<Streaming<InvokeIsolateRequest>>,
    ) -> Result<Response<Self::StreamInvokeIsolateStream>, Status> {
        let mut rx = request.into_inner();
        let (tx, rx_stream) = mpsc::channel(10);
        let received_requests = self.received_requests.clone();
        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            while let Some(Ok(req)) = rx.next().await {
                received_requests.lock().unwrap().push(req.clone());
                let resp = create_echo_invoke_isolate_response(req);
                let _ = tx.send(Ok(resp)).await;
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx_stream)))
    }

    async fn invoke_isolate(
        &self,
        _request: Request<InvokeIsolateRequest>,
    ) -> Result<Response<InvokeIsolateResponse>, Status> {
        unimplemented!()
    }

    async fn update_isolate_state(
        &self,
        _request: Request<enforcer_proto::enforcer::v1::UpdateIsolateStateRequest>,
    ) -> Result<Response<enforcer_proto::enforcer::v1::UpdateIsolateStateResponse>, Status> {
        unimplemented!()
    }
}

struct ShmResponseCheckingIsolate {
    shm_path: String,
}

#[tonic::async_trait]
impl EzIsolateBridge for ShmResponseCheckingIsolate {
    type StreamInvokeIsolateStream =
        tokio_stream::wrappers::ReceiverStream<Result<InvokeIsolateResponse, Status>>;

    async fn stream_invoke_isolate(
        &self,
        request: Request<Streaming<InvokeIsolateRequest>>,
    ) -> Result<Response<Self::StreamInvokeIsolateStream>, Status> {
        let mut rx = request.into_inner();
        let (tx, rx_stream) = mpsc::channel(10);
        let shm_path = self.shm_path.clone();
        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            // Setup isolate writer pool
            let isolate_pool = shm_slab_pool::ShmSlabPool::new(shm_slab_pool::ShmSlabPoolOptions {
                file_name: format!("{shm_path}/isolate-writes"),
                number_of_slots: 64,
                slot_size: 4,
                writer: true,
            })
            .expect("Failed to create isolate writer pool");

            let test_payload = b"stream shm echo";
            let slot_refs =
                isolate_pool.write_to_pool(test_payload).await.expect("Failed to write to pool");

            while let Some(Ok(req)) = rx.next().await {
                let mut resp = create_echo_invoke_isolate_response(req);
                resp.isolate_output = Some(payload_proto::enforcer::v1::EzHybridPayload {
                    delivery_method: Some(
                        payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::ShmData(
                            payload_proto::enforcer::v1::ShmSlotData { slots: slot_refs.clone() },
                        ),
                    ),
                });
                let _ = tx.send(Ok(resp)).await;
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx_stream)))
    }

    async fn invoke_isolate(
        &self,
        _request: Request<InvokeIsolateRequest>,
    ) -> Result<Response<InvokeIsolateResponse>, Status> {
        unimplemented!()
    }

    async fn update_isolate_state(
        &self,
        _request: Request<enforcer_proto::enforcer::v1::UpdateIsolateStateRequest>,
    ) -> Result<Response<enforcer_proto::enforcer::v1::UpdateIsolateStateResponse>, Status> {
        unimplemented!()
    }
}

#[derive(Clone)]
struct DelayedHeaderIsolate {
    cancellation_tx: mpsc::Sender<()>,
    first_request_tx: std::sync::Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

#[tonic::async_trait]
impl EzIsolateBridge for DelayedHeaderIsolate {
    type StreamInvokeIsolateStream =
        tokio_stream::wrappers::ReceiverStream<Result<InvokeIsolateResponse, Status>>;

    async fn stream_invoke_isolate(
        &self,
        request: Request<Streaming<InvokeIsolateRequest>>,
    ) -> Result<Response<Self::StreamInvokeIsolateStream>, Status> {
        use tokio_stream::StreamExt;
        let mut stream = request.into_inner();

        if let Some(_msg) = stream.next().await {
            // Signal the test that the stream is open and the first message arrived
            if let Some(tx) = self.first_request_tx.lock().unwrap().take() {
                let _ = tx.send(());
            } // else it was already consumed
        }

        // Deliberately refuse to return `Response::new()` (which sends HTTP/2 headers).
        // We sit in a loop draining the stream until we see EOF (client dropped).
        // Without the fix, the `END_STREAM` frame will never arrive because Tonic
        // won't unblock the proxy task's sender until headers are received.
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(_) => continue,
                Err(_) => break,
            }
        }

        let _ = self.cancellation_tx.send(()).await;

        Err(Status::cancelled("cancelled"))
    }

    async fn invoke_isolate(
        &self,
        _request: Request<InvokeIsolateRequest>,
    ) -> Result<Response<InvokeIsolateResponse>, Status> {
        unimplemented!()
    }

    async fn update_isolate_state(
        &self,
        _request: Request<enforcer_proto::enforcer::v1::UpdateIsolateStateRequest>,
    ) -> Result<Response<enforcer_proto::enforcer::v1::UpdateIsolateStateResponse>, Status> {
        unimplemented!()
    }
}

async fn start_delayed_header_isolate_server(
    test_isolate_id: IsolateId,
    cancellation_tx: mpsc::Sender<()>,
    first_request_tx: tokio::sync::oneshot::Sender<()>,
) -> tokio::sync::oneshot::Sender<()> {
    let fake_isolate = DelayedHeaderIsolate {
        cancellation_tx,
        first_request_tx: std::sync::Arc::new(std::sync::Mutex::new(Some(first_request_tx))),
    };
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

fn spawn_fake_container_manager(
    mut container_manager_request_rx: mpsc::Receiver<ContainerManagerRequest>,
) -> Arc<Mutex<Vec<IsolateId>>> {
    let read_only_mounts = Arc::new(Mutex::new(Vec::new()));
    let recorded_mounts = read_only_mounts.clone();
    tokio::spawn(async move {
        while let Some(request) = container_manager_request_rx.recv().await {
            match request {
                ContainerManagerRequest::MountWritableFile { resp, .. } => {
                    let _ = resp.send(Ok(MountFileResponse {}));
                }
                ContainerManagerRequest::MountReadOnlyFile { req, resp } => {
                    recorded_mounts.lock().unwrap().push(req.isolate_id);
                    let _ = resp.send(Ok(MountFileResponse {}));
                }
                ContainerManagerRequest::MountWritableDirectory { resp, .. } => {
                    let _ = resp.send(Ok(MountDirectoryResponse {}));
                }
                ContainerManagerRequest::MountReadOnlyDirectory { req, resp } => {
                    recorded_mounts.lock().unwrap().push(req.isolate_id);
                    let _ = resp.send(Ok(MountDirectoryResponse {}));
                }
                _ => {}
            }
        }
    });
    read_only_mounts
}

async fn add_receiver_isolate(
    test_harness: &TestHarness,
    allowed_data_scope_type: DataScopeType,
) -> IsolateId {
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(false));
    test_harness
        .isolate_state_manager
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type,
            isolate_id,
        })
        .await;
    // Both the SDK and the Junction channel must be up before the Isolate is considered Ready.
    test_harness
        .isolate_state_manager
        .update_state(isolate_id, IsolateState::Ready)
        .await
        .expect("Receiver isolate should report Ready");
    test_harness
        .isolate_state_manager
        .mark_channel_connected(isolate_id)
        .await
        .expect("Receiver isolate channel should connect");
    isolate_id
}

async fn invoke_with_share_handles(
    test_harness: &TestHarness,
    receiver_id: IsolateId,
    shared_memory_handles: Vec<String>,
    fileshare_handles: Vec<String>,
) -> Result<InvokeIsolateResponse, ez_error::EzError> {
    let mut request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let control_plane_metadata = request.control_plane_metadata.as_mut().unwrap();
    control_plane_metadata.shared_memory_handles = shared_memory_handles;
    control_plane_metadata.fileshare_handles = fileshare_handles;

    test_harness.isolate_junction.invoke_isolate(Some(receiver_id), request, false, None).await
}

async fn receiver_scope(test_harness: &TestHarness, receiver_id: IsolateId) -> DataScopeType {
    test_harness
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id: receiver_id })
        .await
        .expect("Receiver should be registered")
        .current_scope
}

async fn setup_standalone_junction(
    shm_slot_size: u64,
    shm_num_slots: u64,
    shm_payload_threshold: u64,
    ratified: bool,
) -> (IsolateJunction, IsolateStateManager, SharedMemManager, IsolateServiceInfo, IsolateId) {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let (cm_tx, _cm_rx) = mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
    let cm_requester = ContainerManagerRequester::new(cm_tx);
    let shared_mem_manager =
        SharedMemManager::new(cm_requester.clone(), shm_slot_size, shm_num_slots);
    let fileshare_manager = FileshareManager::new(cm_requester.clone());
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let manifest_validator = ManifestValidator::default();
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), cm_requester);

    let isolate_junction = IsolateJunction::new(IsolateJunctionArgs {
        data_scope_requester,
        isolate_service_mapper: isolate_service_mapper.clone(),
        shared_mem_manager: shared_mem_manager.clone(),
        fileshare_manager,
        state_manager: isolate_state_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shm_payload_threshold,
        max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
    });

    let service_info = IsolateServiceInfo {
        operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
        service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
        ..Default::default()
    };
    let binary_services_index = isolate_service_mapper
        .new_binary_index(vec![service_info.clone()], ratified, String::new(), String::new())
        .await
        .expect("Should create binary index");
    manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index,
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope: DataScopeType::UserPrivate,
        })
        .await
        .expect("Should register manifest scope");

    (
        isolate_junction,
        isolate_state_manager,
        shared_mem_manager,
        service_info,
        IsolateId::new(binary_services_index),
    )
}

#[derive(Default)]
struct ScriptedBridgeState {
    saw_shm_request: bool,
    last_valid_header: Option<String>,
    return_invalid_shm_slot: bool,
    return_missing_output_scope_with_data: bool,
}

#[derive(Clone)]
struct ScriptedBridgeIsolate {
    state: Arc<Mutex<ScriptedBridgeState>>,
}

#[tonic::async_trait]
impl EzIsolateBridge for ScriptedBridgeIsolate {
    type StreamInvokeIsolateStream =
        tokio_stream::wrappers::ReceiverStream<Result<InvokeIsolateResponse, Status>>;

    async fn stream_invoke_isolate(
        &self,
        request: Request<Streaming<InvokeIsolateRequest>>,
    ) -> Result<Response<Self::StreamInvokeIsolateStream>, Status> {
        let state = self.state.clone();
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(8);

        tokio::spawn(async move {
            let Some(Ok(first_req)) = inbound.next().await else {
                return;
            };
            if state.lock().unwrap().return_invalid_shm_slot {
                let mut bad_resp = create_echo_invoke_isolate_response(first_req);
                bad_resp.isolate_output = Some(payload_proto::enforcer::v1::EzHybridPayload {
                    delivery_method: Some(
                        payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::ShmData(
                            payload_proto::enforcer::v1::ShmSlotData {
                                slots: vec![payload_proto::enforcer::v1::ShmSlotReference {
                                    slot_index: 999,
                                    length: 64,
                                }],
                            },
                        ),
                    ),
                });
                let _ = tx.send(Ok(bad_resp)).await;
                return;
            }
            let _ = tx.send(Ok(InvokeIsolateResponse::default())).await;
            let _ = tx.send(Ok(create_echo_invoke_isolate_response(first_req))).await;
            let _ = inbound.next().await;
            let _ = tx.send(Err(Status::aborted("stream closed"))).await;
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn invoke_isolate(
        &self,
        request: Request<InvokeIsolateRequest>,
    ) -> Result<Response<InvokeIsolateResponse>, Status> {
        let valid_hdr = request
            .metadata()
            .get(TEST_VALID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let inner = request.into_inner();
        let mut guard = self.state.lock().unwrap();
        if valid_hdr.is_some() {
            guard.last_valid_header = valid_hdr;
        }
        if matches!(
            inner.isolate_input.as_ref().and_then(|h| h.delivery_method.as_ref()),
            Some(payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::ShmData(s))
                if !s.slots.is_empty()
        ) {
            guard.saw_shm_request = true;
        }
        let invalid_shm = guard.return_invalid_shm_slot;
        let missing_scope = guard.return_missing_output_scope_with_data;
        drop(guard);

        let mut resp = create_echo_invoke_isolate_response(inner);
        resp.isolate_output = Some(payload_proto::enforcer::v1::EzHybridPayload {
            delivery_method: Some(if invalid_shm {
                payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::ShmData(
                    payload_proto::enforcer::v1::ShmSlotData {
                        slots: vec![payload_proto::enforcer::v1::ShmSlotReference {
                            slot_index: 999,
                            length: 64,
                        }],
                    },
                )
            } else {
                payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::InlineData(
                    EzPayloadData { datagrams: vec![b"ok".to_vec()] },
                )
            }),
        });
        if missing_scope {
            resp.isolate_output_iscope = None;
        }
        Ok(Response::new(resp))
    }

    async fn update_isolate_state(
        &self,
        _request: Request<enforcer_proto::enforcer::v1::UpdateIsolateStateRequest>,
    ) -> Result<Response<enforcer_proto::enforcer::v1::UpdateIsolateStateResponse>, Status> {
        unimplemented!()
    }
}

async fn start_scripted_bridge_server(
    sock_path: &str,
    state: Arc<Mutex<ScriptedBridgeState>>,
) -> oneshot::Sender<()> {
    let unix_listener = UnixListener::bind(sock_path).unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(EzIsolateBridgeServer::new(ScriptedBridgeIsolate { state }))
            .serve_with_incoming_shutdown(UnixListenerStream::new(unix_listener), async {
                shutdown_rx.await.ok();
            })
            .await
    });
    shutdown_tx
}

#[tokio::test]
async fn test_invoke_and_stream_request_validation_errors() {
    let test_harness = TestHarness::new().await;
    let echo_info = test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap();

    let mut missing_scope_req = create_random_request(echo_info);
    missing_scope_req.isolate_input_iscope = None;
    let err = test_harness
        .isolate_junction
        .invoke_isolate(None, missing_scope_req, false, None)
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), DataScopeError::InvalidDataScopeType.to_string());

    let mut missing_cpm_req = create_random_request(echo_info);
    missing_cpm_req.control_plane_metadata = None;
    let err = test_harness
        .isolate_junction
        .invoke_isolate(None, missing_cpm_req, false, None)
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), DataScopeError::MissingControlPlaneMetadata.to_string());

    let unknown_info = IsolateServiceInfo {
        operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
        service_name: "unregistered_service".to_string(),
        ..Default::default()
    };
    let err = test_harness
        .isolate_junction
        .invoke_isolate(None, create_random_request(&unknown_info), false, None)
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), DataScopeError::InvalidIsolateServiceIndex.to_string());

    let mut empty_stream =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;
    drop(empty_stream.client_to_junction);
    let empty_err = timeout(Duration::from_secs(2), empty_stream.junction_to_client.recv())
        .await
        .expect("Expected prompt error on empty initial stream")
        .expect("Expected error item on stream")
        .unwrap_err();
    assert_eq!(empty_err.to_tonic_status().code(), tonic::Code::InvalidArgument);

    let mut no_cpm_stream =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;
    let mut no_cpm_req = create_random_request(echo_info);
    no_cpm_req.control_plane_metadata = None;
    assert!(no_cpm_stream.client_to_junction.send(no_cpm_req).await.is_ok());
    let no_cpm_err = timeout(Duration::from_secs(2), no_cpm_stream.junction_to_client.recv())
        .await
        .expect("Expected missing metadata error on stream")
        .expect("Expected error item on stream")
        .unwrap_err();
    assert!(matches!(
        no_cpm_err,
        ez_error::EzError::EnforcerError(ref e)
            if e.error_code == tonic::Code::InvalidArgument
                && e.bad_request.as_ref().is_some_and(|br| {
                    br.field_violations.iter().any(|v| v.field == "control_plane_metadata")
                })
    ));

    let mut bad_init_stream =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;
    let mut bad_init_req = create_random_request(echo_info);
    bad_init_req.isolate_input_iscope = None;
    assert!(bad_init_stream.client_to_junction.send(bad_init_req).await.is_ok());
    let init_err = timeout(Duration::from_secs(2), bad_init_stream.junction_to_client.recv())
        .await
        .expect("Expected validation error on stream")
        .expect("Expected stream message")
        .unwrap_err();
    assert_eq!(init_err.to_string(), DataScopeError::InvalidDataScopeType.to_string());

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unmapped_ready_destination_isolate_returns_unavailable_or_internal() {
    let (isolate_junction, isolate_state_manager, _shm, service_info, isolate_id) =
        setup_standalone_junction(0, 0, 100 * 1024 * 1024, false).await;
    isolate_state_manager.add_isolate(create_add_isolate_request(isolate_id)).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();
    isolate_state_manager.mark_channel_connected(isolate_id).await.unwrap();

    let unary_err = isolate_junction
        .invoke_isolate(
            None,
            create_random_request(&service_info),
            false,
            Some(Instant::now() + Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
    assert_eq!(unary_err.to_tonic_status().code(), tonic::Code::Unavailable);

    let mut stream_channels =
        isolate_junction.stream_invoke_isolate(None, false, Some(Duration::from_millis(200))).await;
    assert!(stream_channels
        .client_to_junction
        .send(create_random_request(&service_info))
        .await
        .is_ok());
    let stream_err = timeout(Duration::from_secs(2), stream_channels.junction_to_client.recv())
        .await
        .expect("Expected stream connect failure")
        .expect("Expected error response")
        .unwrap_err();
    assert_eq!(stream_err.to_tonic_status().code(), tonic::Code::Internal);
}

#[tokio::test]
async fn test_shm_transfer_headers_and_stream_edge_cases() {
    let (isolate_junction, isolate_state_manager, shared_mem_manager, service_info, isolate_id) =
        setup_standalone_junction(64, 4, 4, true).await;
    let dir = tempfile::tempdir().unwrap();
    shared_mem_manager
        .setup_bridge_communication_buffers(isolate_id, &dir.path().to_string_lossy())
        .await
        .expect("Should setup SHM buffers");
    let state = Arc::new(Mutex::new(ScriptedBridgeState::default()));
    let sock_path = dir.path().join("bridge.sock").to_string_lossy().into_owned();
    let shutdown_tx = start_scripted_bridge_server(&sock_path, state.clone()).await;
    isolate_state_manager.add_isolate(create_add_isolate_request(isolate_id)).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();
    assert!(isolate_junction.connect_isolate(isolate_id, sock_path).await.is_ok());

    let mut req = create_random_request(&service_info);
    req.isolate_input = Some(payload_proto::enforcer::v1::EzHybridPayload {
        delivery_method: Some(
            payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::InlineData(
                EzPayloadData { datagrams: vec![b"sixteen-byte-msg".to_vec()] },
            ),
        ),
    });
    let headers = &mut req.control_plane_metadata.as_mut().unwrap().metadata_headers;
    headers.insert(TEST_VALID_HEADER.to_string(), TEST_VALID_HEADER_VALUE.to_string());
    headers.insert("invalid key".to_string(), "ignored".to_string());
    headers.insert("x-invalid-val".to_string(), "bad\nval".to_string());
    let resp = isolate_junction
        .invoke_isolate(None, req, false, Some(Instant::now() + Duration::from_secs(2)))
        .await
        .expect("SHM unary call should succeed");
    assert!(resp.isolate_output.is_some());
    {
        let recorded = state.lock().unwrap();
        assert!(recorded.saw_shm_request, "Expected request payload converted to ShmData");
        assert_eq!(recorded.last_valid_header.as_deref(), Some(TEST_VALID_HEADER_VALUE));
    }

    let mut no_input_req = create_random_request(&service_info);
    no_input_req.isolate_input = None;
    assert!(isolate_junction.invoke_isolate(None, no_input_req, false, None).await.is_ok());

    let mut empty_datagrams_req = create_random_request(&service_info);
    empty_datagrams_req.isolate_input = Some(payload_proto::enforcer::v1::EzHybridPayload {
        delivery_method: Some(
            payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::InlineData(
                EzPayloadData { datagrams: vec![] },
            ),
        ),
    });
    assert!(isolate_junction.invoke_isolate(None, empty_datagrams_req, false, None).await.is_ok());

    state.lock().unwrap().return_missing_output_scope_with_data = true;
    let missing_out_scope_err = isolate_junction
        .invoke_isolate(None, create_random_request(&service_info), false, None)
        .await
        .unwrap_err();
    assert_eq!(missing_out_scope_err.to_string(), DataScopeError::InvalidDataScopeType.to_string());
    state.lock().unwrap().return_missing_output_scope_with_data = false;

    state.lock().unwrap().return_invalid_shm_slot = true;
    let bad_unary_shm = isolate_junction
        .invoke_isolate(None, create_random_request(&service_info), false, None)
        .await
        .unwrap_err();
    assert_eq!(bad_unary_shm.to_tonic_status().code(), tonic::Code::Internal);

    let mut bad_stream =
        isolate_junction.stream_invoke_isolate(None, false, Some(Duration::from_secs(2))).await;
    assert!(bad_stream.client_to_junction.send(create_random_request(&service_info)).await.is_ok());
    let bad_stream_shm = timeout(Duration::from_secs(2), bad_stream.junction_to_client.recv())
        .await
        .expect("Expected stream SHM error")
        .expect("Expected stream item")
        .unwrap_err();
    assert_eq!(bad_stream_shm.to_tonic_status().code(), tonic::Code::Internal);
    state.lock().unwrap().return_invalid_shm_slot = false;

    let mut stream =
        isolate_junction.stream_invoke_isolate(None, false, Some(Duration::from_secs(2))).await;
    let init_req = create_random_request(&service_info);
    assert!(stream.client_to_junction.send(init_req.clone()).await.is_ok());
    let first_ok = timeout(Duration::from_secs(2), stream.junction_to_client.recv())
        .await
        .expect("Expected echoed response after empty handshake frame")
        .expect("Expected stream item")
        .expect("Expected Ok response");
    assert_eq!(
        first_ok.control_plane_metadata.unwrap().ipc_message_id,
        init_req.control_plane_metadata.unwrap().ipc_message_id
    );

    let mut missing_mid_scope = create_random_request(&service_info);
    missing_mid_scope.isolate_input_iscope = None;
    assert!(stream.client_to_junction.send(missing_mid_scope).await.is_ok());
    let mid_err = timeout(Duration::from_secs(2), stream.junction_to_client.recv())
        .await
        .expect("Expected mid-stream scope error")
        .expect("Expected stream item")
        .unwrap_err();
    assert_eq!(mid_err.to_string(), DataScopeError::InvalidDataScopeType.to_string());

    let mut no_cpm_mid = create_random_request(&service_info);
    no_cpm_mid.control_plane_metadata = None;
    assert!(stream.client_to_junction.send(no_cpm_mid).await.is_ok());
    let reset_err = timeout(Duration::from_secs(2), stream.junction_to_client.recv())
        .await
        .expect("Expected stream abort from destination")
        .expect("Expected stream item")
        .unwrap_err();
    assert_eq!(reset_err.to_tonic_status().code(), tonic::Code::Aborted);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(stream.client_to_junction.send(create_random_request(&service_info)).await.is_ok());
    let closed_err = timeout(Duration::from_secs(2), stream.junction_to_client.recv())
        .await
        .expect("Expected error after destination closes stream")
        .expect("Expected stream item")
        .unwrap_err();
    assert_eq!(closed_err.to_tonic_status().code(), tonic::Code::Unavailable);

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_fileshare_allowed_and_missing_caller_or_scope_errors() {
    let mut test_harness = TestHarness::new().await;
    let read_only_mounts =
        spawn_fake_container_manager(test_harness.container_manager_request_rx.take().unwrap());
    let fileshare_handle = test_harness
        .fileshare_manager
        .create_fileshare(test_harness.echo_isolate_id, DataScopeType::UserPrivate)
        .await
        .unwrap();
    let memshare_handle = test_harness
        .shared_mem_manager
        .create_shared_mem_file(test_harness.echo_isolate_id, 1024, DataScopeType::UserPrivate)
        .await
        .unwrap()
        .shared_memory_handle;

    let allowed_receiver = add_receiver_isolate(&test_harness, DataScopeType::UserPrivate).await;
    let ok_resp = invoke_with_share_handles(
        &test_harness,
        allowed_receiver,
        vec![],
        vec![fileshare_handle.clone()],
    )
    .await;
    assert!(ok_resp.is_ok());
    assert_eq!(receiver_scope(&test_harness, allowed_receiver).await, DataScopeType::UserPrivate);
    assert!(read_only_mounts.lock().unwrap().contains(&allowed_receiver));

    let echo_info = test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap();
    let mut no_caller_mem_req = create_random_request(echo_info);
    no_caller_mem_req.control_plane_metadata.as_mut().unwrap().shared_memory_handles =
        vec![memshare_handle];
    let mem_err = test_harness
        .isolate_junction
        .invoke_isolate(None, no_caller_mem_req, false, None)
        .await
        .unwrap_err();
    assert_eq!(mem_err.to_tonic_status().code(), tonic::Code::Internal);

    let mut no_caller_file_req = create_random_request(echo_info);
    no_caller_file_req.control_plane_metadata.as_mut().unwrap().fileshare_handles =
        vec![fileshare_handle.clone()];
    let file_err = test_harness
        .isolate_junction
        .invoke_isolate(None, no_caller_file_req.clone(), false, None)
        .await
        .unwrap_err();
    assert_eq!(file_err.to_tonic_status().code(), tonic::Code::Internal);

    let public_receiver = add_receiver_isolate(&test_harness, DataScopeType::Public).await;
    let mut denied_stream = test_harness
        .isolate_junction
        .stream_invoke_isolate(Some(public_receiver), false, None)
        .await;
    assert!(denied_stream.client_to_junction.send(no_caller_file_req.clone()).await.is_ok());
    let stream_denied = timeout(Duration::from_secs(2), denied_stream.junction_to_client.recv())
        .await
        .expect("Expected stream fileshare rejection")
        .expect("Expected stream item")
        .unwrap_err();
    assert_eq!(stream_denied.to_tonic_status().code(), tonic::Code::Internal);
    assert_eq!(receiver_scope(&test_harness, public_receiver).await, DataScopeType::Public);

    let mut no_caller_stream =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;
    assert!(no_caller_stream.client_to_junction.send(no_caller_file_req).await.is_ok());
    let stream_no_caller_err =
        timeout(Duration::from_secs(2), no_caller_stream.junction_to_client.recv())
            .await
            .expect("Expected missing caller fileshare error")
            .expect("Expected stream item")
            .unwrap_err();
    assert_eq!(stream_no_caller_err.to_tonic_status().code(), tonic::Code::Internal);

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_metadata_propagation_to_isolate() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let (container_manager_request_tx, _container_manager_request_rx) =
        tokio::sync::mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_request_tx);
    let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone(), 0, 0);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let manifest_validator = ManifestValidator::default();
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let isolate_junction = IsolateJunction::new(IsolateJunctionArgs {
        data_scope_requester: data_scope_requester.clone(),
        isolate_service_mapper: isolate_service_mapper.clone(),
        shared_mem_manager: shared_memory_manager.clone(),
        fileshare_manager: fileshare_manager.clone(),
        state_manager: isolate_state_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shm_payload_threshold: 100 * 1024 * 1024, // 100MiB
        max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
    });

    let isolate_service_info = IsolateServiceInfo {
        operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
        service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
        ..Default::default()
    };

    let binary_services_index = isolate_service_mapper
        .new_binary_index(
            vec![isolate_service_info.clone()],
            false,
            "publisher_id".to_string(),
            "isolate_name".to_string(),
        )
        .await
        .expect("Should be a valid binary services index");

    manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index,
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope: DataScopeType::UserPrivate,
        })
        .await
        .expect("Should succeed to add input output scopes in manifest validator");

    let isolate_id = IsolateId::new(binary_services_index);

    // Start our custom isolate server
    let header_key = "x-custom-header";
    let header_value = "custom-value";
    let isolate_server_shutdown_tx = start_metadata_checking_isolate_server(
        isolate_id,
        header_key.to_string(),
        header_value.to_string(),
    )
    .await;

    let add_isolate_request = create_add_isolate_request(isolate_id);
    isolate_state_manager.add_isolate(add_isolate_request).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

    assert!(isolate_junction
        .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id),)
        .await
        .is_ok());

    let mut invoke_isolate_request = create_random_request(&isolate_service_info);

    // Add the header to metadata
    let mut metadata_headers = HashMap::new();
    metadata_headers.insert(header_key.to_string(), header_value.to_string());
    if let Some(cp_metadata) = invoke_isolate_request.control_plane_metadata.as_mut() {
        cp_metadata.metadata_headers = metadata_headers;
    }

    let invoke_result =
        isolate_junction.invoke_isolate(None, invoke_isolate_request, false, None).await;

    let _ = isolate_server_shutdown_tx.send(());

    assert!(invoke_result.is_ok(), "Invoke isolate should succeed if header matches");
}

#[tokio::test]
async fn test_junction_unary_flow() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let invoke_isolate_response = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), false, None)
        .await
        .unwrap();

    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    assert_eq!(invoke_isolate_response, expected_invoke_isolate_response)
}

#[tokio::test]
async fn test_junction_application_error_unary_flow() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ERROR_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let status = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), false, None)
        .await
        .expect_err("Request should fail")
        .to_tonic_status();

    let expected_status = Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    assert_eq!(status.code(), expected_status.code());
    assert_eq!(status.message(), expected_status.message());
}

#[tokio::test]
async fn test_junction_unary_flow_manifest_validation_failure() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create an InvokeIsolateRequest with a scope that is not allowed by the manifest.
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    // The manifest in TestHarness allows up to UserPrivate. Sealed is stricter.
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::Sealed.into();

    let invoke_result = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), false, None)
        .await;

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    let err = invoke_result.unwrap_err();
    assert_eq!(err.to_string(), DataScopeError::DisallowedByManifest.to_string());
}

#[tokio::test]
async fn test_validate_streaming_request_scope_failure() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());

    // Send InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive first expected InvokeIsolateResponse
    let invoke_isolate_response_result =
        client_junction_channels.junction_to_client.recv().await.unwrap();

    assert_eq!(invoke_isolate_response_result.unwrap(), expected_invoke_isolate_response);

    // for subsequent requests don't populate Isolate targeting fields
    let empty_isolate_info = IsolateServiceInfo {
        operator_domain: "".to_string(),
        service_name: "".to_string(),
        ..Default::default()
    };

    let mut invoke_isolate_request = create_random_request(&empty_isolate_info);
    // Change DataScopeType to Sealed which is more private than strictest (UserPrivate)
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::Sealed.into();
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let validation_error =
        client_junction_channels.junction_to_client.recv().await.unwrap().unwrap_err();
    assert_eq!(validation_error.to_string(), DataScopeError::DisallowedByManifest.to_string());

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_streaming_subsequent_request_manifest_validation_failure() {
    // Create IsolateJunction w/ fake echo Isolate.
    // DSM will have maximum scope as Userprivate because max-output scope = USER_PRIVATE and
    // max permissible scope is max(max-input scope, max-output scope)
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::DomainOwned,
        None,
    )
    .await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Send a valid initial request to establish the stream.
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    // Change DataScopeType to Public so that first request can go through
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::Public.into();
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive the successful response for the first request.
    assert!(client_junction_channels.junction_to_client.recv().await.unwrap().is_ok());

    let empty_isolate_info = IsolateServiceInfo {
        operator_domain: "".to_string(),
        service_name: "".to_string(),
        ..Default::default()
    };

    // This request should fail because even though Validate Isolate can scope-drag to USER_PRIVATE scope
    // manifest validation will block this request.
    let invoke_isolate_request = create_random_request(&empty_isolate_info);
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let validation_error =
        client_junction_channels.junction_to_client.recv().await.unwrap().unwrap_err();
    assert_eq!(validation_error.to_string(), DataScopeError::DisallowedByManifest.to_string());

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_validate_streaming_request_scope_success() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let first_ipc_message_id =
        invoke_isolate_request.control_plane_metadata.as_ref().unwrap().ipc_message_id;
    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());

    // Send InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive first expected InvokeIsolateResponse
    let invoke_isolate_response_result =
        client_junction_channels.junction_to_client.recv().await.unwrap();

    assert_eq!(invoke_isolate_response_result.unwrap(), expected_invoke_isolate_response);

    // for subsequent requests don't populate Isolate targeting fields
    let empty_isolate_info = IsolateServiceInfo {
        operator_domain: "".to_string(),
        service_name: "".to_string(),
        ..Default::default()
    };

    let invoke_isolate_request = create_random_request(&empty_isolate_info);
    let mut expected_invoke_isolate_response_2 =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());
    // The junction reuses the ipc_message_id from the initial request for all streaming responses.
    expected_invoke_isolate_response_2.control_plane_metadata.as_mut().unwrap().ipc_message_id =
        first_ipc_message_id;
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let invoke_isolate_response =
        client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();

    assert_eq!(expected_invoke_isolate_response_2, invoke_isolate_response);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_isolate_retires_after_sensitive_session_and_resets_when_idle() {
    // Set a sensitive session threshold of 1.
    let mut test_harness = TestHarness::new_with_arguments(
        1,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a sensitive scope (UserPrivate).
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());

    // Create a listener for the reset request.
    let mut rx = test_harness.container_manager_request_rx.take().unwrap();
    let reset_listener = tokio::spawn(async move {
        if let Some(ContainerManagerRequest::ResetIsolateRequest { resp, .. }) = rx.recv().await {
            let _ = resp.send(Ok(ResetIsolateResponse {}));
            return true;
        }
        false
    });

    // Create junction client channels, send the sensitive request, which increments the counter.
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;
    assert!(client_junction_channels
        .client_to_junction
        .send(invoke_isolate_request.clone())
        .await
        .is_ok());

    // The first request should succeed. The response will trigger the state update to Retiring.
    let invoke_isolate_response_result =
        client_junction_channels.junction_to_client.recv().await.unwrap();
    assert_eq!(invoke_isolate_response_result.unwrap(), expected_invoke_isolate_response);

    // The Isolate is now Retiring with 1 in-flight request (the stream).
    // Close the stream, which will decrement the counter, transition the Isolate from
    // Retiring to Idle, and trigger the reset.
    drop(client_junction_channels);

    // Wait for the reset listener to confirm the reset request was received.
    assert!(reset_listener.await.unwrap(), "Container reset was not triggered");

    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let invoke_isolate_response_result_2 =
        client_junction_channels.junction_to_client.recv().await.unwrap();
    let err = invoke_isolate_response_result_2.unwrap_err();
    assert_eq!(err.to_string(), DataScopeError::NoMatchingIsolates.to_string());

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_junction_streaming_flow() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());

    // Send InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive first expected InvokeIsolateResponse
    let invoke_isolate_response_result =
        client_junction_channels.junction_to_client.recv().await.unwrap();

    assert_eq!(invoke_isolate_response_result.unwrap(), expected_invoke_isolate_response);

    // for subsequent requests don't populate Isolate targeting fields
    let empty_isolate_info = IsolateServiceInfo {
        operator_domain: "".to_string(),
        service_name: "".to_string(),
        ..Default::default()
    };

    let mut response_set = HashSet::new();
    // spawn 20 requests concurrently and save the random data in a vec
    for _ in 0..20 {
        let invoke_isolate_request = create_random_request(&empty_isolate_info);
        response_set.insert(
            match invoke_isolate_request
                .isolate_input
                .as_ref()
                .unwrap()
                .delivery_method
                .as_ref()
                .unwrap()
            {
                payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::InlineData(d) => {
                    d.datagrams[0].clone()
                }
                _ => panic!("Expected inline data"),
            },
        );
        let sender_clone = client_junction_channels.client_to_junction.clone();
        tokio::spawn(async move {
            assert!(sender_clone.send(invoke_isolate_request).await.is_ok());
        });
    }

    // assert that the random data in the response came from one of the requests
    // the rest of the response is empty since we didn't populate any routing fields
    for _ in 0..20 {
        let invoke_isolate_response =
            client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();
        let datagrams =
            match invoke_isolate_response.isolate_output.unwrap().delivery_method.unwrap() {
                payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::InlineData(
                    inline,
                ) => inline.datagrams,
                _ => panic!("Expected InlineData"),
            };
        assert!(response_set.contains(&datagrams[0]));
    }

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_junction_streaming_application_error_first_request() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ERROR_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive first expected InvokeIsolateResponse
    let expected_status = Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE);
    let status = client_junction_channels
        .junction_to_client
        .recv()
        .await
        .unwrap()
        .expect_err("First request should fail")
        .to_tonic_status();
    assert_eq!(status.code(), expected_status.code());
    assert_eq!(status.message(), expected_status.message());

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_junction_streaming_application_error_subsequent_request() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request.clone());
    // Create second error InvokeIsolateRequest
    let error_invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ERROR_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send first InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // First request should succeed
    let response = client_junction_channels.junction_to_client.recv().await.unwrap();
    assert_eq!(response.unwrap(), expected_invoke_isolate_response);

    // Send second InvokeIsolateRequest to IsolateJunction
    assert!(client_junction_channels
        .client_to_junction
        .send(error_invoke_isolate_request)
        .await
        .is_ok());

    // Second request should fail
    let expected_status = Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE);
    let status = client_junction_channels
        .junction_to_client
        .recv()
        .await
        .unwrap()
        .expect_err("Second request should fail")
        .to_tonic_status();
    assert_eq!(status.code(), expected_status.code());
    assert_eq!(status.message(), expected_status.message());

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_junction_supports_multiple_services() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let clone_invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(CLONE_ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let invoke_isolate_response = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), false, None)
        .await
        .unwrap();

    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request);

    assert_eq!(invoke_isolate_response, expected_invoke_isolate_response);

    let clone_invoke_isolate_response = test_harness
        .isolate_junction
        .invoke_isolate(None, clone_invoke_isolate_request.clone(), false, None)
        .await
        .unwrap();

    let expected_clone_invoke_isolate_response =
        create_echo_invoke_isolate_response(clone_invoke_isolate_request);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    assert_eq!(clone_invoke_isolate_response, expected_clone_invoke_isolate_response);
}

#[tokio::test]
async fn test_junction_supports_qualified_services() {
    // Create IsolateJunction w/ fake echo Isolate
    let test_harness = TestHarness::new().await;

    // Create InvokeIsolateRequest and expected response
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(QUALIFIED_ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let invoke_isolate_response = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), false, None)
        .await
        .unwrap();

    let expected_invoke_isolate_response =
        create_echo_invoke_isolate_response(invoke_isolate_request);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    assert_eq!(invoke_isolate_response, expected_invoke_isolate_response);
}

#[tokio::test]
async fn test_junction_unary_flow_inflight_counter() {
    // Create IsolateJunction w/ fake echo Isolate
    let mut test_harness = TestHarness::new_with_arguments(
        1,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Listen for the reset request from IsolateStateManager
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut cm_rx = test_harness.container_manager_request_rx.take().unwrap();
    let listener = tokio::spawn(async move {
        if let Some(ContainerManagerRequest::ResetIsolateRequest { resp, .. }) = cm_rx.recv().await
        {
            let _ = resp.send(Ok(ResetIsolateResponse {}));
            tx.send(()).await.unwrap();
        }
    });

    // Send InvokeIsolateRequest to IsolateJunction, which should increment and then decrement the counter
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    // This request should trigger retiring of the Isolate
    let _ = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, None)
        .await
        .expect("Invoke Isolate should succeed");

    // The counter should now be 0, triggering a state change to Idle and a reset request
    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Reset request was not sent after the in-flight request completed");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
    listener.await.unwrap();
}

#[tokio::test]
async fn test_junction_streaming_flow_inflight_counter() {
    // Create IsolateJunction w/ fake echo Isolate
    let mut test_harness = TestHarness::new_with_arguments(
        1,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Listen for the reset request from IsolateStateManager
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut cm_rx = test_harness.container_manager_request_rx.take().unwrap();
    let listener = tokio::spawn(async move {
        if let Some(ContainerManagerRequest::ResetIsolateRequest { resp, .. }) = cm_rx.recv().await
        {
            let _ = resp.send(Ok(ResetIsolateResponse {}));
            tx.send(()).await.unwrap();
        }
    });

    // Create streaming junction client channels
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Send first InvokeIsolateRequest to IsolateJunction, incrementing counter to 1
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Receive the response. The counter is still 1 because the stream is open.
    client_junction_channels
        .junction_to_client
        .recv()
        .await
        .expect("Should receive valid response")
        .expect("Should be a valid InvokeIsolateResponse");

    // The reset should not have happened yet.
    assert!(
        timeout(Duration::from_millis(100), rx.recv()).await.is_err(),
        "Reset request was sent prematurely before stream was closed"
    );

    // Close the client side of the stream, which will trigger teardown and decrement the counter.
    drop(client_junction_channels);

    // The counter should now be 0, triggering a state change to Idle and a reset request.
    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Reset request was not sent after the stream was closed");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
    listener.await.unwrap();
}

#[tokio::test]
async fn test_unary_scope_enforcement_failure_public_api() {
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepSame,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a UserPrivate scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Make the call from the "public API". The junction should reject a response with a UserPrivate scope.
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, true, None)
        .await
        .expect_err("Unary request should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unary_scope_enforcement_failure_public_api_sensitive_scope() {
    let test_harness = TestHarness::new().await;

    // Create a request with a UserPrivate scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Make the call from the "public API". The junction should reject a response with a UserPrivate scope.
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, true, None)
        .await
        .expect_err("Unary request should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unary_scope_enforcement_failure_ratified_isolate_public_api() {
    // Create a TestHarness with a Ratified Isolate.
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepSame,
        true,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a UserPrivate scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Make the call from the "public API". For a Ratified Isolate, this should fail
    // as public API egress checks are still enforced for sensitive scopes.
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request.clone(), true, None)
        .await
        .expect_err(
            "Ratified isolate should not be allowed to egress sensitive scope via public API",
        );

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unary_scope_enforcement_failure_manifest_output_scope() {
    // Create a TestHarness where the fake Isolate will "drag" the scope up by one level.
    // The manifest is configured in TestHarness::new_with_retirement to allow
    // max_output_scope: DataScopeType::UserPrivate.
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::IncreaseByOne,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a UserPrivate scope.
    // The fake Isolate will attempt to respond with MultiUserPrivate (UserPrivate + 1).
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // The Isolate will try to respond with MultiUserPrivate, which is stricter than the
    // manifest's allowed max_output_scope (UserPrivate). This should be rejected.
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, None)
        .await
        .expect_err("Unary call shouldn't be allowed");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unary_scope_enforcement_failure_scope_downgrade() {
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::DecreaseByOne,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a DomainOwned scope.
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::DomainOwned.into();

    // The Isolate will respond with Public (DomainOwned - 1)
    test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, None)
        .await
        .expect_err("Unary call shouldn't be allowed");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_streaming_scope_enforcement_failure_public_api() {
    let test_harness = TestHarness::new().await;

    // Create streaming junction client channels from the "public API".
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, true, None).await;

    // Create a request with a UserPrivate scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send the request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // The junction should reject a response with a UserPrivate scope from the public API.
    client_junction_channels
        .junction_to_client
        .recv()
        .await
        .expect("Should receive error response")
        .expect_err("Streaming call should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_streaming_manifest_output_scope_failure_initial_request() {
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::IncreaseByOne,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create streaming junction client channels.
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Create a request with a UserPrivate scope.
    // The fake Isolate will attempt to respond with MultiUserPrivate (UserPrivate + 1).
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send the request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // The Isolate will try to respond with MultiUserPrivate, which is stricter than the
    // manifest's allowed max_output_scope (UserPrivate). This should be rejected.
    client_junction_channels
        .junction_to_client
        .recv()
        .await
        .expect("Should receive error response")
        .expect_err("Streaming call should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_enforcement_failure_scope_downgrade() {
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::DecreaseByOne,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create streaming junction client channels.
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Create a request with a UserPrivate scope.
    // The fake Isolate will attempt to respond with Public (DomainOwned + 1).
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    invoke_isolate_request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::DomainOwned.into();

    // Send the request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // The Isolate will try to respond with DomainOwned, which is stricter than the
    // manifest's allowed max_output_scope (UserPrivate). This should be rejected.
    client_junction_channels
        .junction_to_client
        .recv()
        .await
        .expect("Should receive error response")
        .expect_err("Streaming call should fail");

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_unary_unspecified_scope_is_overridden() {
    // Create a TestHarness where the fake Isolate will respond with an Unspecified scope.
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create a request with a UserPrivate scope. This will set the Isolate's current scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // The fake Isolate will respond with an Unspecified scope, but the junction should
    // override it with the Isolate's current scope (UserPrivate).
    let response = test_harness
        .isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, None)
        .await
        .expect("Unary call should be allowed");

    let output_scope = response.isolate_output_iscope.unwrap().datagram_iscopes[0].scope_type();
    assert_eq!(
        output_scope,
        DataScopeType::UserPrivate,
        "Expected output scope to be overridden to the isolate's current scope"
    );

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_streaming_unspecified_scope_is_overridden() {
    // Create a TestHarness where the fake Isolate will respond with an Unspecified scope.
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;

    // Create streaming junction client channels.
    let mut client_junction_channels =
        test_harness.isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Create a request with a UserPrivate scope. This will set the Isolate's current scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send the request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());
    // The fake Isolate will respond with Unspecified, but the junction should override it.
    let response = client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();
    let output_scope = response.isolate_output_iscope.unwrap().datagram_iscopes[0].scope_type();
    assert_eq!(output_scope, DataScopeType::UserPrivate);

    // Create a second request with a UserPrivate scope. This will set the Isolate's current scope.
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Send the second request.
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());
    // The fake Isolate will respond with Unspecified, but the junction should override it.
    let response = client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();
    let output_scope = response.isolate_output_iscope.unwrap().datagram_iscopes[0].scope_type();
    assert_eq!(output_scope, DataScopeType::UserPrivate);

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_junction_unary_timeout() {
    // Create IsolateJunction w/ fake echo Isolate that has a delay
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepSame,
        false,
        DataScopeType::UserPrivate,
        Some(Duration::from_secs(5)),
    )
    .await;

    // Create InvokeIsolateRequest
    let invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Invoke the isolate with a short timeout
    let invoke_result = test_harness
        .isolate_junction
        .invoke_isolate(
            None,
            invoke_isolate_request,
            false,
            Some(Instant::now() + Duration::from_millis(100)),
        )
        .await;

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    // Assert that the call timed out
    assert!(invoke_result.is_err());
    let err = invoke_result.unwrap_err();
    assert!(matches!(
        err,
        ez_error::EzError::Status(status) if status.code() == tonic::Code::Cancelled
    ));
}

#[tokio::test]
async fn test_junction_unary_timeout_internal_route() {
    // Create IsolateJunction w/ fake echo Isolate that has a delay
    let test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepSame,
        false,
        DataScopeType::UserPrivate,
        Some(Duration::from_secs(5)),
    )
    .await;

    // Create InvokeIsolateRequest
    let mut invoke_isolate_request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );

    // Mark the request as an internal route
    if let Some(cp_metadata) = invoke_isolate_request.control_plane_metadata.as_mut() {
        cp_metadata.requester_is_local = true;
    }

    // Invoke the isolate with a short timeout
    let invoke_result = test_harness
        .isolate_junction
        .invoke_isolate(
            None,
            invoke_isolate_request,
            false,
            Some(Instant::now() + Duration::from_millis(100)),
        )
        .await;

    // Shutdown fake Isolate server
    let _ = test_harness.isolate_server_shutdown_tx.send(());

    // Assert that the call timed out
    assert!(invoke_result.is_err());
    let err = invoke_result.unwrap_err();
    assert!(matches!(
        err,
        ez_error::EzError::Status(status) if status.code() == tonic::Code::Cancelled
    ));
}

#[tokio::test]
async fn test_junction_unary_datascope_bypass() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let (container_manager_request_tx, _container_manager_request_rx) =
        tokio::sync::mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_request_tx);
    let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone(), 0, 0);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let manifest_validator = ManifestValidator::default();
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());

    let isolate_junction = IsolateJunction::new(IsolateJunctionArgs {
        data_scope_requester: data_scope_requester.clone(),
        isolate_service_mapper: isolate_service_mapper.clone(),
        shared_mem_manager: shared_memory_manager.clone(),
        fileshare_manager: fileshare_manager.clone(),
        state_manager: isolate_state_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shm_payload_threshold: 100 * 1024 * 1024,
        max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
    });

    let isolate_service_info = IsolateServiceInfo {
        operator_domain: "scope.test".to_string(),
        service_name: "ScopeTestIervice".to_string(),
        ..Default::default()
    };

    let binary_services_index = isolate_service_mapper
        .new_binary_index(
            vec![isolate_service_info.clone()],
            false,
            "publisher_id".to_string(),
            "isolate_name".to_string(),
        )
        .await
        .unwrap();

    manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index,
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope: DataScopeType::UserPrivate,
        })
        .await
        .unwrap();

    let isolate_id = IsolateId::new(binary_services_index);
    let isolate_server_shutdown_tx = start_datascope_test_isolate_server(isolate_id).await;

    let add_isolate_request = create_add_isolate_request(isolate_id);
    isolate_state_manager.add_isolate(add_isolate_request).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

    assert!(isolate_junction
        .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id))
        .await
        .is_ok());

    let mut invoke_isolate_request = create_random_request(&isolate_service_info);
    invoke_isolate_request.isolate_input_iscope = Some(EzPayloadIsolateScope {
        datagram_iscopes: vec![IsolateDataScope {
            scope_type: DataScopeType::UserPrivate.into(),
            mapped_scope_owner: None,
        }],
    });

    // Isolate returning an empty payload and scope with extensions payload
    // to the public API is blocked.
    let invoke_result =
        isolate_junction.invoke_isolate(None, invoke_isolate_request, true, None).await;

    let _ = isolate_server_shutdown_tx.send(());

    assert_eq!(
        invoke_result.unwrap_err().to_string(),
        DataScopeError::InvalidDataScopeType.to_string()
    );
}

#[tokio::test]
async fn test_junction_streaming_deadlock_fix() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let (container_manager_request_tx, _container_manager_request_rx) =
        tokio::sync::mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_request_tx);
    let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone(), 0, 0);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let manifest_validator = ManifestValidator::default();
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let isolate_junction = IsolateJunction::new(IsolateJunctionArgs {
        data_scope_requester: data_scope_requester.clone(),
        isolate_service_mapper: isolate_service_mapper.clone(),
        shared_mem_manager: shared_memory_manager.clone(),
        fileshare_manager: fileshare_manager.clone(),
        state_manager: isolate_state_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shm_payload_threshold: 100 * 1024 * 1024,
        max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
    });

    let isolate_service_info = IsolateServiceInfo {
        operator_domain: "deadlock_domain".to_string(),
        service_name: "deadlock_service".to_string(),
        ..Default::default()
    };

    let binary_services_index = isolate_service_mapper
        .new_binary_index(
            vec![isolate_service_info.clone()],
            false,
            "publisher_id".to_string(),
            "isolate_name".to_string(),
        )
        .await
        .unwrap();

    manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index,
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope: DataScopeType::UserPrivate,
        })
        .await
        .unwrap();

    let isolate_id = IsolateId::new(binary_services_index);

    let isolate_server_shutdown_tx = start_deadlock_testing_isolate_server(isolate_id).await;

    let add_isolate_request = create_add_isolate_request(isolate_id);
    isolate_state_manager.add_isolate(add_isolate_request).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

    assert!(isolate_junction
        .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id))
        .await
        .is_ok());

    let mut client_junction_channels =
        isolate_junction.stream_invoke_isolate(None, false, None).await;

    let mut req1 = create_random_request(&isolate_service_info);
    req1.control_plane_metadata.as_mut().unwrap().destination_method_name =
        "deadlock_method".to_string();

    // Create second request using the empty service info logic like other streaming tests
    let empty_isolate_info = IsolateServiceInfo {
        operator_domain: "".to_string(),
        service_name: "".to_string(),
        ..Default::default()
    };
    let req2 = create_random_request(&empty_isolate_info);

    // Send the first request (which initiates the gRPC stream)
    client_junction_channels.client_to_junction.send(req1).await.unwrap();

    // Give it a tiny bit of time to reach the junction and initiate the connection
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send the second request
    client_junction_channels.client_to_junction.send(req2).await.unwrap();

    // Use timeout to ensure it doesn't deadlock. Without the fix, this times out.
    let res1 = timeout(Duration::from_secs(2), client_junction_channels.junction_to_client.recv())
        .await
        .expect("Test deadlocked!")
        .unwrap()
        .unwrap();

    let res2 = timeout(Duration::from_secs(2), client_junction_channels.junction_to_client.recv())
        .await
        .expect("Test deadlocked!")
        .unwrap()
        .unwrap();

    assert!(res1.isolate_output.is_some());
    assert!(res2.isolate_output.is_some());

    let _ = isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_junction_unary_flow_shm_response_to_inline_data() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let (container_manager_request_tx, _container_manager_request_rx) =
        tokio::sync::mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_request_tx);
    let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone(), 64, 4);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let manifest_validator = ManifestValidator::default();
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let isolate_junction = IsolateJunction::new(IsolateJunctionArgs {
        data_scope_requester: data_scope_requester.clone(),
        isolate_service_mapper: isolate_service_mapper.clone(),
        shared_mem_manager: shared_memory_manager.clone(),
        fileshare_manager: fileshare_manager.clone(),
        state_manager: isolate_state_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shm_payload_threshold: 100 * 1024 * 1024, // 100MiB
        max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
    });

    let isolate_service_info = IsolateServiceInfo {
        operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
        service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
        ..Default::default()
    };

    let binary_services_index = isolate_service_mapper
        .new_binary_index(
            vec![isolate_service_info.clone()],
            false,
            "publisher_id".to_string(),
            "isoate_name".to_string(),
        )
        .await
        .expect("Should be a valid binary services index");

    manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index,
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope: DataScopeType::UserPrivate,
        })
        .await
        .expect("Should succeed to add input output scopes in manifest validator");

    let isolate_id = IsolateId::new(binary_services_index);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();

    // Setup bridge communication buffers in the shared memory manager
    shared_memory_manager
        .setup_bridge_communication_buffers(isolate_id, &path)
        .await
        .expect("Failed to setup bridge communication buffers");

    // Create the isolate writer pool
    let isolate_pool = shm_slab_pool::ShmSlabPool::new(shm_slab_pool::ShmSlabPoolOptions {
        file_name: format!("{path}/isolate-writes"),
        number_of_slots: 64,
        slot_size: 4,
        writer: true,
    })
    .expect("Failed to create isolate writer pool");

    let test_payload = b"hello shm world";
    let slot_refs =
        isolate_pool.write_to_pool(test_payload).await.expect("Failed to write to pool");

    // Start fake Isolate server
    let isolate_server_shutdown_tx = isolate_test_utils::start_fake_isolate_server(
        isolate_id,
        ScopeDragInstruction::KeepSame,
        None,
    )
    .await;

    let add_isolate_request = create_add_isolate_request(isolate_id);
    isolate_state_manager.add_isolate(add_isolate_request).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

    assert!(isolate_junction
        .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id),)
        .await
        .is_ok());

    let mut invoke_isolate_request = create_random_request(&isolate_service_info);
    invoke_isolate_request.isolate_input = Some(payload_proto::enforcer::v1::EzHybridPayload {
        delivery_method: Some(
            payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::ShmData(
                payload_proto::enforcer::v1::ShmSlotData { slots: slot_refs },
            ),
        ),
    });

    let response = isolate_junction
        .invoke_isolate(None, invoke_isolate_request, false, None)
        .await
        .expect("Invoke isolate should succeed");

    let _ = isolate_server_shutdown_tx.send(());

    let payload = response.isolate_output.expect("Should have payload");
    let delivery = payload.delivery_method.expect("Should have delivery method");
    match delivery {
        payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::InlineData(data) => {
            assert_eq!(data.datagrams.len(), 1);
            assert_eq!(data.datagrams[0], test_payload);
        }
        _ => panic!("Expected InlineData delivery method"),
    }
}

#[tokio::test]
async fn test_junction_streaming_request_shm_payload() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let (container_manager_request_tx, _container_manager_request_rx) =
        tokio::sync::mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_request_tx);
    let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone(), 64, 4);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let manifest_validator = ManifestValidator::default();
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let isolate_junction = IsolateJunction::new(IsolateJunctionArgs {
        data_scope_requester: data_scope_requester.clone(),
        isolate_service_mapper: isolate_service_mapper.clone(),
        shared_mem_manager: shared_memory_manager.clone(),
        fileshare_manager: fileshare_manager.clone(),
        state_manager: isolate_state_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shm_payload_threshold: 1, // Threshold of 1 byte to force SHM writing
        max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
    });

    let isolate_service_info = IsolateServiceInfo {
        operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
        service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
        ..Default::default()
    };

    let binary_services_index = isolate_service_mapper
        .new_binary_index(
            vec![isolate_service_info.clone()],
            false,
            "publisher_id".to_string(),
            "isolate_name".to_string(),
        )
        .await
        .expect("Should be a valid binary services index");

    manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index,
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope: DataScopeType::UserPrivate,
        })
        .await
        .expect("Should succeed to add input output scopes in manifest validator");

    let isolate_id = IsolateId::new(binary_services_index);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();

    // Setup bridge communication buffers in the shared memory manager
    shared_memory_manager
        .setup_bridge_communication_buffers(isolate_id, &path)
        .await
        .expect("Failed to setup bridge communication buffers");

    // Create the enforcer reader pool to verify written slots
    let enforcer_pool = shm_slab_pool::ShmSlabPool::new(shm_slab_pool::ShmSlabPoolOptions {
        file_name: format!("{path}/enforcer-writes"),
        number_of_slots: 64,
        slot_size: 4,
        writer: false,
    })
    .expect("Failed to create enforcer reader pool");

    // Start custom isolate server
    let received_requests = Arc::new(Mutex::new(Vec::new()));
    let fake_isolate = ShmRequestCheckingIsolate { received_requests: received_requests.clone() };
    let unix_listener =
        UnixListener::bind(format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id)).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        Server::builder()
            .add_service(EzIsolateBridgeServer::new(fake_isolate))
            .serve_with_incoming_shutdown(UnixListenerStream::new(unix_listener), async {
                shutdown_rx.await.ok();
            })
            .await
    });

    let add_isolate_request = create_add_isolate_request(isolate_id);
    isolate_state_manager.add_isolate(add_isolate_request).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

    assert!(isolate_junction
        .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id),)
        .await
        .is_ok());

    let mut client_junction_channels =
        isolate_junction.stream_invoke_isolate(None, false, None).await;

    let invoke_isolate_request = create_random_request(&isolate_service_info);
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let _response = client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();

    let _ = shutdown_tx.send(());

    // Verify that the isolate received the request as ShmData
    let received = received_requests.lock().unwrap().last().unwrap().clone();
    let payload = received.isolate_input.expect("Should have payload");
    let delivery = payload.delivery_method.expect("Should have delivery method");
    match delivery {
        payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::ShmData(shm_data) => {
            let read_bytes =
                enforcer_pool.read_from_pool(&shm_data.slots).expect("Failed to read from pool");
            assert!(!read_bytes.is_empty());
        }
        _ => panic!("Expected ShmData delivery method"),
    }
}

#[tokio::test]
async fn test_junction_streaming_response_shm_payload() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let (container_manager_request_tx, _container_manager_request_rx) =
        tokio::sync::mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_request_tx);
    let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone(), 64, 4);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let manifest_validator = ManifestValidator::default();
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let isolate_junction = IsolateJunction::new(IsolateJunctionArgs {
        data_scope_requester: data_scope_requester.clone(),
        isolate_service_mapper: isolate_service_mapper.clone(),
        shared_mem_manager: shared_memory_manager.clone(),
        fileshare_manager: fileshare_manager.clone(),
        state_manager: isolate_state_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shm_payload_threshold: 100 * 1024 * 1024, // threshold high, so request is inline
        max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
    });

    let isolate_service_info = IsolateServiceInfo {
        operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
        service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
        ..Default::default()
    };

    let binary_services_index = isolate_service_mapper
        .new_binary_index(vec![isolate_service_info.clone()], false, "".to_string(), "".to_string())
        .await
        .expect("Should be a valid binary services index");

    manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index,
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope: DataScopeType::UserPrivate,
        })
        .await
        .expect("Should succeed to add input output scopes in manifest validator");

    let isolate_id = IsolateId::new(binary_services_index);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();

    // Setup bridge communication buffers in the shared memory manager
    shared_memory_manager
        .setup_bridge_communication_buffers(isolate_id, &path)
        .await
        .expect("Failed to setup bridge communication buffers");

    // Start custom isolate server
    let fake_isolate = ShmResponseCheckingIsolate { shm_path: path.clone() };
    let unix_listener =
        UnixListener::bind(format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id)).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        Server::builder()
            .add_service(EzIsolateBridgeServer::new(fake_isolate))
            .serve_with_incoming_shutdown(UnixListenerStream::new(unix_listener), async {
                shutdown_rx.await.ok();
            })
            .await
    });

    let add_isolate_request = create_add_isolate_request(isolate_id);
    isolate_state_manager.add_isolate(add_isolate_request).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

    assert!(isolate_junction
        .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id),)
        .await
        .is_ok());

    let mut client_junction_channels =
        isolate_junction.stream_invoke_isolate(None, false, None).await;

    let invoke_isolate_request = create_random_request(&isolate_service_info);
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    let response = client_junction_channels.junction_to_client.recv().await.unwrap().unwrap();

    let _ = shutdown_tx.send(());

    // Verify that the client received the response as InlineData (it was decrypted from SHM)
    let payload = response.isolate_output.expect("Should have payload");
    let delivery = payload.delivery_method.expect("Should have delivery method");
    match delivery {
        payload_proto::enforcer::v1::ez_hybrid_payload::DeliveryMethod::InlineData(data) => {
            assert_eq!(data.datagrams.len(), 1);
            assert_eq!(data.datagrams[0], b"stream shm echo");
        }
        _ => panic!("Expected InlineData delivery method"),
    }
}

#[tokio::test]
async fn test_junction_streaming_half_close_without_http2_headers() {
    let isolate_service_mapper = IsolateServiceMapper::default();
    let (container_manager_request_tx, _container_manager_request_rx) =
        mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_request_tx);
    let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone(), 0, 0);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let manifest_validator = ManifestValidator::default();
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let isolate_junction = IsolateJunction::new(IsolateJunctionArgs {
        data_scope_requester: data_scope_requester.clone(),
        isolate_service_mapper: isolate_service_mapper.clone(),
        shared_mem_manager: shared_memory_manager.clone(),
        fileshare_manager: fileshare_manager.clone(),
        state_manager: isolate_state_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shm_payload_threshold: 100 * 1024 * 1024,
        max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
    });

    let isolate_service_info = IsolateServiceInfo {
        operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
        service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
        ..Default::default()
    };

    let binary_services_index = isolate_service_mapper
        .new_binary_index(vec![isolate_service_info.clone()], false, "".to_string(), "".to_string())
        .await
        .expect("Should be a valid binary services index");

    manifest_validator
        .add_scope_info(AddManifestScopeRequest {
            binary_services_index,
            max_input_scope: DataScopeType::UserPrivate,
            max_output_scope: DataScopeType::UserPrivate,
        })
        .await
        .expect("Should succeed to add input output scopes in manifest validator");

    let isolate_id = IsolateId::new(binary_services_index);

    let (cancellation_tx, mut cancellation_rx) = mpsc::channel(1);
    let (first_request_tx, first_request_rx) = tokio::sync::oneshot::channel();

    let shutdown_tx =
        start_delayed_header_isolate_server(isolate_id, cancellation_tx, first_request_tx).await;

    let add_isolate_request = create_add_isolate_request(isolate_id);
    isolate_state_manager.add_isolate(add_isolate_request).await;
    isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

    assert!(isolate_junction
        .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id))
        .await
        .is_ok());

    let client_junction_channels = isolate_junction.stream_invoke_isolate(None, false, None).await;

    // Send the first request (which initiates the gRPC stream).
    // The server will intentionally NOT send HTTP/2 response headers back.
    let invoke_isolate_request = create_random_request(&isolate_service_info);
    assert!(client_junction_channels.client_to_junction.send(invoke_isolate_request).await.is_ok());

    // Wait deterministically for the Isolate to acknowledge the first message
    first_request_rx.await.expect("Server failed to receive first request!");

    // Drop the client-side sender! This mimics the upstream ATS/client abruptly
    // half-closing the stream.
    drop(client_junction_channels.client_to_junction);

    // Wait for the mock server to signal that it successfully received the EOF
    // and exited its loop.
    // Without the fix, this times out and panics because Tonic keeps the
    // stream open indefinitely.
    let _ = timeout(Duration::from_secs(4), cancellation_rx.recv())
        .await
        .expect("Test deadlocked! The isolate server never observed a stream half-close sent by the client.");

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_memshare_denied_when_receiver_scope_disallowed() {
    let mut test_harness = TestHarness::new().await;
    let read_only_mounts =
        spawn_fake_container_manager(test_harness.container_manager_request_rx.take().unwrap());
    let memshare_handle = test_harness
        .shared_mem_manager
        .create_shared_mem_file(test_harness.echo_isolate_id, 1024, DataScopeType::UserPrivate)
        .await
        .unwrap()
        .shared_memory_handle;
    let receiver_id = add_receiver_isolate(&test_harness, DataScopeType::Public).await;

    let result =
        invoke_with_share_handles(&test_harness, receiver_id, vec![memshare_handle], vec![]).await;

    assert!(result.is_err());
    assert_eq!(receiver_scope(&test_harness, receiver_id).await, DataScopeType::Public);
    assert!(!read_only_mounts.lock().unwrap().contains(&receiver_id));

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_fileshare_denied_when_receiver_scope_disallowed() {
    let mut test_harness = TestHarness::new().await;
    let read_only_mounts =
        spawn_fake_container_manager(test_harness.container_manager_request_rx.take().unwrap());
    let fileshare_handle = test_harness
        .fileshare_manager
        .create_fileshare(test_harness.echo_isolate_id, DataScopeType::UserPrivate)
        .await
        .unwrap();
    let receiver_id = add_receiver_isolate(&test_harness, DataScopeType::Public).await;

    let result =
        invoke_with_share_handles(&test_harness, receiver_id, vec![], vec![fileshare_handle]).await;

    assert!(result.is_err());
    assert_eq!(receiver_scope(&test_harness, receiver_id).await, DataScopeType::Public);
    assert!(!read_only_mounts.lock().unwrap().contains(&receiver_id));

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_memshare_allowed_when_receiver_scope_permits() {
    let mut test_harness = TestHarness::new().await;
    let read_only_mounts =
        spawn_fake_container_manager(test_harness.container_manager_request_rx.take().unwrap());
    let memshare_handle = test_harness
        .shared_mem_manager
        .create_shared_mem_file(test_harness.echo_isolate_id, 1024, DataScopeType::UserPrivate)
        .await
        .unwrap()
        .shared_memory_handle;
    let receiver_id = add_receiver_isolate(&test_harness, DataScopeType::UserPrivate).await;

    let result =
        invoke_with_share_handles(&test_harness, receiver_id, vec![memshare_handle], vec![]).await;

    assert!(result.is_ok());
    assert_eq!(receiver_scope(&test_harness, receiver_id).await, DataScopeType::UserPrivate);
    assert!(read_only_mounts.lock().unwrap().contains(&receiver_id));

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_memshare_from_ratified_sharer_uses_declared_scope() {
    // The sharer may emit UserPrivate but declares Public, so a Public receiver can mount it.
    let mut test_harness = TestHarness::new_with_arguments(
        u64::MAX,
        ScopeDragInstruction::KeepSame,
        true,
        DataScopeType::UserPrivate,
        None,
    )
    .await;
    let read_only_mounts =
        spawn_fake_container_manager(test_harness.container_manager_request_rx.take().unwrap());
    let memshare_handle = test_harness
        .shared_mem_manager
        .create_shared_mem_file(test_harness.echo_isolate_id, 1024, DataScopeType::Public)
        .await
        .unwrap()
        .shared_memory_handle;
    let receiver_id = add_receiver_isolate(&test_harness, DataScopeType::Public).await;

    let mut request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    request.isolate_input_iscope.as_mut().unwrap().datagram_iscopes[0].scope_type =
        DataScopeType::Public.into();
    request.control_plane_metadata.as_mut().unwrap().shared_memory_handles = vec![memshare_handle];

    let result =
        test_harness.isolate_junction.invoke_isolate(Some(receiver_id), request, false, None).await;

    assert!(result.is_ok());
    assert_eq!(receiver_scope(&test_harness, receiver_id).await, DataScopeType::Public);
    assert!(read_only_mounts.lock().unwrap().contains(&receiver_id));

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_memshare_retires_receiver_at_sensitive_session_threshold() {
    // Retire the receiver as soon as it takes part in a single sensitive session.
    let mut test_harness = TestHarness::new_with_arguments(
        1,
        ScopeDragInstruction::KeepUnspecified,
        false,
        DataScopeType::UserPrivate,
        None,
    )
    .await;
    let _read_only_mounts =
        spawn_fake_container_manager(test_harness.container_manager_request_rx.take().unwrap());
    let memshare_handle = test_harness
        .shared_mem_manager
        .create_shared_mem_file(test_harness.echo_isolate_id, 1024, DataScopeType::UserPrivate)
        .await
        .unwrap()
        .shared_memory_handle;
    let receiver_id = add_receiver_isolate(&test_harness, DataScopeType::UserPrivate).await;

    let result =
        invoke_with_share_handles(&test_harness, receiver_id, vec![memshare_handle], vec![]).await;

    assert!(result.is_ok());
    assert_eq!(
        test_harness.isolate_state_manager.get_isolate_state(receiver_id),
        Some(IsolateState::Retiring)
    );

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_streaming_memshare_denied_terminates_stream() {
    let mut test_harness = TestHarness::new().await;
    let read_only_mounts =
        spawn_fake_container_manager(test_harness.container_manager_request_rx.take().unwrap());
    let memshare_handle = test_harness
        .shared_mem_manager
        .create_shared_mem_file(test_harness.echo_isolate_id, 1024, DataScopeType::UserPrivate)
        .await
        .unwrap()
        .shared_memory_handle;
    let receiver_id = add_receiver_isolate(&test_harness, DataScopeType::Public).await;

    let mut client_channels =
        test_harness.isolate_junction.stream_invoke_isolate(Some(receiver_id), false, None).await;

    let mut request = create_random_request(
        test_harness.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap(),
    );
    let control_plane_metadata = request.control_plane_metadata.as_mut().unwrap();
    control_plane_metadata.shared_memory_handles = vec![memshare_handle];

    assert!(client_channels.client_to_junction.send(request).await.is_ok());

    let response_result = client_channels.junction_to_client.recv().await.unwrap();
    assert!(response_result.is_err());
    assert_eq!(receiver_scope(&test_harness, receiver_id).await, DataScopeType::Public);
    assert!(!read_only_mounts.lock().unwrap().contains(&receiver_id));

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_memshare_with_unowned_handle_fails_before_escalating_receiver() {
    let mut test_harness = TestHarness::new().await;
    let read_only_mounts =
        spawn_fake_container_manager(test_harness.container_manager_request_rx.take().unwrap());
    let owned_handle = test_harness
        .shared_mem_manager
        .create_shared_mem_file(test_harness.echo_isolate_id, 1024, DataScopeType::UserPrivate)
        .await
        .unwrap()
        .shared_memory_handle;
    let other_owner_id = add_receiver_isolate(&test_harness, DataScopeType::UserPrivate).await;
    let unowned_handle = test_harness
        .shared_mem_manager
        .create_shared_mem_file(other_owner_id, 1024, DataScopeType::UserPrivate)
        .await
        .unwrap()
        .shared_memory_handle;
    let receiver_id = add_receiver_isolate(&test_harness, DataScopeType::UserPrivate).await;

    let result = invoke_with_share_handles(
        &test_harness,
        receiver_id,
        vec![owned_handle, unowned_handle],
        vec![],
    )
    .await;

    assert!(result.is_err());
    assert_eq!(receiver_scope(&test_harness, receiver_id).await, DataScopeType::Public);
    assert!(!read_only_mounts.lock().unwrap().contains(&receiver_id));

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}

#[tokio::test]
async fn test_fileshare_with_unowned_handle_fails_before_escalating_receiver() {
    let mut test_harness = TestHarness::new().await;
    let read_only_mounts =
        spawn_fake_container_manager(test_harness.container_manager_request_rx.take().unwrap());
    let owned_handle = test_harness
        .fileshare_manager
        .create_fileshare(test_harness.echo_isolate_id, DataScopeType::UserPrivate)
        .await
        .unwrap();
    let other_owner_id = add_receiver_isolate(&test_harness, DataScopeType::UserPrivate).await;
    let unowned_handle = test_harness
        .fileshare_manager
        .create_fileshare(other_owner_id, DataScopeType::UserPrivate)
        .await
        .unwrap();
    let receiver_id = add_receiver_isolate(&test_harness, DataScopeType::UserPrivate).await;

    let result = invoke_with_share_handles(
        &test_harness,
        receiver_id,
        vec![],
        vec![owned_handle, unowned_handle],
    )
    .await;

    assert!(result.is_err());
    assert_eq!(receiver_scope(&test_harness, receiver_id).await, DataScopeType::Public);
    assert!(!read_only_mounts.lock().unwrap().contains(&receiver_id));

    let _ = test_harness.isolate_server_shutdown_tx.send(());
}
