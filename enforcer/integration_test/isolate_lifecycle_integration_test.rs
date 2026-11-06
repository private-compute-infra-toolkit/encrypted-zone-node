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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use container_manager_request::{
    ContainerManagerRequest, MountDirectoryResponse, MountFileResponse,
};
use data_scope::request::{
    AddBackendDependenciesRequest, AddIsolateRequest, GetIsolateRequest, GetIsolateScopeRequest,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    isolate_ez_bridge_client::IsolateEzBridgeClient,
    isolate_ez_bridge_server::IsolateEzBridgeServer, ControlPlaneMetadata, EzPayloadIsolateScope,
    InvokeEzRequest, InvokeEzResponse, IsolateDataScope, IsolateState, NotifyIsolateStateRequest,
    NotifyIsolateStateResponse,
};
use hyper_util::rt::tokio::TokioIo;
use interceptor::Interceptor;
use isolate_ez_service::{IsolateEzBridgeDependencies, IsolateEzBridgeService};
use isolate_info::{BinaryServicesIndex, IsolateId};
use isolate_test_utils::{
    ScopeDragInstruction, ECHO_ISOLATE_METHOD_NAME, ECHO_ISOLATE_OPERATOR_DOMAIN,
    ECHO_ISOLATE_SERVICE_NAME,
};
use junction_test_utils::TestHarness as JunctionTestHarness;
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Server};
use tower::service_fn;

const CHANNEL_SIZE: usize = 16;
const REGION_SIZE: i64 = 1024;
const SENSITIVE_SESSION_THRESHOLD: u64 = 1;
const SHM_PAYLOAD_LARGE_THRESHOLD: u64 = 100 * 1024 * 1024;
const STREAM_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_PAYLOAD: &[u8] = b"hello world";

/// End-to-end integration harness using real Enforcer components, except the
/// `ContainerManager` or real containers.
struct IntegrationTestHarness {
    inner: JunctionTestHarness,
    read_only_mounts: Arc<Mutex<Vec<IsolateId>>>,
}

impl IntegrationTestHarness {
    async fn new(retirement_threshold: u64, scope_drag_instruction: ScopeDragInstruction) -> Self {
        let mut inner = JunctionTestHarness::new_with_arguments(
            retirement_threshold,
            scope_drag_instruction,
            /* is_ratified= */ false,
            DataScopeType::UserPrivate,
            None,
        )
        .await;
        let read_only_mounts =
            spawn_fake_container_manager(inner.container_manager_request_rx.take().unwrap());
        Self { inner, read_only_mounts }
    }

    /// Registers a new Opaque Isolate in `IsolateState::Starting` (without advancing it to
    /// `Ready`), wires its manifest dependency on the backend service, and spawns a real
    /// `IsolateEzBridgeService` connected to the real `IsolateJunction`.
    async fn spawn_starting_opaque_isolate_bridge(
        &self,
        initial_scope: DataScopeType,
    ) -> (IsolateId, IsolateEzBridgeClient<Channel>) {
        let caller_binary_index = BinaryServicesIndex::new(/* is_ratified_binary= */ false);
        let isolate_id = IsolateId::new(caller_binary_index);
        self.inner
            .isolate_state_manager
            .add_isolate(AddIsolateRequest {
                current_data_scope_type: initial_scope,
                allowed_data_scope_type: DataScopeType::UserPrivate,
                isolate_id,
            })
            .await;

        let echo_service_info =
            self.inner.isolate_service_info_map.get(ECHO_ISOLATE_SERVICE_NAME).unwrap();
        let dependency_index = self
            .inner
            .isolate_service_mapper
            .get_service_index(echo_service_info)
            .await
            .expect("Backend service should be indexed in IsolateServiceMapper");
        self.inner
            .manifest_validator
            .add_backend_dependencies(AddBackendDependenciesRequest {
                binary_services_index: caller_binary_index,
                dependency_index,
            })
            .await
            .expect("Backend dependency should be added to ManifestValidator");

        let service = IsolateEzBridgeService::new(IsolateEzBridgeDependencies {
            isolate_id,
            isolate_junction: Box::new(self.inner.isolate_junction.clone()),
            isolate_state_manager: self.inner.isolate_state_manager.clone(),
            shared_memory_manager: self.inner.shared_mem_manager.clone(),
            fileshare_manager: self.inner.fileshare_manager.clone(),
            external_proxy_connector: None,
            isolate_service_mapper: self.inner.isolate_service_mapper.clone(),
            data_scope_requester: self.inner.data_scope_requester.clone(),
            manifest_validator: self.inner.manifest_validator.clone(),
            ez_to_ez_outbound_handler: None,
            interceptor: Interceptor::new(self.inner.isolate_service_mapper.clone()),
            shm_payload_threshold: SHM_PAYLOAD_LARGE_THRESHOLD,
        });
        let client = spawn_bridge_client(service).await;
        (isolate_id, client)
    }

    /// Marks the Junction channel connected and sends `NotifyIsolateState(Ready)` from the Isolate
    /// over `IsolateEzBridgeService`.
    async fn notify_isolate_ready(
        &self,
        isolate_id: IsolateId,
        client: &mut IsolateEzBridgeClient<Channel>,
    ) -> Result<NotifyIsolateStateResponse, tonic::Status> {
        self.inner
            .isolate_state_manager
            .mark_channel_connected(isolate_id)
            .await
            .expect("mark_channel_connected should succeed");
        let (notify_tx, notify_rx) = mpsc::channel(CHANNEL_SIZE);
        notify_tx
            .send(NotifyIsolateStateRequest { new_isolate_state: IsolateState::Ready.into() })
            .await
            .expect("Should send NotifyIsolateStateRequest");
        let mut notify_stream = client
            .notify_isolate_state(ReceiverStream::new(notify_rx))
            .await
            .expect("notify_isolate_state RPC should start")
            .into_inner();
        timeout(STREAM_TIMEOUT, notify_stream.message())
            .await
            .expect("Timed out waiting for NotifyIsolateStateResponse")?
            .ok_or_else(|| tonic::Status::internal("NotifyIsolateState stream closed unexpectedly"))
    }

    fn shutdown(self) {
        let _ = self.inner.isolate_server_shutdown_tx.send(());
    }
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

async fn spawn_bridge_client(service: IsolateEzBridgeService) -> IsolateEzBridgeClient<Channel> {
    let (client_io, server_io) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(IsolateEzBridgeServer::new(service))
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await;
    });

    let mut client_conn = Some(client_io);
    Endpoint::try_from("http://in-memory.test")
        .expect("Endpoint should be valid")
        .connect_with_connector(service_fn(move |_| {
            let client = client_conn.take();
            async move {
                if let Some(client) = client {
                    Ok(TokioIo::new(client))
                } else {
                    Err(std::io::Error::other("Client already taken"))
                }
            }
        }))
        .await
        .map(IsolateEzBridgeClient::new)
        .expect("Should connect IsolateEzBridgeClient")
}

fn create_echo_request(
    scope: DataScopeType,
    shared_memory_handles: Vec<String>,
) -> InvokeEzRequest {
    InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            ipc_message_id: 1,
            destination_operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
            destination_service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
            destination_method_name: ECHO_ISOLATE_METHOD_NAME.to_string(),
            responder_is_local: true,
            shared_memory_handles,
            ..Default::default()
        }),
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: scope.into(),
                ..Default::default()
            }],
        }),
        isolate_request_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![TEST_PAYLOAD.to_vec()],
            })),
        }),
    }
}

fn extract_inline_payload(response: &InvokeEzResponse) -> &[u8] {
    match response
        .ez_response_payload
        .as_ref()
        .expect("Response should carry payload")
        .delivery_method
        .as_ref()
        .expect("Payload should specify delivery method")
    {
        DeliveryMethod::InlineData(data) => &data.datagrams[0],
        other => panic!("Expected InlineData delivery method, got {other:?}"),
    }
}

/// Verifies end-to-end across `IsolateEzBridgeService`, `IsolateJunction`, `DataScopeManager`,
/// and `IsolateStateManager` that an Opaque Isolate in `Starting` state can initiate a unary
/// (`invoke_ez`) RPC and receive a `UserPrivate` inline payload response back.
#[tokio::test]
async fn test_starting_opaque_isolate_receives_user_private_unary_response() {
    // Configure the integration harness with SENSITIVE_SESSION_THRESHOLD = 1 (> 0, since 0
    // disables sensitive session retirement).
    let harness = IntegrationTestHarness::new(
        SENSITIVE_SESSION_THRESHOLD,
        ScopeDragInstruction::IncreaseByOne,
    )
    .await;

    // Spawn a bridge for an Opaque Isolate that remains in Starting state.
    let (isolate_id, mut unary_client) =
        harness.spawn_starting_opaque_isolate_bridge(DataScopeType::DomainOwned).await;
    assert!(!isolate_id.is_ratified_isolate());
    assert_eq!(
        harness.inner.isolate_state_manager.get_isolate_state(isolate_id),
        Some(IsolateState::Starting)
    );
    let initial_unary_scope_info = harness
        .inner
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id })
        .await
        .expect("Unary caller scope should be registered in DataScopeManager");
    assert_eq!(initial_unary_scope_info.current_scope, DataScopeType::DomainOwned);
    assert_eq!(initial_unary_scope_info.sensitive_session_count, Some(0));

    // Initiate a unary InvokeEz call from the Opaque Isolate while it is in Starting state;
    // the backend increases the response scope by one level from DomainOwned to UserPrivate.
    let unary_response = unary_client
        .invoke_ez(create_echo_request(DataScopeType::DomainOwned, vec![]))
        .await
        .expect("Unary invoke_ez from Starting Opaque Isolate should succeed")
        .into_inner();

    // Verify that the requester Starting Opaque Isolate receives both the UserPrivate scope
    // and the echoed response payload bytes.
    assert_eq!(
        unary_response.ez_response_iscope.as_ref().unwrap().datagram_iscopes[0].scope_type,
        i32::from(DataScopeType::UserPrivate)
    );
    assert_eq!(extract_inline_payload(&unary_response), TEST_PAYLOAD);

    // Verify the unary response escalated the Starting Opaque Isolate's scope to UserPrivate.
    let unary_scope_info = harness
        .inner
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id })
        .await
        .expect("Unary caller scope should be registered in DataScopeManager");
    assert_eq!(unary_scope_info.current_scope, DataScopeType::UserPrivate);

    // TODO(b/484381850): With SENSITIVE_SESSION_THRESHOLD = 1 (> 0), current behavior fails to
    // increment sensitive_session_count or retire a Starting Opaque Isolate when validate_isolate
    // escalates an inline RPC response to UserPrivate; revert these assertions to expect Some(1)
    // and Some(IsolateState::Idle) after fixing b/484381850.
    assert_eq!(unary_scope_info.sensitive_session_count, Some(0));
    assert_eq!(
        harness.inner.isolate_state_manager.get_isolate_state(isolate_id),
        Some(IsolateState::Starting)
    );

    // TODO(b/484381850): With SENSITIVE_SESSION_THRESHOLD = 1 (> 0), when the Isolate subsequently
    // reports Ready via IsolateEzBridgeService, IsolateStateManager promotes isolate_id to Ready
    // and activates it in the inbound routing pool. Once b/484381850 is fixed, isolate_id will
    // transition Starting -> Retiring -> Idle (FastStateReset) when the unary call finishes, so
    // notify_isolate_ready will fail and get_isolate will return Err(NoMatchingIsolates).
    harness.notify_isolate_ready(isolate_id, &mut unary_client).await.expect(
        "Starting isolate reporting Ready via IsolateEzService succeeds before b/484381850 fix",
    );
    assert_eq!(
        harness.inner.isolate_state_manager.get_isolate_state(isolate_id),
        Some(IsolateState::Ready)
    );
    let get_unary_isolate_result = harness
        .inner
        .data_scope_requester
        .get_isolate(GetIsolateRequest {
            binary_services_index: isolate_id.get_binary_services_index(),
            data_scope_type: DataScopeType::UserPrivate,
        })
        .await
        .map(|response| response.isolate_id)
        .map_err(|err| err.to_string());
    assert_eq!(get_unary_isolate_result, Ok(isolate_id));

    harness.shutdown();
}

/// Verifies end-to-end across `IsolateEzBridgeService`, `IsolateJunction`, `SharedMemManager`,
/// `DataScopeManager`, and `IsolateStateManager` that a `Starting` Opaque Isolate can receive a
/// `UserPrivate` streaming (`stream_invoke_ez`) response with an attached shared-memory handle.
/// Because streaming responses do not increment `sensitive_session_count`, attaching the
/// `UserPrivate` shared-memory handle exercises `validate_data_transfer` and `Starting -> Retiring`
/// in the same flow.
#[tokio::test]
async fn test_starting_opaque_isolate_receives_user_private_streaming_and_memshare_response() {
    // Configure the integration harness with SENSITIVE_SESSION_THRESHOLD = 1 (> 0, since 0
    // disables sensitive session retirement).
    let harness = IntegrationTestHarness::new(
        SENSITIVE_SESSION_THRESHOLD,
        ScopeDragInstruction::IncreaseByOne,
    )
    .await;

    // Create a UserPrivate shared memory handle owned by the backend isolate so the response
    // exercises IsolateJunction::handle_shared_memory and SharedMemManager::share_file.
    let memshare_handle = harness
        .inner
        .shared_mem_manager
        .create_shared_mem_file(
            harness.inner.echo_isolate_id,
            REGION_SIZE,
            DataScopeType::UserPrivate,
        )
        .await
        .expect("Backend isolate should create UserPrivate shared memory file")
        .shared_memory_handle;

    // Spawn a bridge for an Opaque Isolate that remains in Starting state to exercise streaming
    // RPC and shared-memory transfer together end to end.
    let (isolate_id, mut stream_client) =
        harness.spawn_starting_opaque_isolate_bridge(DataScopeType::DomainOwned).await;
    assert!(!isolate_id.is_ratified_isolate());
    assert_eq!(
        harness.inner.isolate_state_manager.get_isolate_state(isolate_id),
        Some(IsolateState::Starting)
    );
    let initial_stream_scope_info = harness
        .inner
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id })
        .await
        .expect("Streaming caller scope should be registered in DataScopeManager");
    assert_eq!(initial_stream_scope_info.current_scope, DataScopeType::DomainOwned);
    assert_eq!(initial_stream_scope_info.sensitive_session_count, Some(0));

    // Initiate a streaming StreamInvokeEz call from the Starting Opaque Isolate carrying the
    // backend's UserPrivate shared memory handle. Keep `req_tx` open so the stream remains active
    // while inspecting post-response state.
    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
    req_tx
        .send(create_echo_request(DataScopeType::DomainOwned, vec![memshare_handle.clone()]))
        .await
        .expect("Should send streaming request");

    let mut response_stream = stream_client
        .stream_invoke_ez(ReceiverStream::new(req_rx))
        .await
        .expect("Streaming stream_invoke_ez from Starting Opaque Isolate should succeed")
        .into_inner();

    // Verify that the requester Starting Opaque Isolate receives the UserPrivate streamed response
    // payload, shared memory handle metadata, and read-only shared memory mount in its container.
    let stream_response = timeout(STREAM_TIMEOUT, response_stream.message())
        .await
        .expect("Timed out waiting for streaming response")
        .expect("Streaming response should not error")
        .expect("Streaming response should contain a message");
    assert_eq!(
        stream_response.ez_response_iscope.as_ref().unwrap().datagram_iscopes[0].scope_type,
        i32::from(DataScopeType::UserPrivate)
    );
    assert_eq!(extract_inline_payload(&stream_response), TEST_PAYLOAD);
    assert_eq!(
        stream_response.control_plane_metadata.as_ref().unwrap().shared_memory_handles,
        vec![memshare_handle]
    );
    assert!(harness.read_only_mounts.lock().unwrap().contains(&isolate_id));

    // Verify the UserPrivate shared memory transfer escalated the Starting Opaque Isolate's scope
    // to UserPrivate and incremented sensitive_session_count to 1 via validate_data_transfer.
    let stream_scope_info = harness
        .inner
        .data_scope_requester
        .get_isolate_scope(GetIsolateScopeRequest { isolate_id })
        .await
        .expect("Streaming caller scope should be registered in DataScopeManager");
    assert_eq!(stream_scope_info.current_scope, DataScopeType::UserPrivate);
    assert_eq!(stream_scope_info.sensitive_session_count, Some(1));

    // TODO(b/484381850): Current behavior rejects the (IsolateState::Starting, IsolateState::Retiring)
    // state transition in IsolateStateManager::validate_state_transition when IsolateJunction attempts
    // to retire the Starting caller; revert this assertion to expect Some(IsolateState::Retiring)
    // after fixing b/484381850.
    assert_eq!(
        harness.inner.isolate_state_manager.get_isolate_state(isolate_id),
        Some(IsolateState::Starting)
    );

    // TODO(b/484381850): Because IsolateStateManager rejected the Starting -> Retiring transition
    // above, isolate_id remained in Starting state, so reporting Ready from the Isolate via
    // IsolateEzBridgeService promotes it to Ready (not Retiring) and activates it in the inbound
    // routing pool. Once b/484381850 is fixed in IsolateStateManager::validate_state_transition,
    // isolate_id will be in Retiring state so notify_isolate_ready will fail and never
    // activate it.
    harness.notify_isolate_ready(isolate_id, &mut stream_client).await.expect(
        "Starting isolate reporting Ready via IsolateEzService succeeds before b/484381850 fix",
    );
    assert_eq!(
        harness.inner.isolate_state_manager.get_isolate_state(isolate_id),
        Some(IsolateState::Ready)
    );
    let get_retired_isolate_result = harness
        .inner
        .data_scope_requester
        .get_isolate(GetIsolateRequest {
            binary_services_index: isolate_id.get_binary_services_index(),
            data_scope_type: DataScopeType::UserPrivate,
        })
        .await
        .map(|response| response.isolate_id)
        .map_err(|err| err.to_string());
    assert_eq!(get_retired_isolate_result, Ok(isolate_id));

    drop(req_tx);
    harness.shutdown();
}
