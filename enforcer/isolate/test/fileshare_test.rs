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

use anyhow::Result;
use container_manager_requester::ContainerManagerRequester;

use container_manager_request::ContainerManagerRequest;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::requester::DataScopeRequester;
use enforcer_proto::enforcer::v1::{
    fileshare_event, isolate_ez_bridge_client::IsolateEzBridgeClient,
    isolate_ez_bridge_server::IsolateEzBridgeServer,
    publish_event_for_request::Payload as PublishPayload,
    stream_subscribe_to_response::Payload as StreamPayload, EventTopic, FileshareEvent,
    PublishEventForRequest, StreamSubscribeToRequest,
};
use fileshare_manager::FileshareManager;
use hyper_util::rt::tokio::TokioIo;
use isolate_ez_service::{IsolateEzBridgeDependencies, IsolateEzBridgeService};
use isolate_info::{BinaryServicesIndex, IsolateId};
use isolate_service_mapper::IsolateServiceMapper;
use junction_test_utils::FakeJunction;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};
use tonic::transport::{Channel, Endpoint, Server};
use tower::service_fn;

#[derive(Debug)]
struct TestHarness {
    isolate_id: IsolateId,
    client: IsolateEzBridgeClient<Channel>,
}

impl TestHarness {
    async fn new_with_shared_components(
        isolate_id: IsolateId,
        fileshare_manager: FileshareManager,
    ) -> Result<Self> {
        let mock_junction = FakeJunction::default();
        let service_mapper = IsolateServiceMapper::default();

        let (tx, _container_manager_rx) = mpsc::channel(1);
        let container_manager_requester = ContainerManagerRequester::new(tx);
        let data_scope_requester = DataScopeRequester::new(0);

        let manifest_validator = ManifestValidator::default();
        let isolate_state_manager = IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        );
        let shared_memory_manager = SharedMemManager::new(container_manager_requester);
        let interceptor_instance = interceptor::Interceptor::new(service_mapper.clone());

        let deps = IsolateEzBridgeDependencies {
            isolate_id,
            isolate_junction: Box::new(mock_junction),
            isolate_state_manager,
            shared_memory_manager,
            fileshare_manager,
            external_proxy_connector: None,
            isolate_service_mapper: service_mapper,
            data_scope_requester,
            manifest_validator,
            ez_to_ez_outbound_handler: None,
            interceptor: interceptor_instance,
        };
        let isolate_ez_bridge_service = IsolateEzBridgeService::new(deps);
        let client = spawn_test_server(isolate_ez_bridge_service).await;

        Ok(Self { isolate_id, client })
    }
}

async fn spawn_test_server(service: IsolateEzBridgeService) -> IsolateEzBridgeClient<Channel> {
    let (client_io, server_io) = tokio::io::duplex(1024);

    tokio::spawn(async move {
        Server::builder()
            .add_service(IsolateEzBridgeServer::new(service))
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await
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
        .expect("Should connect client")
}

#[tokio::test]
async fn test_fileshare_event_propagation() {
    // 1. Setup Sender and Receiver Harnesses
    let (container_manager_tx, mut container_manager_rx) = mpsc::channel(1);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_tx);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());

    // Spawn a mock ContainerManager to handle requests
    tokio::spawn(async move {
        while let Some(req) = container_manager_rx.recv().await {
            match req {
                ContainerManagerRequest::MountWritableDirectory { req: _, resp } => {
                    let _ = resp.send(Ok(container_manager_request::MountDirectoryResponse {}));
                }
                ContainerManagerRequest::MountReadOnlyDirectory { req: _, resp } => {
                    let _ = resp.send(Ok(container_manager_request::MountDirectoryResponse {}));
                }
                _ => {
                    eprintln!("Unexpected ContainerManager request");
                }
            }
        }
    });

    // Receiver
    let mut receiver_harness = TestHarness::new_with_shared_components(
        IsolateId::new(BinaryServicesIndex::new(false)),
        fileshare_manager.clone(),
    )
    .await
    .expect("Receiver harness");

    // Sender
    let mut sender_harness = TestHarness::new_with_shared_components(
        IsolateId::new(BinaryServicesIndex::new(false)),
        fileshare_manager.clone(),
    )
    .await
    .expect("Sender harness");

    // 2. Register Receiver Stream
    let request = StreamSubscribeToRequest { topic: EventTopic::Fileshare.into() };
    let mut response_stream = receiver_harness
        .client
        .stream_subscribe_to(request)
        .await
        .expect("Stream setup")
        .into_inner();

    // Wait for stream to be registered? It happens in the handler. To be sure, we can sleep a bit.
    sleep(Duration::from_millis(50)).await;

    // 3. Establish Fileshare
    let receiver_id = receiver_harness.isolate_id;
    let sender_id = sender_harness.isolate_id;

    let fileshare_handle = fileshare_manager
        .create_fileshare(sender_id)
        .await
        .expect("Create fileshare should succeed");

    fileshare_manager
        .share_file(&fileshare_handle, sender_id, receiver_id)
        .await
        .expect("Share file manually");

    // 4. Sender Notifies Event
    let request = PublishEventForRequest {
        topic: EventTopic::Fileshare.into(),
        handle: fileshare_handle.clone(),
        payload: Some(PublishPayload::FileshareEvent(FileshareEvent {
            event_type: fileshare_event::FileshareEventType::FileUpdated.into(),
        })),
    };
    sender_harness.client.publish_event_for(request).await.expect("Notify success");

    // 5. Verify Receiver gets it
    let event = timeout(Duration::from_secs(2), response_stream.message())
        .await
        .expect("Timeout waiting for event")
        .expect("Stream error")
        .expect("Stream closed");

    assert_eq!(event.handle, fileshare_handle);
    let payload = event.payload.expect("Missing payload");
    let StreamPayload::FileshareEvent(fileshare_event) = payload;
    assert_eq!(fileshare_event.event_type, fileshare_event::FileshareEventType::FileUpdated as i32);
}
