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

use common_proto::enforcer::v2::IsolateType;
use container_manager_request::{
    ContainerManagerRequest, GetSetupIsolateClientResponse, LoadWorkloadIsolatesResponse,
    LoadWorkloadManifestsResponse,
};
use container_manager_requester::ContainerManagerRequester;
use data_scope::request::AddIsolateRequest;
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::IsolateState;
use ez_management_proto::enforcer::v2::ez_management_service_server::{
    EzManagementService, EzManagementServiceServer,
};
use ez_management_proto::enforcer::v2::{
    load_isolates_response, AllPackagesLoadedResponse, LoadIsolatesRequest, LoadIsolatesResponse,
    RatifiedIsolateManifestPayload,
};
use isolate_info::{register_isolate_type, BinaryServicesIndex, IsolateId};
use junction_test_utils::FakeJunction;
use node_bootstrap::{NodeBootstrap, NodeBootstrapConfig};
use opaque_isolate_manifest_proto::enforcer::v2::OpaqueIsolateManifest;
use ratified_isolate_manifest_proto::enforcer::v2::RatifiedIsolateManifest;
use setup_isolate_client::SetupIsolateClient;
use state_manager::IsolateStateManager;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const MAX_DECODING_SIZE: usize = 4 * 1024 * 1024;
const CHANNEL_SIZE: usize = 128;
const SETUP_PUBLISHER_ID: &str = "EZ_Trusted";
// Long enough for an erroneously ungated bootstrap to reach the service, short enough to keep
// the test fast.
const NOT_CONTACTED_WINDOW: Duration = Duration::from_millis(500);
const CONTACTED_TIMEOUT: Duration = Duration::from_secs(10);

// The Isolate type registry is process-global, so every harness registers a distinct Setup
// Isolate name to keep tests independent of one another.
static SETUP_ISOLATE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Minimal EzManagementService that reports when `LoadIsolates` is invoked and then replies
/// with two empty manifests followed by `AllPackagesLoaded`.
struct FakeManagementService {
    contacted_tx: mpsc::Sender<()>,
}

struct TestHarness {
    bootstrap: NodeBootstrap,
    state_manager: IsolateStateManager,
    setup_isolate_id: IsolateId,
    contacted_rx: Arc<Mutex<mpsc::Receiver<()>>>,
    _package_dir: tempfile::TempDir,
    _server_dir: tempfile::TempDir,
    _server_handle: tokio::task::JoinHandle<()>,
}

#[tokio::test]
async fn test_bootstrap_does_not_contact_management_service_until_setup_isolate_is_ready() {
    let harness = TestHarness::new().await;
    let bootstrap = harness.bootstrap.clone();
    let run_handle = tokio::spawn(async move { bootstrap.run().await });

    assert!(
        !harness.management_service_contacted_within(NOT_CONTACTED_WINDOW).await,
        "EzManagementService must not be contacted before the Setup Isolate is Ready"
    );
    assert!(!run_handle.is_finished(), "Bootstrap should still be waiting on the Setup Isolate");

    harness.mark_setup_isolate_ready().await;

    assert!(
        harness.management_service_contacted_within(CONTACTED_TIMEOUT).await,
        "EzManagementService must be contacted once the Setup Isolate is Ready"
    );

    timeout(CONTACTED_TIMEOUT, run_handle)
        .await
        .expect("Bootstrap should finish once the Setup Isolate is Ready")
        .expect("Bootstrap task should not panic")
        .expect("Bootstrap should succeed");
}

#[tokio::test]
async fn test_bootstrap_runs_when_setup_isolate_is_already_ready() {
    let harness = TestHarness::new().await;
    harness.mark_setup_isolate_ready().await;

    let loaded_indices = timeout(CONTACTED_TIMEOUT, harness.bootstrap.run())
        .await
        .expect("Bootstrap should not block when the Setup Isolate is already Ready")
        .expect("Bootstrap should succeed");

    assert!(loaded_indices.is_empty(), "No workload packages were served by the fake service");
}

#[tokio::test]
async fn test_bootstrap_fails_without_a_setup_isolate() {
    let harness = TestHarness::new().await;

    // Emulate a v1 boot, where the ContainerManager has no Setup Isolate to hand out.
    let (request_tx, mut request_rx) = mpsc::channel(CHANNEL_SIZE);
    tokio::spawn(async move {
        while let Some(request) = request_rx.recv().await {
            if let ContainerManagerRequest::GetSetupIsolateClient { resp } = request {
                let _ = resp.send(Ok(GetSetupIsolateClientResponse { client: None }));
            }
        }
    });

    let bootstrap = NodeBootstrap::new(
        harness.state_manager.clone(),
        ContainerManagerRequester::new(request_tx),
        NodeBootstrapConfig {
            ez_management_address: "unix:/nonexistent.sock".to_string(),
            max_decoding_message_size: MAX_DECODING_SIZE,
        },
    );

    let result = timeout(CONTACTED_TIMEOUT, bootstrap.run())
        .await
        .expect("Bootstrap should fail fast without a Setup Isolate");
    assert!(result.is_err(), "Bootstrap must not proceed without a Setup Isolate");
    assert!(
        !harness.management_service_contacted_within(NOT_CONTACTED_WINDOW).await,
        "EzManagementService must not be contacted without a Setup Isolate"
    );
}

#[tonic::async_trait]
impl EzManagementService for FakeManagementService {
    type LoadIsolatesStream =
        Pin<Box<dyn Stream<Item = Result<LoadIsolatesResponse, Status>> + Send + 'static>>;

    async fn load_isolates(
        &self,
        _request: Request<Streaming<LoadIsolatesRequest>>,
    ) -> Result<Response<Self::LoadIsolatesStream>, Status> {
        let _ = self.contacted_tx.send(()).await;

        let responses = vec![
            LoadIsolatesResponse {
                response: Some(load_isolates_response::Response::RatifiedIsolateManifest(
                    RatifiedIsolateManifestPayload {
                        manifest: Some(RatifiedIsolateManifest::default()),
                        ..Default::default()
                    },
                )),
            },
            LoadIsolatesResponse {
                response: Some(load_isolates_response::Response::OpaqueIsolateManifest(
                    OpaqueIsolateManifest::default(),
                )),
            },
            LoadIsolatesResponse {
                response: Some(load_isolates_response::Response::AllPackagesLoaded(
                    AllPackagesLoadedResponse {},
                )),
            },
        ];

        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        tokio::spawn(async move {
            for response in responses {
                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

impl TestHarness {
    async fn new() -> Self {
        let package_dir = tempfile::tempdir().expect("package dir");
        std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", package_dir.path());

        let server_dir = tempfile::tempdir().expect("server dir");
        let uds_path = server_dir.path().join("ez_management.sock");
        let uds_stream =
            UnixListenerStream::new(UnixListener::bind(&uds_path).expect("bind management UDS"));
        let (contacted_tx, contacted_rx) = mpsc::channel(CHANNEL_SIZE);
        let server_handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(EzManagementServiceServer::new(FakeManagementService { contacted_tx }))
                .serve_with_incoming(uds_stream)
                .await;
        });

        // Register the Setup Isolate's services the way the ContainerManager would, so that the
        // SetupIsolateClient can resolve its own BinaryServicesIndex.
        let isolate_name =
            format!("setup_isolate_{}", SETUP_ISOLATE_SEQ.fetch_add(1, Ordering::SeqCst));
        let setup_isolate_index = BinaryServicesIndex::new(/*is_ratified_binary=*/ true);
        register_isolate_type(
            setup_isolate_index,
            IsolateType {
                publisher_id: SETUP_PUBLISHER_ID.to_string(),
                isolate_name: isolate_name.clone(),
            },
        );
        let setup_isolate_client = Arc::new(SetupIsolateClient::new(
            Box::new(FakeJunction::default()),
            SETUP_PUBLISHER_ID.to_string(),
            isolate_name,
            "SetupService".to_string(),
        ));

        let (request_tx, request_rx) = mpsc::channel(CHANNEL_SIZE);
        let container_manager_requester = ContainerManagerRequester::new(request_tx);
        spawn_fake_container_manager(request_rx, setup_isolate_client);

        let state_manager = IsolateStateManager::new(
            DataScopeRequester::new(u64::MAX),
            container_manager_requester.clone(),
        );

        // Register the Setup Isolate as booted but not yet Ready.
        let setup_isolate_id = IsolateId::new(setup_isolate_index);
        state_manager
            .add_isolate(AddIsolateRequest {
                isolate_id: setup_isolate_id,
                current_data_scope_type: DataScopeType::Public,
                allowed_data_scope_type: DataScopeType::Public,
            })
            .await;
        state_manager.mark_channel_connected(setup_isolate_id).await.expect("channel connected");

        let bootstrap = NodeBootstrap::new(
            state_manager.clone(),
            container_manager_requester,
            NodeBootstrapConfig {
                ez_management_address: format!("unix:{}", uds_path.display()),
                max_decoding_message_size: MAX_DECODING_SIZE,
            },
        );

        Self {
            bootstrap,
            state_manager,
            setup_isolate_id,
            contacted_rx: Arc::new(Mutex::new(contacted_rx)),
            _package_dir: package_dir,
            _server_dir: server_dir,
            _server_handle: server_handle,
        }
    }

    async fn mark_setup_isolate_ready(&self) {
        self.state_manager
            .update_state(self.setup_isolate_id, IsolateState::Ready)
            .await
            .expect("Setup Isolate should transition to Ready");
    }

    /// Waits up to `duration` for the EzManagementService to be contacted.
    async fn management_service_contacted_within(&self, duration: Duration) -> bool {
        let contacted_rx = self.contacted_rx.clone();
        timeout(duration, async move { contacted_rx.lock().await.recv().await }).await.is_ok()
    }
}

/// Serves the ContainerManager requests the bootstrap sequence issues, so that it can run
/// without a real ContainerManager.
fn spawn_fake_container_manager(
    mut request_rx: mpsc::Receiver<ContainerManagerRequest>,
    setup_isolate_client: Arc<SetupIsolateClient>,
) {
    tokio::spawn(async move {
        while let Some(request) = request_rx.recv().await {
            match request {
                ContainerManagerRequest::GetSetupIsolateClient { resp } => {
                    let _ = resp.send(Ok(GetSetupIsolateClientResponse {
                        client: Some(setup_isolate_client.clone()),
                    }));
                }
                ContainerManagerRequest::LoadWorkloadManifests { resp, .. } => {
                    let _ =
                        resp.send(Ok(LoadWorkloadManifestsResponse { registered_indices: vec![] }));
                }
                ContainerManagerRequest::LoadWorkloadIsolates { resp, .. } => {
                    let _ = resp.send(Ok(LoadWorkloadIsolatesResponse { loaded_indices: vec![] }));
                }
                _ => {}
            }
        }
    });
}
