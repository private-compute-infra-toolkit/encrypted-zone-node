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
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, InvokeEzRequest, InvokeEzResponse, InvokeIsolateRequest,
    InvokeIsolateResponse, IsolateState,
};
use ez_error::EzError;
use ez_management_proto::enforcer::v2::ez_management_service_server::{
    EzManagementService, EzManagementServiceServer,
};
use ez_management_proto::enforcer::v2::{
    load_isolates_response, AllPackagesLoadedResponse, FetchIsolateStartupParametersRequest,
    FetchIsolateStartupParametersResponse, FetchOperatorInfoRequest, FetchOperatorInfoResponse,
    LoadIsolatesRequest, LoadIsolatesResponse, OperatorInfo, RatifiedIsolateManifestPayload,
};
use inbound_ez_to_ez_handler::InboundEzToEzHandler;
use isolate_info::{register_isolate_type, BinaryServicesIndex, IsolateId};
use junction_test_utils::FakeJunction;
use junction_trait::Junction;
use node_bootstrap::{NodeBootstrapV2, NodeBootstrapV2Config};
use opaque_isolate_manifest_proto::enforcer::v2::OpaqueIsolateManifest;
use outbound_ez_to_ez_client::{OutboundEzToEzClient, OutboundTlsConfig};
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use prost::Message;
use ratified_isolate_manifest_proto::enforcer::v2::RatifiedIsolateManifest;
use setup_isolate_client::SetupIsolateClient;
use setup_isolate_proto::enforcer::v2::FetchTlsCertificateResponse;
use state_manager::IsolateStateManager;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;
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
const OPERATOR_DOMAIN: &str = "operator.example.com";
const OPERATOR_ROLE: &str = "PRIMARY";
// Long enough for an erroneously ungated bootstrap to reach the service, short enough to keep
// the test fast.
const NOT_CONTACTED_WINDOW: Duration = Duration::from_millis(500);
const CONTACTED_TIMEOUT: Duration = Duration::from_secs(10);

// The Isolate type registry is process-global, so every harness registers a distinct Setup
// Isolate name to keep tests independent of one another.
static SETUP_ISOLATE_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Default)]
struct FakeOutboundEzToEzClient {
    tls_config: Arc<StdMutex<Option<OutboundTlsConfig>>>,
}

#[tonic::async_trait]
impl OutboundEzToEzClient for FakeOutboundEzToEzClient {
    async fn remote_invoke(
        &self,
        _request: InvokeEzRequest,
        _deadline: Option<Instant>,
    ) -> anyhow::Result<InvokeEzResponse> {
        unimplemented!()
    }

    async fn remote_streaming_connect(
        &self,
        _first_request_metadata: Option<&ControlPlaneMetadata>,
        _from_local_rx: mpsc::Receiver<InvokeEzRequest>,
        _timeout: Option<Duration>,
    ) -> anyhow::Result<mpsc::Receiver<anyhow::Result<InvokeEzResponse>>> {
        unimplemented!()
    }

    fn set_tls_config(&self, tls_config: OutboundTlsConfig) -> anyhow::Result<()> {
        *self.tls_config.lock().unwrap() = Some(tls_config);
        Ok(())
    }
}

#[derive(Clone)]
struct FakeSetupJunction {
    response: Result<InvokeIsolateResponse, String>,
}

#[tonic::async_trait]
impl Junction for FakeSetupJunction {
    async fn invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        _invoke_isolate_request: InvokeIsolateRequest,
        _is_from_public_api: bool,
        _deadline: Option<Instant>,
    ) -> Result<InvokeIsolateResponse, EzError> {
        match &self.response {
            Ok(res) => Ok(res.clone()),
            Err(msg) => Err(EzError::Status(Status::internal(msg.clone()))),
        }
    }

    async fn stream_invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        _is_from_public_api: bool,
        _timeout: Option<Duration>,
    ) -> junction_trait::JunctionChannels {
        unimplemented!()
    }

    async fn connect_isolate(
        &self,
        _isolate_id: IsolateId,
        _isolate_address: String,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }
}

/// Minimal EzManagementService that serves a fixed `OperatorInfo`, reports when `LoadIsolates` is
/// invoked, and then replies with two empty manifests followed by `AllPackagesLoaded`.
struct FakeManagementService {
    contacted_tx: mpsc::Sender<()>,
}

struct TestHarness {
    bootstrap: NodeBootstrapV2,
    state_manager: IsolateStateManager,
    container_manager_requester: ContainerManagerRequester,
    ez_management_address: String,
    setup_isolate_id: IsolateId,
    contacted_rx: Arc<Mutex<mpsc::Receiver<()>>>,
    _package_dir: tempfile::TempDir,
    server_dir: tempfile::TempDir,
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

    timeout(CONTACTED_TIMEOUT, harness.bootstrap.run())
        .await
        .expect("Bootstrap should not block when the Setup Isolate is already Ready")
        .expect("Bootstrap should succeed");
}

#[tokio::test]
async fn test_fetch_operator_info_before_setup_isolate_is_ready() {
    let harness = TestHarness::new().await;

    let operator_info = timeout(CONTACTED_TIMEOUT, harness.bootstrap.fetch_operator_info())
        .await
        .expect("fetch_operator_info must not wait for the Setup Isolate")
        .expect("fetch_operator_info should succeed");

    assert_eq!(operator_info.operator_domain, OPERATOR_DOMAIN);
    assert_eq!(operator_info.operator_role, OPERATOR_ROLE);
    assert!(
        !harness.management_service_contacted_within(NOT_CONTACTED_WINDOW).await,
        "LoadIsolates must not start before the Setup Isolate is Ready"
    );
}

#[tokio::test]
async fn test_bootstrap_launches_inbound_ez_to_ez_server() {
    let harness = TestHarness::new().await;
    harness.mark_setup_isolate_ready().await;

    let inbound_uds_path = harness.server_dir.path().join("inbound_ez_to_ez.sock");
    let inbound_address = format!("unix:{}", inbound_uds_path.display());
    let inbound_handler = InboundEzToEzHandler::new(Box::new(FakeJunction::default()));

    let bootstrap = NodeBootstrapV2::new(
        harness.state_manager.clone(),
        harness.container_manager_requester.clone(),
        None,
        Some((inbound_handler, inbound_address)),
        NodeBootstrapV2Config {
            ez_management_address: harness.ez_management_address.clone(),
            max_decoding_message_size: MAX_DECODING_SIZE,
            enable_mtls: false,
            ez_to_ez_handshake_timeout: Duration::from_secs(5),
            ez_to_ez_max_concurrent_handshakes: 10,
        },
    )
    .await
    .expect("NodeBootstrapV2 should connect to the EzManagementService");

    timeout(CONTACTED_TIMEOUT, bootstrap.run())
        .await
        .expect("Bootstrap should not block")
        .expect("Bootstrap should succeed");

    timeout(CONTACTED_TIMEOUT, async {
        while !inbound_uds_path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Inbound EZ-to-EZ UDS socket should be created");
}

#[tokio::test]
async fn test_bootstrap_with_mtls_configures_outbound_tls() {
    let harness = TestHarness::new().await;
    let fake_outbound = FakeOutboundEzToEzClient::default();
    let bootstrap = NodeBootstrapV2::new(
        harness.state_manager.clone(),
        harness.container_manager_requester.clone(),
        Some(Box::new(fake_outbound.clone())),
        None,
        NodeBootstrapV2Config {
            ez_management_address: harness.ez_management_address.clone(),
            max_decoding_message_size: MAX_DECODING_SIZE,
            enable_mtls: true,
            ez_to_ez_handshake_timeout: Duration::from_secs(5),
            ez_to_ez_max_concurrent_handshakes: 10,
        },
    )
    .await
    .expect("NodeBootstrapV2 should connect to the EzManagementService");
    let run_handle = tokio::spawn(async move { bootstrap.run().await });

    assert!(
        !harness.management_service_contacted_within(NOT_CONTACTED_WINDOW).await,
        "EzManagementService must not be contacted before the Setup Isolate is Ready"
    );
    assert!(!run_handle.is_finished(), "Bootstrap should still be waiting on the Setup Isolate");

    harness.mark_setup_isolate_ready().await;

    timeout(CONTACTED_TIMEOUT, run_handle)
        .await
        .expect("Bootstrap should finish once the Setup Isolate is Ready")
        .expect("Bootstrap task should not panic")
        .expect("Bootstrap with mTLS should succeed");

    let configured_outbound = fake_outbound
        .tls_config
        .lock()
        .unwrap()
        .clone()
        .expect("set_tls_config should be called on outbound handler");
    assert_eq!(configured_outbound.trust_domain, "avs.tca.fakeca");
}

#[tokio::test]
async fn test_bootstrap_does_not_contact_management_service_when_mtls_fetch_fails() {
    let harness =
        TestHarness::new_with_setup_response(Err("setup isolate cert fetch failed".to_string()))
            .await;
    harness.mark_setup_isolate_ready().await;

    let bootstrap = NodeBootstrapV2::new(
        harness.state_manager.clone(),
        harness.container_manager_requester.clone(),
        None,
        None,
        NodeBootstrapV2Config {
            ez_management_address: harness.ez_management_address.clone(),
            max_decoding_message_size: MAX_DECODING_SIZE,
            enable_mtls: true,
            ez_to_ez_handshake_timeout: Duration::from_secs(5),
            ez_to_ez_max_concurrent_handshakes: 10,
        },
    )
    .await
    .expect("NodeBootstrapV2 should connect to the EzManagementService");

    let result = timeout(CONTACTED_TIMEOUT, bootstrap.run())
        .await
        .expect("Bootstrap should fail fast when mTLS cert fetch fails");
    assert!(result.is_err(), "Bootstrap must fail when mTLS cert fetch fails");
    assert!(
        !harness.management_service_contacted_within(NOT_CONTACTED_WINDOW).await,
        "EzManagementService must not be contacted when mTLS identity acquisition fails"
    );
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

    let bootstrap = NodeBootstrapV2::new(
        harness.state_manager.clone(),
        ContainerManagerRequester::new(request_tx),
        None,
        None,
        NodeBootstrapV2Config {
            ez_management_address: harness.ez_management_address.clone(),
            max_decoding_message_size: MAX_DECODING_SIZE,
            enable_mtls: false,
            ez_to_ez_handshake_timeout: Duration::from_secs(5),
            ez_to_ez_max_concurrent_handshakes: 10,
        },
    )
    .await
    .expect("NodeBootstrapV2 should connect to the EzManagementService");

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
    async fn fetch_operator_info(
        &self,
        _request: Request<FetchOperatorInfoRequest>,
    ) -> Result<Response<FetchOperatorInfoResponse>, Status> {
        Ok(Response::new(FetchOperatorInfoResponse {
            operator_info: Some(OperatorInfo {
                operator_domain: OPERATOR_DOMAIN.to_string(),
                operator_role: OPERATOR_ROLE.to_string(),
            }),
        }))
    }

    async fn fetch_isolate_startup_parameters(
        &self,
        _request: Request<FetchIsolateStartupParametersRequest>,
    ) -> Result<Response<FetchIsolateStartupParametersResponse>, Status> {
        Ok(Response::new(FetchIsolateStartupParametersResponse::default()))
    }

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
        let leaf_der =
            std::fs::read("enforcer/ez_to_ez/test/testdata/leaf.der").expect("read test leaf.der");
        let root_der =
            std::fs::read("enforcer/ez_to_ez/test/testdata/root.der").expect("read test root.der");
        let cert_res = FetchTlsCertificateResponse {
            certificate_chain: vec![leaf_der],
            trust_anchors: vec![root_der],
        };
        let response = InvokeIsolateResponse {
            isolate_output: Some(EzHybridPayload {
                delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                    datagrams: vec![cert_res.encode_to_vec()],
                })),
            }),
            ..Default::default()
        };
        Self::new_with_setup_response(Ok(response)).await
    }

    async fn new_with_setup_response(response: Result<InvokeIsolateResponse, String>) -> Self {
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
        let setup_junction = FakeSetupJunction { response };
        let setup_isolate_client = SetupIsolateClient::new(
            Box::new(setup_junction),
            SETUP_PUBLISHER_ID.to_string(),
            isolate_name,
            "SetupService".to_string(),
        );

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

        let ez_management_address = format!("unix:{}", uds_path.display());
        let bootstrap = NodeBootstrapV2::new(
            state_manager.clone(),
            container_manager_requester.clone(),
            None,
            None,
            NodeBootstrapV2Config {
                ez_management_address: ez_management_address.clone(),
                max_decoding_message_size: MAX_DECODING_SIZE,
                enable_mtls: false,
                ez_to_ez_handshake_timeout: Duration::from_secs(5),
                ez_to_ez_max_concurrent_handshakes: 10,
            },
        )
        .await
        .expect("NodeBootstrapV2 should connect to the EzManagementService");

        Self {
            bootstrap,
            state_manager,
            container_manager_requester,
            ez_management_address,
            setup_isolate_id,
            contacted_rx: Arc::new(Mutex::new(contacted_rx)),
            _package_dir: package_dir,
            server_dir,
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
    setup_isolate_client: SetupIsolateClient,
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
