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
use container_manager_request::{ContainerManagerRequest, GetSetupIsolateClientResponse};
use container_manager_requester::ContainerManagerRequester;
use data_scope::request::AddIsolateRequest;
use data_scope::requester::DataScopeRequester;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, InvokeEzRequest, InvokeEzResponse, InvokeIsolateRequest,
    InvokeIsolateResponse, IsolateState,
};
use ez_error::EzError;
use ez_mtls_proto::enforcer::v1::ez_mtls_service_server::{EzMtlsService, EzMtlsServiceServer};
use ez_mtls_proto::enforcer::v1::{
    GetCertificateRequest, GetCertificateResponse, ReportSniRequest, ReportSniResponse,
};
use isolate_info::{register_isolate_type, BinaryServicesIndex, IsolateId};
use junction_trait::Junction;
use node_bootstrap::{NodeBootstrapV1, NodeBootstrapV1Config};
use outbound_ez_to_ez_client::{OutboundEzToEzClient, OutboundTlsConfig};
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use prost::Message;
use setup_isolate_client::SetupIsolateClient;
use setup_isolate_proto::enforcer::v2::FetchTlsCertificateResponse;
use state_manager::IsolateStateManager;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

const MAX_DECODING_SIZE: usize = 4 * 1024 * 1024;
const CHANNEL_SIZE: usize = 128;
const SETUP_PUBLISHER_ID: &str = "EZ_Trusted";
const NOT_CONTACTED_WINDOW: Duration = Duration::from_millis(500);
const CONTACTED_TIMEOUT: Duration = Duration::from_secs(10);

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
    response: InvokeIsolateResponse,
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
        Ok(self.response.clone())
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

struct MockMtlsService {
    leaf_der: Vec<u8>,
    root_der: Vec<u8>,
}

#[tonic::async_trait]
impl EzMtlsService for MockMtlsService {
    async fn get_certificate(
        &self,
        _request: Request<GetCertificateRequest>,
    ) -> Result<Response<GetCertificateResponse>, Status> {
        Ok(Response::new(GetCertificateResponse {
            certs: vec![self.leaf_der.clone(), self.root_der.clone()],
            ..Default::default()
        }))
    }

    async fn report_sni(
        &self,
        _request: Request<ReportSniRequest>,
    ) -> Result<Response<ReportSniResponse>, Status> {
        Ok(Response::new(ReportSniResponse::default()))
    }
}

struct TestHarness {
    state_manager: IsolateStateManager,
    container_manager_requester: ContainerManagerRequester,
    setup_isolate_id: IsolateId,
}

impl TestHarness {
    async fn new() -> Self {
        let isolate_name =
            format!("setup_isolate_v1_{}", SETUP_ISOLATE_SEQ.fetch_add(1, Ordering::SeqCst));
        let setup_isolate_index = BinaryServicesIndex::new(/*is_ratified_binary=*/ true);
        register_isolate_type(
            setup_isolate_index,
            IsolateType {
                publisher_id: SETUP_PUBLISHER_ID.to_string(),
                isolate_name: isolate_name.clone(),
            },
        );
        let leaf_der =
            std::fs::read("enforcer/ez_to_ez/test/testdata/leaf.der").expect("read test leaf.der");
        let root_der =
            std::fs::read("enforcer/ez_to_ez/test/testdata/root.der").expect("read test root.der");
        let cert_res = FetchTlsCertificateResponse {
            certificate_chain: vec![leaf_der],
            trust_anchors: vec![root_der],
        };
        let setup_junction = FakeSetupJunction {
            response: InvokeIsolateResponse {
                isolate_output: Some(EzHybridPayload {
                    delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                        datagrams: vec![cert_res.encode_to_vec()],
                    })),
                }),
                ..Default::default()
            },
        };
        let setup_isolate_client = SetupIsolateClient::new(
            Box::new(setup_junction),
            SETUP_PUBLISHER_ID.to_string(),
            isolate_name,
            "SetupService".to_string(),
        );

        let (request_tx, mut request_rx) = mpsc::channel(CHANNEL_SIZE);
        let container_manager_requester = ContainerManagerRequester::new(request_tx);
        tokio::spawn(async move {
            while let Some(request) = request_rx.recv().await {
                if let ContainerManagerRequest::GetSetupIsolateClient { resp } = request {
                    let _ = resp.send(Ok(GetSetupIsolateClientResponse {
                        client: Some(setup_isolate_client.clone()),
                    }));
                }
            }
        });

        let state_manager = IsolateStateManager::new(
            DataScopeRequester::new(u64::MAX),
            container_manager_requester.clone(),
        );

        let setup_isolate_id = IsolateId::new(setup_isolate_index);
        state_manager
            .add_isolate(AddIsolateRequest {
                isolate_id: setup_isolate_id,
                current_data_scope_type: DataScopeType::Public,
                allowed_data_scope_type: DataScopeType::Public,
            })
            .await;
        state_manager.mark_channel_connected(setup_isolate_id).await.expect("channel connected");

        Self { state_manager, container_manager_requester, setup_isolate_id }
    }

    async fn mark_setup_isolate_ready(&self) {
        self.state_manager
            .update_state(self.setup_isolate_id, IsolateState::Ready)
            .await
            .expect("Setup Isolate should transition to Ready");
    }
}

fn dummy_managers() -> (IsolateStateManager, ContainerManagerRequester) {
    let (request_tx, _request_rx) = mpsc::channel(CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(request_tx);
    let state_manager = IsolateStateManager::new(
        DataScopeRequester::new(u64::MAX),
        container_manager_requester.clone(),
    );
    (state_manager, container_manager_requester)
}

#[tokio::test]
async fn test_bootstrap_v1_without_mtls() {
    let (state_manager, container_manager_requester) = dummy_managers();
    let config = NodeBootstrapV1Config {
        enable_mtls: false,
        enable_tls_cert_remote_fetch: false,
        mtls_control_plane_uds_path: None,
        mtls_key_path: None,
        mtls_leaf_csr_path: None,
        ez_to_ez_handshake_timeout: Duration::from_secs(5),
        ez_to_ez_max_concurrent_handshakes: 10,
        max_decoding_message_size: MAX_DECODING_SIZE,
    };
    let bootstrap =
        NodeBootstrapV1::new(state_manager, container_manager_requester, None, None, config);
    bootstrap.run().await.expect("Bootstrap V1 should succeed without mTLS");
}

#[tokio::test]
async fn test_bootstrap_v1_mtls_missing_fields_fail() {
    let (state_manager, container_manager_requester) = dummy_managers();

    // Missing control plane UDS path
    let config = NodeBootstrapV1Config {
        enable_mtls: true,
        enable_tls_cert_remote_fetch: false,
        mtls_control_plane_uds_path: None,
        mtls_key_path: Some("key.pem".to_string()),
        mtls_leaf_csr_path: Some("csr.pem".to_string()),
        ez_to_ez_handshake_timeout: Duration::from_secs(5),
        ez_to_ez_max_concurrent_handshakes: 10,
        max_decoding_message_size: MAX_DECODING_SIZE,
    };
    let bootstrap = NodeBootstrapV1::new(
        state_manager.clone(),
        container_manager_requester.clone(),
        None,
        None,
        config,
    );
    let err = bootstrap.run().await.unwrap_err().to_string();
    assert!(err.contains("mtls_control_plane_uds_path is missing"));

    // Missing key path
    let config = NodeBootstrapV1Config {
        enable_mtls: true,
        enable_tls_cert_remote_fetch: false,
        mtls_control_plane_uds_path: Some("unix:/tmp/test.sock".to_string()),
        mtls_key_path: None,
        mtls_leaf_csr_path: Some("csr.pem".to_string()),
        ez_to_ez_handshake_timeout: Duration::from_secs(5),
        ez_to_ez_max_concurrent_handshakes: 10,
        max_decoding_message_size: MAX_DECODING_SIZE,
    };
    let bootstrap = NodeBootstrapV1::new(
        state_manager.clone(),
        container_manager_requester.clone(),
        None,
        None,
        config,
    );
    let err = bootstrap.run().await.unwrap_err().to_string();
    assert!(err.contains("mtls_key_path is missing"));

    // Missing CSR path
    let config = NodeBootstrapV1Config {
        enable_mtls: true,
        enable_tls_cert_remote_fetch: false,
        mtls_control_plane_uds_path: Some("unix:/tmp/test.sock".to_string()),
        mtls_key_path: Some("key.pem".to_string()),
        mtls_leaf_csr_path: None,
        ez_to_ez_handshake_timeout: Duration::from_secs(5),
        ez_to_ez_max_concurrent_handshakes: 10,
        max_decoding_message_size: MAX_DECODING_SIZE,
    };
    let bootstrap =
        NodeBootstrapV1::new(state_manager, container_manager_requester, None, None, config);
    let err = bootstrap.run().await.unwrap_err().to_string();
    assert!(err.contains("mtls_leaf_csr_path is missing"));
}

#[tokio::test]
async fn test_bootstrap_v1_with_mtls_configures_outbound_tls() {
    let (state_manager, container_manager_requester) = dummy_managers();
    let leaf_der =
        std::fs::read("enforcer/ez_to_ez/test/testdata/leaf.der").expect("read test leaf.der");
    let root_der =
        std::fs::read("enforcer/ez_to_ez/test/testdata/root.der").expect("read test root.der");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let local_addr = listener.local_addr().expect("local addr");
    let server_addr = format!("http://{}", local_addr);

    let service = MockMtlsService { leaf_der, root_der };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(EzMtlsServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                shutdown_rx.await.ok();
            })
            .await;
    });

    let config = NodeBootstrapV1Config {
        enable_mtls: true,
        enable_tls_cert_remote_fetch: false,
        mtls_control_plane_uds_path: Some(server_addr),
        mtls_key_path: Some("enforcer/ez_to_ez/test/testdata/leaf.key".to_string()),
        mtls_leaf_csr_path: Some("enforcer/ez_to_ez/test/testdata/leaf.csr".to_string()),
        ez_to_ez_handshake_timeout: Duration::from_secs(5),
        ez_to_ez_max_concurrent_handshakes: 10,
        max_decoding_message_size: MAX_DECODING_SIZE,
    };

    let fake_outbound = FakeOutboundEzToEzClient::default();
    let bootstrap = NodeBootstrapV1::new(
        state_manager,
        container_manager_requester,
        Some(Box::new(fake_outbound.clone())),
        None,
        config,
    );

    bootstrap.run().await.expect("Bootstrap V1 with mTLS should succeed");

    let configured_outbound = fake_outbound
        .tls_config
        .lock()
        .unwrap()
        .clone()
        .expect("set_tls_config should be called on outbound handler");
    assert_eq!(configured_outbound.trust_domain, "avs.tca.fakeca");

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_bootstrap_v1_remote_fetch_waits_for_setup_isolate() {
    let harness = TestHarness::new().await;
    let config = NodeBootstrapV1Config {
        enable_mtls: true,
        enable_tls_cert_remote_fetch: true,
        mtls_control_plane_uds_path: None,
        mtls_key_path: None,
        mtls_leaf_csr_path: None,
        ez_to_ez_handshake_timeout: Duration::from_secs(5),
        ez_to_ez_max_concurrent_handshakes: 10,
        max_decoding_message_size: MAX_DECODING_SIZE,
    };
    let fake_outbound = FakeOutboundEzToEzClient::default();
    let bootstrap = NodeBootstrapV1::new(
        harness.state_manager.clone(),
        harness.container_manager_requester.clone(),
        Some(Box::new(fake_outbound.clone())),
        None,
        config,
    );
    let run_handle = tokio::spawn(async move { bootstrap.run().await });

    tokio::time::sleep(NOT_CONTACTED_WINDOW).await;
    assert!(!run_handle.is_finished(), "Bootstrap V1 should still be waiting on the Setup Isolate");

    harness.mark_setup_isolate_ready().await;

    timeout(CONTACTED_TIMEOUT, run_handle)
        .await
        .expect("Bootstrap V1 should finish once the Setup Isolate is Ready")
        .expect("Bootstrap V1 task should not panic")
        .expect("Bootstrap V1 should succeed");

    let configured_outbound = fake_outbound
        .tls_config
        .lock()
        .unwrap()
        .clone()
        .expect("set_tls_config should be called on outbound handler");
    assert_eq!(configured_outbound.trust_domain, "avs.tca.fakeca");
}

#[tokio::test]
async fn test_bootstrap_v1_remote_fetch_fails_without_setup_isolate() {
    let harness = TestHarness::new().await;

    let (request_tx, mut request_rx) = mpsc::channel(CHANNEL_SIZE);
    tokio::spawn(async move {
        while let Some(request) = request_rx.recv().await {
            if let ContainerManagerRequest::GetSetupIsolateClient { resp } = request {
                let _ = resp.send(Ok(GetSetupIsolateClientResponse { client: None }));
            }
        }
    });

    let config = NodeBootstrapV1Config {
        enable_mtls: true,
        enable_tls_cert_remote_fetch: true,
        mtls_control_plane_uds_path: None,
        mtls_key_path: None,
        mtls_leaf_csr_path: None,
        ez_to_ez_handshake_timeout: Duration::from_secs(5),
        ez_to_ez_max_concurrent_handshakes: 10,
        max_decoding_message_size: MAX_DECODING_SIZE,
    };
    let bootstrap = NodeBootstrapV1::new(
        harness.state_manager.clone(),
        ContainerManagerRequester::new(request_tx),
        None,
        None,
        config,
    );

    let result = timeout(CONTACTED_TIMEOUT, bootstrap.run())
        .await
        .expect("Bootstrap V1 should fail fast without a Setup Isolate");
    let err = result.expect_err("Bootstrap V1 must fail without a Setup Isolate").to_string();
    assert!(
        err.contains("No Setup Isolate is configured"),
        "Expected 'No Setup Isolate is configured' error, got: {err}"
    );
}
