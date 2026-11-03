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

use anyhow::{Context, Result};
use container_manager_request::ContainerManagerRequest;
use container_manager_requester::ContainerManagerRequester;
use data_scope::{
    manifest_validator::ManifestValidator,
    request::{AddBackendDependenciesRequest, AddIsolateRequest},
    requester::DataScopeRequester,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    isolate_ez_bridge_client::IsolateEzBridgeClient,
    isolate_ez_bridge_server::IsolateEzBridgeServer, ControlPlaneMetadata, EzPayloadIsolateScope,
    InvokeEzRequest, InvokeEzResponse, IsolateDataScope,
};
use external_proxy_connector::{ExternalProxyChannel, ExternalProxyConnectorError};
use fileshare_manager::FileshareManager;
use hyper_util::rt::tokio::TokioIo;
use isolate_ez_service::{IsolateEzBridgeDependencies, IsolateEzBridgeService};
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use isolate_test_utils::{
    DefaultEchoIsolate, ScopeDragInstruction, TEST_ERROR_CODE, TEST_ERROR_MESSAGE,
    TEST_ERROR_SERVICE_NAME,
};
use junction_test_utils::FakeJunction;
use manifest_proto::enforcer::v1::ez_backend_dependency::RouteType;
use outbound_ez_to_ez_client::{OutboundEzToEzClient, OutboundTlsConfig};
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc::{self, Receiver};
use tokio::time::{sleep, Duration};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Status;
use tower::service_fn;

pub const CHANNEL_SIZE: usize = 10;
// Large threshold keeps requests on the inline gRPC-over-UDS path unless overridden.
pub const SHM_PAYLOAD_LARGE_THRESHOLD: u64 = 100 * 1024 * 1024;
pub const TEST_REMOTE_OPERATOR_DOMAIN: &str = "some.remote.service";
pub const TEST_SERVICE_NAME: &str = "TestService";
pub const TEST_INTERNAL_OPERATOR_DOMAIN: &str = "internal";
pub const TEST_EXTERNAL_OPERATOR_DOMAIN: &str = "some.external.service";
pub const TEST_SOME_DOMAIN: &str = "some.domain";
pub const TEST_SOME_SERVICE_NAME: &str = "some_service";
pub const TEST_INTERNAL_ROUTE_TYPE: RouteType = RouteType::Internal;
pub const TEST_EXTERNAL_ROUTE_TYPE: RouteType = RouteType::External;
pub const TEST_REMOTE_ROUTE_TYPE: RouteType = RouteType::Remote;
pub const TEST_PAYLOAD: &[u8] = b"hello world";

// Mock proxy component for testing Ez-to-External unary and streaming calls.
#[derive(Clone, Debug, Default)]
pub struct MockExternalProxy {
    pub call_count: Arc<AtomicUsize>,
    pub received_from_bridge_count: Arc<AtomicUsize>,
    pub delay: Arc<Mutex<Duration>>,
}

#[tonic::async_trait]
impl ExternalProxyChannel for MockExternalProxy {
    async fn proxy_external(
        &self,
        _: IsolateId,
        request: InvokeEzRequest,
        timeout: Option<std::time::Instant>,
    ) -> Result<InvokeEzResponse, ExternalProxyConnectorError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        if let Some(t) = timeout {
            let delay = *self.delay.lock().unwrap();
            if std::time::Instant::now() + delay > t {
                return Err(ExternalProxyConnectorError::ConnectionFailed(
                    "Mock proxy timeout".to_string(),
                ));
            }
        }
        let delay = *self.delay.lock().unwrap();
        sleep(delay).await;
        if request.control_plane_metadata.unwrap().destination_service_name
            == TEST_ERROR_SERVICE_NAME
        {
            return Err(ExternalProxyConnectorError::UnaryCallFailed(Status::new(
                TEST_ERROR_CODE.into(),
                TEST_ERROR_MESSAGE,
            )));
        }
        Ok(get_sample_invoke_ez_response())
    }

    async fn stream_proxy_external(
        &self,
        _: IsolateId,
        mut from_bridge_rx: Receiver<InvokeEzRequest>,
        _timeout: Option<std::time::Duration>,
    ) -> Result<Receiver<Result<InvokeEzResponse, Status>>, ExternalProxyConnectorError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let received_from_bridge_count = self.received_from_bridge_count.clone();
        let (to_bridge_tx, to_bridge_rx) = mpsc::channel(1);
        let ret = if let Some(first_request) = from_bridge_rx.recv().await {
            received_from_bridge_count.fetch_add(1, Ordering::SeqCst);
            if first_request.control_plane_metadata.unwrap().destination_service_name
                == TEST_ERROR_SERVICE_NAME
            {
                return Err(ExternalProxyConnectorError::StreamFailed(Box::new(Status::new(
                    TEST_ERROR_CODE.into(),
                    TEST_ERROR_MESSAGE,
                ))));
            }
            Ok(to_bridge_rx)
        } else {
            Err(ExternalProxyConnectorError::ConnectionFailed(
                "Failed to receive first request".to_string(),
            ))
        };
        tokio::spawn(async move {
            while from_bridge_rx.recv().await.is_some() {
                received_from_bridge_count.fetch_add(1, Ordering::SeqCst);
                to_bridge_tx
                    .send(Ok(get_sample_invoke_ez_response()))
                    .await
                    .expect("Should be able to send response");
            }
        });
        ret
    }

    fn clone_channel(&self) -> Box<dyn ExternalProxyChannel> {
        Box::new(self.clone())
    }
}

// Mock outbound handler for testing remote Ez-to-Ez routing.
#[derive(Clone, Debug, Default)]
pub struct MockEzToEzOutboundHandler {
    pub call_count: Arc<AtomicUsize>,
    pub delay: Arc<Mutex<Duration>>,
}

#[tonic::async_trait]
impl OutboundEzToEzClient for MockEzToEzOutboundHandler {
    async fn remote_invoke(
        &self,
        request: InvokeEzRequest,
        timeout: Option<std::time::Instant>,
    ) -> anyhow::Result<InvokeEzResponse> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        if let Some(t) = timeout {
            let delay = *self.delay.lock().unwrap();
            if std::time::Instant::now() + delay > t {
                return Err(anyhow::anyhow!("Mock remote_invoke timeout"));
            }
        }
        let delay = *self.delay.lock().unwrap();
        sleep(delay).await;
        if request.control_plane_metadata.unwrap().destination_service_name
            == TEST_ERROR_SERVICE_NAME
        {
            return Err(Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE).into());
        }
        Ok(InvokeEzResponse::default())
    }

    async fn remote_streaming_connect(
        &self,
        _first_request_metadata: Option<&ControlPlaneMetadata>,
        mut from_local_rx: Receiver<InvokeEzRequest>,
        _timeout: Option<std::time::Duration>,
    ) -> anyhow::Result<Receiver<Result<InvokeEzResponse>>> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let (to_caller_tx, to_caller_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            while let Some(request) = from_local_rx.recv().await {
                if request.control_plane_metadata.unwrap().destination_service_name
                    == TEST_ERROR_SERVICE_NAME
                {
                    to_caller_tx
                        .send(Err(Status::new(TEST_ERROR_CODE.into(), TEST_ERROR_MESSAGE).into()))
                        .await
                        .expect("Should be able to send error response");
                    continue;
                }
                to_caller_tx
                    .send(Ok(get_sample_invoke_ez_response()))
                    .await
                    .expect("Should be able to send response");
            }
        });
        Ok(to_caller_rx)
    }

    fn set_tls_config(&self, _tls_config: OutboundTlsConfig) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct TestHarnessConfig {
    pub is_ratified: bool,
    pub instruction: ScopeDragInstruction,
    pub shm_payload_threshold: u64,
    pub self_hosted_services: Vec<IsolateServiceInfo>,
    pub with_outbound_handlers: bool,
}

impl Default for TestHarnessConfig {
    fn default() -> Self {
        Self {
            is_ratified: false,
            instruction: ScopeDragInstruction::KeepSame,
            shm_payload_threshold: SHM_PAYLOAD_LARGE_THRESHOLD,
            self_hosted_services: Vec::new(),
            with_outbound_handlers: true,
        }
    }
}

// Shared across both `isolate_ez_service_*` test binaries; each binary reads a subset of fields.
#[allow(dead_code)]
#[derive(Debug)]
pub struct TestHarness {
    pub isolate_id: IsolateId,
    pub mock_junction: FakeJunction,
    pub mock_proxy: MockExternalProxy,
    pub mock_ez_to_ez: MockEzToEzOutboundHandler,
    pub mapper: IsolateServiceMapper,
    pub isolate_state_manager: IsolateStateManager,
    pub data_scope_requester: DataScopeRequester,
    pub manifest_validator: ManifestValidator,
    pub container_manager_rx: Receiver<ContainerManagerRequest>,
    pub client: IsolateEzBridgeClient<Channel>,
    pub interceptor: interceptor::Interceptor,
    pub shared_memory_manager: SharedMemManager,
}

impl TestHarness {
    pub async fn with_config(config: TestHarnessConfig) -> Result<Self> {
        log::set_max_level(log::LevelFilter::Trace);
        let mock_proxy = MockExternalProxy::default();
        let mut mock_junction = FakeJunction::default();
        mock_junction.set_fake_isolate(Box::new(DefaultEchoIsolate::new(config.instruction, None)));
        let mock_ez_to_ez = MockEzToEzOutboundHandler::default();
        let service_mapper = IsolateServiceMapper::default();
        let binary_services_index = if config.self_hosted_services.is_empty() {
            BinaryServicesIndex::new(config.is_ratified)
        } else {
            service_mapper
                .new_binary_index(
                    config.self_hosted_services,
                    config.is_ratified,
                    String::new(),
                    String::new(),
                )
                .await?
        };
        let isolate_id = IsolateId::new(binary_services_index);
        let (tx, container_manager_rx) = mpsc::channel(1);
        let container_manager_requester = ContainerManagerRequester::new(tx);
        let data_scope_requester = DataScopeRequester::new(0);
        let manifest_validator = ManifestValidator::default();
        let isolate_state_manager = IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        );
        let shared_memory_manager =
            SharedMemManager::new(container_manager_requester.clone(), 64, 4);
        let interceptor_instance = interceptor::Interceptor::new(service_mapper.clone());
        let fileshare_manager = FileshareManager::new(container_manager_requester);
        let external_proxy_connector: Option<Box<dyn ExternalProxyChannel>> =
            config.with_outbound_handlers.then(|| Box::new(mock_proxy.clone()) as _);
        let ez_to_ez_outbound_handler: Option<Box<dyn OutboundEzToEzClient>> =
            config.with_outbound_handlers.then(|| Box::new(mock_ez_to_ez.clone()) as _);
        let deps = IsolateEzBridgeDependencies {
            isolate_id,
            isolate_junction: Box::new(mock_junction.clone()),
            isolate_state_manager: isolate_state_manager.clone(),
            shared_memory_manager: shared_memory_manager.clone(),
            fileshare_manager,
            external_proxy_connector,
            isolate_service_mapper: service_mapper.clone(),
            data_scope_requester: data_scope_requester.clone(),
            manifest_validator: manifest_validator.clone(),
            ez_to_ez_outbound_handler,
            interceptor: interceptor_instance.clone(),
            shm_payload_threshold: config.shm_payload_threshold,
        };
        let client = spawn_test_server(IsolateEzBridgeService::new(deps)).await;
        Ok(Self {
            isolate_id,
            mock_junction,
            mock_proxy,
            mock_ez_to_ez,
            mapper: service_mapper,
            isolate_state_manager,
            data_scope_requester,
            manifest_validator,
            container_manager_rx,
            client,
            interceptor: interceptor_instance,
            shared_memory_manager,
        })
    }

    pub async fn new_with_arguments(
        is_ratified: bool,
        instruction: ScopeDragInstruction,
        shm_payload_threshold: u64,
    ) -> Result<Self> {
        Self::with_config(TestHarnessConfig {
            is_ratified,
            instruction,
            shm_payload_threshold,
            ..Default::default()
        })
        .await
    }

    pub async fn new() -> Result<Self> {
        Self::with_config(TestHarnessConfig::default()).await
    }
}

/// Spawns an in-memory `IsolateEzBridgeServer` and connects a test client to it.
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

/// Creates a default inline `InvokeEzRequest` targeting `(domain, name)`.
pub fn create_test_request(domain: &str, name: &str) -> InvokeEzRequest {
    InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            destination_operator_domain: domain.to_string(),
            destination_service_name: name.to_string(),
            ..Default::default()
        }),
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Unspecified.into(),
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

/// Creates an `InvokeEzRequest` targeting `(domain, name)` with explicit `scope`.
pub fn create_test_request_with_scope(
    domain: &str,
    name: &str,
    scope: DataScopeType,
) -> InvokeEzRequest {
    let mut request = create_test_request(domain, name);
    request.isolate_request_iscope = Some(EzPayloadIsolateScope {
        datagram_iscopes: vec![IsolateDataScope { scope_type: scope.into(), ..Default::default() }],
    });
    request
}

pub async fn add_backend_dependencies(
    isolate_id: IsolateId,
    isolate_service_mapper: IsolateServiceMapper,
    manifest_validator: ManifestValidator,
    isolate_service_info: &IsolateServiceInfo,
    is_internal: bool,
    route_type: RouteType,
) -> Result<()> {
    if is_internal {
        isolate_service_mapper
            .new_binary_index(
                vec![isolate_service_info.clone()],
                false,
                isolate_service_info.publisher_id.clone(),
                isolate_service_info.isolate_name.clone(),
            )
            .await
            .context("Should be a valid binary services index")?;
    }
    let dependency_index = isolate_service_mapper
        .add_backend_dependency_service(isolate_service_info, route_type)
        .await
        .context("Should be able to add external isolate service")?;
    manifest_validator
        .add_backend_dependencies(AddBackendDependenciesRequest {
            binary_services_index: isolate_id.get_binary_services_index(),
            dependency_index,
        })
        .await
        .context("Should be able to add backend dependency in manifest")?;
    Ok(())
}

pub async fn add_to_data_scope_requester(
    data_scope_requester: DataScopeRequester,
    isolate_id: IsolateId,
) -> Result<()> {
    data_scope_requester
        .add_isolate(AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: DataScopeType::UserPrivate,
            isolate_id,
        })
        .await?;
    Ok(())
}

pub fn get_sample_invoke_ez_response() -> InvokeEzResponse {
    InvokeEzResponse {
        ez_response_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![TEST_PAYLOAD.to_vec()],
            })),
        }),
        ez_response_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                mapped_scope_owner: None,
            }],
        }),
        ..Default::default()
    }
}
