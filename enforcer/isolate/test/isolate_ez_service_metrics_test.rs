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
use data_scope::{
    manifest_validator::ManifestValidator,
    request::{AddBackendDependenciesRequest, ValidateIsolateRequest},
    requester::DataScopeRequester,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    isolate_ez_bridge_client::IsolateEzBridgeClient,
    isolate_ez_bridge_server::IsolateEzBridgeServer, ControlPlaneMetadata, EzPayloadIsolateScope,
    InvokeEzRequest, IsolateDataScope,
};
use fake_opentelemetry_collector::FakeCollectorServer;
use interceptor::Interceptor;
use isolate_ez_service::{IsolateEzBridgeDependencies, IsolateEzBridgeService};
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use isolate_test_utils::{DefaultEchoIsolate, ScopeDragInstruction};
use junction_test_utils::FakeJunction;
use manifest_proto::enforcer::ez_backend_dependency::RouteType;
use metrics::setup_otel_metrics;
use metrics_test_utils::MetricsVerifier;
use payload_proto::enforcer::v1::EzPayloadData;
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Endpoint, Server};

const TEST_INTERNAL_OPERATOR_DOMAIN: &str = "internal";
const CHANNEL_SIZE: usize = 10;
const TEST_INTERNAL_ROUTE_TYPE: RouteType = RouteType::Internal;

#[derive(Debug)]
struct TestHarness {
    isolate_id: IsolateId,
    _mock_junction: FakeJunction,
    mapper: IsolateServiceMapper,
    data_scope_requester: DataScopeRequester,
    manifest_validator: ManifestValidator,
    client: IsolateEzBridgeClient<tonic::transport::Channel>,
}

impl TestHarness {
    async fn new(listener: TcpListener) -> Result<Self> {
        let port = listener.local_addr().unwrap().port();
        let mut mock_junction = FakeJunction::default();
        mock_junction.set_fake_isolate(Box::new(DefaultEchoIsolate::new(
            ScopeDragInstruction::KeepSame,
            None,
        )));

        let service_mapper = IsolateServiceMapper::default();
        let isolate_id = IsolateId::new(BinaryServicesIndex::new(false));

        let (tx, _container_manager_rx) = mpsc::channel(1);
        let container_manager_requester = ContainerManagerRequester::new(tx);
        let data_scope_requester = DataScopeRequester::new(0);

        let manifest_validator = ManifestValidator::default();
        let isolate_state_manager = IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        );
        let shared_memory_manager = SharedMemManager::new(container_manager_requester);

        let deps = IsolateEzBridgeDependencies {
            isolate_id,
            isolate_junction: Box::new(mock_junction.clone()),
            isolate_state_manager: isolate_state_manager.clone(),
            shared_memory_manager,
            external_proxy_connector: None,
            isolate_service_mapper: service_mapper.clone(),
            data_scope_requester: data_scope_requester.clone(),
            manifest_validator: manifest_validator.clone(),
            ez_to_ez_outbound_handler: None,
            interceptor: Interceptor::new(service_mapper.clone()),
        };
        let isolate_ez_bridge_service = IsolateEzBridgeService::new(deps);

        // Start Server
        tokio::spawn(async move {
            Server::builder()
                .add_service(IsolateEzBridgeServer::new(isolate_ez_bridge_service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("Server failed");
        });

        // Create Client
        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;
        let channel =
            Endpoint::from_shared(format!("http://127.0.0.1:{}", port))?.connect().await?;
        let client = IsolateEzBridgeClient::new(channel);

        Ok(Self {
            isolate_id,
            _mock_junction: mock_junction,
            mapper: service_mapper,
            data_scope_requester,
            manifest_validator,
            client,
        })
    }

    async fn add_backend_dependency(
        &self,
        service_info: &IsolateServiceInfo,
        route_type: RouteType,
    ) -> Result<()> {
        if route_type == RouteType::Internal {
            self.mapper.new_binary_index(vec![service_info.clone()], false).await?;
        } else {
            self.mapper.add_backend_dependency_service(service_info, route_type).await?;
        }
        self.manifest_validator
            .add_backend_dependencies(AddBackendDependenciesRequest {
                binary_services_index: self.isolate_id.get_binary_services_index(),
                dependency_index: self.mapper.get_service_index(service_info).await.unwrap(),
            })
            .await?;
        Ok(())
    }

    async fn setup_data_scope(&self) -> Result<()> {
        // Add Isolate
        self.data_scope_requester
            .add_isolate(data_scope::request::AddIsolateRequest {
                isolate_id: self.isolate_id,
                allowed_data_scope_type: DataScopeType::Public,
                current_data_scope_type: DataScopeType::Public,
            })
            .await?;

        // Validate Isolate
        self.data_scope_requester
            .validate_isolate_scope(ValidateIsolateRequest {
                isolate_id: self.isolate_id,
                requested_scope: DataScopeType::Public,
            })
            .await?;

        // Add Initial Scope
        self.data_scope_requester
            .get_isolate_scope(data_scope::request::GetIsolateScopeRequest {
                isolate_id: self.isolate_id,
            })
            .await?;

        Ok(())
    }
}

fn create_test_request(operator_domain: &str, service_name: &str) -> InvokeEzRequest {
    InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            destination_operator_domain: operator_domain.to_string(),
            destination_service_name: service_name.to_string(),
            destination_method_name: "test_method".to_string(),
            requester_spiffe: String::new(),
            ..Default::default()
        }),
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                ..Default::default()
            }],
        }),
        isolate_request_payload: Some(EzPayloadData::default()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_isolate_ez_service_stream_metrics() {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut collector = FakeCollectorServer::start().await.expect("Failed to start fake collector");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    // Setup metrics with the collector
    let providers = setup_otel_metrics(Some(collector.endpoint()), Some(collector.endpoint()))
        .await
        .expect("Failed to setup OTel metrics");

    let harness = TestHarness::new(listener).await.expect("Harness should start");

    let service_info = IsolateServiceInfo {
        operator_domain: TEST_INTERNAL_OPERATOR_DOMAIN.to_string(),
        service_name: "service".into(),
    };

    harness
        .add_backend_dependency(&service_info, TEST_INTERNAL_ROUTE_TYPE)
        .await
        .expect("Failed to add backend dependency");

    harness.setup_data_scope().await.expect("Should be able to add to DSM/RIM");

    let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);

    // Send 2 requests
    let req1 = create_test_request(&service_info.operator_domain, &service_info.service_name);
    let req2 = create_test_request(&service_info.operator_domain, &service_info.service_name);

    let client_handle = tokio::spawn(async move {
        let mut client = harness.client;

        let _ = req_tx.send(req1).await;

        let request_stream = ReceiverStream::new(req_rx);
        let mut response_stream =
            client.stream_invoke_ez(request_stream).await.unwrap().into_inner();

        // Wait for first response
        if let Ok(Some(_)) = response_stream.message().await {}

        let _ = req_tx.send(req2).await;
        // Wait for second response
        if let Ok(Some(_)) = response_stream.message().await {}

        // Drop tx to close stream
        drop(req_tx);
        // Drain
        while let Ok(Some(_)) = response_stream.message().await {}
    });

    client_handle.await.unwrap();

    // Check Metrics
    // Give time for metrics to be processed and sent
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Some(ref provider) = providers.safe {
        let _ = provider.force_flush();
    }
    if let Some(ref provider) = providers.unsafe_metrics {
        let _ = provider.force_flush();
    }
    // Small delay to ensure flush completes and collector receives data
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let exported = collector.exported_metrics(5, Duration::from_secs(2)).await;

    let verifier = MetricsVerifier::new(exported);

    // Verify Message Count (enforcer.isolate_ez_service.message_size)
    let message_size_points =
        verifier.get_histogram_points("enforcer.isolate_ez_service.message_size");
    let mut request_message_count: u64 = 0;
    let mut response_message_count: u64 = 0;
    let mut found_route_type = false;

    for (count, attrs) in message_size_points {
        if let Some(dir) = attrs.get("direction") {
            if dir.contains("request") {
                request_message_count = request_message_count.max(count);
            } else if dir.contains("response") {
                response_message_count = response_message_count.max(count);
            }
        }
        if let Some(rt) = attrs.get("route_type") {
            if rt.contains("internal") {
                found_route_type = true;
            }
        }
    }

    assert_eq!(request_message_count, 2, "Expected 2 request messages");
    assert_eq!(response_message_count, 2, "Expected 2 response messages");
    assert!(found_route_type, "Should have found route_type=internal attribute");

    // Verify Processing Duration (enforcer.isolate_ez_service.message.processing_duration)
    let processing_points =
        verifier.get_histogram_points("enforcer.isolate_ez_service.message.processing_duration");
    let processing_count: u64 = processing_points.iter().map(|(c, _)| *c).max().unwrap_or(0);

    assert!(
        processing_count >= 2,
        "Expected at least 2 processing duration samples (received {})",
        processing_count
    );

    // Verify Request Duration (enforcer.isolate_ez_service.request.duration)
    let duration_points =
        verifier.get_histogram_points("enforcer.isolate_ez_service.request.duration");
    let duration_count: u64 = duration_points.iter().map(|(c, _)| *c).max().unwrap_or(0);

    assert_eq!(duration_count, 1, "Expected 1 stream duration");
}
