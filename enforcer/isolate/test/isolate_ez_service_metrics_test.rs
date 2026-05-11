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
use fileshare_manager::FileshareManager;
use hyper_util::rt::TokioIo;
use interceptor::Interceptor;
use isolate_ez_service::{IsolateEzBridgeDependencies, IsolateEzBridgeService};
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use isolate_test_utils::{DefaultEchoIsolate, ScopeDragInstruction};
use junction_test_utils::FakeJunction;
use manifest_proto::enforcer::v1::ez_backend_dependency::RouteType;
use manifest_proto::enforcer::v1::{AllowedMetric, IsolateMetricsPolicy};
use metrics::setup_otel_metrics;
use metrics_test_utils::MetricsVerifier;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
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
const METRICS_UDS_NAME: &str = "otel-metrics.sock";
const ISOLATE_UDS_NAME: &str = "isolate-ez-bridge-uds";
const FIFO_NAME: &str = "fifo";
const TEST_ISOLATE_NAME: &str = "test-isolate";
const TEST_PUBLISHER_ID: &str = "test-publisher";

#[derive(Debug)]
struct TestHarness {
    isolate_id: IsolateId,
    _mock_junction: FakeJunction,
    mapper: IsolateServiceMapper,
    data_scope_requester: DataScopeRequester,
    manifest_validator: ManifestValidator,
    client: IsolateEzBridgeClient<tonic::transport::Channel>,
    server_handle: tokio::task::JoinHandle<()>,
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
        let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone());
        let fileshare_manager = FileshareManager::new(container_manager_requester);

        let deps = IsolateEzBridgeDependencies {
            isolate_id,
            isolate_junction: Box::new(mock_junction.clone()),
            isolate_state_manager: isolate_state_manager.clone(),
            shared_memory_manager,
            fileshare_manager,
            external_proxy_connector: None,
            isolate_service_mapper: service_mapper.clone(),
            data_scope_requester: data_scope_requester.clone(),
            manifest_validator: manifest_validator.clone(),
            ez_to_ez_outbound_handler: None,
            interceptor: Interceptor::new(service_mapper.clone()),
        };
        let isolate_ez_bridge_service = IsolateEzBridgeService::new(deps);

        // Start Server
        let server_handle = tokio::spawn(async move {
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
            server_handle,
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

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.server_handle.abort();
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
        isolate_request_payload: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData::default())),
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_metrics_server_reception() {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempfile::Builder::new().prefix("test-metrics-server").tempdir().unwrap();
    let metrics_uds_path = temp_dir.path().join(METRICS_UDS_NAME);
    let isolate_uds_path = temp_dir.path().join(ISOLATE_UDS_NAME);
    let fifo_path = temp_dir.path().join(FIFO_NAME);

    let _ = nix::unistd::mkfifo(&fifo_path, nix::sys::stat::Mode::S_IRWXU);

    let service_mapper = IsolateServiceMapper::default();
    let data_scope_requester = DataScopeRequester::new(0);
    let (tx, _container_manager_rx) = tokio::sync::mpsc::channel(1);
    let container_manager_requester = ContainerManagerRequester::new(tx);
    let manifest_validator = ManifestValidator::default();

    let deps = isolate_ez_service_manager::IsolateEzServiceManagerDependencies {
        isolate_junction: Box::new(FakeJunction::default()),
        isolate_state_manager: IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        ),
        shared_memory_manager: SharedMemManager::new(container_manager_requester.clone()),
        fileshare_manager: FileshareManager::new(container_manager_requester),
        external_proxy_connector: None,
        isolate_service_mapper: service_mapper.clone(),
        data_scope_requester: data_scope_requester.clone(),
        manifest_validator: manifest_validator.clone(),
        ez_to_ez_outbound_handler: None,
        max_decoding_message_size: 4 * 1024 * 1024,
        interceptor: Interceptor::new(service_mapper),
        otel_endpoint: None,
        disable_metrics_filtering: false,
    };

    let manager = isolate_ez_service_manager::IsolateEzServiceManager::new(deps);
    let isolate_id = IsolateId::new(BinaryServicesIndex::new(false));

    manager
        .start_isolate_ez_server(isolate_ez_service_manager::StartIsolateEzServerArgs {
            isolate_id,
            isolate_address: isolate_uds_path.display().to_string(),
            isolate_fifo_path: fifo_path.display().to_string(),
            otel_metrics_address: metrics_uds_path.display().to_string(),
            metrics_policy: IsolateMetricsPolicy {
                allowed_metrics: vec![AllowedMetric {
                    name: "test_metric".to_string(),
                    r#type: 1, // Gauge
                }],
            },
            isolate_name: TEST_ISOLATE_NAME.to_string(),
            publisher_id: TEST_PUBLISHER_ID.to_string(),
        })
        .await;

    // Unblock the FIFO opening task in IsolateEzServiceManager
    let read_fifo_path = fifo_path.clone();
    tokio::spawn(async move {
        let _ = tokio::fs::OpenOptions::new().read(true).open(read_fifo_path).await;
    });

    let mut attempts = 0;
    while !metrics_uds_path.exists() && attempts < 100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        attempts += 1;
    }
    assert!(metrics_uds_path.exists(), "Metrics UDS path was never created");

    let stream = tokio::net::UnixStream::connect(&metrics_uds_path).await;
    assert!(stream.is_ok(), "Failed to connect to Metrics UDS via simple UnixStream");
    drop(stream);

    let endpoint = Endpoint::from_shared("http://127.0.0.1".to_string()).unwrap();
    let socket_path = metrics_uds_path.clone();
    let channel = endpoint
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let socket_path = socket_path.clone();
            async move {
                Ok::<_, std::io::Error>(TokioIo::new(
                    tokio::net::UnixStream::connect(socket_path).await?,
                ))
            }
        }))
        .await
        .unwrap();
    let mut client = opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient::new(channel);

    let metric = opentelemetry_proto::tonic::metrics::v1::Metric {
        name: "test_metric".to_string(),
        description: "".to_string(),
        unit: "".to_string(),
        data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(
            opentelemetry_proto::tonic::metrics::v1::Gauge {
                data_points: vec![opentelemetry_proto::tonic::metrics::v1::NumberDataPoint {
                    attributes: vec![],
                    value: Some(
                        opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(
                            42,
                        ),
                    ),
                    ..Default::default()
                }],
            },
        )),
        ..Default::default()
    };

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
            resource: Some(Default::default()),
            scope_metrics: vec![opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                scope: None,
                metrics: vec![metric],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    };

    let response = client.export(request).await;
    assert!(response.is_err(), "Expected export to fail when no safe endpoint is configured");

    drop(client); // Close connection
    manager.stop_isolate_servers(isolate_id).await;
    metrics::global::reset_global_providers();
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
        let mut client = harness.client.clone();

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

    metrics::global::reset_global_providers();
}
