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

use anyhow::{Context, Result};
use grpc_connector::GrpcChannelPool;
use manifest_proto::enforcer::v1::{allowed_metric::MetricType, IsolateMetricsPolicy};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::metrics::v1::{metric::Data, Metric};
use std::collections::HashMap;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

/// Receiver for Isolate metrics. It filters metrics based on policy and injects identity.
pub struct IsolateMetricsReceiver {
    /// Map of allowed metric names to their expected types.
    /// Key: Metric name (e.g., "http.server.duration").
    /// Value: Allowed MetricType enum.
    allowed_metrics: HashMap<String, MetricType>,
    isolate_name: String,
    publisher_id: String,
    client: Option<MetricsServiceClient<Channel>>,
    disable_filtering: bool,
}

impl IsolateMetricsReceiver {
    /// Creates a new `IsolateMetricsReceiver`.
    pub async fn new(
        policy: IsolateMetricsPolicy,
        isolate_name: String,
        publisher_id: String,
        otel_endpoint: Option<String>,
        max_decoding_message_size: usize,
        disable_filtering: bool,
    ) -> Result<Self> {
        let mut allowed_metrics = HashMap::new();

        for allowed in &policy.allowed_metrics {
            allowed_metrics.insert(
                allowed.name.clone(),
                MetricType::try_from(allowed.r#type).unwrap_or(MetricType::Unspecified),
            );
        }

        let client = if let Some(endpoint) = otel_endpoint {
            let pool = GrpcChannelPool::new(
                endpoint.clone(),
                1,
                grpc_connector::DEFAULT_CONNECT_RETRY_COUNT,
                grpc_connector::DEFAULT_CONNECT_RETRY_DELAY_MS,
                grpc_connector::DEFAULT_CONNECT_RETRY_SCALING,
            )
            .await
            .context("Failed to create GrpcChannelPool to safe endpoint")?;
            let channel = pool.next_channel();
            Some(
                MetricsServiceClient::new(channel)
                    .max_decoding_message_size(max_decoding_message_size)
                    .max_encoding_message_size(max_decoding_message_size),
            )
        } else {
            None
        };

        Ok(Self { allowed_metrics, isolate_name, publisher_id, client, disable_filtering })
    }

    /// Gets the MetricsServiceClient.
    fn get_client(&self) -> Result<MetricsServiceClient<Channel>> {
        self.client.clone().context("No safe endpoint configured or client not initialized")
    }

    /// Filters metrics based on policy and injects identity.
    pub fn filter_metrics(&self, request: &mut ExportMetricsServiceRequest) {
        if self.disable_filtering {
            return;
        }
        for resource_metrics in &mut request.resource_metrics {
            // Inject resource attributes, ignoring whatever the Isolate claimed to be.
            let resource = resource_metrics.resource.get_or_insert_with(Default::default);

            // Remove existing attributes that we want to override
            resource.attributes.retain(|attr| {
                attr.key != "ez_component_name"
                    && attr.key != "ez_isolate_name"
                    && attr.key != "ez_publisher_id"
            });

            let mut extra_attrs = vec![
                KeyValue {
                    key: "ez_component_name".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "isolate".to_string(),
                            ),
                        ),
                    }),
                },
                KeyValue {
                    key: "ez_isolate_name".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                self.isolate_name.clone(),
                            ),
                        ),
                    }),
                },
                KeyValue {
                    key: "ez_publisher_id".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                self.publisher_id.clone(),
                            ),
                        ),
                    }),
                },
            ];
            resource.attributes.append(&mut extra_attrs);

            for scope_metrics in &mut resource_metrics.scope_metrics {
                scope_metrics.metrics.retain_mut(|metric| {
                    let Some(allowed_type) = self.allowed_metrics.get(&metric.name) else {
                        log::trace!("Dropping metric '{}': not in allowlist", metric.name);
                        return false;
                    };
                    if !Self::match_type(metric, *allowed_type) {
                        log::trace!("Dropping metric '{}': type mismatch", metric.name);
                        return false;
                    }
                    true
                });
            }
        }
    }

    fn match_type(metric: &Metric, allowed_type: MetricType) -> bool {
        matches!(
            (&metric.data, allowed_type),
            (Some(Data::Gauge(_)), MetricType::Gauge)
                | (Some(Data::Sum(_)), MetricType::Sum)
                | (Some(Data::Histogram(_)), MetricType::Histogram)
                | (Some(Data::ExponentialHistogram(_)), MetricType::ExponentialHistogram)
                | (Some(Data::Summary(_)), MetricType::Summary)
        )
    }
}

#[tonic::async_trait]
impl MetricsService for IsolateMetricsReceiver {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let mut metrics_request = request.into_inner();
        self.filter_metrics(&mut metrics_request);

        let mut client = self.get_client().map_err(|e| {
            log::error!("Failed to get MetricsServiceClient: {:?}", e);
            Status::internal("Failed to connect to safe endpoint")
        })?;

        client.export(metrics_request).await
    }
}
