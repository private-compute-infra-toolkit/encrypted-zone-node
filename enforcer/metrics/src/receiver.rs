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
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::metrics::v1::{metric::Data, Metric};
use std::collections::{HashMap, HashSet};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

struct MetricPolicy {
    metric_type: MetricType,
    allowed_attributes: HashSet<String>,
}

impl MetricPolicy {
    /// Checks if this policy matches the given metric's data type.
    fn matches(&self, metric: &Metric) -> bool {
        if self.metric_type == MetricType::Unspecified {
            return true;
        }
        matches!(
            (&metric.data, self.metric_type),
            (Some(Data::Gauge(_)), MetricType::Gauge)
                | (Some(Data::Sum(_)), MetricType::Sum)
                | (Some(Data::Histogram(_)), MetricType::Histogram)
                | (Some(Data::ExponentialHistogram(_)), MetricType::ExponentialHistogram)
                | (Some(Data::Summary(_)), MetricType::Summary)
        )
    }
}

/// Receiver for Isolate metrics. It filters metrics based on policy and injects identity.
pub struct IsolateMetricsReceiver {
    /// Map of exact allowed metric names to their expected policies.
    /// Key: Metric name (e.g., "http.server.duration").
    exact_metrics: HashMap<String, Vec<MetricPolicy>>,
    /// List of allowed metric prefixes to their expected policies
    /// Strings store here will have their trailing * stripped
    prefix_metrics: Vec<(String, MetricPolicy)>,
    isolate_name: String,
    publisher_id: String,
    is_ratified: bool,
    client: Option<MetricsServiceClient<Channel>>,
    disable_filtering: bool,
}

impl IsolateMetricsReceiver {
    /// Creates a new `IsolateMetricsReceiver`.
    pub async fn new(
        policy: IsolateMetricsPolicy,
        isolate_name: String,
        publisher_id: String,
        is_ratified: bool,
        otel_endpoint: Option<String>,
        max_decoding_message_size: usize,
        disable_filtering: bool,
    ) -> Result<Self> {
        let mut exact_metrics: HashMap<String, Vec<MetricPolicy>> = HashMap::new();
        let mut prefix_metrics = Vec::new();

        for allowed in &policy.allowed_metrics {
            let allowed_attributes: HashSet<String> =
                allowed.allowed_attributes.iter().cloned().collect();
            let metric_policy = MetricPolicy {
                metric_type: MetricType::try_from(allowed.r#type)
                    .unwrap_or(MetricType::Unspecified),
                allowed_attributes,
            };
            if allowed.name.ends_with('*') {
                prefix_metrics
                    .push((allowed.name.trim_end_matches('*').to_string(), metric_policy));
            } else {
                exact_metrics.entry(allowed.name.clone()).or_default().push(metric_policy);
            }
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

        Ok(Self {
            exact_metrics,
            prefix_metrics,
            isolate_name,
            publisher_id,
            is_ratified,
            client,
            disable_filtering,
        })
    }

    /// Gets the MetricsServiceClient.
    fn get_client(&self) -> Result<MetricsServiceClient<Channel>> {
        self.client.clone().context("No safe endpoint configured or client not initialized")
    }

    /// Finds the matching policy for a metric.
    fn find_policy(&self, metric: &Metric) -> Option<&MetricPolicy> {
        if let Some(policies) = self.exact_metrics.get(&metric.name) {
            for policy in policies {
                if policy.matches(metric) {
                    return Some(policy);
                }
            }
        }

        for (prefix, policy) in &self.prefix_metrics {
            if metric.name.starts_with(prefix) && policy.matches(metric) {
                return Some(policy);
            }
        }
        None
    }

    /// Filters metrics based on policy.
    pub fn filter_metrics(&self, request: &mut ExportMetricsServiceRequest) {
        if self.disable_filtering {
            return;
        }
        for resource_metrics in &mut request.resource_metrics {
            for scope_metrics in &mut resource_metrics.scope_metrics {
                scope_metrics.metrics.retain_mut(|metric| {
                    if let Some(policy) = self.find_policy(metric) {
                        Self::filter_attributes(metric, policy);
                        return true;
                    }
                    log::trace!(
                        "Dropping metric '{}': not in allowlist or type mismatch",
                        metric.name
                    );
                    false
                });
            }
        }
    }

    /// Appends identity and metadata attributes to the resource attributes of the request.
    pub fn enrich_metrics(&self, request: &mut ExportMetricsServiceRequest) {
        for resource_metrics in &mut request.resource_metrics {
            let resource = resource_metrics.resource.get_or_insert_with(Default::default);

            resource.attributes.retain(|attr| {
                attr.key != "ez_isolate_name"
                    && attr.key != "ez_publisher_id"
                    && attr.key != "ez_isolate_type"
                    && attr.key != "ez_safety_level"
                    && attr.key != "ez_enforcer_version"
                    && attr.key != "ez_component_name"
            });

            resource.attributes.push(make_string_attribute(
                "ez_component_name".to_string(),
                "isolate".to_string(),
            ));
            resource.attributes.push(make_string_attribute(
                "ez_isolate_name".to_string(),
                self.isolate_name.clone(),
            ));
            resource.attributes.push(make_string_attribute(
                "ez_publisher_id".to_string(),
                self.publisher_id.clone(),
            ));

            let isolate_type_str = if self.is_ratified { "ratified" } else { "opaque" };
            resource.attributes.push(make_string_attribute(
                "ez_isolate_type".to_string(),
                isolate_type_str.to_string(),
            ));

            resource
                .attributes
                .push(make_string_attribute("ez_safety_level".to_string(), "safe".to_string()));

            let enforcer_version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
            resource.attributes.push(make_string_attribute(
                "ez_enforcer_version".to_string(),
                enforcer_version.to_string(),
            ));
        }
    }

    /// Filters attributes in the metric data points based on the policy.
    fn filter_attributes(metric: &mut Metric, policy: &MetricPolicy) {
        if let Some(data) = &mut metric.data {
            match data {
                Data::Gauge(gauge) => {
                    for dp in &mut gauge.data_points {
                        Self::sanitize_attributes(&mut dp.attributes, &policy.allowed_attributes);
                    }
                }
                Data::Sum(sum) => {
                    for dp in &mut sum.data_points {
                        Self::sanitize_attributes(&mut dp.attributes, &policy.allowed_attributes);
                    }
                }
                Data::Histogram(hist) => {
                    for dp in &mut hist.data_points {
                        Self::sanitize_attributes(&mut dp.attributes, &policy.allowed_attributes);
                    }
                }
                Data::ExponentialHistogram(exp_hist) => {
                    for dp in &mut exp_hist.data_points {
                        Self::sanitize_attributes(&mut dp.attributes, &policy.allowed_attributes);
                    }
                }
                Data::Summary(summary) => {
                    for dp in &mut summary.data_points {
                        Self::sanitize_attributes(&mut dp.attributes, &policy.allowed_attributes);
                    }
                }
            }
        }
    }

    /// Sanitizes the attributes vector by resetting values of forbidden keys to defaults.
    fn sanitize_attributes(attributes: &mut [KeyValue], allowed_keys: &HashSet<String>) {
        for kv in attributes {
            if !allowed_keys.contains(&kv.key) {
                if let Some(any_val) = &mut kv.value {
                    if let Some(val) = &mut any_val.value {
                        match val {
                            Value::StringValue(s) => *s = "".to_string(),
                            Value::BoolValue(b) => *b = false,
                            Value::IntValue(i) => *i = 0,
                            Value::DoubleValue(d) => *d = 0.0,
                            Value::ArrayValue(arr) => arr.values.clear(),
                            Value::KvlistValue(kv_list) => kv_list.values.clear(),
                            Value::BytesValue(bytes) => bytes.clear(),
                        }
                    }
                }
            }
        }
    }
}

#[tonic::async_trait]
impl MetricsService for IsolateMetricsReceiver {
    /// Exports the filtered metrics to the safe endpoint.
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let mut metrics_request = request.into_inner();
        self.filter_metrics(&mut metrics_request);
        self.enrich_metrics(&mut metrics_request);

        let mut client = self.get_client().map_err(|e| {
            log::error!("Failed to get MetricsServiceClient: {:?}", e);
            Status::internal("Failed to connect to safe endpoint")
        })?;

        client.export(metrics_request).await
    }
}

/// Helper to create a KeyValue attribute with a string value.
fn make_string_attribute(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                value,
            )),
        }),
    }
}
