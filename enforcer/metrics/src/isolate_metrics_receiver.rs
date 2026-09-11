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
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use ez_error_trait::ToEzError;

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

/// An identifier representing a metric attribute name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct AttributeName(Cow<'static, str>);

impl AttributeName {
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for AttributeName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for AttributeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&'static str> for AttributeName {
    fn from(s: &'static str) -> Self {
        Self(Cow::Borrowed(s))
    }
}

impl From<String> for AttributeName {
    fn from(s: String) -> Self {
        Self(Cow::Owned(s))
    }
}

/// Represents a custom metric attribute tag categorized by its level (Resource or Scope).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CustomAttribute {
    Resource(AttributeName),
    Scope(AttributeName),
}

impl CustomAttribute {
    pub fn name(&self) -> &AttributeName {
        match self {
            Self::Resource(name) | Self::Scope(name) => name,
        }
    }
}

/// Configuration for initializing an [`IsolateMetricsReceiver`].
#[derive(Debug, Default, Clone)]
pub struct IsolateMetricsReceiverConfig {
    pub policy: IsolateMetricsPolicy,
    pub isolate_name: String,
    pub publisher_id: String,
    pub is_ratified: bool,
    pub isolate_instance_id: String,
    pub otel_endpoint: Option<String>,
    pub max_decoding_message_size: usize,
    pub disable_filtering: bool,
    pub custom_attributes: HashMap<CustomAttribute, Value>,
}

impl IsolateMetricsReceiverConfig {
    /// Attempts to add a custom attribute to this configuration, returning an error
    /// if an attribute with the same level and name has already been configured.
    pub fn try_add_custom_attribute(
        &mut self,
        attribute: CustomAttribute,
        value: impl Into<Value>,
    ) -> Result<(), MetricsReceiverError> {
        let name = attribute.name().to_string();
        match attribute {
            CustomAttribute::Resource(_) => {
                if self.custom_attributes.insert(attribute, value.into()).is_some() {
                    return Err(MetricsReceiverError::ConfigurationError(format!(
                        "Duplicate resource attribute key: '{name}'"
                    )));
                }
            }
            CustomAttribute::Scope(_) => {
                if self.custom_attributes.insert(attribute, value.into()).is_some() {
                    return Err(MetricsReceiverError::ConfigurationError(format!(
                        "Duplicate scope attribute key: '{name}'"
                    )));
                }
            }
        }
        Ok(())
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
    /// Unified map of all identity and metadata attributes (Resource and Scope)
    /// attached by this receiver.
    attributes: HashMap<CustomAttribute, KeyValue>,
    client: Option<MetricsServiceClient<Channel>>,
    disable_filtering: bool,
}

impl IsolateMetricsReceiver {
    /// Creates a new `IsolateMetricsReceiver`.
    pub async fn new(config: IsolateMetricsReceiverConfig) -> Result<Self> {
        let mut exact_metrics: HashMap<String, Vec<MetricPolicy>> = HashMap::new();
        let mut prefix_metrics = Vec::new();

        for allowed in &config.policy.allowed_metrics {
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

        let client = if let Some(ref endpoint) = config.otel_endpoint {
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
                    .max_decoding_message_size(config.max_decoding_message_size)
                    .max_encoding_message_size(config.max_decoding_message_size),
            )
        } else {
            None
        };

        let attributes = Self::build_and_validate_attributes(&config)?;

        Ok(Self {
            exact_metrics,
            prefix_metrics,
            attributes,
            client,
            disable_filtering: config.disable_filtering,
        })
    }

    /// Builds and validates the unified attributes collection from configuration.
    fn build_and_validate_attributes(
        config: &IsolateMetricsReceiverConfig,
    ) -> Result<HashMap<CustomAttribute, KeyValue>> {
        let mut attributes = HashMap::new();
        let standard_attrs = get_isolate_attribute_data(
            &config.isolate_name,
            &config.publisher_id,
            config.is_ratified,
            &config.isolate_instance_id,
        );
        let standard_resource_keys: HashSet<AttributeName> =
            standard_attrs.keys().map(|attr| attr.name().clone()).collect();

        for (attr, v) in standard_attrs {
            let kv = make_string_attribute(attr.name().as_str(), v);
            attributes.insert(attr, kv);
        }

        let mut custom_resource_keys = HashSet::new();
        let mut custom_scope_keys = HashSet::new();

        for (attr, val) in &config.custom_attributes {
            let key = attr.name();
            match attr {
                CustomAttribute::Resource(_) => {
                    if standard_resource_keys.contains(key) {
                        return Err(MetricsReceiverError::ConfigurationError(format!(
                            "Duplicate resource attribute key: '{key}'"
                        ))
                        .into());
                    }
                    custom_resource_keys.insert(key);
                }
                CustomAttribute::Scope(_) => {
                    if standard_resource_keys.contains(key) {
                        return Err(MetricsReceiverError::ConfigurationError(format!(
                            "Attribute key '{key}' is defined in both resource and scope attributes"
                        ))
                        .into());
                    }
                    custom_scope_keys.insert(key);
                }
            }
            let kv = KeyValue {
                key: key.to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(val.clone()),
                }),
            };
            attributes.insert(attr.clone(), kv);
        }

        for scope_key in &custom_scope_keys {
            if custom_resource_keys.contains(scope_key) {
                return Err(MetricsReceiverError::ConfigurationError(format!(
                    "Attribute key '{scope_key}' is defined in both resource and scope attributes"
                ))
                .into());
            }
        }

        Ok(attributes)
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

    /// Enriches metrics by adding isolate resource attributes (ez_isolate_name,
    /// ez_publisher_id, ez_isolate_instance_id, ez_component_name, ez_isolate_type,
    /// ez_enforcer_version) to resource.attributes and scope attributes to
    /// scope.attributes, while purging any client-spoofed identity attributes.
    pub fn enrich_metrics(&self, request: &mut ExportMetricsServiceRequest) {
        let identity_keys: HashSet<&str> =
            self.attributes.keys().map(|attr| attr.name().as_str()).collect();
        let is_identity_attr = |key: &str| identity_keys.contains(key);

        let resource_attrs = self.resource_attributes();
        let scope_attrs = self.scope_attributes();

        for resource_metrics in &mut request.resource_metrics {
            let resource = resource_metrics.resource.get_or_insert_with(Default::default);
            resource.attributes.retain(|attr| !is_identity_attr(&attr.key));
            if !resource_attrs.is_empty() {
                resource.attributes.extend_from_slice(&resource_attrs);
            }

            for scope_metrics in &mut resource_metrics.scope_metrics {
                let scope = scope_metrics.scope.get_or_insert_with(Default::default);
                scope.attributes.retain(|attr| !is_identity_attr(&attr.key));
                if !scope_attrs.is_empty() {
                    scope.attributes.extend_from_slice(&scope_attrs);
                }
            }
        }
    }

    /// Returns all attributes (Resource and Scope) configured for this receiver.
    pub fn attributes(&self) -> &HashMap<CustomAttribute, KeyValue> {
        &self.attributes
    }

    /// Returns the enriched resource attributes configured for this receiver.
    pub fn resource_attributes(&self) -> Vec<KeyValue> {
        self.attributes
            .iter()
            .filter_map(|(attr, kv)| match attr {
                CustomAttribute::Resource(_) => Some(kv.clone()),
                _ => None,
            })
            .collect()
    }

    /// Returns the scope attributes configured for this receiver.
    ///
    /// This is empty by default because all isolate metadata is attached at the
    /// resource level. Support for scope attributes is preserved for future
    /// instrumentation scope metadata.
    pub fn scope_attributes(&self) -> Vec<KeyValue> {
        self.attributes
            .iter()
            .filter_map(|(attr, kv)| match attr {
                CustomAttribute::Scope(_) => Some(kv.clone()),
                _ => None,
            })
            .collect()
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

    /// Sanitizes the attributes vector by removing forbidden keys.
    fn sanitize_attributes(attributes: &mut Vec<KeyValue>, allowed_keys: &HashSet<String>) {
        attributes.retain(|kv| allowed_keys.contains(&kv.key));
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
            MetricsReceiverError::ClientConnectionError(e).to_tonic_status()
        })?;

        client.export(metrics_request).await
    }
}

/// Helper to create a KeyValue attribute with a string value.
fn make_string_attribute(key: impl Into<String>, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                value.into(),
            )),
        }),
    }
}

/// Returns the standard isolate resource attributes (`ez_isolate_name`, `ez_publisher_id`,
/// `ez_isolate_instance_id`, `ez_component_name`, `ez_isolate_type`, `ez_enforcer_version`)
/// representing the isolate entity in Monarch root labels.
pub fn get_isolate_attribute_data(
    isolate_name: &str,
    publisher_id: &str,
    is_ratified: bool,
    isolate_instance_id: &str,
) -> HashMap<CustomAttribute, std::borrow::Cow<'static, str>> {
    let isolate_type_str = if is_ratified { "ratified" } else { "opaque" };
    HashMap::from([
        (CustomAttribute::Resource("ez_isolate_name".into()), isolate_name.to_string().into()),
        (CustomAttribute::Resource("ez_publisher_id".into()), publisher_id.to_string().into()),
        (
            CustomAttribute::Resource("ez_isolate_instance_id".into()),
            isolate_instance_id.to_string().into(),
        ),
        (CustomAttribute::Resource("ez_component_name".into()), "isolate".into()),
        (CustomAttribute::Resource("ez_isolate_type".into()), isolate_type_str.into()),
        (
            CustomAttribute::Resource("ez_enforcer_version".into()),
            crate::get_enforcer_version().into(),
        ),
    ])
}

#[derive(Debug)]
pub enum MetricsReceiverError {
    ClientConnectionError(anyhow::Error),
    ConfigurationError(String),
}

impl std::fmt::Display for MetricsReceiverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricsReceiverError::ClientConnectionError(e) => {
                write!(f, "Failed to connect to safe metrics endpoint: {:?}", e)
            }
            MetricsReceiverError::ConfigurationError(e) => {
                write!(f, "Configuration error: {}", e)
            }
        }
    }
}

impl std::error::Error for MetricsReceiverError {}

impl ToEzError for MetricsReceiverError {
    fn to_ez_error(&self) -> ez_error::EzError {
        let code = match self {
            MetricsReceiverError::ClientConnectionError(_) => tonic::Code::Internal,
            MetricsReceiverError::ConfigurationError(_) => tonic::Code::InvalidArgument,
        };
        let source = error_detail_proto::enforcer::v1::ez_error_detail::ErrorSource::Enforcer;

        let enforcer_error = ez_error::EnforcerError {
            message: self.to_string(),
            error_code: code,
            source,
            error_reason: None,
            bad_request: None,
        };
        ez_error::EzError::EnforcerError(enforcer_error)
    }
}
