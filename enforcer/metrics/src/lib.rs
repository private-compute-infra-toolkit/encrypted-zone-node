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

use anyhow::Result;
use grpc_connector::{
    GrpcChannelPool, DEFAULT_CONNECT_RETRY_COUNT, DEFAULT_CONNECT_RETRY_DELAY_MS,
    DEFAULT_CONNECT_RETRY_SCALING, DEFAULT_POOL_SIZE,
};
use opentelemetry::metrics::MeterProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};

pub mod common;
pub mod data_scope;
pub mod external_proxy_connector;
pub mod ez_to_ez_inbound;
pub mod global;
pub mod health_manager;
pub mod isolate_ez_service;
pub mod junction;
pub mod observed_proxy_channel;
pub mod observed_stream;
pub mod outbound;
pub mod public_api;
pub mod receiver;

/// Holds the providers so they can be kept alive or shut down gracefully.
pub struct OtelProviders {
    pub safe: Option<SdkMeterProvider>,
    pub unsafe_metrics: Option<SdkMeterProvider>,
}

// Initializes the OpenTelemetry metrics pipeline and sets the global meter providers.
// Takes optional endpoints for safe and unsafe pipelines.
pub async fn setup_otel_metrics(
    safe_endpoint: Option<String>,
    unsafe_endpoint: Option<String>,
) -> Result<OtelProviders> {
    let create_provider = |endpoint: String, provider_type: &'static str| async move {
        let exporter_builder = opentelemetry_otlp::MetricExporter::builder().with_tonic();

        let exporter = if let Some(path) = endpoint.strip_prefix("unix:") {
            log::info!("Waiting for OTel collector socket at {}...", path);

            let channel_pool = GrpcChannelPool::new(
                endpoint.to_string(),
                DEFAULT_POOL_SIZE,
                DEFAULT_CONNECT_RETRY_COUNT,
                DEFAULT_CONNECT_RETRY_DELAY_MS,
                DEFAULT_CONNECT_RETRY_SCALING,
            )
            .await?;
            let channel = channel_pool.next_channel();
            exporter_builder.with_channel(channel)
        } else {
            exporter_builder.with_endpoint(endpoint)
        }
        .build()?;

        let resource = Resource::builder()
            .with_attribute(KeyValue::new("service.name", "ez-enforcer"))
            .with_attribute(KeyValue::new("privacy.scope", provider_type))
            .with_attribute(KeyValue::new("ez_component_name", "enforcer"))
            .with_attribute(KeyValue::new("ez_isolate_name", "null_isolate"))
            .with_attribute(KeyValue::new("ez_publisher_id", "null_publisher"))
            .with_attribute(KeyValue::new("ez_isolate_type", "null_type"))
            .with_attribute(KeyValue::new(
                "ez_enforcer_version",
                option_env!("CARGO_PKG_VERSION").unwrap_or("unknown"),
            ))
            .build();

        Ok::<SdkMeterProvider, anyhow::Error>(
            SdkMeterProvider::builder()
                .with_resource(resource)
                .with_periodic_exporter(exporter)
                .build(),
        )
    };

    // Setup Safe Provider
    let safe_provider = if let Some(endpoint) = safe_endpoint {
        let provider = create_provider(endpoint.clone(), "safe").await?;

        global::set_safe_meter_provider(provider.clone());

        log::info!("Safe OTel metrics pipeline initialized with endpoint: {}", endpoint);

        // Record initialization metric to track enforcer starts.
        // This metric should be visible only if we're not in tests.
        if std::env::var("TEST_WORKSPACE").is_err() {
            record_initialization_metric(&provider);
        }

        Some(provider)
    } else {
        log::info!("No Safe OTel endpoint provided. Safe metrics will be No-Op.");
        None
    };

    // Setup Unsafe Provider
    let unsafe_provider = if let Some(endpoint) = unsafe_endpoint {
        let provider = create_provider(endpoint.clone(), "unsafe").await?;

        global::set_unsafe_meter_provider(provider.clone());

        log::info!("Unsafe OTel metrics pipeline initialized with endpoint: {}", endpoint);
        Some(provider)
    } else {
        log::debug!("No Unsafe OTel endpoint provided. Unsafe metrics will be No-Op.");
        None
    };

    Ok(OtelProviders { safe: safe_provider, unsafe_metrics: unsafe_provider })
}

/// Records an initialization metric to the safe OTel collector.
pub fn record_initialization_metric(provider: &SdkMeterProvider) {
    let meter = provider.meter(crate::meter_name!());
    let counter = meter.u64_counter(crate::metric_name!("init_metric")).build();
    counter.add(1, &[]);

    // Force flush to ensure the metric is sent immediately
    if let Err(e) = provider.force_flush() {
        log::warn!("Failed to flush initial metric: {:?}", e);
    } else {
        log::info!("Initialization metric 'enforcer_init_metric' sent to safe OTel collector.");
    }
}
