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
use opentelemetry::KeyValue;
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};

pub mod common;
pub mod global;
pub mod isolate_ez_service;
pub mod junction;
pub mod observed_stream;
pub mod public_api;

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

            let channel = grpc_connector::connect(
                endpoint,
                grpc_connector::DEFAULT_CONNECT_RETRY_COUNT,
                grpc_connector::DEFAULT_CONNECT_RETRY_DELAY_MS,
            )
            .await?;
            exporter_builder.with_channel(channel)
        } else {
            exporter_builder.with_endpoint(endpoint)
        }
        .build()?;

        Ok::<SdkMeterProvider, anyhow::Error>(
            SdkMeterProvider::builder()
                .with_resource(
                    Resource::builder()
                        .with_attribute(KeyValue::new("service.name", "ez-enforcer"))
                        .with_attribute(KeyValue::new("privacy.scope", provider_type))
                        .build(),
                )
                .with_periodic_exporter(exporter)
                .build(),
        )
    };

    // Setup Safe Provider
    let safe_provider = if let Some(endpoint) = safe_endpoint {
        let provider = create_provider(endpoint.clone(), "safe").await?;

        global::set_safe_meter_provider(provider.clone());

        log::info!("Safe OTel metrics pipeline initialized with endpoint: {}", endpoint);
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
