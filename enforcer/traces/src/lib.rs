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
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::{trace, Resource};
use tracing_subscriber::{prelude::*, EnvFilter, Registry};

async fn init_tracer(endpoint: &str) -> Result<trace::SdkTracerProvider, trace::TraceError> {
    let mut exporter_builder = SpanExporter::builder().with_tonic();

    if endpoint.starts_with("unix:") {
        let channel = grpc_connector::connect(
            endpoint.to_string(),
            grpc_connector::DEFAULT_CONNECT_RETRY_COUNT,
            grpc_connector::DEFAULT_CONNECT_RETRY_DELAY_MS,
        )
        .await
        .map_err(|e| trace::TraceError::Other(e.into()))?;

        exporter_builder = exporter_builder.with_channel(channel);
    } else {
        exporter_builder = exporter_builder.with_endpoint(endpoint);
    }
    let exporter = exporter_builder.build().map_err(|e| trace::TraceError::Other(e.into()))?;

    let resource = Resource::builder()
        .with_attributes(vec![KeyValue::new("service.name", "enforcer")])
        .build();

    let provider = trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(provider.clone());

    Ok(provider)
}

pub async fn setup_telemetry(
    endpoint: &Option<String>,
) -> anyhow::Result<trace::SdkTracerProvider> {
    if let Some(endpoint) = endpoint {
        let tracer_provider = init_tracer(endpoint).await?;
        let tracer = tracer_provider.tracer("enforcer");

        let filter = EnvFilter::new("debug,h2=info");
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = Registry::default().with(telemetry.with_filter(filter));

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        Ok(tracer_provider)
    } else {
        Ok(trace::SdkTracerProvider::builder().build())
    }
}
