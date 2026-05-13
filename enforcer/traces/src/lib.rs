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
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::{trace, Resource};
use tracing_subscriber::{prelude::*, EnvFilter, Registry};

pub const ENFORCER_SERVICE_NAME: &str = "enforcer";

async fn init_tracer(
    service_name: &str,
    endpoint: &str,
    sampler_probability: f64,
) -> Result<trace::SdkTracerProvider, trace::TraceError> {
    let mut exporter_builder = SpanExporter::builder().with_tonic();

    if endpoint.starts_with("unix:") {
        let channel_pool = GrpcChannelPool::new(
            endpoint.to_string(),
            DEFAULT_POOL_SIZE,
            DEFAULT_CONNECT_RETRY_COUNT,
            DEFAULT_CONNECT_RETRY_DELAY_MS,
            DEFAULT_CONNECT_RETRY_SCALING,
        )
        .await
        .map_err(|e| trace::TraceError::Other(e.into()))?;
        let channel = channel_pool.next_channel();

        exporter_builder = exporter_builder.with_channel(channel);
    } else {
        exporter_builder = exporter_builder.with_endpoint(endpoint);
    }
    let exporter = exporter_builder.build().map_err(|e| trace::TraceError::Other(e.into()))?;

    let resource = Resource::builder()
        .with_attributes(vec![KeyValue::new("service.name", service_name.to_string())])
        .build();

    let sampler = opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(sampler_probability);

    let provider = trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(sampler)))
        .build();

    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(provider.clone());

    Ok(provider)
}

pub async fn setup_telemetry(
    service_name: &str,
    endpoint: &Option<String>,
    console_subscriber_port: &Option<u16>,
    sampler_probability: f64,
) -> anyhow::Result<trace::SdkTracerProvider> {
    // 1. Initialize OpenTelemetry tracing IF an endpoint is provided.
    let (telemetry_layer, tracer_provider) = if let Some(endpoint) = endpoint {
        let tracer_provider = init_tracer(service_name, endpoint, sampler_probability).await?;
        let tracer = tracer_provider.tracer(service_name.to_string());
        let filter = EnvFilter::new("debug,h2=info");
        let layer = tracing_opentelemetry::layer().with_tracer(tracer).with_filter(filter);
        (Some(layer), tracer_provider)
    } else {
        (None, trace::SdkTracerProvider::builder().build())
    };

    // 2. Initialize console_subscriber IF a port is provided, independent of OpenTelemetry.
    // This starts the gRPC server listening on localhost for tokio-console connections.
    let console_layer = console_subscriber_port.as_ref().map(|port| {
        let (layer, server) = console_subscriber::ConsoleLayer::builder()
            .server_addr((std::net::Ipv6Addr::LOCALHOST, *port))
            .build();
        let port = *port;
        tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                tracing::error!("Console subscriber failed to start on port {}: {}", port, e);
            }
        });
        layer
    });

    // 3. Register whatever layers were successfully configured.
    // Registry::default() builds the base subscriber. We optionally add our layers to it.
    let subscriber = Registry::default().with(telemetry_layer).with(console_layer);

    // 4. Set the global tracing subscriber.
    // We ignore errors here because `set_global_default` will fail if it's called
    // multiple times in the same process, which happens frequently during unit tests.
    let _ = tracing::subscriber::set_global_default(subscriber);

    Ok(tracer_provider)
}
