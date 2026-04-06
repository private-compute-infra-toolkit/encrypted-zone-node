// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::common::{self, ServiceMetrics};
use crate::global;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

#[derive(Clone, Debug)]
pub struct ExternalProxyConnectorMetrics {
    pub requests: Counter<u64>,
    pub duration_sec: Histogram<f64>,
    pub errors: Counter<u64>,
    pub active_requests: UpDownCounter<i64>,
    pub message_size_bytes: Histogram<u64>,
    pub message_processing_duration: Histogram<f64>,
}

impl ExternalProxyConnectorMetrics {
    pub fn new() -> Self {
        Self::from_meters(
            global::safe_meter("enforcer.external"),
            global::unsafe_meter("enforcer.external"),
        )
    }

    pub fn from_meters(safe_meter: Meter, unsafe_meter: Meter) -> Self {
        let requests = safe_meter
            .u64_counter("enforcer.external.request")
            .with_description("Total requests forwarded to external proxy categorized by status")
            .build();

        let errors = safe_meter
            .u64_counter("enforcer.external.error")
            .with_description("Total errors encountered by proxy connector")
            .build();

        let active_requests = safe_meter
            .i64_up_down_counter("enforcer.external.active_requests")
            .with_description("Monitor concurrent external request load")
            .build();

        let duration_sec = unsafe_meter
            .f64_histogram("enforcer.external.request.duration")
            .with_boundaries(common::default_latency_boundaries())
            .with_description("Latency of round-trip call to external proxy for unary requests")
            .with_unit("s")
            .build();

        let message_size_bytes = unsafe_meter
            .u64_histogram("enforcer.external.message_size")
            .with_description("Size of individual messages")
            .with_unit("By")
            .build();

        let message_processing_duration = unsafe_meter
            .f64_histogram("enforcer.external.message.processing_duration")
            .with_boundaries(common::default_latency_boundaries())
            .with_description("Time spent processing external stream message")
            .with_unit("s")
            .build();

        Self {
            requests,
            duration_sec,
            errors,
            active_requests,
            message_size_bytes,
            message_processing_duration,
        }
    }
}

impl Default for ExternalProxyConnectorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceMetrics for ExternalProxyConnectorMetrics {
    fn requests(&self) -> Option<&Counter<u64>> {
        Some(&self.requests)
    }

    fn errors(&self) -> Option<&Counter<u64>> {
        Some(&self.errors)
    }

    fn active_requests(&self) -> Option<&UpDownCounter<i64>> {
        Some(&self.active_requests)
    }

    fn duration_sec(&self) -> Option<&Histogram<f64>> {
        Some(&self.duration_sec)
    }

    fn message_processing_duration(&self) -> Option<&Histogram<f64>> {
        Some(&self.message_processing_duration)
    }

    fn message_size_bytes(&self) -> Option<&Histogram<u64>> {
        Some(&self.message_size_bytes)
    }
}
