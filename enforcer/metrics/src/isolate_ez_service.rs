// Copyright 2025 Google LLC
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

use crate::common::ServiceMetrics;
use crate::global;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

#[derive(Clone, Debug)]
pub struct IsolateEzServiceMetrics {
    pub requests: Counter<u64>,
    pub duration_sec: Histogram<f64>,
    pub errors: Counter<u64>,
    pub active_requests: UpDownCounter<i64>,
    pub message_processing_duration: Histogram<f64>,
    pub message_size_bytes: Histogram<u64>,
}

impl IsolateEzServiceMetrics {
    pub fn new() -> Self {
        Self::from_meters(
            global::safe_meter("enforcer.isolate_ez_service"),
            global::unsafe_meter("enforcer.isolate_ez_service"),
        )
    }

    pub fn from_meters(safe_meter: Meter, unsafe_meter: Meter) -> Self {
        let requests = safe_meter
            .u64_counter("enforcer.isolate_ez_service.request")
            .with_description("Total number of requests from an isolate to the enforcer.")
            .build();

        let duration_sec = unsafe_meter
            .f64_histogram("enforcer.isolate_ez_service.request.duration")
            .with_description("Latency of unary and streaming calls handled by the Isolate Bridge.")
            .with_unit("s")
            .build();

        let errors = safe_meter
            .u64_counter("enforcer.isolate_ez_service.error")
            .with_description("Total number of failed requests in the Isolate Bridge.")
            .build();

        let active_requests = unsafe_meter
            .i64_up_down_counter("enforcer.isolate_ez_service.active_requests")
            .with_description("Monitor concurrent request load (unary and streaming).")
            .build();

        let message_processing_duration = unsafe_meter
            .f64_histogram("enforcer.isolate_ez_service.message.processing_duration")
            .with_description("Time spent processing stream message.")
            .with_unit("s")
            .build();

        let message_size_bytes = unsafe_meter
            .u64_histogram("enforcer.isolate_ez_service.message_size")
            .with_description("Size of messages (requests and responses) in bytes.")
            .with_unit("By")
            .build();

        Self {
            requests,
            duration_sec,
            errors,
            active_requests,
            message_processing_duration,
            message_size_bytes,
        }
    }
}

impl Default for IsolateEzServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceMetrics for IsolateEzServiceMetrics {
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
