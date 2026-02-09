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
pub struct PublicApiMetrics {
    pub requests: Counter<u64>,
    pub duration_sec: Histogram<f64>,
    pub errors: Counter<u64>,
    pub active_requests: UpDownCounter<i64>,
    pub message_size_bytes: Histogram<u64>,
    pub message_processing_duration: Histogram<f64>,
}

impl PublicApiMetrics {
    pub fn new() -> Self {
        Self::from_meters(
            global::safe_meter("enforcer.public_api"),
            global::unsafe_meter("enforcer.public_api"),
        )
    }

    pub fn from_meters(safe_meter: Meter, _unsafe_meter: Meter) -> Self {
        // Safe Metrics
        let requests = safe_meter
            .u64_counter("enforcer.public_api.request")
            .with_description("Total number of public api requests.")
            .build();

        let errors = safe_meter
            .u64_counter("enforcer.public_api.error")
            .with_description("Total number of failed public API requests.")
            .build();

        let active_requests = safe_meter
            .i64_up_down_counter("enforcer.public_api.active_requests")
            .with_description("Monitor concurrent request load (unary and streaming).")
            .build();

        let duration_sec = safe_meter
            .f64_histogram("enforcer.public_api.request.duration")
            .with_description("Duration of unary and streaming requests to the public API.")
            .with_unit("s")
            .build();

        let message_size_bytes = safe_meter
            .u64_histogram("enforcer.public_api.message_size")
            .with_description("Size of individual messages.")
            .with_unit("By")
            .build();

        let message_processing_duration = safe_meter
            .f64_histogram("enforcer.public_api.message.processing_duration")
            .with_description("Time spent processing stream message.")
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

impl Default for PublicApiMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceMetrics for PublicApiMetrics {
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
