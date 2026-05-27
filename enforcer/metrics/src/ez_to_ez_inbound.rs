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
pub struct EzToEzInboundMetrics {
    pub requests: Counter<u64>,
    pub duration_sec: Histogram<f64>,
    pub errors: Counter<u64>,
    pub active_requests: UpDownCounter<i64>,
    pub message_size_bytes: Histogram<u64>,
    pub message_processing_duration: Histogram<f64>,
}

impl EzToEzInboundMetrics {
    pub fn new() -> Self {
        Self::from_meters(
            global::safe_meter(crate::meter_name!()),
            global::unsafe_meter(crate::meter_name!()),
        )
    }

    pub fn from_meters(safe_meter: Meter, unsafe_meter: Meter) -> Self {
        // Safe Metrics
        let requests = safe_meter
            .u64_counter(crate::metric_name!("ez_to_ez.inbound.request"))
            .with_description("Total number of inbound EZ-to-EZ requests.")
            .build();

        let errors = safe_meter
            .u64_counter(crate::metric_name!("ez_to_ez.inbound.error"))
            .with_description("Total number of failed inbound EZ-to-EZ requests.")
            .build();

        let active_requests = safe_meter
            .i64_up_down_counter(crate::metric_name!("ez_to_ez.inbound.active_requests"))
            .with_description("Monitor concurrent request load (unary and streaming).")
            .build();

        let duration_sec = unsafe_meter
            .f64_histogram(crate::metric_name!("ez_to_ez.inbound.request.duration"))
            .with_boundaries(common::default_latency_boundaries())
            .with_description("Duration of unary and streaming inbound EZ-to-EZ requests.")
            .with_unit("s")
            .build();

        let message_size_bytes = unsafe_meter
            .u64_histogram(crate::metric_name!("ez_to_ez.inbound.message_size"))
            .with_description("Size of individual messages.")
            .with_unit("By")
            .build();

        let message_processing_duration = unsafe_meter
            .f64_histogram(crate::metric_name!("ez_to_ez.inbound.message.processing_duration"))
            .with_boundaries(common::default_latency_boundaries())
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

impl Default for EzToEzInboundMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceMetrics for EzToEzInboundMetrics {
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
