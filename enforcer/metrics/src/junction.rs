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

use crate::common::ServiceMetrics;
use crate::global;

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

#[derive(Clone, Debug)]
pub struct JunctionMetrics {
    pub requests: Counter<u64>,
    pub duration_sec: Histogram<f64>,
    pub isolate_rpc_duration_sec: Histogram<f64>,
    pub request_error: Counter<u64>,
    pub active_requests: UpDownCounter<i64>,
    pub message_processing_duration: Histogram<f64>,
}

impl JunctionMetrics {
    pub fn new() -> Self {
        Self::from_meters(
            global::safe_meter("enforcer.junction"),
            global::unsafe_meter("enforcer.junction"),
        )
    }

    pub fn from_meters(safe_meter: Meter, unsafe_meter: Meter) -> Self {
        let requests = safe_meter
            .u64_counter("enforcer.junction.request")
            .with_description("Total number of requests to the junction.")
            .build();
        let duration_sec = unsafe_meter
            .f64_histogram("enforcer.junction.request.duration")
            .with_description("Latency of invoke_isolate and streaming_invoke_isolate calls.")
            .with_unit("s")
            .build();
        let isolate_rpc_duration_sec = unsafe_meter
            .f64_histogram("enforcer.junction.isolate_rpc.duration")
            .with_description("Latency of underlying unary rpc call to isolate.")
            .with_unit("s")
            .build();
        let request_error = safe_meter
            .u64_counter("enforcer.junction.request.error")
            .with_description("Total number of errors from IsolateJunction.")
            .build();
        let active_requests = unsafe_meter
            .i64_up_down_counter("enforcer.junction.active_requests")
            .with_description("Monitor concurrent request load (unary and streaming).")
            .build();
        let message_processing_duration = unsafe_meter
            .f64_histogram("enforcer.junction.message.processing_duration")
            .with_description("Time spent processing stream message.")
            .with_unit("s")
            .build();
        Self {
            requests,
            duration_sec,
            isolate_rpc_duration_sec,
            request_error,
            active_requests,
            message_processing_duration,
        }
    }
}

impl Default for JunctionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceMetrics for JunctionMetrics {
    fn requests(&self) -> Option<&Counter<u64>> {
        Some(&self.requests)
    }

    fn errors(&self) -> Option<&Counter<u64>> {
        Some(&self.request_error)
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
}
