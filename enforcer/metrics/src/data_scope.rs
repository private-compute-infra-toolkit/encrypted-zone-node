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

use crate::common::{build_reset_counter, default_latency_boundaries};
use crate::global::safe_meter;
use opentelemetry::metrics::{Counter, Histogram};

#[derive(Clone, Debug)]
pub struct DataScopeMetrics {
    pub request: Counter<u64>,
    pub request_duration: Histogram<f64>,
    pub validation_duration: Histogram<f64>,
    pub request_error: Counter<u64>,
    pub sensitive: Counter<u64>,
    pub reset: Counter<u64>,
    pub denial: Counter<u64>,
    pub scopes_dragged: Counter<u64>,
}

impl DataScopeMetrics {
    pub fn new() -> Self {
        let meter = safe_meter(crate::meter_name!());

        let request = meter
            .u64_counter(crate::metric_name!("data_scope.request"))
            .with_description("Total number of requests to the DSM.")
            .build();

        let request_duration = meter
            .f64_histogram(crate::metric_name!("data_scope.request.duration"))
            .with_description("Latency for DSM operations.")
            .with_boundaries(default_latency_boundaries())
            .build();

        let validation_duration = meter
            .f64_histogram(crate::metric_name!("data_scope.validation.request.duration"))
            .with_description("Latency for data scope validation checks.")
            .with_boundaries(default_latency_boundaries())
            .build();

        let request_error = meter
            .u64_counter(crate::metric_name!("data_scope.request.error"))
            .with_description("Total number of errors from DSM.")
            .build();

        let sensitive = meter
            .u64_counter(crate::metric_name!("data_scope.sensitive"))
            .with_description("Count of sensitive sessions handled.")
            .build();

        let reset = build_reset_counter(&meter);

        let denial = meter
            .u64_counter(crate::metric_name!("data_scope.denial"))
            .with_description("Count of requests denied by DSM.")
            .build();

        let scopes_dragged = meter
            .u64_counter(crate::metric_name!("data_scope.scopes_dragged"))
            .with_description("Count of data scopes dynamically dragged up.")
            .build();

        Self {
            request,
            request_duration,
            validation_duration,
            request_error,
            sensitive,
            reset,
            denial,
            scopes_dragged,
        }
    }
}

impl Default for DataScopeMetrics {
    fn default() -> Self {
        Self::new()
    }
}
