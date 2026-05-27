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

use crate::common;
use crate::global;
use opentelemetry::metrics::{Counter, Gauge, Meter};

#[derive(Clone, Debug)]
pub struct HealthManagerMetrics {
    pub state: Gauge<i64>,
    pub container_run_status: Gauge<i64>,
    pub reset: Counter<u64>,
    pub system_fd_count: Gauge<i64>,
    pub system_memory_rss: Gauge<i64>,
    pub system_cpu_percent: Gauge<f64>,
}

impl HealthManagerMetrics {
    pub fn new() -> Self {
        Self::from_meter(global::safe_meter(crate::meter_name!()))
    }

    pub fn from_meter(meter: Meter) -> Self {
        let state = meter
            .i64_gauge(crate::metric_name!("health.isolate.state"))
            .with_description("The state should be one of the IsolateState values (e.g. READY).")
            .build();

        let reset = common::build_reset_counter(&meter);

        let system_fd_count = meter
            .i64_gauge(crate::metric_name!("health.system.file_descriptors"))
            .with_description("Number of file descriptors currently open by the enforcer process.")
            .build();

        let system_memory_rss = meter
            .i64_gauge(crate::metric_name!("health.system.memory_rss"))
            .with_description(
                "Resident Set Size (RSS) memory currently used by the enforcer process.",
            )
            .build();

        let system_cpu_percent = meter
            .f64_gauge(crate::metric_name!("health.system.cpu.usage"))
            .with_description("CPU usage of the enforcer process.")
            .build();

        let container_run_status = meter
            .i64_gauge(crate::metric_name!("health.isolate.container_run_status"))
            .with_description("The container run status (e.g. RUNNING, EXITED, etc).")
            .build();

        Self {
            state,
            container_run_status,
            reset,
            system_fd_count,
            system_memory_rss,
            system_cpu_percent,
        }
    }
}

impl Default for HealthManagerMetrics {
    fn default() -> Self {
        Self::new()
    }
}
