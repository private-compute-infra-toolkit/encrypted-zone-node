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

use container_manager_requester::ContainerManagerRequester;
use data_scope::request::GetIsolateScopeRequest;
use data_scope::requester::DataScopeRequester;
use enforcer_proto::enforcer::v1::{
    EzIsolateHealth, EzIsolateHealthReport, IsolateServiceInfo, IsolateState,
};
use health_ops::get_ops_for_state;
use isolate_info::IsolateId;
use isolate_service_mapper::IsolateServiceMapper;
use opentelemetry::KeyValue;
use state_manager::IsolateStateManager;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::{self, Duration, MissedTickBehavior};

#[derive(Debug, Clone)]
pub struct HealthManager {
    latest_report: Arc<RwLock<EzIsolateHealthReport>>,
    container_manager_requester: ContainerManagerRequester,
    isolate_state_manager: IsolateStateManager,
    isolate_service_mapper: IsolateServiceMapper,
    data_scope_requester: DataScopeRequester,
    metrics: Arc<metrics::health_manager::HealthManagerMetrics>,
    last_cpu_sample: Arc<std::sync::Mutex<Option<(u64, u64)>>>,
    isolate_attributes_cache: Arc<Mutex<HashMap<IsolateId, Vec<KeyValue>>>>,
}

impl HealthManager {
    pub fn new(
        isolate_state_manager: IsolateStateManager,
        container_manager_requester: ContainerManagerRequester,
        isolate_service_mapper: IsolateServiceMapper,
        data_scope_requester: DataScopeRequester,
    ) -> Self {
        Self {
            latest_report: Arc::new(RwLock::new(EzIsolateHealthReport::default())),
            container_manager_requester,
            isolate_state_manager,
            isolate_service_mapper,
            data_scope_requester,
            metrics: Arc::new(metrics::health_manager::HealthManagerMetrics::new()),
            last_cpu_sample: Arc::new(std::sync::Mutex::new(None)),
            isolate_attributes_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl HealthManager {
    pub fn run_in_background(&self, interval: Duration) {
        let health_manager = self.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                health_manager.run().await;
            }
        });
    }

    pub async fn run(&self) {
        let start_time = to_timestamp(std::time::SystemTime::now());
        log::debug!("[Health Manager] Running at {}", start_time);
        let isolate_states = self.isolate_state_manager.get_all_isolate_states();
        let health_results = self.run_health_operations(isolate_states).await;
        self.update_health_metrics(&health_results);

        let isolates_report: Vec<EzIsolateHealth> =
            health_results.into_iter().map(|(_, h)| h).collect();
        // Acquire write lock and update the latest report.
        let report = EzIsolateHealthReport {
            isolates: isolates_report,
            start_timestamp: start_time,
            end_timestamp: to_timestamp(std::time::SystemTime::now()),
        };
        let mut latest = self.latest_report.write().await;
        if latest.isolates != report.isolates {
            log::info!("[Health Manager] Report: {:#?}", report);
        } else {
            log::info!("[Health Manager] Report: No changes");
        }
        *latest = report;
    }

    fn update_health_metrics(&self, isolates: &[(IsolateId, EzIsolateHealth)]) {
        let mut cache = match self.isolate_attributes_cache.lock() {
            Ok(lock) => lock,
            Err(e) => {
                log::error!("[Health Manager] Attribute cache lock poisoned: {:?}", e);
                return;
            }
        };

        let active_ids: HashSet<_> = isolates.iter().map(|(id, _)| *id).collect();
        cache.retain(|id, _| active_ids.contains(id));
        for (isolate_id, isolate) in isolates {
            let state_val = isolate.state.unwrap_or(0) as i64;
            let container_run_status_val =
                isolate.container_run_status.as_ref().map(|s| s.status).unwrap_or(0) as i64;
            let container_rss_memory_val = isolate.container_rss_memory_bytes;
            let container_peak_rss_memory_val = isolate.container_peak_rss_memory_bytes;
            let container_virt_memory_val = isolate.container_virt_memory_bytes;
            let container_shared_memory_val = isolate.container_shared_memory_bytes;
            let container_data_memory_val = isolate.container_data_memory_bytes;

            let attributes = cache.entry(*isolate_id).or_insert_with(|| {
                let binary_services_index = isolate_id.get_binary_services_index();
                let isolate_type_opt = isolate_info::get_isolate_type(&binary_services_index);
                let (isolate_name, publisher_id) = isolate_type_opt
                    .as_ref()
                    .map(|it| (it.isolate_name.as_str(), it.publisher_id.as_str()))
                    .unwrap_or(("unknown", "unknown"));
                let is_ratified = isolate_id.is_ratified_isolate();
                build_health_isolate_attributes(isolate_name, publisher_id, is_ratified)
            });

            self.metrics.state.record(state_val, attributes);
            self.metrics.container_run_status.record(container_run_status_val, attributes);
            if let Some(mem_val) = container_rss_memory_val {
                self.metrics.container_rss_memory.record(mem_val, attributes);
            }
            if let Some(mem_val) = container_peak_rss_memory_val {
                self.metrics.container_peak_rss_memory.record(mem_val, attributes);
            }
            if let Some(mem_val) = container_virt_memory_val {
                self.metrics.container_virt_memory.record(mem_val, attributes);
            }
            if let (Some(rss), Some(shared)) =
                (container_rss_memory_val, container_shared_memory_val)
            {
                let private_mem = rss.saturating_sub(shared);
                self.metrics.container_private_memory.record(private_mem, attributes);
            }
            if let Some(mem_val) = container_data_memory_val {
                self.metrics.container_data_memory.record(mem_val, attributes);
            }
        }

        if let Some(fd_count) = system_metrics::get_system_fd_count() {
            self.metrics.system_fd_count.record(fd_count, &[]);
        }

        if let Some(rss_bytes) = system_metrics::get_system_memory_rss_bytes() {
            self.metrics.system_memory_rss.record(rss_bytes, &[]);
        }

        if let Some(current_ticks) = system_metrics::get_system_cpu_ticks() {
            let mut last_sample = self.last_cpu_sample.lock().unwrap();
            if let Some(cpu_usage) =
                system_metrics::calculate_cpu_usage(current_ticks, &mut last_sample)
            {
                self.metrics.system_cpu_percent.record(cpu_usage, &[]);
            }
        }
    }

    async fn run_health_operations(
        &self,
        isolate_states: Vec<(IsolateId, IsolateState)>,
    ) -> Vec<(IsolateId, EzIsolateHealth)> {
        log::debug!("[Health Manager] Running health ops for {} Isolates", isolate_states.len());
        let mut join_set = JoinSet::new();
        for (isolate_id, state) in isolate_states {
            let ops = get_ops_for_state(state);
            let container_manager_requester = self.container_manager_requester.clone();
            let isolate_service_mapper = self.isolate_service_mapper.clone();
            let ds_requester = self.data_scope_requester.clone();
            let metrics = self.metrics.clone();
            join_set.spawn(async move {
                let mut health = EzIsolateHealth {
                    isolate_id: isolate_id.to_string(),
                    state: Some(state as i32),
                    ..Default::default()
                };
                // Populate current_scope
                if let Ok(response) =
                    ds_requester.get_isolate_scope(GetIsolateScopeRequest { isolate_id }).await
                {
                    health.current_scope = Some(response.current_scope as i32);
                }

                // Populate operator_domain and service_name
                let binary_services_index = isolate_id.get_binary_services_index();
                let services_opt =
                    isolate_service_mapper.get_isolate_service_infos(&binary_services_index).await;
                if let Some(services) = services_opt {
                    health.services = services
                        .into_iter()
                        .map(|s| IsolateServiceInfo {
                            operator_domain: s.operator_domain,
                            service_name: s.service_name,
                            isolate_name: s.isolate_name,
                            publisher_id: s.publisher_id,
                        })
                        .collect();
                }
                for op in ops {
                    op.run(isolate_id, &mut health, &container_manager_requester, &metrics).await;
                }
                (isolate_id, health)
            });
        }
        let mut results = Vec::with_capacity(join_set.len());
        while let Some(res) = join_set.join_next().await {
            if let Ok(res_tuple) = res {
                results.push(res_tuple);
            }
        }
        results.sort_by_key(|a| a.0);
        results
    }

    pub async fn get_report(&self) -> EzIsolateHealthReport {
        self.latest_report.read().await.clone()
    }
}

fn to_timestamp(time: std::time::SystemTime) -> i64 {
    time.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn build_health_isolate_attributes(
    isolate_name: &str,
    publisher_id: &str,
    is_ratified: bool,
) -> Vec<KeyValue> {
    metrics::receiver::get_isolate_attribute_data(isolate_name, publisher_id, is_ratified)
        .into_iter()
        .map(|(k, v)| KeyValue::new(k, v))
        .collect()
}
