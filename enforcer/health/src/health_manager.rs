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
use state_manager::IsolateStateManager;
use std::sync::Arc;
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
        log::info!("[Health Manager] Running at {}", start_time);
        let isolate_states = self.isolate_state_manager.get_all_isolate_states();
        let health_results = self.run_health_operations(isolate_states).await;
        // Acquire write lock and update the latest report.
        let report = EzIsolateHealthReport {
            isolates: health_results,
            start_timestamp: start_time,
            end_timestamp: to_timestamp(std::time::SystemTime::now()),
        };
        log::info!("[Health Manager] Report: {:#?}", report);
        *self.latest_report.write().await = report;
    }

    async fn run_health_operations(
        &self,
        isolate_states: Vec<(IsolateId, IsolateState)>,
    ) -> Vec<EzIsolateHealth> {
        log::info!("[Health Manager] Running health ops for {} Isolates", isolate_states.len());
        let mut join_set = JoinSet::new();
        for (isolate_id, state) in isolate_states {
            let ops = get_ops_for_state(state);
            let container_manager_requester = self.container_manager_requester.clone();
            let isolate_service_mapper = self.isolate_service_mapper.clone();
            let ds_requester = self.data_scope_requester.clone();
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
                        .iter()
                        .map(|s| IsolateServiceInfo {
                            operator_domain: s.operator_domain.clone(),
                            service_name: s.service_name.clone(),
                        })
                        .collect();
                }
                for op in ops {
                    op.run(isolate_id, &mut health, &container_manager_requester).await;
                }
                health
            });
        }
        let mut results = Vec::with_capacity(join_set.len());
        while let Some(res) = join_set.join_next().await {
            if let Ok(health) = res {
                results.push(health);
            }
        }
        results
    }

    pub async fn get_report(&self) -> EzIsolateHealthReport {
        self.latest_report.read().await.clone()
    }
}

fn to_timestamp(time: std::time::SystemTime) -> i64 {
    time.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}
