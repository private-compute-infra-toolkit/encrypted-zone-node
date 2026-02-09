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

use container::ContainerRunStatus as ContainerRunStatusEnum;
use container_manager_request::{GetRunStatusRequest, ResetIsolateRequest};
use container_manager_requester::ContainerManagerRequester;
use enforcer_proto::enforcer::v1::ez_isolate_health::container_run_status::Status as RunStatus;
use enforcer_proto::enforcer::v1::ez_isolate_health::ContainerRunStatus;
use enforcer_proto::enforcer::v1::{EzIsolateHealth, IsolateState};
use isolate_info::IsolateId;

#[derive(Clone, Debug)]
pub enum HealthOp {
    // Is the container running? If not, what is the exit code or signal?
    CheckContainerRunStatus,
}

impl HealthOp {
    pub async fn run(
        &self,
        isolate_id: IsolateId,
        health: &mut EzIsolateHealth,
        requester: &ContainerManagerRequester,
    ) {
        match self {
            HealthOp::CheckContainerRunStatus => {
                let status =
                    match requester.get_run_status(GetRunStatusRequest { isolate_id }).await {
                        Ok(resp) => resp.status,
                        Err(e) => {
                            log::error!("Failed to get container run status: {:?}", e);
                            return;
                        }
                    };
                let (run_status, exit_code, signal) = match status {
                    ContainerRunStatusEnum::Running => (RunStatus::Running, None, None),
                    ContainerRunStatusEnum::Exited(code) => (RunStatus::Exited, Some(code), None),
                    ContainerRunStatusEnum::Signaled(sig) => (RunStatus::Signaled, None, Some(sig)),
                    ContainerRunStatusEnum::NotFound => (RunStatus::NotFound, None, None),
                };
                health.container_run_status =
                    Some(ContainerRunStatus { status: run_status as i32, exit_code, signal });
                if run_status != RunStatus::Running {
                    log::info!(
                        "[HealthOp::CheckContainerRunStatus] Isolate {} is not running (status: {:?}). Requesting reset.",
                        isolate_id,
                        run_status
                    );
                    let _ = requester.reset_container(ResetIsolateRequest { isolate_id }).await;
                    health.container_reset_requested = true;
                }
            }
        }
    }
}

// Returns a list of HealthOps to run for the given IsolateState.
// Note, right now we only have one HealthOp, but more are coming.
pub fn get_ops_for_state(state: IsolateState) -> Vec<HealthOp> {
    match state {
        IsolateState::Starting | IsolateState::Ready => {
            vec![HealthOp::CheckContainerRunStatus]
        }
        _ => vec![],
    }
}
