// Copyright 2026 Google LLC
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

use console_helper::ConsoleHelperService;
use diagnostics_proto::enforcer::diagnostics::v1::{
    TaskStat, TokioProfileResult, TokioProfileSpec,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tonic::Status;

#[derive(Clone)]
pub struct TokioDiagnosticService {
    console_helper: Arc<ConsoleHelperService>,
}

impl TokioDiagnosticService {
    pub fn new(console_helper: Arc<ConsoleHelperService>) -> Self {
        Self { console_helper }
    }

    pub async fn get_summary(&self, spec: &TokioProfileSpec) -> Result<TokioProfileResult, Status> {
        let duration = spec
            .duration
            .as_ref()
            .map(|d| std::time::Duration::new(d.seconds as u64, d.nanos as u32))
            .unwrap_or(std::time::Duration::from_secs(1));
        if spec.top_n == 0 {
            return Err(Status::invalid_argument("top_n must be greater than 0"));
        }
        let top_n = spec.top_n as usize;
        let filter = &spec.location_filter;

        let stream = self.console_helper.task_update_stream();
        tokio::pin!(stream);

        let task_metrics = self.collect_metrics(stream, duration, filter).await;

        let top_busy = self.extract_top_busy(&task_metrics, top_n);
        let top_delay = self.extract_top_delay(&task_metrics, top_n);

        Ok(TokioProfileResult {
            text_summary: "Tokio task statistics collected".to_string(),
            top_busy,
            top_delay,
        })
    }

    async fn collect_metrics(
        &self,
        mut stream: impl StreamExt<Item = console_helper::FormattedTaskUpdate> + Unpin,
        duration: std::time::Duration,
        filter: &str,
    ) -> HashMap<u64, (String, std::time::Duration, std::time::Duration)> {
        let mut task_metrics = HashMap::new();
        let start = std::time::Instant::now();

        while start.elapsed() < duration {
            if let Some(update) = stream.next().await {
                if !filter.is_empty() && !update.location.contains(filter) {
                    continue;
                }

                let entry = task_metrics.entry(update.task_id).or_insert((
                    update.location,
                    std::time::Duration::default(),
                    std::time::Duration::default(),
                ));

                entry.1 += update.busy_time;
                if update.scheduled_delay > entry.2 {
                    entry.2 = update.scheduled_delay;
                }
            } else {
                break;
            }
        }
        task_metrics
    }

    fn extract_top_busy(
        &self,
        task_metrics: &HashMap<u64, (String, std::time::Duration, std::time::Duration)>,
        top_n: usize,
    ) -> Vec<TaskStat> {
        let mut busy_vec: Vec<_> = task_metrics.iter().collect();
        busy_vec.sort_by_key(|a| std::cmp::Reverse((a.1).1));
        busy_vec
            .into_iter()
            .take(top_n)
            .map(|(id, (loc, busy, _))| TaskStat {
                task_id: *id,
                duration: Some(diagnostics_proto::duration_proto::google::protobuf::Duration {
                    seconds: busy.as_secs() as i64,
                    nanos: busy.subsec_nanos() as i32,
                }),
                location: loc.clone(),
            })
            .collect()
    }

    fn extract_top_delay(
        &self,
        task_metrics: &HashMap<u64, (String, std::time::Duration, std::time::Duration)>,
        top_n: usize,
    ) -> Vec<TaskStat> {
        let mut delay_vec: Vec<_> = task_metrics.iter().collect();
        delay_vec.sort_by_key(|a| std::cmp::Reverse((a.1).2));
        delay_vec
            .into_iter()
            .take(top_n)
            .map(|(id, (loc, _, delay))| TaskStat {
                task_id: *id,
                duration: Some(diagnostics_proto::duration_proto::google::protobuf::Duration {
                    seconds: delay.as_secs() as i64,
                    nanos: delay.subsec_nanos() as i32,
                }),
                location: loc.clone(),
            })
            .collect()
    }
}
