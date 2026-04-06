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

use console_api::instrument::instrument_client::InstrumentClient;
use console_api::instrument::InstrumentRequest;
use console_api::tasks::Stats;
use futures_core::Stream;
use std::pin::Pin;
use tonic::{Request, Response, Status};

// Use the generated crate directly

// Re-export the generated server trait so main.rs can use it
use console_helper_proto::enforcer::console_helper::console_helper_server::ConsoleHelper;
pub use console_helper_proto::enforcer::console_helper::console_helper_server::ConsoleHelperServer;
use console_helper_proto::enforcer::console_helper::{
    WatchChannelSaturationRequest, WatchChannelSaturationResponse, WatchResourceContentionRequest,
    WatchResourceContentionResponse, WatchTaskChurnRequest, WatchTaskChurnResponse,
    WatchTaskLatencyRequest, WatchTaskLatencyResponse, WatchTaskSizeRequest, WatchTaskSizeResponse,
    WatchThroughputRequest, WatchThroughputResponse,
};
use std::collections::HashMap;

/// Static information about tokio tasks, captured at the call site.
struct Metadata {
    /// The name assigned to the task (e.g., "enforcer_main").
    name: String,
    /// The Rust module path where the span is defined (e.g., "enforcer::container").
    target: String,
    /// The source file path.
    file: String,
    /// The line number in the source file.
    line: u32,
    /// The column number in the source file.
    column: u32,
}

/// TaskTracker manages the translation between raw numeric IDs from the
/// console-subscriber wire protocol and human-readable task information.
struct TaskTracker {
    /// Maps a Metadata ID (unique per static code location) to its details.
    /// Key: u64 Metadata ID provided by the console-subscriber.
    metadata_map: HashMap<u64, Metadata>,

    /// Maps a specific Task Instance ID to its corresponding Metadata ID.
    /// Key: u64 Task ID; Value: u64 Metadata ID.
    task_metadata_map: HashMap<u64, u64>,

    /// Maps a Task Instance ID to the string representation of its spawn location.
    /// Key: u64 Task ID; Value: String (e.g., "src/main.rs:42:10").
    task_spawn_loc_map: HashMap<u64, String>,

    /// Maps internal console-subscriber IDs to user-facing 'display' IDs.
    /// console-subscriber uses internal u64s for tracking, but the tracing
    /// ecosystem often assigns a different ID for display in logs.
    /// Key: u64 Internal ID; Value: u64 Display ID.
    task_ids_map: HashMap<u64, u64>,

    /// Maps a Task Instance ID to the location string where the task itself is defined.
    /// Key: u64 Task ID; Value: String (e.g., "enforcer/src/lib.rs:100").
    task_locations_map: HashMap<u64, String>,
}

impl TaskTracker {
    fn new() -> Self {
        Self {
            metadata_map: HashMap::new(),
            task_metadata_map: HashMap::new(),
            task_spawn_loc_map: HashMap::new(),
            task_ids_map: HashMap::new(),
            task_locations_map: HashMap::new(),
        }
    }

    fn update(&mut self, update: &console_api::instrument::Update) {
        if let Some(meta_update) = &update.new_metadata {
            for new_meta in &meta_update.metadata {
                let id = new_meta.id.map(|i| i.id).unwrap_or(0);
                if let Some(m) = &new_meta.metadata {
                    let file = m.location.as_ref().and_then(|l| l.file.clone()).unwrap_or_default();
                    let line = m.location.as_ref().and_then(|l| l.line).unwrap_or(0);
                    let column = m.location.as_ref().and_then(|l| l.column).unwrap_or(0);

                    self.metadata_map.insert(
                        id,
                        Metadata {
                            name: m.name.clone(),
                            target: m.target.clone(),
                            file,
                            line,
                            column,
                        },
                    );
                }
            }
        }

        if let Some(task_update) = &update.task_update {
            for task in &task_update.new_tasks {
                let task_id = task.id.as_ref().map(|id| id.id).unwrap_or(0);
                let meta_id = task.metadata.as_ref().map(|m| m.id).unwrap_or(0);
                self.task_metadata_map.insert(task_id, meta_id);

                if let Some(loc) = &task.location {
                    let file = loc.file.as_deref().unwrap_or("");
                    if !file.is_empty() {
                        let line = loc.line.unwrap_or(0);
                        let col = loc.column.unwrap_or(0);
                        let s = if col > 0 {
                            format!("{}:{}:{}", file, line, col)
                        } else {
                            format!("{}:{}", file, line)
                        };
                        self.task_locations_map.insert(task_id, s);
                    }
                }

                let mut spawn_location = String::new();

                for field in &task.fields {
                    if let Some(console_api::field::Name::StrName(name)) = &field.name {
                        if name == "task.id" {
                            match &field.value {
                                Some(console_api::field::Value::U64Val(u)) => {
                                    self.task_ids_map.insert(task_id, *u);
                                }
                                Some(console_api::field::Value::I64Val(i)) => {
                                    self.task_ids_map.insert(task_id, *i as u64);
                                }
                                _ => {}
                            }
                        }
                        if name == "spawn.location" {
                            match &field.value {
                                Some(console_api::field::Value::StrVal(s)) => {
                                    spawn_location = s.clone();
                                }
                                Some(console_api::field::Value::DebugVal(s)) => {
                                    spawn_location = s.clone();
                                }
                                _ => {}
                            }
                        }
                    }
                }
                if !spawn_location.is_empty() {
                    self.task_spawn_loc_map.insert(task_id, spawn_location);
                }
            }
        }
    }

    fn get_task_info(&self, task_id: u64) -> (u64, String, String) {
        let display_id = self.task_ids_map.get(&task_id).cloned().unwrap_or(task_id);

        let default_meta = Metadata {
            name: "unknown".to_string(),
            target: String::new(),
            file: String::new(),
            line: 0,
            column: 0,
        };

        let meta = self
            .task_metadata_map
            .get(&task_id)
            .and_then(|mid| self.metadata_map.get(mid))
            .unwrap_or(&default_meta);

        let spawn_loc = self.task_spawn_loc_map.get(&task_id).cloned().unwrap_or_default();
        let task_loc = self.task_locations_map.get(&task_id).cloned().unwrap_or_default();

        let display_name = if !spawn_loc.is_empty() {
            spawn_loc.clone()
        } else if !meta.file.is_empty() {
            if meta.column > 0 {
                format!("{}:{}:{}", meta.file, meta.line, meta.column)
            } else {
                format!("{}:{}", meta.file, meta.line)
            }
        } else {
            format!("{} ({})", meta.name, meta.target)
        };

        let location = if !task_loc.is_empty() {
            task_loc
        } else if !spawn_loc.is_empty() {
            spawn_loc
        } else if !meta.file.is_empty() {
            if meta.column > 0 {
                format!("{}:{}:{}", meta.file, meta.line, meta.column)
            } else {
                format!("{}:{}", meta.file, meta.line)
            }
        } else {
            "unknown".to_string()
        };

        (display_id, display_name, location)
    }
}

fn extract_task_timings(stats: &Stats) -> (std::time::Duration, std::time::Duration) {
    let busy = stats
        .poll_stats
        .as_ref()
        .and_then(|p| p.busy_time.as_ref())
        .map(|t| std::time::Duration::new(t.seconds as u64, t.nanos as u32))
        .unwrap_or_default();

    let scheduled = stats
        .scheduled_time
        .as_ref()
        .map(|t| std::time::Duration::new(t.seconds as u64, t.nanos as u32))
        .unwrap_or_default();

    (busy, scheduled)
}

#[derive(Debug, Clone)]
pub struct FormattedTaskUpdate {
    pub task_id: u64,
    pub location: String,
    pub busy_time: std::time::Duration,
    pub scheduled_delay: std::time::Duration,
}

#[derive(Debug, Clone)]
pub struct ConsoleHelperService {
    upstream_addr: String,
}

impl ConsoleHelperService {
    pub fn new(upstream_addr: String) -> Self {
        Self { upstream_addr }
    }

    // A helper to get a stream from the upstream console
    async fn upstream_stream(
        &self,
    ) -> Result<tonic::Streaming<console_api::instrument::Update>, Status> {
        let addr = self.upstream_addr.clone();
        // Connect to the REAL console-subscriber running in fake_enforcer
        let mut client = InstrumentClient::connect(addr).await.map_err(|e| {
            Status::unavailable(format!("Failed to connect to upstream console: {}", e))
        })?;

        let request = Request::new(InstrumentRequest {});
        let stream = client.watch_updates(request).await?.into_inner();
        Ok(stream)
    }

    pub fn task_update_stream(&self) -> impl Stream<Item = FormattedTaskUpdate> + Send + 'static {
        let upstream_addr = self.upstream_addr.clone();

        async_stream::stream! {
            let mut client = match InstrumentClient::connect(upstream_addr).await {
                Ok(c) => c,
                Err(_) => return,
            };

            let request = Request::new(InstrumentRequest {});
            let mut upstream = match client.watch_updates(request).await {
                Ok(s) => s.into_inner(),
                Err(_) => return,
            };

            let mut tracker = TaskTracker::new();

            while let Ok(Some(update)) = upstream.message().await {
                tracker.update(&update);

                if let Some(task_update) = update.task_update {
                    for (id, stats) in task_update.stats_update {
                        let (busy_time, scheduled_delay) = extract_task_timings(&stats);
                        let (display_id, _, location) = tracker.get_task_info(id);

                        yield FormattedTaskUpdate {
                            task_id: display_id,
                            location,
                            busy_time,
                            scheduled_delay,
                        };
                    }
                }
            }
        }
    }
}

#[tonic::async_trait]
impl ConsoleHelper for ConsoleHelperService {
    type WatchTaskLatencyStream =
        Pin<Box<dyn Stream<Item = Result<WatchTaskLatencyResponse, Status>> + Send + 'static>>;

    async fn watch_task_latency(
        &self,
        request: Request<WatchTaskLatencyRequest>,
    ) -> Result<Response<Self::WatchTaskLatencyStream>, Status> {
        let req = request.into_inner();
        let filter = req.task_location_filter;
        let mut upstream = self.upstream_stream().await?;

        // Transform the stream
        let output = async_stream::try_stream! {
            let mut tracker = TaskTracker::new();

            while let Some(update) = upstream
                .message()
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            {
                tracker.update(&update);

                if let Some(task_update) = update.task_update {
                    for (id, stats) in task_update.stats_update {
                        if stats.poll_stats.is_some() {
                            let (busy_dur, scheduled_dur) = extract_task_timings(&stats);
                            let busy = format!(
                                "{}.{:09}s",
                                busy_dur.as_secs(),
                                busy_dur.subsec_nanos()
                            );
                            let scheduled = format!(
                                "{}.{:09}s",
                                scheduled_dur.as_secs(),
                                scheduled_dur.subsec_nanos()
                            );

                            let (display_id, _, location) = tracker.get_task_info(id);

                            if !filter.is_empty() && !location.contains(&filter) {
                                continue;
                            }

                            yield WatchTaskLatencyResponse {
                                task_id: display_id,
                                location,
                                busy_time: busy,
                                scheduled_time: scheduled,
                            };
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::WatchTaskLatencyStream))
    }

    type WatchResourceContentionStream = Pin<
        Box<dyn Stream<Item = Result<WatchResourceContentionResponse, Status>> + Send + 'static>,
    >;

    async fn watch_resource_contention(
        &self,
        request: Request<WatchResourceContentionRequest>,
    ) -> Result<Response<Self::WatchResourceContentionStream>, Status> {
        let req = request.into_inner();
        let filter = req.source_filter;
        let mut upstream = self.upstream_stream().await?;

        let output = async_stream::try_stream! {
            while let Some(update) = upstream.message().await.map_err(|e| Status::internal(e.to_string()))? {
                if let Some(async_op_update) = update.async_op_update {
                    for op in async_op_update.new_async_ops {
                        if !filter.is_empty() && !op.source.contains(&filter) {
                            continue;
                        }
                        let resource_id = op.resource_id.map(|id| id.id).unwrap_or(0);
                        yield WatchResourceContentionResponse {
                            resource_id,
                            source: op.source,
                        };
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::WatchResourceContentionStream))
    }

    type WatchThroughputStream =
        Pin<Box<dyn Stream<Item = Result<WatchThroughputResponse, Status>> + Send + 'static>>;

    async fn watch_throughput(
        &self,
        _request: Request<WatchThroughputRequest>,
    ) -> Result<Response<Self::WatchThroughputStream>, Status> {
        let mut upstream = self.upstream_stream().await?;

        let output = async_stream::try_stream! {
            let mut tracker = TaskTracker::new();

            while let Some(update) = upstream
                .message()
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            {
                tracker.update(&update);

                if let Some(task_update) = update.task_update {
                    for (id, stats) in task_update.stats_update {
                        let polls = stats.poll_stats.map(|p| p.polls).unwrap_or(0);
                        let last_wake = stats.last_wake.map(|t| {
                            console_helper_proto::timestamp_proto::google::protobuf::Timestamp {
                                seconds: t.seconds,
                                nanos: t.nanos,
                            }
                        });

                        let (display_id, _, location) = tracker.get_task_info(id);

                        yield WatchThroughputResponse {
                            task_id: display_id,
                            wakes: stats.wakes,
                            polls,
                            last_wake,
                            location,
                        };
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::WatchThroughputStream))
    }

    type WatchTaskSizeStream =
        Pin<Box<dyn Stream<Item = Result<WatchTaskSizeResponse, Status>> + Send + 'static>>;

    async fn watch_task_size(
        &self,
        _request: Request<WatchTaskSizeRequest>,
    ) -> Result<Response<Self::WatchTaskSizeStream>, Status> {
        let mut upstream = self.upstream_stream().await?;

        let output = async_stream::try_stream! {
            while let Some(update) = upstream.message().await.map_err(|e| Status::internal(e.to_string()))? {
                if let Some(task_update) = update.task_update {
                    for task in task_update.new_tasks {
                        // Look for "size.bytes" field
                        let mut size_bytes = 0;
                        for field in task.fields {
                            if let Some(console_api::field::Name::StrName(name)) = field.name {
                                if name == "size.bytes" {
                                    if let Some(console_api::field::Value::U64Val(val)) = field.value {
                                        size_bytes = val;
                                        break;
                                    }
                                }
                            }
                        }

                        yield WatchTaskSizeResponse {
                            task_id: task.id.map(|id| id.id).unwrap_or(0),
                            size_bytes,
                        };
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::WatchTaskSizeStream))
    }

    type WatchChannelSaturationStream = Pin<
        Box<dyn Stream<Item = Result<WatchChannelSaturationResponse, Status>> + Send + 'static>,
    >;

    async fn watch_channel_saturation(
        &self,
        _request: Request<WatchChannelSaturationRequest>,
    ) -> Result<Response<Self::WatchChannelSaturationStream>, Status> {
        let mut upstream = self.upstream_stream().await?;

        let output = async_stream::try_stream! {
            while let Some(update) = upstream.message().await.map_err(|e| Status::internal(e.to_string()))? {
                if let Some(res_update) = update.resource_update {
                    for res in res_update.new_resources {
                        let concrete = res.concrete_type.clone();
                        if concrete.contains("Channel") || concrete.contains("Sender") || concrete.contains("Receiver") {
                            let kind_str = match res.kind.and_then(|k| k.kind) {
                                Some(console_api::resources::resource::kind::Kind::Known(k)) => format!("Known({})", k),
                                Some(console_api::resources::resource::kind::Kind::Other(s)) => s,
                                None => "Unknown".to_string(),
                            };

                            yield WatchChannelSaturationResponse {
                                resource_id: res.id.map(|id| id.id).unwrap_or(0),
                                r#type: concrete,
                                kind: kind_str,
                            };
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::WatchChannelSaturationStream))
    }

    type WatchTaskChurnStream =
        Pin<Box<dyn Stream<Item = Result<WatchTaskChurnResponse, Status>> + Send + 'static>>;

    async fn watch_task_churn(
        &self,
        _request: Request<WatchTaskChurnRequest>,
    ) -> Result<Response<Self::WatchTaskChurnStream>, Status> {
        let mut upstream = self.upstream_stream().await?;

        let output = async_stream::try_stream! {
            while let Some(update) = upstream.message().await.map_err(|e| Status::internal(e.to_string()))? {
                if let Some(task_update) = update.task_update {
                    for (id, stats) in task_update.stats_update {
                        if let Some(dropped) = stats.dropped_at {
                            let created_at = stats.created_at.map(|t| {
                                console_helper_proto::timestamp_proto::google::protobuf::Timestamp {
                                    seconds: t.seconds,
                                    nanos: t.nanos,
                                }
                            });
                            let dropped_at = Some(
                                console_helper_proto::timestamp_proto::google::protobuf::Timestamp {
                                    seconds: dropped.seconds,
                                    nanos: dropped.nanos,
                                },
                            );

                            yield WatchTaskChurnResponse {
                                dropped_task_id: id,
                                created_at,
                                dropped_at,
                            };
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::WatchTaskChurnStream))
    }
}
