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

use diagnostics_proto::enforcer::diagnostics::v1::get_cpu_profile_response;
use diagnostics_proto::enforcer::diagnostics::v1::pprof_service_server::PprofService;
use diagnostics_proto::enforcer::diagnostics::v1::{GetCpuProfileRequest, GetCpuProfileResponse};

use ::pprof::protos::Message;
use flate2::write::GzEncoder;
use flate2::Compression;
use once_cell::sync::Lazy;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::{Request, Response, Status};

static PROFILER_LOCK: Lazy<Arc<Mutex<()>>> = Lazy::new(|| Arc::new(Mutex::new(())));

#[derive(Clone)]
pub struct EnforcerPprofService {}

impl EnforcerPprofService {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }

    fn setup_profiler(&self) -> anyhow::Result<::pprof::ProfilerGuard<'static>> {
        let profiler_guard = ::pprof::ProfilerGuardBuilder::default()
            .frequency(1000)
            .blocklist(&["libc", "libgcc", "libdl", "libm", "libpthread", "librt"])
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build profiler guard: {e}"))?;
        Ok(profiler_guard)
    }

    pub(crate) async fn collect_cpu_profile(
        &self,
        duration: StdDuration,
    ) -> anyhow::Result<Vec<u8>> {
        log::info!("Starting CPU profile collection for {} seconds", duration.as_secs());
        let _guard_lock = PROFILER_LOCK.lock().await;

        let profiler_guard = self.setup_profiler()?;

        sleep(duration).await;

        let report = profiler_guard.report().build().map_err(|e| {
            log::error!("Failed to build profiler report: {}", e);
            anyhow::anyhow!("Failed to build profiler report: {e}")
        })?;

        let profile = report.pprof().map_err(|e| {
            log::error!("Failed to generate pprof profile: {}", e);
            anyhow::anyhow!("Failed to generate pprof profile: {e}")
        })?;

        let mut pprof_proto_data = Vec::new();
        profile.encode(&mut pprof_proto_data).map_err(|e| {
            log::error!("Failed to encode pprof profile: {}", e);
            anyhow::anyhow!("Failed to encode pprof profile: {e}")
        })?;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&pprof_proto_data).map_err(|e| {
            log::error!("Failed to gzip pprof profile: {}", e);
            anyhow::anyhow!("Failed to gzip pprof profile: {e}")
        })?;
        let pprof_data = encoder.finish().map_err(|e| {
            log::error!("Failed to finalize gzip compression: {}", e);
            anyhow::anyhow!("Failed to finalize gzip compression: {e}")
        })?;

        log::info!("Successfully collected and gzipped CPU profile ({} bytes)", pprof_data.len());
        Ok(pprof_data)
    }
}

#[tonic::async_trait]
impl PprofService for EnforcerPprofService {
    async fn get_cpu_profile(
        &self,
        request: Request<GetCpuProfileRequest>,
    ) -> Result<Response<GetCpuProfileResponse>, Status> {
        let duration_seconds = request.into_inner().duration_seconds;
        log::info!("Received GetCpuProfile request for {} seconds", duration_seconds);
        let duration = if duration_seconds > 0 {
            StdDuration::from_secs(duration_seconds as u64)
        } else {
            log::warn!("Invalid duration requested for GetCpuProfile: {}", duration_seconds);
            return Ok(Response::new(GetCpuProfileResponse {
                result: Some(get_cpu_profile_response::Result::Error(
                    "invalid duration (ie. < 1)".to_string(),
                )),
            }));
        };

        let result = match self.collect_cpu_profile(duration).await {
            Ok(pprof_data) => Some(get_cpu_profile_response::Result::PprofData(pprof_data)),
            Err(e) => {
                log::error!("CPU profiling failed: {}", e);
                Some(get_cpu_profile_response::Result::Error(e.to_string()))
            }
        };

        Ok(Response::new(GetCpuProfileResponse { result }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_collect_cpu_profile_success() {
        let service = EnforcerPprofService::new();
        let result = service.collect_cpu_profile(StdDuration::from_millis(100)).await;
        assert!(result.is_ok());
        let pprof_data = result.unwrap();
        assert!(!pprof_data.is_empty());
        // Verify gzip magic bytes 1f 8b
        assert!(pprof_data.len() >= 2);
        assert_eq!(pprof_data[0], 0x1f);
        assert_eq!(pprof_data[1], 0x8b);
    }

    #[tokio::test]
    async fn test_get_cpu_profile_trait_success() {
        let service = EnforcerPprofService::new();
        let request = Request::new(GetCpuProfileRequest { duration_seconds: 1 });
        let response = service.get_cpu_profile(request).await.unwrap().into_inner();
        match response.result {
            Some(get_cpu_profile_response::Result::PprofData(data)) => {
                assert!(!data.is_empty());
            }
            _ => panic!("Expected PprofData, got {:?}", response.result),
        }
    }

    #[tokio::test]
    async fn test_get_cpu_profile_invalid_duration() {
        let service = EnforcerPprofService::new();
        let request = Request::new(GetCpuProfileRequest { duration_seconds: 0 });
        let response = service.get_cpu_profile(request).await.unwrap().into_inner();
        match response.result {
            Some(get_cpu_profile_response::Result::Error(err)) => {
                assert_eq!(err, "invalid duration (ie. < 1)");
            }
            _ => panic!("Expected Error, got {:?}", response.result),
        }
    }
}
