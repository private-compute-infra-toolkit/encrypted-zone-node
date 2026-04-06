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

pub mod tokio_service;

pub use tokio_service::TokioDiagnosticService;

use diagnostics_proto::enforcer::diagnostics::v1::diagnostic_service_server::DiagnosticService;
use diagnostics_proto::enforcer::diagnostics::v1::{
    get_diagnostic_request, get_diagnostic_response, CpuProfileResult, ErrorResponse,
    GetDiagnosticRequest, GetDiagnosticResponse,
};
use std::time::Duration as StdDuration;
use tonic::{Request, Response, Status};

mod pprof;

pub use crate::pprof::EnforcerPprofService;

#[derive(Clone)]
pub struct EnforcerDiagnosticService {
    pprof_service: EnforcerPprofService,
    tokio_service: TokioDiagnosticService,
}

impl EnforcerDiagnosticService {
    pub fn new(pprof_service: EnforcerPprofService, tokio_service: TokioDiagnosticService) -> Self {
        Self { pprof_service, tokio_service }
    }
}

#[tonic::async_trait]
impl DiagnosticService for EnforcerDiagnosticService {
    async fn get_diagnostic(
        &self,
        request: Request<GetDiagnosticRequest>,
    ) -> Result<Response<GetDiagnosticResponse>, Status> {
        let profile_spec = request.into_inner().profile_spec.ok_or_else(|| {
            log::warn!("GetDiagnostic request missing profile_spec");
            Status::invalid_argument("profile_spec is required")
        })?;

        match profile_spec {
            get_diagnostic_request::ProfileSpec::Cpu(cpu_spec) => {
                let duration_seconds = cpu_spec.duration_seconds;
                log::info!("Received GetDiagnostic(CPU) request for {} seconds", duration_seconds);
                let duration = if duration_seconds > 0 {
                    StdDuration::from_secs(duration_seconds as u64)
                } else {
                    log::warn!(
                        "Invalid duration requested for GetDiagnostic(CPU): {}",
                        duration_seconds
                    );
                    return Err(Status::invalid_argument("invalid duration (ie. < 1)"));
                };

                let result = match self.pprof_service.collect_cpu_profile(duration).await {
                    Ok(pprof_data) => get_diagnostic_response::Result::Cpu(CpuProfileResult {
                        pprof_data,
                        captured_duration_seconds: duration.as_secs() as i64,
                    }),
                    Err(e) => {
                        log::error!("GetDiagnostic(CPU) failed: {}", e);
                        get_diagnostic_response::Result::Error(ErrorResponse {
                            message: e.to_string(),
                        })
                    }
                };

                Ok(Response::new(GetDiagnosticResponse { result: Some(result) }))
            }
            get_diagnostic_request::ProfileSpec::Tokio(tokio_spec) => {
                log::info!("Received GetDiagnostic(Tokio) request");
                let result = match self.tokio_service.get_summary(&tokio_spec).await {
                    Ok(res) => res,
                    Err(e) => {
                        log::error!("GetDiagnostic(Tokio) failed: {}", e);
                        return Err(e);
                    }
                };
                Ok(Response::new(GetDiagnosticResponse {
                    result: Some(get_diagnostic_response::Result::Tokio(result)),
                }))
            }
            _ => {
                log::warn!("Requested unsupported profiling type in GetDiagnostic");
                Err(Status::unimplemented("Requested profiling type is not supported"))
            }
        }
    }
}
