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

use anyhow::{Context, Ok, Result};
use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion};
use data_scope::{
    request::{AddIsolateRequest, GetIsolateRequest, ValidateIsolateRequest},
    requester::DataScopeRequester,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use isolate_info::{BinaryServicesIndex, IsolateId};
use manifest_parser::{get_strictest_scope, parse_manifest};
use manifest_proto::enforcer::{
    ez_manifest::ManifestType, BinaryManifest, BundleManifest, EzManifest, EzServiceSpec,
};
use rand::Rng;
use std::cmp::max;
use std::collections::HashMap;
use std::hint::black_box;
use tokio::runtime::Runtime;

// Keep this array sorted from least strict scope to most strict scope.
const DATA_SCOPES: [DataScopeType; 3] =
    [DataScopeType::Public, DataScopeType::DomainOwned, DataScopeType::UserPrivate];
const JSON_MANIFEST_PATH: &str = "enforcer/benches/manifests/ez_benchmark_manifest.json";
const DATA_SCOPE_ENUM_STR_PREFIX: &str = "DATA_SCOPE_TYPE_";

/// Manages the setup and execution of `DataScopeManager` benchmarks.
/// It parses a manifest to populate the `DataScopeManager` with Isolates and provides
/// helper methods to generate benchmark requests.
struct DsmBenchmarkManager {
    data_scope_requester: DataScopeRequester,
    /// Maps a data scope to all binary services that can handle that scope.
    available_scope_to_binary_services: HashMap<DataScopeType, Vec<BinaryServicesIndex>>,
    /// Maps a binary service to the set of Isolates that implement it.
    binary_service_to_isolate_set: HashMap<BinaryServicesIndex, Vec<IsolateId>>,
    /// Whether to treat Isolates in manifest as Ratified Isolates.
    treat_as_ratified_isolates: bool,
}

impl DsmBenchmarkManager {
    fn start(
        ez_manifest: EzManifest,
        rt: &Runtime,
        treat_as_ratified_isolates: bool,
    ) -> Result<Self> {
        let dsm_benchmark_manager = rt
            .block_on(async {
                let data_scope_requester = DataScopeRequester::new(0);
                let mut dsm_benchmark_manager = Self {
                    data_scope_requester,
                    available_scope_to_binary_services: HashMap::new(),
                    binary_service_to_isolate_set: HashMap::new(),
                    treat_as_ratified_isolates,
                };

                dsm_benchmark_manager
                    .process_manifest(ez_manifest)
                    .await
                    .context("Failed to parse manifest and add isolates")?;
                Ok(dsm_benchmark_manager)
            })
            .context("Failed to parse manifest and start DSM")?;
        Ok(dsm_benchmark_manager)
    }

    async fn process_manifest(&mut self, manifest: EzManifest) -> Result<()> {
        if let Some(manifest_type) = manifest.manifest_type {
            match manifest_type {
                ManifestType::BinaryManifest(BinaryManifest { service_specs, .. }) => {
                    for service_spec in service_specs {
                        let service_name = service_spec.service_name.clone();
                        self.add_isolate(service_spec)
                            .await
                            .context(format!("Failed to add isolate {0}.", service_name))?;
                    }
                }
                ManifestType::BundleManifest(BundleManifest { manifests, .. }) => {
                    for sub_manifest in manifests {
                        Box::pin(self.process_manifest(sub_manifest))
                            .await
                            .context("Failed to process bundle manifest")?;
                    }
                }
                _ => panic!("Only Binary and Bundle manifests are supported"),
            }
        }
        Ok(())
    }

    async fn add_isolate(&mut self, service_spec: EzServiceSpec) -> Result<()> {
        let add_isolate_request = self.get_add_request(service_spec);
        let binary_service_index = add_isolate_request.isolate_id.get_binary_services_index();
        let max_data_scope = add_isolate_request.allowed_data_scope_type;
        let isolate_id = add_isolate_request.isolate_id;

        self.data_scope_requester
            .add_isolate(add_isolate_request)
            .await
            .context("Failed to add isolate")?;

        for data_scope in DATA_SCOPES {
            if data_scope > max_data_scope {
                break;
            }

            self.available_scope_to_binary_services
                .entry(data_scope)
                .or_default()
                .push(binary_service_index);
        }

        self.binary_service_to_isolate_set
            .entry(binary_service_index)
            .or_default()
            .push(isolate_id);
        Ok(())
    }

    fn has_isolate(&self, data_scope: DataScopeType) -> bool {
        self.available_scope_to_binary_services.contains_key(&data_scope)
    }

    fn create_get_request(&self, data_scope: DataScopeType) -> GetIsolateRequest {
        let binary_services = self.available_scope_to_binary_services.get(&data_scope).unwrap();
        let mut rng = rand::rng();
        let binary_services_index = binary_services[rng.random_range(0..binary_services.len())];
        GetIsolateRequest { binary_services_index, data_scope_type: data_scope }
    }

    fn create_validate_request(&self, data_scope: DataScopeType) -> ValidateIsolateRequest {
        let binary_services = self.available_scope_to_binary_services.get(&data_scope).unwrap();
        let mut rng = rand::rng();
        let binary_services_index = binary_services[rng.random_range(0..binary_services.len())];
        let isolate_ids = self.binary_service_to_isolate_set.get(&binary_services_index).unwrap();
        let isolate_id = isolate_ids[rng.random_range(0..isolate_ids.len())];
        ValidateIsolateRequest { isolate_id, requested_scope: data_scope }
    }

    async fn call_get_request(&self, get_isolate_request: GetIsolateRequest) -> Result<()> {
        self.data_scope_requester
            .get_isolate(get_isolate_request)
            .await
            .context("Failed to send get isolate request")?;
        Ok(())
    }

    async fn call_validate_request(
        &self,
        validate_isolate_request: ValidateIsolateRequest,
    ) -> Result<()> {
        self.data_scope_requester
            .validate_isolate_scope(validate_isolate_request)
            .await
            .context("Failed to send validate isolate request")?;
        Ok(())
    }

    fn get_add_request(&self, service_spec: EzServiceSpec) -> AddIsolateRequest {
        let binary_services_index = BinaryServicesIndex::new(self.treat_as_ratified_isolates);
        let (max_input_scope, max_output_scope) = get_strictest_scope(service_spec.method_specs);
        AddIsolateRequest {
            current_data_scope_type: DataScopeType::Public,
            allowed_data_scope_type: max(max_input_scope, max_output_scope),
            isolate_id: IsolateId::new(binary_services_index),
        }
    }
}

fn benchmark_helper(
    mut group: BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
    treat_as_ratified_isolates: bool,
) {
    let ez_manifest =
        parse_manifest(JSON_MANIFEST_PATH.to_string()).expect("Failed to parse JSON manifest file");
    let manager = DsmBenchmarkManager::start(ez_manifest, rt, treat_as_ratified_isolates)
        .expect("Failed to start DSM Benchmark Manager");

    for data_scope in DATA_SCOPES {
        if !manager.has_isolate(data_scope) {
            continue;
        }

        let data_scope_str =
            data_scope.as_str_name().strip_prefix(DATA_SCOPE_ENUM_STR_PREFIX).unwrap();

        // A 25% noise threshold is acceptable here because the benchmark is in the
        // microsecond range, where small, insignificant fluctuations can appear as
        // large relative changes.
        group
            .bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "get_isolate_api_data_scope_{}",
                    &data_scope_str
                )),
                &data_scope,
                |b, &data_scope| {
                    b.to_async(rt).iter_batched(
                        || manager.create_get_request(black_box(data_scope)),
                        |request| async {
                            black_box(manager.call_get_request(black_box(request)).await).unwrap()
                        },
                        BatchSize::SmallInput,
                    );
                },
            )
            .noise_threshold(0.25);

        group
            .bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "validate_isolate_api_data_scope_{}",
                    &data_scope_str
                )),
                &data_scope,
                |b, &data_scope| {
                    b.to_async(rt).iter_batched(
                        || manager.create_validate_request(black_box(data_scope)),
                        |request| async {
                            black_box(manager.call_validate_request(black_box(request)).await)
                                .unwrap()
                        },
                        BatchSize::SmallInput,
                    );
                },
            )
            .noise_threshold(0.25);
    }
}

fn benchmark_dsm(c: &mut Criterion, rt: &Runtime) {
    const TREAT_AS_RATIFIED_ISOLATES: bool = false;
    let group = c.benchmark_group("benchmark_data_scope_manager");
    benchmark_helper(group, rt, TREAT_AS_RATIFIED_ISOLATES);
}

fn benchmark_rim(c: &mut Criterion, rt: &Runtime) {
    const TREAT_AS_RATIFIED_ISOLATES: bool = true;
    let group = c.benchmark_group("benchmark_ratified_isolates_manager");
    benchmark_helper(group, rt, TREAT_AS_RATIFIED_ISOLATES);
}

pub fn benchmark_data_scope() {
    let mut criterion: criterion::Criterion<_> =
        (criterion::Criterion::default()).configure_from_args();
    let tokio_runtime = tokio::runtime::Runtime::new().expect("Failed to start tokio runtime");
    benchmark_dsm(&mut criterion, &tokio_runtime);
    benchmark_rim(&mut criterion, &tokio_runtime);
}
