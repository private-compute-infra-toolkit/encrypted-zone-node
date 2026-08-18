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

use data_scope_proto::enforcer::v1::DataScopeType;
use manifest_proto::enforcer::v1::{BinaryManifest, EzMethodSpec};

pub(crate) mod parser_util;
pub mod v1;
pub mod v2;

pub use v1::{parse_isolate_runtime_configs, parse_manifest, serialize_backend_dependencies};
pub use v2::{SetupManifest, WorkloadManifests};

/// A parsed isolate containing its identity and binary specification.
#[derive(Clone, Debug, PartialEq)]
pub struct ParsedIsolate {
    pub isolate_name: String,
    pub publisher_id: String,
    pub package_filename: String,
    pub binary_manifest: BinaryManifest,
}

/// Determines the strictest input and output `DataScopeType` from method specifications.
pub fn get_strictest_scope(method_specs: Vec<EzMethodSpec>) -> (DataScopeType, DataScopeType) {
    let strictest_input_scope = method_specs
        .iter()
        .flat_map(|spec| &spec.input_scope_types)
        .max()
        .copied()
        .unwrap_or(DataScopeType::Unspecified as i32);
    let strictest_output_scope = method_specs
        .iter()
        .flat_map(|spec| &spec.output_scope_types)
        .max()
        .copied()
        .unwrap_or(DataScopeType::Unspecified as i32);

    (
        DataScopeType::try_from(strictest_input_scope).unwrap_or(DataScopeType::Unspecified),
        DataScopeType::try_from(strictest_output_scope).unwrap_or(DataScopeType::Unspecified),
    )
}
