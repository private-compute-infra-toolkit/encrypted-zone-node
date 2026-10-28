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
use manifest_proto::enforcer::v1::{
    EzBackendDependency, EzMethodSpec, EzServiceSpec, InterceptingServices, IsolateMetricsPolicy,
};

pub(crate) mod parser_util;
pub mod v1;
pub mod v2;

/// Kind-specific settings of an isolate.
#[derive(Clone, Debug, PartialEq)]
pub enum IsolateKind {
    Ratified { services_to_intercept: Vec<InterceptingServices> },
    Opaque,
}

/// A parsed isolate containing its identity, startup configuration, and security specifications.
#[derive(Clone, Debug, PartialEq)]
pub struct ParsedIsolate {
    pub isolate_name: String,
    pub publisher_id: String,
    pub package_filename: String,
    pub number_of_isolates: i32,
    pub binary_filename: String,
    // Manifest V1: parsed from the EzManifest.
    // Manifest V2: the manifest has no such fields, so the parser leaves these empty and
    // ContainerManager fills them for Opaque Isolates from the IsolateStartupParameters fetched
    // from EzManagementService.
    pub command_line_arguments: Vec<String>,
    pub environment_variables: Vec<String>,
    pub service_specs: Vec<EzServiceSpec>,
    pub ez_backend_dependencies: Vec<EzBackendDependency>,
    pub metrics_policy: Option<IsolateMetricsPolicy>,
    pub kind: IsolateKind,
}

impl ParsedIsolate {
    pub fn is_ratified(&self) -> bool {
        matches!(&self.kind, IsolateKind::Ratified { .. })
    }

    pub fn services_to_intercept(&self) -> &[InterceptingServices] {
        match &self.kind {
            IsolateKind::Ratified { services_to_intercept } => services_to_intercept,
            IsolateKind::Opaque => &[],
        }
    }
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
