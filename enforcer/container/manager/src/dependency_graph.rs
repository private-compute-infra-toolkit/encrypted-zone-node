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

use anyhow::Result;
use indexmap::IndexSet;
use isolate_info::{get_isolate_name, BinaryServicesIndex, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use manifest_proto::enforcer::v1::ez_backend_dependency::RouteType;
use manifest_proto::enforcer::v1::EzBackendDependency;
use std::collections::{HashMap, HashSet};

/// Builds the dependency graph from isolate backend dependencies and resolves target isolate indices using `IsolateServiceMapper`.
/// Returns a map of BinaryServicesIndex to the set of BinaryServicesIndex that it depends on.
pub async fn build_isolate_dependency_graph(
    isolate_dependencies_map: &HashMap<BinaryServicesIndex, Vec<EzBackendDependency>>,
    isolate_service_mapper: &IsolateServiceMapper,
) -> Result<HashMap<BinaryServicesIndex, HashSet<BinaryServicesIndex>>> {
    let mut dep_graph = HashMap::new();
    for (binary_services_index, backend_dependencies) in isolate_dependencies_map.iter() {
        let mut deps = HashSet::new();
        let source_isolate_name = get_isolate_name(binary_services_index);

        for dep in backend_dependencies {
            if matches!(
                RouteType::try_from(dep.route_type),
                Ok(RouteType::External | RouteType::Remote)
            ) {
                continue;
            }
            let target = IsolateServiceInfo {
                operator_domain: dep.operator_domain.clone(),
                service_name: dep.service_name.clone(),
                isolate_name: dep.isolate_name.clone(),
                publisher_id: dep.publisher_id.clone(),
            };
            if let Some(target_idx) = isolate_service_mapper.get_binary_index(&target).await {
                if target_idx != *binary_services_index
                    && isolate_dependencies_map.contains_key(&target_idx)
                    && deps.insert(target_idx)
                {
                    log::info!(
                        "Found dependency for {} on {} ({}/{})",
                        source_isolate_name,
                        get_isolate_name(&target_idx),
                        dep.operator_domain,
                        dep.service_name
                    );
                }
            }
        }
        dep_graph.insert(*binary_services_index, deps);
    }
    Ok(dep_graph)
}

/// Validates that the dependency graph is a valid DAG and contains no circular dependencies.
pub fn validate_isolate_dependency_graph(
    dep_graph: &HashMap<BinaryServicesIndex, HashSet<BinaryServicesIndex>>,
) -> Result<()> {
    fn find_cycle(
        node: BinaryServicesIndex,
        graph: &HashMap<BinaryServicesIndex, HashSet<BinaryServicesIndex>>,
        visited: &mut HashSet<BinaryServicesIndex>,
        stack: &mut IndexSet<BinaryServicesIndex>,
    ) -> Option<Vec<BinaryServicesIndex>> {
        if let Some(pos) = stack.get_index_of(&node) {
            let mut cycle: Vec<BinaryServicesIndex> = stack[pos..].iter().copied().collect();
            cycle.push(node);
            return Some(cycle);
        }
        if visited.contains(&node) {
            return None;
        }
        visited.insert(node);
        stack.insert(node);
        if let Some(deps) = graph.get(&node) {
            for &dep in deps {
                if let Some(cycle) = find_cycle(dep, graph, visited, stack) {
                    return Some(cycle);
                }
            }
        }
        stack.pop();
        None
    }

    let mut visited = HashSet::new();
    let mut stack = IndexSet::new();
    for &node in dep_graph.keys() {
        if let Some(cycle) = find_cycle(node, dep_graph, &mut visited, &mut stack) {
            let cycle_str = cycle.iter().map(get_isolate_name).collect::<Vec<_>>().join(" -> ");
            log::error!("Circular isolate dependency detected in manifest: {}", cycle_str);
            anyhow::bail!(
                "Circular isolate dependency detected in manifest: {}; cannot start isolates.",
                cycle_str
            );
        }
    }
    Ok(())
}
