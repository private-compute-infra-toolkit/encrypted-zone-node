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

use container_manager::dependency_graph::{
    build_isolate_dependency_graph, validate_isolate_dependency_graph,
};
use isolate_info::{BinaryServicesIndex, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use manifest_proto::enforcer::v1::ez_backend_dependency::RouteType;
use manifest_proto::enforcer::v1::EzBackendDependency;
use std::collections::{HashMap, HashSet};

fn create_backend_dep(
    operator_domain: &str,
    publisher_id: &str,
    isolate_name: &str,
    service_name: &str,
    route_type: RouteType,
) -> EzBackendDependency {
    EzBackendDependency {
        operator_domain: operator_domain.to_string(),
        publisher_id: publisher_id.to_string(),
        isolate_name: isolate_name.to_string(),
        service_name: service_name.to_string(),
        method_name: "test_method".to_string(),
        route_type: route_type as i32,
    }
}

fn create_service_info(
    operator_domain: &str,
    publisher_id: &str,
    isolate_name: &str,
    service_name: &str,
) -> IsolateServiceInfo {
    IsolateServiceInfo {
        operator_domain: operator_domain.to_string(),
        publisher_id: publisher_id.to_string(),
        isolate_name: isolate_name.to_string(),
        service_name: service_name.to_string(),
    }
}

#[test]
fn test_validate_isolate_dependency_graph_empty() {
    let graph: HashMap<BinaryServicesIndex, HashSet<BinaryServicesIndex>> = HashMap::new();
    assert!(validate_isolate_dependency_graph(&graph).is_ok());
}

#[test]
fn test_validate_isolate_dependency_graph_single_node_no_deps() {
    let idx = BinaryServicesIndex::new(false);
    let mut graph = HashMap::new();
    graph.insert(idx, HashSet::new());
    assert!(validate_isolate_dependency_graph(&graph).is_ok());
}

#[test]
fn test_validate_isolate_dependency_graph_linear_chain() {
    // idx1 -> idx2 -> idx3
    let idx1 = BinaryServicesIndex::new(false);
    let idx2 = BinaryServicesIndex::new(false);
    let idx3 = BinaryServicesIndex::new(false);

    let mut graph = HashMap::new();
    graph.insert(idx1, HashSet::from([idx2]));
    graph.insert(idx2, HashSet::from([idx3]));
    graph.insert(idx3, HashSet::new());

    assert!(validate_isolate_dependency_graph(&graph).is_ok());
}

#[test]
fn test_validate_isolate_dependency_graph_diamond_dag() {
    // a -> {b, c}, b -> {d}, c -> {d}, d -> {}
    let a = BinaryServicesIndex::new(false);
    let b = BinaryServicesIndex::new(false);
    let c = BinaryServicesIndex::new(false);
    let d = BinaryServicesIndex::new(false);

    let mut graph = HashMap::new();
    graph.insert(a, HashSet::from([b, c]));
    graph.insert(b, HashSet::from([d]));
    graph.insert(c, HashSet::from([d]));
    graph.insert(d, HashSet::new());

    assert!(validate_isolate_dependency_graph(&graph).is_ok());
}

#[test]
fn test_validate_isolate_dependency_graph_self_loop() {
    let a = BinaryServicesIndex::new(false);

    let mut graph = HashMap::new();
    graph.insert(a, HashSet::from([a]));

    let result = validate_isolate_dependency_graph(&graph);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Circular isolate dependency detected in manifest"));
}

#[test]
fn test_validate_isolate_dependency_graph_cycle_path_in_error() {
    use isolate_info::{register_isolate_type, IsolateType};

    let a = BinaryServicesIndex::new(false);
    let b = BinaryServicesIndex::new(false);
    let c = BinaryServicesIndex::new(false);

    register_isolate_type(
        a,
        IsolateType { publisher_id: "pub".to_string(), isolate_name: "isolate1".to_string() },
    );
    register_isolate_type(
        b,
        IsolateType { publisher_id: "pub".to_string(), isolate_name: "isolate2".to_string() },
    );
    register_isolate_type(
        c,
        IsolateType { publisher_id: "pub".to_string(), isolate_name: "isolate3".to_string() },
    );

    let mut graph = HashMap::new();
    graph.insert(a, HashSet::from([b]));
    graph.insert(b, HashSet::from([c]));
    graph.insert(c, HashSet::from([a]));

    let result = validate_isolate_dependency_graph(&graph);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("isolate1 -> isolate2 -> isolate3 -> isolate1")
            || err_msg.contains("isolate2 -> isolate3 -> isolate1 -> isolate2")
            || err_msg.contains("isolate3 -> isolate1 -> isolate2 -> isolate3"),
        "Unexpected error message: {}",
        err_msg
    );
}

#[test]
fn test_validate_isolate_dependency_graph_two_node_cycle() {
    let a = BinaryServicesIndex::new(false);
    let b = BinaryServicesIndex::new(false);

    let mut graph = HashMap::new();
    graph.insert(a, HashSet::from([b]));
    graph.insert(b, HashSet::from([a]));

    let result = validate_isolate_dependency_graph(&graph);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Circular isolate dependency detected"));
}

#[test]
fn test_validate_isolate_dependency_graph_disconnected_subgraph_cycle() {
    let a = BinaryServicesIndex::new(false);
    let b = BinaryServicesIndex::new(false);
    let c = BinaryServicesIndex::new(false);
    let d = BinaryServicesIndex::new(false);
    let e = BinaryServicesIndex::new(false);

    let mut graph = HashMap::new();
    // Component 1: valid DAG (a -> b)
    graph.insert(a, HashSet::from([b]));
    graph.insert(b, HashSet::from([]));
    // Component 2: cyclic (c -> d -> e -> c)
    graph.insert(c, HashSet::from([d]));
    graph.insert(d, HashSet::from([e]));
    graph.insert(e, HashSet::from([c]));

    let result = validate_isolate_dependency_graph(&graph);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Circular isolate dependency detected"));
}

#[test]
fn test_validate_isolate_dependency_graph_deep_cycle() {
    let nodes: Vec<BinaryServicesIndex> =
        (0..10).map(|_| BinaryServicesIndex::new(false)).collect();

    let mut graph = HashMap::new();
    for i in 0..9 {
        graph.insert(nodes[i], HashSet::from([nodes[i + 1]]));
    }
    // Create a cycle from node 9 back to node 3
    graph.insert(nodes[9], HashSet::from([nodes[3]]));

    let result = validate_isolate_dependency_graph(&graph);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Circular isolate dependency detected"));
}

#[test]
fn test_validate_isolate_dependency_graph_dep_not_in_graph_keys() {
    let a = BinaryServicesIndex::new(false);
    let b = BinaryServicesIndex::new(false);

    // Node `b` is present as a dependency of `a`, but not registered as a key in `graph`
    let mut graph = HashMap::new();
    graph.insert(a, HashSet::from([b]));

    assert!(validate_isolate_dependency_graph(&graph).is_ok());
}

#[tokio::test]
async fn test_build_isolate_dependency_graph_empty() {
    let mapper = IsolateServiceMapper::default();
    let isolate_deps_map = HashMap::new();

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert!(graph.is_empty());
}

#[tokio::test]
async fn test_build_isolate_dependency_graph_single_isolate_no_deps() {
    let mapper = IsolateServiceMapper::default();
    let svc = create_service_info("domain.com", "pub1", "iso1", "svc1");
    let idx =
        mapper.new_binary_index(vec![svc], false, "pub1".into(), "iso1".into()).await.unwrap();

    let mut isolate_deps_map = HashMap::new();
    isolate_deps_map.insert(idx, vec![]);

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert_eq!(graph.len(), 1);
    assert!(graph.get(&idx).unwrap().is_empty());
}

#[tokio::test]
async fn test_build_isolate_dependency_graph_internal_dependency() {
    let mapper = IsolateServiceMapper::default();
    let svc1 = create_service_info("domain.com", "pub1", "iso1", "svc1");
    let svc2 = create_service_info("domain.com", "pub2", "iso2", "svc2");

    let idx1 =
        mapper.new_binary_index(vec![svc1], false, "pub1".into(), "iso1".into()).await.unwrap();
    let idx2 =
        mapper.new_binary_index(vec![svc2], false, "pub2".into(), "iso2".into()).await.unwrap();

    let dep_to_svc2 = create_backend_dep("domain.com", "pub2", "iso2", "svc2", RouteType::Internal);

    let mut isolate_deps_map = HashMap::new();
    isolate_deps_map.insert(idx1, vec![dep_to_svc2]);
    isolate_deps_map.insert(idx2, vec![]);

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert_eq!(graph.len(), 2);
    assert_eq!(graph.get(&idx1).unwrap(), &HashSet::from([idx2]));
    assert!(graph.get(&idx2).unwrap().is_empty());
}

#[tokio::test]
async fn test_build_isolate_dependency_graph_non_internal_dependencies_skipped() {
    let mapper = IsolateServiceMapper::default();
    let svc1 = create_service_info("domain.com", "pub1", "iso1", "svc1");
    let svc2 = create_service_info("domain.com", "pub2", "iso2", "svc2");

    let idx1 =
        mapper.new_binary_index(vec![svc1], false, "pub1".into(), "iso1".into()).await.unwrap();
    let idx2 =
        mapper.new_binary_index(vec![svc2], false, "pub2".into(), "iso2".into()).await.unwrap();

    let ext_dep = create_backend_dep("domain.com", "pub2", "iso2", "svc2", RouteType::External);
    let remote_dep = create_backend_dep("domain.com", "pub2", "iso2", "svc2", RouteType::Remote);
    let unspec_dep =
        create_backend_dep("domain.com", "pub2", "iso2", "svc2", RouteType::Unspecified);

    let mut isolate_deps_map = HashMap::new();
    isolate_deps_map.insert(idx1, vec![ext_dep, remote_dep, unspec_dep]);
    isolate_deps_map.insert(idx2, vec![]);

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert_eq!(graph.len(), 2);
    assert!(graph.get(&idx1).unwrap().is_empty());
    assert!(graph.get(&idx2).unwrap().is_empty());
}

#[tokio::test]
async fn test_build_isolate_dependency_graph_self_dependency_ignored() {
    let mapper = IsolateServiceMapper::default();
    let svc1 = create_service_info("domain.com", "pub1", "iso1", "svc1");

    let idx1 =
        mapper.new_binary_index(vec![svc1], false, "pub1".into(), "iso1".into()).await.unwrap();

    // Dependency targeting self
    let self_dep = create_backend_dep("domain.com", "pub1", "iso1", "svc1", RouteType::Internal);

    let mut isolate_deps_map = HashMap::new();
    isolate_deps_map.insert(idx1, vec![self_dep]);

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert_eq!(graph.len(), 1);
    assert!(graph.get(&idx1).unwrap().is_empty());
}

#[tokio::test]
async fn test_build_isolate_dependency_graph_target_not_in_startup_args() {
    let mapper = IsolateServiceMapper::default();
    let svc1 = create_service_info("domain.com", "pub1", "iso1", "svc1");
    let svc2 = create_service_info("domain.com", "pub2", "iso2", "svc2");

    let idx1 =
        mapper.new_binary_index(vec![svc1], false, "pub1".into(), "iso1".into()).await.unwrap();
    let _idx2 =
        mapper.new_binary_index(vec![svc2], false, "pub2".into(), "iso2".into()).await.unwrap();

    let dep_to_svc2 = create_backend_dep("domain.com", "pub2", "iso2", "svc2", RouteType::Internal);

    let mut isolate_deps_map = HashMap::new();
    // Only idx1 is inserted; idx2 is omitted from isolate_deps_map
    isolate_deps_map.insert(idx1, vec![dep_to_svc2]);

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert_eq!(graph.len(), 1);
    assert!(graph.get(&idx1).unwrap().is_empty());
}

#[tokio::test]
async fn test_build_isolate_dependency_graph_service_not_found_in_mapper() {
    let mapper = IsolateServiceMapper::default();
    let svc1 = create_service_info("domain.com", "pub1", "iso1", "svc1");

    let idx1 =
        mapper.new_binary_index(vec![svc1], false, "pub1".into(), "iso1".into()).await.unwrap();

    // Dependency on unregistered service
    let unreg_dep = create_backend_dep(
        "unknown.com",
        "unknown_pub",
        "unknown_iso",
        "unknown_svc",
        RouteType::Internal,
    );

    let mut isolate_deps_map = HashMap::new();
    isolate_deps_map.insert(idx1, vec![unreg_dep]);

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert_eq!(graph.len(), 1);
    assert!(graph.get(&idx1).unwrap().is_empty());
}

#[tokio::test]
async fn test_build_isolate_dependency_graph_duplicate_dependencies_deduplicated() {
    let mapper = IsolateServiceMapper::default();
    let svc1 = create_service_info("domain.com", "pub1", "iso1", "svc1");
    let svc2 = create_service_info("domain.com", "pub2", "iso2", "svc2");

    let idx1 =
        mapper.new_binary_index(vec![svc1], false, "pub1".into(), "iso1".into()).await.unwrap();
    let idx2 =
        mapper.new_binary_index(vec![svc2], false, "pub2".into(), "iso2".into()).await.unwrap();

    let dep1 = create_backend_dep("domain.com", "pub2", "iso2", "svc2", RouteType::Internal);
    let dep2 = create_backend_dep("domain.com", "pub2", "iso2", "svc2", RouteType::Internal);

    let mut isolate_deps_map = HashMap::new();
    isolate_deps_map.insert(idx1, vec![dep1, dep2]);
    isolate_deps_map.insert(idx2, vec![]);

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert_eq!(graph.len(), 2);
    assert_eq!(graph.get(&idx1).unwrap().len(), 1);
    assert!(graph.get(&idx1).unwrap().contains(&idx2));
}

#[tokio::test]
async fn test_build_isolate_dependency_graph_complex_dependencies() {
    let mapper = IsolateServiceMapper::default();
    let svc_a = create_service_info("domain.com", "pub_a", "iso_a", "svc_a");
    let svc_b = create_service_info("domain.com", "pub_b", "iso_b", "svc_b");
    let svc_c = create_service_info("domain.com", "pub_c", "iso_c", "svc_c");

    let idx_a =
        mapper.new_binary_index(vec![svc_a], false, "pub_a".into(), "iso_a".into()).await.unwrap();
    let idx_b =
        mapper.new_binary_index(vec![svc_b], false, "pub_b".into(), "iso_b".into()).await.unwrap();
    let idx_c =
        mapper.new_binary_index(vec![svc_c], false, "pub_c".into(), "iso_c".into()).await.unwrap();

    // Isolate A depends on B (internal), C (internal), external, remote, and unknown services
    let dep_a_to_b =
        create_backend_dep("domain.com", "pub_b", "iso_b", "svc_b", RouteType::Internal);
    let dep_a_to_c =
        create_backend_dep("domain.com", "pub_c", "iso_c", "svc_c", RouteType::Internal);
    let dep_a_ext =
        create_backend_dep("ext.com", "pub_ext", "iso_ext", "svc_ext", RouteType::External);
    let dep_a_remote =
        create_backend_dep("domain.com", "pub_rem", "iso_rem", "svc_rem", RouteType::Remote);
    let dep_a_unspecified =
        create_backend_dep("domain.com", "pub_u", "iso_u", "svc_u", RouteType::Unspecified);
    let dep_a_unknown =
        create_backend_dep("unknown.com", "pub_u", "iso_u", "svc_u", RouteType::Internal);

    // Isolate B depends on C
    let dep_b_to_c =
        create_backend_dep("domain.com", "pub_c", "iso_c", "svc_c", RouteType::Internal);

    let mut isolate_deps_map = HashMap::new();
    isolate_deps_map.insert(
        idx_a,
        vec![dep_a_to_b, dep_a_to_c, dep_a_ext, dep_a_remote, dep_a_unspecified, dep_a_unknown],
    );
    isolate_deps_map.insert(idx_b, vec![dep_b_to_c]);
    isolate_deps_map.insert(idx_c, vec![]);

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert_eq!(graph.len(), 3);
    assert_eq!(graph.get(&idx_a).unwrap(), &HashSet::from([idx_b, idx_c]));
    assert_eq!(graph.get(&idx_b).unwrap(), &HashSet::from([idx_c]));
    assert!(graph.get(&idx_c).unwrap().is_empty());

    // Validating this graph should succeed
    assert!(validate_isolate_dependency_graph(&graph).is_ok());
}

#[tokio::test]
async fn test_build_and_validate_end_to_end_cyclic() {
    let mapper = IsolateServiceMapper::default();
    let svc_a = create_service_info("domain.com", "pub_a", "iso_a", "svc_a");
    let svc_b = create_service_info("domain.com", "pub_b", "iso_b", "svc_b");

    let idx_a =
        mapper.new_binary_index(vec![svc_a], false, "pub_a".into(), "iso_a".into()).await.unwrap();
    let idx_b =
        mapper.new_binary_index(vec![svc_b], false, "pub_b".into(), "iso_b".into()).await.unwrap();

    let dep_a_to_b =
        create_backend_dep("domain.com", "pub_b", "iso_b", "svc_b", RouteType::Internal);
    let dep_b_to_a =
        create_backend_dep("domain.com", "pub_a", "iso_a", "svc_a", RouteType::Internal);

    let mut isolate_deps_map = HashMap::new();
    isolate_deps_map.insert(idx_a, vec![dep_a_to_b]);
    isolate_deps_map.insert(idx_b, vec![dep_b_to_a]);

    let graph = build_isolate_dependency_graph(&isolate_deps_map, &mapper).await.unwrap();
    assert_eq!(graph.get(&idx_a).unwrap(), &HashSet::from([idx_b]));
    assert_eq!(graph.get(&idx_b).unwrap(), &HashSet::from([idx_a]));

    let validation_result = validate_isolate_dependency_graph(&graph);
    assert!(validation_result.is_err());
    assert!(validation_result
        .unwrap_err()
        .to_string()
        .contains("Circular isolate dependency detected"));
}
