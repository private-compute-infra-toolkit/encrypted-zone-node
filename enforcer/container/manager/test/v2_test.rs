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

use std::collections::HashMap;

use container_manager_test_utils::*;
use container_test_utils::FakeContainer;
use isolate_info::{BinaryServicesIndex, IsolateServiceInfo};
use manifest_parser::v2::WorkloadManifests;
use manifest_proto::enforcer::v1::IsolateRuntimeConfigs;

#[tokio::test]
async fn test_container_manager_v2_start_multiple_isolates() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    assert_eq!(setup_containers.len(), 1);
    let _client = notify_isolate_ready(setup_containers[0].1.clone())
        .await
        .expect("Setup isolate should be ready");

    // Load composite workload manifests (2 ratified + 1 opaque) dynamically
    harness
        .load_workload_manifests_from_paths(
            Some("enforcer/manifest_parser/test/testdata/v2_ratified.json"),
            Some("enforcer/manifest_parser/test/testdata/v2_opaque.json"),
        )
        .await
        .expect("Should load workload manifests");

    let workload_containers = start_and_ready_containers_in_dependency_order(vec![
        HELLOWORLD_BINARY,
        HELLOWORLD_BINARY,
        HELLOWORLD_BINARY,
    ])
    .await
    .expect("Workload containers should start and be readied");
    assert_eq!(workload_containers.len(), 3);

    // Verify SetupService (from setup isolate)
    let setup_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: SETUP_ISOLATE_DOMAIN.to_string(),
            service_name: SETUP_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("SetupService should be registered");
    assert!(service_info_has_valid_binary_index(&setup_index));

    // Verify Greeter service (from ratified isolate 1)
    let greeter_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: RATIFIED_ISOLATE_DOMAIN.to_string(),
            service_name: GREETER_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Ratified Greeter should be registered");
    assert!(service_info_has_valid_binary_index(&greeter_index));

    // Verify AuthService (from ratified isolate 2)
    let auth_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: RATIFIED_ISOLATE_DOMAIN.to_string(),
            service_name: AUTH_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Ratified AuthService should be registered");
    assert!(service_info_has_valid_binary_index(&auth_index));

    // Verify Greeter service (from opaque isolate)
    let opaque_greeter_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: HELLOWORLD_DOMAIN.to_string(),
            service_name: GREETER_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Opaque Greeter should be registered");
    assert!(service_info_has_valid_binary_index(&opaque_greeter_index));

    let all_containers: Vec<_> =
        setup_containers.into_iter().chain(workload_containers.into_iter()).collect();

    {
        let tracker = FakeContainer::get_tracker();

        // Verify Setup container args
        let setup_container = tracker
            .iter()
            .find(|c| c.value().binary_filename.as_deref() == Some(SETUP_BINARY))
            .expect("Setup container should be tracked");
        assert_eq!(setup_container.value().command_line_arguments, vec!["--init".to_string()]);

        // Verify Ratified container args
        let ratified_containers: Vec<_> = tracker
            .iter()
            .filter(|c| {
                c.value().binary_filename.as_deref() == Some(HELLOWORLD_BINARY)
                    && !c.value().command_line_arguments.is_empty()
            })
            .collect();
        assert_eq!(ratified_containers.len(), 2);
        assert_eq!(ratified_containers[0].value().command_line_arguments, vec!["".to_string()]);
        assert_eq!(ratified_containers[1].value().command_line_arguments, vec!["".to_string()]);

        // Verify Opaque container args (must be empty from manifest)
        tracker
            .iter()
            .find(|c| {
                c.value().binary_filename.as_deref() == Some(HELLOWORLD_BINARY)
                    && c.value().command_line_arguments.is_empty()
            })
            .expect("Opaque container should be tracked with empty command line args");

        // Verify Layer 1 Enforcer platform environment variables on all containers
        for container_ref in tracker.iter() {
            let envs = &container_ref.value().env;
            assert!(envs.iter().any(|e| e.starts_with("EZ_MAX_DECODING_MESSAGE_SIZE=")));
            assert!(envs.iter().any(|e| e.starts_with("EZ_SHM_NUM_SLOTS=")));
            assert!(envs.iter().any(|e| e.starts_with("EZ_SHM_SLOT_SIZE=")));
            assert!(envs.iter().any(|e| e.starts_with("EZ_SHM_PAYLOAD_THRESHOLD=")));
            assert!(envs.iter().any(|e| e == "EZ_OPERATOR_ROLE=test_operator"));
        }
    }

    harness.stop().await;
    for (id, _) in all_containers {
        ensure_isolate_stopped(id).await.expect("Container should stop");
    }
}

#[tokio::test]
async fn test_v2_explicit_startup_parameters_scaling() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    assert_eq!(setup_containers.len(), 1);
    let _client = notify_isolate_ready(setup_containers[0].1.clone())
        .await
        .expect("Setup isolate should be ready");

    // Load explicit opaque manifest (number_of_isolates = 3)
    harness
        .load_workload_manifests_from_paths(
            None,
            Some("enforcer/manifest_parser/test/testdata/v2_opaque_explicit_startup_params.json"),
        )
        .await
        .expect("Should load explicit opaque manifest");

    let workload_containers = start_and_ready_containers_in_dependency_order(vec![
        ENTRYPOINT_BINARY,
        ENTRYPOINT_BINARY,
        ENTRYPOINT_BINARY,
    ])
    .await
    .expect("All 3 opaque replicas should start and be readied");
    assert_eq!(workload_containers.len(), 3);

    let all_containers: Vec<_> =
        setup_containers.into_iter().chain(workload_containers.into_iter()).collect();

    {
        let tracker = FakeContainer::get_tracker();
        let opaque_containers: Vec<_> = tracker
            .iter()
            .filter(|c| c.value().binary_filename.as_deref() == Some(ENTRYPOINT_BINARY))
            .collect();
        assert_eq!(
            opaque_containers.len(),
            3,
            "3 container instances must be spawned for number_of_isolates = 3"
        );
        for oc in opaque_containers {
            assert_eq!(oc.value().command_line_arguments, Vec::<String>::new());
        }
    }

    harness.stop().await;
    for (id, _) in all_containers {
        ensure_isolate_stopped(id).await.expect("Container should stop");
    }
}

#[tokio::test]
async fn test_v2_load_workload_manifests_without_starting_isolates() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");
    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    assert_eq!(setup_containers.len(), 1);
    let (_fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) =
        setup_containers[0].clone();
    let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
        .await
        .expect("Setup isolate should be ready");
    let workload_manifests = load_workload_manifests_from_paths(
        Some("enforcer/manifest_parser/test/testdata/v2_ratified.json"),
        Some("enforcer/manifest_parser/test/testdata/v2_opaque.json"),
    )
    .expect("Should parse workload manifests");
    let response = harness
        .container_manager_requester
        .load_workload_manifests(LoadWorkloadManifestsRequest {
            workload_manifests: workload_manifests.clone(),
        })
        .await
        .expect("Should load workload manifests without starting isolates");
    let indices = response.registered_indices;
    assert_eq!(indices.len(), 3);
    let greeter_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: RATIFIED_ISOLATE_DOMAIN.to_string(),
            service_name: GREETER_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Ratified Greeter should be registered");
    assert!(service_info_has_valid_binary_index(&greeter_index));
    let auth_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: RATIFIED_ISOLATE_DOMAIN.to_string(),
            service_name: AUTH_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Ratified AuthService should be registered");
    assert!(service_info_has_valid_binary_index(&auth_index));
    let isolate_packages: HashMap<BinaryServicesIndex, String> = indices
        .into_iter()
        .zip(workload_manifests.into_parsed_isolates().expect("Failed to parse workload isolates"))
        .map(|(idx, i)| (idx, i.package_filename))
        .collect();
    let started_response = harness
        .container_manager_requester
        .load_workload_isolates(LoadWorkloadIsolatesRequest { isolate_packages })
        .await
        .expect("Should start workload isolates via load_workload_isolates");
    assert_eq!(started_response.loaded_indices.len(), 3);
    let workload_containers = start_and_ready_containers_in_dependency_order(vec![
        HELLOWORLD_BINARY,
        HELLOWORLD_BINARY,
        HELLOWORLD_BINARY,
    ])
    .await
    .expect("Workload containers should start and be readied");
    assert_eq!(workload_containers.len(), 3);
    harness.stop().await;
    for (id, _) in setup_containers.into_iter().chain(workload_containers.into_iter()) {
        ensure_isolate_stopped(id).await.expect("Container should stop");
    }
}

#[tokio::test]
async fn test_v2_empty_workload_manifests_returns_error() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    assert_eq!(setup_containers.len(), 1);
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) = setup_containers[0].clone();
    let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
        .await
        .expect("Setup isolate should be ready");

    let result = harness
        .container_manager_requester
        .load_workload_isolates(LoadWorkloadIsolatesRequest { isolate_packages: HashMap::new() })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().to_string(), "isolate packages cannot be empty");

    let manifest_only_result = harness
        .container_manager_requester
        .load_workload_manifests(LoadWorkloadManifestsRequest {
            workload_manifests: WorkloadManifests::default(),
        })
        .await;
    assert!(manifest_only_result.is_err());
    assert_eq!(manifest_only_result.unwrap_err().to_string(), "workload isolates cannot be empty");

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

#[tokio::test]
async fn test_container_manager_v2_get_setup_isolate_client() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    assert_eq!(setup_containers.len(), 1);
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) = setup_containers[0].clone();
    let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
        .await
        .expect("Setup isolate should be ready");

    let client = harness
        .container_manager_requester
        .get_setup_isolate_client()
        .await
        .expect("get_setup_isolate_client should succeed");
    assert!(client.is_some());

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

#[tokio::test]
async fn test_v2_ratified_isolate_accepts_non_ez_trusted_publisher() {
    const EXTERNAL_PUBLISHER: &str = "pcit-release-bot@google.com";

    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    let _client = notify_isolate_ready(setup_containers[0].1.clone())
        .await
        .expect("Setup isolate should be ready");

    harness
        .load_workload_manifests_from_paths(
            Some("enforcer/manifest_parser/test/testdata/v2_ratified_external_publisher.json"),
            None,
        )
        .await
        .expect("Ratified isolate with a non EZ_Trusted publisher should load");

    let workload_containers =
        start_and_ready_containers_in_dependency_order(vec![HELLOWORLD_BINARY])
            .await
            .expect("Workload container should start and be readied");

    let greeter_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: EXTERNAL_PUBLISHER.to_string(),
            service_name: GREETER_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Ratified Greeter should be registered under its own publisher domain");
    assert!(service_info_has_valid_binary_index(&greeter_index));

    harness.stop().await;
    for (id, _) in setup_containers.into_iter().chain(workload_containers.into_iter()) {
        ensure_isolate_stopped(id).await.expect("Container should stop");
    }
}
