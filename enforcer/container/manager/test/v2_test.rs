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
        vec![
            setup_startup_parameters(),
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, RATIFIED_1_ISOLATE_NAME, 1),
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, RATIFIED_2_ISOLATE_NAME, 1),
            startup_parameters(HELLOWORLD_DOMAIN, HELLOWORLD_ISOLATE_NAME, 1),
        ],
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
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
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
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
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
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
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
        assert!(setup_container.value().command_line_arguments.is_empty());

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
        vec![
            setup_startup_parameters(),
            IsolateStartupParameters {
                command_line_arguments: vec!["--foo=bar".to_string()],
                environment_variables: vec!["KEY=value".to_string()],
                ..startup_parameters(EXPLICIT_OPAQUE_DOMAIN, EXPLICIT_OPAQUE_ISOLATE_NAME, 2)
            },
        ],
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    assert_eq!(setup_containers.len(), 1);
    let _client = notify_isolate_ready(setup_containers[0].1.clone())
        .await
        .expect("Setup isolate should be ready");

    // The fetched number_of_isolates = 2 overrides the one embedded in the manifest (3).
    harness
        .load_workload_manifests_from_paths(
            None,
            Some("enforcer/manifest_parser/test/testdata/v2_opaque_explicit_startup_params.json"),
        )
        .await
        .expect("Should load explicit opaque manifest");

    let workload_containers =
        start_and_ready_containers_in_dependency_order(vec![ENTRYPOINT_BINARY, ENTRYPOINT_BINARY])
            .await
            .expect("All 2 opaque replicas should start and be readied");
    assert_eq!(workload_containers.len(), 2);

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
            2,
            "2 container instances must be spawned for number_of_isolates = 2"
        );
        for oc in opaque_containers {
            assert_eq!(oc.value().command_line_arguments, vec!["--foo=bar".to_string()]);
            assert!(oc.value().env.iter().any(|e| e == "KEY=value"));
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
        vec![
            setup_startup_parameters(),
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, RATIFIED_1_ISOLATE_NAME, 1),
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, RATIFIED_2_ISOLATE_NAME, 1),
            startup_parameters(HELLOWORLD_DOMAIN, HELLOWORLD_ISOLATE_NAME, 1),
        ],
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
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
            service_name: GREETER_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Ratified Greeter should be registered");
    assert!(service_info_has_valid_binary_index(&greeter_index));
    let auth_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
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
        vec![setup_startup_parameters()],
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
        vec![setup_startup_parameters()],
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
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![
            setup_startup_parameters(),
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, "crypto_oracle", 1),
        ],
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
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
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

#[tokio::test]
async fn test_v2_start_fails_without_setup_isolate_startup_parameters() {
    let err = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![],
    )
    .await
    .expect_err("ContainerManager should not start without Setup Isolate startup parameters");
    assert!(format!("{err:#}").contains("No IsolateStartupParameters"), "{err:#}");
}

#[tokio::test]
async fn test_v2_start_fails_with_isolate_runtime_configs() {
    let runtime_configs = IsolateRuntimeConfigs {
        configs: vec![manifest_proto::enforcer::v1::IsolateRuntimeConfig {
            publisher_id: V2_RATIFIED_PUBLISHER_ID.to_string(),
            binary_filename: SETUP_BINARY.to_string(),
            command_line_arguments: vec!["--foo=bar".to_string()],
            ..Default::default()
        }],
    };
    let err = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &runtime_configs,
        "test_operator".to_string(),
        vec![setup_startup_parameters()],
    )
    .await
    .expect_err("ContainerManager should not start with IsolateRuntimeConfigs in Manifest V2");
    assert!(format!("{err:#}").contains("IsolateRuntimeConfigs are not supported"), "{err:#}");
}

#[tokio::test]
async fn test_v2_start_fails_with_command_line_arguments_for_setup_isolate() {
    let err = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![IsolateStartupParameters {
            command_line_arguments: vec!["--foo=bar".to_string()],
            ..setup_startup_parameters()
        }],
    )
    .await
    .expect_err("Setup Isolate startup parameters must not set command_line_arguments");
    assert!(format!("{err:#}").contains("cannot have command-line arguments"), "{err:#}");
}

#[tokio::test]
async fn test_v2_start_fails_with_environment_variables_for_setup_isolate() {
    let err = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![IsolateStartupParameters {
            environment_variables: vec!["KEY=value".to_string()],
            ..setup_startup_parameters()
        }],
    )
    .await
    .expect_err("Setup Isolate startup parameters must not set environment_variables");
    assert!(format!("{err:#}").contains("cannot have environment variables"), "{err:#}");
}

#[tokio::test]
async fn test_v2_start_fails_with_zero_number_of_isolates_for_setup_isolate() {
    let err = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![startup_parameters(V2_RATIFIED_PUBLISHER_ID, SETUP_ISOLATE_NAME, 0)],
    )
    .await
    .expect_err("Setup Isolate must have number_of_isolates >= 1");
    assert!(format!("{err:#}").contains("number_of_isolates must be >= 1"), "{err:#}");
}

#[tokio::test]
async fn test_v2_load_workload_manifests_fails_with_startup_parameters_for_unknown_isolate() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![
            setup_startup_parameters(),
            startup_parameters(HELLOWORLD_DOMAIN, HELLOWORLD_ISOLATE_NAME, 1),
            startup_parameters(HELLOWORLD_DOMAIN, "unknown", 1),
        ],
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) = setup_containers[0].clone();
    let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
        .await
        .expect("Setup isolate should be ready");

    let err = harness
        .load_workload_manifests_from_paths(
            None,
            Some("enforcer/manifest_parser/test/testdata/v2_opaque.json"),
        )
        .await
        .expect_err("Startup parameters for an unknown Isolate should be rejected");
    assert!(format!("{err:#}").contains("unknown Isolate"), "{err:#}");

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

#[tokio::test]
async fn test_v2_load_workload_manifests_fails_with_zero_number_of_isolates_for_ratified_isolate() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![
            setup_startup_parameters(),
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, RATIFIED_1_ISOLATE_NAME, 0),
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, RATIFIED_2_ISOLATE_NAME, 1),
        ],
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) = setup_containers[0].clone();
    let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
        .await
        .expect("Setup isolate should be ready");

    let err = harness
        .load_workload_manifests_from_paths(
            Some("enforcer/manifest_parser/test/testdata/v2_ratified.json"),
            None,
        )
        .await
        .expect_err("Ratified Isolate with number_of_isolates = 0 should be rejected");
    assert!(format!("{err:#}").contains("number_of_isolates must be >= 1"), "{err:#}");

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

#[tokio::test]
async fn test_v2_load_workload_manifests_fails_with_reserved_environment_variables_for_opaque() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![
            setup_startup_parameters(),
            IsolateStartupParameters {
                environment_variables: vec!["EZ_OPERATOR_ROLE=spoofed".to_string()],
                ..startup_parameters(HELLOWORLD_DOMAIN, HELLOWORLD_ISOLATE_NAME, 1)
            },
        ],
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) = setup_containers[0].clone();
    let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
        .await
        .expect("Setup isolate should be ready");

    let err = harness
        .load_workload_manifests_from_paths(
            None,
            Some("enforcer/manifest_parser/test/testdata/v2_opaque.json"),
        )
        .await
        .expect_err("Opaque Isolate startup parameters must not set reserved EZ_ variables");
    assert!(format!("{err:#}").contains("reserved 'EZ_' environment variables"), "{err:#}");

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

#[tokio::test]
async fn test_v2_load_workload_manifests_fails_with_negative_number_of_isolates_for_opaque() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![
            setup_startup_parameters(),
            startup_parameters(HELLOWORLD_DOMAIN, HELLOWORLD_ISOLATE_NAME, -1),
        ],
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) = setup_containers[0].clone();
    let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
        .await
        .expect("Setup isolate should be ready");

    let err = harness
        .load_workload_manifests_from_paths(
            None,
            Some("enforcer/manifest_parser/test/testdata/v2_opaque.json"),
        )
        .await
        .expect_err("Opaque Isolate with negative number_of_isolates should be rejected");
    assert!(format!("{err:#}").contains("number_of_isolates cannot be negative"), "{err:#}");

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

#[tokio::test]
async fn test_v2_starts_isolate_with_slash_in_publisher_id() {
    let slash_publisher_id = "google.com/pcit-release-bot";
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![
            setup_startup_parameters(),
            startup_parameters(slash_publisher_id, "crypto_oracle", 1),
        ],
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    let _client = notify_isolate_ready(setup_containers[0].1.clone())
        .await
        .expect("Setup isolate should be ready");

    let mut workload_manifests = load_workload_manifests_from_paths(
        Some("enforcer/manifest_parser/test/testdata/v2_ratified_external_publisher.json"),
        None::<&str>,
    )
    .expect("Should parse workload manifest");
    workload_manifests.ratified_isolate_manifest.as_mut().unwrap().ratified_isolate_descriptors
        [0]
    .isolate_type
    .as_mut()
    .unwrap()
    .publisher_id = slash_publisher_id.to_string();

    let parsed_isolates =
        workload_manifests.clone().into_parsed_isolates().expect("Failed to parse isolates");
    let resp = harness
        .container_manager_requester
        .load_workload_manifests(LoadWorkloadManifestsRequest { workload_manifests })
        .await
        .expect("Should load workload manifest with slash in publisher_id");
    let isolate_packages: HashMap<BinaryServicesIndex, String> = resp
        .registered_indices
        .into_iter()
        .zip(parsed_isolates)
        .map(|(idx, i)| (idx, i.package_filename))
        .collect();
    harness
        .container_manager_requester
        .load_workload_isolates(LoadWorkloadIsolatesRequest { isolate_packages })
        .await
        .expect("Should launch isolate with slash in publisher_id");

    let workload_containers =
        start_and_ready_containers_in_dependency_order(vec![HELLOWORLD_BINARY])
            .await
            .expect("Workload container with slash in publisher_id should start and be readied");

    let greeter_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: slash_publisher_id.to_string(),
            service_name: GREETER_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Service should be registered under publisher_id containing slash");
    assert!(service_info_has_valid_binary_index(&greeter_index));

    harness.stop().await;
    for (id, _) in setup_containers.into_iter().chain(workload_containers.into_iter()) {
        ensure_isolate_stopped(id).await.expect("Container should stop");
    }
}
