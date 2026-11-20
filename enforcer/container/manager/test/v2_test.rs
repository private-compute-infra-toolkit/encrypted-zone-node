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
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, PUBSUB_BROKER_ISOLATE_NAME, 1),
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

    harness
        .load_workload_manifests_from_paths(V2_WORKLOAD_MANIFEST_PATHS)
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

    let pubsub_broker_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
            service_name: PUBSUB_BROKER_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Ratified PubSubBroker should be registered");
    assert!(service_info_has_valid_binary_index(&pubsub_broker_index));

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

        // Each container is started with the arguments and environment variables of its manifest.
        let mut arguments_and_variables: Vec<_> = tracker
            .iter()
            .map(|c| (c.value().command_line_arguments.clone(), manifest_variables(&c.value().env)))
            .collect();
        arguments_and_variables.sort();
        assert_eq!(
            arguments_and_variables,
            [
                // pubsub_broker
                (vec![], vec![]),
                // helloworld
                (
                    to_strings(&["--foo=manifest", "--verbose"]),
                    to_strings(&["KEY=manifest", "LOG_LEVEL=info"])
                ),
                // setup_isolate
                (to_strings(&["--mode=setup"]), to_strings(&["SETUP_ENV=1"])),
                // ratified_2
                (to_strings(&["--policy-flag=active"]), to_strings(&["BASE_ENV=1"])),
            ]
        );

        // Verify Layer 1 Enforcer platform environment variables on all containers
        let mut observed_identities: Vec<(String, String)> = Vec::new();
        for container_ref in tracker.iter() {
            let envs = &container_ref.value().env;
            assert!(envs.iter().any(|e| e.starts_with("EZ_MAX_DECODING_MESSAGE_SIZE=")));
            assert!(envs.iter().any(|e| e.starts_with("EZ_SHM_NUM_SLOTS=")));
            assert!(envs.iter().any(|e| e.starts_with("EZ_SHM_SLOT_SIZE=")));
            assert!(envs.iter().any(|e| e.starts_with("EZ_SHM_PAYLOAD_THRESHOLD=")));
            assert!(envs.iter().any(|e| e == "EZ_OPERATOR_ROLE=test_operator"));
            let isolate_name_env = envs
                .iter()
                .find(|e| e.starts_with(ISOLATE_NAME_ENV_PREFIX))
                .expect("Container should have EZ_ISOLATE_NAME")
                .clone();
            let publisher_id_env = envs
                .iter()
                .find(|e| e.starts_with(PUBLISHER_ID_ENV_PREFIX))
                .expect("Container should have EZ_PUBLISHER_ID")
                .clone();
            observed_identities.push((isolate_name_env, publisher_id_env));
        }
        observed_identities.sort();
        let mut expected_identities = vec![
            (
                format!("{ISOLATE_NAME_ENV_PREFIX}{SETUP_ISOLATE_NAME}"),
                format!("{PUBLISHER_ID_ENV_PREFIX}{V2_RATIFIED_PUBLISHER_ID}"),
            ),
            (
                format!("{ISOLATE_NAME_ENV_PREFIX}{PUBSUB_BROKER_ISOLATE_NAME}"),
                format!("{PUBLISHER_ID_ENV_PREFIX}{V2_RATIFIED_PUBLISHER_ID}"),
            ),
            (
                format!("{ISOLATE_NAME_ENV_PREFIX}{RATIFIED_2_ISOLATE_NAME}"),
                format!("{PUBLISHER_ID_ENV_PREFIX}{V2_RATIFIED_PUBLISHER_ID}"),
            ),
            (
                format!("{ISOLATE_NAME_ENV_PREFIX}{HELLOWORLD_ISOLATE_NAME}"),
                format!("{PUBLISHER_ID_ENV_PREFIX}{HELLOWORLD_DOMAIN}"),
            ),
        ];
        expected_identities.sort();
        assert_eq!(observed_identities, expected_identities);
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
                override_command_line_arguments: vec![
                    "--foo=bar".to_string(),
                    "--verbose=false".to_string(),
                ],
                override_environment_variables: vec!["KEY=value".to_string()],
                ..startup_parameters(HELLOWORLD_DOMAIN, HELLOWORLD_ISOLATE_NAME, 2)
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

    harness
        .load_workload_manifests_from_paths(&[
            "enforcer/manifest_parser/test/testdata/v2_opaque_helloworld.json",
        ])
        .await
        .expect("Should load opaque manifest");

    let workload_containers =
        start_and_ready_containers_in_dependency_order(vec![HELLOWORLD_BINARY, HELLOWORLD_BINARY])
            .await
            .expect("All 2 opaque replicas should start and be readied");
    assert_eq!(workload_containers.len(), 2);

    let all_containers: Vec<_> =
        setup_containers.into_iter().chain(workload_containers.into_iter()).collect();

    {
        let tracker = FakeContainer::get_tracker();
        let opaque_containers: Vec<_> = tracker
            .iter()
            .filter(|c| c.value().binary_filename.as_deref() == Some(HELLOWORLD_BINARY))
            .collect();
        assert_eq!(
            opaque_containers.len(),
            2,
            "2 container instances must be spawned for number_of_isolates = 2"
        );
        let expected_isolate_name_env =
            format!("{ISOLATE_NAME_ENV_PREFIX}{HELLOWORLD_ISOLATE_NAME}");
        let expected_publisher_id_env = format!("{PUBLISHER_ID_ENV_PREFIX}{HELLOWORLD_DOMAIN}");
        // --foo, --verbose, and KEY from the manifest are replaced by the overrides, which are added
        // after the remaining entries.
        for oc in opaque_containers {
            assert_eq!(oc.value().command_line_arguments, ["--foo=bar", "--verbose=false"]);
            assert_eq!(manifest_variables(&oc.value().env), ["LOG_LEVEL=info", "KEY=value"]);
            assert!(oc.value().env.iter().any(|e| e == &expected_isolate_name_env));
            assert!(oc.value().env.iter().any(|e| e == &expected_publisher_id_env));
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
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, PUBSUB_BROKER_ISOLATE_NAME, 1),
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
    let workload_manifests = load_workload_manifests_from_paths(V2_WORKLOAD_MANIFEST_PATHS)
        .expect("Should parse workload manifests");
    let response = harness
        .container_manager_requester
        .load_workload_manifests(LoadWorkloadManifestsRequest { workload_manifests })
        .await
        .expect("Should load workload manifests without starting isolates");
    let indices = response.registered_indices;
    assert_eq!(indices.len(), 3);
    let pubsub_broker_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
            service_name: PUBSUB_BROKER_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Ratified PubSubBroker should be registered");
    assert!(service_info_has_valid_binary_index(&pubsub_broker_index));
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
    let isolate_packages: HashMap<BinaryServicesIndex, String> =
        indices.into_iter().map(|idx| (idx, WORKLOAD_PACKAGE_PATH.to_string())).collect();
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
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, RATIFIED_2_ISOLATE_NAME, 1),
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
        .load_workload_manifests_from_paths(&[
            "enforcer/manifest_parser/test/testdata/v2_ratified_2.json",
        ])
        .await
        .expect("Ratified isolate with a non EZ_Trusted publisher should load");

    let workload_containers =
        start_and_ready_containers_in_dependency_order(vec![HELLOWORLD_BINARY])
            .await
            .expect("Workload container should start and be readied");

    let auth_service_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
            service_name: AUTH_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Ratified AuthService should be registered under its own publisher domain");
    assert!(service_info_has_valid_binary_index(&auth_service_index));

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
            override_command_line_arguments: vec!["--foo=bar".to_string()],
            ..setup_startup_parameters()
        }],
    )
    .await
    .expect_err("Setup Isolate startup parameters must not set override_command_line_arguments");
    assert!(format!("{err:#}").contains("cannot have command-line arguments"), "{err:#}");
}

#[tokio::test]
async fn test_v2_start_fails_with_environment_variables_for_setup_isolate() {
    let err = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![IsolateStartupParameters {
            override_environment_variables: vec!["KEY=value".to_string()],
            ..setup_startup_parameters()
        }],
    )
    .await
    .expect_err("Setup Isolate startup parameters must not set override_environment_variables");
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
        .load_workload_manifests_from_paths(&[
            "enforcer/manifest_parser/test/testdata/v2_opaque_helloworld.json",
        ])
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
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, PUBSUB_BROKER_ISOLATE_NAME, 0),
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
        .load_workload_manifests_from_paths(&[
            "enforcer/manifest_parser/test/testdata/v2_ratified_pubsub_broker.json",
            "enforcer/manifest_parser/test/testdata/v2_ratified_2.json",
        ])
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
                override_environment_variables: vec!["EZ_OPERATOR_ROLE=spoofed".to_string()],
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
        .load_workload_manifests_from_paths(&[
            "enforcer/manifest_parser/test/testdata/v2_opaque_helloworld.json",
        ])
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
        .load_workload_manifests_from_paths(&[
            "enforcer/manifest_parser/test/testdata/v2_opaque_helloworld.json",
        ])
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
            startup_parameters(slash_publisher_id, RATIFIED_2_ISOLATE_NAME, 1),
        ],
    )
    .await
    .expect("ContainerManager should start with v2 setup manifest");

    let setup_containers =
        check_container_started(vec![SETUP_BINARY]).await.expect("Setup container should start");
    let _client = notify_isolate_ready(setup_containers[0].1.clone())
        .await
        .expect("Setup isolate should be ready");

    let mut workload_manifests = load_workload_manifests_from_paths(&[
        "enforcer/manifest_parser/test/testdata/v2_ratified_2.json",
    ])
    .expect("Should parse workload manifest");
    workload_manifests.binary_manifests[0].isolate_type.as_mut().unwrap().publisher_id =
        slash_publisher_id.to_string();

    let resp = harness
        .container_manager_requester
        .load_workload_manifests(LoadWorkloadManifestsRequest { workload_manifests })
        .await
        .expect("Should load workload manifest with slash in publisher_id");
    let isolate_packages: HashMap<BinaryServicesIndex, String> = resp
        .registered_indices
        .into_iter()
        .map(|idx| (idx, WORKLOAD_PACKAGE_PATH.to_string()))
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

    let auth_service_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: slash_publisher_id.to_string(),
            service_name: AUTH_SERVICE.to_string(),
            ..Default::default()
        })
        .await
        .expect("Service should be registered under publisher_id containing slash");
    assert!(service_info_has_valid_binary_index(&auth_service_index));

    harness.stop().await;
    for (id, _) in setup_containers.into_iter().chain(workload_containers.into_iter()) {
        ensure_isolate_stopped(id).await.expect("Container should stop");
    }
}

#[tokio::test]
async fn test_v2_load_workload_manifests_fails_with_disallowed_overrides() {
    // "--foo", "--verbose", and "KEY" are listed in allowed_overrides of v2_opaque_helloworld.json.
    let flag_error = "must be --key=value";
    let variable_error = "must be KEY=value";
    let repeat_error = "Duplicate key in";
    for (arguments, variables, rejected, expected_error) in [
        (vec!["--bar=secret"], vec![], "override_command_line_arguments[0]", flag_error),
        // Booleans have to be written as --verbose=true or --verbose=false.
        (vec!["--verbose"], vec![], "override_command_line_arguments[0]", flag_error),
        // KEY is allowed only as a variable, and --foo only as a flag.
        (vec!["KEY=secret"], vec![], "override_command_line_arguments[0]", flag_error),
        (vec![], vec!["--foo=secret"], "override_environment_variables[0]", variable_error),
        (vec![], vec!["OTHER=secret"], "override_environment_variables[0]", variable_error),
        (vec![], vec!["KEY"], "override_environment_variables[0]", variable_error),
        (
            vec![],
            vec!["KEY=value", "KEY=secret"],
            "override_environment_variables[1]",
            repeat_error,
        ),
    ] {
        let mut harness = TestHarness::new_v2(
            "enforcer/manifest_parser/test/testdata/v2_setup.json",
            &IsolateRuntimeConfigs::default(),
            "test_operator".to_string(),
            vec![
                setup_startup_parameters(),
                IsolateStartupParameters {
                    override_command_line_arguments: to_strings(&arguments),
                    override_environment_variables: to_strings(&variables),
                    ..startup_parameters(HELLOWORLD_DOMAIN, HELLOWORLD_ISOLATE_NAME, 1)
                },
            ],
        )
        .await
        .expect("ContainerManager should start with v2 setup manifest");

        let setup_containers = check_container_started(vec![SETUP_BINARY])
            .await
            .expect("Setup container should start");
        let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) =
            setup_containers[0].clone();
        let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
            .await
            .expect("Setup isolate should be ready");

        let err = harness
            .load_workload_manifests_from_paths(&[
                "enforcer/manifest_parser/test/testdata/v2_opaque_helloworld.json",
            ])
            .await
            .expect_err("Overrides outside allowed_overrides should be rejected");
        let err = format!("{err:#}");
        assert!(err.contains(&format!("IsolateStartupParameters.{rejected} for Opaque")), "{err}");
        assert!(err.contains(expected_error), "{err}");
        assert!(!err.contains("secret"), "{err}");

        harness.stop().await;
        ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
    }
}

#[tokio::test]
async fn test_v2_load_workload_manifests_clears_earlier_dependencies_on_label_conflict() {
    let mut harness = TestHarness::new_v2(
        "enforcer/manifest_parser/test/testdata/v2_setup.json",
        &IsolateRuntimeConfigs::default(),
        "test_operator".to_string(),
        vec![
            setup_startup_parameters(),
            startup_parameters(V2_RATIFIED_PUBLISHER_ID, PUBSUB_BROKER_ISOLATE_NAME, 1),
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

    let mut workload_manifests = load_workload_manifests_from_paths(&[
        "enforcer/manifest_parser/test/testdata/v2_ratified_pubsub_broker.json",
    ])
    .expect("Should parse workload manifest");

    let base_dep = workload_manifests.binary_manifests[0].ez_backend_dependencies[0].clone();
    let mut dep_a = base_dep.clone();
    dep_a.service_name = "ServiceA".to_string();
    dep_a.method_name = "Method1".to_string();
    dep_a.target_label = "label-a".to_string();

    let mut dep_b1 = base_dep.clone();
    dep_b1.service_name = "ServiceB".to_string();
    dep_b1.method_name = "Method1".to_string();
    dep_b1.target_label = "label-b".to_string();

    let mut dep_b2 = base_dep;
    dep_b2.service_name = "ServiceB".to_string();
    dep_b2.method_name = "Method2".to_string();
    dep_b2.target_label = "label-c".to_string();

    workload_manifests.binary_manifests[0].ez_backend_dependencies = vec![dep_a, dep_b1, dep_b2];

    let err = harness
        .container_manager_requester
        .load_workload_manifests(LoadWorkloadManifestsRequest { workload_manifests })
        .await
        .expect_err("Conflicting target_label on ServiceB should fail manifest loading");
    assert!(
        format!("{err:#}").contains("Failed to add backend dependency in Manifest Scope Validator"),
        "{err:#}"
    );

    let binary_services_index = harness
        .isolate_service_mapper
        .get_binary_index(&IsolateServiceInfo {
            operator_domain: V2_RATIFIED_PUBLISHER_ID.to_string(),
            service_name: PUBSUB_BROKER_SERVICE.to_string(),
            isolate_name: PUBSUB_BROKER_ISOLATE_NAME.to_string(),
            publisher_id: V2_RATIFIED_PUBLISHER_ID.to_string(),
        })
        .await
        .expect("Binary index should exist");
    let service_a_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: "cdpush".to_string(),
            service_name: "ServiceA".to_string(),
            ..Default::default()
        })
        .await
        .expect("ServiceA index should exist in mapper");

    let validation_result = harness
        .manifest_validator
        .validate_backend_dependency(ValidateBackendDependencyRequest {
            binary_services_index,
            destination_isolate_service: service_a_index,
        })
        .await;
    assert_eq!(
        validation_result,
        Err(DataScopeError::InvalidIsolateServiceIndex),
        "Earlier dependency ServiceA should be cleared after conflict failure"
    );

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

/// Returns the environment variables without the EZ_ ones added by the Enforcer.
fn manifest_variables(env: &[String]) -> Vec<String> {
    env.iter().filter(|e| !e.starts_with("EZ_")).cloned().collect()
}

fn to_strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}
