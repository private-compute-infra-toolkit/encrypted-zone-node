// Copyright 2025 Google LLC
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

use anyhow::{ensure, Context, Result};
use container_manager::{ContainerManager, ContainerManagerArgs};
use container_manager_request::{
    ContainerManagerRequest, MountReadOnlyFile, MountWritableFile, ResetIsolateRequest,
};
use container_manager_requester::ContainerManagerRequester;
use container_test_utils::{FakeContainer, Status};
use data_scope::request::{
    ValidateBackendDependencyRequest, ValidateManifestInputScopeRequest,
    ValidateManifestOutputScopeRequest,
};
use data_scope::{
    error::DataScopeError, manifest_validator::ManifestValidator, request::GetIsolateRequest,
    requester::DataScopeRequester,
};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    isolate_ez_bridge_client::IsolateEzBridgeClient, ControlPlaneMetadata, CreateMemshareRequest,
    EzPayloadIsolateScope, InvokeEzRequest, InvokeEzResponse, IsolateDataScope, IsolateState,
    IsolateStatus, NotifyIsolateStateRequest, NotifyIsolateStateResponse,
};
use ez_service_proto::enforcer::v1::CallRequest;
use hyper_util::rt::TokioIo;
use interceptor::{Interceptor, RequestType};
use isolate_ez_service_manager::{IsolateEzServiceManager, IsolateEzServiceManagerDependencies};
use isolate_info::{BinaryServicesIndex, IsolateId, IsolateServiceIndex, IsolateServiceInfo};
use isolate_service_mapper::IsolateServiceMapper;
use junction_test_utils::FakeJunction;
use manifest_proto::enforcer::IsolateRuntimeConfigs;
use payload_proto::enforcer::v1::EzPayloadData;
use shared_memory_manager::SharedMemManager;
use simple_tonic_stream::SimpleStreamingWrapper;
use state_manager::{IsolateStateManager, IsolateStateManagerError};
use std::fs::OpenOptions;
use std::path::PathBuf;
use tokio::net::UnixStream;
use tokio::sync::mpsc::channel;
use tokio::task::spawn_blocking;
use tokio::time::{sleep, timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

const CHANNEL_SIZE: usize = 64;
const MAX_DECODING_SIZE: usize = 4 * 1024 * 1024;
const JSON_MANIFEST_PATH_ONE_ISOLATE: &str =
    "enforcer/container/manager/test/testdata/test_manifest_one_isolate.json";
const JSON_MANIFEST_PATH_MULTIPLE_ISOLATE: &str =
    "enforcer/container/manager/test/testdata/test_manifest_multiple_isolates.json";
const JSON_MANIFEST_PATH_RATIFIED_ISOLATE: &str =
    "enforcer/container/manager/test/testdata/test_manifest_ratified_isolate.json";
const JSON_MANIFEST_PATH_WRONG_DOMAIN: &str =
    "enforcer/container/manager/test/testdata/test_manifest_wrong_domain.json";
const JSON_MANIFEST_PATH_INTERCEPTOR: &str =
    "enforcer/container/manager/test/testdata/test_manifest_interceptor.json";

// The following constants are from the test manifests.
// From test_manifest_one_isolate.json
const HELLOWORLD_BINARY: &str = "/usr/local/bin/main";
const HELLOWORLD_DOMAIN: &str = "helloworld_domain";
const GREETER_SERVICE: &str = "Greeter";
const SAY_HELLO_METHOD: &str = "SayHello";
const RATIFIED_ISOLATE_DOMAIN: &str = "EZ_Trusted";

// From test_manifest_multiple_isolates.json
const SUMMATION_BINARY: &str = "/usr/local/bin/summation_by_lookup_table_with_backend";
const PRECOMPUTED_BACKEND_BINARY: &str = "/usr/local/bin/summation_precomputed_backend";
const PLAYGROUND_EXAMPLE_DOMAIN: &str = "playground_example";
const SUMMATION_SERVICE: &str = "SimpleAdd";
const PRECOMPUTED_BACKEND_SERVICE: &str = "PrecomputedBackend";

const TEST_ENFORCER_WRITE_FILE: &str = "temp-test-enforcer-write";
const TEST_CONTAINER_WRITE_FILE: &str = "temp-test-container-write";
const TEST_ENFORCER_READ_FILE: &str = "temp-test-enforcer-read";
const TEST_CONTAINER_READ_FILE: &str = "temp-test-container-read";
const DEV_SHM_PATH: &str = "/dev/shm/";

#[derive(Debug)]
struct TestHarness {
    isolate_junction: FakeJunction,
    isolate_state_manager: IsolateStateManager,
    isolate_service_mapper: IsolateServiceMapper,
    isolate_ez_service_manager: IsolateEzServiceManager,
    manifest_validator: ManifestValidator,
    container_manager_requester: ContainerManagerRequester,
    data_scope_requester: DataScopeRequester,
    shared_memory_manager: SharedMemManager,
    container_manager: ContainerManager<FakeContainer>,
    interceptor: Interceptor,
}

impl TestHarness {
    async fn new(
        manifest_path: &str,
        isolate_runtime_configs: &IsolateRuntimeConfigs,
    ) -> Result<Self> {
        // Clear the tracker for test isolation.
        FakeContainer::clear_tracker();

        let (container_manager_request_tx, container_manager_request_rx) =
            channel::<ContainerManagerRequest>(CHANNEL_SIZE);

        let container_manager_requester =
            ContainerManagerRequester::new(container_manager_request_tx);
        let data_scope_requester = DataScopeRequester::new(u64::MAX);
        let isolate_state_manager = IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        );
        let isolate_service_mapper = IsolateServiceMapper::default();
        let manifest_validator = ManifestValidator::default();
        let shared_memory_manager = SharedMemManager::new(container_manager_requester.clone());
        let isolate_junction = FakeJunction::default();
        let interceptor = Interceptor::new(isolate_service_mapper.clone());

        let isolate_ez_service_manager =
            IsolateEzServiceManager::new(IsolateEzServiceManagerDependencies {
                isolate_junction: Box::new(isolate_junction.clone()),
                isolate_state_manager: isolate_state_manager.clone(),
                shared_memory_manager: shared_memory_manager.clone(),
                external_proxy_connector: None,
                isolate_service_mapper: isolate_service_mapper.clone(),
                data_scope_requester: data_scope_requester.clone(),
                manifest_validator: manifest_validator.clone(),
                ez_to_ez_outbound_handler: None,
                max_decoding_message_size: MAX_DECODING_SIZE,
            });

        let container_manager_args = ContainerManagerArgs {
            isolate_junction: Box::new(isolate_junction.clone()),
            container_manager_request_rx,
            isolate_state_manager: isolate_state_manager.clone(),
            isolate_service_mapper: isolate_service_mapper.clone(),
            isolate_ez_service_mngr: isolate_ez_service_manager.clone(),
            manifest_validator: manifest_validator.clone(),
            shared_mem_manager: shared_memory_manager.clone(),
            manifest_path: manifest_path.to_string(),
            common_bind_mounts: vec![],
            max_decoding_message_size: MAX_DECODING_SIZE,
            isolate_runtime_configs: isolate_runtime_configs.clone(),
            interceptor: interceptor.clone(),
        };

        let container_manager = ContainerManager::<FakeContainer>::start(container_manager_args)
            .await
            .context("Container Manager should start")?;

        let harness = Self {
            isolate_junction,
            isolate_state_manager,
            isolate_service_mapper,
            isolate_ez_service_manager,
            manifest_validator,
            container_manager_requester,
            data_scope_requester,
            shared_memory_manager,
            container_manager,
            interceptor,
        };

        // Wait for the Isolate to be connected by the ContainerManager.
        timeout(Duration::from_secs(30), async {
            while harness.isolate_junction.connected_isolates.is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("Isolate did not connect within the timeout")?;

        Ok(harness)
    }

    async fn stop(&mut self) {
        let mut isolate_ids = Vec::with_capacity(3);
        self.isolate_junction
            .connected_isolates
            .iter()
            .for_each(|entry| isolate_ids.push(*entry.key()));
        for isolate_id in isolate_ids.iter() {
            self.isolate_ez_service_manager.stop_isolate_ez_server(*isolate_id).await
        }
        self.container_manager.stop().await;
    }
}

// This test checks that:
// 1. A container is started up correctly.
// 2. The Isolate is added to the Junction.
// 3. Manifest-defined data scopes are correctly validated.
// 4. The Isolate can interact with the IsolateEZ Service.
// 5. The ContainerManager correctly tracks the state of the Isolate.
#[tokio::test]
async fn test_start_one_isolate() {
    let mut harness =
        TestHarness::new(JSON_MANIFEST_PATH_ONE_ISOLATE, &IsolateRuntimeConfigs::default())
            .await
            .expect("TestHarness::new should succeed");

    let isolate_and_uds_vec =
        check_container_started(vec![HELLOWORLD_BINARY]).await.expect("Container should start");
    assert_eq!(isolate_and_uds_vec.len(), 1);
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) =
        isolate_and_uds_vec[0].clone();

    // Assert that Isolate is added in junction
    assert_eq!(harness.isolate_junction.connected_isolates.len(), 1);
    let isolate_id = *harness.isolate_junction.connected_isolates.iter().next().unwrap().key();

    let mut client =
        ensure_isolate_ready(&mut harness, isolate_ez_bridge_enforcer_side_uds_path, isolate_id)
            .await
            .expect("Isolate should be ready");

    let binary_services_index = get_binary_service_index(
        &mut harness,
        HELLOWORLD_DOMAIN.to_string(),
        GREETER_SERVICE.to_string(),
    )
    .await
    .expect("Should get valid binary index");
    assert_eq!(binary_services_index, isolate_id.get_binary_services_index());

    check_valid_manifest_scopes(
        &mut harness,
        binary_services_index,
        DataScopeType::Public,
        DataScopeType::Public,
    )
    .await
    .expect("Valid manifest scope validation should pass");

    check_invalid_manifest_scopes(
        &mut harness,
        binary_services_index,
        DataScopeType::UserPrivate,
        DataScopeType::UserPrivate,
    )
    .await
    .expect("Invalid manifest scope validation should pass");

    // Ensure that the scope is correctly parsed.
    let get_isolate_resp = harness
        .data_scope_requester
        .get_isolate(GetIsolateRequest {
            binary_services_index,
            data_scope_type: DataScopeType::Public,
        })
        .await
        .expect("Should be valid Get Response");
    assert_eq!(get_isolate_resp.isolate_id, isolate_id);

    // Ensure that a request for a stricter scope fails.
    let get_isolate_err = harness
        .data_scope_requester
        .get_isolate(GetIsolateRequest {
            binary_services_index,
            data_scope_type: DataScopeType::UserPrivate,
        })
        .await
        .expect_err("Should fail to fetch user scope isolate");
    assert!(matches!(get_isolate_err, DataScopeError::NoMatchingIsolates));

    // Verify Isolate can interact with IsolateEZ Service
    let invoke_ez_request = create_random_request(&IsolateServiceInfo {
        operator_domain: HELLOWORLD_DOMAIN.to_string(),
        service_name: GREETER_SERVICE.to_string(),
    });
    let expected_response = create_echo_invoke_ez_response(invoke_ez_request.clone());
    let response =
        invoke_ez(&mut client, invoke_ez_request).await.expect("should be able to invoke ez");
    assert_eq!(response, expected_response);

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

// This test verifies the file mounting capabilities of the ContainerManager. It checks that:
// 1. An Isolate is started correctly.
// 2. A writable file can be successfully mounted into the Isolate's container.
// 3. The mounted writable file appears with the correct paths on both the enforcer and container side.
// 4. A read-only file can be successfully mounted.
// 5. The mounted read-only file also has the correct paths.
#[tokio::test]
async fn test_mount_files() {
    let mut harness =
        TestHarness::new(JSON_MANIFEST_PATH_ONE_ISOLATE, &IsolateRuntimeConfigs::default())
            .await
            .expect("TestHarness::new should succeed");

    let isolate_and_uds_vec =
        check_container_started(vec![HELLOWORLD_BINARY]).await.expect("Container should start");
    assert_eq!(isolate_and_uds_vec.len(), 1);
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) =
        isolate_and_uds_vec[0].clone();

    // Assert that Isolate is added in junction
    assert_eq!(harness.isolate_junction.connected_isolates.len(), 1);
    let isolate_id = *harness.isolate_junction.connected_isolates.iter().next().unwrap().key();

    ensure_isolate_ready(&mut harness, isolate_ez_bridge_enforcer_side_uds_path, isolate_id)
        .await
        .expect("Isolate should be ready");

    assert!(harness
        .container_manager_requester
        .mount_writable_file(MountWritableFile {
            isolate_id,
            region_size: 128,
            enforcer_file_name: TEST_ENFORCER_WRITE_FILE.to_string(),
            container_file_name: TEST_CONTAINER_WRITE_FILE.to_string(),
        })
        .await
        .is_ok());

    // Assert that writable file was mounted
    let tracker = FakeContainer::get_tracker();
    assert_eq!(tracker.len(), 1);
    let tracked_container = tracker.get(&fake_container_id).expect("Should have isolate");
    let write_mounts_map = &tracked_container.value().mounts;
    assert_eq!(write_mounts_map.len(), 1);
    let write_mount_entry = write_mounts_map.iter().next().expect("Should have one entry");
    assert_eq!(*write_mount_entry.0, format!("{}{}", DEV_SHM_PATH, TEST_ENFORCER_WRITE_FILE));
    assert_eq!(write_mount_entry.1, TEST_CONTAINER_WRITE_FILE);
    drop(tracked_container);

    // Check that the file was created on the enforcer side.
    assert!(std::path::Path::new(&format!("{}{}", DEV_SHM_PATH, TEST_ENFORCER_WRITE_FILE)).exists());

    assert!(harness
        .container_manager_requester
        .mount_read_only_file(MountReadOnlyFile {
            isolate_id,
            enforcer_file_name: TEST_ENFORCER_READ_FILE.to_string(),
            container_file_name: TEST_CONTAINER_READ_FILE.to_string(),
        })
        .await
        .is_ok());

    // Assert that readable file was mounted
    let tracker = FakeContainer::get_tracker();
    assert_eq!(tracker.len(), 1);
    let tracked_container = tracker.get(&fake_container_id).expect("Should have isolate");
    let read_mounts_map = &tracked_container.value().readonly_mounts;
    assert_eq!(read_mounts_map.len(), 1);
    let read_mount_entry = read_mounts_map.iter().next().expect("Should have one entry");
    assert_eq!(*read_mount_entry.0, format!("{}{}", DEV_SHM_PATH, TEST_ENFORCER_READ_FILE));
    assert_eq!(read_mount_entry.1, TEST_CONTAINER_READ_FILE);
    drop(tracked_container);

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

// This test verifies the Isolate reset functionality. It checks that:
// 1. Multiple Isolates are started correctly from a manifest.
// 2. Isolates can be made ready and are correctly tracked.
// 3. Shared memory can be created and shared between Isolates.
// 4. Manifest-defined data scopes are correctly validated for an Isolate.
// 5. A specific Isolate can be identified and requested for reset.
// 6. After a reset, the old Isolate's container is stopped.
// 7. A new Isolate is started to replace the reset one, and is connected to the junction.
// 8. State related to the old Isolate (e.g., in SharedMemManager) is cleaned up.
#[tokio::test]
async fn test_isolate_reset() {
    let mut harness =
        TestHarness::new(JSON_MANIFEST_PATH_MULTIPLE_ISOLATE, &IsolateRuntimeConfigs::default())
            .await
            .expect("TestHarness::new should succeed");

    let isolate_and_uds_vec =
        check_container_started(vec![SUMMATION_BINARY, PRECOMPUTED_BACKEND_BINARY])
            .await
            .expect("Container should start");
    assert_eq!(isolate_and_uds_vec.len(), 2);
    let mut fake_container_ids: Vec<u64> = Vec::with_capacity(2);

    for tracked_container in FakeContainer::get_tracker().iter() {
        assert_eq!(tracked_container.value().status, Status::Started);
        let isolate_ez_bridge_enforcer_side_uds_path =
            tracked_container.isolate_ez_bridge_enforcer_side_uds_path.to_owned().unwrap();
        fake_container_ids.push(*tracked_container.key());

        let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
            .await
            .expect("Isolate should be ready");
    }

    // Assert that Isolate is added in junction
    assert_eq!(harness.isolate_junction.connected_isolates.len(), 2);

    let mut isolate_ids: Vec<IsolateId> = vec![];
    harness
        .isolate_junction
        .connected_isolates
        .iter()
        .for_each(|entry| isolate_ids.push(*entry.key()));
    assert_eq!(isolate_ids.len(), 2);

    let mem_share_response = harness
        .shared_memory_manager
        .create_shared_mem_file(isolate_ids[0], CreateMemshareRequest { region_size: 128 })
        .await;

    harness
        .shared_memory_manager
        .share_file(isolate_ids[0], isolate_ids[1], mem_share_response.shared_memory_handle.clone())
        .await
        .expect("File share should succeed");

    // Get Binary Service Index
    let binary_services_index = get_binary_service_index(
        &mut harness,
        PLAYGROUND_EXAMPLE_DOMAIN.to_string(),
        PRECOMPUTED_BACKEND_SERVICE.to_string(),
    )
    .await
    .expect("Should get Binary Service Index");

    check_valid_manifest_scopes(
        &mut harness,
        binary_services_index,
        DataScopeType::UserPrivate,
        DataScopeType::UserPrivate,
    )
    .await
    .expect("Valid manifest scope validation should pass");

    check_invalid_manifest_scopes(
        &mut harness,
        binary_services_index,
        DataScopeType::MultiUserPrivate,
        DataScopeType::MultiUserPrivate,
    )
    .await
    .expect("Invalid manifest scope validation should pass");

    // Get Isolate to remove
    let get_isolate_response = harness
        .data_scope_requester
        .get_isolate(GetIsolateRequest {
            binary_services_index,
            data_scope_type: DataScopeType::Public,
        })
        .await
        .expect("Isolate Get should be Successful");

    assert!(harness
        .container_manager_requester
        .reset_container(ResetIsolateRequest { isolate_id: get_isolate_response.isolate_id })
        .await
        .is_ok());

    // Isolate is already removed, so shared memory manager should fail
    harness
        .shared_memory_manager
        .share_file(isolate_ids[0], isolate_ids[1], mem_share_response.shared_memory_handle)
        .await
        .expect_err("Shared Memory Manager Map should be empty by now");
    harness
        .isolate_state_manager
        .update_state(get_isolate_response.isolate_id, IsolateState::Ready)
        .await
        .expect_err("Isolate should be unrecognized");

    let mut old_container_stopped = false;
    for tracked_container in FakeContainer::get_tracker().iter() {
        let binary_filename =
            tracked_container.binary_filename.to_owned().expect("Should have binary filename");
        if tracked_container.status == Status::Started
            && binary_filename.eq(PRECOMPUTED_BACKEND_BINARY)
        {
            // If this assert fails, the test will timeout because the read end of the ready pipe will never be opened.
            assert!(
                !fake_container_ids.contains(tracked_container.key()),
                "This should be the new isolate not in fake_container_ids"
            );
            let isolate_ez_bridge_enforcer_side_uds_path =
                tracked_container.isolate_ez_bridge_enforcer_side_uds_path.to_owned().unwrap();

            // Find the newly added Isolate and mark the Isolate ready.
            // Also consume the read end of the ready pipe for the new Isolate.
            let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
                .await
                .expect("Isolate should be ready");
        }

        // The old container for the precomputed backend should have stopped
        if fake_container_ids.contains(tracked_container.key())
            && binary_filename.eq(PRECOMPUTED_BACKEND_BINARY)
        {
            assert_eq!(tracked_container.status, Status::Stopped);
            old_container_stopped = true;
        }
    }

    // Total container count should now be 3
    assert_eq!(harness.isolate_junction.connected_isolates.len(), 3);
    assert!(old_container_stopped, "The old container was not stopped.");

    harness.stop().await;
    // The original containers should be stopped. The new one is stopped by harness.stop().
    for fake_container_id in fake_container_ids {
        ensure_isolate_stopped(fake_container_id).await.expect("Isolate should stop");
    }
}

// This test verifies the Isolate config override functionality. It checks that:
// 1. A single container is started correctly with config configs.
// 2. Isolates can be made ready and are correctly tracked.
// 3. The created container contains the overridden configuration.
#[tokio::test]
async fn test_start_one_isolate_with_override() {
    let etc_hosts = "172.28.58.209	csd.c.googlers.com csd";
    let configs = IsolateRuntimeConfigs {
        configs: vec![manifest_proto::enforcer::IsolateRuntimeConfig {
            publisher_id: HELLOWORLD_DOMAIN.to_string(),
            binary_filename: HELLOWORLD_BINARY.to_string(),
            command_line_arguments: vec!["--new_arg".to_string()],
            environment_variables: vec!["NEW_ENV=1".to_string()],
            etc_hosts: etc_hosts.to_string(),
        }],
    };
    let mut harness = TestHarness::new(JSON_MANIFEST_PATH_ONE_ISOLATE, &configs)
        .await
        .expect("TestHarness::new should succeed");

    let isolate_and_uds_vec =
        check_container_started(vec![HELLOWORLD_BINARY]).await.expect("Container should start");
    assert_eq!(isolate_and_uds_vec.len(), 1);
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) =
        isolate_and_uds_vec[0].clone();
    let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
        .await
        .expect("Isolate should be ready");

    {
        // We must put the check inside a scope to delete those after use.
        // Otherwise, harness.stop will hang forever.
        let tracker = FakeContainer::get_tracker();
        let tracked_container = tracker.iter().next().unwrap();
        assert_eq!(tracked_container.value().command_line_arguments, vec!["--new_arg".to_string()]);
        assert_eq!(
            tracked_container.value().env,
            vec!["NEW_ENV=1".to_string(), "EZ_MAX_DECODING_MESSAGE_SIZE=4194304".to_string()]
        );
        let etc_hosts_mount = tracked_container
            .value()
            .boot_mounts
            .iter()
            .find(|m| m.destination == PathBuf::from("/etc/hosts"))
            .expect("should have /etc/hosts mount");

        let contents = std::fs::read(etc_hosts_mount.source.clone()).unwrap();
        assert_eq!(contents, etc_hosts.as_bytes().to_vec());
    }

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

// This test verifies that Ratified Isolates are correctly identified. It checks that:
// 1. An Isolate is started from a manifest specifying it as ratified.
// 2. The resulting IsolateId correctly reports that it is for a Ratified Isolate.
// 3. The BinaryServicesIndex associated with the Isolate's binary also correctly
//    reports that it is for a ratified binary.
// 4. Manifest-defined data scopes are correctly validated.
#[tokio::test]
async fn test_ratified_isolate() {
    let mut harness =
        TestHarness::new(JSON_MANIFEST_PATH_RATIFIED_ISOLATE, &IsolateRuntimeConfigs::default())
            .await
            .expect("TestHarness::new should succeed");

    let isolate_and_uds_vec =
        check_container_started(vec![HELLOWORLD_BINARY]).await.expect("Container should start");
    assert_eq!(isolate_and_uds_vec.len(), 1);
    let (fake_container_id, isolate_ez_bridge_enforcer_side_uds_path) =
        isolate_and_uds_vec[0].clone();

    // Assert that Isolate is added in junction
    assert_eq!(harness.isolate_junction.connected_isolates.len(), 1);
    let isolate_id = *harness.isolate_junction.connected_isolates.iter().next().unwrap().key();

    let mut client =
        ensure_isolate_ready(&mut harness, isolate_ez_bridge_enforcer_side_uds_path, isolate_id)
            .await
            .expect("Isolate should be ready");

    let binary_services_index = get_binary_service_index(
        &mut harness,
        RATIFIED_ISOLATE_DOMAIN.to_string(),
        GREETER_SERVICE.to_string(),
    )
    .await
    .expect("Should get valid binary index");

    assert!(isolate_id.is_ratified_isolate());
    assert!(binary_services_index.is_ratified_binary());
    assert_eq!(binary_services_index, isolate_id.get_binary_services_index());

    check_valid_manifest_scopes(
        &mut harness,
        binary_services_index,
        DataScopeType::Public,
        DataScopeType::Public,
    )
    .await
    .expect("Valid manifest scope validation should pass");

    check_invalid_manifest_scopes(
        &mut harness,
        binary_services_index,
        DataScopeType::UserPrivate,
        DataScopeType::UserPrivate,
    )
    .await
    .expect("Invalid manifest scope validation should pass");

    // Ensure that the scope is correctly parsed.
    let get_isolate_resp = harness
        .data_scope_requester
        .get_isolate(GetIsolateRequest {
            binary_services_index,
            data_scope_type: DataScopeType::Public,
        })
        .await
        .expect("Should be valid Get Response");
    assert_eq!(get_isolate_resp.isolate_id, isolate_id);

    // Ensure that a request for a stricter scope fails.
    let get_isolate_err = harness
        .data_scope_requester
        .get_isolate(GetIsolateRequest {
            binary_services_index,
            data_scope_type: DataScopeType::UserPrivate,
        })
        .await
        .expect_err("Should fail to fetch user scope isolate");
    assert!(matches!(get_isolate_err, DataScopeError::NoMatchingIsolates));

    // Verify Isolate can interact with IsolateEZ Service
    let invoke_ez_request = create_random_request(&IsolateServiceInfo {
        operator_domain: RATIFIED_ISOLATE_DOMAIN.to_string(),
        service_name: GREETER_SERVICE.to_string(),
    });
    let expected_response = create_echo_invoke_ez_response(invoke_ez_request.clone());
    let response =
        invoke_ez(&mut client, invoke_ez_request).await.expect("should be able to invoke ez");
    assert_eq!(response, expected_response);

    harness.stop().await;
    ensure_isolate_stopped(fake_container_id).await.expect("Container should stop");
}

#[tokio::test]
async fn test_ratified_isolate_wrong_domain_manifest() {
    let _ = TestHarness::new(JSON_MANIFEST_PATH_WRONG_DOMAIN, &IsolateRuntimeConfigs::default())
        .await
        .expect_err("TestHarness::new should fail");
}

// This test verifies that backend dependencies declared in the manifest are correctly processed and enforced. It checks that:
// 1. Multiple Isolates, including one with a backend dependency, are started correctly.
// 2. The `ManifestValidator` is correctly populated with the dependency information from the manifest.
// 3. A call to `validate_backend_dependency` succeeds for a dependency that is explicitly allowed in the manifest.
// 4. A call to `validate_backend_dependency` fails for a dependency that is not listed in the manifest.
#[tokio::test]
async fn test_backend_dependencies() {
    let mut harness =
        TestHarness::new(JSON_MANIFEST_PATH_MULTIPLE_ISOLATE, &IsolateRuntimeConfigs::default())
            .await
            .expect("TestHarness::new should succeed");

    // Wait for containers to start
    let isolate_and_uds_vec =
        check_container_started(vec![SUMMATION_BINARY, PRECOMPUTED_BACKEND_BINARY])
            .await
            .expect("Container should start");
    assert_eq!(isolate_and_uds_vec.len(), 2);
    let mut fake_container_ids: Vec<u64> = Vec::with_capacity(2);

    for tracked_container in FakeContainer::get_tracker().iter() {
        assert_eq!(tracked_container.value().status, Status::Started);
        let isolate_ez_bridge_enforcer_side_uds_path =
            tracked_container.isolate_ez_bridge_enforcer_side_uds_path.to_owned().unwrap();
        fake_container_ids.push(*tracked_container.key());

        let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
            .await
            .expect("Isolate should be ready");
    }

    // Get the BinaryServicesIndex for the binary that has a dependency.
    let summation_binary_index = get_binary_service_index(
        &mut harness,
        PLAYGROUND_EXAMPLE_DOMAIN.to_string(),
        SUMMATION_SERVICE.to_string(),
    )
    .await
    .expect("Should get valid binary index for summation service");

    // Get the BinaryServicesIndex for the dependency binary.
    let dependency_binary_index = get_binary_service_index(
        &mut harness,
        PLAYGROUND_EXAMPLE_DOMAIN.to_string(),
        PRECOMPUTED_BACKEND_SERVICE.to_string(),
    )
    .await
    .expect("Should get valid binary index for backend service");

    // Get the IsolateServiceIndex for the dependency service.
    let backend_service_index = harness
        .isolate_service_mapper
        .get_service_index(&IsolateServiceInfo {
            operator_domain: PLAYGROUND_EXAMPLE_DOMAIN.to_string(),
            service_name: PRECOMPUTED_BACKEND_SERVICE.to_string(),
        })
        .await
        .expect("Should get isolate index for backend service");
    assert_eq!(
        backend_service_index
            .get_binary_services_index()
            .expect("Should have a valid binary services index for backend service"),
        dependency_binary_index
    );

    // Validate that the dependency is allowed.
    harness
        .manifest_validator
        .validate_backend_dependency(ValidateBackendDependencyRequest {
            binary_services_index: summation_binary_index,
            destination_isolate_service: backend_service_index,
        })
        .await
        .expect("Valid backend dependency should be allowed");

    // Validate that a non-existent dependency is not allowed.
    let unknown_service_index = IsolateServiceIndex::new(None, "domain.unknown", false)
        .expect("Valid isolate service index");

    let result = harness
        .manifest_validator
        .validate_backend_dependency(ValidateBackendDependencyRequest {
            binary_services_index: summation_binary_index,
            destination_isolate_service: unknown_service_index,
        })
        .await;
    assert!(matches!(result, Err(DataScopeError::BackendDependencyNotAllowed)));

    harness.stop().await;
    for fake_container_id in fake_container_ids {
        ensure_isolate_stopped(fake_container_id).await.expect("Isolate should stop");
    }
}

#[tokio::test]
async fn test_interceptor() {
    let mut harness =
        TestHarness::new(JSON_MANIFEST_PATH_INTERCEPTOR, &IsolateRuntimeConfigs::default())
            .await
            .expect("TestHarness::new should succeed");

    // Wait for containers to start
    let isolate_and_uds_vec =
        check_container_started(vec![SUMMATION_BINARY, PRECOMPUTED_BACKEND_BINARY])
            .await
            .expect("Container should start");
    assert_eq!(isolate_and_uds_vec.len(), 2);

    for tracked_container in FakeContainer::get_tracker().iter() {
        let isolate_ez_bridge_enforcer_side_uds_path =
            tracked_container.isolate_ez_bridge_enforcer_side_uds_path.to_owned().unwrap();
        let _client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path)
            .await
            .expect("Isolate should be ready");
    }

    let mut call_request = create_call_request_for_interceptor();
    harness.interceptor.replace_with_interceptor(&mut call_request, RequestType::Unary).await;
    assert_eq!(call_request.operator_domain, RATIFIED_ISOLATE_DOMAIN);
    assert_eq!(call_request.service_name, "PrecomputedBackend");
    assert_eq!(call_request.method_name, "Fetch");

    let mut call_request = create_call_request_for_interceptor();
    harness.interceptor.replace_with_interceptor(&mut call_request, RequestType::Streaming).await;
    assert_eq!(call_request.operator_domain, RATIFIED_ISOLATE_DOMAIN);
    assert_eq!(call_request.service_name, "PrecomputedBackend");
    assert_eq!(call_request.method_name, "StreamingFetch");

    harness.stop().await;
}

// Helper function to create a gRPC client for the IsolateEzBridge service
// over a Unix Domain Socket.
async fn start_isolate_ez_bridge(
    isolate_ez_bridge_enforcer_side_uds_path: &str,
) -> Result<IsolateEzBridgeClient<Channel>> {
    // Wait for the Isolate EZ server to start up
    let uds_path = isolate_ez_bridge_enforcer_side_uds_path.to_string();
    let open_future = spawn_blocking(move || {
        let isolate_fifo_path = format!("{}.ready", uds_path);
        OpenOptions::new().read(true).open(isolate_fifo_path).unwrap();
    });
    timeout(Duration::from_millis(100), open_future)
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for Isolate EZ server to be ready."))?
        .map_err(|e| anyhow::anyhow!("Failed to open ready pipe: {}", e))?;

    let endpoint = Endpoint::from_shared(
        "http://localhost:".to_owned() + isolate_ez_bridge_enforcer_side_uds_path,
    )
    .context("Invalid endpoint")?;
    let channel = endpoint
        .connect_with_connector(service_fn(|uri: Uri| async move {
            let path = uri.path();
            Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(path).await?))
        }))
        .await
        .context("Invalid channel")?;
    Ok(IsolateEzBridgeClient::new(channel))
}

// Helper function to notify the IsolateEzBridge service that an Isolate is ready.
// It sends a `NotifyIsolateStateRequest` with the `Ready` state and waits for
// a `NotifyIsolateStateResponse`.
// CAUTION: Use this function only to mark an Isolate ready so that the read end of
// the ready pipe is opened and the test doesn't gets stalled.
async fn notify_ready(
    client: &mut IsolateEzBridgeClient<Channel>,
) -> Result<NotifyIsolateStateResponse> {
    let (notify_isolate_state_tx, notify_isolate_state_rx) =
        channel::<NotifyIsolateStateRequest>(CHANNEL_SIZE);
    let outbound_stream = ReceiverStream::new(notify_isolate_state_rx);
    let inbound = client
        .notify_isolate_state(outbound_stream)
        .await
        .context("Failed to get inbound channel")?;
    notify_isolate_state_tx
        .send(NotifyIsolateStateRequest { new_isolate_state: IsolateState::Ready.into() })
        .await
        .context("Failed to get notify isolate sender")?;

    let mut invoke_isolate_response_stream: SimpleStreamingWrapper<NotifyIsolateStateResponse> =
        inbound.into_inner().into();
    let response = invoke_isolate_response_stream.message().await;
    ensure!(response.is_some(), "Failed to receive Notify Isolate State Response");
    Ok(response.unwrap())
}

// Helper function to send a unary `InvokeEzRequest` to the IsolateEzBridge service
// and return the `InvokeEzResponse`.
async fn invoke_ez(
    client: &mut IsolateEzBridgeClient<Channel>,
    invoke_ez_request: InvokeEzRequest,
) -> Result<InvokeEzResponse> {
    Ok(client.invoke_ez(invoke_ez_request).await?.into_inner())
}

// Helper function to create a sample `InvokeEzRequest` with a random payload for testing.
fn create_random_request(isolate_service_info: &IsolateServiceInfo) -> InvokeEzRequest {
    // Generate 8 random bytes for request
    let random_request_data =
        [0u8; 8].into_iter().map(|_| rand::random::<u8>()).collect::<Vec<_>>();
    let original_msg_id = rand::random();

    InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata {
            ipc_message_id: original_msg_id,
            requester_spiffe: String::new(),
            requester_is_local: false,
            responder_is_local: true,
            destination_operator_domain: isolate_service_info.operator_domain.clone(),
            destination_service_name: isolate_service_info.service_name.clone(),
            destination_method_name: SAY_HELLO_METHOD.to_string(),
            shared_memory_handles: Vec::new(),
            destination_ez_instance_id: "".to_string(),
            ..Default::default()
        }),
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                mapped_scope_owner: None,
            }],
        }),
        isolate_request_payload: Some(EzPayloadData {
            datagrams: vec![random_request_data.to_vec()],
        }),
    }
}

// Helper function to create an `InvokeEzResponse` that echoes the data from an
// `InvokeEzRequest`, simulating an echo service for tests.
fn create_echo_invoke_ez_response(invoke_isolate_request: InvokeEzRequest) -> InvokeEzResponse {
    InvokeEzResponse {
        control_plane_metadata: invoke_isolate_request.control_plane_metadata,
        status: Some(IsolateStatus::default()),
        ez_response_iscope: invoke_isolate_request.isolate_request_iscope,
        ez_response_payload: invoke_isolate_request.isolate_request_payload,
    }
}

// Helper function to verify that containers have been started correctly. It checks that the
// number of tracked containers matches the expected count and that their status is `Started`.
// It returns a vector of tuples, each containing a container's ID and its UDS path.
async fn check_container_started(binary_file_names: Vec<&str>) -> Result<Vec<(u64, String)>> {
    // Assert the container was created and tracked.
    let tracker = FakeContainer::get_tracker();
    ensure!(
        tracker.len() == binary_file_names.len(),
        "Tracker length mismatched. Expected: {}, Got: {}",
        binary_file_names.len(),
        tracker.len()
    );

    let mut results: Vec<(u64, String)> = vec![];
    for entry in tracker.iter() {
        let container_data = entry.value();
        let actual_binary_filename = container_data
            .binary_filename
            .as_ref()
            .context("Expected a valid binary filename, but it was None")?;
        ensure!(
            binary_file_names.contains(&actual_binary_filename.as_str()),
            "Binary Filename not found: Expected from {:?}, Got: {}",
            binary_file_names,
            actual_binary_filename
        );
        ensure!(
            container_data.status == Status::Started,
            "Status mismatched. Expected: {:?}, Got: {:?}",
            Status::Started,
            container_data.status
        );

        let isolate_ez_bridge_enforcer_side_uds_path = container_data
            .isolate_ez_bridge_enforcer_side_uds_path
            .as_ref()
            .context("Expected a valid UDS path for isolate-ez-bridge, but it was None")?
            .clone();
        results.push((*entry.key(), isolate_ez_bridge_enforcer_side_uds_path));
    }

    Ok(results)
}

// Helper function that establishes a client connection to an Isolate's gRPC server
// and sends a `Ready` state notification.
async fn notify_isolate_ready(
    isolate_ez_bridge_enforcer_side_uds_path: String,
) -> Result<IsolateEzBridgeClient<Channel>> {
    // Verify that the server is started by IsolateEzServiceManager
    let mut client = start_isolate_ez_bridge(&isolate_ez_bridge_enforcer_side_uds_path).await?;
    // Notify Isolate Ready
    notify_ready(&mut client).await.context("Should notify ready")?;
    Ok(client)
}

// Helper function to ensure an Isolate is fully ready for testing. It notifies the
// Isolate that it is ready and then verifies that the `IsolateStateManager` correctly
// handles the state update.
async fn ensure_isolate_ready(
    harness: &mut TestHarness,
    isolate_ez_bridge_enforcer_side_uds_path: String,
    isolate_id: IsolateId,
) -> Result<IsolateEzBridgeClient<Channel>> {
    let client = notify_isolate_ready(isolate_ez_bridge_enforcer_side_uds_path).await?;

    // Assert that Isolate is added in Isolate State Manager via IsolateEZ Bridge
    ensure!(
        harness.isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.is_err(),
        "Expected state update to fail with IsolateStateManagerError::DuplicateStateUpdate."
    );

    let err = harness
        .isolate_state_manager
        .update_state(isolate_id, IsolateState::Ready)
        .await
        .err()
        .unwrap();
    ensure!(
        matches!(err.downcast().unwrap(), IsolateStateManagerError::DuplicateStateUpdate),
        "Expected IsolateStateManagerError::DuplicateStateUpdate error"
    );

    Ok(client)
}

// Helper function to retrieve the `BinaryServicesIndex` for a given service,
// abstracting the interaction with the `IsolateServiceMapper`.
async fn get_binary_service_index(
    harness: &mut TestHarness,
    operator_domain: String,
    service_name: String,
) -> Result<BinaryServicesIndex> {
    // Assert that Isolate is added in Isolate Service Mapper
    harness
        .isolate_service_mapper
        .get_binary_index(&IsolateServiceInfo { operator_domain, service_name })
        .await
        .context("Should be valid binary index")
}

// Helper function to ensure that a container has been stopped. It checks the
// container's status in the `FakeContainer` tracker.
async fn ensure_isolate_stopped(fake_container_id: u64) -> Result<()> {
    // Assert the container was stopped.
    let tracker = FakeContainer::get_tracker();
    let tracked_container = tracker.get(&fake_container_id).context("Should have isolate")?;
    ensure!(
        tracked_container.value().status == Status::Stopped,
        "Expected container status to be Stopped, but it was {:?}",
        tracked_container.value().status
    );
    Ok(())
}

// Helper function to verify that the manifest's data scopes are correctly validated.
async fn check_valid_manifest_scopes(
    harness: &mut TestHarness,
    binary_services_index: BinaryServicesIndex,
    pass_input_scope: DataScopeType,
    pass_output_scope: DataScopeType,
) -> Result<()> {
    harness
        .manifest_validator
        .validate_input_scope(ValidateManifestInputScopeRequest {
            binary_services_index,
            requested_scope: pass_input_scope,
        })
        .await
        .context("Validation of input manifest scope should pass")?;
    harness
        .manifest_validator
        .validate_output_scope(ValidateManifestOutputScopeRequest {
            binary_services_index,
            emitted_scope: pass_output_scope,
        })
        .await
        .context("Validation of output manifest scope should pass")?;
    Ok(())
}

async fn check_invalid_manifest_scopes(
    harness: &mut TestHarness,
    binary_services_index: BinaryServicesIndex,
    fail_input_scope: DataScopeType,
    fail_output_scope: DataScopeType,
) -> Result<()> {
    let input_scope_result = harness
        .manifest_validator
        .validate_input_scope(ValidateManifestInputScopeRequest {
            binary_services_index,
            requested_scope: fail_input_scope,
        })
        .await;
    ensure!(
        matches!(input_scope_result, Err(DataScopeError::DisallowedByManifest)),
        "Validation of input manifest scope should fail with DisallowedByManifest, but got {:?}",
        input_scope_result
    );

    let output_scope_result = harness
        .manifest_validator
        .validate_output_scope(ValidateManifestOutputScopeRequest {
            binary_services_index,
            emitted_scope: fail_output_scope,
        })
        .await;
    ensure!(
        matches!(output_scope_result, Err(DataScopeError::DisallowedByManifest)),
        "Validation of output manifest scope should fail with DisallowedByManifest, but got {:?}",
        output_scope_result
    );

    Ok(())
}

fn create_call_request_for_interceptor() -> CallRequest {
    CallRequest {
        operator_domain: "playground_example".to_string(),
        service_name: "SimpleAdd".to_string(),
        method_name: "StreamingIntegerSequence".to_string(),
        ..Default::default()
    }
}
