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

use common_proto::enforcer::v2::IsolateType;
use container_manager::{ContainerManager, ContainerManagerArgs, ManifestSource};
use container_manager_request::ContainerManagerRequest;
use container_manager_requester::{
    ContainerManagerRequester, LoadWorkloadIsolatesResponse, LoadWorkloadManifestsResponse,
};
use container_test_utils::FakeContainer;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::requester::DataScopeRequester;
use ez_management::{EzManagementClient, EzManagementError};
use ez_management_proto::enforcer::v2::ez_management_service_server::{
    EzManagementService, EzManagementServiceServer,
};
use ez_management_proto::enforcer::v2::{
    load_isolates_request, load_isolates_response, AllPackagesLoadedResponse, IsolatePackageChunk,
    LoadIsolatesRequest, LoadIsolatesResponse,
};
use fileshare_manager::FileshareManager;
use interceptor::Interceptor;
use isolate_ez_service_manager::{IsolateEzServiceManager, IsolateEzServiceManagerDependencies};
use isolate_service_mapper::IsolateServiceMapper;
use junction_test_utils::FakeJunction;
use manifest_proto::enforcer::v1::IsolateRuntimeConfigs;
use opaque_isolate_manifest_proto::enforcer::v2::{OpaqueIsolateDescriptor, OpaqueIsolateManifest};
use ratified_isolate_manifest_proto::enforcer::v2::{
    RatifiedIsolateDescriptor, RatifiedIsolateManifest,
};
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const MAX_DECODING_SIZE: usize = 4 * 1024 * 1024;
const CHANNEL_SIZE: usize = 128;
const SHM_NUM_SLOTS: u64 = 10;
const SHM_SLOT_SIZE: u64 = 1024;
const SHM_PAYLOAD_THRESHOLD: u64 = 512;

struct MockManagementService {
    responses_to_send: Arc<Mutex<Vec<LoadIsolatesResponse>>>,
    received_requests: Arc<Mutex<Vec<LoadIsolatesRequest>>>,
}

#[tokio::test]
async fn test_load_isolates_success_flow() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let responses = vec![
        create_ratified_response(create_ratified_manifest(
            "ratified_workload_1",
            "ratified_pkg.tar",
        )),
        create_opaque_response(create_opaque_manifest("opaque_workload_1", "opaque_pkg.tar")),
        // Stream ratified package in 2 chunks
        LoadIsolatesResponse {
            response: Some(load_isolates_response::Response::IsolatePackageChunk(
                IsolatePackageChunk {
                    isolate_package_endorsements: vec![],
                    package_name: "ratified_pkg.tar".to_string(),
                    chunk_sequence: 0,
                    package_tar_chunk: vec![0u8; 512],
                    is_last_chunk: false,
                },
            )),
        },
        LoadIsolatesResponse {
            response: Some(load_isolates_response::Response::IsolatePackageChunk(
                IsolatePackageChunk {
                    isolate_package_endorsements: vec![],
                    package_name: "".to_string(),
                    chunk_sequence: 1,
                    package_tar_chunk: vec![0u8; 512],
                    is_last_chunk: true,
                },
            )),
        },
        create_single_chunk_response("opaque_pkg.tar", vec![0u8; 1024]),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context(responses).await;
    let indices = ctx.client.load_packages().await.expect("load_isolates should succeed");
    assert_eq!(indices.len(), 2);

    // Verify that the ez_packages_ directory was cleaned up after loading packages!
    let mut entries = tokio::fs::read_dir(pkg_temp_dir.path()).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        assert!(!entry.file_name().to_string_lossy().starts_with("ez_packages_"));
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let reqs = ctx.received_requests.lock().unwrap().clone();
    assert_eq!(reqs.len(), 3);
    match &reqs[0].request {
        Some(load_isolates_request::Request::ReadyToLoadPackages(_)) => {}
        other => panic!("Expected ReadyToLoadPackagesRequest, got {:?}", other),
    }
    match &reqs[1].request {
        Some(load_isolates_request::Request::LoadIsolatesResult(res)) => {
            assert_eq!(res.package_name, "ratified_pkg.tar");
            assert!(res.success);
        }
        other => panic!("Expected LoadIsolatesResult, got {:?}", other),
    }
    match &reqs[2].request {
        Some(load_isolates_request::Request::LoadIsolatesResult(res)) => {
            assert_eq!(res.package_name, "opaque_pkg.tar");
            assert!(res.success);
        }
        other => panic!("Expected LoadIsolatesResult, got {:?}", other),
    }
}

#[tokio::test]
async fn test_manifest_ordering_opaque_first() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let responses = vec![
        create_opaque_response(create_opaque_manifest("opaque_workload_1", "opaque_pkg.tar")),
        create_ratified_response(create_ratified_manifest(
            "ratified_workload_1",
            "ratified_pkg.tar",
        )),
        create_single_chunk_response("ratified_pkg.tar", vec![0u8; 1024]),
        create_single_chunk_response("opaque_pkg.tar", vec![0u8; 1024]),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context(responses).await;
    let indices = ctx.client.load_packages().await.expect("load_isolates should succeed");
    assert_eq!(indices.len(), 2);
}

#[tokio::test]
async fn test_package_chunk_before_manifests_fails() {
    let responses = vec![create_single_chunk_response("premature.tar", vec![1, 2, 3])];
    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("before both manifests were received"));
        }
        other => panic!("Expected UnexpectedMessage error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_stream_closed_prematurely() {
    let responses: Vec<LoadIsolatesResponse> = vec![];
    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::StreamError(msg) => {
            assert!(msg.contains("prematurely") || msg.contains("closed"));
        }
        other => panic!("Expected StreamError, got {:?}", other),
    }
}

#[tokio::test]
async fn test_unknown_package_name_in_chunk() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let responses = vec![
        create_ratified_response(create_ratified_manifest("known_workload", "known_pkg.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        create_single_chunk_response("completely_unknown_pkg.tar", vec![0u8; 1024]),
    ];

    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("completely_unknown_pkg.tar"));
            assert!(msg.contains("not present in the manifest"));
        }
        other => panic!("Expected UnexpectedMessage, got {:?}", other),
    }

    assert!(!pkg_temp_dir.path().join("completely_unknown_pkg.tar").exists());

    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "completely_unknown_pkg.tar");
    assert!(!res.success);
}

#[tokio::test]
async fn test_unexpected_manifest_after_initialization_fails() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_1", "ratified_pkg.tar")),
        create_opaque_response(create_opaque_manifest("opaque_1", "opaque_pkg.tar")),
        create_single_chunk_response("ratified_pkg.tar", vec![0u8; 1024]),
        create_ratified_response(RatifiedIsolateManifest::default()),
    ];

    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("unexpected manifest"));
        }
        other => panic!("Expected UnexpectedMessage, got {:?}", other),
    }
}

#[tokio::test]
async fn test_empty_response_during_manifest_phase() {
    let responses = vec![LoadIsolatesResponse { response: None }];
    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("empty"));
        }
        other => panic!("Expected UnexpectedMessage, got {:?}", other),
    }
}

#[tokio::test]
async fn test_missing_ratified_manifest_fails() {
    let opaque_manifest = OpaqueIsolateManifest::default();
    let responses = vec![
        create_opaque_response(opaque_manifest.clone()),
        create_opaque_response(opaque_manifest),
    ];
    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::ManifestParsingFailed(msg) => {
            assert!(msg.contains("Missing RatifiedIsolateManifest"));
        }
        other => panic!("Expected ManifestParsingFailed, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ratified_manifest_payload_missing_inner_manifest_fails() {
    let responses = vec![LoadIsolatesResponse {
        response: Some(load_isolates_response::Response::RatifiedIsolateManifest(
            ez_management_proto::enforcer::v2::RatifiedIsolateManifestPayload {
                manifest: None,
                manifest_endorsements: vec![],
            },
        )),
    }];
    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::ManifestParsingFailed(msg) => {
            assert!(msg.contains("missing inner manifest"));
        }
        other => panic!("Expected ManifestParsingFailed, got {:?}", other),
    }
}

#[tokio::test]
async fn test_missing_opaque_manifest_fails() {
    let ratified_manifest = RatifiedIsolateManifest::default();
    let responses = vec![
        create_ratified_response(ratified_manifest.clone()),
        create_ratified_response(ratified_manifest),
    ];
    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::ManifestParsingFailed(msg) => {
            assert!(msg.contains("Missing OpaqueIsolateManifest"));
        }
        other => panic!("Expected ManifestParsingFailed, got {:?}", other),
    }
}

#[tokio::test]
async fn test_empty_chunk_package_name_first_chunk_fails() {
    let responses = vec![
        create_ratified_response(create_ratified_manifest("test_workload", "test_pkg.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        create_single_chunk_response("", vec![0u8; 10]),
    ];
    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("without package_name"));
        }
        other => panic!("Expected UnexpectedMessage, got {:?}", other),
    }
}

#[tokio::test]
async fn test_empty_response_ignored_during_package_streaming() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let responses = vec![
        create_ratified_response(create_ratified_manifest("test_workload", "test_pkg.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        LoadIsolatesResponse { response: None },
        create_single_chunk_response("test_pkg.tar", vec![0u8; 1024]),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context(responses).await;
    let indices = ctx.client.load_packages().await.expect("load_packages should succeed");
    assert_eq!(indices.len(), 1);
}

#[tokio::test]
async fn test_write_package_to_disk_io_failure() {
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", "/dev/null/forbidden_dir");
    let (container_manager_requester, _cm_temp_dir) = create_fake_container_manager().await;
    let server_temp_dir = tempdir().unwrap();
    let uds_path = server_temp_dir.path().join("ez_mgmt_test.sock");
    use tokio::net::UnixListener;
    use tokio_stream::wrappers::UnixListenerStream;
    let uds = UnixListener::bind(&uds_path).unwrap();
    let uds_stream = UnixListenerStream::new(uds);

    let received_requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let service = MockManagementService {
        responses_to_send: std::sync::Arc::new(std::sync::Mutex::new(vec![])),
        received_requests: received_requests.clone(),
    };
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ez_management_proto::enforcer::v2::ez_management_service_server::EzManagementServiceServer::new(service))
            .serve_with_incoming(uds_stream)
            .await;
    });
    let address = format!("unix:{}", uds_path.to_str().unwrap());
    let result =
        EzManagementClient::new(&address, container_manager_requester, MAX_DECODING_SIZE).await;
    server_handle.abort();
    std::env::remove_var("EZ_PACKAGE_OUTPUT_DIR");

    assert!(result.is_err());
    match result {
        Err(EzManagementError::IoError(_)) => {}
        _other => panic!("Expected IoError"),
    }
}

#[tokio::test]
async fn test_partial_packages_loaded_only_boots_streamed_isolates() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    // Two isolates defined in manifests, but only one package streamed
    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_1", "pkg_1.tar")),
        create_opaque_response(create_opaque_manifest("opaque_2", "pkg_2.tar")),
        create_single_chunk_response("pkg_1.tar", vec![0u8; 1024]),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context(responses).await;
    let indices = ctx
        .client
        .load_packages()
        .await
        .expect("load_packages should succeed for received packages");
    assert_eq!(indices.len(), 1);
}

#[tokio::test]
async fn test_stream_closed_before_all_packages_loaded_fails() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    // Stream closes after packages without sending AllPackagesLoaded
    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_1", "pkg_1.tar")),
        create_opaque_response(create_opaque_manifest("opaque_2", "pkg_2.tar")),
        create_single_chunk_response("pkg_1.tar", vec![0u8; 1024]),
    ];

    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::StreamError(msg) => {
            assert!(msg.contains("before receiving AllPackagesLoaded"));
        }
        other => panic!("Expected StreamError, got {:?}", other),
    }
}

#[tokio::test]
async fn test_all_packages_loaded_during_chunk_stream_fails() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_1", "pkg_1.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        LoadIsolatesResponse {
            response: Some(load_isolates_response::Response::IsolatePackageChunk(
                IsolatePackageChunk {
                    isolate_package_endorsements: vec![],
                    package_name: "pkg_1.tar".to_string(),
                    chunk_sequence: 0,
                    package_tar_chunk: vec![0u8; 512],
                    is_last_chunk: false,
                },
            )),
        },
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("still being streamed"));
        }
        other => panic!("Expected UnexpectedMessage, got {:?}", other),
    }
}

#[tokio::test]
async fn test_chunk_sequence_initial_not_zero_fails() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_1", "pkg_1.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        LoadIsolatesResponse {
            response: Some(load_isolates_response::Response::IsolatePackageChunk(
                IsolatePackageChunk {
                    isolate_package_endorsements: vec![],
                    package_name: "pkg_1.tar".to_string(),
                    chunk_sequence: 1,
                    package_tar_chunk: vec![0u8; 512],
                    is_last_chunk: true,
                },
            )),
        },
    ];

    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("Invalid chunk sequence") && msg.contains("expected 0, got 1"));
        }
        other => panic!("Expected UnexpectedMessage, got {:?}", other),
    }
}

#[tokio::test]
async fn test_chunk_sequence_out_of_order_fails() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_1", "pkg_1.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        LoadIsolatesResponse {
            response: Some(load_isolates_response::Response::IsolatePackageChunk(
                IsolatePackageChunk {
                    isolate_package_endorsements: vec![],
                    package_name: "pkg_1.tar".to_string(),
                    chunk_sequence: 0,
                    package_tar_chunk: vec![0u8; 512],
                    is_last_chunk: false,
                },
            )),
        },
        LoadIsolatesResponse {
            response: Some(load_isolates_response::Response::IsolatePackageChunk(
                IsolatePackageChunk {
                    isolate_package_endorsements: vec![],
                    package_name: "".to_string(),
                    chunk_sequence: 2,
                    package_tar_chunk: vec![0u8; 512],
                    is_last_chunk: true,
                },
            )),
        },
    ];

    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("Invalid chunk sequence") && msg.contains("expected 1, got 2"));
        }
        other => panic!("Expected UnexpectedMessage, got {:?}", other),
    }
}

#[tokio::test]
async fn test_chunk_package_name_mismatch_mid_stream_fails() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_1", "pkg_1.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        LoadIsolatesResponse {
            response: Some(load_isolates_response::Response::IsolatePackageChunk(
                IsolatePackageChunk {
                    isolate_package_endorsements: vec![],
                    package_name: "pkg_1.tar".to_string(),
                    chunk_sequence: 0,
                    package_tar_chunk: vec![0u8; 512],
                    is_last_chunk: false,
                },
            )),
        },
        LoadIsolatesResponse {
            response: Some(load_isolates_response::Response::IsolatePackageChunk(
                IsolatePackageChunk {
                    isolate_package_endorsements: vec![],
                    package_name: "other_pkg.tar".to_string(),
                    chunk_sequence: 1,
                    package_tar_chunk: vec![0u8; 512],
                    is_last_chunk: true,
                },
            )),
        },
    ];

    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("while still streaming package 'pkg_1.tar'"));
        }
        other => panic!("Expected UnexpectedMessage, got {:?}", other),
    }
}

#[tokio::test]
async fn test_path_traversal_in_package_name_fails() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let mut responses = create_single_ratified_manifest_responses("traversal_workload", "..");
    responses.push(create_single_chunk_response("..", vec![0u8; 100]));

    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("cannot resolve file name") || msg.contains("path traversal"));
        }
        other => panic!("Expected UnexpectedMessage error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_empty_package_filename_cannot_resolve_file_name_fails() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let mut responses = create_single_ratified_manifest_responses("no_filename_workload", "/");
    responses.push(create_single_chunk_response("/", vec![0u8; 100]));

    let mut ctx = setup_test_context(responses).await;
    let result = ctx.client.load_packages().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EzManagementError::UnexpectedMessage(msg) => {
            assert!(msg.contains("cannot resolve file name"));
        }
        other => panic!("Expected UnexpectedMessage error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_write_package_to_disk_file_write_failure() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let mut responses =
        create_single_ratified_manifest_responses("fail_write_isolate", "fail_write_pkg.tar");
    responses.push(create_single_chunk_response("fail_write_pkg.tar", vec![0u8; 1024]));

    let mut ctx = setup_test_context(responses).await;
    // Create a directory with the package name so writing to it as a file fails (EISDIR)
    let ez_pkg_dir = get_ez_packages_dir(pkg_temp_dir.path()).await;
    tokio::fs::create_dir(ez_pkg_dir.join("fail_write_pkg.tar")).await.unwrap();
    let result = ctx.client.load_packages().await;
    assert!(matches!(result, Err(EzManagementError::LoadIsolatesFailed(_))));

    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "fail_write_pkg.tar");
    assert!(!res.success);
}

#[tokio::test]
async fn test_load_workload_manifests_container_manager_failure() {
    let requester = create_mock_container_manager_requester(
        Err("Container manager rejected manifests"),
        Ok(()),
    );
    let responses = create_single_ratified_manifest_responses("workload_1", "pkg_1.tar");

    let mut ctx = setup_test_context_with_requester(responses, requester).await;
    let result = ctx.client.load_packages().await;
    match result {
        Err(EzManagementError::LoadIsolatesFailed(msg)) => {
            assert!(msg.contains("Container manager rejected manifests"));
        }
        other => panic!("Expected LoadIsolatesFailed, got {:?}", other),
    }
}

#[tokio::test]
async fn test_isolate_missing_binary_services_index_fails() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let requester = create_mock_container_manager_requester(Ok(()), Ok(()));
    let mut responses =
        create_single_ratified_manifest_responses("unregistered_wl", "unregistered.tar");
    responses.push(create_single_chunk_response("unregistered.tar", vec![0u8; 100]));

    let mut ctx = setup_test_context_with_requester(responses, requester).await;
    let result = ctx.client.load_packages().await;
    match result {
        Err(EzManagementError::LoadIsolatesFailed(msg)) => {
            assert!(msg.contains("Failed to get binary services index"));
        }
        other => panic!("Expected LoadIsolatesFailed, got {:?}", other),
    }

    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "unregistered.tar");
    assert!(!res.success);
}

#[tokio::test]
async fn test_load_workload_isolates_container_manager_failure() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let requester = create_mock_container_manager_requester(
        Ok(()),
        Err("Booting isolates failed in container manager"),
    );
    let mut responses = create_single_ratified_manifest_responses("workload_boot_fail", "pkg.tar");
    responses.push(create_all_packages_loaded_response());

    let mut ctx = setup_test_context_with_requester(responses, requester).await;
    let result = ctx.client.load_packages().await;
    match result {
        Err(EzManagementError::LoadIsolatesFailed(msg)) => {
            assert!(msg.contains("Booting isolates failed in container manager"));
        }
        other => panic!("Expected LoadIsolatesFailed, got {:?}", other),
    }
}

#[tokio::test]
async fn test_manifest_descriptor_with_missing_isolate_type_ignored() {
    let pkg_temp_dir = tempdir().unwrap();
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", pkg_temp_dir.path());

    let requester = create_mock_container_manager_requester(Ok(()), Ok(()));

    let mut ratified = create_ratified_manifest("valid_ratified", "ratified.tar");
    ratified.ratified_isolate_descriptors.push(RatifiedIsolateDescriptor {
        isolate_type: None,
        package_filename: "none_ratified.tar".to_string(),
        ..Default::default()
    });

    let mut opaque = create_opaque_manifest("valid_opaque", "opaque.tar");
    opaque.opaque_isolate_descriptors.push(OpaqueIsolateDescriptor {
        isolate_type: None,
        package_filename: "none_opaque.tar".to_string(),
        ..Default::default()
    });

    let responses = vec![
        create_ratified_response(ratified),
        create_opaque_response(opaque),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context_with_requester(responses, requester).await;
    let indices = ctx.client.load_packages().await.expect("Should succeed when manifests load");
    assert!(indices.is_empty());
}

#[tonic::async_trait]
impl EzManagementService for MockManagementService {
    type LoadIsolatesStream =
        Pin<Box<dyn Stream<Item = Result<LoadIsolatesResponse, Status>> + Send + 'static>>;

    async fn load_isolates(
        &self,
        request: Request<Streaming<LoadIsolatesRequest>>,
    ) -> Result<Response<Self::LoadIsolatesStream>, Status> {
        let mut in_stream = request.into_inner();
        let received_requests = self.received_requests.clone();

        tokio::spawn(async move {
            while let Ok(Some(req)) = in_stream.message().await {
                received_requests.lock().unwrap().push(req);
            }
        });

        let responses = self.responses_to_send.lock().unwrap().clone();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            for resp in responses {
                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

struct TestContext {
    client: EzManagementClient,
    received_requests: Arc<Mutex<Vec<LoadIsolatesRequest>>>,
    server_handle: tokio::task::JoinHandle<()>,
    _server_temp_dir: tempfile::TempDir,
    _cm_temp_dir: tempfile::TempDir,
}

impl TestContext {
    async fn get_last_load_result(&self) -> ez_management_proto::enforcer::v2::LoadIsolatesResult {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let reqs = self.received_requests.lock().unwrap().clone();
        for req in reqs.into_iter().rev() {
            if let Some(load_isolates_request::Request::LoadIsolatesResult(res)) = req.request {
                return res;
            }
        }
        panic!("No LoadIsolatesResult found in received requests");
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.server_handle.abort();
    }
}

async fn create_fake_container_manager() -> (ContainerManagerRequester, tempfile::TempDir) {
    FakeContainer::clear_tracker();

    let temp_dir = tempdir().unwrap();
    let manifest_file = temp_dir.path().join("empty_manifest.json");
    tokio::fs::write(&manifest_file, r#"{"bundle_manifest": {"manifests": []}}"#).await.unwrap();

    let (container_manager_request_tx, container_manager_request_rx) =
        mpsc::channel::<ContainerManagerRequest>(CHANNEL_SIZE);
    let container_manager_requester = ContainerManagerRequester::new(container_manager_request_tx);
    let data_scope_requester = DataScopeRequester::new(u64::MAX);
    let isolate_state_manager =
        IsolateStateManager::new(data_scope_requester.clone(), container_manager_requester.clone());
    let isolate_service_mapper = IsolateServiceMapper::default();
    let manifest_validator = ManifestValidator::default();
    let shared_memory_manager =
        SharedMemManager::new(container_manager_requester.clone(), SHM_NUM_SLOTS, SHM_SLOT_SIZE);
    let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
    let isolate_junction = FakeJunction::default();
    let interceptor = Interceptor::new(isolate_service_mapper.clone());
    let isolate_ez_service_manager =
        IsolateEzServiceManager::new(IsolateEzServiceManagerDependencies {
            isolate_junction: Box::new(isolate_junction.clone()),
            isolate_state_manager: isolate_state_manager.clone(),
            shared_memory_manager: shared_memory_manager.clone(),
            fileshare_manager: fileshare_manager.clone(),
            external_proxy_connector: None,
            isolate_service_mapper: isolate_service_mapper.clone(),
            data_scope_requester: data_scope_requester.clone(),
            manifest_validator: manifest_validator.clone(),
            ez_to_ez_outbound_handler: None,
            max_decoding_message_size: MAX_DECODING_SIZE,
            interceptor: interceptor.clone(),
            otel_endpoint: None,
            disable_metrics_filtering: false,
            shm_payload_threshold: SHM_PAYLOAD_THRESHOLD,
        });
    let container_manager_args = ContainerManagerArgs {
        isolate_junction: Box::new(isolate_junction.clone()),
        container_manager_request_rx,
        isolate_state_manager: isolate_state_manager.clone(),
        isolate_service_mapper: isolate_service_mapper.clone(),
        isolate_ez_service_mngr: isolate_ez_service_manager.clone(),
        manifest_validator: manifest_validator.clone(),
        shared_mem_manager: shared_memory_manager.clone(),
        fileshare_manager: fileshare_manager.clone(),
        manifest_source: ManifestSource::V1 {
            manifest_path: manifest_file.to_str().unwrap().to_string(),
        },
        common_bind_mounts: vec![],
        max_decoding_message_size: MAX_DECODING_SIZE,
        isolate_runtime_configs: IsolateRuntimeConfigs::default(),
        interceptor: interceptor.clone(),
        otel_traces_endpoint: None,
        run_isolate_as_unprivileged: false,
        enable_syscall_filtering: false,
        shm_num_slots: SHM_NUM_SLOTS,
        shm_slot_size: SHM_SLOT_SIZE,
        shm_payload_threshold: SHM_PAYLOAD_THRESHOLD,
        operator_role: "PRIMARY".to_string(),
    };
    let _cm = ContainerManager::<FakeContainer>::start(container_manager_args)
        .await
        .expect("ContainerManager should start");
    (container_manager_requester, temp_dir)
}

async fn setup_test_context_with_requester(
    responses: Vec<LoadIsolatesResponse>,
    requester: ContainerManagerRequester,
) -> TestContext {
    let server_temp_dir = tempdir().unwrap();
    let uds_path = server_temp_dir.path().join("ez_mgmt_test.sock");
    let uds = UnixListener::bind(&uds_path).unwrap();
    let uds_stream = UnixListenerStream::new(uds);
    let received_requests = Arc::new(Mutex::new(Vec::new()));
    let service = MockManagementService {
        responses_to_send: Arc::new(Mutex::new(responses)),
        received_requests: received_requests.clone(),
    };

    let server_handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(EzManagementServiceServer::new(service))
            .serve_with_incoming(uds_stream)
            .await;
    });

    let address = format!("unix:{}", uds_path.to_str().unwrap());
    let client = EzManagementClient::new(&address, requester, MAX_DECODING_SIZE)
        .await
        .expect("Client should connect");
    TestContext {
        client,
        received_requests,
        server_handle,
        _server_temp_dir: server_temp_dir,
        _cm_temp_dir: tempdir().unwrap(),
    }
}

async fn get_ez_packages_dir(base_dir: &std::path::Path) -> std::path::PathBuf {
    let mut entries = tokio::fs::read_dir(base_dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.file_name().to_string_lossy().starts_with("ez_packages_") {
            return entry.path();
        }
    }
    panic!("ez_packages_ directory not found in {:?}", base_dir);
}

async fn setup_test_context(responses: Vec<LoadIsolatesResponse>) -> TestContext {
    let (container_manager_requester, cm_temp_dir) = create_fake_container_manager().await;
    let mut ctx = setup_test_context_with_requester(responses, container_manager_requester).await;
    ctx._cm_temp_dir = cm_temp_dir;
    ctx
}

fn create_ratified_manifest(isolate_name: &str, package_filename: &str) -> RatifiedIsolateManifest {
    RatifiedIsolateManifest {
        ratified_isolate_descriptors: vec![RatifiedIsolateDescriptor {
            isolate_type: Some(IsolateType {
                isolate_name: isolate_name.to_string(),
                publisher_id: "EZ_Trusted".to_string(),
            }),
            package_filename: package_filename.to_string(),
            binary_filename: format!("main_{isolate_name}"),
            ..Default::default()
        }],
    }
}

fn create_opaque_manifest(isolate_name: &str, package_filename: &str) -> OpaqueIsolateManifest {
    OpaqueIsolateManifest {
        opaque_isolate_descriptors: vec![OpaqueIsolateDescriptor {
            isolate_type: Some(IsolateType {
                isolate_name: isolate_name.to_string(),
                publisher_id: "adtech.com".to_string(),
            }),
            package_filename: package_filename.to_string(),
            binary_filename: format!("main_{isolate_name}"),
            ..Default::default()
        }],
    }
}

fn create_ratified_response(manifest: RatifiedIsolateManifest) -> LoadIsolatesResponse {
    LoadIsolatesResponse {
        response: Some(load_isolates_response::Response::RatifiedIsolateManifest(
            ez_management_proto::enforcer::v2::RatifiedIsolateManifestPayload {
                manifest: Some(manifest),
                manifest_endorsements: vec![],
            },
        )),
    }
}

fn create_opaque_response(manifest: OpaqueIsolateManifest) -> LoadIsolatesResponse {
    LoadIsolatesResponse {
        response: Some(load_isolates_response::Response::OpaqueIsolateManifest(manifest)),
    }
}

fn create_single_chunk_response(package_name: &str, chunk_bytes: Vec<u8>) -> LoadIsolatesResponse {
    LoadIsolatesResponse {
        response: Some(load_isolates_response::Response::IsolatePackageChunk(
            IsolatePackageChunk {
                isolate_package_endorsements: vec![],
                package_name: package_name.to_string(),
                chunk_sequence: 0,
                package_tar_chunk: chunk_bytes,
                is_last_chunk: true,
            },
        )),
    }
}

fn create_all_packages_loaded_response() -> LoadIsolatesResponse {
    LoadIsolatesResponse {
        response: Some(load_isolates_response::Response::AllPackagesLoaded(
            AllPackagesLoadedResponse {},
        )),
    }
}

fn create_mock_container_manager_requester(
    manifest_result: Result<(), &'static str>,
    isolates_result: Result<(), &'static str>,
) -> ContainerManagerRequester {
    let (container_manager_request_tx, mut rx) =
        mpsc::channel::<ContainerManagerRequest>(CHANNEL_SIZE);
    tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            match req {
                ContainerManagerRequest::LoadWorkloadManifests { resp, .. } => {
                    let res = manifest_result
                        .map_err(|e| anyhow::anyhow!(e))
                        .map(|_| LoadWorkloadManifestsResponse { registered_indices: vec![] });
                    let _ = resp.send(res);
                }
                ContainerManagerRequest::LoadWorkloadIsolates { resp, .. } => {
                    let res = isolates_result
                        .map_err(|e| anyhow::anyhow!(e))
                        .map(|_| LoadWorkloadIsolatesResponse { loaded_indices: vec![] });
                    let _ = resp.send(res);
                }
                _ => {}
            }
        }
    });
    ContainerManagerRequester::new(container_manager_request_tx)
}

fn create_single_ratified_manifest_responses(
    isolate_name: &str,
    package_filename: &str,
) -> Vec<LoadIsolatesResponse> {
    vec![
        create_ratified_response(create_ratified_manifest(isolate_name, package_filename)),
        create_opaque_response(OpaqueIsolateManifest::default()),
    ]
}
