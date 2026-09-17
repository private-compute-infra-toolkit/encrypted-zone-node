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

mod test_utils;

use ez_management::{package_utils, EzManagementClient, EzManagementError, LoadIsolatesError};
use ez_management_proto::enforcer::v2::ez_management_service_server::EzManagementServiceServer;
use ez_management_proto::enforcer::v2::{
    load_isolates_request, load_isolates_response, LoadIsolatesResponse,
    RatifiedIsolateManifestPayload,
};
use opaque_isolate_manifest_proto::enforcer::v2::{OpaqueIsolateDescriptor, OpaqueIsolateManifest};
use ratified_isolate_manifest_proto::enforcer::v2::{
    RatifiedIsolateDescriptor, RatifiedIsolateManifest,
};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use test_utils::*;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

#[tokio::test]
async fn test_load_isolates_success_flow() {
    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_wl", "ratified.tar")),
        create_opaque_response(create_opaque_manifest("opaque_wl", "opaque.tar")),
        create_chunk_response("ratified.tar", 0, &VALID_TAR[..512], false, &[]),
        create_chunk_response("", 1, &VALID_TAR[512..], true, &[]),
        create_single_chunk_response("opaque.tar", VALID_TAR),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context(responses).await;
    let pkg_dir = ctx.pkg_dir().to_path_buf();
    let indices = ctx.client.load_packages().await.expect("load_isolates should succeed");
    assert_eq!(indices.len(), 2);

    // Verify that the temporary ez_packages directory was cleaned up after loading packages.
    let mut entries = tokio::fs::read_dir(&pkg_dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        assert!(!entry.file_name().to_string_lossy().starts_with("ez_packages_"));
    }

    // Verify received server requests: ReadyToLoadPackages, then LoadIsolatesResult for each package.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let reqs = ctx.received_requests.lock().unwrap().clone();
    assert_eq!(reqs.len(), 3);
    assert!(matches!(
        reqs[0].request,
        Some(load_isolates_request::Request::ReadyToLoadPackages(_))
    ));
    match &reqs[1].request {
        Some(load_isolates_request::Request::LoadIsolatesResult(res)) => {
            assert_eq!(res.package_name, "ratified.tar");
            assert!(res.success);
            assert!(res.validate_isolate_endorsement_result.is_some());
        }
        other => panic!("Expected LoadIsolatesResult for ratified, got {:?}", other),
    }
    match &reqs[2].request {
        Some(load_isolates_request::Request::LoadIsolatesResult(res)) => {
            assert_eq!(res.package_name, "opaque.tar");
            assert!(res.success);
            assert!(res.validate_isolate_endorsement_result.is_none());
        }
        other => panic!("Expected LoadIsolatesResult for opaque, got {:?}", other),
    }
}

#[tokio::test]
async fn test_manifest_ordering_and_partial_packages() {
    // Manifest ordering: Opaque manifest received first, then Ratified manifest.
    let responses = vec![
        create_opaque_response(create_opaque_manifest("opaque_wl", "opaque.tar")),
        create_ratified_response(create_ratified_manifest("ratified_wl", "ratified.tar")),
        create_single_chunk_response("ratified.tar", VALID_TAR),
        create_single_chunk_response("opaque.tar", VALID_TAR),
        create_all_packages_loaded_response(),
    ];
    let mut ctx = setup_test_context(responses).await;
    assert_eq!(ctx.client.load_packages().await.unwrap().len(), 2);

    // Partial packages: Manifest specifies 2 workloads, but only 1 package is streamed.
    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_1", "pkg_1.tar")),
        create_opaque_response(create_opaque_manifest("opaque_2", "pkg_2.tar")),
        create_single_chunk_response("pkg_1.tar", VALID_TAR),
        create_all_packages_loaded_response(),
    ];
    let mut ctx = setup_test_context(responses).await;
    assert_eq!(ctx.client.load_packages().await.unwrap().len(), 1);

    // Empty response is ignored during package streaming.
    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_1", "pkg_1.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        LoadIsolatesResponse { response: None },
        create_single_chunk_response("pkg_1.tar", VALID_TAR),
        create_all_packages_loaded_response(),
    ];
    let mut ctx = setup_test_context(responses).await;
    assert_eq!(ctx.client.load_packages().await.unwrap().len(), 1);
}

#[tokio::test]
async fn test_manifest_validation_errors() {
    let manifest_err_cases = [
        (
            vec![
                create_opaque_response(OpaqueIsolateManifest::default()),
                create_opaque_response(OpaqueIsolateManifest::default()),
            ],
            "Missing RatifiedIsolateManifest",
        ),
        (
            vec![
                create_ratified_response(RatifiedIsolateManifest::default()),
                create_ratified_response(RatifiedIsolateManifest::default()),
            ],
            "Missing OpaqueIsolateManifest",
        ),
        (
            vec![LoadIsolatesResponse {
                response: Some(load_isolates_response::Response::RatifiedIsolateManifest(
                    RatifiedIsolateManifestPayload {
                        manifest: None,
                        manifest_endorsements: vec![],
                    },
                )),
            }],
            "missing inner manifest",
        ),
    ];
    for (responses, expected_msg) in manifest_err_cases {
        let mut ctx = setup_test_context(responses).await;
        assert_manifest_err(ctx.client.load_packages().await, expected_msg);
    }

    let unexpected_cases = [
        (vec![LoadIsolatesResponse { response: None }], "empty"),
        (
            vec![
                create_ratified_response(create_ratified_manifest(
                    "ratified_1",
                    "ratified_pkg.tar",
                )),
                create_opaque_response(create_opaque_manifest("opaque_1", "opaque_pkg.tar")),
                create_single_chunk_response("ratified_pkg.tar", VALID_TAR),
                create_ratified_response(RatifiedIsolateManifest::default()),
            ],
            "unexpected manifest",
        ),
    ];
    for (responses, expected_msg) in unexpected_cases {
        let mut ctx = setup_test_context(responses).await;
        assert_unexpected_msg(ctx.client.load_packages().await, expected_msg);
    }
}

#[tokio::test]
async fn test_chunk_streaming_sequencing_and_framing_errors() {
    let mut ctx =
        setup_test_context(vec![create_single_chunk_response("premature.tar", VALID_TAR)]).await;
    assert_unexpected_msg(ctx.client.load_packages().await, "before both manifests were received");

    let chunk_cases = [
        (vec![create_single_chunk_response("", VALID_TAR)], "without package_name"),
        (
            vec![create_chunk_response("test_pkg.tar", 1, &VALID_TAR[..512], true, &[])],
            "expected 0, got 1",
        ),
        (
            vec![
                create_chunk_response("test_pkg.tar", 0, &VALID_TAR[..512], false, &[]),
                create_chunk_response("", 2, &VALID_TAR[512..], true, &[]),
            ],
            "expected 1, got 2",
        ),
        (
            vec![
                create_chunk_response("test_pkg.tar", 0, &VALID_TAR[..512], false, &[]),
                create_chunk_response("other_pkg.tar", 1, &VALID_TAR[512..], true, &[]),
            ],
            "while still streaming package 'test_pkg.tar'",
        ),
        (
            vec![
                create_chunk_response("test_pkg.tar", 0, &VALID_TAR[..512], false, &[]),
                create_all_packages_loaded_response(),
            ],
            "still being streamed",
        ),
    ];

    for (chunks, expected_msg) in chunk_cases {
        let mut responses = create_single_ratified_manifest_responses("test_wl", "test_pkg.tar");
        responses.extend(chunks);
        let mut ctx = setup_test_context(responses).await;
        assert_unexpected_msg(ctx.client.load_packages().await, expected_msg);
    }
}

#[tokio::test]
async fn test_stream_closed_errors() {
    let cases = [
        (vec![], "closed"),
        (
            vec![
                create_ratified_response(create_ratified_manifest("ratified_1", "pkg_1.tar")),
                create_opaque_response(create_opaque_manifest("opaque_2", "pkg_2.tar")),
                create_single_chunk_response("pkg_1.tar", VALID_TAR),
            ],
            "before receiving AllPackagesLoaded",
        ),
    ];
    for (responses, expected) in cases {
        let mut ctx = setup_test_context(responses).await;
        assert_stream_err(ctx.client.load_packages().await, expected);
    }
}

#[tokio::test]
async fn test_package_path_validation_and_disk_errors() {
    let path_cases = [
        ("..", "..", "path traversal"),
        ("/", "/", "cannot resolve file name"),
        ("known_pkg.tar", "unknown_pkg.tar", "not present in the manifest"),
    ];
    for (manifest_pkg, chunk_pkg, expected_err) in path_cases {
        let mut ctx = setup_test_context(vec![
            create_ratified_response(create_ratified_manifest("wl", manifest_pkg)),
            create_opaque_response(OpaqueIsolateManifest::default()),
            create_single_chunk_response(chunk_pkg, VALID_TAR),
        ])
        .await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ManifestParsingFailure,
            expected_err,
        );
        if chunk_pkg == "unknown_pkg.tar" {
            let res = ctx.get_last_load_result().await;
            assert_eq!(res.package_name, "unknown_pkg.tar");
            assert!(!res.success);
        }
    }

    // Write failure when target path is already a directory (EISDIR).
    let mut ctx = setup_test_context(vec![
        create_ratified_response(create_ratified_manifest("fail_write", "fail_write.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        create_single_chunk_response("fail_write.tar", VALID_TAR),
    ])
    .await;
    let ez_pkg_dir = get_ez_packages_dir(ctx.pkg_dir()).await;
    tokio::fs::create_dir(ez_pkg_dir.join("fail_write.tar")).await.unwrap();
    assert_load_failed(ctx.client.load_packages().await, LoadIsolatesError::IoFailure, "");
    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "fail_write.tar");
    assert!(!res.success);
}

#[tokio::test]
async fn test_container_manager_errors() {
    // Container manager fails during LoadWorkloadManifests.
    let requester = create_mock_container_manager_requester(Err("CM manifest rejection"), Ok(()));
    let mut ctx = setup_test_context_with_requester(
        create_single_ratified_manifest_responses("wl_1", "pkg_1.tar"),
        requester,
    )
    .await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::ManifestParsingFailure,
        "CM manifest rejection",
    );

    // Container manager returns empty registered indices so get_package_binary_index fails.
    let requester = create_mock_container_manager_requester(Ok(()), Ok(()));
    let mut ctx = setup_test_context_with_requester(
        vec![
            create_ratified_response(create_ratified_manifest(
                "unregistered_wl",
                "unregistered.tar",
            )),
            create_opaque_response(OpaqueIsolateManifest::default()),
            create_single_chunk_response("unregistered.tar", VALID_TAR),
        ],
        requester,
    )
    .await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::ManifestParsingFailure,
        "Failed to get binary services index",
    );
    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "unregistered.tar");
    assert!(!res.success);

    // Container manager fails during LoadWorkloadIsolates.
    let requester =
        create_mock_container_manager_requester(Ok(()), Err("Booting isolates failed in CM"));
    let mut ctx = setup_test_context_with_requester(
        vec![
            create_ratified_response(create_ratified_manifest("boot_fail_wl", "pkg.tar")),
            create_opaque_response(OpaqueIsolateManifest::default()),
            create_all_packages_loaded_response(),
        ],
        requester,
    )
    .await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::UnpackingFailure,
        "Booting isolates failed in CM",
    );

    // Manifest descriptor with None isolate_type is ignored and loading succeeds.
    let requester = create_mock_container_manager_requester(Ok(()), Ok(()));
    let mut ratified = create_ratified_manifest("valid_ratified", "ratified.tar");
    ratified
        .ratified_isolate_descriptors
        .push(RatifiedIsolateDescriptor { isolate_type: None, ..Default::default() });
    let mut opaque = create_opaque_manifest("valid_opaque", "opaque.tar");
    opaque
        .opaque_isolate_descriptors
        .push(OpaqueIsolateDescriptor { isolate_type: None, ..Default::default() });
    let mut ctx = setup_test_context_with_requester(
        vec![
            create_ratified_response(ratified),
            create_opaque_response(opaque),
            create_all_packages_loaded_response(),
        ],
        requester,
    )
    .await;
    let indices =
        ctx.client.load_packages().await.expect("Should succeed with empty descriptors ignored");
    assert!(indices.is_empty());
}

#[tokio::test]
async fn test_ratified_isolate_endorsement_validation() {
    let mock_junction = MockSetupIsolateJunction::new_success();
    let endorsement_bytes = vec![1u8, 2, 3, 4, 5];
    let expected_hash = package_utils::calculate_sha256(VALID_TAR);

    // Single chunk ratified package validation.
    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_workload", "ratified.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        create_chunk_response("ratified.tar", 0, VALID_TAR, true, &endorsement_bytes),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context_with_junction(responses, mock_junction.clone()).await;
    let indices = ctx.client.load_packages().await.expect("load_packages should succeed");
    assert_eq!(indices.len(), 1);

    let invoked_requests = mock_junction.get_invoked_requests();
    assert_eq!(invoked_requests.len(), 1);
    let req = &invoked_requests[0];
    let claims = req.expected_claims.as_ref().expect("claims should be present");
    assert_eq!(claims.publisher_id, "EZ_Trusted");
    assert_eq!(claims.isolate_name, "ratified_workload");
    assert_eq!(req.isolate_package_endorsement, endorsement_bytes);
    assert_eq!(req.package_digest_sha256, expected_hash);

    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "ratified.tar");
    assert!(res.success);
    let val_resp =
        res.validate_isolate_endorsement_result.expect("validate result should be present");
    let validity = val_resp.validity.expect("validity should be present");
    assert_eq!(validity.not_before.unwrap().seconds, 1000);
    assert_eq!(validity.not_after.unwrap().seconds, 2000);

    // Multi-chunk ratified package captures endorsements from the first chunk.
    let multi_chunk_endorsement = vec![99u8, 98, 97];
    let responses = vec![
        create_ratified_response(create_ratified_manifest("chunked_ratified", "chunked.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        create_chunk_response("chunked.tar", 0, &VALID_TAR[..512], false, &multi_chunk_endorsement),
        create_chunk_response("chunked.tar", 1, &VALID_TAR[512..], true, &[]),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context_with_junction(responses, mock_junction.clone()).await;
    let indices = ctx.client.load_packages().await.expect("load_packages should succeed");
    assert_eq!(indices.len(), 1);

    let invoked = mock_junction.get_invoked_requests();
    assert_eq!(invoked.len(), 2);
    let req2 = &invoked[1];
    assert_eq!(req2.isolate_package_endorsement, multi_chunk_endorsement);
    assert_eq!(req2.package_digest_sha256, expected_hash);
}

#[tokio::test]
async fn test_ratified_isolate_endorsement_validation_errors() {
    let responses = || {
        vec![
            create_ratified_response(create_ratified_manifest("ratified_workload", "ratified.tar")),
            create_opaque_response(OpaqueIsolateManifest::default()),
            create_single_chunk_response("ratified.tar", VALID_TAR),
            create_all_packages_loaded_response(),
        ]
    };

    // Validation failure: Setup isolate returns validity: None.
    let mock_junction = MockSetupIsolateJunction::new_failure(42, "Untrusted signature");
    let mut ctx = setup_test_context_with_junction(responses(), mock_junction).await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::ValidationFailure,
        "Endorsement validation failed",
    );

    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "ratified.tar");
    assert!(!res.success);
    assert_eq!(res.load_isolates_error, LoadIsolatesError::ValidationFailure as i32);
    let val_resp =
        res.validate_isolate_endorsement_result.expect("validate result should be present");
    assert!(val_resp.validity.is_none());
    assert_eq!(val_resp.validation_error, 42);
    assert_eq!(val_resp.error_message, "Untrusted signature");

    // Validation failure: Setup isolate returns non-zero validation_error even with validity present.
    let mock_junction =
        MockSetupIsolateJunction::new_failure_with_validity(6, "Package digest mismatch");
    let mut ctx = setup_test_context_with_junction(responses(), mock_junction).await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::ValidationFailure,
        "Endorsement validation failed",
    );

    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "ratified.tar");
    assert!(!res.success);
    assert_eq!(res.load_isolates_error, LoadIsolatesError::ValidationFailure as i32);
    let val_resp =
        res.validate_isolate_endorsement_result.expect("validate result should be present");
    assert!(val_resp.validity.is_some());
    assert_eq!(val_resp.validation_error, 6);
    assert_eq!(val_resp.error_message, "Package digest mismatch");

    // Junction RPC failure during validation.
    let mock_junction = MockSetupIsolateJunction::new_rpc_error("RPC connection reset");
    let mut ctx = setup_test_context_with_junction(responses(), mock_junction).await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::ValidationFailure,
        "RPC connection reset",
    );

    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "ratified.tar");
    assert!(!res.success);
    assert_eq!(res.load_isolates_error, LoadIsolatesError::ValidationFailure as i32);
    assert!(res.validate_isolate_endorsement_result.is_none());
}

#[tokio::test]
async fn test_opaque_isolates_and_setup_client_behavior() {
    // Opaque isolate skips endorsement validation completely.
    let mock_junction = MockSetupIsolateJunction::new_success();
    let responses = vec![
        create_ratified_response(RatifiedIsolateManifest::default()),
        create_opaque_response(create_opaque_manifest("opaque_workload", "opaque.tar")),
        create_single_chunk_response("opaque.tar", VALID_TAR),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context_with_junction(responses, mock_junction.clone()).await;
    let indices = ctx.client.load_packages().await.expect("load_packages should succeed");
    assert_eq!(indices.len(), 1);
    assert!(mock_junction.get_invoked_requests().is_empty());

    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "opaque.tar");
    assert!(res.success);
    assert!(res.validate_isolate_endorsement_result.is_none());

    // Opaque isolates do not need SetupIsolateClient to load.
    let responses = vec![
        create_ratified_response(RatifiedIsolateManifest::default()),
        create_opaque_response(create_opaque_manifest("opaque_workload", "opaque.tar")),
        create_single_chunk_response("opaque.tar", VALID_TAR),
        create_all_packages_loaded_response(),
    ];
    let mut ctx = setup_test_context_without_setup_client(responses).await;
    let indices = ctx
        .client
        .load_packages()
        .await
        .expect("Opaque packages should load without setup isolate client");
    assert_eq!(indices.len(), 1);

    // Ratified isolate fails cleanly when SetupIsolateClient is unavailable.
    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_workload", "ratified.tar")),
        create_opaque_response(OpaqueIsolateManifest::default()),
        create_single_chunk_response("ratified.tar", VALID_TAR),
        create_all_packages_loaded_response(),
    ];
    let mut ctx = setup_test_context_without_setup_client(responses).await;
    match ctx.client.load_packages().await {
        Err(EzManagementError::InternalError(msg)) => {
            assert!(msg.contains("Setup isolate client is not available"));
        }
        other => panic!("Expected InternalError, got {:?}", other),
    }

    let res = ctx.get_last_load_result().await;
    assert_eq!(res.package_name, "ratified.tar");
    assert!(!res.success);
    assert_eq!(res.load_isolates_error, LoadIsolatesError::ValidationFailure as i32);
    assert!(res.validate_isolate_endorsement_result.is_none());
}

#[tokio::test]
async fn test_write_package_to_disk_io_failure() {
    std::env::set_var("EZ_PACKAGE_OUTPUT_DIR", "/dev/null/forbidden_dir");
    let (container_manager_requester, _cm_temp_dir) = create_fake_container_manager(None).await;
    let server_temp_dir = tempdir().unwrap();
    let uds_path = server_temp_dir.path().join("ez_mgmt_test.sock");
    let uds = UnixListener::bind(&uds_path).unwrap();
    let uds_stream = UnixListenerStream::new(uds);

    let service = MockManagementService {
        responses_to_send: Arc::new(Mutex::new(vec![])),
        received_requests: Arc::new(Mutex::new(vec![])),
    };
    let server_handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(EzManagementServiceServer::new(service))
            .serve_with_incoming(uds_stream)
            .await;
    });
    let address = format!("unix:{}", uds_path.to_str().unwrap());
    let result =
        EzManagementClient::new(&address, container_manager_requester, MAX_DECODING_SIZE).await;
    server_handle.abort();
    std::env::remove_var("EZ_PACKAGE_OUTPUT_DIR");

    assert!(matches!(result, Err(EzManagementError::IoError(_))));
}

#[tokio::test]
async fn test_duplicate_package_filename_across_manifests() {
    // Duplicate package_filename across ratified and opaque manifests.
    let responses = vec![
        create_ratified_response(create_ratified_manifest("ratified_wl", "shared.tar")),
        create_opaque_response(create_opaque_manifest("opaque_wl", "shared.tar")),
    ];
    let mut ctx = setup_test_context(responses).await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::ManifestParsingFailure,
        "Duplicate package_filename 'shared.tar' found across manifests",
    );

    // Duplicate package_filename within the same manifest.
    let mut ratified = create_ratified_manifest("ratified_1", "duplicate.tar");
    ratified.ratified_isolate_descriptors.push(RatifiedIsolateDescriptor {
        isolate_type: Some(common_proto::enforcer::v2::IsolateType {
            isolate_name: "ratified_2".to_string(),
            publisher_id: "EZ_Trusted".to_string(),
        }),
        package_filename: "duplicate.tar".to_string(),
        binary_filename: "main_ratified_2".to_string(),
        ..Default::default()
    });
    let responses = vec![
        create_ratified_response(ratified),
        create_opaque_response(OpaqueIsolateManifest::default()),
    ];
    let mut ctx = setup_test_context(responses).await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::ManifestParsingFailure,
        "Duplicate package_filename 'duplicate.tar' found across manifests",
    );
}
