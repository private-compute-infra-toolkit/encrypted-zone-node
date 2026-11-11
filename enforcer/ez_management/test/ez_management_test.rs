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

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod test_utils;

use common_proto::enforcer::v2::IsolateType;
use ez_management::{
    package_utils, EzManagementClient, EzManagementError, IsolateStartupParameters,
    LoadIsolatesError,
};
use ez_management_proto::enforcer::v2::ez_management_service_server::EzManagementServiceServer;
use ez_management_proto::enforcer::v2::{
    ez_manifest, load_isolates_request, BundleManifest, EzManifest, LoadIsolatesResponse,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use test_utils::*;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

#[tokio::test]
async fn test_load_isolates_success_flow() {
    let responses = vec![
        create_bundle_response(vec![
            create_ratified_manifest("ratified_wl"),
            create_opaque_manifest("opaque_wl"),
        ]),
        create_chunk_response(
            Some(ratified_isolate_type("ratified_wl")),
            0,
            &VALID_TAR[..512],
            false,
            &[],
        ),
        create_chunk_response(None, 1, &VALID_TAR[512..], true, &[]),
        create_single_chunk_response(opaque_isolate_type("opaque_wl"), VALID_TAR),
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

    // Verify received server requests: ReadyToLoadPackages, a LoadIsolatesResult with the
    // endorsement validation response for each BinaryManifest, then a LoadIsolatesResult for each
    // package.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let reqs = ctx.received_requests.lock().unwrap().clone();
    assert_eq!(reqs.len(), 5);
    assert!(matches!(
        reqs[0].request,
        Some(load_isolates_request::Request::ReadyToLoadPackages(_))
    ));
    let ratified_wl = ratified_isolate_type("ratified_wl");
    let opaque_wl = opaque_isolate_type("opaque_wl");
    let expected_results =
        [(&ratified_wl, true), (&opaque_wl, true), (&ratified_wl, false), (&opaque_wl, false)];
    for (req, (isolate_type, has_validate_result)) in reqs[1..].iter().zip(expected_results) {
        match &req.request {
            Some(load_isolates_request::Request::LoadIsolatesResult(res)) => {
                assert_eq!(res.isolate_type.as_ref(), Some(isolate_type));
                assert!(res.success);
                assert_eq!(res.validate_isolate_endorsement_result.is_some(), has_validate_result);
            }
            other => panic!("Expected LoadIsolatesResult for {isolate_type:?}, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn test_same_isolate_name_from_different_publishers() {
    // The isolate_name alone is ambiguous: both manifests declare "shared_name". Packages are
    // keyed by the (isolate_name, publisher_id) pair, so both Isolates load independently.
    let responses = vec![
        create_bundle_response(vec![
            create_ratified_manifest("shared_name"),
            create_opaque_manifest("shared_name"),
        ]),
        create_single_chunk_response(ratified_isolate_type("shared_name"), VALID_TAR),
        create_single_chunk_response(opaque_isolate_type("shared_name"), VALID_TAR),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context(responses).await;
    assert_eq!(ctx.client.load_packages().await.unwrap().len(), 2);
}

#[tokio::test]
async fn test_package_ordering_and_partial_packages() {
    // Packages arrive in a different order than their manifests.
    let responses = vec![
        create_bundle_response(vec![
            create_ratified_manifest("ratified_wl"),
            create_opaque_manifest("opaque_wl"),
        ]),
        create_single_chunk_response(opaque_isolate_type("opaque_wl"), VALID_TAR),
        create_single_chunk_response(ratified_isolate_type("ratified_wl"), VALID_TAR),
        create_all_packages_loaded_response(),
    ];
    let mut ctx = setup_test_context(responses).await;
    assert_eq!(ctx.client.load_packages().await.unwrap().len(), 2);

    // Incomplete packages: Manifest specifies 2 workloads, but only 1 package is streamed.
    let responses = vec![
        create_bundle_response(vec![
            create_ratified_manifest("ratified_1"),
            create_opaque_manifest("opaque_2"),
        ]),
        create_single_chunk_response(ratified_isolate_type("ratified_1"), VALID_TAR),
        create_all_packages_loaded_response(),
    ];
    let mut ctx = setup_test_context(responses).await;
    match ctx.client.load_packages().await {
        Err(EzManagementError::IncompletePackagesLoaded { expected, received }) => {
            assert_eq!(expected, 2);
            assert_eq!(received, 1);
        }
        other => panic!("Expected IncompletePackagesLoaded, got {:?}", other),
    }

    // Empty response is ignored during package streaming.
    let responses = vec![
        create_bundle_response(vec![create_ratified_manifest("ratified_1")]),
        LoadIsolatesResponse { response: None },
        create_single_chunk_response(ratified_isolate_type("ratified_1"), VALID_TAR),
        create_all_packages_loaded_response(),
    ];
    let mut ctx = setup_test_context(responses).await;
    assert_eq!(ctx.client.load_packages().await.unwrap().len(), 1);
}

#[tokio::test]
async fn test_manifest_validation_errors() {
    let manifest_err_cases = [
        (b"not json".to_vec(), "couldn't parse enforcer.v2.BinaryManifest"),
        (vec![0xff, 0xfe], "not valid UTF-8"),
    ];
    for (binary_manifest_json, expected_msg) in manifest_err_cases {
        let mut ctx =
            setup_test_context(vec![create_bundle_response(vec![create_endorsed_manifest(
                binary_manifest_json,
            )])])
            .await;
        assert_manifest_err(ctx.client.load_packages().await, expected_msg);
    }

    let missing_identity_cases = [
        br#"{"ratified": {}}"#.to_vec(),
        create_binary_manifest_json(
            &IsolateType { publisher_id: String::new(), isolate_name: "wl".to_string() },
            "ratified",
            VALID_TAR,
        ),
    ];
    for binary_manifest_json in missing_identity_cases {
        let mut ctx =
            setup_test_context(vec![create_bundle_response(vec![create_endorsed_manifest(
                binary_manifest_json,
            )])])
            .await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ManifestParsingFailure,
            "missing a valid isolate_type",
        );
    }

    let unexpected_cases = [
        (vec![LoadIsolatesResponse { response: None }], "empty"),
        (vec![create_all_packages_loaded_response()], "before the BundleManifest"),
        (
            vec![
                create_bundle_response(vec![create_ratified_manifest("ratified_1")]),
                create_single_chunk_response(ratified_isolate_type("ratified_1"), VALID_TAR),
                create_bundle_response(vec![]),
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
    let mut ctx = setup_test_context(vec![create_single_chunk_response(
        ratified_isolate_type("premature_wl"),
        VALID_TAR,
    )])
    .await;
    assert_unexpected_msg(ctx.client.load_packages().await, "before the BundleManifest");

    let test_wl = || ratified_isolate_type("test_wl");
    let chunk_cases = [
        (vec![create_chunk_response(None, 0, VALID_TAR, true, &[])], "without isolate_type"),
        (
            vec![create_chunk_response(Some(test_wl()), 1, &VALID_TAR[..512], true, &[])],
            "expected 0, got 1",
        ),
        (
            vec![
                create_chunk_response(Some(test_wl()), 0, &VALID_TAR[..512], false, &[]),
                create_chunk_response(None, 2, &VALID_TAR[512..], true, &[]),
            ],
            "expected 1, got 2",
        ),
        (
            vec![
                create_chunk_response(Some(test_wl()), 0, &VALID_TAR[..512], false, &[]),
                create_chunk_response(
                    Some(ratified_isolate_type("other_wl")),
                    1,
                    &VALID_TAR[512..],
                    true,
                    &[],
                ),
            ],
            &format!("while still streaming Isolate '{:?}'", ratified_isolate_type("test_wl")),
        ),
        (
            vec![
                create_chunk_response(Some(test_wl()), 0, &VALID_TAR[..512], false, &[]),
                create_all_packages_loaded_response(),
            ],
            "still being streamed",
        ),
    ];

    for (chunks, expected_msg) in chunk_cases {
        let mut responses = create_single_ratified_manifest_responses("test_wl");
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
                create_bundle_response(vec![
                    create_ratified_manifest("ratified_1"),
                    create_opaque_manifest("opaque_2"),
                ]),
                create_single_chunk_response(ratified_isolate_type("ratified_1"), VALID_TAR),
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
    let mut ctx = setup_test_context(vec![
        create_bundle_response(vec![create_ratified_manifest("wl")]),
        create_single_chunk_response(ratified_isolate_type("unknown_wl"), VALID_TAR),
    ])
    .await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::ManifestParsingFailure,
        "not present in the manifest",
    );
    let res = ctx.get_last_load_result().await;
    assert_eq!(res.isolate_type, Some(ratified_isolate_type("unknown_wl")));
    assert!(!res.success);

    let invalid_identity_cases = [
        (
            IsolateType { publisher_id: ".".to_string(), isolate_name: "wl".to_string() },
            "Invalid publisher_id '.'",
        ),
        (
            IsolateType { publisher_id: "..".to_string(), isolate_name: "wl".to_string() },
            "Invalid publisher_id '..'",
        ),
        (
            IsolateType { publisher_id: "pub/sub".to_string(), isolate_name: "wl".to_string() },
            "Invalid publisher_id 'pub/sub'",
        ),
        (
            IsolateType {
                publisher_id: RATIFIED_PUBLISHER_ID.to_string(),
                isolate_name: ".".to_string(),
            },
            "Invalid isolate_name '.'",
        ),
        (
            IsolateType {
                publisher_id: RATIFIED_PUBLISHER_ID.to_string(),
                isolate_name: "..".to_string(),
            },
            "Invalid isolate_name '..'",
        ),
        (
            IsolateType {
                publisher_id: RATIFIED_PUBLISHER_ID.to_string(),
                isolate_name: "nested/wl".to_string(),
            },
            "Invalid isolate_name 'nested/wl'",
        ),
    ];
    for (invalid_type, expected_err) in invalid_identity_cases {
        let manifest = create_endorsed_manifest(create_binary_manifest_json(
            &invalid_type,
            "ratified",
            VALID_TAR,
        ));
        let requester = create_mock_container_manager_requester(Ok(()), Ok(()));
        let mut ctx = setup_test_context_with_requester(
            vec![
                create_bundle_response(vec![manifest]),
                create_single_chunk_response(invalid_type.clone(), VALID_TAR),
            ],
            requester,
        )
        .await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ManifestParsingFailure,
            expected_err,
        );
        let res = ctx.get_last_load_result().await;
        assert_eq!(res.isolate_type, Some(invalid_type));
        assert!(!res.success);
    }

    // Write failure when target path is already a directory (EISDIR).
    let mut ctx = setup_test_context(vec![
        create_bundle_response(vec![create_ratified_manifest("fail_write_wl")]),
        create_single_chunk_response(ratified_isolate_type("fail_write_wl"), VALID_TAR),
    ])
    .await;
    let ez_pkg_dir = get_ez_packages_dir(ctx.pkg_dir()).await;
    tokio::fs::create_dir_all(
        ez_pkg_dir.join("EZ_Trusted").join("fail_write_wl").join("package.tar"),
    )
    .await
    .unwrap();
    assert_load_failed(ctx.client.load_packages().await, LoadIsolatesError::IoFailure, "");
    let res = ctx.get_last_load_result().await;
    assert_eq!(res.isolate_type, Some(ratified_isolate_type("fail_write_wl")));
    assert!(!res.success);
}

#[tokio::test]
async fn test_package_digest_mismatch() {
    // wl_a's package carries wl_b's bytes, which match wl_b's digest but not wl_a's.
    let wl_b_package = [1u8; 1024];
    let mut ctx = setup_test_context(vec![
        create_bundle_response(vec![
            create_ratified_manifest("wl_a"),
            create_endorsed_manifest(create_binary_manifest_json(
                &opaque_isolate_type("wl_b"),
                "opaque",
                &wl_b_package,
            )),
        ]),
        create_single_chunk_response(ratified_isolate_type("wl_a"), &wl_b_package),
        create_all_packages_loaded_response(),
    ])
    .await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::PackageDigestMismatch,
        &format!(
            "'{}' of Isolate '{:?}'",
            package_utils::calculate_sha256(VALID_TAR),
            ratified_isolate_type("wl_a")
        ),
    );
    let res = ctx.get_last_load_result().await;
    assert_eq!(res.isolate_type, Some(ratified_isolate_type("wl_a")));
    assert!(!res.success);
    assert_eq!(res.load_isolates_error, LoadIsolatesError::PackageDigestMismatch as i32);
    let ez_pkg_dir = get_ez_packages_dir(ctx.pkg_dir()).await;
    assert!(!ez_pkg_dir.join(RATIFIED_PUBLISHER_ID).join("wl_a").exists());
}

#[tokio::test]
async fn test_missing_or_invalid_isolate_digest_rejected() {
    let missing = "isolate_digest is required for Ratified and Opaque Isolates";
    let invalid = "isolate_digest.package_sha256 must be a lowercase hex-encoded SHA-256 digest";
    let empty_digest = r#""isolateDigest": {},"#.to_string();
    let uppercase_digest =
        format!(r#""isolateDigest": {{"packageSha256": "{}"}},"#, "A".repeat(64));
    let cases = [
        (ratified_isolate_type("no_digest"), "ratified", String::new(), missing),
        (opaque_isolate_type("no_digest"), "opaque", String::new(), missing),
        (ratified_isolate_type("empty_digest"), "ratified", empty_digest.clone(), invalid),
        (opaque_isolate_type("empty_digest"), "opaque", empty_digest, invalid),
        (opaque_isolate_type("uppercase_digest"), "opaque", uppercase_digest, invalid),
    ];
    for (isolate_type, kind, isolate_digest, expected_msg) in cases {
        let binary_manifest_json = format!(
            r#"{{
                "isolateType": {{"isolateName": {:?}, "publisherId": {:?}}},
                {isolate_digest}
                "binaryFilename": "main_wl",
                {kind:?}: {{}}
            }}"#,
            isolate_type.isolate_name, isolate_type.publisher_id
        );
        let mock_junction = MockSetupIsolateJunction::new_success();
        let mut ctx = setup_test_context_with_junction(
            vec![create_bundle_response(vec![create_endorsed_manifest(
                binary_manifest_json.into_bytes(),
            )])],
            mock_junction.clone(),
        )
        .await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ManifestParsingFailure,
            &format!(
                "BinaryManifest for '{isolate_type:?}' isolate_digest field parsing failure: \
                 {expected_msg}"
            ),
        );
        assert!(mock_junction.get_invoked_requests().is_empty());
    }
}

#[tokio::test]
async fn test_container_manager_errors() {
    // Container manager fails during LoadWorkloadManifests.
    let requester = create_mock_container_manager_requester(Err("CM manifest rejection"), Ok(()));
    let mut ctx = setup_test_context_with_requester(
        create_single_ratified_manifest_responses("wl_1"),
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
            create_bundle_response(vec![create_ratified_manifest("unregistered_wl")]),
            create_single_chunk_response(ratified_isolate_type("unregistered_wl"), VALID_TAR),
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
    assert_eq!(res.isolate_type, Some(ratified_isolate_type("unregistered_wl")));
    assert!(!res.success);

    // Container manager fails during LoadWorkloadIsolates.
    let requester =
        create_mock_container_manager_requester(Ok(()), Err("Booting isolates failed in CM"));
    let mut ctx = setup_test_context_with_requester(
        vec![create_bundle_response(vec![]), create_all_packages_loaded_response()],
        requester,
    )
    .await;
    assert_load_failed(
        ctx.client.load_packages().await,
        LoadIsolatesError::UnpackingFailure,
        "Booting isolates failed in CM",
    );
}

#[tokio::test]
async fn test_manifest_endorsement_validation() {
    let mock_junction = MockSetupIsolateJunction::new_success();

    let endorsement_bytes = vec![1u8, 2, 3, 4, 5];
    let mut manifest = create_ratified_manifest("ratified_workload");
    manifest.binary_manifest_endorsements = endorsement_bytes.clone();
    let expected_digest = package_utils::calculate_sha256(&manifest.binary_manifest_json);
    let responses = vec![
        create_bundle_response(vec![manifest]),
        create_single_chunk_response(ratified_isolate_type("ratified_workload"), VALID_TAR),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context_with_junction(responses, mock_junction.clone()).await;
    let indices = ctx.client.load_packages().await.expect("load_packages should succeed");
    assert_eq!(indices.len(), 1);

    let invoked_requests = mock_junction.get_invoked_requests();
    assert_eq!(invoked_requests.len(), 1);
    let req = &invoked_requests[0];
    let claims = req.expected_claims.as_ref().expect("claims should be present");
    assert_eq!(claims.publisher_id, RATIFIED_PUBLISHER_ID);
    assert_eq!(claims.isolate_name, "ratified_workload");
    assert_eq!(claims.package_filename, "binary_manifest.json");
    assert_eq!(req.isolate_package_endorsement, endorsement_bytes);
    assert_eq!(req.package_digest_sha256, expected_digest);

    let res = ctx.get_load_results().await.remove(0);
    assert_eq!(res.isolate_type, Some(ratified_isolate_type("ratified_workload")));
    assert!(res.success);
    let val_resp =
        res.validate_isolate_endorsement_result.expect("validate result should be present");
    let validity = val_resp.validity.expect("validity should be present");
    assert_eq!(validity.not_before.unwrap().seconds, 1000);
    assert_eq!(validity.not_after.unwrap().seconds, 2000);

    let opaque_endorsement_bytes = vec![99u8, 98, 97];
    let mut manifest = create_opaque_manifest("opaque_workload");
    manifest.binary_manifest_endorsements = opaque_endorsement_bytes.clone();
    let expected_digest = package_utils::calculate_sha256(&manifest.binary_manifest_json);
    let responses = vec![
        create_bundle_response(vec![manifest]),
        create_single_chunk_response(opaque_isolate_type("opaque_workload"), VALID_TAR),
        create_all_packages_loaded_response(),
    ];

    let mut ctx = setup_test_context_with_junction(responses, mock_junction.clone()).await;
    let indices = ctx.client.load_packages().await.expect("load_packages should succeed");
    assert_eq!(indices.len(), 1);

    let invoked = mock_junction.get_invoked_requests();
    assert_eq!(invoked.len(), 2);
    let req2 = &invoked[1];
    let claims = req2.expected_claims.as_ref().expect("claims should be present");
    assert_eq!(claims.publisher_id, OPAQUE_PUBLISHER_ID);
    assert_eq!(claims.isolate_name, "opaque_workload");
    assert_eq!(claims.package_filename, "binary_manifest.json");
    assert_eq!(req2.isolate_package_endorsement, opaque_endorsement_bytes);
    assert_eq!(req2.package_digest_sha256, expected_digest);
    let res = ctx.get_load_results().await.remove(0);
    assert_eq!(res.isolate_type, Some(opaque_isolate_type("opaque_workload")));
    assert!(res.success);
    assert!(res.validate_isolate_endorsement_result.is_some());
}

#[tokio::test]
async fn test_manifest_endorsement_validation_errors() {
    let isolates = [
        (create_ratified_manifest("ratified_workload"), ratified_isolate_type("ratified_workload")),
        (create_opaque_manifest("opaque_workload"), opaque_isolate_type("opaque_workload")),
    ];
    for (manifest, isolate_type) in isolates {
        // Send only the BundleManifest without package chunks. If the Enforcer waited until
        // package arrival to validate endorsements, the closed stream would return StreamError
        // instead of ValidationFailure.
        let responses = || vec![create_bundle_response(vec![manifest.clone()])];

        let mock_junction = MockSetupIsolateJunction::new_failure(42, "Untrusted signature");
        let mut ctx = setup_test_context_with_junction(responses(), mock_junction).await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ValidationFailure,
            "Endorsement validation failed",
        );

        let res = ctx.get_last_load_result().await;
        assert_eq!(res.isolate_type, Some(isolate_type.clone()));
        assert!(!res.success);
        assert_eq!(res.load_isolates_error, LoadIsolatesError::ValidationFailure as i32);
        let val_resp =
            res.validate_isolate_endorsement_result.expect("validate result should be present");
        assert!(val_resp.validity.is_none());
        assert_eq!(val_resp.validation_error, 42);
        assert_eq!(val_resp.error_message, "Untrusted signature");

        let mock_junction =
            MockSetupIsolateJunction::new_failure_with_validity(6, "Package digest mismatch");
        let mut ctx = setup_test_context_with_junction(responses(), mock_junction).await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ValidationFailure,
            "Endorsement validation failed",
        );

        let res = ctx.get_last_load_result().await;
        assert_eq!(res.isolate_type, Some(isolate_type.clone()));
        assert!(!res.success);
        assert_eq!(res.load_isolates_error, LoadIsolatesError::ValidationFailure as i32);
        let val_resp =
            res.validate_isolate_endorsement_result.expect("validate result should be present");
        assert!(val_resp.validity.is_some());
        assert_eq!(val_resp.validation_error, 6);
        assert_eq!(val_resp.error_message, "Package digest mismatch");

        let mock_junction = MockSetupIsolateJunction::new_rpc_error("RPC connection reset");
        let mut ctx = setup_test_context_with_junction(responses(), mock_junction).await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ValidationFailure,
            "RPC connection reset",
        );

        let res = ctx.get_last_load_result().await;
        assert_eq!(res.isolate_type, Some(isolate_type.clone()));
        assert!(!res.success);
        assert_eq!(res.load_isolates_error, LoadIsolatesError::ValidationFailure as i32);
        assert!(res.validate_isolate_endorsement_result.is_none());

        let mut ctx = setup_test_context_without_setup_client(responses()).await;
        match ctx.client.load_packages().await {
            Err(EzManagementError::InternalError(msg)) => {
                assert!(msg.contains("Setup isolate client is not available"));
            }
            other => panic!("Expected InternalError, got {:?}", other),
        }

        let res = ctx.get_last_load_result().await;
        assert_eq!(res.isolate_type, Some(isolate_type));
        assert!(!res.success);
        assert_eq!(res.load_isolates_error, LoadIsolatesError::ValidationFailure as i32);
        assert!(res.validate_isolate_endorsement_result.is_none());
    }
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
        ..Default::default()
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
async fn test_duplicate_isolates() {
    let nested_duplicate = EzManifest {
        manifest: Some(ez_manifest::Manifest::BundleManifest(BundleManifest {
            manifests: vec![binary_manifest_entry(create_ratified_manifest("ratified_1"))],
        })),
    };
    let bundles = [
        create_bundle_response(vec![
            create_ratified_manifest("ratified_1"),
            create_ratified_manifest("ratified_1"),
        ]),
        create_bundle_response_from_entries(vec![
            binary_manifest_entry(create_ratified_manifest("ratified_1")),
            nested_duplicate,
        ]),
    ];
    for bundle in bundles {
        let mock_junction = MockSetupIsolateJunction::new_success();
        let mut ctx = setup_test_context_with_junction(vec![bundle], mock_junction.clone()).await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ManifestParsingFailure,
            &format!(
                "Duplicate Isolate '{:?}' in the BundleManifest",
                ratified_isolate_type("ratified_1")
            ),
        );
        // The first entry is correct, but its endorsement must not go to the Setup Isolate.
        assert!(mock_junction.get_invoked_requests().is_empty());
    }
}

#[tokio::test]
async fn test_invalid_manifest_rejected_before_endorsement_validation() {
    let setup_type = ratified_isolate_type("setup");
    let missing_kind_type = ratified_isolate_type("no_kind");
    let no_binary_type = ratified_isolate_type("no_binary");
    let cases = [
        (
            create_binary_manifest_json(&setup_type, "setup", VALID_TAR),
            format!("Setup Isolate '{setup_type:?}' is not allowed in the BundleManifest"),
        ),
        (
            format!(
                r#"{{"isolateType": {{"isolateName": {:?}, "publisherId": {:?}}}}}"#,
                missing_kind_type.isolate_name, missing_kind_type.publisher_id
            )
            .into_bytes(),
            format!("BinaryManifest for '{missing_kind_type:?}' is missing isolate_kind"),
        ),
        (
            format!(
                r#"{{
                    "isolateType": {{"isolateName": {:?}, "publisherId": {:?}}},
                    "isolateDigest": {{"packageSha256": {:?}}},
                    "ratified": {{}}
                }}"#,
                no_binary_type.isolate_name,
                no_binary_type.publisher_id,
                package_utils::calculate_sha256(VALID_TAR)
            )
            .into_bytes(),
            format!("BinaryManifest for '{no_binary_type:?}': binary_filename cannot be empty"),
        ),
    ];
    for (binary_manifest_json, expected_msg) in cases {
        // Configure the Setup Isolate mock to fail so that calling it before the manifest checks
        // would return ValidationFailure instead of ManifestParsingFailure.
        let mock_junction = MockSetupIsolateJunction::new_failure(42, "Untrusted signature");
        let mut ctx = setup_test_context_with_junction(
            vec![create_bundle_response(vec![create_endorsed_manifest(binary_manifest_json)])],
            mock_junction.clone(),
        )
        .await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ManifestParsingFailure,
            &expected_msg,
        );
        assert!(mock_junction.get_invoked_requests().is_empty());
    }
}

#[tokio::test]
async fn test_empty_bundle_entry_rejected() {
    let nested_empty = EzManifest {
        manifest: Some(ez_manifest::Manifest::BundleManifest(BundleManifest {
            manifests: vec![EzManifest { manifest: None }],
        })),
    };
    for invalid_entry in [EzManifest { manifest: None }, nested_empty] {
        // Place a valid entry before the invalid entry to verify that a malformed bundle aborts
        // before sending any endorsement RPC to the Setup Isolate.
        let mock_junction = MockSetupIsolateJunction::new_success();
        let mut ctx = setup_test_context_with_junction(
            vec![create_bundle_response_from_entries(vec![
                binary_manifest_entry(create_ratified_manifest("wl")),
                invalid_entry,
            ])],
            mock_junction.clone(),
        )
        .await;
        assert_load_failed(
            ctx.client.load_packages().await,
            LoadIsolatesError::ManifestParsingFailure,
            "BundleManifest entry has no manifest",
        );
        assert!(mock_junction.get_invoked_requests().is_empty());
    }
}

#[tokio::test]
async fn test_bundle_of_binary_manifests_loads() {
    let nested_bundle = EzManifest {
        manifest: Some(ez_manifest::Manifest::BundleManifest(BundleManifest {
            manifests: vec![binary_manifest_entry(create_opaque_manifest("opaque_wl"))],
        })),
    };
    let mock_junction = MockSetupIsolateJunction::new_success();
    let mut ctx = setup_test_context_with_junction(
        vec![
            create_bundle_response_from_entries(vec![
                binary_manifest_entry(create_ratified_manifest("ratified_wl")),
                nested_bundle,
            ]),
            create_single_chunk_response(ratified_isolate_type("ratified_wl"), VALID_TAR),
            create_single_chunk_response(opaque_isolate_type("opaque_wl"), VALID_TAR),
            create_all_packages_loaded_response(),
        ],
        mock_junction.clone(),
    )
    .await;
    let indices = ctx.client.load_packages().await.expect("load_packages should succeed");
    assert_eq!(indices.len(), 2);

    let validated: Vec<_> = mock_junction
        .get_invoked_requests()
        .into_iter()
        .map(|req| {
            let claims = req.expected_claims.expect("claims should be present");
            (claims.publisher_id, claims.isolate_name)
        })
        .collect();
    assert_eq!(
        validated,
        [
            (RATIFIED_PUBLISHER_ID.to_string(), "ratified_wl".to_string()),
            (OPAQUE_PUBLISHER_ID.to_string(), "opaque_wl".to_string()),
        ]
    );
}

#[tokio::test]
async fn test_fetch_operator_info_and_errors() {
    let ctx = setup_test_context(vec![]).await;
    let client = &ctx.client;
    let operator_info = &ctx.operator_info;

    let fetched = client.fetch_operator_info().await.expect("fetch_operator_info should succeed");
    assert_eq!(fetched.operator_domain, "test_operator_domain");
    assert_eq!(fetched.operator_role, "PRIMARY");

    *operator_info.lock().unwrap() = Ok(None);
    let err =
        client.fetch_operator_info().await.expect_err("missing operator_info should be rejected");
    assert!(matches!(err, EzManagementError::InvalidOperatorInfo(_)));

    let invalid_cases = [("", "PRIMARY"), ("example.com", "")];
    for (domain, role) in invalid_cases {
        *operator_info.lock().unwrap() = Ok(Some(ez_management::OperatorInfo {
            operator_domain: domain.to_string(),
            operator_role: role.to_string(),
        }));
        let err = client
            .fetch_operator_info()
            .await
            .expect_err("invalid operator_info should be rejected");
        assert!(
            matches!(err, EzManagementError::InvalidOperatorInfo(_)),
            "domain={domain:?} role={role:?}: {err:?}"
        );
    }

    *operator_info.lock().unwrap() = Err(tonic::Status::internal("failed to resolve role"));
    let err = client
        .fetch_operator_info()
        .await
        .expect_err("RPC error should return FetchOperatorInfoFailed");
    assert!(matches!(err, EzManagementError::FetchOperatorInfoFailed(_)));
}

#[tokio::test]
async fn test_fetch_isolate_startup_parameters_and_errors() {
    let ctx = setup_test_context(vec![]).await;
    let client = &ctx.client;
    let isolate_startup_parameters = &ctx.isolate_startup_parameters;
    let opaque = IsolateStartupParameters {
        identifier: Some(opaque_isolate_type("opaque_wl")),
        command_line_arguments: vec!["--foo=bar".to_string()],
        environment_variables: vec!["KEY=value".to_string()],
        number_of_isolates: 2,
    };

    *isolate_startup_parameters.lock().unwrap() = Ok(vec![opaque.clone()]);
    let fetched = client
        .fetch_isolate_startup_parameters()
        .await
        .expect("fetch_isolate_startup_parameters should succeed");
    assert_eq!(fetched, HashMap::from([(opaque_isolate_type("opaque_wl"), opaque.clone())]));

    let invalid_cases = [
        vec![IsolateStartupParameters { identifier: None, ..opaque.clone() }],
        vec![IsolateStartupParameters {
            identifier: Some(IsolateType {
                publisher_id: String::new(),
                isolate_name: "opaque_wl".to_string(),
            }),
            ..opaque.clone()
        }],
        vec![IsolateStartupParameters {
            identifier: Some(IsolateType {
                publisher_id: "example.com".to_string(),
                isolate_name: String::new(),
            }),
            ..opaque.clone()
        }],
        vec![IsolateStartupParameters { number_of_isolates: -1, ..opaque.clone() }],
        vec![IsolateStartupParameters { number_of_isolates: 0, ..opaque.clone() }],
        vec![IsolateStartupParameters { number_of_isolates: 65, ..opaque.clone() }],
        vec![opaque.clone(), opaque.clone()],
    ];
    for startup_parameters in invalid_cases {
        *isolate_startup_parameters.lock().unwrap() = Ok(startup_parameters.clone());
        let err = client
            .fetch_isolate_startup_parameters()
            .await
            .expect_err("invalid IsolateStartupParameters should be rejected");
        assert!(
            matches!(err, EzManagementError::InvalidIsolateStartupParameters(_)),
            "{startup_parameters:?}: {err:?}"
        );
    }

    *isolate_startup_parameters.lock().unwrap() =
        Err(tonic::Status::unimplemented("unimplemented"));
    let err = client
        .fetch_isolate_startup_parameters()
        .await
        .expect_err("RPC error should return FetchIsolateStartupParametersFailed");
    assert!(matches!(err, EzManagementError::FetchIsolateStartupParametersFailed(_)));
}
