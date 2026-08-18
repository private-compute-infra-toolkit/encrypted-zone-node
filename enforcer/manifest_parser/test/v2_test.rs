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

use manifest_parser::{SetupManifest, WorkloadManifests};

const V2_SETUP_JSON_PATH: &str =
    "enforcer/manifest_parser/test/testdata/test_manifest_v2_setup.json";
const V2_RATIFIED_JSON_PATH: &str =
    "enforcer/manifest_parser/test/testdata/test_manifest_v2_ratified.json";
const V2_OPAQUE_JSON_PATH: &str =
    "enforcer/manifest_parser/test/testdata/test_manifest_v2_opaque.json";

#[test]
fn test_load_v2_setup_manifest() {
    let setup =
        SetupManifest::load_from_path(V2_SETUP_JSON_PATH).expect("Failed to load setup manifest");
    assert!(setup.setup_isolate_manifest.setup_isolate_descriptor.is_some());
    let isolate = setup.into_parsed_isolate();
    assert_eq!(isolate.isolate_name, "ezpkg://setup.example.com");
    assert_eq!(isolate.publisher_id, "EZ_Trusted");
    assert_eq!(isolate.binary_manifest.binary_filename, "/usr/local/bin/setup");
    assert!(isolate.binary_manifest.is_ratified_isolate);
}

#[test]
fn test_load_v2_workload_manifests() {
    let workload =
        WorkloadManifests::load_from_paths(Some(V2_RATIFIED_JSON_PATH), Some(V2_OPAQUE_JSON_PATH))
            .expect("Failed to load workload manifests");
    assert!(workload.ratified_isolate_manifest.is_some());
    assert!(workload.opaque_isolate_manifest.is_some());

    let isolates = workload.into_parsed_isolates();
    assert_eq!(isolates.len(), 3);

    // Ratified isolate 1 (with ez_backend_dependencies)
    assert_eq!(isolates[0].isolate_name, "ezpkg://ratified1.example.com");
    assert_eq!(isolates[0].publisher_id, "EZ_Trusted");
    assert_eq!(isolates[0].binary_manifest.binary_filename, "/usr/local/bin/main");
    assert!(isolates[0].binary_manifest.is_ratified_isolate);
    assert_eq!(isolates[0].binary_manifest.ez_backend_dependencies.len(), 1);
    assert_eq!(
        isolates[0].binary_manifest.ez_backend_dependencies[0].operator_domain,
        "EZ_Trusted"
    );
    assert_eq!(isolates[0].binary_manifest.ez_backend_dependencies[0].service_name, "Greeter");
    assert_eq!(isolates[0].binary_manifest.ez_backend_dependencies[0].method_name, "SayHello");

    // Ratified isolate 2
    assert_eq!(isolates[1].isolate_name, "ezpkg://ratified2.example.com");
    assert_eq!(isolates[1].publisher_id, "EZ_Trusted");
    assert_eq!(isolates[1].binary_manifest.binary_filename, "/usr/local/bin/main");
    assert!(isolates[1].binary_manifest.is_ratified_isolate);

    // Opaque isolate
    assert_eq!(isolates[2].isolate_name, "ezpkg://helloworld.com");
    assert_eq!(isolates[2].publisher_id, "helloworld_domain");
    assert_eq!(isolates[2].binary_manifest.binary_filename, "/usr/local/bin/main");
    assert!(!isolates[2].binary_manifest.is_ratified_isolate);
}

#[test]
fn test_v2_extract_sni_params() {
    let setup =
        SetupManifest::load_from_path(V2_SETUP_JSON_PATH).expect("Failed to load setup manifest");
    let setup_sni = setup.extract_sni_params();
    assert_eq!(setup_sni, Some(("ezpkg://setup.example.com", "EZ_Trusted")));

    let workload =
        WorkloadManifests::load_from_paths(Some(V2_RATIFIED_JSON_PATH), Some(V2_OPAQUE_JSON_PATH))
            .expect("Failed to load workload manifests");
    let workload_snis = workload.extract_sni_params();
    assert_eq!(workload_snis.len(), 3);
    assert_eq!(workload_snis[0], ("ezpkg://ratified1.example.com", "EZ_Trusted"));
    assert_eq!(workload_snis[1], ("ezpkg://ratified2.example.com", "EZ_Trusted"));
    assert_eq!(workload_snis[2], ("ezpkg://helloworld.com", "helloworld_domain"));
}
