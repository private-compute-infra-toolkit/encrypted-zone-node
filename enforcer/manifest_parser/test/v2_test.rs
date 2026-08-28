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

use manifest_parser::v2::{
    parse_opaque_isolate_manifest, parse_ratified_isolate_manifest, parse_setup_isolate_manifest,
    IsolateIdentity, SetupManifest, WorkloadManifests,
};
use manifest_parser_test_utils::load_workload_manifests_from_paths;

const V2_SETUP_JSON_PATH: &str = "enforcer/manifest_parser/test/testdata/v2_setup.json";
const V2_RATIFIED_JSON_PATH: &str = "enforcer/manifest_parser/test/testdata/v2_ratified.json";
const V2_OPAQUE_JSON_PATH: &str = "enforcer/manifest_parser/test/testdata/v2_opaque.json";

const V2_OPAQUE_DEFAULT_STARTUP_PARAMS_JSON_PATH: &str =
    "enforcer/manifest_parser/test/testdata/v2_opaque_default_startup_params.json";
const V2_RATIFIED_DEFAULT_STARTUP_PARAMS_JSON_PATH: &str =
    "enforcer/manifest_parser/test/testdata/v2_ratified_default_startup_params.json";
const V2_OPAQUE_EXPLICIT_STARTUP_PARAMS_JSON_PATH: &str =
    "enforcer/manifest_parser/test/testdata/v2_opaque_explicit_startup_params.json";
const V2_RATIFIED_EXPLICIT_STARTUP_PARAMS_JSON_PATH: &str =
    "enforcer/manifest_parser/test/testdata/v2_ratified_explicit_startup_params.json";

#[test]
fn test_load_v2_setup_manifest() {
    let raw_setup = parse_setup_isolate_manifest(V2_SETUP_JSON_PATH)
        .expect("Failed to parse raw setup manifest");
    let setup = SetupManifest::new(raw_setup);
    assert!(setup.setup_isolate_manifest.setup_isolate_descriptor.is_some());
    let isolate = setup.into_parsed_isolate().expect("Failed to parse setup isolate");
    assert_eq!(isolate.isolate_name, "ezpkg://setup.example.com");
    assert_eq!(isolate.publisher_id, "EZ_Trusted");
    assert_eq!(isolate.binary_filename(), "/usr/local/bin/setup");
    assert_eq!(isolate.command_line_arguments(), &["--init"]);
    assert_eq!(isolate.number_of_isolates, 1);
    assert!(isolate.is_ratified());
}

#[test]
fn test_load_v2_workload_manifests() {
    let ratified = parse_ratified_isolate_manifest(V2_RATIFIED_JSON_PATH)
        .expect("Failed to parse ratified manifest");
    let opaque = parse_opaque_isolate_manifest(V2_OPAQUE_JSON_PATH)
        .expect("Failed to parse opaque manifest");

    let workload = WorkloadManifests::new(Some(ratified), Some(opaque));
    assert!(workload.ratified_isolate_manifest.is_some());
    assert!(workload.opaque_isolate_manifest.is_some());

    let isolates = workload.into_parsed_isolates().expect("Failed to parse workload isolates");
    assert_eq!(isolates.len(), 3);

    // Ratified isolate 1 (with ez_backend_dependencies)
    assert_eq!(isolates[0].isolate_name, "ezpkg://ratified1.example.com");
    assert_eq!(isolates[0].publisher_id, "EZ_Trusted");
    assert_eq!(isolates[0].binary_filename(), "/usr/local/bin/main");
    assert!(isolates[0].is_ratified());
    assert_eq!(isolates[0].ez_backend_dependencies.len(), 1);
    assert_eq!(isolates[0].ez_backend_dependencies[0].operator_domain, "EZ_Trusted");
    assert_eq!(isolates[0].ez_backend_dependencies[0].service_name, "Greeter");
    assert_eq!(isolates[0].ez_backend_dependencies[0].method_name, "SayHello");

    // Ratified isolate 2
    assert_eq!(isolates[1].isolate_name, "ezpkg://ratified2.example.com");
    assert_eq!(isolates[1].publisher_id, "EZ_Trusted");
    assert_eq!(isolates[1].binary_filename(), "/usr/local/bin/main");
    assert!(isolates[1].is_ratified());

    // Opaque isolate
    assert_eq!(isolates[2].isolate_name, "ezpkg://helloworld.com");
    assert_eq!(isolates[2].publisher_id, "helloworld_domain");
    assert_eq!(isolates[2].binary_filename(), "/usr/local/bin/main");
    assert_eq!(isolates[2].command_line_arguments(), &[] as &[String]);
    assert_eq!(isolates[2].environment_variables(), &[] as &[String]);
    assert!(!isolates[2].is_ratified());
}

#[test]
fn test_v2_extract_sni_params() {
    let setup =
        SetupManifest::load_from_path(V2_SETUP_JSON_PATH).expect("Failed to load setup manifest");
    let setup_sni = setup.extract_sni_params();
    assert_eq!(setup_sni, IsolateIdentity::new("ezpkg://setup.example.com", "EZ_Trusted"));

    let workload =
        load_workload_manifests_from_paths(Some(V2_RATIFIED_JSON_PATH), Some(V2_OPAQUE_JSON_PATH))
            .expect("Failed to load workload manifests");
    let workload_snis = workload.extract_sni_params();
    assert_eq!(workload_snis.len(), 3);
    assert_eq!(
        workload_snis[0],
        IsolateIdentity::new("ezpkg://ratified1.example.com", "EZ_Trusted")
    );
    assert_eq!(
        workload_snis[1],
        IsolateIdentity::new("ezpkg://ratified2.example.com", "EZ_Trusted")
    );
    assert_eq!(
        workload_snis[2],
        IsolateIdentity::new("ezpkg://helloworld.com", "helloworld_domain")
    );
}

#[test]
fn test_v2_default_fallbacks() {
    let raw_opaque = parse_opaque_isolate_manifest(V2_OPAQUE_DEFAULT_STARTUP_PARAMS_JSON_PATH)
        .expect("Failed to parse defaults opaque manifest");
    let workload_opaque = WorkloadManifests::new(None, Some(raw_opaque));
    let opaque_isolates =
        workload_opaque.into_parsed_isolates().expect("Failed to parse default opaque isolates");
    assert_eq!(opaque_isolates.len(), 1);
    let oi = &opaque_isolates[0];
    assert_eq!(oi.number_of_isolates, 0);
    assert_eq!(oi.command_line_arguments(), &[] as &[String]);
    assert_eq!(oi.environment_variables(), &[] as &[String]);

    let raw_ratified =
        parse_ratified_isolate_manifest(V2_RATIFIED_DEFAULT_STARTUP_PARAMS_JSON_PATH)
            .expect("Failed to parse defaults ratified manifest");
    let workload_ratified = WorkloadManifests::new(Some(raw_ratified), None);
    let ratified_isolates = workload_ratified
        .into_parsed_isolates()
        .expect("Failed to parse default ratified isolates");
    assert_eq!(ratified_isolates.len(), 1);
    let ri = &ratified_isolates[0];
    assert_eq!(ri.number_of_isolates, 0);
    assert_eq!(ri.command_line_arguments(), &[] as &[String]);
    assert_eq!(ri.environment_variables(), &[] as &[String]);
}

#[test]
fn test_v2_explicit_overrides() {
    let raw_opaque = parse_opaque_isolate_manifest(V2_OPAQUE_EXPLICIT_STARTUP_PARAMS_JSON_PATH)
        .expect("Failed to parse explicit opaque manifest");
    let workload_opaque = WorkloadManifests::new(None, Some(raw_opaque));
    let opaque_isolates =
        workload_opaque.into_parsed_isolates().expect("Failed to parse explicit opaque isolates");
    assert_eq!(opaque_isolates.len(), 1);
    let oi = &opaque_isolates[0];
    assert_eq!(oi.number_of_isolates, 3);
    assert_eq!(oi.command_line_arguments(), &[] as &[String]);
    assert_eq!(oi.environment_variables(), &[] as &[String]);

    let raw_ratified =
        parse_ratified_isolate_manifest(V2_RATIFIED_EXPLICIT_STARTUP_PARAMS_JSON_PATH)
            .expect("Failed to parse explicit ratified manifest");
    let workload_ratified = WorkloadManifests::new(Some(raw_ratified), None);
    let ratified_isolates = workload_ratified
        .into_parsed_isolates()
        .expect("Failed to parse explicit ratified isolates");
    assert_eq!(ratified_isolates.len(), 1);
    let ri = &ratified_isolates[0];
    assert_eq!(ri.number_of_isolates, 2);
    assert_eq!(ri.command_line_arguments(), vec!["--policy-flag=active"]);
    assert_eq!(ri.environment_variables(), vec!["BASE_ENV=1"]);
}

#[test]
fn test_v2_negative_number_of_isolates_fails() {
    use common_proto::enforcer::v2::IsolateType;
    use isolate_startup_parameters_proto::enforcer::v2::IsolateStartupParameters;
    use opaque_isolate_manifest_proto::enforcer::v2::{
        OpaqueIsolateDescriptor, OpaqueIsolateManifest,
    };

    let manifest = OpaqueIsolateManifest {
        opaque_isolate_descriptors: vec![OpaqueIsolateDescriptor {
            isolate_type: Some(IsolateType {
                isolate_name: "ezpkg://test.com".to_string(),
                publisher_id: "test_pub".to_string(),
            }),
            package_filename: "test.tar".to_string(),
            binary_filename: "/bin/main".to_string(),
            startup_parameters: Some(IsolateStartupParameters {
                number_of_isolates: -1,
                ..Default::default()
            }),
            ..Default::default()
        }],
    };

    let workload = WorkloadManifests::new(None, Some(manifest));
    let result = workload.into_parsed_isolates();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cannot be negative"));
}

#[test]
fn test_v2_missing_isolate_type_fails() {
    use opaque_isolate_manifest_proto::enforcer::v2::{
        OpaqueIsolateDescriptor, OpaqueIsolateManifest,
    };

    let manifest = OpaqueIsolateManifest {
        opaque_isolate_descriptors: vec![OpaqueIsolateDescriptor {
            isolate_type: None,
            package_filename: "test.tar".to_string(),
            binary_filename: "/bin/main".to_string(),
            ..Default::default()
        }],
    };

    let workload = WorkloadManifests::new(None, Some(manifest));
    let result = workload.into_parsed_isolates();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("isolate_type must be specified"));
}

#[test]
fn test_v2_empty_isolate_name_or_publisher_id_fails() {
    use common_proto::enforcer::v2::IsolateType;
    use opaque_isolate_manifest_proto::enforcer::v2::{
        OpaqueIsolateDescriptor, OpaqueIsolateManifest,
    };

    let empty_name = OpaqueIsolateManifest {
        opaque_isolate_descriptors: vec![OpaqueIsolateDescriptor {
            isolate_type: Some(IsolateType {
                isolate_name: "".to_string(),
                publisher_id: "test_pub".to_string(),
            }),
            package_filename: "test.tar".to_string(),
            binary_filename: "/bin/main".to_string(),
            ..Default::default()
        }],
    };
    assert!(WorkloadManifests::new(None, Some(empty_name)).into_parsed_isolates().is_err());

    let empty_pub = OpaqueIsolateManifest {
        opaque_isolate_descriptors: vec![OpaqueIsolateDescriptor {
            isolate_type: Some(IsolateType {
                isolate_name: "ezpkg://test.com".to_string(),
                publisher_id: "".to_string(),
            }),
            package_filename: "test.tar".to_string(),
            binary_filename: "/bin/main".to_string(),
            ..Default::default()
        }],
    };
    assert!(WorkloadManifests::new(None, Some(empty_pub)).into_parsed_isolates().is_err());
}

#[test]
fn test_v2_empty_service_or_method_name_fails() {
    use common_proto::enforcer::v2::IsolateType;
    use ez_service_spec_proto::enforcer::v2::{EzMethodSpec, EzServiceSpec};
    use opaque_isolate_manifest_proto::enforcer::v2::{
        OpaqueIsolateDescriptor, OpaqueIsolateManifest,
    };

    let empty_service = OpaqueIsolateManifest {
        opaque_isolate_descriptors: vec![OpaqueIsolateDescriptor {
            isolate_type: Some(IsolateType {
                isolate_name: "ezpkg://test.com".to_string(),
                publisher_id: "test_pub".to_string(),
            }),
            package_filename: "test.tar".to_string(),
            binary_filename: "/bin/main".to_string(),
            service_specs: vec![EzServiceSpec {
                service_name: "".to_string(),
                method_specs: vec![],
            }],
            ..Default::default()
        }],
    };
    assert!(WorkloadManifests::new(None, Some(empty_service)).into_parsed_isolates().is_err());

    let empty_method = OpaqueIsolateManifest {
        opaque_isolate_descriptors: vec![OpaqueIsolateDescriptor {
            isolate_type: Some(IsolateType {
                isolate_name: "ezpkg://test.com".to_string(),
                publisher_id: "test_pub".to_string(),
            }),
            package_filename: "test.tar".to_string(),
            binary_filename: "/bin/main".to_string(),
            service_specs: vec![EzServiceSpec {
                service_name: "TestService".to_string(),
                method_specs: vec![EzMethodSpec {
                    method_name: "".to_string(),
                    ..Default::default()
                }],
            }],
            ..Default::default()
        }],
    };
    assert!(WorkloadManifests::new(None, Some(empty_method)).into_parsed_isolates().is_err());
}
