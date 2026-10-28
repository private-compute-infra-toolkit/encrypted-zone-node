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

use binary_manifest_proto::enforcer::v2::{
    binary_manifest, BinaryManifest, RatifiedControls, SetupControls,
};
use intercepting_services_proto::enforcer::v2::InterceptingServices;
use manifest_parser::v2::{
    load_binary_manifest, parse_binary_manifest, parse_opaque_isolate_manifest,
    parse_ratified_isolate_manifest, IsolateIdentity, SetupManifest, WorkloadManifests,
};
use manifest_parser::{IsolateKind, ParsedIsolate};
use manifest_parser_test_utils::load_workload_manifests_from_paths;

const V2_SETUP_JSON_PATH: &str = "enforcer/manifest_parser/test/testdata/v2_setup.json";
const V2_RATIFIED_PUBSUB_BROKER_JSON_PATH: &str =
    "enforcer/manifest_parser/test/testdata/v2_ratified_pubsub_broker.json";
const V2_OPAQUE_HELLOWORLD_JSON_PATH: &str =
    "enforcer/manifest_parser/test/testdata/v2_opaque_helloworld.json";
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

fn load_manifest(path: &str) -> BinaryManifest {
    load_binary_manifest(path).expect("Failed to load BinaryManifest")
}

#[test]
fn test_load_v2_setup_manifest() {
    let setup =
        SetupManifest::load_from_path(V2_SETUP_JSON_PATH).expect("Failed to load setup manifest");
    let isolate = setup.into_parsed_isolate().expect("Failed to parse setup isolate");
    assert_eq!(isolate.isolate_name, "setup_isolate");
    assert_eq!(isolate.publisher_id, "pcit-release-bot@google.com");
    assert_eq!(isolate.package_filename, "enforcer/container/test_data/test_binary.tar");
    assert_eq!(isolate.binary_filename, "/usr/local/bin/setup");
    // The startup script in the package sets the Setup Isolate's arguments and environment.
    assert!(isolate.command_line_arguments.is_empty());
    assert!(isolate.environment_variables.is_empty());
    assert_eq!(isolate.kind, IsolateKind::Ratified { services_to_intercept: vec![] });
}

#[test]
fn test_v2_setup_manifest_rejects_other_kinds() {
    let setup = SetupManifest::new(load_manifest(V2_RATIFIED_PUBSUB_BROKER_JSON_PATH));
    let err = setup.into_parsed_isolate().unwrap_err();
    assert!(err.to_string().contains("isolate_kind to setup"), "{err}");
}

#[test]
fn test_v2_setup_manifest_requires_package_filename() {
    let mut manifest = load_manifest(V2_SETUP_JSON_PATH);
    manifest.isolate_kind = Some(binary_manifest::IsolateKind::Setup(SetupControls::default()));
    let err = SetupManifest::new(manifest).into_parsed_isolate().unwrap_err();
    assert!(err.to_string().contains("package_filename cannot be empty"), "{err}");
}

#[test]
fn test_v2_binary_manifest_ratified() {
    let isolate = ParsedIsolate::try_from(load_manifest(V2_RATIFIED_PUBSUB_BROKER_JSON_PATH))
        .expect("Failed to parse ratified BinaryManifest");
    assert_eq!(isolate.isolate_name, "pubsub_broker");
    assert_eq!(isolate.publisher_id, "pcit-release-bot@google.com");
    assert!(isolate.package_filename.is_empty());
    assert_eq!(isolate.binary_filename, "/usr/local/bin/main");
    assert_eq!(isolate.kind, IsolateKind::Ratified { services_to_intercept: vec![] });
    assert_eq!(isolate.service_specs.len(), 1);
    assert_eq!(isolate.service_specs[0].service_name, "PubSubBroker");
    assert_eq!(isolate.ez_backend_dependencies.len(), 1);
    assert_eq!(isolate.ez_backend_dependencies[0].operator_domain, "cdpush");
    let metrics_policy = isolate.metrics_policy.expect("Ratified metrics_policy should be parsed");
    assert_eq!(
        metrics_policy.allowed_metrics[0].name,
        "encrypted_zone.ratified_isolate.init_heartbeat"
    );
}

#[test]
fn test_v2_binary_manifest_opaque() {
    let isolate = ParsedIsolate::try_from(load_manifest(V2_OPAQUE_HELLOWORLD_JSON_PATH))
        .expect("Failed to parse opaque BinaryManifest");
    assert_eq!(isolate.isolate_name, "helloworld");
    assert_eq!(isolate.publisher_id, "helloworld_domain");
    assert_eq!(isolate.binary_filename, "/usr/local/bin/main");
    assert_eq!(isolate.kind, IsolateKind::Opaque);
    assert_eq!(isolate.service_specs.len(), 1);
    let metrics_policy = isolate.metrics_policy.expect("Opaque metrics_policy should be parsed");
    assert_eq!(metrics_policy.allowed_metrics[0].name, "test_metric");
}

#[test]
fn test_v2_binary_manifest_ratified_controls() {
    let mut manifest = load_manifest(V2_RATIFIED_PUBSUB_BROKER_JSON_PATH);
    manifest.isolate_kind = Some(binary_manifest::IsolateKind::Ratified(RatifiedControls {
        services_to_intercept: vec![InterceptingServices {
            intercepting_service_name: "Greeter".to_string(),
            interceptor_service_name: "Interceptor".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    }));
    let isolate = ParsedIsolate::try_from(manifest).expect("Failed to parse RatifiedControls");
    assert_eq!(isolate.services_to_intercept().len(), 1);
    assert_eq!(isolate.services_to_intercept()[0].interceptor_service_name, "Interceptor");
}

#[test]
fn test_v2_binary_manifest_rejects_ratified_controls_on_opaque() {
    let err = parse_binary_manifest(r#"{"opaque": {}, "ratified": {}}"#).unwrap_err();
    assert!(format!("{err:#}").contains("multiple fields provided for oneof"), "{err:#}");
}

#[test]
fn test_v2_binary_manifest_missing_kind_fails() {
    let mut manifest = load_manifest(V2_RATIFIED_PUBSUB_BROKER_JSON_PATH);
    manifest.isolate_kind = None;
    let err = ParsedIsolate::try_from(manifest).unwrap_err();
    assert!(err.to_string().contains("isolate_kind must be specified"), "{err}");
}

#[test]
fn test_v2_binary_manifest_empty_binary_filename_fails() {
    let mut manifest = load_manifest(V2_OPAQUE_HELLOWORLD_JSON_PATH);
    manifest.binary_filename = String::new();
    let err = ParsedIsolate::try_from(manifest).unwrap_err();
    assert!(err.to_string().contains("binary_filename cannot be empty"), "{err}");
}

#[test]
fn test_v2_binary_manifest_invalid_digest_fails() {
    for digest in ["", "abc", &"0123456789ABCDEF".repeat(4)] {
        for path in [V2_RATIFIED_PUBSUB_BROKER_JSON_PATH, V2_OPAQUE_HELLOWORLD_JSON_PATH] {
            let mut manifest = load_manifest(path);
            manifest.package_digest_sha256 = digest.to_string();
            let err = ParsedIsolate::try_from(manifest).unwrap_err();
            assert!(err.to_string().contains("package_digest_sha256"), "{err}");
        }
    }
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
    assert_eq!(isolates[0].isolate_name, "ratified_1");
    assert_eq!(isolates[0].publisher_id, "pcit-release-bot@google.com");
    assert_eq!(isolates[0].binary_filename, "/usr/local/bin/main");
    assert!(isolates[0].is_ratified());
    assert_eq!(isolates[0].ez_backend_dependencies.len(), 1);
    assert_eq!(isolates[0].ez_backend_dependencies[0].operator_domain, "EZ_Trusted");
    assert_eq!(isolates[0].ez_backend_dependencies[0].service_name, "Greeter");
    assert_eq!(isolates[0].ez_backend_dependencies[0].method_name, "SayHello");

    // Ratified isolate 2
    assert_eq!(isolates[1].isolate_name, "ratified_2");
    assert_eq!(isolates[1].publisher_id, "pcit-release-bot@google.com");
    assert_eq!(isolates[1].binary_filename, "/usr/local/bin/main");
    assert!(isolates[1].is_ratified());

    // Opaque isolate
    assert_eq!(isolates[2].isolate_name, "helloworld");
    assert_eq!(isolates[2].publisher_id, "helloworld_domain");
    assert_eq!(isolates[2].binary_filename, "/usr/local/bin/main");
    assert_eq!(isolates[2].command_line_arguments, &[] as &[String]);
    assert_eq!(isolates[2].environment_variables, &[] as &[String]);
    assert!(!isolates[2].is_ratified());
}

#[test]
fn test_v2_extract_sni_params() {
    let setup =
        SetupManifest::load_from_path(V2_SETUP_JSON_PATH).expect("Failed to load setup manifest");
    let setup_sni = setup.extract_sni_params();
    assert_eq!(setup_sni, IsolateIdentity::new("setup_isolate", "pcit-release-bot@google.com"));

    let workload =
        load_workload_manifests_from_paths(Some(V2_RATIFIED_JSON_PATH), Some(V2_OPAQUE_JSON_PATH))
            .expect("Failed to load workload manifests");
    let workload_snis = workload.extract_sni_params();
    assert_eq!(workload_snis.len(), 3);
    assert_eq!(workload_snis[0], IsolateIdentity::new("ratified_1", "pcit-release-bot@google.com"));
    assert_eq!(workload_snis[1], IsolateIdentity::new("ratified_2", "pcit-release-bot@google.com"));
    assert_eq!(workload_snis[2], IsolateIdentity::new("helloworld", "helloworld_domain"));
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
    assert_eq!(oi.command_line_arguments, &[] as &[String]);
    assert_eq!(oi.environment_variables, &[] as &[String]);

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
    assert_eq!(ri.command_line_arguments, &[] as &[String]);
    assert_eq!(ri.environment_variables, &[] as &[String]);
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
    assert_eq!(oi.command_line_arguments, &[] as &[String]);
    assert_eq!(oi.environment_variables, &[] as &[String]);

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
    assert_eq!(ri.command_line_arguments, vec!["--policy-flag=active"]);
    assert_eq!(ri.environment_variables, vec!["BASE_ENV=1"]);
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
