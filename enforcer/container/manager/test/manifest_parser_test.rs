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

use anyhow::{Context, Result};
use data_scope_proto::enforcer::v1::DataScopeType;
use manifest_parser::{get_strictest_scope, parse_isolate_runtime_configs, parse_manifest};
use manifest_proto::enforcer::{
    ez_manifest::ManifestType, BinaryManifest, BundleManifest, EzManifest, EzMethodSpec,
};
use prost_reflect::{DescriptorPool, DynamicMessage};

const JSON_MANIFEST_PATH: &str = "enforcer/container/manager/test/testdata/test_manifest.json";
const JSON_MANIFEST_EMPTY_PATH: &str =
    "enforcer/container/manager/test/testdata/test_manifest_empty.json";
const PROTO_DESCRIPTOR_PATH: &str = "enforcer/proto/manifest_descriptor_set.pb";
const PROTO_DESCRIPTOR_FILE_NAME: &str = "enforcer.EzManifest";
const TEXT_PROTO_MANIFEST_PATH: &str =
    "enforcer/container/manager/test/testdata/test_manifest.txtpb";
use std::collections::HashMap;

fn get_manifest_from_text_proto() -> Result<EzManifest> {
    // Get proto descriptor set file
    let file_desc_bytes =
        std::fs::read(PROTO_DESCRIPTOR_PATH).context(format!("opening {PROTO_DESCRIPTOR_PATH}"))?;

    let pool = DescriptorPool::decode(file_desc_bytes.as_slice())
        .context(format!("decoding {PROTO_DESCRIPTOR_PATH}"))?;

    let message_descriptor = pool
        .get_message_by_name(PROTO_DESCRIPTOR_FILE_NAME)
        .context(format!("Couldn't find message descriptor {PROTO_DESCRIPTOR_FILE_NAME}"))?;

    // Parse from manifest text proto into EzManifest
    let manifest_txtpb_string = std::fs::read_to_string(TEXT_PROTO_MANIFEST_PATH).context(
        format!("couldn't open manifest text proto file at: {}", TEXT_PROTO_MANIFEST_PATH),
    )?;

    let dynamic_message =
        DynamicMessage::parse_text_format(message_descriptor, &manifest_txtpb_string)
            .context("couldn't parse manifest text proto".to_string())?;

    let ez_manifest: EzManifest =
        dynamic_message.transcode_to().context("couldn't parse manifest file".to_string())?;
    Ok(ez_manifest)
}

fn get_method_specs_from_manifest(
    manifest: EzManifest,
) -> Result<HashMap<String, Vec<EzMethodSpec>>> {
    let mut service_method_specs = HashMap::new();
    if let Some(manifest_type) = manifest.manifest_type {
        match manifest_type {
            ManifestType::BinaryManifest(BinaryManifest { service_specs, .. }) => {
                for service_spec in service_specs {
                    service_method_specs
                        .insert(service_spec.service_name, service_spec.method_specs);
                }
            }
            ManifestType::BundleManifest(BundleManifest { manifests, .. }) => {
                for sub_manifest in manifests {
                    let sub_specs = get_method_specs_from_manifest(sub_manifest)?;
                    service_method_specs.extend(sub_specs);
                }
            }
            _ => panic!("Only Binary and Bundle manifests are supported"),
        }
    }
    Ok(service_method_specs)
}

#[test]
fn test_parse_manifest() {
    let result_manifest_proto =
        parse_manifest(JSON_MANIFEST_PATH.to_string()).expect("Failed to parse JSON manifest file");

    let expected_manifest_proto =
        get_manifest_from_text_proto().expect("Failed to parse Text Proto manifest file");

    assert_eq!(result_manifest_proto, expected_manifest_proto, "Expected and result are not equal");

    let result = parse_manifest(JSON_MANIFEST_EMPTY_PATH.to_string())
        .expect("Failed to parse JSON manifest file");
    assert_eq!(result, EzManifest::default(), "Expected and result are not equal");
}

#[test]
fn test_get_strictest_scope() {
    let manifest_proto = get_manifest_from_text_proto().expect("Failed to parse manifest");
    let service_method_specs =
        get_method_specs_from_manifest(manifest_proto).expect("Could not get method specs");

    let summation_service_specs =
        service_method_specs.get("SimpleAdd").expect("SimpleAdd not found");
    assert_eq!(
        get_strictest_scope(summation_service_specs.clone()),
        (DataScopeType::DomainOwned, DataScopeType::UserPrivate),
        "Expected Domain Owned scope"
    );

    let precomputed_backend_specs =
        service_method_specs.get("PrecomputedBackend").expect("PrecomputedBackend not found");
    assert_eq!(
        get_strictest_scope(precomputed_backend_specs.clone()),
        (DataScopeType::UserPrivate, DataScopeType::Public),
        "Expected User Private scope"
    );

    let empty_specs: Vec<EzMethodSpec> = vec![];
    assert_eq!(
        get_strictest_scope(empty_specs),
        (DataScopeType::Unspecified, DataScopeType::Unspecified),
        "Expected Unspecified scope"
    );
}

#[test]
fn test_parse_isolate_runtime_configs_success() {
    let json_str = r#"
        {
            "configs": [
                {
                    "publisher_id": "example.com",
                    "binary_filename": "/usr/bin/server",
                    "command_line_arguments": ["--arg1", "value1"],
                    "environment_variables": ["ENV_VAR=1"]
                }
            ]
        }
        "#;
    let result = parse_isolate_runtime_configs(json_str.to_string());
    assert!(result.is_ok());
    let configs = result.unwrap();
    assert_eq!(configs.configs.len(), 1);
    let config = &configs.configs[0];
    assert_eq!(config.publisher_id, "example.com");
    assert_eq!(config.binary_filename, "/usr/bin/server");
    assert_eq!(config.command_line_arguments, vec!["--arg1", "value1"]);
    assert_eq!(config.environment_variables, vec!["ENV_VAR=1"]);
}

#[test]
fn test_parse_isolate_runtime_configs_empty() {
    let json_str = "";
    let result = parse_isolate_runtime_configs(json_str.to_string());
    assert!(result.is_ok());
    let configs = result.unwrap();
    assert!(configs.configs.is_empty());
}

#[test]
fn test_parse_isolate_runtime_configs_no_configs() {
    let json_str = r#"{"configs":[]}"#;
    let result = parse_isolate_runtime_configs(json_str.to_string());
    assert!(result.is_ok());
    let configs = result.unwrap();
    assert!(configs.configs.is_empty());
}

#[test]
fn test_parse_isolate_runtime_configs_partial() {
    let json_str = r#"
        {
            "configs": [
                {
                    "publisher_id": "example.com",
                    "binary_filename": "/usr/bin/server"
                }
            ]
        }
        "#;
    let result = parse_isolate_runtime_configs(json_str.to_string());
    assert!(result.is_ok());
    let configs = result.unwrap();
    assert_eq!(configs.configs.len(), 1);
    let config = &configs.configs[0];
    assert_eq!(config.publisher_id, "example.com");
    assert_eq!(config.binary_filename, "/usr/bin/server");
    assert!(config.command_line_arguments.is_empty());
    assert!(config.environment_variables.is_empty());
}

#[test]
fn test_parse_isolate_runtime_configs_multiple() {
    let json_str = r#"
        {
            "configs": [
                {
                    "publisher_id": "example.com",
                    "binary_filename": "/usr/bin/server"
                },
                {
                    "publisher_id": "example.org",
                    "binary_filename": "/usr/bin/server2",
                    "command_line_arguments": ["--verbose"]
                }
            ]
        }
        "#;
    let result = parse_isolate_runtime_configs(json_str.to_string());
    assert!(result.is_ok());
    let configs = result.unwrap();
    assert_eq!(configs.configs.len(), 2);
    assert_eq!(configs.configs[0].binary_filename, "/usr/bin/server");
    assert_eq!(configs.configs[1].binary_filename, "/usr/bin/server2");
    assert_eq!(configs.configs[1].command_line_arguments, vec!["--verbose"]);
}

#[test]
fn test_parse_isolate_runtime_configs_invalid_json() {
    let json_str = r#"{"configs": [}"#;
    let result = parse_isolate_runtime_configs(json_str.to_string());
    assert!(result.is_err());
}

#[test]
fn test_parse_isolate_runtime_configs_type_mismatch() {
    let json_str = r#"
        {
            "configs": [
                {
                    "publisher_id": "example.com",
                    "binary_filename": 123
                }
            ]
        }
        "#;
    let result = parse_isolate_runtime_configs(json_str.to_string());
    assert!(result.is_err());
}
