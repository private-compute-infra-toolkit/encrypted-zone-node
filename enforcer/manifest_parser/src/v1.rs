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

use anyhow::{Context, Result};
use manifest_proto::enforcer::v1::ez_manifest::ManifestType;
use manifest_proto::enforcer::v1::{
    EzBackendDependencies, EzBackendDependency, EzManifest, IsolateRuntimeConfigs,
};
use prost_reflect::prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use std::fs::read_to_string;
use std::path::Path;
use std::sync::OnceLock;

use super::parser_util::parse_proto_message;
use super::{IsolateKind, ParsedIsolate};

const PROTO_DESCRIPTOR_V1_BYTES: &[u8] = include_bytes!(env!("MANIFEST_V1_DESCRIPTOR_SET_PATH"));

static V1_DESCRIPTOR_POOL: OnceLock<DescriptorPool> = OnceLock::new();

fn get_v1_descriptor_pool() -> &'static DescriptorPool {
    V1_DESCRIPTOR_POOL.get_or_init(|| {
        DescriptorPool::decode(PROTO_DESCRIPTOR_V1_BYTES)
            .expect("embedded v1 descriptor set bytes must be valid")
    })
}

/// Parses a legacy (v1) JSON manifest file into an [`EzManifest`].
pub fn parse_manifest(manifest_path: impl AsRef<Path>) -> Result<EzManifest> {
    let path = manifest_path.as_ref();
    let manifest_json_string = read_to_string(path)
        .context(format!("couldn't open manifest Json file at: {}", path.display()))?;
    parse_proto_message(get_v1_descriptor_pool(), "enforcer.v1.EzManifest", &manifest_json_string)
}

/// Flattens an [`EzManifest`] (binary or bundle) into a list of [`ParsedIsolate`]s.
pub fn flatten_manifest(ez_manifest: EzManifest) -> Result<Vec<ParsedIsolate>> {
    let mut isolates = Vec::new();
    flatten_manifest_helper(ez_manifest, &mut isolates)?;
    Ok(isolates)
}

fn flatten_manifest_helper(ez_manifest: EzManifest, output: &mut Vec<ParsedIsolate>) -> Result<()> {
    let manifest_type =
        ez_manifest.manifest_type.context("manifest_type can't be empty in EzManifest")?;
    match manifest_type {
        ManifestType::BundleManifest(bundle_manifest) => {
            for manifest in bundle_manifest.manifests {
                flatten_manifest_helper(manifest, output)?;
            }
        }
        ManifestType::BinaryManifest(manifest) => {
            if manifest.number_of_isolates < 0 {
                anyhow::bail!(
                    "number_of_isolates cannot be negative, got {}",
                    manifest.number_of_isolates
                );
            }
            let kind = if manifest.is_ratified_isolate {
                IsolateKind::Ratified {
                    binary_filename: manifest.binary_filename,
                    command_line_arguments: manifest.command_line_arguments,
                    environment_variables: manifest.environment_variables,
                    services_to_intercept: manifest.services_to_intercept,
                }
            } else {
                IsolateKind::Opaque {
                    binary_filename: manifest.binary_filename,
                    command_line_arguments: manifest.command_line_arguments,
                    environment_variables: manifest.environment_variables,
                }
            };
            output.push(ParsedIsolate {
                isolate_name: ez_manifest.isolate_name,
                publisher_id: ez_manifest.publisher_id,
                package_filename: ez_manifest.package_filename,
                number_of_isolates: manifest.number_of_isolates,
                service_specs: manifest.service_specs,
                ez_backend_dependencies: manifest.ez_backend_dependencies,
                metrics_policy: manifest.metrics_policy,
                kind,
            });
        }
        _ => anyhow::bail!("Provided ManifestType in EzManifest is not supported yet"),
    }
    Ok(())
}

/// Parses a JSON string into an [`IsolateRuntimeConfigs`] proto.
pub fn parse_isolate_runtime_configs(configs_json: &str) -> Result<IsolateRuntimeConfigs> {
    if configs_json.is_empty() {
        return Ok(IsolateRuntimeConfigs::default());
    }
    parse_proto_message(get_v1_descriptor_pool(), "enforcer.v1.IsolateRuntimeConfigs", configs_json)
}

/// Serializes a vector of [`EzBackendDependency`] into a textproto string.
pub fn serialize_backend_dependencies(deps: Vec<EzBackendDependency>) -> Result<String> {
    let wrapped = EzBackendDependencies { ez_backend_dependencies: deps };
    let bytes = wrapped.encode_to_vec();

    let pool = get_v1_descriptor_pool();
    let descriptor = pool
        .get_message_by_name("enforcer.v1.EzBackendDependencies")
        .context("Couldn't find message descriptor for enforcer.v1.EzBackendDependencies")?;

    let dynamic_message = DynamicMessage::decode(descriptor, &bytes[..])
        .context("failed to decode into DynamicMessage")?;

    Ok(dynamic_message.to_string())
}
