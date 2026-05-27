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
use manifest_proto::enforcer::v1::{
    EzBackendDependencies, EzBackendDependency, EzManifest, EzMethodSpec, IsolateRuntimeConfigs,
};
use prost_reflect::prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::de::Deserializer;

const PROTO_DESCRIPTOR_BYTES: &[u8] = include_bytes!(env!("TEST_MANIFEST_DESCRIPTOR_SET_PATH"));
const PROTO_DESCRIPTOR_MESSAGE: &str = "enforcer.v1.EzManifest";

/// Parses a JSON manifest file into an `EzManifest` proto.
///
/// This function reads a JSON file from the given `manifest_path`,
/// uses a pre-compiled proto descriptor set to deserialize the JSON
/// into a `DynamicMessage`, and then transcodes it to an `EzManifest`.
///
/// # Arguments
///
/// * `manifest_path` - A `String` slice that holds the path to the manifest JSON file.
///
/// # Returns
///
/// A `Result` which is either:
/// - `Ok(EzManifest)` containing the parsed manifest.
/// - `Err(anyhow::Error)` if file reading, descriptor loading, or parsing fails.
pub fn parse_manifest(manifest_path: String) -> Result<EzManifest> {
    let pool = DescriptorPool::decode(PROTO_DESCRIPTOR_BYTES)
        .context("decoding manifest descriptor set")?;

    let message_descriptor = pool
        .get_message_by_name(PROTO_DESCRIPTOR_MESSAGE)
        .context(format!("Couldn't find message descriptor for {PROTO_DESCRIPTOR_MESSAGE}"))?;

    // Parse provided manifest json file into EzManifest
    let manifest_json_string = std::fs::read_to_string(&manifest_path)
        .context(format!("couldn't open manifest Json file at: {}", manifest_path))?;
    let mut deserializer = Deserializer::from_str(&manifest_json_string);
    let dynamic_message = DynamicMessage::deserialize(message_descriptor, &mut deserializer)
        .context("couldn't parse manifest file".to_string())?;

    let ez_manifest: EzManifest =
        dynamic_message.transcode_to().context("couldn't parse manifest file".to_string())?;
    Ok(ez_manifest)
}

/// Parses a JSON string into an `IsolateRuntimeConfigs` proto.
///
/// This function reads a JSON string containing serialized IsolateRuntimeConfigs
/// uses a pre-compiled proto descriptor set to deserialize the JSON
/// into a `DynamicMessage`, and then transcodes it to an `IsolateRuntimeConfigs`.
///
/// # Arguments
///
/// * `configs_json` - A `String` slice that holds the serialized json string of IsolateRuntimeConfigs. If the string is empty, we return an empty IsolateRuntimeConfigs.
///
/// # Returns
///
/// A `Result` which is either:
/// - `Ok(IsolateRuntimeConfigs)` containing the parsed IsolateRuntimeConfigs.
/// - `Err(anyhow::Error)` if parsing fails.
pub fn parse_isolate_runtime_configs(configs_json: String) -> Result<IsolateRuntimeConfigs> {
    if configs_json.is_empty() {
        return Ok(IsolateRuntimeConfigs::default());
    }
    let pool = DescriptorPool::decode(PROTO_DESCRIPTOR_BYTES)
        .context("decoding manifest descriptor set")?;
    let message_descriptor =
        pool.get_message_by_name("enforcer.v1.IsolateRuntimeConfigs").context(
            "Couldn't find message descriptor enforcer.v1.IsolateRuntimeConfigs".to_string(),
        )?;
    let mut deserializer = Deserializer::from_str(&configs_json);
    let dynamic_message = DynamicMessage::deserialize(message_descriptor, &mut deserializer)
        .context("couldn't parse isolate config configs".to_string())?;
    let configs: IsolateRuntimeConfigs = dynamic_message
        .transcode_to()
        .context("couldn't parse isolate config configs".to_string())?;
    Ok(configs)
}

/// Determines the strictest input and output `DataScopeType` from a vector of `EzMethodSpec`.
///
/// This function iterates through each `EzMethodSpec` and its `input_scope_types`
/// and `output_scope_types`, returning a tuple containing the highest enum value
/// found for both input and output scopes. The highest value represents the strictest scope.
/// `DataScopeType::Unspecified` is returned for a scope type if no scopes are found or if an
/// invalid scope value is encountered.
///
/// # Arguments
///
/// * `method_specs` - A `Vec<EzMethodSpec>` containing the method specifications.
///
/// # Returns
///
/// A tuple `(DataScopeType, DataScopeType)` representing the strictest input scope and strictest
/// output scope found, respectively.
pub fn get_strictest_scope(method_specs: Vec<EzMethodSpec>) -> (DataScopeType, DataScopeType) {
    let strictest_input_scope = method_specs
        .iter()
        .flat_map(|spec| &spec.input_scope_types)
        .max()
        .copied()
        .unwrap_or(DataScopeType::Unspecified as i32);
    let strictest_output_scope = method_specs
        .iter()
        .flat_map(|spec| &spec.output_scope_types)
        .max()
        .copied()
        .unwrap_or(DataScopeType::Unspecified as i32);

    (
        DataScopeType::try_from(strictest_input_scope).unwrap_or(DataScopeType::Unspecified),
        DataScopeType::try_from(strictest_output_scope).unwrap_or(DataScopeType::Unspecified),
    )
}

/// Serializes a vector of `EzBackendDependency` into a textproto string.
///
/// This function wraps the dependencies in an `EzBackendDependencies` proto message,
/// serializes it to binary, decodes it into a `DynamicMessage` using the pre-compiled
/// descriptors, and then formats it as a textproto string.
pub fn serialize_backend_dependencies(deps: Vec<EzBackendDependency>) -> Result<String> {
    let wrapped = EzBackendDependencies { ez_backend_dependencies: deps };
    let bytes = wrapped.encode_to_vec();

    let pool = DescriptorPool::decode(PROTO_DESCRIPTOR_BYTES)
        .context("failed to decode proto descriptor set")?;
    let descriptor = pool
        .get_message_by_name("enforcer.v1.EzBackendDependencies")
        .context("Couldn't find message descriptor for enforcer.v1.EzBackendDependencies")?;

    let dynamic_message = DynamicMessage::decode(descriptor, &bytes[..])
        .context("failed to decode into DynamicMessage")?;

    Ok(dynamic_message.to_string())
}
