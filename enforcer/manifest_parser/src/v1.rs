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
use manifest_proto::enforcer::v1::{
    EzBackendDependencies, EzBackendDependency, EzManifest, IsolateRuntimeConfigs,
};
use prost_reflect::prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use std::fs::read_to_string;
use std::path::Path;
use std::sync::OnceLock;

use super::parser_util::parse_proto_message;

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
