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

use anyhow::{ensure, Context, Result};
use opaque_isolate_manifest_proto::enforcer::v2::OpaqueIsolateManifest;
use prost_reflect::DescriptorPool;
use ratified_isolate_manifest_proto::enforcer::v2::RatifiedIsolateManifest;
use setup_isolate_manifest_proto::enforcer::v2::SetupIsolateManifest;
use std::fs::read_to_string;
use std::path::Path;
use std::sync::OnceLock;

use super::parser_util::{
    convert_opaque_descriptor_to_parsed_isolate, convert_ratified_descriptor_to_parsed_isolate,
    convert_setup_descriptor_to_parsed_isolate, parse_proto_message,
};
use super::ParsedIsolate;

const PROTO_DESCRIPTOR_V2_BYTES: &[u8] = include_bytes!(env!("MANIFEST_V2_DESCRIPTOR_SET_PATH"));

static V2_DESCRIPTOR_POOL: OnceLock<DescriptorPool> = OnceLock::new();

fn get_v2_descriptor_pool() -> &'static DescriptorPool {
    V2_DESCRIPTOR_POOL.get_or_init(|| {
        DescriptorPool::decode(PROTO_DESCRIPTOR_V2_BYTES)
            .expect("embedded v2 descriptor set bytes must be valid")
    })
}

/// Container for a setup isolate manifest (v2 schema).
#[derive(Clone, Debug, PartialEq)]
pub struct SetupManifest {
    pub setup_isolate_manifest: SetupIsolateManifest,
}

/// Container for workload isolate manifests (ratified and opaque isolates, v2 schema).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WorkloadManifests {
    pub ratified_isolate_manifest: Option<RatifiedIsolateManifest>,
    pub opaque_isolate_manifest: Option<OpaqueIsolateManifest>,
}

impl SetupManifest {
    /// Loads and parses the setup isolate manifest from a JSON file path.
    pub fn load_from_path(setup_path: impl AsRef<Path>) -> Result<Self> {
        let setup_isolate_manifest = parse_setup_isolate_manifest(setup_path)?;
        Ok(Self { setup_isolate_manifest })
    }

    /// Converts the setup isolate manifest into a [`ParsedIsolate`].
    pub fn into_parsed_isolate(self) -> ParsedIsolate {
        convert_setup_descriptor_to_parsed_isolate(
            self.setup_isolate_manifest.setup_isolate_descriptor.unwrap_or_default(),
        )
    }

    /// Extracts `(isolate_name, publisher_id)` for the setup isolate descriptor.
    pub fn extract_sni_params(&self) -> Option<(&str, &str)> {
        self.setup_isolate_manifest
            .setup_isolate_descriptor
            .as_ref()
            .map(|d| (d.isolate_name.as_str(), d.publisher_id.as_str()))
    }
}

impl WorkloadManifests {
    /// Loads and parses workload (ratified and opaque) isolate manifests from file paths.
    pub fn load_from_paths(
        ratified_path: Option<impl AsRef<Path>>,
        opaque_path: Option<impl AsRef<Path>>,
    ) -> Result<Self> {
        let ratified = ratified_path.map(parse_ratified_isolate_manifest).transpose()?;
        let opaque = opaque_path.map(parse_opaque_isolate_manifest).transpose()?;

        Ok(Self { ratified_isolate_manifest: ratified, opaque_isolate_manifest: opaque })
    }

    /// Flattens workload descriptors into a sequence of [`ParsedIsolate`]s.
    pub fn into_parsed_isolates(self) -> Vec<ParsedIsolate> {
        let ratified_iter = self
            .ratified_isolate_manifest
            .into_iter()
            .flat_map(|m| m.ratified_isolate_descriptors)
            .map(convert_ratified_descriptor_to_parsed_isolate);

        let opaque_iter = self
            .opaque_isolate_manifest
            .into_iter()
            .flat_map(|m| m.opaque_isolate_descriptors)
            .map(convert_opaque_descriptor_to_parsed_isolate);

        ratified_iter.chain(opaque_iter).collect()
    }

    /// Extracts `(isolate_name, publisher_id)` pairs across workload descriptors for mTLS SNI configuration.
    pub fn extract_sni_params(&self) -> Vec<(&str, &str)> {
        let ratified_iter = self
            .ratified_isolate_manifest
            .iter()
            .flat_map(|m| &m.ratified_isolate_descriptors)
            .map(|d| (d.isolate_name.as_str(), d.publisher_id.as_str()));

        let opaque_iter = self
            .opaque_isolate_manifest
            .iter()
            .flat_map(|m| &m.opaque_isolate_descriptors)
            .map(|d| (d.isolate_name.as_str(), d.publisher_id.as_str()));

        ratified_iter.chain(opaque_iter).collect()
    }
}

fn parse_setup_isolate_manifest(manifest_path: impl AsRef<Path>) -> Result<SetupIsolateManifest> {
    let path = manifest_path.as_ref();
    let json_string = read_to_string(path).with_context(|| {
        format!("couldn't open setup manifest Json file at: {}", path.display())
    })?;
    let manifest: SetupIsolateManifest = parse_proto_message(
        get_v2_descriptor_pool(),
        "enforcer.v2.SetupIsolateManifest",
        &json_string,
    )?;
    ensure!(
        manifest.setup_isolate_descriptor.is_some(),
        "Setup isolate manifest must contain a setup_isolate_descriptor"
    );
    Ok(manifest)
}

fn parse_ratified_isolate_manifest(
    manifest_path: impl AsRef<Path>,
) -> Result<RatifiedIsolateManifest> {
    let path = manifest_path.as_ref();
    let json_string = read_to_string(path).with_context(|| {
        format!("couldn't open ratified manifest Json file at: {}", path.display())
    })?;
    parse_proto_message(
        get_v2_descriptor_pool(),
        "enforcer.v2.RatifiedIsolateManifest",
        &json_string,
    )
}

fn parse_opaque_isolate_manifest(manifest_path: impl AsRef<Path>) -> Result<OpaqueIsolateManifest> {
    let path = manifest_path.as_ref();
    let json_string = read_to_string(path).with_context(|| {
        format!("couldn't open opaque manifest Json file at: {}", path.display())
    })?;
    parse_proto_message(get_v2_descriptor_pool(), "enforcer.v2.OpaqueIsolateManifest", &json_string)
}
