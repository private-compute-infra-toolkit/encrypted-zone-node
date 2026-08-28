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

/// Represents the identity of an isolate for mTLS and routing configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IsolateIdentity {
    pub isolate_name: String,
    pub publisher_id: String,
}

impl IsolateIdentity {
    pub fn new(isolate_name: impl Into<String>, publisher_id: impl Into<String>) -> Self {
        Self { isolate_name: isolate_name.into(), publisher_id: publisher_id.into() }
    }
}

impl SetupManifest {
    /// Creates a new [`SetupManifest`] from an in-memory manifest proto.
    pub fn new(setup_isolate_manifest: SetupIsolateManifest) -> Self {
        Self { setup_isolate_manifest }
    }

    /// Loads and parses the setup isolate manifest from a JSON file path.
    pub fn load_from_path(setup_path: impl AsRef<Path>) -> Result<Self> {
        let setup_isolate_manifest = parse_setup_isolate_manifest(setup_path)?;
        Ok(Self::new(setup_isolate_manifest))
    }

    /// Converts the setup isolate manifest into a [`ParsedIsolate`].
    pub fn into_parsed_isolate(self) -> Result<ParsedIsolate> {
        convert_setup_descriptor_to_parsed_isolate(
            self.setup_isolate_manifest.setup_isolate_descriptor.unwrap_or_default(),
        )
    }

    /// Extracts [`IsolateIdentity`] for the setup isolate descriptor.
    pub fn extract_sni_params(&self) -> IsolateIdentity {
        self.setup_isolate_manifest
            .setup_isolate_descriptor
            .as_ref()
            .and_then(|d| d.isolate_type.as_ref())
            .map(|it| IsolateIdentity::new(it.isolate_name.clone(), it.publisher_id.clone()))
            .expect("setup_isolate_descriptor and isolate_type must be present in SetupManifest")
    }
}

impl WorkloadManifests {
    /// Creates a new [`WorkloadManifests`] container from in-memory manifest protos.
    pub fn new(
        ratified_isolate_manifest: Option<RatifiedIsolateManifest>,
        opaque_isolate_manifest: Option<OpaqueIsolateManifest>,
    ) -> Self {
        Self { ratified_isolate_manifest, opaque_isolate_manifest }
    }

    /// Flattens workload descriptors into a sequence of [`ParsedIsolate`]s.
    pub fn into_parsed_isolates(self) -> Result<Vec<ParsedIsolate>> {
        let mut isolates = Vec::new();
        if let Some(manifest) = self.ratified_isolate_manifest {
            for desc in manifest.ratified_isolate_descriptors {
                isolates.push(convert_ratified_descriptor_to_parsed_isolate(desc)?);
            }
        }
        if let Some(manifest) = self.opaque_isolate_manifest {
            for desc in manifest.opaque_isolate_descriptors {
                isolates.push(convert_opaque_descriptor_to_parsed_isolate(desc)?);
            }
        }
        Ok(isolates)
    }

    /// Extracts [`IsolateIdentity`] entries across workload descriptors for mTLS SNI configuration.
    pub fn extract_sni_params(&self) -> Vec<IsolateIdentity> {
        let ratified_iter = self
            .ratified_isolate_manifest
            .iter()
            .flat_map(|m| &m.ratified_isolate_descriptors)
            .filter_map(|d| d.isolate_type.as_ref())
            .map(|it| IsolateIdentity::new(it.isolate_name.clone(), it.publisher_id.clone()));

        let opaque_iter = self
            .opaque_isolate_manifest
            .iter()
            .flat_map(|m| &m.opaque_isolate_descriptors)
            .filter_map(|d| d.isolate_type.as_ref())
            .map(|it| IsolateIdentity::new(it.isolate_name.clone(), it.publisher_id.clone()));

        ratified_iter.chain(opaque_iter).collect()
    }
}

/// Parses a setup isolate manifest from a JSON file path.
pub fn parse_setup_isolate_manifest(
    manifest_path: impl AsRef<Path>,
) -> Result<SetupIsolateManifest> {
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

/// Parses a ratified isolate manifest from a JSON file path.
pub fn parse_ratified_isolate_manifest(
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

/// Parses an opaque isolate manifest from a JSON file path.
pub fn parse_opaque_isolate_manifest(
    manifest_path: impl AsRef<Path>,
) -> Result<OpaqueIsolateManifest> {
    let path = manifest_path.as_ref();
    let json_string = read_to_string(path).with_context(|| {
        format!("couldn't open opaque manifest Json file at: {}", path.display())
    })?;
    parse_proto_message(get_v2_descriptor_pool(), "enforcer.v2.OpaqueIsolateManifest", &json_string)
}
