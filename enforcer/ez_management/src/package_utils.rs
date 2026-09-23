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

use crate::types::EzManagementError;
use common_proto::enforcer::v2::IsolateType;
use ez_management_proto::enforcer::v2::IsolatePackageChunk;
use sha2::{Digest, Sha256};

/// Calculates the SHA256 hash of a byte slice and returns it as a lowercase hex string.
pub fn calculate_sha256(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

pub const ENV_MAX_PACKAGE_SIZE_BYTES: &str = "EZ_MAX_PACKAGE_SIZE_BYTES";
pub const DEFAULT_MAX_PACKAGE_SIZE_BYTES: usize = 512 * 1024 * 1024; // 512 MB

/// An assembled isolate package containing all accumulated chunks and metadata.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssembledPackage {
    pub isolate_type: IsolateType,
    pub package_bytes: Vec<u8>,
    pub endorsements: Vec<u8>,
}

/// Accumulator for streaming chunks of an isolate package.
#[derive(Debug, Clone)]
pub struct PackageAccumulator {
    pub current_isolate_type: Option<IsolateType>,
    pub buffer: Vec<u8>,
    pub expected_sequence: i32,
    pub endorsements: Vec<u8>,
    pub max_package_size_bytes: usize,
}

impl PackageAccumulator {
    /// Creates a new [`PackageAccumulator`] using the max package size configured
    /// from [`ENV_MAX_PACKAGE_SIZE_BYTES`] or [`DEFAULT_MAX_PACKAGE_SIZE_BYTES`].
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            current_isolate_type: None,
            buffer: Vec::new(),
            expected_sequence: 0,
            endorsements: Vec::new(),
            max_package_size_bytes: get_max_package_size_bytes_from_env(),
        }
    }

    /// Returns true while the chunks of a package are still being accumulated.
    pub(crate) fn is_package_in_progress(&self) -> bool {
        self.current_isolate_type.is_some()
    }

    /// Resets all internal state for package accumulation.
    pub fn reset(&mut self) {
        self.current_isolate_type = None;
        self.buffer.clear();
        self.expected_sequence = 0;
        self.endorsements.clear();
    }

    /// Pushes a package chunk into the accumulator.
    /// The isolate_type is expected only on the first chunk of a package; subsequent
    /// chunks may either repeat the same isolate_type or leave it unset.
    ///
    /// Returns `Ok(Some(AssembledPackage))` only when the last chunk has been received.
    /// Automatically clears all internal state upon completion or on error.
    pub fn push_chunk(
        &mut self,
        mut chunk: IsolatePackageChunk,
    ) -> Result<Option<AssembledPackage>, EzManagementError> {
        if let Some(incoming) = chunk.isolate_type.take() {
            let current = self.current_isolate_type.get_or_insert_with(|| incoming.clone());
            if *current != incoming {
                let err = format!(
                    "Received chunk for Isolate '{incoming:?}' while still streaming Isolate '{current:?}'"
                );
                self.reset();
                return Err(EzManagementError::UnexpectedMessage(err));
            }
        } else if self.current_isolate_type.is_none() {
            self.reset();
            return Err(EzManagementError::UnexpectedMessage(
                "Received IsolatePackageChunk without isolate_type in current or prior chunks"
                    .to_string(),
            ));
        }

        if chunk.chunk_sequence != self.expected_sequence {
            let err = format!(
                "Invalid chunk sequence for Isolate '{}': expected {}, got {}",
                self.describe_current_isolate_type(),
                self.expected_sequence,
                chunk.chunk_sequence
            );
            self.reset();
            return Err(EzManagementError::UnexpectedMessage(err));
        }

        self.expected_sequence = match self.expected_sequence.checked_add(1) {
            Some(seq) => seq,
            None => {
                let err = format!(
                    "Chunk sequence overflowed for Isolate '{}'",
                    self.describe_current_isolate_type()
                );
                self.reset();
                return Err(EzManagementError::UnexpectedMessage(err));
            }
        };

        self.append_chunk_data(&mut chunk.package_tar_chunk)?;
        self.handle_endorsements(chunk.chunk_sequence, &mut chunk.isolate_package_endorsements)?;

        if chunk.is_last_chunk {
            let package = AssembledPackage {
                isolate_type: self.current_isolate_type.take().unwrap_or_default(),
                package_bytes: std::mem::take(&mut self.buffer),
                endorsements: std::mem::take(&mut self.endorsements),
            };
            self.reset();
            Ok(Some(package))
        } else {
            Ok(None)
        }
    }

    fn handle_endorsements(
        &mut self,
        chunk_sequence: i32,
        endorsements: &mut Vec<u8>,
    ) -> Result<(), EzManagementError> {
        if !endorsements.is_empty() {
            if chunk_sequence != 0 {
                let err = format!(
                    "Received isolate_package_endorsements on chunk sequence {chunk_sequence} for Isolate '{}', expected only on initial chunk",
                    self.describe_current_isolate_type()
                );
                self.reset();
                return Err(EzManagementError::UnexpectedMessage(err));
            }
            self.endorsements = std::mem::take(endorsements);
        }
        Ok(())
    }

    fn append_chunk_data(&mut self, chunk_data: &mut Vec<u8>) -> Result<(), EzManagementError> {
        let new_size = match self.buffer.len().checked_add(chunk_data.len()) {
            Some(sz) => sz,
            None => {
                let err = format!(
                    "Package size calculation overflowed for Isolate '{}'",
                    self.describe_current_isolate_type()
                );
                self.reset();
                return Err(EzManagementError::UnexpectedMessage(err));
            }
        };

        if new_size > self.max_package_size_bytes {
            let err = format!(
                "Package for Isolate '{}' exceeds maximum allowed size of {} bytes (accumulated {} bytes)",
                self.describe_current_isolate_type(),
                self.max_package_size_bytes,
                new_size
            );
            self.reset();
            return Err(EzManagementError::UnexpectedMessage(err));
        }

        if self.buffer.is_empty() {
            self.buffer = std::mem::take(chunk_data);
        } else {
            self.buffer.append(chunk_data);
        }

        Ok(())
    }

    fn describe_current_isolate_type(&self) -> String {
        self.current_isolate_type.as_ref().map(|t| format!("{t:?}")).unwrap_or_default()
    }
}

fn get_max_package_size_bytes_from_env() -> usize {
    std::env::var(ENV_MAX_PACKAGE_SIZE_BYTES)
        .or_else(|_| std::env::var("MAX_PACKAGE_SIZE_BYTES"))
        .ok()
        .and_then(|val| match val.parse::<usize>() {
            Ok(size) => Some(size),
            Err(e) => {
                log::warn!(
                    "Invalid {ENV_MAX_PACKAGE_SIZE_BYTES} value '{val}': {e}. Using default {DEFAULT_MAX_PACKAGE_SIZE_BYTES} bytes"
                );
                None
            }
        })
        .unwrap_or(DEFAULT_MAX_PACKAGE_SIZE_BYTES)
}
