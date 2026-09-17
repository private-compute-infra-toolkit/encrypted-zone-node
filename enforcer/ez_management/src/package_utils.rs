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
use ez_management_proto::enforcer::v2::IsolatePackageChunk;
use sha2::{Digest, Sha256};

/// Calculates the SHA256 hash of a byte slice and returns it as a lowercase hex string.
pub fn calculate_sha256(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

pub const ENV_MAX_PACKAGE_SIZE_BYTES: &str = "EZ_MAX_PACKAGE_SIZE_BYTES";
pub const DEFAULT_MAX_PACKAGE_SIZE_BYTES: usize = 512 * 1024 * 1024; // 512 MB

/// An assembled isolate package containing all accumulated chunks and metadata.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AssembledPackage {
    pub package_name: String,
    pub package_bytes: Vec<u8>,
    pub endorsements: Vec<u8>,
}

/// Accumulator for streaming chunks of an isolate package.
#[derive(Debug, Clone)]
pub struct PackageAccumulator {
    pub current_package_name: String,
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
            current_package_name: String::new(),
            buffer: Vec::new(),
            expected_sequence: 0,
            endorsements: Vec::new(),
            max_package_size_bytes: get_max_package_size_bytes_from_env(),
        }
    }

    /// Resets all internal state for package accumulation.
    pub fn reset(&mut self) {
        self.current_package_name.clear();
        self.buffer.clear();
        self.expected_sequence = 0;
        self.endorsements.clear();
    }

    /// Pushes a package chunk into the accumulator.
    ///
    /// Returns `Ok(Some(AssembledPackage))` only when the last chunk has been received.
    /// Automatically clears all internal state upon completion or on error.
    pub fn push_chunk(
        &mut self,
        mut chunk: IsolatePackageChunk,
    ) -> Result<Option<AssembledPackage>, EzManagementError> {
        if self.current_package_name.is_empty() {
            if chunk.package_name.is_empty() {
                self.reset();
                return Err(EzManagementError::UnexpectedMessage(
                    "Received IsolatePackageChunk without package_name in current or prior chunks"
                        .to_string(),
                ));
            }
            self.current_package_name = std::mem::take(&mut chunk.package_name);
        } else if !chunk.package_name.is_empty() && chunk.package_name != self.current_package_name
        {
            let err = format!(
                "Received chunk for package '{}' while still streaming package '{}'",
                chunk.package_name, self.current_package_name
            );
            self.reset();
            return Err(EzManagementError::UnexpectedMessage(err));
        }

        if chunk.chunk_sequence != self.expected_sequence {
            let err = format!(
                "Invalid chunk sequence for package '{}': expected {}, got {}",
                self.current_package_name, self.expected_sequence, chunk.chunk_sequence
            );
            self.reset();
            return Err(EzManagementError::UnexpectedMessage(err));
        }

        self.expected_sequence = match self.expected_sequence.checked_add(1) {
            Some(seq) => seq,
            None => {
                let err = format!(
                    "Chunk sequence overflowed for package '{}'",
                    self.current_package_name
                );
                self.reset();
                return Err(EzManagementError::UnexpectedMessage(err));
            }
        };

        self.append_chunk_data(&mut chunk.package_tar_chunk)?;
        self.handle_endorsements(chunk.chunk_sequence, &mut chunk.isolate_package_endorsements)?;

        if chunk.is_last_chunk {
            let package = AssembledPackage {
                package_name: std::mem::take(&mut self.current_package_name),
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
                    "Received isolate_package_endorsements on chunk sequence {chunk_sequence} for package '{}', expected only on initial chunk",
                    self.current_package_name
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
                    "Package size calculation overflowed for package '{}'",
                    self.current_package_name
                );
                self.reset();
                return Err(EzManagementError::UnexpectedMessage(err));
            }
        };

        if new_size > self.max_package_size_bytes {
            let err = format!(
                "Package '{}' exceeds maximum allowed size of {} bytes (accumulated {} bytes)",
                self.current_package_name, self.max_package_size_bytes, new_size
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
