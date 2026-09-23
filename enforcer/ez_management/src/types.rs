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

use ez_management_proto::enforcer::v2::LoadIsolatesError;

#[derive(thiserror::Error, Debug)]
pub enum EzManagementError {
    #[error("Failed to connect to EzManagementService: {0}")]
    ConnectionFailed(String),
    #[error("gRPC stream error: {0}")]
    StreamError(String),
    #[error("Unexpected message received: {0}")]
    UnexpectedMessage(String),
    #[error("Failed to parse manifest: {0}")]
    ManifestParsingFailed(String),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Failed to load isolates ({0:?}): {1}")]
    LoadIsolatesFailed(LoadIsolatesError, String),
    #[error("Incomplete packages loaded: expected {expected}, received {received}")]
    IncompletePackagesLoaded { expected: usize, received: usize },
    #[error("Internal error: {0}")]
    InternalError(String),
}
