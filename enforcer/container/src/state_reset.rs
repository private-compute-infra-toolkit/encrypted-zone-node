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

use anyhow::Result;
use std::fmt::Debug;
use std::path::PathBuf;

/// Represents an active state reset session for a specific running container process.
pub trait StateResetSession: Send + Sync {
    /// Captures a checkpoint of the process state (memory, registers, file descriptors).
    fn checkpoint(&mut self) -> Result<()>;

    /// Resets the process state back to the captured checkpoint.
    /// Returns the number of memory pages/regions restored, if available.
    fn reset(&mut self) -> Result<usize>;
}

/// Factory for creating state reset sessions on container processes.
pub trait StateResetEngine: Send + Sync + Debug {
    /// Initializes a state reset session for the given process PID and root/checkpoint directories.
    fn create_session(
        &self,
        pid: i32,
        checkpoint_dir: PathBuf,
        sandbox_root: Option<PathBuf>,
    ) -> Result<Box<dyn StateResetSession>>;
}
