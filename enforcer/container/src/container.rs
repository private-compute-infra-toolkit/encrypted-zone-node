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

use derivative::Derivative;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

/// Represents the root of the container, which can be a tar image path or a read-only root directory.
#[derive(Debug)]
pub enum ContainerRoot {
    /// The path to a tar image file that contains the container root filesystem.
    TarImagePath(String),
    /// A read-only root directory that contains the container root filesystem.
    ReadOnlyRoot(Arc<TempDir>),
}

/// Represents the running status of the container process.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ContainerRunStatus {
    /// The container process is running.
    Running,
    /// The container process is not running and exit code not available.
    NotFound,
    /// The container process has exited with the given exit code.
    Exited(i32),
    /// The container process was killed by the given signal.
    Signaled(i32),
}

/// Options for configuring a mount in the container.
#[derive(Clone, Debug)]
pub struct MountOptions {
    /// The source path to mount from.
    pub source: PathBuf,
    /// The destination path to mount to inside the container.
    pub destination: PathBuf,
}

/// Options for configuring the network namespace of the container.
#[derive(Debug, Derivative)]
#[derivative(Default)]
pub struct NetworkOptions {
    /// If true, the container will be in the EZ network namespace and can access the host network.
    #[derivative(Default(value = "false"))]
    pub disable_network_namespace: bool,
    /// If true, the container will have a loopback interface brought up.
    /// This is useful for containers that need to communicate over localhost
    /// or asserts that the loopback interface is available.
    #[derivative(Default(value = "true"))]
    pub bring_up_loopback_interface: bool,
}

/// Options for configuring a container instance.
#[derive(Debug, Default)]
pub struct ContainerOptions {
    /// The name of the container.
    pub name: String,
    /// The path to the binary that will be executed inside the container.
    pub binary_filename: String,
    /// Command line arguments to pass to the binary when starting the container.
    pub command_line_arguments: Vec<String>,
    /// Mounts for the container.
    pub mounts: Vec<MountOptions>,
    /// Network options for the container.
    pub network: NetworkOptions,
    /// Environment variables to set in the container.
    pub env: Vec<String>,
}

#[tonic::async_trait]
pub trait Container: Send + Sync {
    /// Creates a new instance of Container.
    fn new(root: ContainerRoot) -> anyhow::Result<Self>
    where
        Self: Sized;

    /// Starts a container instance with the given options.
    /// This not only sets up the container environment, but also launches the binary.
    async fn start(&mut self, options: &ContainerOptions) -> anyhow::Result<()>;

    /// Stops the running container instance.
    async fn stop(&mut self) -> anyhow::Result<()>;

    /// Clean up the container instance.
    async fn delete(&mut self) -> anyhow::Result<()>;

    /// Mounts a source directory to a destination directory in the container.
    fn mount(&mut self, source: &str, destination: &str) -> anyhow::Result<()>;

    /// Mounts a source directory to a destination directory in the container as read-only.
    fn mount_readonly(&mut self, source: &str, destination: &str) -> anyhow::Result<()>;

    /// Checks if the container process is running. Fetches exit code / signal if available.
    fn get_run_status(&self) -> anyhow::Result<ContainerRunStatus>;
}
