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
use container::{Container, ContainerOptions, ContainerRoot, ContainerRunStatus, MountOptions};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

static FAKE_CONTAINER_TRACKER: Lazy<Arc<DashMap<u64, FakeContainerData>>> =
    Lazy::new(|| Arc::new(DashMap::new()));
// UDS name for Isolate -> EZ Bridge. This will exist under [SHARING_DIR_NAME] for Container.
const ISOLATE_EZ_BRIDGE_ENFORCER_UDS: &str = "/isolate-ez-bridge-uds";

#[derive(Clone, PartialEq, Debug)]
pub enum Status {
    New,
    Started,
    Stopped,
    Deleted,
}

#[derive(Clone, Debug)]
pub struct FakeContainerData {
    pub status: Status,
    pub binary_filename: Option<String>,
    pub mounts: HashMap<String, String>,
    pub readonly_mounts: HashMap<String, String>,
    pub isolate_ez_bridge_enforcer_side_uds_path: Option<String>,
    pub root_dir: PathBuf,
    pub command_line_arguments: Vec<String>,
    pub env: Vec<String>,
    // mounts happened during boot time.
    pub boot_mounts: Vec<MountOptions>,
}

#[derive(Debug)]
// Fake Container that just tracks the functions invoked
pub struct FakeContainer {
    /// An internal ID to track fake containers
    pub fake_container_id: u64,
}

impl FakeContainer {
    /// Returns a thread-safe handle to the global container tracker.
    pub fn get_tracker() -> Arc<DashMap<u64, FakeContainerData>> {
        FAKE_CONTAINER_TRACKER.clone()
    }

    /// Clears all containers from the tracker. Useful for test isolation.
    pub fn clear_tracker() {
        FAKE_CONTAINER_TRACKER.clear();
    }
}

#[tonic::async_trait]
impl Container for FakeContainer {
    fn new(root: ContainerRoot) -> Result<Self> {
        let root_dir = match root {
            ContainerRoot::TarImagePath(path) => PathBuf::from(path),
            ContainerRoot::ReadOnlyRoot(dir) => dir.path().to_owned(),
        };

        let container_data = FakeContainerData {
            status: Status::New,
            binary_filename: None,
            mounts: HashMap::new(),
            readonly_mounts: HashMap::new(),
            isolate_ez_bridge_enforcer_side_uds_path: None,
            root_dir,
            command_line_arguments: Vec::<String>::new(),
            env: Vec::<String>::new(),
            boot_mounts: Vec::new(),
        };

        let fake_container_id = rand::random();

        FAKE_CONTAINER_TRACKER.insert(fake_container_id, container_data);
        Ok(Self { fake_container_id })
    }

    async fn start(&mut self, options: &ContainerOptions) -> Result<()> {
        let mut container_data = FAKE_CONTAINER_TRACKER
            .get_mut(&self.fake_container_id)
            .context("Container should be present")?;
        container_data.status = Status::Started;
        container_data.binary_filename = Some(options.binary_filename.clone());
        container_data.isolate_ez_bridge_enforcer_side_uds_path =
            Some(get_isolate_ez_bridge_enforcer_side_uds_path(
                options.mounts[0].source.to_str().unwrap(),
            ));
        container_data.command_line_arguments = options.command_line_arguments.clone();
        container_data.env = options.env.clone();
        container_data.boot_mounts = options.mounts.clone();
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        let mut container_data = FAKE_CONTAINER_TRACKER
            .get_mut(&self.fake_container_id)
            .context("Container should be present")?;
        container_data.status = Status::Stopped;
        Ok(())
    }

    async fn delete(&mut self) -> Result<()> {
        let mut container_data = FAKE_CONTAINER_TRACKER
            .get_mut(&self.fake_container_id)
            .context("Container should be present")?;
        container_data.status = Status::Deleted;
        Ok(())
    }

    fn mount(&mut self, source: &str, destination: &str) -> Result<()> {
        let mut container_data = FAKE_CONTAINER_TRACKER
            .get_mut(&self.fake_container_id)
            .context("Container should be present")?;
        container_data.mounts.insert(source.to_string(), destination.to_string());
        Ok(())
    }

    fn mount_readonly(&mut self, source: &str, destination: &str) -> Result<()> {
        let mut container_data = FAKE_CONTAINER_TRACKER
            .get_mut(&self.fake_container_id)
            .context("Container should be present")?;
        container_data.readonly_mounts.insert(source.to_string(), destination.to_string());
        Ok(())
    }

    fn get_run_status(&self) -> anyhow::Result<ContainerRunStatus> {
        Ok(ContainerRunStatus::Running)
    }
}

fn get_isolate_ez_bridge_enforcer_side_uds_path(sharing_dir: &str) -> String {
    sharing_dir.to_owned() + ISOLATE_EZ_BRIDGE_ENFORCER_UDS
}
