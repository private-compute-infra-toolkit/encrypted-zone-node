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
use container_manager_request::{MountReadOnlyFile, MountWritableFile};
use container_manager_requester::ContainerManagerRequester;
use dashmap::DashMap;
use enforcer_proto::enforcer::v1::{CreateMemshareRequest, CreateMemshareResponse, IsolateStatus};
use isolate_info::IsolateId;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;

// We internally represent all FileHandles as u64. But we send them as Strings to Isolates.
type FileHandle = u64;

/// Central place to maintain state for all the SharedMemory b/w Isolates. It calls
/// ContainerManager to perform the file mounting for shared memory.
#[derive(Clone, Debug)]
pub struct SharedMemManager {
    // The same file in Enforcer is mounted in Container with a different file name
    // Map: {IsolateId, {Isolate recognized FileHandle, Enforcer recognized FileHandle}}
    isolate_enforcer_file_handle_index: Arc<DashMap<IsolateId, HashMap<FileHandle, FileHandle>>>,
    // The Isolate which creates the shared memory is the owner of the file
    // Map: {Isolate recognized FileHandle, IsolateId of owner of the File}
    file_owner_index: Arc<DashMap<FileHandle, IsolateId>>,
    container_mngr_requester: ContainerManagerRequester,
}

#[derive(Copy, Clone, Debug, Error)]
pub enum SharedMemManagerError {
    #[error("Only the Isolate owning the file can share the file")]
    ReadOnlyFileCannotBeShared,
    #[error("The owner of the file can't share the file with itself")]
    DestinationSameAsOwner,
}

impl SharedMemManager {
    pub fn new(container_mngr_requester: ContainerManagerRequester) -> Self {
        Self {
            isolate_enforcer_file_handle_index: Arc::new(DashMap::new()),
            file_owner_index: Arc::new(DashMap::new()),
            container_mngr_requester,
        }
    }

    /// Creates a file in /dev/shm and mounts it as writable file into the Isolate Container.
    pub async fn create_shared_mem_file(
        &self,
        isolate_id: IsolateId,
        create_shared_mem_request: CreateMemshareRequest,
    ) -> CreateMemshareResponse {
        let enforcer_file_handle: FileHandle = rand::random();
        let container_file_handle: FileHandle = rand::random();
        let mount_write_result = self
            .container_mngr_requester
            .mount_writable_file(MountWritableFile {
                isolate_id,
                region_size: create_shared_mem_request.region_size,
                enforcer_file_name: enforcer_file_handle.to_string(),
                container_file_name: container_file_handle.to_string(),
            })
            .await;
        match mount_write_result {
            Ok(_) => {
                self.isolate_enforcer_file_handle_index
                    .entry(isolate_id)
                    .or_default()
                    .insert(container_file_handle, enforcer_file_handle);
                self.file_owner_index.insert(container_file_handle, isolate_id);

                CreateMemshareResponse {
                    shared_memory_handle: container_file_handle.to_string(),
                    status: None,
                }
            }
            Err(e) => CreateMemshareResponse {
                shared_memory_handle: "".to_string(),
                status: Some(IsolateStatus {
                    code: tonic::Code::Internal.into(),
                    message: e.to_string(),
                }),
            },
        }
    }

    /// Shares the file that is writable in source Isolate as a read-only file into
    /// destination Isolate.
    pub async fn share_file(
        &self,
        source_isolate_id: IsolateId,
        destination_isolate_id: IsolateId,
        file_handle: String,
    ) -> Result<()> {
        let file_handle_u64 =
            file_handle.parse::<u64>().context("Provided file handle is invalid")?;

        let owner_of_file_isolate_id = *self
            .file_owner_index
            .get(&file_handle_u64)
            .context("Unrecognized FileHandle")?
            .value();

        if owner_of_file_isolate_id != source_isolate_id {
            return Err(SharedMemManagerError::ReadOnlyFileCannotBeShared.into());
        }
        if owner_of_file_isolate_id == destination_isolate_id {
            return Err(SharedMemManagerError::DestinationSameAsOwner.into());
        }

        let isolate_file_index_ref = self
            .isolate_enforcer_file_handle_index
            .get(&source_isolate_id)
            .context("Unrecognized Isolate")?;
        let enforcer_file_handle = isolate_file_index_ref
            .value()
            .get(&file_handle_u64)
            .cloned()
            .context("Unrecognized FileHandle for Isolate")?;
        drop(isolate_file_index_ref); // drop ref to minimize contention for DashMap

        self.container_mngr_requester
            .mount_read_only_file(MountReadOnlyFile {
                isolate_id: destination_isolate_id,
                enforcer_file_name: enforcer_file_handle.to_string(),
                container_file_name: file_handle,
            })
            .await?;
        Ok(())
    }

    /// Removes all the state associated with the Isolate.
    pub async fn remove_isolate(&self, isolate_id: IsolateId) -> Result<()> {
        let (_isolate_id, isolate_file_index) = self
            .isolate_enforcer_file_handle_index
            .remove(&isolate_id)
            .context("Unrecognized Isolate")?;
        for (container_file_handle, _) in isolate_file_index {
            self.file_owner_index.remove(&container_file_handle);
        }
        Ok(())
    }
}
