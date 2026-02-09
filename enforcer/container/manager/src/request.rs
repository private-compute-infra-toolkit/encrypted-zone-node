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

use container::ContainerRunStatus;
use isolate_info::IsolateId;
use tokio::sync::oneshot;

/// A response callback type for the sender of a [ContainerManagerRequest]
pub type ContainerManagerResponseCallback<T> = oneshot::Sender<ContainerManagerResponse<T>>;

/// A response type for the receiver of a [ContainerManagerResponseCallback]
pub type ContainerManagerResponse<T> = anyhow::Result<T>;

pub enum ContainerManagerRequest {
    ResetIsolateRequest {
        req: ResetIsolateRequest,
        resp: ContainerManagerResponseCallback<ResetIsolateResponse>,
    },
    MountWritableFile {
        req: MountWritableFile,
        resp: ContainerManagerResponseCallback<MountFileResponse>,
    },
    MountReadOnlyFile {
        req: MountReadOnlyFile,
        resp: ContainerManagerResponseCallback<MountFileResponse>,
    },
    GetRunStatus {
        req: GetRunStatusRequest,
        resp: ContainerManagerResponseCallback<GetRunStatusResponse>,
    },
}

#[derive(Debug)]
pub struct ResetIsolateRequest {
    pub isolate_id: IsolateId,
}

#[derive(Debug)]
pub struct ResetIsolateResponse {}

#[derive(Debug)]
pub struct MountWritableFile {
    // Target Isolate
    pub isolate_id: IsolateId,
    // File size
    pub region_size: i64,
    // File name recognized by Enforcer
    pub enforcer_file_name: String,
    // File name recognized by Container
    pub container_file_name: String,
}

#[derive(Debug)]
pub struct MountReadOnlyFile {
    // Target Isolate
    pub isolate_id: IsolateId,
    // File name recognized by Enforcer
    pub enforcer_file_name: String,
    // File name recognized by Container
    pub container_file_name: String,
}

#[derive(Debug)]
pub struct MountFileResponse {}

#[derive(Debug)]
pub struct GetRunStatusRequest {
    pub isolate_id: IsolateId,
}

#[derive(Debug)]
pub struct GetRunStatusResponse {
    pub status: ContainerRunStatus,
}
