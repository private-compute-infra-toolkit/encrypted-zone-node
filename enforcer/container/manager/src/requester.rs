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

use anyhow::Result;
use container_manager_request::{
    ContainerManagerRequest, GetRunStatusRequest, GetRunStatusResponse, MountFileResponse,
    MountReadOnlyFile, MountWritableFile, ResetIsolateRequest, ResetIsolateResponse,
};
use tokio::sync::{mpsc, oneshot};

/// Requester to send requests to ContainerManager. Consumers are encouraged to clone the requester.
// TODO See if we can merge this with DataScopeRequester by using generics
#[derive(Debug, Clone)]
pub struct ContainerManagerRequester {
    pub request_sender: mpsc::Sender<ContainerManagerRequest>,
}

impl ContainerManagerRequester {
    pub fn new(request_sender: mpsc::Sender<ContainerManagerRequest>) -> Self {
        Self { request_sender }
    }

    /// Send request to ContainerManager to reset the Container.
    pub async fn reset_container(
        &self,
        reset_isolate_req: ResetIsolateRequest,
    ) -> Result<ResetIsolateResponse> {
        let (response_tx, response_rx) = oneshot::channel();
        self.send_request(ContainerManagerRequest::ResetIsolateRequest {
            req: reset_isolate_req,
            resp: response_tx,
        })
        .await;
        response_rx.await?
    }

    /// Send request to ContainerManager to mount a writable file b/w Enforcer and the Container.
    pub async fn mount_writable_file(
        &self,
        mount_writable_file_req: MountWritableFile,
    ) -> Result<MountFileResponse> {
        let (response_tx, response_rx) = oneshot::channel();
        self.send_request(ContainerManagerRequest::MountWritableFile {
            req: mount_writable_file_req,
            resp: response_tx,
        })
        .await;
        response_rx.await?
    }

    /// Send request to ContainerManager to mount a read-only file b/w Enforcer and the Container.
    pub async fn mount_read_only_file(
        &self,
        mount_read_only_file_req: MountReadOnlyFile,
    ) -> Result<MountFileResponse> {
        let (response_tx, response_rx) = oneshot::channel();
        self.send_request(ContainerManagerRequest::MountReadOnlyFile {
            req: mount_read_only_file_req,
            resp: response_tx,
        })
        .await;
        response_rx.await?
    }

    /// Send request to ContainerManager to get the run status of the Container.
    pub async fn get_run_status(
        &self,
        get_run_status_req: GetRunStatusRequest,
    ) -> Result<GetRunStatusResponse> {
        let (response_tx, response_rx) = oneshot::channel();
        self.send_request(ContainerManagerRequest::GetRunStatus {
            req: get_run_status_req,
            resp: response_tx,
        })
        .await;
        response_rx.await?
    }

    async fn send_request(&self, container_manager_request: ContainerManagerRequest) {
        let send_result = self.request_sender.send(container_manager_request).await;
        if send_result.is_err() {
            // Ideally, should never happen.
            log::error!(
                "Couldn't send requests to ContainerManager {:?}",
                send_result.unwrap_err()
            );
        }
    }
}
