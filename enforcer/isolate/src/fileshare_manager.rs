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
use container_manager_request::{MountReadOnlyDirectory, MountWritableDirectory};
use container_manager_requester::ContainerManagerRequester;
use dashmap::DashMap;
use isolate_info::IsolateId;
use std::sync::Arc;
use thiserror::Error;

use enforcer_proto::enforcer::v1 as enforcer_v1;
use enforcer_v1::StreamSubscribeToResponse;
use ez_error_trait::ToEzError;
use tokio::sync::mpsc::Sender;
use tonic::Status;

type FileshareHandle = u64;

/// Represents a fileshare between two Isolates.
#[derive(Clone, Debug)]
struct Fileshare {
    owner_id: IsolateId,
    receiver_id: Option<IsolateId>,
}

/// Manages fileshares between Isolates.
#[derive(Clone, Debug)]
pub struct FileshareManager {
    /// Map of fileshare handles to fileshares.
    fileshares: Arc<DashMap<FileshareHandle, Fileshare>>,
    /// Registered event streams for each receiver Isolate.
    registered_event_streams:
        Arc<DashMap<IsolateId, Sender<Result<StreamSubscribeToResponse, Status>>>>,
    /// Container manager requester.
    container_manager_requester: ContainerManagerRequester,
}

/// Errors returned by the FileshareManager.
#[derive(Clone, Debug, Error)]
pub enum FileshareManagerError {
    #[error("Only the Isolate owning the fileshare can notify of events")]
    UnauthorizedNotifyEvent,
    #[error("The owner of the fileshare can't share the file with itself")]
    DestinationSameAsOwner,
    #[error("Fileshare handle not found")]
    HandleNotFound,
    #[error("Invalid fileshare handle format")]
    InvalidHandle,
    #[error("Failed to mount fileshare directory: {0}")]
    MountFailed(String),
}

impl ToEzError for FileshareManagerError {
    fn to_ez_error(&self) -> ez_error::EzError {
        use error_detail_proto::enforcer::v1::ez_error_detail::ErrorSource;

        let code = match self {
            FileshareManagerError::UnauthorizedNotifyEvent => tonic::Code::PermissionDenied,
            FileshareManagerError::DestinationSameAsOwner => tonic::Code::InvalidArgument,
            FileshareManagerError::HandleNotFound => tonic::Code::NotFound,
            FileshareManagerError::InvalidHandle => tonic::Code::InvalidArgument,
            FileshareManagerError::MountFailed(_) => tonic::Code::Internal,
        };

        let enforcer_error = ez_error::EnforcerError {
            message: self.to_string(),
            error_code: code,
            source: ErrorSource::Enforcer,
            error_reason: None,
            bad_request: None, // TODO: Add bad request details for InvalidArgument errors.
        };
        ez_error::EzError::EnforcerError(enforcer_error)
    }
}

impl FileshareManager {
    /// Creates a new [FileshareManager].
    pub fn new(container_manager_requester: ContainerManagerRequester) -> Self {
        Self {
            fileshares: Arc::new(DashMap::new()),
            registered_event_streams: Arc::new(DashMap::new()),
            container_manager_requester,
        }
    }

    /// Creates a new fileshare for the given [IsolateId].
    pub async fn create_fileshare(
        &self,
        sender_isolate_id: IsolateId,
    ) -> Result<String, FileshareManagerError> {
        let fileshare_handle: FileshareHandle = rand::random();
        self.container_manager_requester
            .mount_writable_directory(MountWritableDirectory {
                isolate_id: sender_isolate_id,
                enforcer_dir_name: fileshare_handle.to_string(),
                container_dir_name: fileshare_handle.to_string(),
            })
            .await
            .map_err(|e| FileshareManagerError::MountFailed(e.to_string()))?;

        self.fileshares
            .insert(fileshare_handle, Fileshare { owner_id: sender_isolate_id, receiver_id: None });
        Ok(fileshare_handle.to_string())
    }

    /// Shares a file with the given [IsolateId].
    pub async fn share_file(
        &self,
        fileshare_handle: &str,
        source_isolate_id: IsolateId,
        destination_isolate_id: IsolateId,
    ) -> Result<(), FileshareManagerError> {
        if source_isolate_id == destination_isolate_id {
            return Err(FileshareManagerError::DestinationSameAsOwner);
        }
        let handle_u64 =
            fileshare_handle.parse::<u64>().map_err(|_| FileshareManagerError::InvalidHandle)?;
        let mut fileshare =
            self.fileshares.get_mut(&handle_u64).ok_or(FileshareManagerError::HandleNotFound)?;
        if fileshare.owner_id != source_isolate_id {
            return Err(FileshareManagerError::UnauthorizedNotifyEvent);
        }
        self.container_manager_requester
            .mount_read_only_directory(MountReadOnlyDirectory {
                isolate_id: destination_isolate_id,
                enforcer_dir_name: handle_u64.to_string(),
                container_dir_name: handle_u64.to_string(),
            })
            .await
            .map_err(|e| FileshareManagerError::MountFailed(e.to_string()))?;
        fileshare.receiver_id = Some(destination_isolate_id);
        Ok(())
    }

    /// Registers an event stream for the given [IsolateId].
    pub fn register_event_stream(
        &self,
        isolate_id: IsolateId,
        sender: Sender<Result<StreamSubscribeToResponse, Status>>,
    ) {
        self.registered_event_streams.insert(isolate_id, sender);
    }

    /// Notifies registered receiver of an event.
    pub async fn notify_event(
        &self,
        fileshare_handle: &str,
        event_type: enforcer_v1::fileshare_event::FileshareEventType,
        sender_id: IsolateId,
    ) -> Result<(), FileshareManagerError> {
        let receiver_id_option = {
            let handle_u64 = fileshare_handle
                .parse::<u64>()
                .map_err(|_| FileshareManagerError::InvalidHandle)?;
            let fileshare =
                self.fileshares.get(&handle_u64).ok_or(FileshareManagerError::HandleNotFound)?;
            if fileshare.owner_id != sender_id {
                return Err(FileshareManagerError::UnauthorizedNotifyEvent);
            }
            fileshare.receiver_id
        };

        let receiver_id = match receiver_id_option {
            Some(id) => id,
            None => {
                log::info!("Fileshare handle {} has no receiver", fileshare_handle);
                return Ok(());
            }
        };
        if let Some(tx) = self.registered_event_streams.get(&receiver_id) {
            let response = StreamSubscribeToResponse {
                topic: enforcer_v1::EventTopic::Fileshare.into(),
                handle: fileshare_handle.to_string(),
                payload: Some(enforcer_v1::stream_subscribe_to_response::Payload::FileshareEvent(
                    enforcer_v1::FileshareEvent { event_type: event_type.into() },
                )),
            };
            if let Err(e) = tx.send(Ok(response)).await {
                log::warn!("Failed to send fileshare event to Isolate {:?}: {:?}", receiver_id, e);
            }
        }
        Ok(())
    }

    /// Removes an [IsolateId] from its associated fileshares.
    pub async fn remove_isolate(&self, isolate_id: IsolateId) -> Result<()> {
        self.registered_event_streams.remove(&isolate_id);
        // TODO: Since this traverses the entire map, we should consider its impact on state reset.
        // One possible mitigation is to keep a list of fileshare handles per isolate which will help in this clean up.
        self.fileshares.retain(|_fileshare_handle, fileshare| {
            // If the isolate being removed is the owner, discard the fileshare.
            if fileshare.owner_id == isolate_id {
                return false;
            }
            // If it's the receiver, clear the receiver_id but keep the fileshare (owned by someone else).
            if fileshare.receiver_id == Some(isolate_id) {
                fileshare.receiver_id = None;
            }
            true
        });
        Ok(())
    }
}
