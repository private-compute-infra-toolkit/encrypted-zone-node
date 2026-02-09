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

use dyn_clone::DynClone;
use enforcer_proto::enforcer::v1::{InvokeIsolateRequest, InvokeIsolateResponse};
use ez_error::EzError;
use isolate_info::IsolateId;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub struct JunctionChannels {
    pub client_to_junction: Sender<InvokeIsolateRequest>,
    pub junction_to_client: Receiver<Result<InvokeIsolateResponse, EzError>>,
}

/// The common connection point from Enforcer to all Isolates. This provides an abstraction over the EzIsolateBridge IPC layer.
/// IsolateId should be provided if requests originate from an Isolate
#[tonic::async_trait]
pub trait Junction: Send + Sync + DynClone {
    /// Sends InvokeIsolateRequest to be routed to valid Isolate. Returns [InvokeIsolateResponse] with the
    /// response from the Isolate.
    async fn invoke_isolate(
        &self,
        client_isolate_id_option: Option<IsolateId>,
        invoke_isolate_request: InvokeIsolateRequest,
        is_from_public_api: bool,
    ) -> Result<InvokeIsolateResponse, EzError>;

    /// Start streaming connection with IsolateJunction. Returns [JunctionChannels] which holds the
    /// channels for the client & IsolateJunction communication.
    /// All requests will be serviced by the same Isolate for this connection
    async fn stream_invoke_isolate(
        &self,
        client_isolate_id_option: Option<IsolateId>,
        is_from_public_api: bool,
    ) -> JunctionChannels;

    /// Establish connection with Isolate server. Typically used when starting a new
    /// Isolate instance, as IPC connections are held for the Isolate lifetime.
    async fn connect_isolate(
        &self,
        isolate_id: IsolateId,
        isolate_address: String,
    ) -> anyhow::Result<()>;
}

dyn_clone::clone_trait_object!(Junction);
impl core::fmt::Debug for Box<dyn Junction> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Box<dyn Junction>")
    }
}
