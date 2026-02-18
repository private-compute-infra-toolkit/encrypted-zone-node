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
use dyn_clone::DynClone;
use enforcer_proto::enforcer::v1::{InvokeEzRequest, InvokeEzResponse};
use tokio::sync::mpsc::Receiver;

/// The OutboundEzToEzClient trait defines the interface for sending requests to a remote EZ enforcer from an Isolate.
#[tonic::async_trait]
pub trait OutboundEzToEzClient: Send + Sync + DynClone {
    /// Sends a unary InvokeEzRequest to a remote enforcer and returns the response.
    async fn remote_invoke(
        &self,
        request: InvokeEzRequest,
        timeout: Option<std::time::Duration>,
    ) -> Result<InvokeEzResponse>;

    /// Establishes a streaming connection with a remote enforcer.
    async fn remote_streaming_connect(
        &self,
        from_local_rx: Receiver<InvokeEzRequest>,
    ) -> Result<Receiver<Result<InvokeEzResponse>>>;
}

dyn_clone::clone_trait_object!(OutboundEzToEzClient);
impl core::fmt::Debug for Box<dyn OutboundEzToEzClient> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Box<dyn OutboundEzToEzClient>")
    }
}
