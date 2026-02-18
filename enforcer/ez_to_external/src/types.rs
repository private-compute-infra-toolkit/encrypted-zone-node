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

use enforcer_proto::enforcer::v1::{InvokeEzRequest, InvokeEzResponse};
use isolate_info::IsolateId;
use tokio::sync::mpsc::Receiver;
use tonic::async_trait;

#[derive(thiserror::Error, Debug)]
pub enum ExternalProxyConnectorError {
    #[error("Failed to connect to the external proxy: {0}")]
    ConnectionFailed(String),
    #[error("Proxy communication stream failed: {0}")]
    StreamFailed(Box<tonic::Status>),
    #[error("Translation error: {0}")]
    TranslationError(String),
}

impl From<tonic::Status> for ExternalProxyConnectorError {
    fn from(status: tonic::Status) -> Self {
        ExternalProxyConnectorError::StreamFailed(Box::new(status))
    }
}

// Trait defining the contract for an external proxy channel.
// This allows for mocking in tests.
#[async_trait]
pub trait ExternalProxyChannel: std::fmt::Debug + Send + Sync {
    // Handles a unary request to the external proxy.
    async fn proxy_external(
        &self,
        isolate_id: IsolateId,
        request: InvokeEzRequest,
        timeout: Option<std::time::Duration>,
    ) -> Result<InvokeEzResponse, ExternalProxyConnectorError>;

    // Handles streaming requests to the external proxy.
    async fn stream_proxy_external(
        &self,
        isolate_id: IsolateId,
        mut from_bridge_rx: Receiver<InvokeEzRequest>,
    ) -> Result<Receiver<InvokeEzResponse>, ExternalProxyConnectorError>;

    /// Creates a clone of the channel which is returned as a trait object.
    fn clone_channel(&self) -> Box<dyn ExternalProxyChannel>;
}

impl Clone for Box<dyn ExternalProxyChannel> {
    fn clone(&self) -> Self {
        self.clone_channel()
    }
}
