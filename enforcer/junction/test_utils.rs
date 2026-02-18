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
use dashmap::DashMap;
use data_scope::error::DataScopeError;
use derivative::Derivative;
use enforcer_proto::enforcer::v1::{InvokeIsolateRequest, InvokeIsolateResponse};
use error_detail_proto::enforcer::v1::ez_error_detail::ErrorSource;

use isolate_info::IsolateId;
use isolate_test_utils::{DefaultEchoIsolate, FakeIsolate, ScopeDragInstruction};
use junction_trait::{Junction, JunctionChannels};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc::channel;

pub const JUNCTION_TEST_CHANNEL_SIZE: usize = 128;
pub const UNKNOWN_ISOLATE_DOMAIN: &str = "unknown_isolate";

// FakeJunction, by default uses DefaultEchoIsolate as the FakeIsolate
#[derive(Debug, Clone, Derivative)]
#[derivative(Default)]
pub struct FakeJunction {
    pub last_timeout: Arc<Mutex<Option<std::time::Duration>>>,
    #[derivative(Default(
        value = "Box::new(DefaultEchoIsolate::new(ScopeDragInstruction::KeepSame, None))"
    ))]
    pub fake_isolate: Box<dyn FakeIsolate>,
    pub connected_isolates: Arc<DashMap<IsolateId, String>>,
    pub call_count: Arc<AtomicUsize>,
    pub stream_call_count: Arc<AtomicUsize>,
    pub invoked_isolate_requests: Arc<Mutex<Vec<InvokeIsolateRequest>>>,
}

#[tonic::async_trait]
impl Junction for FakeJunction {
    async fn invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        invoke_isolate_request: InvokeIsolateRequest,
        _is_from_public_api: bool,
        timeout: Option<std::time::Duration>,
    ) -> Result<InvokeIsolateResponse, ez_error::EzError> {
        *self.last_timeout.lock().unwrap() = timeout;
        self.call_count.fetch_add(1, Ordering::SeqCst);
        self.invoked_isolate_requests.lock().unwrap().push(invoke_isolate_request.clone());
        if invoke_isolate_request
            .control_plane_metadata
            .as_ref()
            .is_some_and(|cpm| cpm.destination_operator_domain.contains(UNKNOWN_ISOLATE_DOMAIN))
        {
            return Err(ez_error::EzError::EnforcerError(ez_error::EnforcerError {
                message: DataScopeError::InvalidIsolateServiceIndex.to_string(),
                error_code: tonic::Code::InvalidArgument,
                source: ErrorSource::Enforcer,
                error_reason: None,
                bad_request: None,
            }));
        }
        Ok(self.fake_isolate.create_isolate_response(&invoke_isolate_request).await)
    }

    async fn connect_isolate(
        &self,
        isolate_id: IsolateId,
        isolate_address: String,
    ) -> anyhow::Result<()> {
        self.connected_isolates.insert(isolate_id, isolate_address);
        Ok(())
    }

    async fn stream_invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        _is_from_public_api: bool,
    ) -> JunctionChannels {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let (client_to_junction_tx, client_to_junction_rx) = channel(JUNCTION_TEST_CHANNEL_SIZE);
        let (junction_to_client_tx, junction_to_client_rx) = channel(JUNCTION_TEST_CHANNEL_SIZE);

        let junction_channel = JunctionChannels {
            client_to_junction: client_to_junction_tx,
            junction_to_client: junction_to_client_rx,
        };

        let fake_isolate = self.fake_isolate.clone();
        let stream_call_count_clone = self.stream_call_count.clone();
        let invoke_isolate_requests_clone = self.invoked_isolate_requests.clone();
        tokio::spawn(async move {
            fake_isolate
                .process_isolate_requests(
                    client_to_junction_rx,
                    junction_to_client_tx,
                    stream_call_count_clone,
                    invoke_isolate_requests_clone,
                )
                .await;
        });

        return junction_channel;
    }
}

impl FakeJunction {
    // Allows user to add their own desired behavior for the FakeIsolates running behind this FakeJunction.
    // Note: All Isolates connected to this FakeJunction will share the same behavior (default is to echo back requests)
    pub fn set_fake_isolate(&mut self, fake_isolate: Box<dyn FakeIsolate>) {
        self.fake_isolate = fake_isolate;
    }
}
