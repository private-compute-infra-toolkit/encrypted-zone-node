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

use container_manager_request::ContainerManagerRequest;
use container_manager_requester::ContainerManagerRequester;
use data_scope::manifest_validator::ManifestValidator;
use data_scope::request::AddManifestScopeRequest;
use data_scope::{request::AddIsolateRequest, requester::DataScopeRequester};
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::IsolateState;
use fileshare_manager::FileshareManager;
use isolate_info::IsolateId;
use isolate_info::IsolateServiceInfo;
use isolate_service_mapper::IsolateServiceMapper;
use isolate_test_utils::{
    start_fake_isolate_server, DEFAULT_ISOLATE_UNIX_SOCKET, ECHO_ISOLATE_OPERATOR_DOMAIN,
    ECHO_ISOLATE_SERVICE_NAME, ERROR_ISOLATE_SERVICE_NAME,
};
use isolate_test_utils::{
    DefaultEchoIsolate, FakeIsolate, ScopeDragInstruction, TEST_ERROR_CODE, TEST_ERROR_MESSAGE,
    TEST_ERROR_SERVICE_NAME,
};
use junction::IsolateJunction;
use junction_trait::{Junction, JunctionChannels};
use shared_memory_manager::SharedMemManager;
use state_manager::IsolateStateManager;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tonic::Status;

pub const JUNCTION_TEST_CHANNEL_SIZE: usize = 128;
pub const UNKNOWN_ISOLATE_DOMAIN: &str = "unknown_isolate";
pub const CLONE_ECHO_ISOLATE_SERVICE_NAME: &str = "clone_echo_isolate_service";
pub const QUALIFIED_ECHO_ISOLATE_SERVICE_NAME: &str = "qualified_echo_isolate_service";

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
        if invoke_isolate_request
            .control_plane_metadata
            .as_ref()
            .is_some_and(|cpm| cpm.destination_service_name.contains(TEST_ERROR_SERVICE_NAME))
        {
            return Err(ez_error::EzError::Status(Status::new(
                TEST_ERROR_CODE.into(),
                TEST_ERROR_MESSAGE,
            )));
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

pub struct TestHarness {
    pub isolate_junction: IsolateJunction,
    pub isolate_service_info_map: HashMap<String, IsolateServiceInfo>,
    pub isolate_server_shutdown_tx: oneshot::Sender<()>,
    pub container_manager_request_rx: Option<Receiver<ContainerManagerRequest>>,
}

impl TestHarness {
    // Create a real IsolateJunction connected to a single fake Isolate (DefaultEchoIsolate)
    pub async fn new_with_arguments(
        retirement_threshold: u64,
        scope_drag_instruction: ScopeDragInstruction,
        is_ratified: bool,
        manifest_input_scope: DataScopeType,
        delay: Option<Duration>,
    ) -> Self {
        // Using real implementations of all IsolateJunction deps (except Isolate)
        let isolate_service_mapper = IsolateServiceMapper::default();
        let (container_manager_request_tx, container_manager_request_rx) =
            tokio::sync::mpsc::channel(JUNCTION_TEST_CHANNEL_SIZE);
        let container_manager_requester =
            ContainerManagerRequester::new(container_manager_request_tx);
        let shared_memory_manager =
            SharedMemManager::new(container_manager_requester.clone(), 0, 0);
        let fileshare_manager = FileshareManager::new(container_manager_requester.clone());
        let data_scope_requester = DataScopeRequester::new(retirement_threshold);
        let manifest_validator = ManifestValidator::default();
        let isolate_state_manager = IsolateStateManager::new(
            data_scope_requester.clone(),
            container_manager_requester.clone(),
        );
        let isolate_junction = IsolateJunction::new(
            data_scope_requester.clone(),
            isolate_service_mapper.clone(),
            shared_memory_manager.clone(),
            fileshare_manager.clone(),
            isolate_state_manager.clone(),
            manifest_validator.clone(),
            100 * 1024 * 1024, // 100 MiB to force inline
        );

        // Start fake Isolate server
        let isolate_service_info = IsolateServiceInfo {
            operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
            service_name: ECHO_ISOLATE_SERVICE_NAME.to_string(),
            ..Default::default()
        };
        let clone_isolate_service_info = IsolateServiceInfo {
            operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
            service_name: CLONE_ECHO_ISOLATE_SERVICE_NAME.to_string(),
            ..Default::default()
        };
        let error_isolate_service_info = IsolateServiceInfo {
            operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
            service_name: ERROR_ISOLATE_SERVICE_NAME.to_string(),
            ..Default::default()
        };
        let qualified_isolate_service_info = IsolateServiceInfo {
            operator_domain: ECHO_ISOLATE_OPERATOR_DOMAIN.to_string(),
            service_name: QUALIFIED_ECHO_ISOLATE_SERVICE_NAME.to_string(),
            isolate_name: "echo_iso".to_string(),
            publisher_id: "echo_pub".to_string(),
        };

        // Add fake Isolate to IsolateServiceMapper and save the mapped IsolateServiceIndex
        // This is usually done by ContainerManager when we parse ez manifest
        let isolate_services_vec = vec![
            isolate_service_info.clone(),
            clone_isolate_service_info.clone(),
            error_isolate_service_info.clone(),
            qualified_isolate_service_info.clone(),
        ];
        let binary_services_index = isolate_service_mapper
            .new_binary_index(
                isolate_services_vec,
                is_ratified,
                "echo_pub".to_string(),
                "echo_iso".to_string(),
            )
            .await
            .expect("Should be a valid binary services index");

        manifest_validator
            .add_scope_info(AddManifestScopeRequest {
                binary_services_index,
                max_input_scope: manifest_input_scope,
                max_output_scope: DataScopeType::UserPrivate,
            })
            .await
            .expect("Should succeed to add input output scopes in manifest validator");

        let isolate_id = IsolateId::new(binary_services_index);
        let isolate_server_shutdown_tx =
            start_fake_isolate_server(isolate_id, scope_drag_instruction, delay).await;

        // Add fake Isolate to DataScopeRequester
        // This is usually done by ContainerManager calling IsolateStateManager
        let add_isolate_request = create_add_isolate_request(isolate_id);
        isolate_state_manager.add_isolate(add_isolate_request).await;
        // The Isolate must be ready before it can be used.
        isolate_state_manager.update_state(isolate_id, IsolateState::Ready).await.unwrap();

        // Connect fake Isolate to IsolateJunction
        assert!(isolate_junction
            .connect_isolate(isolate_id, format!("{}{}", DEFAULT_ISOLATE_UNIX_SOCKET, isolate_id),)
            .await
            .is_ok());

        let mut isolate_service_info_map = HashMap::new();
        isolate_service_info_map
            .insert(ECHO_ISOLATE_SERVICE_NAME.to_string(), isolate_service_info);
        isolate_service_info_map
            .insert(CLONE_ECHO_ISOLATE_SERVICE_NAME.to_string(), clone_isolate_service_info);
        isolate_service_info_map
            .insert(ERROR_ISOLATE_SERVICE_NAME.to_string(), error_isolate_service_info);
        isolate_service_info_map.insert(
            QUALIFIED_ECHO_ISOLATE_SERVICE_NAME.to_string(),
            qualified_isolate_service_info,
        );

        Self {
            isolate_junction,
            isolate_service_info_map,
            isolate_server_shutdown_tx,
            container_manager_request_rx: Some(container_manager_request_rx),
        }
    }

    pub async fn new() -> Self {
        TestHarness::new_with_arguments(
            u64::MAX,
            ScopeDragInstruction::KeepUnspecified,
            false,
            DataScopeType::UserPrivate,
            None,
        )
        .await
    }
}

pub fn create_add_isolate_request(isolate_id: IsolateId) -> AddIsolateRequest {
    AddIsolateRequest {
        current_data_scope_type: DataScopeType::Public,
        allowed_data_scope_type: DataScopeType::UserPrivate,
        isolate_id,
    }
}
