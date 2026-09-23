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

use anyhow::{Context, Result};
use common_proto::enforcer::v2::IsolateType;
use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, EzPayloadIsolateScope, InvokeIsolateRequest, IsolateDataScope,
};
use isolate_info::{get_binary_services_index, BinaryServicesIndex};
use junction_trait::Junction;
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use prost::Message;
use setup_isolate_proto::enforcer::v2::{
    FetchTlsCertificateRequest, FetchTlsCertificateResponse, ValidateIsolateEndorsementRequest,
    ValidateIsolateEndorsementResponse,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SetupIsolateClient {
    junction: Arc<Box<dyn Junction>>,
    publisher_id: String,
    isolate_name: String,
    service_name: String,
    binary_services_index: Option<BinaryServicesIndex>,
}

impl SetupIsolateClient {
    /// Creates a new [`SetupIsolateClient`].
    ///
    /// The setup isolate's [`BinaryServicesIndex`] is resolved here, so the client must be
    /// created only after the setup isolate's services have been registered.
    pub fn new(
        junction: Box<dyn Junction>,
        publisher_id: String,
        isolate_name: String,
        service_name: String,
    ) -> Self {
        let binary_services_index = get_binary_services_index(&IsolateType {
            publisher_id: publisher_id.clone(),
            isolate_name: isolate_name.clone(),
        });
        Self {
            junction: Arc::new(junction),
            publisher_id,
            isolate_name,
            service_name,
            binary_services_index,
        }
    }

    /// Returns the [`BinaryServicesIndex`] of the setup isolate.
    pub fn binary_services_index(&self) -> Option<BinaryServicesIndex> {
        self.binary_services_index
    }

    /// Invokes the ValidateIsolateEndorsement RPC on the setup isolate.
    pub async fn validate_isolate_endorsement(
        &self,
        request: ValidateIsolateEndorsementRequest,
    ) -> Result<ValidateIsolateEndorsementResponse> {
        self.invoke_rpc("ValidateIsolateEndorsement", request).await
    }

    /// Invokes the FetchMtlsCertificate RPC on the setup isolate.
    pub async fn fetch_mtls_certificate(
        &self,
        request: FetchTlsCertificateRequest,
    ) -> Result<FetchTlsCertificateResponse> {
        self.invoke_rpc("FetchMtlsCertificate", request).await
    }

    /// Generic helper to pack the request and invoke the isolate
    async fn invoke_rpc<Req: Message, Res: Message + Default>(
        &self,
        method_name: &str,
        request: Req,
    ) -> Result<Res> {
        let payload = request.encode_to_vec();
        let invoke_req = InvokeIsolateRequest {
            control_plane_metadata: Some(ControlPlaneMetadata {
                ipc_message_id: rand::random::<u64>(),
                requester_spiffe: String::new(),
                requester_is_local: true,
                responder_is_local: true,
                // TODO: b/562279963 - Until we support empty domain for routing.
                destination_operator_domain: self.publisher_id.clone(),
                destination_publisher_id: self.publisher_id.clone(),
                destination_isolate_name: self.isolate_name.clone(),
                destination_service_name: self.service_name.clone(),
                destination_method_name: method_name.to_string(),
                ..Default::default()
            }),
            isolate_input_iscope: Some(EzPayloadIsolateScope {
                datagram_iscopes: vec![IsolateDataScope {
                    scope_type: DataScopeType::Public.into(),
                    ..Default::default()
                }],
            }),
            isolate_input: Some(EzHybridPayload {
                delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                    datagrams: vec![payload],
                })),
            }),
        };

        let response = self
            .junction
            .invoke_isolate(None, invoke_req, false, None)
            .await
            .map_err(|e| anyhow::anyhow!("Junction error: {:?}", e))?;
        let output = response.isolate_output.context("Missing isolate output")?;
        let inline_data = match output.delivery_method {
            Some(DeliveryMethod::InlineData(data)) => data,
            _ => return Err(anyhow::anyhow!("Expected inline data in isolate response")),
        };
        let response_bytes =
            inline_data.datagrams.into_iter().next().context("Missing output datagram")?;
        Ok(Res::decode(&*response_bytes)?)
    }
}
