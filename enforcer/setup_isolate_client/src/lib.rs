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
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, EzPayloadIsolateScope, InvokeIsolateRequest,
};
use junction_trait::Junction;
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use prost::Message;
use setup_isolate_proto::enforcer::v2::{
    ValidateIsolateEndorsementRequest, ValidateIsolateEndorsementResponse,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SetupIsolateClient {
    junction: Arc<Box<dyn Junction>>,
    publisher_id: String,
    isolate_name: String,
}

impl SetupIsolateClient {
    pub fn new(junction: Box<dyn Junction>, publisher_id: String, isolate_name: String) -> Self {
        Self { junction: Arc::new(junction), publisher_id, isolate_name }
    }

    /// Invokes the ValidateIsolateEndorsement RPC on the setup isolate.
    pub async fn validate_isolate_endorsement(
        &self,
        request: ValidateIsolateEndorsementRequest,
    ) -> Result<ValidateIsolateEndorsementResponse> {
        self.invoke_rpc("ValidateIsolateEndorsement", request).await
    }

    // TODO: Support other SetupService RPCs like GetTlsCsr, FetchMtlsCertificate, FetchFrontendTlsCertificate.

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
                destination_operator_domain: String::new(),
                destination_publisher_id: self.publisher_id.clone(),
                destination_isolate_name: self.isolate_name.clone(),
                destination_method_name: method_name.to_string(),
                ..Default::default()
            }),
            isolate_input_iscope: Some(EzPayloadIsolateScope { datagram_iscopes: vec![] }),
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
