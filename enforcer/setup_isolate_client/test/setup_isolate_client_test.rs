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

use enforcer_proto::enforcer::v1::{InvokeIsolateRequest, InvokeIsolateResponse};
use ez_error::EzError;
use isolate_info::IsolateId;
use junction_trait::Junction;
use payload_proto::enforcer::v1::{
    ez_hybrid_payload::DeliveryMethod, EzHybridPayload, EzPayloadData,
};
use prost::Message;
use setup_isolate_client::SetupIsolateClient;
use setup_isolate_proto::enforcer::v2::{
    ValidateIsolateEndorsementRequest, ValidateIsolateEndorsementResponse, Validity,
};
use setup_isolate_proto::timestamp_proto::google::protobuf::Timestamp;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
struct MockJunction {
    response: Result<InvokeIsolateResponse, String>,
    last_request: Arc<std::sync::Mutex<Option<InvokeIsolateRequest>>>,
}

impl MockJunction {
    fn new(response: Result<InvokeIsolateResponse, String>) -> Self {
        Self { response, last_request: Arc::new(std::sync::Mutex::new(None)) }
    }
}

#[tokio::test]
async fn test_validate_isolate_endorsement_success() {
    let validity = Validity {
        not_before: Some(Timestamp { seconds: 12345, nanos: 0 }),
        not_after: Some(Timestamp { seconds: 67890, nanos: 0 }),
    };
    let expected_res = ValidateIsolateEndorsementResponse {
        validation_error: 0,
        validity: Some(validity),
        ..Default::default()
    };

    let junction =
        MockJunction::new(Ok(create_valid_invoke_response(expected_res.encode_to_vec())));
    let last_request = junction.last_request.clone();
    let client = SetupIsolateClient::new(
        Box::new(junction),
        "PCIT".to_string(),
        "setup-isolate".to_string(),
    );
    let req = ValidateIsolateEndorsementRequest { ..Default::default() };
    let res = client.validate_isolate_endorsement(req).await.expect("RPC failed");

    assert_eq!(res.validation_error, 0);
    assert_eq!(res.validity, Some(validity));

    let invoked = last_request.lock().unwrap().clone().expect("No request invoked");
    let cpm = invoked.control_plane_metadata.expect("Missing metadata");
    assert_eq!(cpm.destination_method_name, "ValidateIsolateEndorsement");
    assert_eq!(cpm.destination_publisher_id, "PCIT");
    assert_eq!(cpm.destination_isolate_name, "setup-isolate");
}

#[tokio::test]
async fn test_junction_error() {
    let junction = MockJunction::new(Err("junction failure".to_string()));
    let client = SetupIsolateClient::new(Box::new(junction), "p".to_string(), "i".to_string());
    let res =
        client.validate_isolate_endorsement(ValidateIsolateEndorsementRequest::default()).await;
    let err_msg = res.as_ref().unwrap_err().to_string();

    assert!(err_msg.contains("Junction error"));
    assert!(err_msg.contains("junction failure"));
}

#[tokio::test]
async fn test_missing_isolate_output() {
    let junction =
        MockJunction::new(Ok(InvokeIsolateResponse { isolate_output: None, ..Default::default() }));
    let client = SetupIsolateClient::new(Box::new(junction), "p".to_string(), "i".to_string());
    let res =
        client.validate_isolate_endorsement(ValidateIsolateEndorsementRequest::default()).await;
    assert_eq!(res.as_ref().unwrap_err().to_string(), "Missing isolate output");
}

#[tokio::test]
async fn test_invalid_delivery_method() {
    let junction = MockJunction::new(Ok(InvokeIsolateResponse {
        isolate_output: Some(EzHybridPayload { delivery_method: None }),
        ..Default::default()
    }));
    let client = SetupIsolateClient::new(Box::new(junction), "p".to_string(), "i".to_string());
    let res =
        client.validate_isolate_endorsement(ValidateIsolateEndorsementRequest::default()).await;
    assert_eq!(res.as_ref().unwrap_err().to_string(), "Expected inline data in isolate response");
}

#[tokio::test]
async fn test_missing_datagram() {
    let junction = MockJunction::new(Ok(InvokeIsolateResponse {
        isolate_output: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData { datagrams: vec![] })),
        }),
        ..Default::default()
    }));
    let client = SetupIsolateClient::new(Box::new(junction), "p".to_string(), "i".to_string());

    let res =
        client.validate_isolate_endorsement(ValidateIsolateEndorsementRequest::default()).await;
    assert_eq!(res.as_ref().unwrap_err().to_string(), "Missing output datagram");
}

#[tokio::test]
async fn test_decode_error() {
    let junction = MockJunction::new(Ok(create_valid_invoke_response(vec![0xFF, 0xFF]))); // Invalid protobuf
    let client = SetupIsolateClient::new(Box::new(junction), "p".to_string(), "i".to_string());
    let res =
        client.validate_isolate_endorsement(ValidateIsolateEndorsementRequest::default()).await;
    assert!(
        res.as_ref().unwrap_err().to_string().contains("decode error")
            || res.as_ref().unwrap_err().to_string().contains("failed to decode")
    );
}

#[tonic::async_trait]
impl Junction for MockJunction {
    async fn invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        invoke_isolate_request: InvokeIsolateRequest,
        _is_from_public_api: bool,
        _deadline: Option<Instant>,
    ) -> Result<InvokeIsolateResponse, EzError> {
        *self.last_request.lock().unwrap() = Some(invoke_isolate_request);
        match &self.response {
            Ok(res) => Ok(res.clone()),
            Err(e) => Err(EzError::Status(tonic::Status::internal(e.to_string()))),
        }
    }

    async fn stream_invoke_isolate(
        &self,
        _client_isolate_id_option: Option<IsolateId>,
        _is_from_public_api: bool,
        _timeout: Option<std::time::Duration>,
    ) -> junction_trait::JunctionChannels {
        unimplemented!()
    }

    async fn connect_isolate(
        &self,
        _isolate_id: IsolateId,
        _isolate_address: String,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }
}

fn create_valid_invoke_response(payload: Vec<u8>) -> InvokeIsolateResponse {
    InvokeIsolateResponse {
        isolate_output: Some(EzHybridPayload {
            delivery_method: Some(DeliveryMethod::InlineData(EzPayloadData {
                datagrams: vec![payload],
            })),
        }),
        ..Default::default()
    }
}
