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

use data_scope_proto::enforcer::v1::DataScopeType;
use enforcer_proto::enforcer::v1::{
    ControlPlaneMetadata, EzPayloadIsolateScope, InvokeEzRequest, IsolateDataScope,
};
use ez_to_ez_service_proto::enforcer::v1::{
    ez_to_ez_api_server::{EzToEzApi, EzToEzApiServer},
    EzCallRequest, EzCallResponse, EzStatus,
};
use outbound_ez_to_ez_client::OutboundEzToEzClient;
use outbound_ez_to_ez_handler::OutboundEzToEzHandler;
use payload_proto::enforcer::v1::EzPayloadData;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

struct FakeEzToEzProxy {
    response_delay: Option<Duration>,
}

impl FakeEzToEzProxy {
    fn new(response_delay: Option<Duration>) -> Self {
        Self { response_delay }
    }
}

#[tonic::async_trait]
impl EzToEzApi for FakeEzToEzProxy {
    async fn ez_call(
        &self,
        request: Request<EzCallRequest>,
    ) -> Result<Response<EzCallResponse>, Status> {
        if let Some(delay) = self.response_delay {
            tokio::time::sleep(delay).await;
        }
        let req = request.into_inner();
        Ok(Response::new(EzCallResponse {
            payload_scope: req.payload_scope,
            payload_data: req.payload_data,
            status: Some(EzStatus { code: 0, message: "OK".to_string() }),
        }))
    }

    type EzStreamingCallStream = ReceiverStream<Result<EzCallResponse, Status>>;

    async fn ez_streaming_call(
        &self,
        request: Request<Streaming<EzCallRequest>>,
    ) -> Result<Response<Self::EzStreamingCallStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(req) => {
                        let resp = EzCallResponse {
                            payload_scope: req.payload_scope,
                            payload_data: req.payload_data,
                            status: Some(EzStatus { code: 0, message: "OK".to_string() }),
                        };
                        tx.send(Ok(resp)).await.unwrap();
                    }
                    Err(e) => {
                        tx.send(Err(e)).await.unwrap();
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn start_fake_proxy_server(response_delay: Option<Duration>) -> (u16, oneshot::Sender<()>) {
    let fake_proxy = FakeEzToEzProxy::new(response_delay);
    let sockaddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let listener = TcpListener::bind(sockaddr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(EzToEzApiServer::new(fake_proxy))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    shutdown_rx.await.ok();
                },
            )
            .await
            .unwrap();
    });
    (port, shutdown_tx)
}

fn create_test_request(input_payload: &str) -> InvokeEzRequest {
    InvokeEzRequest {
        control_plane_metadata: Some(ControlPlaneMetadata::default()),
        isolate_request_iscope: Some(EzPayloadIsolateScope {
            datagram_iscopes: vec![IsolateDataScope {
                scope_type: DataScopeType::Public.into(),
                ..Default::default()
            }],
        }),
        isolate_request_payload: Some(EzPayloadData {
            datagrams: vec![input_payload.as_bytes().to_vec()],
        }),
    }
}

#[tokio::test]
async fn test_outbound_unary_flow() {
    let (port, shutdown_tx) = start_fake_proxy_server(None).await;
    let server_address = format!("http://localhost:{}", port);
    let handler = OutboundEzToEzHandler::new(server_address).await.unwrap();

    let expected_payload = "hello unary";
    let request = create_test_request(expected_payload);

    let response = handler.remote_invoke(request, None).await.unwrap();

    assert_eq!(response.status.unwrap().code, 0);
    let output_payload = response.ez_response_payload.unwrap().datagrams[0].clone();
    assert_eq!(output_payload, expected_payload.as_bytes());

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_outbound_unary_call_timeout_propagation() {
    // NOTE: Delay is longer than the timeout to ensure client times out.
    let server_delay = Duration::from_millis(200);
    let (port, shutdown_tx) = start_fake_proxy_server(Some(server_delay)).await;
    let server_address = format!("http://localhost:{}", port);
    let handler = OutboundEzToEzHandler::new(server_address).await.unwrap();

    let request = create_test_request("timeout test");
    let client_timeout = Duration::from_millis(100);

    let handle =
        tokio::spawn(async move { handler.remote_invoke(request, Some(client_timeout)).await });

    let result = handle.await.unwrap();

    assert!(result.is_err());
    let err = result.unwrap_err();

    // Ideally, we should check for a specific timeout error code or message.
    // For now, we check if the error message contains "deadline exceeded" or "timed out".
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("deadline exceeded") || err_msg.contains("Timeout expired"),
        "Expected timeout error, got: {}",
        err_msg
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_outbound_streaming_flow() {
    let (port, shutdown_tx) = start_fake_proxy_server(None).await;
    let server_address = format!("http://localhost:{}", port);
    let handler = OutboundEzToEzHandler::new(server_address).await.unwrap();

    let first_payload = "hello";
    let second_payload = "world";

    let (local_to_outbound, from_local_rx) = mpsc::channel(10);
    let mut outbound_to_local = handler.remote_streaming_connect(from_local_rx).await.unwrap();

    let initial_request = create_test_request(first_payload);
    local_to_outbound.send(initial_request).await.unwrap();

    let first_response = outbound_to_local.recv().await.unwrap().unwrap();
    assert_eq!(first_response.status.unwrap().code, 0);
    let first_output_payload = first_response.ez_response_payload.unwrap().datagrams[0].clone();
    assert_eq!(first_output_payload, first_payload.as_bytes());

    let second_request = create_test_request(second_payload);
    local_to_outbound.send(second_request).await.unwrap();

    let second_response = outbound_to_local.recv().await.unwrap().unwrap();
    assert_eq!(second_response.status.unwrap().code, 0);
    let second_output_payload = second_response.ez_response_payload.unwrap().datagrams[0].clone();
    assert_eq!(second_output_payload, second_payload.as_bytes());

    drop(local_to_outbound);
    assert!(outbound_to_local.recv().await.is_none());

    let _ = shutdown_tx.send(());
}
