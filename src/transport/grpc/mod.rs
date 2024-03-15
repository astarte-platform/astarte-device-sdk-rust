/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

//! # Astarte GRPC Transport Module
//!
//! This module provides an implementation of the Astarte transport layer using the GRPC protocol.
//! It defines the `Grpc` struct, which represents a GRPC connection, along with traits for publishing,
//! receiving, and registering interfaces.

pub mod convert;

use std::{collections::HashMap, ops::Deref, sync::Arc};

use astarte_message_hub_proto::tonic::codegen::InterceptedService;
use astarte_message_hub_proto::tonic::metadata::MetadataValue;
use astarte_message_hub_proto::tonic::service::Interceptor;
use astarte_message_hub_proto::tonic::transport::Channel;
use astarte_message_hub_proto::tonic::{Request, Status};
use astarte_message_hub_proto::{
    astarte_message::Payload, message_hub_client::MessageHubClient, tonic, AstarteMessage, Node,
};
use async_trait::async_trait;
use itertools::Itertools;
use log::trace;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    builder::{ConnectionConfig, DeviceBuilder},
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
    },
    interfaces::{self, Interfaces},
    shared::SharedDevice,
    store::PropertyStore,
    types::AstarteType,
    validate::{ValidatedIndividual, ValidatedObject},
    Interface, Timestamp,
};

use super::{Disconnect, Publish, Receive, ReceivedEvent, Register};

use self::convert::{map_values_to_astarte_type, MessageHubProtoError};

/// Errors raised while using the [`Grpc`] transport
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    #[error("Transport error while working with grpc: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Status error {0}")]
    Status(#[from] tonic::Status),
    #[error("Error while serializing the interfaces")]
    InterfacesSerialization(#[from] serde_json::Error),
    #[error("Attempting to deserialize individual message but got an object")]
    DeserializationExpectedIndividual,
    #[error("Attempting to deserialize object message but got an individual value")]
    DeserializationExpectedObject,
    #[error("Graceful close of the grpc channel failed, the Arc is still shared")]
    GracefulClose,
    #[error(transparent)]
    MessageHubProtoConversion(#[from] MessageHubProtoError),
}

type MessageHubClientWithInterceptor =
    MessageHubClient<InterceptedService<Channel, NodeIdInterceptor>>;

pub(crate) struct NodeIdInterceptor(Uuid);

impl NodeIdInterceptor {
    const NODE_ID_METADATA_KEY: &'static str = "node-id-bin";

    pub(crate) fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Interceptor for NodeIdInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let val = MetadataValue::from_bytes(self.0.as_bytes());
        req.metadata_mut()
            .insert_bin(Self::NODE_ID_METADATA_KEY, val);
        Ok(req)
    }
}

/// Shared data of the grpc connection, this struct is internal to the [`Grpc`] connection
/// where is wrapped in an arc to share an immutable reference across tasks.
#[derive(Debug)]
pub struct SharedGrpc {
    client: Mutex<MessageHubClientWithInterceptor>,
    stream: Mutex<tonic::codec::Streaming<AstarteMessage>>,
    uuid: Uuid,
}

/// This struct represents a GRPC connection handler for an Astarte device. It manages the
/// interaction with the [astarte-message-hub](https://github.com/astarte-platform/astarte-message-hub), sending and receiving [`AstarteMessage`]
/// following the Astarte message hub protocol.
#[derive(Clone)]
pub struct Grpc {
    shared: Arc<SharedGrpc>,
}

impl Grpc {
    pub(crate) fn new(
        client: MessageHubClientWithInterceptor,
        stream: tonic::codec::Streaming<AstarteMessage>,
        uuid: Uuid,
    ) -> Self {
        Self {
            shared: Arc::new(SharedGrpc {
                client: Mutex::new(client),
                stream: Mutex::new(stream),
                uuid,
            }),
        }
    }

    /// Polls a message from the tonic stream and tries reattaching if necessary
    ///
    /// An [`Option`] is returned directly from the [`tonic::codec::Streaming::message`] method.
    /// A result of [`None`] signals a disconnection and should be handled by the caller
    async fn next_message(&self) -> Result<Option<AstarteMessage>, tonic::Status> {
        self.stream.lock().await.message().await
    }

    async fn attach(
        client: &mut MessageHubClientWithInterceptor,
        data: NodeData,
    ) -> Result<tonic::codec::Streaming<AstarteMessage>, GrpcError> {
        client
            .attach(tonic::Request::new(data.node))
            .await
            .map(|r| r.into_inner())
            .map_err(GrpcError::from)
    }

    async fn detach(
        mut client: MessageHubClientWithInterceptor,
        uuid: &Uuid,
    ) -> Result<(), GrpcError> {
        // During the detach phase only the uuid is needed we can pass an empty array
        // as the interface_json since the interfaces are already known to the message hub
        // this api will change in the future
        client
            .detach(Node::new(uuid, &Vec::<Vec<u8>>::new()))
            .await
            .map(|_| ())
            .map_err(GrpcError::from)
    }

    async fn reattach(&self, data: NodeData) -> Result<(), GrpcError> {
        // the lock on stream is actually intended since we are detaching and re-attaching
        // we want to make sure no one uses the stream while the client is detached
        let mut stream = self.stream.lock().await;
        let mut client = self.client.lock().await;

        client
            .detach(Node::new(self.uuid, &Vec::<Vec<u8>>::new()))
            .await
            .map(|_| ())?;

        *stream = Grpc::attach(&mut client, data).await?;

        Ok(())
    }
}

impl std::fmt::Debug for Grpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grpc")
            .field("uuid", &self.uuid)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Publish for Grpc {
    async fn send_individual(&self, data: ValidatedIndividual<'_>) -> Result<(), crate::Error> {
        self.client
            .lock()
            .await
            .send(tonic::Request::new(
                data.try_into().map_err(GrpcError::from)?,
            ))
            .await
            .map(|_| ())
            .map_err(|e| GrpcError::from(e).into())
    }

    async fn send_object(&self, data: ValidatedObject<'_>) -> Result<(), crate::Error> {
        self.client
            .lock()
            .await
            .send(tonic::Request::new(
                data.try_into().map_err(GrpcError::from)?,
            ))
            .await
            .map(|_| ())
            .map_err(|e| GrpcError::from(e).into())
    }
}

impl Deref for Grpc {
    type Target = SharedGrpc;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

#[async_trait]
impl Receive for Grpc {
    type Payload = GrpcReceivePayload;

    async fn next_event<S>(
        &self,
        device: &SharedDevice<S>,
    ) -> Result<ReceivedEvent<Self::Payload>, crate::Error>
    where
        S: PropertyStore,
    {
        loop {
            match self.next_message().await {
                Ok(Some(message)) => {
                    let event: ReceivedEvent<Self::Payload> =
                        message.try_into().map_err(GrpcError::from)?;

                    return Ok(event);
                }
                Err(s)
                    if s.code() != tonic::Code::Unavailable && s.code() != tonic::Code::Unknown =>
                {
                    return Err(GrpcError::from(s).into());
                }
                Ok(None) | Err(_) => {
                    // try reattaching

                    let data = {
                        let interfaces = device.interfaces.read().await;
                        NodeData::try_from_interfaces(&self.uuid, &interfaces)?
                    };

                    let mut stream = self.stream.lock().await;
                    let mut client = self.client.lock().await;

                    *stream = Grpc::attach(&mut client, data).await?;
                }
            }
        }
    }

    fn deserialize_individual(
        &self,
        _mapping: MappingRef<'_, &Interface>,
        payload: Self::Payload,
    ) -> Result<(AstarteType, Option<Timestamp>), crate::Error> {
        let data = match payload.data {
            Payload::AstarteData(data) => data,
            Payload::AstarteUnset(astarte_message_hub_proto::AstarteUnset {}) => {
                trace!("unset received");

                return Ok((AstarteType::Unset, payload.timestamp));
            }
        };

        trace!("unset received");
        let individual = data
            .take_individual()
            .ok_or(GrpcError::DeserializationExpectedIndividual)?;

        let data: AstarteType = individual
            .try_into()
            .map_err(GrpcError::MessageHubProtoConversion)?;

        trace!("received {}", data.display_type());

        Ok((data, payload.timestamp))
    }

    fn deserialize_object(
        &self,
        _object: ObjectRef,
        _path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), crate::Error> {
        let object = payload
            .data
            .take_data()
            .and_then(|d| d.take_object())
            .ok_or(GrpcError::DeserializationExpectedObject)?;

        let data = map_values_to_astarte_type(object.object_data)
            .map_err(GrpcError::MessageHubProtoConversion)?;

        trace!("object received");

        Ok((data, payload.timestamp))
    }
}

#[async_trait]
impl Register for Grpc {
    async fn add_interface(
        &self,
        interfaces: &Interfaces,
        added: &interfaces::Validated,
    ) -> Result<(), crate::Error> {
        let iter = interfaces.iter_with_added(added);
        let data = NodeData::try_from_iter(&self.uuid, iter)?;

        self.reattach(data).await.map_err(crate::Error::from)
    }

    async fn remove_interface(
        &self,
        interfaces: &Interfaces,
        removed: &Interface,
    ) -> Result<(), crate::Error> {
        let iter = interfaces.iter_with_removed(removed);

        let data = NodeData::try_from_iter(&self.uuid, iter)?;

        self.reattach(data).await.map_err(crate::Error::from)
    }

    async fn extend_interfaces(
        &self,
        interfaces: &Interfaces,
        added: &interfaces::ValidatedCollection,
    ) -> Result<(), crate::Error> {
        let iter = interfaces.iter_with_added_many(added);

        let data = NodeData::try_from_iter(&self.uuid, iter)?;

        self.reattach(data).await.map_err(crate::Error::from)
    }
}

#[async_trait]
impl Disconnect for Grpc {
    async fn disconnect(mut self) -> Result<(), crate::Error> {
        let SharedGrpc {
            client,
            stream: _stream,
            uuid,
        } = Arc::into_inner(self.shared).ok_or(GrpcError::GracefulClose)?;

        let client = client.into_inner();

        Self::detach(client, &uuid).await.map_err(|e| e.into())
    }
}

/// Internal struct holding the received grpc message
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GrpcReceivePayload {
    data: Payload,
    timestamp: Option<Timestamp>,
}

impl GrpcReceivePayload {
    pub(crate) fn new(data: Payload, timestamp: Option<Timestamp>) -> Self {
        Self { data, timestamp }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GrpcConfig {
    uuid: Uuid,
    endpoint: String,
}

impl GrpcConfig {
    pub fn new(uuid: Uuid, endpoint: String) -> Self {
        Self { uuid, endpoint }
    }
}

/// Configuration for the grpc connection
#[async_trait]
impl ConnectionConfig for GrpcConfig {
    type Con = Grpc;
    type Err = GrpcError;

    async fn connect<S, C>(self, builder: &DeviceBuilder<S, C>) -> Result<Self::Con, Self::Err>
    where
        S: PropertyStore,
        C: Send + Sync,
    {
        let channel = tonic::transport::Endpoint::new(self.endpoint)?
            .connect()
            .await?;

        let node_id_interceptor = NodeIdInterceptor::new(self.uuid);

        let mut client = MessageHubClient::with_interceptor(channel, node_id_interceptor);

        let node_data = NodeData::try_from_interfaces(&self.uuid, &builder.interfaces)?;
        let stream = Grpc::attach(&mut client, node_data).await?;

        Ok(Grpc::new(client, stream, self.uuid))
    }
}

/// Wrapper that contains data needed while connecting the node to the astarte message hub
struct NodeData {
    node: Node,
}

impl NodeData {
    fn try_from_iter<'a, I>(uuid: &Uuid, interfaces: I) -> Result<Self, GrpcError>
    where
        I: IntoIterator<Item = &'a Interface>,
    {
        let interface_jsons = interfaces
            .into_iter()
            .map(serde_json::to_vec)
            .try_collect()?;

        Ok(Self {
            node: Node {
                uuid: uuid.to_string(),
                interface_jsons,
            },
        })
    }

    fn try_from_interfaces(uuid: &Uuid, interfaces: &Interfaces) -> Result<Self, GrpcError> {
        Self::try_from_iter(uuid, interfaces.iter())
    }
}

#[cfg(test)]
mod test {
    use std::{future::Future, net::SocketAddr, str::FromStr};

    use astarte_message_hub_proto::{
        message_hub_server::{MessageHub, MessageHubServer},
        pbjson_types,
    };
    use async_trait::async_trait;
    use tokio::{net::TcpListener, sync::mpsc};
    use uuid::uuid;

    use crate::{
        error,
        transport::test::{mock_shared_device, mock_validate_individual, mock_validate_object},
        Aggregation, AstarteAggregate, AstarteDeviceDataEvent,
    };

    use super::*;

    #[derive(Debug)]
    enum ServerReceivedRequest {
        Attach(Node),
        Send(AstarteMessage),
        Detach(Node),
    }

    impl ServerReceivedRequest {
        fn as_attach(&self) -> Option<&Node> {
            if let Self::Attach(v) = self {
                Some(v)
            } else {
                None
            }
        }
    }

    type ServerSenderValuesVec = Vec<Result<AstarteMessage, tonic::Status>>;

    pub(crate) struct TestMessageHubServer {
        /// This stream can be used to send test events that will be handled by the astarte device sdk code
        /// and by the Grpc client.
        /// Each received elements is a "session": the [`Vec`] received contains messages that will be sent
        /// after the client attaches to the server.
        /// Every successive [`Vec`] is only returned if the client reattaches to the server.
        server_send: Mutex<mpsc::Receiver<ServerSenderValuesVec>>,
        /// This stream contains requests received by the server
        server_received: mpsc::Sender<ServerReceivedRequest>,
    }

    impl TestMessageHubServer {
        fn new(
            server_send: mpsc::Receiver<Vec<Result<AstarteMessage, tonic::Status>>>,
            server_received: mpsc::Sender<ServerReceivedRequest>,
        ) -> Self {
            Self {
                server_send: Mutex::new(server_send),
                server_received,
            }
        }
    }

    #[async_trait]
    impl MessageHub for TestMessageHubServer {
        type AttachStream =
            futures::stream::Iter<std::vec::IntoIter<Result<AstarteMessage, tonic::Status>>>;

        async fn attach(
            &self,
            request: tonic::Request<Node>,
        ) -> Result<tonic::Response<Self::AttachStream>, tonic::Status> {
            let inner = request.into_inner();
            println!("Client '{}' attached", inner.uuid.clone());

            self.server_received.send(ServerReceivedRequest::Attach(inner)).await
                .expect("Could not send notification of a server received message, connect a channel to the Receiver");

            let mut receiver_lock = self.server_send.lock().await;

            let response_vec = receiver_lock.recv().await.unwrap();

            Ok(tonic::Response::new(futures::stream::iter(response_vec)))
        }

        async fn send(
            &self,
            request: tonic::Request<AstarteMessage>,
        ) -> Result<tonic::Response<pbjson_types::Empty>, tonic::Status> {
            self.server_received.send(ServerReceivedRequest::Send(request.into_inner())).await
                .expect("Could not send notification of a server received message, connect a channel to the Receiver");

            Ok(tonic::Response::new(pbjson_types::Empty::default()))
        }

        async fn detach(
            &self,
            request: tonic::Request<Node>,
        ) -> Result<tonic::Response<pbjson_types::Empty>, tonic::Status> {
            let inner = request.into_inner();
            println!("Client '{}' detached", inner.uuid.clone());

            self.server_received.send(ServerReceivedRequest::Detach(inner)).await
                .expect("Could not send notification of a server received message, connect a channel to the Receiver");

            Ok(tonic::Response::new(pbjson_types::Empty::default()))
        }
    }

    fn make_server(
        sock: TcpListener,
        server: TestMessageHubServer,
    ) -> Result<impl Future<Output = ()>, Box<dyn std::error::Error>> {
        Ok(async move {
            tonic::transport::Server::builder()
                .add_service(MessageHubServer::new(server))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(sock))
                .await
                .unwrap();
        })
    }

    async fn make_client(
        addr: SocketAddr,
        interceptor: NodeIdInterceptor,
    ) -> impl Future<Output = MessageHubClientWithInterceptor> {
        async move {
            let channel = loop {
                let channel_res = tonic::transport::Endpoint::try_from(format!("http://{}", addr))
                    .unwrap()
                    .connect()
                    .await;

                match channel_res {
                    Ok(channel) => break channel,
                    Err(err) => println!("Failed attempt of connecting with error: {}", err),
                }
            };

            MessageHubClient::with_interceptor(channel, interceptor)
        }
    }

    async fn mock_grpc_actors(
        server_impl: TestMessageHubServer,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<Output = MessageHubClientWithInterceptor>,
        ),
        Box<dyn std::error::Error>,
    > {
        // bind to port 0 to make the kernel choose an open port
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;

        let addr = socket.local_addr()?;

        let server = make_server(socket, server_impl)?;

        let interceptor = NodeIdInterceptor::new(ID);

        let client = make_client(addr, interceptor).await;

        Ok((server, client))
    }

    const ID: Uuid = uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8");

    async fn mock_astarte_grpc_client(
        mut message_hub_client: MessageHubClientWithInterceptor,
        interfaces: &Interfaces,
    ) -> Result<Grpc, Box<dyn std::error::Error>> {
        let node_data = NodeData::try_from_interfaces(&ID, interfaces)?;
        let stream = Grpc::attach(&mut message_hub_client, node_data).await?;

        Ok(Grpc::new(message_hub_client, stream, ID))
    }

    struct TestServerChannels {
        server_response_sender: mpsc::Sender<Vec<Result<AstarteMessage, tonic::Status>>>,
        server_request_receiver: mpsc::Receiver<ServerReceivedRequest>,
    }

    fn build_test_message_hub_server() -> (TestMessageHubServer, TestServerChannels) {
        // Holds the stream of messages that will follow an attach, the server stores the receiver
        // and relays messages to the stream got by the client that called `attach`
        let server_response_channel = mpsc::channel(10);

        // This channel holds requests that arrived to the server and can be used to verify that the
        // requests received are correct, the server will store the transmitting end of the channel
        // to send events when a new request is received
        let server_request_channel = mpsc::channel(10);

        (
            TestMessageHubServer::new(server_response_channel.1, server_request_channel.0),
            TestServerChannels {
                server_response_sender: server_response_channel.0,
                server_request_receiver: server_request_channel.1,
            },
        )
    }

    macro_rules! expect_messages {
        ($poll_result_fn:expr; $($pattern:pat $($(=> $var:ident = $expr_value:expr;)? if $guard:expr),+),+) => {{
            let mut i = 0usize;

            $(
                // One based indexing
                i += 1;

                match $poll_result_fn {
                    Result::Ok(v) => {
                        match v {
                            $pattern => {
                                $(
                                    $(
                                        let $var = $expr_value;
                                    )?

                                    if !($guard) {
                                        panic!("The message n.{} didn't pass the guard '{}'", i, stringify!($guard));
                                    }
                                )*

                                println!("Matched message n.{}", i);
                            },
                            // Depending on the user declaration this pattern could be unreachable and this is fine
                            #[allow(unreachable_patterns)]
                            actual => panic!("Expected message n.{} to be matching the pattern '{}' but got '{:?}'", i, stringify!($pattern), actual),
                        }
                    }
                    Result::Err(e) => {
                        panic!("Expected message n.{} with pattern '{}' but the `{}` returned an `Err` {:?}", i, stringify!($pattern), stringify!($poll_result_fn), e);
                    }
                }
            )+
        }};
    }

    #[tokio::test]
    async fn test_attach_detach() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        // no messages are read as responses by the server so we pass an empty vec
        channels.server_response_sender.send(vec![]).await.unwrap();

        let client_operations = async move {
            let client = client_future.await;
            // When the grpc connection gets created the attach methods is called
            let connection = mock_astarte_grpc_client(client, &Interfaces::new())
                .await
                .unwrap();

            // manually calling detach
            connection.disconnect().await.unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            ServerReceivedRequest::Detach(a) if a.uuid == ID.to_string()
        );
    }

    #[tokio::test]
    async fn test_server_error() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        let err = tonic::Status::unknown("Test unknown reattach");
        println!("{:?} eq {}", err, err.code() == tonic::Code::Unknown);

        // send first error which causes a reattach
        channels
            .server_response_sender
            .send(vec![Err(tonic::Status::unknown("Test unknown reattach"))])
            .await
            .unwrap();

        // send second error which causes a reattach
        channels
            .server_response_sender
            .send(vec![Err(tonic::Status::unavailable(
                "Test unavailable reattach",
            ))])
            .await
            .unwrap();

        // no reattach
        channels
            .server_response_sender
            .send(vec![Err(tonic::Status::not_found("Test no reattach"))])
            .await
            .unwrap();

        let client_operations = async move {
            let client = client_future.await;
            // When the grpc connection gets created the attach methods is called
            let connection = mock_astarte_grpc_client(client, &Interfaces::new())
                .await
                .unwrap();

            let mock_shared_device = mock_shared_device(Interfaces::new(), mpsc::channel(1).0); // the channel won't be used

            // poll the three messages the first two received errors will simply reconnect without returning
            assert!(matches!(
                connection.next_event(&mock_shared_device).await,
                Err(error::Error::Grpc(GrpcError::Status(_)))
            ));

            // manually calling detach
            connection.disconnect().await.unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            // connection creation attach
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            // first error attach
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            // second error attach
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            ServerReceivedRequest::Detach(a) if a.uuid == ID.to_string()
        );
    }

    #[tokio::test]
    async fn test_add_remove_interface() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        // send first error which causes a reattach
        channels.server_response_sender.send(vec![]).await.unwrap();
        // second attach add_interface
        channels.server_response_sender.send(vec![]).await.unwrap();
        // third attach remove_interface
        channels.server_response_sender.send(vec![]).await.unwrap();

        let client_operations = async move {
            let client = client_future.await;
            // When the grpc connection gets created the attach methods is called
            let connection = mock_astarte_grpc_client(client, &Interfaces::new())
                .await
                .unwrap();

            let mock_shared_device = mock_shared_device(Interfaces::new(), mpsc::channel(1).0); // the channel won't be used

            let interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();
            let interfaces = mock_shared_device.interfaces.read().await;
            let validated = interfaces.validate(interface).unwrap().unwrap();

            connection
                .add_interface(&interfaces, &validated)
                .await
                .unwrap();

            connection
                .remove_interface(&interfaces, &validated)
                .await
                .unwrap();

            // manually calling detach
            connection.disconnect().await.unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            // connection creation attach
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            // add interface reattach
            ServerReceivedRequest::Detach(a) if a.uuid == ID.to_string(),
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            // remove interface reattach
            ServerReceivedRequest::Detach(a) if a.uuid == ID.to_string(),
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            // detach
            ServerReceivedRequest::Detach(a) if a.uuid == ID.to_string()
        );
    }

    #[tokio::test]
    async fn test_send_individual() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        const INTERFACE_NAME: &str =
            "org.astarte-platform.rust.examples.individual-properties.DeviceProperties";
        const STRING_VALUE: &str = "value";

        // no messages are read as responses by the server so we pass an empty vec
        channels.server_response_sender.send(vec![]).await.unwrap();

        let client_operations = async move {
            let client = client_future.await;

            let path = MappingPath::try_from("/1/name").unwrap();
            let interfaces =
                Interfaces::from_iter([
                    Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap()
                ]);
            let mapping_ref = interfaces.interface_mapping(INTERFACE_NAME, &path).unwrap();

            let connection = mock_astarte_grpc_client(client, &interfaces).await.unwrap();

            let validated_individual = mock_validate_individual(
                mapping_ref,
                &path,
                AstarteType::String(STRING_VALUE.to_string()),
                None,
            )
            .unwrap();

            connection
                .send_individual(validated_individual)
                .await
                .unwrap();

            connection.disconnect().await.unwrap();
        };

        // Poll client and server future
        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            ServerReceivedRequest::Send(m)
            => data_event = AstarteDeviceDataEvent::try_from(m).expect("Malformed message");
                if data_event.interface == "org.astarte-platform.rust.examples.individual-properties.DeviceProperties"
                && data_event.path == "/1/name"
                && matches!(data_event.data, Aggregation::Individual(AstarteType::String(v)) if v == STRING_VALUE),
            ServerReceivedRequest::Detach(d) if d.uuid == ID.to_string()
        );
    }

    struct MockObject {}

    impl AstarteAggregate for MockObject {
        fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, crate::error::Error> {
            let mut obj = HashMap::new();
            obj.insert("endpoint1".to_string(), AstarteType::Double(4.2));
            obj.insert(
                "endpoint2".to_string(),
                AstarteType::String("obj".to_string()),
            );
            obj.insert(
                "endpoint3".to_string(),
                AstarteType::BooleanArray(vec![true]),
            );

            Ok(obj)
        }
    }

    #[tokio::test]
    async fn test_send_object_timestamp() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        // no messages are read as responses by the server so we pass an empty vec
        channels.server_response_sender.send(vec![]).await.unwrap();

        let client_operations = async move {
            let client = client_future.await;

            let interface = Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap();
            let path = MappingPath::try_from("/1").unwrap();
            let interfaces = Interfaces::from_iter([interface.clone()]);

            let connection = mock_astarte_grpc_client(client, &interfaces).await.unwrap();

            let validated_object = mock_validate_object(
                &interface,
                &path,
                MockObject {},
                Some(chrono::offset::Utc::now()),
            )
            .unwrap();

            connection.send_object(validated_object).await.unwrap()
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            ServerReceivedRequest::Send(m)
            => data_event = AstarteDeviceDataEvent::try_from(m).expect("Malformed message");
                if data_event.interface == "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream"
                    && data_event.path == "/1",
            => object_value = {  let Aggregation::Object(v) = data_event.data else { panic!("Expected object") }; v };
                if object_value["endpoint1"] == AstarteType::Double(4.2)
                    && object_value["endpoint2"] == AstarteType::String("obj".to_string())
                    && object_value["endpoint3"] == AstarteType::BooleanArray(vec![true])
        );
    }

    #[tokio::test]
    async fn test_connection_receive_object() {
        let (server_impl, channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        let expected_object = Aggregation::Object((MockObject {}).astarte_aggregate().unwrap());

        let proto_payload: astarte_message_hub_proto::astarte_message::Payload =
            expected_object.try_into().unwrap();

        let astarte_message = AstarteMessage {
            interface_name: "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream"
                .to_string(),
            path: "/1".to_string(),
            timestamp: None,
            payload: Some(proto_payload.clone()),
        };

        // Send object from server
        channels
            .server_response_sender
            .send(vec![Ok(astarte_message)])
            .await
            .unwrap();

        let interfaces =
            Interfaces::from_iter([
                Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap()
            ]);

        let client_connection = async {
            let client = client_future.await;

            mock_astarte_grpc_client(client, &interfaces).await
        };

        let connection = tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            res = client_connection => {
                println!("Client connected correctly: {}", res.is_ok());

                res.expect("Expected correct connection in test")
            },
        };

        let mock_shared_device = mock_shared_device(interfaces, mpsc::channel(1).0); // the channel won't be used

        expect_messages!(connection.next_event(&mock_shared_device).await;
            ReceivedEvent {
                ref interface,
                ref path,
                payload: GrpcReceivePayload {
                    data,
                    timestamp: None,
                },
            } if interface == "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream"
                && path == "/1"
                && data == proto_payload
        );
    }

    #[tokio::test]
    async fn test_connection_receive_unset() {
        let (server_impl, channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        let proto_payload: astarte_message_hub_proto::astarte_message::Payload =
            AstarteType::Unset.try_into().unwrap();

        let exp_interface =
            "org.astarte-platform.rust.examples.individual-properties.ServerProperties";
        let exp_path = "/1/enable";
        let astarte_message = AstarteMessage {
            interface_name: exp_interface.to_string(),
            path: exp_path.to_string(),
            timestamp: None,
            payload: Some(proto_payload.clone()),
        };

        // Send object from server
        channels
            .server_response_sender
            .send(vec![Ok(astarte_message)])
            .await
            .unwrap();

        let interfaces =
            Interfaces::from_iter([Interface::from_str(crate::test::SERVER_PROPERTIES).unwrap()]);

        let client_connection = async {
            let client = client_future.await;

            mock_astarte_grpc_client(client, &interfaces).await
        };

        let connection = tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            res = client_connection => {
                println!("Client connected correctly: {}", res.is_ok());

                res.expect("Expected correct connection in test")
            },
        };

        let mock_shared_device = mock_shared_device(interfaces, mpsc::channel(1).0); // the channel won't be used

        expect_messages!(connection.next_event(&mock_shared_device).await;
            ReceivedEvent {
                ref interface,
                ref path,
                payload: GrpcReceivePayload {
                    data,
                    timestamp: None,
                },
            } if interface == exp_interface
                && path == exp_path
                && data == proto_payload
        );
    }

    #[tokio::test]
    async fn should_extend_interfaces() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        // no messages are read as responses by the server so we pass an empty vec
        channels.server_response_sender.send(vec![]).await.unwrap();
        channels.server_response_sender.send(vec![]).await.unwrap();

        let to_add = [
            Interface::from_str(include_str!(
                "../../../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.DeviceProperty.json"
            ))
            .unwrap(),
            Interface::from_str(include_str!(
                "../../../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.ServerProperty.json"
            ))
            .unwrap(),
        ];

        let map = to_add
            .iter()
            .cloned()
            .map(|i| (i.interface_name().to_string(), i))
            .collect();

        let interfaces = Interfaces::new();

        let validated = interfaces.validate_many(to_add).unwrap();
        let client_operations = async move {
            let client = client_future.await;
            // When the grpc connection gets created the attach methods is called
            let connection = mock_astarte_grpc_client(client, &Interfaces::new())
                .await
                .unwrap();

            // manually calling detach
            connection
                .extend_interfaces(&interfaces, &validated)
                .await
                .unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            ServerReceivedRequest::Detach(a) if a.uuid == ID.to_string()
        );

        let recv = channels.server_request_receiver.try_recv().unwrap();
        let recv = recv.as_attach().unwrap();

        let id = Uuid::parse_str(&recv.uuid).unwrap();
        assert_eq!(id, ID);

        let interfaces: HashMap<String, Interface> = recv
            .interface_jsons
            .iter()
            .map(|i| {
                serde_json::from_slice(i).map(|i: Interface| (i.interface_name().to_string(), i))
            })
            .try_collect()
            .unwrap();

        assert_eq!(interfaces, map);
    }
}
