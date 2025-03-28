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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::time::Duration;

use astarte_message_hub_proto::prost::{DecodeError, Message};
use astarte_message_hub_proto::tonic::codec::Streaming;
use astarte_message_hub_proto::tonic::codegen::InterceptedService;
use astarte_message_hub_proto::tonic::metadata::MetadataValue;
use astarte_message_hub_proto::tonic::service::Interceptor;
use astarte_message_hub_proto::tonic::transport::{Channel, Endpoint};
use astarte_message_hub_proto::tonic::{Request, Status};
use astarte_message_hub_proto::{
    astarte_message::Payload as ProtoPayload, message_hub_client::MessageHubClient,
    pbjson_types::Empty, AstarteMessage, InterfacesJson, InterfacesName, MessageHubError,
    MessageHubEvent, Node,
};
use bytes::Bytes;
use sync_wrapper::SyncWrapper;
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use self::convert::MessageHubProtoError;
use self::store::GrpcStore;
use super::{
    Connection, Disconnect, Publish, Receive, ReceivedEvent, Reconnect, Register, TransportError,
};
use crate::builder::ConnectionBuildConfig;
use crate::client::RecvError;
use crate::error::AggregateError;
use crate::interface::Aggregation;
use crate::retention::memory::SharedVolatileStore;
use crate::retention::{PublishInfo, RetentionId};
use crate::Value;
use crate::{
    builder::{ConnectionConfig, DeviceTransport},
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
    },
    interfaces::{self, Interfaces},
    retention::StoredRetention,
    retry::ExponentialIter,
    store::{wrapper::StoreWrapper, PropertyStore, StoreCapabilities},
    types::AstarteType,
    validate::{ValidatedIndividual, ValidatedObject, ValidatedUnset},
    Error, Interface, Timestamp,
};

pub mod convert;
pub mod store;

#[cfg(feature = "message-hub")]
#[cfg_attr(docsrs, doc(cfg(feature = "message-hub")))]
pub use astarte_message_hub_proto::tonic;

/// Errors raised while using the [`Grpc`] transport
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    /// The gRPC connection returned an error.
    #[error("Transport error while working with grpc: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Status code error.
    #[error("Status error {0}")]
    Status(#[from] tonic::Status),
    #[error("Error while serializing the interfaces")]
    /// Couldn't serialize interface to json.
    InterfacesSerialization(#[from] serde_json::Error),
    /// Couldn't decode gRPC message
    #[error("couldn't decode grpc message")]
    Decode(#[from] DecodeError),
    /// Failed to convert a proto message.
    #[error("couldn't convert proto message")]
    MessageHubProtoConversion(#[from] MessageHubProtoError),
    /// Error returned by the message hub server
    #[error("error returned by the message hub server")]
    Server(#[from] MessageHubError),
}

type MsgHubClient = MessageHubClient<InterceptedService<Channel, NodeIdInterceptor>>;

#[derive(Debug, Clone, Copy)]
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

/// Client to send packets to the [Message Hub](https://github.com/astarte-platform/astarte-message-hub).
#[derive(Debug, Clone)]
pub struct GrpcClient<S> {
    client: MsgHubClient,
    store: StoreWrapper<S>,
    volatile: SharedVolatileStore,
}

impl<S> GrpcClient<S> {
    /// Create a new client.
    pub(crate) fn new(
        client: MsgHubClient,
        store: StoreWrapper<S>,
        volatile: SharedVolatileStore,
    ) -> Self {
        Self {
            client,
            store,
            volatile,
        }
    }

    async fn mark_received(&self, id: &RetentionId) -> Result<(), Error>
    where
        S: StoreCapabilities,
    {
        match id {
            RetentionId::Volatile(id) => {
                self.volatile.mark_received(id).await;
            }
            RetentionId::Stored(id) => {
                if let Some(retention) = self.store.get_retention() {
                    retention.mark_received(id).await?;
                }
            }
        }

        Ok(())
    }

    async fn detach(&mut self) -> Result<(), GrpcError> {
        self.client
            .detach(tonic::Request::new(Empty {}))
            .await
            .map(|_| ())
            .map_err(GrpcError::from)
    }
}

impl<S> Publish for GrpcClient<S>
where
    S: StoreCapabilities + Send + Sync,
{
    async fn send_individual(&mut self, data: ValidatedIndividual) -> Result<(), crate::Error> {
        self.client
            .send(tonic::Request::new(data.into()))
            .await
            .map_err(GrpcError::from)?;

        Ok(())
    }

    async fn send_object(&mut self, data: ValidatedObject) -> Result<(), crate::Error> {
        self.client
            .send(tonic::Request::new(data.into()))
            .await
            .map_err(GrpcError::from)?;

        Ok(())
    }

    async fn send_individual_stored(
        &mut self,
        id: RetentionId,
        data: ValidatedIndividual,
    ) -> Result<(), crate::Error> {
        let value = AstarteMessage::from(data);

        self.client
            .send(tonic::Request::new(value))
            .await
            .map_err(|e| Error::Grpc(GrpcError::from(e)))?;

        self.mark_received(&id).await?;

        Ok(())
    }

    async fn send_object_stored(
        &mut self,
        id: RetentionId,
        data: ValidatedObject,
    ) -> Result<(), crate::Error> {
        let value = AstarteMessage::from(data);

        self.client
            .send(tonic::Request::new(value))
            .await
            .map_err(|e| Error::Grpc(GrpcError::from(e)))?;

        self.mark_received(&id).await?;

        Ok(())
    }

    async fn resend_stored(
        &mut self,
        id: RetentionId,
        data: PublishInfo<'_>,
    ) -> Result<(), crate::Error> {
        let msg = AstarteMessage::decode(data.value.borrow()).map_err(GrpcError::Decode)?;

        self.client
            .send(tonic::Request::new(msg))
            .await
            .map_err(GrpcError::from)?;

        self.mark_received(&id).await?;

        Ok(())
    }

    async fn unset(&mut self, data: ValidatedUnset) -> Result<(), crate::Error> {
        self.client
            .send(tonic::Request::new(data.into()))
            .await
            .map(|_| ())
            .map_err(|e| GrpcError::from(e).into())
    }

    fn serialize_individual(&self, data: &ValidatedIndividual) -> Result<Vec<u8>, crate::Error> {
        Ok(AstarteMessage::from(data.clone()).encode_to_vec())
    }

    fn serialize_object(&self, data: &ValidatedObject) -> Result<Vec<u8>, crate::Error> {
        Ok(AstarteMessage::from(data.clone()).encode_to_vec())
    }
}

impl<S> Register for GrpcClient<S>
where
    S: Send,
{
    async fn add_interface(
        &mut self,
        _interfaces: &Interfaces,
        added: &interfaces::Validated,
    ) -> Result<(), crate::Error> {
        let interfaces_json = InterfacesJson::try_from_iter([added.deref()])
            .map_err(|err| Error::Grpc(GrpcError::InterfacesSerialization(err)))?;

        self.client
            .add_interfaces(tonic::Request::new(interfaces_json))
            .await
            .map(|_| ())
            .map_err(|s| crate::Error::Grpc(GrpcError::Status(s)))
    }

    async fn extend_interfaces(
        &mut self,
        _interfaces: &Interfaces,
        added: &interfaces::ValidatedCollection,
    ) -> Result<(), crate::Error> {
        let interfaces_json = InterfacesJson::try_from_iter(added.iter_interfaces())
            .map_err(|err| Error::Grpc(GrpcError::InterfacesSerialization(err)))?;

        self.client
            .add_interfaces(tonic::Request::new(interfaces_json))
            .await
            .map(|_| ())
            .map_err(|s| crate::Error::Grpc(GrpcError::Status(s)))
    }

    async fn remove_interface(
        &mut self,
        _interfaces: &Interfaces,
        removed: &Interface,
    ) -> Result<(), crate::Error> {
        let interfaces_name = InterfacesName {
            names: vec![removed.interface_name().to_string()],
        };

        self.client
            .remove_interfaces(tonic::Request::new(interfaces_name))
            .await
            .map(|_| ())
            .map_err(|s| crate::Error::Grpc(GrpcError::Status(s)))
    }

    async fn remove_interfaces(
        &mut self,
        _interfaces: &Interfaces,
        removed_interfaces: &HashMap<&str, &Interface>,
    ) -> Result<(), Error> {
        let interfaces_name = removed_interfaces
            .iter()
            .map(|(iface_name, _iface)| iface_name.to_string())
            .collect();

        let interfaces_name = InterfacesName {
            names: interfaces_name,
        };

        self.client
            .remove_interfaces(tonic::Request::new(interfaces_name))
            .await
            .map(|_| ())
            .map_err(|s| crate::Error::Grpc(GrpcError::Status(s)))
    }
}

impl<S> Disconnect for GrpcClient<S>
where
    S: Send,
{
    async fn disconnect(&mut self) -> Result<(), crate::Error> {
        self.detach().await.map_err(Error::Grpc)?;

        Ok(())
    }
}

/// Handles a gRPC connection between the device and Astarte.
///
/// It manages the interaction with the
/// [astarte-message-hub](https://github.com/astarte-platform/astarte-message-hub), sending and
/// receiving [`AstarteMessage`] following the Astarte message hub protocol.
pub struct Grpc<S> {
    uuid: Uuid,
    client: MsgHubClient,
    stream: SyncWrapper<Streaming<MessageHubEvent>>,
    /// Store used in the client
    _store: PhantomData<S>,
}

impl<S> Grpc<S> {
    pub(crate) fn new(
        uuid: Uuid,
        client: MsgHubClient,
        stream: Streaming<MessageHubEvent>,
    ) -> Self {
        Self {
            uuid,
            client,
            stream: SyncWrapper::new(stream),
            _store: PhantomData,
        }
    }

    /// Polls a message from the tonic stream and tries reattaching if necessary
    ///
    /// An [`Option`] is returned directly from the [`tonic::codec::Streaming::message`] method.
    /// A result of [`None`] signals a disconnection and should be handled by the caller
    async fn next_message(&mut self) -> Result<Option<MessageHubEvent>, tonic::Status> {
        self.stream.get_mut().message().await
    }

    async fn attach(
        client: &mut MsgHubClient,
        data: NodeData,
    ) -> Result<Streaming<MessageHubEvent>, GrpcError> {
        client
            .attach(tonic::Request::new(data.node))
            .await
            .map(|r| r.into_inner())
            .map_err(GrpcError::from)
    }
}

impl<S> std::fmt::Debug for Grpc<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grpc")
            .field("uuid", &self.uuid)
            .finish_non_exhaustive()
    }
}

impl<T> Receive for Grpc<T>
where
    T: Send,
{
    type Payload = GrpcPayload;

    async fn next_event(&mut self) -> Result<Option<ReceivedEvent<Self::Payload>>, TransportError> {
        match self.next_message().await {
            Ok(Some(message)) => {
                let event = message.try_into().map_err(RecvError::connection)?;

                Ok(Some(event))
            }
            Err(s) => {
                error!(status = %s, "error returned by the server");

                Ok(None)
            }
            Ok(None) => {
                warn!("Stream closed");

                Ok(None)
            }
        }
    }

    fn deserialize_individual(
        &self,
        mapping: &MappingRef<'_, &Interface>,
        payload: Self::Payload,
    ) -> Result<Option<(AstarteType, Option<Timestamp>)>, TransportError> {
        let Self::Payload { data, timestamp } = payload;

        let value: Value = data.try_into().map_err(RecvError::connection)?;

        let astarte_type = match value {
            Value::Individual(astarte_type) => Ok(astarte_type),
            Value::Object(_hash_map) => {
                let aggr_err = AggregateError::for_payload(
                    mapping.interface().interface_name(),
                    mapping.path().to_string(),
                    Aggregation::Individual,
                    Aggregation::Object,
                );
                Err(RecvError::Aggregation(aggr_err))
            }
            Value::Unset => {
                debug!("unset received");
                return Ok(None);
            }
        }?;

        trace!("received {}", astarte_type.display_type());

        Ok(Some((astarte_type, timestamp)))
    }

    fn deserialize_object(
        &self,
        object: &ObjectRef,
        path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), TransportError> {
        let Self::Payload { data, timestamp } = payload;

        let value: Value = data.try_into().map_err(RecvError::connection)?;

        let object = match value {
            Value::Object(hash_map) => Ok(hash_map),
            Value::Individual(_) | Value::Unset => {
                Err(RecvError::Aggregation(AggregateError::for_payload(
                    object.interface.interface_name(),
                    path.to_string(),
                    Aggregation::Object,
                    Aggregation::Individual,
                )))
            }
        }?;

        trace!("object received");

        Ok((object, timestamp))
    }
}

impl<S> Reconnect for Grpc<S>
where
    S: StoreCapabilities + Send + Sync,
{
    async fn reconnect(&mut self, interfaces: &Interfaces) -> Result<(), crate::Error> {
        // try reattaching
        let data = NodeData::try_from(interfaces)?;

        let mut exp_back = ExponentialIter::default();

        let stream = loop {
            match Grpc::<S>::attach(&mut self.client, data.clone()).await {
                Ok(stream) => break stream,
                Err(err) => {
                    error!("Grpc error while trying to reconnect {err}");

                    let timeout = exp_back.next();

                    debug!("waiting {timeout} seconds before retrying");

                    tokio::time::sleep(Duration::from_secs(timeout)).await;
                }
            };
        };

        self.stream = SyncWrapper::new(stream);

        Ok(())
    }
}

impl<S> Connection for Grpc<S>
where
    S: PropertyStore,
{
    type Sender = GrpcClient<GrpcStore>;
}

/// Internal struct holding the received grpc message
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GrpcPayload {
    data: ProtoPayload,
    timestamp: Option<Timestamp>,
}

impl GrpcPayload {
    pub(crate) fn new(data: ProtoPayload, timestamp: Option<Timestamp>) -> Self {
        Self { data, timestamp }
    }
}

/// Configuration for the mqtt connection
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    uuid: Uuid,
    endpoint: Endpoint,
}

impl GrpcConfig {
    /// Create a new config.
    pub const fn new(uuid: Uuid, endpoint: Endpoint) -> Self {
        Self { uuid, endpoint }
    }

    /// Create a new config from node id and Message Hub endpoint.
    pub fn from_url(uuid: Uuid, url: impl Into<Bytes>) -> Result<Self, GrpcError> {
        let endpoint = Endpoint::from_shared(url)?;

        Ok(Self::new(uuid, endpoint))
    }

    /// Returns a mutable reference to configure the endpoint.
    pub fn endpoint_mut(&mut self) -> &mut Endpoint {
        &mut self.endpoint
    }
}

impl<S> ConnectionConfig<S> for GrpcConfig
where
    S: StoreCapabilities + PropertyStore + Send + Sync,
{
    type Store = GrpcStore;
    type Conn = Grpc<GrpcStore>;
    type Err = GrpcError;

    async fn connect(
        self,
        config: ConnectionBuildConfig<'_, S>,
    ) -> Result<DeviceTransport<Self::Store, Self::Conn>, Self::Err> {
        let ConnectionBuildConfig {
            interfaces, config, ..
        } = config;

        let channel = self.endpoint.connect().await.map_err(GrpcError::from)?;

        let node_id_interceptor = NodeIdInterceptor::new(self.uuid);

        let mut client = MessageHubClient::with_interceptor(channel, node_id_interceptor);

        let node_data = NodeData::try_from(interfaces)?;
        let stream = Grpc::<StoreWrapper<S>>::attach(&mut client, node_data).await?;

        let grpc_store = StoreWrapper::new(GrpcStore::new(client.clone()));

        let sender = GrpcClient::new(client.clone(), grpc_store.clone(), config.volatile.clone());
        let receiver = Grpc::new(self.uuid, client, stream);

        Ok(DeviceTransport {
            sender,
            connection: receiver,
            store: grpc_store,
        })
    }

    fn volatile_capacity_override() -> Option<usize> {
        // disable retention for the store
        Some(0)
    }
}

/// Wrapper that contains data needed while connecting the node to the astarte message hub.
#[derive(Debug, Clone)]
struct NodeData {
    node: Node,
}

impl NodeData {
    fn try_from_iter<'a, I>(interfaces: I) -> Result<Self, GrpcError>
    where
        I: IntoIterator<Item = &'a Interface>,
    {
        let node = Node::from_interfaces(interfaces)?;

        Ok(Self { node })
    }
}

impl<'a> TryFrom<&'a Interfaces> for NodeData {
    type Error = GrpcError;

    fn try_from(value: &'a Interfaces) -> Result<Self, Self::Error> {
        Self::try_from_iter(value.iter())
    }
}

#[cfg(test)]
mod test {
    use std::{future::Future, net::SocketAddr, str::FromStr};

    use astarte_message_hub_proto::{
        message_hub_server::{MessageHub, MessageHubServer},
        AstarteMessage, Property, PropertyIdentifier, StoredProperties, StoredPropertiesFilter,
    };
    use async_trait::async_trait;
    use tokio::{
        net::TcpListener,
        sync::{mpsc, Mutex},
    };
    use uuid::uuid;

    use crate::{
        builder::DEFAULT_VOLATILE_CAPACITY,
        store::memory::MemoryStore,
        transport::{
            test::{mock_validate_individual, mock_validate_object},
            ReceivedEvent,
        },
        AstarteAggregate, DeviceEvent, Value,
    };

    use super::*;

    pub(crate) const ID: Uuid = uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8");

    #[derive(Debug)]
    pub(crate) enum ServerReceivedRequest {
        Attach((Uuid, Node)),
        Send(AstarteMessage),
        Detach(Empty),
        AddInterfaces(InterfacesJson),
        RemoveInterfaces(InterfacesName),
        GetProperties(InterfacesName),
        GetAllProperties(StoredPropertiesFilter),
        GetProperty(PropertyIdentifier),
    }

    type ServerSenderValuesVec = Vec<Result<MessageHubEvent, tonic::Status>>;

    fn get_node_id_from_metadata(request: &tonic::Request<Node>) -> Result<Uuid, tonic::Status> {
        // check only node-id-bin since the Node does not insert node-id metadata in the request
        let Some(metadata_val) = request.metadata().get_bin("node-id-bin") else {
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                "absent node id into metadata".to_string(),
            ));
        };

        let node_id_bytes = metadata_val
            .to_bytes()
            .map_err(|e| Status::new(tonic::Code::InvalidArgument, e.to_string()))?;

        Uuid::from_slice(&node_id_bytes)
            .map_err(|e| Status::new(tonic::Code::InvalidArgument, e.to_string()))
    }

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
            server_send: mpsc::Receiver<Vec<Result<MessageHubEvent, tonic::Status>>>,
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
            futures::stream::Iter<std::vec::IntoIter<Result<MessageHubEvent, tonic::Status>>>;

        async fn attach(
            &self,
            request: tonic::Request<Node>,
        ) -> Result<tonic::Response<Self::AttachStream>, tonic::Status> {
            // retrieve the node id from the metadata request
            let node_id = get_node_id_from_metadata(&request)?;

            let inner = request.into_inner();
            println!("Client '{node_id}' attached");

            self.server_received.send(ServerReceivedRequest::Attach((node_id, inner))).await
                .expect("Could not send notification of a server received message, connect a channel to the Receiver");

            let mut receiver_lock = self.server_send.lock().await;

            let response_vec = receiver_lock.recv().await.unwrap();

            Ok(tonic::Response::new(futures::stream::iter(response_vec)))
        }

        async fn send(
            &self,
            request: tonic::Request<AstarteMessage>,
        ) -> Result<tonic::Response<Empty>, tonic::Status> {
            self.server_received.send(ServerReceivedRequest::Send(request.into_inner())).await
                .expect("Could not send notification of a server received message, connect a channel to the Receiver");

            Ok(tonic::Response::new(Empty::default()))
        }

        async fn detach(
            &self,
            _request: tonic::Request<Empty>,
        ) -> Result<tonic::Response<Empty>, tonic::Status> {
            println!("Client detached");

            self.server_received.send(ServerReceivedRequest::Detach(Empty{})).await
                .expect("Could not send notification of a server received message, connect a channel to the Receiver");

            Ok(tonic::Response::new(Empty::default()))
        }

        async fn add_interfaces(
            &self,
            request: tonic::Request<InterfacesJson>,
        ) -> Result<tonic::Response<Empty>, Status> {
            self.server_received.send(ServerReceivedRequest::AddInterfaces(request.into_inner())).await.expect("Could not send notification of a server received message, connect a channel to the Receiver");

            Ok(tonic::Response::new(Empty::default()))
        }

        async fn remove_interfaces(
            &self,
            request: tonic::Request<InterfacesName>,
        ) -> Result<tonic::Response<Empty>, Status> {
            self.server_received.send(ServerReceivedRequest::RemoveInterfaces(request.into_inner())).await.expect("Could not send notification of a server received message, connect a channel to the Receiver");

            Ok(tonic::Response::new(Empty::default()))
        }

        async fn get_properties(
            &self,
            request: Request<InterfacesName>,
        ) -> Result<tonic::Response<StoredProperties>, Status> {
            self.server_received
                .send(ServerReceivedRequest::GetProperties(request.into_inner()))
                .await.expect("Could not send notification of a server received message, connect a channel to the Receiver");

            // return no properties
            Ok(tonic::Response::new(StoredProperties {
                interface_properties: HashMap::new(),
            }))
        }

        async fn get_all_properties(
            &self,
            request: Request<StoredPropertiesFilter>,
        ) -> Result<tonic::Response<StoredProperties>, Status> {
            self.server_received
                .send(ServerReceivedRequest::GetAllProperties(
                    request.into_inner(),
                ))
                .await.expect("Could not send notification of a server received message, connect a channel to the Receiver");

            // return no properties
            Ok(tonic::Response::new(StoredProperties {
                interface_properties: HashMap::new(),
            }))
        }

        async fn get_property(
            &self,
            request: Request<PropertyIdentifier>,
        ) -> Result<tonic::Response<Property>, Status> {
            self.server_received
                .send(ServerReceivedRequest::GetProperty(request.into_inner()))
                .await.expect("Could not send notification of a server received message, connect a channel to the Receiver");

            // return no properties
            Ok(tonic::Response::new(Property {
                path: "".to_owned(),
                data: None,
            }))
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
    ) -> impl Future<Output = MsgHubClient> {
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

    pub(crate) async fn mock_grpc_actors(
        server_impl: TestMessageHubServer,
    ) -> Result<
        (impl Future<Output = ()>, impl Future<Output = MsgHubClient>),
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

    pub(crate) async fn mock_astarte_grpc_client<S>(
        mut message_hub_client: MsgHubClient,
        interfaces: &Interfaces,
        store: S,
    ) -> Result<(GrpcClient<S>, Grpc<S>), Box<dyn std::error::Error>>
    where
        S: PropertyStore,
    {
        let node_data = NodeData::try_from(interfaces)?;
        let stream = Grpc::<S>::attach(&mut message_hub_client, node_data).await?;

        let client = GrpcClient::new(
            message_hub_client.clone(),
            StoreWrapper::new(store),
            SharedVolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY),
        );
        let grcp = Grpc::new(ID, message_hub_client, stream);

        Ok((client, grcp))
    }

    pub(crate) struct TestServerChannels {
        pub(crate) server_response_sender:
            mpsc::Sender<Vec<Result<MessageHubEvent, tonic::Status>>>,
        pub(crate) server_request_receiver: mpsc::Receiver<ServerReceivedRequest>,
    }

    pub(crate) fn build_test_message_hub_server() -> (TestMessageHubServer, TestServerChannels) {
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
        ($poll_result_fn:expr; $($pattern:pat $($(=> $var:ident = $expr_value:expr;)? $(if $guard:expr)?),*),+) => {{
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

                                    $(if !($guard) {
                                        panic!("The message n.{} didn't pass the guard '{}'", i, stringify!($guard));
                                    })?
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

    pub(crate) use expect_messages;

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
            let (mut client, _connection) =
                mock_astarte_grpc_client(client, &Interfaces::new(), MemoryStore::new())
                    .await
                    .unwrap();

            // manually calling detach
            client.disconnect().await.unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach((id, _)) if id == ID,
            ServerReceivedRequest::Detach(_),
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
            let (mut client, mut connection) =
                mock_astarte_grpc_client(client, &Interfaces::new(), MemoryStore::new())
                    .await
                    .unwrap();

            // poll the three messages the first two received errors will simply reconnect without returning
            assert!(matches!(connection.next_event().await, Ok(None)));

            // to reconnect but errors
            assert!(matches!(
                connection.reconnect(&Interfaces::new()).await,
                Ok(())
            ));

            // second error
            assert!(matches!(connection.next_event().await, Ok(None)));

            assert!(matches!(
                connection.reconnect(&Interfaces::new()).await,
                Ok(())
            ));

            // third error
            assert!(matches!(connection.next_event().await, Ok(None)));

            // manually calling detach
            client.disconnect().await.unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            // connection creation attach
            ServerReceivedRequest::Attach((id, _)) if id == ID,
            // first error attach
            ServerReceivedRequest::Attach((id, _)) if id == ID,
            // second error attach
            ServerReceivedRequest::Attach((id, _)) if id == ID,
            ServerReceivedRequest::Detach(_),
        );
    }

    #[tokio::test]
    async fn test_add_remove_interface() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        channels.server_response_sender.send(vec![]).await.unwrap();

        let client_operations = async move {
            let client = client_future.await;
            // When the grpc connection gets created the attach methods is called
            let (mut client, _connection) =
                mock_astarte_grpc_client(client, &Interfaces::new(), MemoryStore::new())
                    .await
                    .unwrap();

            let interfaces = Interfaces::new();

            let interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();
            let validated = interfaces.validate(interface.clone()).unwrap().unwrap();

            client.add_interface(&interfaces, &validated).await.unwrap();

            client
                .remove_interface(&interfaces, &validated)
                .await
                .unwrap();

            let additional_interface =
                Interface::from_str(crate::test::E2E_DEVICE_PROPERTY).unwrap();
            let to_add = Interfaces::new()
                .validate_many([interface.clone(), additional_interface.clone()])
                .unwrap();

            client
                .extend_interfaces(&interfaces, &to_add)
                .await
                .unwrap();

            let to_remove = HashMap::from([
                (interface.interface_name(), &interface),
                (additional_interface.interface_name(), &additional_interface),
            ]);

            client
                .remove_interfaces(&interfaces, &to_remove)
                .await
                .unwrap();

            // manually calling detach
            client.disconnect().await.unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        let interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();
        let additional_interface = Interface::from_str(crate::test::E2E_DEVICE_PROPERTY).unwrap();

        let mut expect_added = [&additional_interface, &interface]
            .map(|i| serde_json::to_string(i).unwrap())
            .to_vec();
        expect_added.sort();

        let mut expect_removed = vec![
            additional_interface.interface_name().to_string(),
            interface.interface_name().to_string(),
        ];
        expect_removed.sort();

        expect_messages!(channels.server_request_receiver.try_recv();
            // connection creation attach
            ServerReceivedRequest::Attach((id, _)) if id == ID,
            // add interface
            ServerReceivedRequest::AddInterfaces(i) if i.interfaces_json == vec![serde_json::to_string(&interface).unwrap()],
            // remove interface
            ServerReceivedRequest::RemoveInterfaces(i) if i.names == vec![interface.interface_name().to_string()],
            // add more interfaces
            ServerReceivedRequest::AddInterfaces(mut i)
            => ordered = {i.interfaces_json.sort(); i.interfaces_json} ;
                if ordered == expect_added,
            // remove more interfaces
            ServerReceivedRequest::RemoveInterfaces(mut i)
            => ordered = {i.names.sort(); i.names} ;
                if ordered == expect_removed,
            // detach
            ServerReceivedRequest::Detach(Empty {}),
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

            let (mut client, _connection) =
                mock_astarte_grpc_client(client, &interfaces, MemoryStore::new())
                    .await
                    .unwrap();

            let validated_individual = mock_validate_individual(
                mapping_ref,
                &path,
                AstarteType::String(STRING_VALUE.to_string()),
                None,
            )
            .unwrap();

            client.send_individual(validated_individual).await.unwrap();

            client.disconnect().await.unwrap();
        };

        // Poll client and server future
        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach((id, _)) if id == ID,
            ServerReceivedRequest::Send(m)
            => data_event = DeviceEvent::try_from(m).expect("Malformed message");
                if data_event.interface == "org.astarte-platform.rust.examples.individual-properties.DeviceProperties"
                && data_event.path == "/1/name"
                && matches!(data_event.data, Value::Individual(AstarteType::String(v)) if v == STRING_VALUE),
            ServerReceivedRequest::Detach(_),
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

            let (mut client, _connection) =
                mock_astarte_grpc_client(client, &interfaces, MemoryStore::new())
                    .await
                    .unwrap();

            let validated_object = mock_validate_object(
                &interface,
                &path,
                MockObject {},
                Some(chrono::offset::Utc::now()),
            )
            .unwrap();

            client.send_object(validated_object).await.unwrap()
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach((id, _)) if id == ID,
            ServerReceivedRequest::Send(m)
            => data_event = DeviceEvent::try_from(m).expect("Malformed message");
                if data_event.interface == "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream"
                    && data_event.path == "/1",
            => object_value = {  let Value::Object(v) = data_event.data else { panic!("Expected object") }; v };
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

        let expected_object = Value::Object((MockObject {}).astarte_aggregate().unwrap());

        let proto_payload: astarte_message_hub_proto::astarte_message::Payload =
            expected_object.into();

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
            .send(vec![Ok(astarte_message.into())])
            .await
            .unwrap();

        let interfaces =
            Interfaces::from_iter([
                Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap()
            ]);

        let client_connection = async {
            let client = client_future.await;

            mock_astarte_grpc_client(client, &interfaces, MemoryStore::new()).await
        };

        let (_client, mut connection) = tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            res = client_connection => {
                println!("Client connected correctly: {}", res.is_ok());

                res.expect("Expected correct connection in test")
            },
        };

        expect_messages!(connection.next_event().await;
            Some(ReceivedEvent {
                ref interface,
                ref path,
                payload: GrpcPayload {
                    data,
                    timestamp: None,
                },
            }) if interface == "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream"
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

        let exp_interface =
            "org.astarte-platform.rust.examples.individual-properties.ServerProperties";
        let exp_path = "/1/enable";
        let proto_payload: ProtoPayload = Value::Unset.into();
        let astarte_message = super::convert::test::new_astarte_message(
            exp_interface.to_string(),
            exp_path.to_string(),
            None,
            proto_payload.clone(),
        );

        // Send object from server
        channels
            .server_response_sender
            .send(vec![Ok(astarte_message.into())])
            .await
            .unwrap();

        let interfaces =
            Interfaces::from_iter([Interface::from_str(crate::test::SERVER_PROPERTIES).unwrap()]);

        let client_connection = async {
            let client = client_future.await;

            mock_astarte_grpc_client(client, &interfaces, MemoryStore::new()).await
        };

        let (_client, mut connection) = tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            res = client_connection => {
                println!("Client connected correctly: {}", res.is_ok());

                res.expect("Expected correct connection in test")
            },
        };

        assert!(
            matches!(connection.next_event().await, Ok(Some(ReceivedEvent {
                ref interface,
                ref path,
                payload: GrpcPayload {
                    data,
                    timestamp: None,
                },
            })) if interface == exp_interface
                && path == exp_path
                && data == proto_payload)
        )
    }

    #[test]
    fn create_config() {
        let uuid = Uuid::new_v4();

        let mut config = GrpcConfig::from_url(uuid, "http://hub.example.com").unwrap();

        assert_eq!(config.endpoint_mut().uri().host(), Some("hub.example.com"));
    }
}
