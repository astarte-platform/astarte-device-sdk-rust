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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use astarte_message_hub_proto::prost::{DecodeError, Message};
use astarte_message_hub_proto::tonic::codegen::InterceptedService;
use astarte_message_hub_proto::tonic::metadata::MetadataValue;
use astarte_message_hub_proto::tonic::service::Interceptor;
use astarte_message_hub_proto::tonic::transport::{Channel, Endpoint};
use astarte_message_hub_proto::tonic::{Request, Status};
use astarte_message_hub_proto::{
    astarte_message::Payload as ProtoPayload, pbjson_types::Empty, AstarteMessage, InterfacesJson,
    InterfacesName, MessageHubError, MessageHubEvent, Node,
};
use astarte_message_hub_proto::{PropertyIdentifier, StoredPropertiesFilter};
use bytes::Bytes;
use itertools::Itertools;
use sync_wrapper::SyncWrapper;
use tokio::sync::Mutex;
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use self::convert::MessageHubProtoError;
use super::{
    Connection, Disconnect, Publish, Receive, ReceivedEvent, Reconnect, Register, TransportError,
    WrapStore,
};
use crate::client::RecvError;
use crate::error::AggregateError;
use crate::interface::Aggregation;
use crate::retention::memory::SharedVolatileStore;
use crate::retention::{PublishInfo, RetentionId};
use crate::store::{OptStoredProp, StoreInterfaceData, StoredProp};
use crate::{
    builder::{ConnectionConfig, DeviceBuilder, DeviceTransport},
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
    },
    interfaces::{self, Interfaces},
    retention::StoredRetention,
    retry::ExponentialIter,
    store::{wrapper::StoreWrapper, PropertyStore, StoreCapabilities},
    transport::grpc::convert::map_values_to_astarte_type,
    types::AstarteType,
    validate::{ValidatedIndividual, ValidatedObject, ValidatedUnset},
    Error, Interface, Timestamp,
};

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

#[cfg(not(test))]
type MessageHubClient<T> = astarte_message_hub_proto::message_hub_client::MessageHubClient<T>;
#[cfg(test)]
type MessageHubClient<T> = astarte_message_hub_proto::mock::MockMessageHubClient<T>;

#[cfg(not(test))]
type Streaming<T> = astarte_message_hub_proto::tonic::codec::Streaming<T>;
#[cfg(test)]
type Streaming<T> = astarte_message_hub_proto::mock::MockStreaming<T>;

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
        let data = match payload.data {
            ProtoPayload::AstarteData(data) => data,
            ProtoPayload::AstarteUnset(astarte_message_hub_proto::AstarteUnset {}) => {
                debug!("unset received");
                return Ok(None);
            }
        };

        let individual = data.take_individual().ok_or_else(|| {
            let aggr_err = AggregateError::for_payload(
                mapping.interface().interface_name(),
                mapping.path().to_string(),
                Aggregation::Individual,
                Aggregation::Object,
            );
            TransportError::Recv(RecvError::Aggregation(aggr_err))
        })?;

        let data = AstarteType::try_from(individual).map_err(RecvError::connection)?;

        trace!("received {}", data.display_type());

        Ok(Some((data, payload.timestamp)))
    }

    fn deserialize_object(
        &self,
        object: &ObjectRef,
        path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), TransportError> {
        let object = payload
            .data
            .take_data()
            .and_then(|d| d.take_object())
            .ok_or_else(|| {
                RecvError::Aggregation(AggregateError::for_payload(
                    object.interface.interface_name(),
                    path.to_string(),
                    Aggregation::Object,
                    Aggregation::Individual,
                ))
            })?;

        let data = map_values_to_astarte_type(object).map_err(RecvError::connection)?;

        trace!("object received");

        Ok((data, payload.timestamp))
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
    type Sender = GrpcClient<S>;
}

impl<S> WrapStore<S> for Grpc<S>
where
    S: PropertyStore,
{
    type Store = GrpcStore<S>;

    fn wrap_store(store: S, sender: &Self::Sender) -> Self::Store {
        GrpcStore::new(store, sender.client.clone())
    }
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
    type Conn = Grpc<S>;
    type Err = Error;

    async fn connect<C>(
        self,
        builder: &DeviceBuilder<S, C>,
    ) -> Result<DeviceTransport<Grpc<S>>, Self::Err>
    where
        C: Send + Sync,
    {
        let channel = self.endpoint.connect().await.map_err(GrpcError::from)?;
        let node_id_interceptor = NodeIdInterceptor::new(self.uuid);
        let mut client = MessageHubClient::with_interceptor(channel, node_id_interceptor);

        let node_data = NodeData::try_from(&builder.interfaces)?;
        let stream = Grpc::<StoreWrapper<S>>::attach(&mut client, node_data).await?;

        let sender = GrpcClient::new(
            client.clone(),
            StoreWrapper::new(builder.store.clone()),
            builder.volatile.clone(),
        );
        let receiver = Grpc::new(self.uuid, client, stream);

        Ok(DeviceTransport {
            sender,
            connection: receiver,
        })
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

/// Store wrapper designed specifically for the grpc connection
/// Used mainly to request device owned properties to the message hub instead of looking them up in the local storage
#[derive(Clone)]
pub struct GrpcStore<S> {
    pub(crate) store: S,
    pub(crate) client: Arc<Mutex<MsgHubClient>>,
}

impl<S> GrpcStore<S> {
    pub(crate) fn new(store: S, client: MsgHubClient) -> Self {
        Self {
            store,
            client: Arc::new(Mutex::new(client)),
        }
    }

    async fn load_device_properties(&self) -> Result<Vec<StoredProp>, GrpcStoreError<S::Err>>
    where
        S: PropertyStore,
    {
        self.client
            .lock()
            .await
            .get_all_properties(StoredPropertiesFilter {
                ownership: Some(astarte_message_hub_proto::Ownership::Device.into()),
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(GrpcStoreError::from_status)
            .and_then(|p| {
                convert::map_set_stored_properties(p).map_err(GrpcStoreError::from_conversion)
            })
    }
}

// manual impl to avoid the debug constraint on contained elements
impl<T> std::fmt::Debug for GrpcStore<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcStore")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

/// Error returned while operating on the store of a grpc connection
/// This store needs to request properties from the message hub server
/// and has additional errors consequently
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum GrpcStoreError<E> {
    /// Underlying store error
    #[error("Underlying store error: {0}")]
    StoreError(E),
    /// Error while retrieving data from the message hub server
    #[error("Error while retrieving data from the message hub server: {0}")]
    StatusError(tonic::Status),
    /// Error while converting a proto received value to an internal type
    #[error("Error while converting a proto received value to an internal type: {0}")]
    ConversionError(MessageHubProtoError),
}

impl<E> GrpcStoreError<E> {
    fn from_err(err: E) -> Self {
        Self::StoreError(err)
    }

    fn from_status(status: tonic::Status) -> Self {
        Self::StatusError(status)
    }

    fn from_conversion(conversion: MessageHubProtoError) -> Self {
        Self::ConversionError(conversion)
    }
}

impl<S> StoreCapabilities for GrpcStore<S>
where
    S: StoreCapabilities,
{
    type Retention = S::Retention;

    fn get_retention(&self) -> Option<&Self::Retention> {
        self.store.get_retention()
    }
}

/// We implement the PropertyStore to override the behavior when retrieving or storing
/// device owned properies. We only want to store and load from the store server owned properties.
/// Device owned properties won't be stored and will be requested to the message hub server.
impl<S> PropertyStore for GrpcStore<S>
where
    S: PropertyStore,
    S::Err: Display,
{
    type Err = GrpcStoreError<S::Err>;

    async fn store_prop(&self, prop: StoredProp<&str, &AstarteType>) -> Result<(), Self::Err> {
        if prop.ownership.is_device() {
            // do not store device owned properties
            return Ok(());
        }

        self.store
            .store_prop(prop)
            .await
            .map_err(GrpcStoreError::from_err)
    }

    async fn load_prop<I>(
        &self,
        interface: &StoreInterfaceData<I>,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, Self::Err>
    where
        I: AsRef<str> + Send + Sync,
    {
        if interface.ownership.is_server() {
            return self
                .store
                .load_prop(interface, path, interface_major)
                .await
                .map_err(GrpcStoreError::from_err);
        }

        let property = self
            .client
            .lock()
            .await
            .get_property(PropertyIdentifier {
                interface_name: interface.name.as_ref().to_owned(),
                path: path.to_owned(),
            })
            .await
            .map_err(GrpcStoreError::from_status)
            .map(tonic::Response::into_inner)?;

        convert::map_property_to_astarte_type(property).map_err(GrpcStoreError::from_conversion)
    }

    async fn unset_prop<I>(
        &self,
        interface: &StoreInterfaceData<I>,
        path: &str,
    ) -> Result<(), Self::Err>
    where
        I: AsRef<str> + Send + Sync,
    {
        if interface.ownership.is_device() {
            // we won't store a device unset since we always request the properties to the server
            return Ok(());
        }

        self.store
            .unset_prop(interface, path)
            .await
            .map_err(GrpcStoreError::from_err)
    }

    async fn delete_prop<I>(
        &self,
        interface: &StoreInterfaceData<I>,
        path: &str,
    ) -> Result<(), Self::Err>
    where
        I: AsRef<str> + Send + Sync,
    {
        if interface.ownership.is_device() {
            // we won't delete a device property since we always request the properties to the server
            return Ok(());
        }

        self.store
            .delete_prop(interface, path)
            .await
            .map_err(GrpcStoreError::from_err)
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        self.store.clear().await.map_err(GrpcStoreError::from_err)
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let load_server = async {
            self.store
                .server_props()
                .await
                .map_err(GrpcStoreError::from_err)
        };

        let (server_result, device_result) =
            futures::future::join(load_server, self.load_device_properties()).await;

        let merged_properties = server_result?
            .into_iter()
            .chain(device_result?.into_iter())
            .collect_vec();

        Ok(merged_properties)
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.store
            .server_props()
            .await
            .map_err(GrpcStoreError::from_err)
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.load_device_properties().await
    }

    async fn interface_props<I>(
        &self,
        interface: &StoreInterfaceData<I>,
    ) -> Result<Vec<StoredProp>, Self::Err>
    where
        I: AsRef<str> + Send + Sync,
    {
        if interface.ownership.is_server() {
            return self
                .store
                .interface_props(interface)
                .await
                .map_err(GrpcStoreError::from_err);
        }

        self.client
            .lock()
            .await
            .get_properties(InterfacesName {
                names: vec![interface.name.as_ref().to_owned()],
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(GrpcStoreError::from_status)
            .and_then(|p| {
                convert::map_set_stored_properties(p).map_err(GrpcStoreError::from_conversion)
            })
    }

    async fn delete_interface<I>(&self, interface: &StoreInterfaceData<I>) -> Result<(), Self::Err>
    where
        I: AsRef<str> + Send + Sync,
    {
        if interface.ownership.is_device() {
            // we don't delete a device interface locally
            return Ok(());
        }

        self.store
            .delete_interface(interface)
            .await
            .map_err(GrpcStoreError::from_err)
    }

    async fn device_props_with_unset(&self) -> Result<Vec<OptStoredProp>, Self::Err> {
        // unused for grpc connection
        Ok(vec![])
    }
}

#[cfg(test)]
mod test {
    use astarte_message_hub_proto::{mockall::Sequence, pbjson_types, tonic};
    use uuid::uuid;

    use std::str::FromStr;

    use crate::{
        builder::DEFAULT_VOLATILE_CAPACITY, interface::Ownership, store::memory::MemoryStore,
        AstarteAggregate, DeviceEvent, Value,
    };

    use super::*;

    const ID: Uuid = uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8");

    struct MockDeviceObject {}

    impl AstarteAggregate for MockDeviceObject {
        fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, crate::error::Error> {
            let mut obj = HashMap::new();
            obj.insert("endpoint1".to_string(), AstarteType::Double(4.2));
            obj.insert(
                "endpoint2".to_string(),
                AstarteType::String("obj".to_string()),
            );
            obj.insert(
                "endpoint3".to_string(),
                AstarteType::BooleanArray(vec![true, false, true]),
            );

            Ok(obj)
        }
    }

    struct MockServerObject {}

    impl AstarteAggregate for MockServerObject {
        fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error> {
            let mut obj = HashMap::new();
            obj.insert("double_endpoint".to_string(), AstarteType::Double(4.2));
            obj.insert(
                "string_endpoint".to_string(),
                AstarteType::String("obj".to_string()),
            );
            obj.insert(
                "boleanarray_endpoint".to_string(),
                AstarteType::BooleanArray(vec![true, false, true]),
            );

            Ok(obj)
        }
    }

    trait InterfaceRequestUtils {
        fn match_interfaces(
            &self,
            interfaces: &[Interface],
        ) -> Result<bool, Box<dyn std::error::Error>>;
    }

    impl InterfaceRequestUtils for tonic::Request<InterfacesJson> {
        fn match_interfaces(
            &self,
            interfaces: &[Interface],
        ) -> Result<bool, Box<dyn std::error::Error>> {
            let mut request_interfaces = self.get_ref().interfaces_json.clone();
            request_interfaces.sort_unstable();

            let mut expected_interfaces = interfaces
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()?;
            expected_interfaces.sort_unstable();

            Ok(request_interfaces == expected_interfaces)
        }
    }

    impl InterfaceRequestUtils for tonic::Request<InterfacesName> {
        fn match_interfaces(
            &self,
            interfaces: &[Interface],
        ) -> Result<bool, Box<dyn std::error::Error>> {
            let mut request_interfaces = self.get_ref().names.clone();
            request_interfaces.sort_unstable();

            let mut expected_interfaces =
                interfaces.iter().map(|i| i.interface_name()).collect_vec();
            expected_interfaces.sort_unstable();

            Ok(request_interfaces == expected_interfaces)
        }
    }

    impl InterfaceRequestUtils for InterfacesName {
        fn match_interfaces(
            &self,
            interfaces: &[Interface],
        ) -> Result<bool, Box<dyn std::error::Error>> {
            let mut request_interfaces = self.names.clone();
            request_interfaces.sort_unstable();

            let mut expected_interfaces =
                interfaces.iter().map(|i| i.interface_name()).collect_vec();
            expected_interfaces.sort_unstable();

            Ok(request_interfaces == expected_interfaces)
        }
    }

    async fn mock_grpc<S>(
        message_hub_client_tx: MsgHubClient,
        mut message_hub_client_rx: MsgHubClient,
        interfaces: &Interfaces,
        store: S,
    ) -> Result<(GrpcClient<S>, Grpc<S>), Box<dyn std::error::Error>>
    where
        S: PropertyStore,
    {
        let store = StoreWrapper::new(store);
        let volatile = SharedVolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY);
        let client = GrpcClient::new(message_hub_client_tx, store, volatile);

        let node_data = NodeData::try_from(interfaces)?;
        let stream = Grpc::<S>::attach(&mut message_hub_client_rx, node_data).await?;
        let connection = Grpc::new(ID, message_hub_client_rx, stream);

        Ok((client, connection))
    }

    fn mock_stream<T>(v: Vec<Result<Option<T>, Status>>) -> Streaming<T>
    where
        T: Send + Clone,
    {
        let mut streaming_server_response = Streaming::new();

        v.into_iter().for_each(|resp| {
            streaming_server_response
                .expect_message()
                .return_once(move || resp);
        });

        streaming_server_response
    }

    #[tokio::test]
    async fn test_attach_detach() {
        let mut seq = Sequence::new();
        // no expectations for the store
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(MemoryStore::new(), mock_store_client);
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        // When the grpc connection gets created the attach methods is called
        mock_client_rx
            .expect_attach::<tonic::Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| {
                // no messages are read as responses by the server so we pass an empty vec
                Ok(tonic::Response::new(mock_stream(vec![])))
            });
        // when disconnect is called detach gets called internally
        mock_client_tx
            .expect_detach::<tonic::Request<pbjson_types::Empty>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));

        let (mut client, _connection) =
            mock_grpc(mock_client_tx, mock_client_rx, &Interfaces::new(), store)
                .await
                .unwrap();

        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_server_error() {
        let mut seq = Sequence::new();
        // no expectations for the store
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(MemoryStore::new(), mock_store_client);
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        // 2 attach and 2 error returned
        mock_client_rx
            .expect_attach::<tonic::Request<astarte_message_hub_proto::Node>>()
            .times(2)
            .in_sequence(&mut seq)
            .returning(|_i| {
                Ok(tonic::Response::new(mock_stream(
                    // send an Err response as the first message
                    vec![Err(tonic::Status::unknown("Test unknown reattach"))],
                )))
            })
            .times(2);
        // attach no responses
        mock_client_rx
            .expect_attach::<tonic::Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| Ok(tonic::Response::new(mock_stream(vec![]))));
        // expect detach
        mock_client_tx
            .expect_detach::<tonic::Request<pbjson_types::Empty>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));

        // first attach is called when the connection is created
        let (mut client, mut connection) =
            mock_grpc(mock_client_tx, mock_client_rx, &Interfaces::new(), store)
                .await
                .unwrap();
        // poll the next message (error)
        assert!(matches!(connection.next_event().await, Ok(None)));
        // reconnect (second attach)
        assert!(matches!(
            connection.reconnect(&Interfaces::new()).await,
            Ok(())
        ));
        // poll the next message (second error)
        assert!(matches!(connection.next_event().await, Ok(None)));
        // after the second error we reconnect with no messages
        assert!(matches!(
            connection.reconnect(&Interfaces::new()).await,
            Ok(())
        ));

        // manually calling detach
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_add_remove_interface() {
        let mut seq = Sequence::new();
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(MemoryStore::new(), mock_store_client);
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        // When the grpc connection gets created the attach methods is called
        mock_client_rx
            .expect_attach::<tonic::Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| {
                // no messages are read as responses by the server so we pass an empty vec
                Ok(tonic::Response::new(mock_stream(vec![])))
            });
        mock_client_tx
            .expect_add_interfaces::<tonic::Request<astarte_message_hub_proto::InterfacesJson>>()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|r| {
                r.match_interfaces(&[Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap()])
                    .unwrap()
            })
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));
        mock_client_tx
            .expect_remove_interfaces::<tonic::Request<astarte_message_hub_proto::InterfacesName>>()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|r| {
                r.match_interfaces(&[Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap()])
                    .unwrap()
            })
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));
        mock_client_tx
            .expect_add_interfaces::<tonic::Request<astarte_message_hub_proto::InterfacesJson>>()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|r| {
                r.match_interfaces(&[
                    Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
                    Interface::from_str(crate::test::E2E_DEVICE_PROPERTY).unwrap(),
                ])
                .unwrap()
            })
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));
        mock_client_tx
            .expect_remove_interfaces::<tonic::Request<astarte_message_hub_proto::InterfacesName>>()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|r| {
                r.match_interfaces(&[
                    Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
                    Interface::from_str(crate::test::E2E_DEVICE_PROPERTY).unwrap(),
                ])
                .unwrap()
            })
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));
        // when disconnect is called detach is called
        mock_client_tx
            .expect_detach::<tonic::Request<pbjson_types::Empty>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));

        let (mut client, _connection) =
            mock_grpc(mock_client_tx, mock_client_rx, &Interfaces::new(), store)
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

        let additional_interface: Interface =
            Interface::from_str(crate::test::E2E_DEVICE_PROPERTY).unwrap();
        let list_to_add = Interfaces::new()
            .validate_many([interface.clone(), additional_interface.clone()])
            .unwrap();
        client
            .extend_interfaces(&interfaces, &list_to_add)
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
    }

    #[tokio::test]
    async fn test_send_individual() {
        let mut seq = Sequence::new();
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(MemoryStore::new(), mock_store_client);
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        const PATH: &str = "/1/name";
        const STRING_VALUE: &str = "value";
        let interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();
        let interface_name = interface.interface_name().to_owned();

        mock_client_rx
            .expect_attach::<tonic::Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| {
                // no messages are read as responses by the server so we pass an empty vec
                Ok(tonic::Response::new(mock_stream(vec![])))
            });

        let interface_name_cl = interface_name.clone();
        mock_client_tx
            .expect_send::<tonic::Request<AstarteMessage>>()
            .times(1)
            .in_sequence(&mut seq)
            .withf(move |r| {
                DeviceEvent::try_from(r.get_ref().clone()).is_ok_and(|e| {
                    e.interface == interface_name_cl
                        && e.path == PATH
                        && matches!(e.data, Value::Individual(AstarteType::String(v))
                            if v == STRING_VALUE)
                })
            })
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));

        mock_client_tx
            .expect_detach::<tonic::Request<pbjson_types::Empty>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));

        let (mut client, _connection) =
            mock_grpc(mock_client_tx, mock_client_rx, &Interfaces::new(), store)
                .await
                .unwrap();

        let path = MappingPath::try_from(PATH).unwrap();
        let interfaces = Interfaces::from_iter([interface]);
        let mapping_ref = interfaces
            .interface_mapping(&interface_name, &path)
            .unwrap();
        let validated = ValidatedIndividual::validate(
            mapping_ref,
            &path,
            AstarteType::String(STRING_VALUE.to_string()),
            None,
        )
        .unwrap();
        client.send_individual(validated).await.unwrap();
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_object_timestamp() {
        let mut seq = Sequence::new();
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(MemoryStore::new(), mock_store_client);
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        const PATH: &str = "/1";
        let interface = Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap();
        let interface_name = interface.interface_name().to_owned();

        mock_client_rx
            .expect_attach::<tonic::Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| {
                // no messages are read as responses by the server so we pass an empty vec
                Ok(tonic::Response::new(mock_stream(vec![])))
            });

        let interface_name_cl = interface_name.clone();
        mock_client_tx
            .expect_send::<tonic::Request<AstarteMessage>>()
            .times(1)
            .in_sequence(&mut seq)
            .withf(move |r| {
                // TODO verify timestamp
                DeviceEvent::try_from(r.get_ref().clone()).is_ok_and(|e| {
                    e.interface == interface_name_cl
                        && e.path == PATH
                        && matches!(e.data, Value::Object(o)
                            if ((MockDeviceObject {}).astarte_aggregate()
                                .map(|expected| expected == o).unwrap()))
                })
            })
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));

        mock_client_tx
            .expect_detach::<tonic::Request<pbjson_types::Empty>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i: Request<_>| Ok(tonic::Response::new(pbjson_types::Empty {})));

        let (mut client, _connection) =
            mock_grpc(mock_client_tx, mock_client_rx, &Interfaces::new(), store)
                .await
                .unwrap();

        let path = MappingPath::try_from(PATH).unwrap();
        let interfaces = Interfaces::from_iter([interface]);
        let object_ref = interfaces
            .get(&interface_name)
            .and_then(ObjectRef::new)
            .unwrap();
        let validated = ValidatedObject::validate(
            object_ref,
            &path,
            MockDeviceObject {}.astarte_aggregate().unwrap(),
            None,
        )
        .unwrap();

        client.send_object(validated).await.unwrap();
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_connection_receive_object() {
        let mut seq = Sequence::new();
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(MemoryStore::new(), mock_store_client);
        let mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        const PATH: &str = "/1";
        let interface = Interface::from_str(crate::test::E2E_SERVER_DATASTREAM).unwrap();
        let interface_name = interface.interface_name().to_owned();
        let expected_object = Value::Object((MockServerObject {}).astarte_aggregate().unwrap());
        let proto_payload: astarte_message_hub_proto::astarte_message::Payload =
            expected_object.into();
        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: PATH.to_string(),
            timestamp: None,
            payload: Some(proto_payload.clone()),
        };

        mock_client_rx
            .expect_attach::<tonic::Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_i| {
                Ok(tonic::Response::new(mock_stream(vec![Ok(Some(
                    astarte_message_hub_proto::MessageHubEvent {
                        event: Some(
                            astarte_message_hub_proto::message_hub_event::Event::Message(
                                astarte_message.clone(),
                            ),
                        ),
                    },
                ))])))
            });

        let (_client, mut connection) =
            mock_grpc(mock_client_tx, mock_client_rx, &Interfaces::new(), store)
                .await
                .unwrap();

        let Some(event) = connection.next_event().await.unwrap() else {
            panic!("Event received did not match the pattern");
        };

        assert_eq!(event.interface, interface_name);
        assert_eq!(event.path, PATH);
        assert_eq!(event.payload.data, proto_payload);
    }

    #[tokio::test]
    async fn test_connection_receive_unset() {
        let mut seq = Sequence::new();
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(MemoryStore::new(), mock_store_client);
        let mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        const PATH: &str = "/1/enable";
        let interface = Interface::from_str(crate::test::SERVER_PROPERTIES).unwrap();
        let interface_name = interface.interface_name().to_owned();
        let proto_payload = ProtoPayload::AstarteUnset(astarte_message_hub_proto::AstarteUnset {});
        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: PATH.to_string(),
            timestamp: None,
            payload: Some(proto_payload.clone()),
        };

        mock_client_rx
            .expect_attach::<tonic::Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_i| {
                Ok(tonic::Response::new(mock_stream(vec![Ok(Some(
                    astarte_message_hub_proto::MessageHubEvent {
                        event: Some(
                            astarte_message_hub_proto::message_hub_event::Event::Message(
                                astarte_message.clone(),
                            ),
                        ),
                    },
                ))])))
            });

        let (_client, mut connection) =
            mock_grpc(mock_client_tx, mock_client_rx, &Interfaces::new(), store)
                .await
                .unwrap();

        let Some(event) = connection.next_event().await.unwrap() else {
            panic!("Event received did not match the pattern");
        };

        assert_eq!(event.interface, interface_name);
        assert_eq!(event.path, PATH);
        assert_eq!(event.payload.data, proto_payload);
    }

    #[test]
    fn create_config() {
        let uuid = Uuid::new_v4();

        let mut config = GrpcConfig::from_url(uuid, "http://hub.example.com").unwrap();

        assert_eq!(config.endpoint_mut().uri().host(), Some("hub.example.com"));
    }

    #[tokio::test]
    async fn test_grpc_store_grpc_client_calls() {
        let device_interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();
        let server_interface = Interface::from_str(crate::test::SERVER_PROPERTIES).unwrap();
        const PATH: &str = "/path1";
        let mut seq = Sequence::new();
        let mut mock_store_client = MsgHubClient::new();
        mock_store_client
            .expect_get_all_properties::<astarte_message_hub_proto::StoredPropertiesFilter>()
            .times(1)
            .in_sequence(&mut seq)
            //.withf(|r| r.get_ref().ownership() == astarte_message_hub_proto::Ownership::Device)
            .returning(|_i| {
                Ok(tonic::Response::new(
                    astarte_message_hub_proto::StoredProperties {
                        interface_properties: HashMap::new(),
                    },
                ))
            });
        mock_store_client
            .expect_get_properties::<astarte_message_hub_proto::InterfacesName>()
            .times(1)
            .in_sequence(&mut seq)
            .withf(move |r| {
                r.match_interfaces(&[Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap()])
                    .unwrap()
            })
            .returning(|_i| {
                Ok(tonic::Response::new(
                    astarte_message_hub_proto::StoredProperties {
                        interface_properties: HashMap::new(),
                    },
                ))
            });
        mock_store_client
            .expect_get_property::<astarte_message_hub_proto::PropertyIdentifier>()
            .times(1)
            .in_sequence(&mut seq)
            .withf(move |r| {
                r.interface_name
                    == Interface::from_str(crate::test::DEVICE_PROPERTIES)
                        .unwrap()
                        .interface_name()
                    && r.path == PATH
            })
            .returning(|_i| {
                Ok(tonic::Response::new(astarte_message_hub_proto::Property {
                    path: PATH.to_owned(),
                    value: Some(astarte_message_hub_proto::property::Value::AstarteUnset(
                        astarte_message_hub_proto::AstarteUnset {},
                    )),
                }))
            });
        let store = GrpcStore::new(MemoryStore::new(), mock_store_client);

        // all properties
        let _device_properties = store.device_props().await.unwrap();
        // no call should be made for the server owned properties
        let _server_properties = store.server_props().await.unwrap();

        // get properties
        let _device_interface_properties = store
            .interface_props(&(&device_interface).into())
            .await
            .unwrap();
        // if you get the properties of a server interface no request should be made
        let _server_interface_properties = store
            .interface_props(&(&server_interface).into())
            .await
            .unwrap();

        // get property
        let _device_prop = store
            .load_prop(&(&device_interface).into(), PATH, 1)
            .await
            .unwrap();
        // no request should be made
        let _server_prop = store
            .load_prop(&(&server_interface).into(), PATH, 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_grpc_store_device_prop_not_stored() {
        let inner_value = AstarteType::Integer(1);
        const PATH: &str = "/path1";
        let server_interface = "com.server.interface";
        let server_prop = StoredProp {
            interface: server_interface,
            path: PATH,
            value: &inner_value,
            interface_major: 1,
            ownership: Ownership::Server,
        };
        let server_interface_data = &(&server_prop).into();
        let device_interface = "com.device.interface";
        let device_prop = StoredProp {
            interface: device_interface,
            path: PATH,
            value: &inner_value,
            interface_major: 1,
            ownership: Ownership::Device,
        };
        let device_interface_data = &(&device_prop).into();

        let mock_store_client = MsgHubClient::new();
        let base_store = MemoryStore::new();
        let store = GrpcStore::new(base_store.clone(), mock_store_client);

        // test store server owned property
        store.store_prop(server_prop).await.unwrap();
        assert_eq!(
            store
                .load_prop(server_interface_data, PATH, 1)
                .await
                .unwrap(),
            Some(inner_value.clone())
        );
        // test store device owned property (should not store)
        store.store_prop(device_prop).await.unwrap();
        assert!(base_store
            .load_prop(device_interface_data, PATH, 1)
            .await
            .unwrap()
            .is_none());

        // cleanup
        store.clear().await.unwrap();

        // delete server owned should delete the underlying prop
        base_store.store_prop(server_prop).await.unwrap();
        store
            .delete_prop(server_interface_data, PATH)
            .await
            .unwrap();
        assert!(base_store
            .load_prop(server_interface_data, PATH, 1)
            .await
            .unwrap()
            .is_none());
        // deleting a device owned prop should not delete the underlying prop
        // since those don't get stored
        base_store.store_prop(device_prop).await.unwrap();
        store
            .delete_prop(device_interface_data, PATH)
            .await
            .unwrap();
        assert_eq!(
            base_store
                .load_prop(device_interface_data, PATH, 1)
                .await
                .unwrap(),
            Some(inner_value.clone())
        );
    }
}
