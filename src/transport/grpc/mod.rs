// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! # Astarte GRPC Transport Module
//!
//! This module provides an implementation of the Astarte transport layer using the GRPC protocol.
//! It defines the `Grpc` struct, which represents a GRPC connection, along with traits for publishing,
//! receiving, and registering interfaces.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use astarte_device_error::{Error, ResultExt, WrapError};
use astarte_interfaces::schema::{Aggregation, InterfaceType};
use astarte_interfaces::{
    DatastreamIndividual, DatastreamObject, Interface, MappingPath, Properties, Schema,
};
use astarte_message_hub_proto::prost::Message;
use astarte_message_hub_proto::tonic::codegen::InterceptedService;
use astarte_message_hub_proto::tonic::metadata::MetadataValue;
use astarte_message_hub_proto::tonic::service::Interceptor;
use astarte_message_hub_proto::tonic::transport::{Channel, Endpoint};
use astarte_message_hub_proto::tonic::{Request, Status};
use astarte_message_hub_proto::{
    AstarteMessage, InterfacesJson, InterfacesName, MessageHubEvent, Node,
    astarte_message::Payload as ProtoPayload,
};
use bytes::Bytes;
use sync_wrapper::SyncWrapper;
use tracing::{error, trace, warn};
use uuid::Uuid;

use self::convert::{try_from_individual, try_from_object, try_from_property};
use self::error::GrpcError;
use self::store::GrpcStore;
use super::{Connection, Disconnect, Publish, Receive, ReceivedEvent, Register, ValidatedProperty};
use crate::Timestamp;
use crate::aggregate::AstarteObject;
use crate::builder::{BuildConfig, ConnectionConfig, DeviceTransport};
use crate::error::{AstarteError, ErrorKind, InterfaceError, Report};
use crate::interfaces::MappingRef;
use crate::interfaces::{self, Interfaces};
use crate::retention::{PublishInfo, RetentionId, StoredRetention};
use crate::state::SharedState;
use crate::store::{OptStoredProp, PropertyStore, StoreCapabilities};
use crate::transport::AttemptStatus;
use crate::types::AstarteData;
use crate::validate::{ValidatedIndividual, ValidatedObject, ValidatedUnset};

pub mod convert;
pub mod error;
pub mod store;

#[cfg(feature = "message-hub")]
#[cfg_attr(astarte_device_sdk_docsrs, doc(cfg(feature = "message-hub")))]
pub use astarte_message_hub_proto::tonic;

cfg_if::cfg_if! {
    if #[cfg(test)] {
        type MessageHubClient<T> = astarte_message_hub_proto_mock::MockMessageHubClient<T>;
        type Streaming<T> = astarte_message_hub_proto_mock::MockStreaming<T>;
    }
    else {
        type MessageHubClient<T> =
            astarte_message_hub_proto::message_hub_client::MessageHubClient<T>;
        type Streaming<T> = astarte_message_hub_proto::tonic::codec::Streaming<T>;
    }
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
    store: S,
    state: Arc<SharedState>,
}

impl<S> GrpcClient<S> {
    /// Create a new client.
    pub(crate) fn new(client: MsgHubClient, store: S, state: Arc<SharedState>) -> Self {
        Self {
            client,
            store,
            state,
        }
    }

    async fn mark_received(&self, id: &RetentionId) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        match id {
            RetentionId::Volatile(id) => {
                self.state.volatile_store.mark_received(id).await;
            }
            RetentionId::Stored(id) => {
                if let Some(retention) = self.store.get_retention() {
                    retention
                        .mark_received(id)
                        .await
                        .map_kind(ErrorKind::Retention)?;
                }
            }
        }

        Ok(())
    }

    async fn detach(&mut self) -> Result<(), AstarteError> {
        self.client
            .detach(tonic::Request::new(()))
            .await
            .map_err(|status| {
                AstarteError::with(ErrorKind::Grpc(GrpcError::Status), "while detaching node")
                    .set_ctx(status)
            })?;

        Ok(())
    }
}

impl<S> Publish for GrpcClient<S>
where
    S: StoreCapabilities + Send + Sync,
{
    async fn send_individual(&mut self, data: ValidatedIndividual) -> Result<(), AstarteError> {
        let data = AstarteMessage::from(data);

        self.client
            .send(tonic::Request::new(data))
            .await
            .map_err(|status| {
                AstarteError::with(
                    ErrorKind::Grpc(GrpcError::Status),
                    "while sending individual",
                )
                .set_ctx(status)
            })?;

        Ok(())
    }

    async fn send_property(&mut self, data: ValidatedProperty) -> Result<(), AstarteError> {
        let data = AstarteMessage::from(data);

        self.client
            .send(tonic::Request::new(data))
            .await
            .map_err(|status| {
                AstarteError::with(ErrorKind::Grpc(GrpcError::Status), "while sending property")
                    .set_ctx(status)
            })?;

        Ok(())
    }

    async fn send_object(&mut self, data: ValidatedObject) -> Result<(), AstarteError> {
        let data = AstarteMessage::from(data);

        self.client
            .send(tonic::Request::new(data))
            .await
            .map_err(|status| {
                AstarteError::with(ErrorKind::Grpc(GrpcError::Status), "while sending object")
                    .set_ctx(status)
            })?;

        Ok(())
    }

    async fn send_individual_stored(
        &mut self,
        id: RetentionId,
        data: ValidatedIndividual,
    ) -> Result<(), AstarteError> {
        let data = AstarteMessage::from(data);

        self.client
            .send(tonic::Request::new(data))
            .await
            .map_err(|status| {
                AstarteError::with(
                    ErrorKind::Grpc(GrpcError::Status),
                    "while sending individual stored",
                )
                .set_ctx(status)
            })?;

        self.mark_received(&id).await?;

        Ok(())
    }

    async fn send_object_stored(
        &mut self,
        id: RetentionId,
        data: ValidatedObject,
    ) -> Result<(), AstarteError> {
        let data = AstarteMessage::from(data);

        self.client
            .send(tonic::Request::new(data))
            .await
            .map_err(|status| {
                AstarteError::with(
                    ErrorKind::Grpc(GrpcError::Status),
                    "while sending object stored",
                )
                .set_ctx(status)
            })?;

        self.mark_received(&id).await?;

        Ok(())
    }

    async fn resend_stored(
        &mut self,
        id: RetentionId,
        data: PublishInfo<'_>,
    ) -> Result<(), AstarteError> {
        let msg = AstarteMessage::decode(data.value.borrow()).wrap_err_msg(
            ErrorKind::Grpc(GrpcError::Decode),
            "while decoding stored payload",
        )?;

        self.client
            .send(tonic::Request::new(msg))
            .await
            .map_err(|status| {
                AstarteError::with(ErrorKind::Grpc(GrpcError::Status), "while resending stored")
                    .set_ctx(status)
            })?;

        self.mark_received(&id).await?;

        Ok(())
    }

    async fn resend_stored_property(&mut self, data: OptStoredProp) -> Result<(), AstarteError> {
        self.client
            .send(tonic::Request::new(data.into()))
            .await
            .map_err(|status| {
                AstarteError::with(ErrorKind::Grpc(GrpcError::Status), "while resending stored")
                    .set_ctx(status)
            })?;

        Ok(())
    }

    async fn unset(&mut self, data: ValidatedUnset) -> Result<(), AstarteError> {
        self.client
            .send(tonic::Request::new(data.into()))
            .await
            .map_err(|status| {
                AstarteError::with(
                    ErrorKind::Grpc(GrpcError::Status),
                    "while unsetting property",
                )
                .set_ctx(status)
            })?;

        Ok(())
    }

    fn serialize_individual(&self, data: &ValidatedIndividual) -> Result<Vec<u8>, AstarteError> {
        let data = AstarteMessage::from(data.clone());

        Ok(data.encode_to_vec())
    }

    fn serialize_object(&self, data: &ValidatedObject) -> Result<Vec<u8>, AstarteError> {
        let data = AstarteMessage::from(data.clone());

        Ok(data.encode_to_vec())
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
    ) -> Result<(), AstarteError> {
        let interfaces_json = InterfacesJson::try_from_iter([added.deref()])
            .wrap_err(ErrorKind::Grpc(GrpcError::Introspection))?;

        self.client
            .add_interfaces(tonic::Request::new(interfaces_json))
            .await
            .map_err(|status| {
                AstarteError::with(ErrorKind::Grpc(GrpcError::Status), "while adding interface")
                    .set_ctx(status)
            })?;

        Ok(())
    }

    async fn extend_interfaces(
        &mut self,
        _interfaces: &Interfaces,
        added: &interfaces::ValidatedCollection,
    ) -> Result<(), AstarteError> {
        let interfaces_json = InterfacesJson::try_from_iter(added.iter_interfaces())
            .wrap_err(ErrorKind::Grpc(GrpcError::Introspection))?;

        self.client
            .add_interfaces(tonic::Request::new(interfaces_json))
            .await
            .map_err(|status| {
                AstarteError::with(
                    ErrorKind::Grpc(GrpcError::Status),
                    "while extending interfaces",
                )
                .set_ctx(status)
            })?;

        Ok(())
    }

    async fn remove_interface(
        &mut self,
        _interfaces: &Interfaces,
        removed: &Interface,
    ) -> Result<(), AstarteError> {
        let interfaces_name = InterfacesName {
            names: vec![removed.interface_name().to_string()],
        };

        self.client
            .remove_interfaces(tonic::Request::new(interfaces_name))
            .await
            .map_err(|status| {
                AstarteError::with(
                    ErrorKind::Grpc(GrpcError::Status),
                    "while removing interface",
                )
                .set_ctx(status)
            })?;

        Ok(())
    }

    async fn remove_interfaces(
        &mut self,
        _interfaces: &Interfaces,
        removed_interfaces: &HashMap<&str, &Interface>,
    ) -> Result<(), AstarteError> {
        let interfaces_name = removed_interfaces
            .keys()
            .map(|iface_name| iface_name.to_string())
            .collect();

        let interfaces_name = InterfacesName {
            names: interfaces_name,
        };

        self.client
            .remove_interfaces(tonic::Request::new(interfaces_name))
            .await
            .map_err(|status| {
                AstarteError::with(
                    ErrorKind::Grpc(GrpcError::Status),
                    "while removing interfaces",
                )
                .set_ctx(status)
            })?;

        Ok(())
    }
}

impl<S> Disconnect for GrpcClient<S>
where
    S: Send,
{
    async fn disconnect(&mut self) -> Result<(), AstarteError> {
        self.detach().await?;

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
    stream: Option<SyncWrapper<Streaming<MessageHubEvent>>>,
    _store_type: PhantomData<S>,
}

impl<S> Grpc<S> {
    pub(crate) fn new_disconnected(uuid: Uuid, client: MsgHubClient) -> Self {
        Self {
            uuid,
            client,
            stream: None,
            _store_type: PhantomData,
        }
    }

    /// Polls a message from the tonic stream and tries reattaching if necessary
    ///
    /// An [`Option`] is returned directly from the [`tonic::codec::Streaming::message`] method.
    /// A result of [`None`] signals a disconnection and should be handled by the caller
    async fn next_message(&mut self) -> Result<Option<MessageHubEvent>, tonic::Status> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };

        stream.get_mut().message().await
    }

    async fn attach(
        client: &mut MsgHubClient,
        data: NodeData,
    ) -> Result<Streaming<MessageHubEvent>, Error<GrpcError>> {
        client
            .attach(tonic::Request::new(data.node))
            .await
            .map(|r| r.into_inner())
            .map_err(|status| {
                Error::with(GrpcError::Status, "while removing interfaces").set_ctx(status)
            })
    }
}

impl<S> std::fmt::Debug for Grpc<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grpc")
            .field("uuid", &self.uuid)
            .finish_non_exhaustive()
    }
}

impl<S> Receive for Grpc<S>
where
    S: PropertyStore,
{
    type Payload = GrpcPayload;

    async fn next_event(&mut self) -> Result<Option<ReceivedEvent<Self::Payload>>, AstarteError> {
        loop {
            match self.next_message().await {
                Ok(Some(message)) => {
                    let event = match ReceivedEvent::try_from(message) {
                        Ok(event) => event,
                        Err(error) => {
                            error!(%error, "couldn't convert MessageHUb event");

                            continue;
                        }
                    };

                    return Ok(Some(event));
                }
                Err(status) => {
                    error!(%status, "error returned by the server");

                    return Ok(None);
                }
                Ok(None) => {
                    warn!("Stream closed");

                    return Ok(None);
                }
            }
        }
    }

    async fn reconnect(
        &mut self,
        interfaces: &Interfaces,
    ) -> Result<AttemptStatus<Self::Payload>, AstarteError> {
        // try reattaching
        let data = NodeData::try_from(interfaces).map_kind(ErrorKind::Grpc)?;

        match Grpc::<S>::attach(&mut self.client, data.clone()).await {
            Ok(stream) => {
                self.stream = Some(SyncWrapper::new(stream));

                Ok(AttemptStatus::Connected {
                    session_present: false,
                })
            }
            Err(err) => {
                error!(error = %Report::new(err), "error while trying to reconnect");
                self.stream = None;

                Ok(AttemptStatus::Disconnected)
            }
        }
    }

    fn deserialize_property(
        &self,
        mapping: &MappingRef<'_, Properties>,
        payload: Self::Payload,
    ) -> Result<Option<AstarteData>, AstarteError> {
        let ProtoPayload::PropertyIndividual(prop) = payload.data else {
            return Err(InterfaceError::interface_type(
                "while deserializing property",
                mapping.interface(),
                mapping.path(),
                InterfaceType::Properties,
                InterfaceType::Datastream,
            ))
            .map_kind(ErrorKind::Interface);
        };

        let data = try_from_property(prop)
            .map_kind(|kind| ErrorKind::Grpc(GrpcError::Conversion(kind)))?;

        trace!(
            "deserialized {}",
            data.as_ref().map(|p| p.display_type()).unwrap_or("unset")
        );

        Ok(data)
    }

    fn deserialize_individual(
        &self,
        mapping: &MappingRef<'_, DatastreamIndividual>,
        payload: Self::Payload,
    ) -> Result<(AstarteData, Option<Timestamp>), AstarteError> {
        let individual = match payload.data {
            ProtoPayload::DatastreamIndividual(individual) => individual,
            ProtoPayload::DatastreamObject(_) => {
                return Err(InterfaceError::aggregation(
                    "while deserializing individual",
                    mapping.interface().name(),
                    mapping.path(),
                    Aggregation::Individual,
                    Aggregation::Object,
                ))
                .map_kind(ErrorKind::Interface);
            }
            ProtoPayload::PropertyIndividual(_) => {
                return Err(InterfaceError::interface_type(
                    "while deserializing individual",
                    mapping.interface().name(),
                    mapping.path(),
                    InterfaceType::Datastream,
                    InterfaceType::Properties,
                ))
                .map_kind(ErrorKind::Interface);
            }
        };

        let (data, timestamp) = try_from_individual(individual)
            .map_kind(|kind| ErrorKind::Grpc(GrpcError::Conversion(kind)))?;

        trace!("deserialized {}", data.display_type());

        Ok((data, timestamp))
    }

    fn deserialize_object(
        &self,
        object: &DatastreamObject,
        path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(AstarteObject, Option<Timestamp>), AstarteError> {
        let data = match payload.data {
            ProtoPayload::DatastreamObject(object) => object,
            ProtoPayload::DatastreamIndividual(_) => {
                return Err(InterfaceError::aggregation(
                    "while deserializing object",
                    object.name(),
                    path,
                    Aggregation::Object,
                    Aggregation::Individual,
                ))
                .map_kind(ErrorKind::Interface);
            }
            ProtoPayload::PropertyIndividual(_) => {
                return Err(InterfaceError::interface_type(
                    "while deserializing object",
                    object.name(),
                    path,
                    InterfaceType::Datastream,
                    InterfaceType::Properties,
                ))
                .map_kind(ErrorKind::Interface);
            }
        };

        let (data, timestamp) =
            try_from_object(data).map_kind(|kind| ErrorKind::Grpc(GrpcError::Conversion(kind)))?;

        trace!("object received");

        Ok((data, timestamp))
    }
}

impl<S> Connection for Grpc<S>
where
    S: StoreCapabilities,
{
    type Sender = GrpcClient<GrpcStore<S>>;

    type Store = GrpcStore<S>;

    async fn is_paired(&self) -> Result<bool, std::io::Error> {
        // The device never pairs with the message-hub
        Ok(false)
    }
}

/// Internal struct holding the received grpc message
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GrpcPayload {
    data: ProtoPayload,
}

impl GrpcPayload {
    pub(crate) fn new(data: ProtoPayload) -> Self {
        Self { data }
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
    pub fn from_url(uuid: Uuid, url: impl Into<Bytes>) -> Result<Self, Error<GrpcError>> {
        let endpoint = Endpoint::from_shared(url)
            .wrap_err_msg(GrpcError::InvalidArgument, "for MessageHub URL")?;

        Ok(Self::new(uuid, endpoint))
    }

    /// Returns a mutable reference to configure the endpoint.
    pub fn endpoint_mut(&mut self) -> &mut Endpoint {
        &mut self.endpoint
    }
}

impl<S> ConnectionConfig<S> for GrpcConfig
where
    S: StoreCapabilities + PropertyStore,
{
    type Conn = Grpc<S>;
    type Store = GrpcStore<S>;

    async fn connect(
        self,
        config: BuildConfig<S>,
    ) -> Result<DeviceTransport<Self::Conn>, AstarteError> {
        let channel = self.endpoint.connect_lazy();
        let node_id_interceptor = NodeIdInterceptor::new(self.uuid);
        let client = MessageHubClient::with_interceptor(channel, node_id_interceptor);
        let store = GrpcStore::new(client.clone(), config.store);

        let state = Arc::clone(&config.state);

        let sender = GrpcClient::new(client.clone(), store.clone(), state);

        let connection = Grpc::new_disconnected(self.uuid, client);

        Ok(DeviceTransport {
            sender,
            connection,
            store,
        })
    }
}

/// Wrapper that contains data needed while connecting the node to the astarte message hub.
#[derive(Debug, Clone)]
struct NodeData {
    node: Node,
}

impl NodeData {
    fn try_from_iter<'a, I>(interfaces: I) -> Result<Self, Error<GrpcError>>
    where
        I: IntoIterator<Item = &'a Interface>,
    {
        let node = Node::from_interfaces(interfaces).wrap_err(GrpcError::Introspection)?;

        Ok(Self { node })
    }
}

impl<'a> TryFrom<&'a Interfaces> for NodeData {
    type Error = Error<GrpcError>;

    fn try_from(value: &'a Interfaces) -> Result<Self, Self::Error> {
        Self::try_from_iter(value.iter())
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use astarte_message_hub_proto::tonic::Request;
    use astarte_message_hub_proto::{AstarteDatastreamIndividual, AstarteMessage};
    use astarte_message_hub_proto::{AstarteDatastreamObject, AstartePropertyIndividual, tonic};
    use astarte_message_hub_proto_mock::mockall::{Sequence, predicate};
    use chrono::Utc;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use uuid::uuid;

    use crate::builder::Config;
    use crate::retention::memory::VolatileStore;
    use crate::store::memory::MemoryStore;
    use crate::test::{
        DEVICE_OBJECT, DEVICE_PROPERTIES, DEVICE_PROPERTIES_NAME, E2E_DEVICE_PROPERTY,
        E2E_DEVICE_PROPERTY_NAME, E2E_SERVER_DATASTREAM,
    };
    use crate::{aggregate::AstarteObject, builder::DEFAULT_VOLATILE_CAPACITY};

    use super::*;

    impl<S> Grpc<S> {
        pub(crate) fn new(
            uuid: Uuid,
            client: MsgHubClient,
            stream: Streaming<MessageHubEvent>,
        ) -> Self {
            Self {
                uuid,
                client,
                stream: Some(SyncWrapper::new(stream)),
                _store_type: PhantomData,
            }
        }
    }

    pub(crate) const ID: Uuid = uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8");

    struct MockDeviceObject {}

    impl MockDeviceObject {
        fn mock_object() -> AstarteObject {
            AstarteObject::from_iter([
                ("endpoint1".to_string(), AstarteData::try_from(4.2).unwrap()),
                (
                    "endpoint2".to_string(),
                    AstarteData::String("obj".to_string()),
                ),
                (
                    "endpoint3".to_string(),
                    AstarteData::BooleanArray(vec![true, false, true]),
                ),
            ])
        }
    }

    struct MockServerObject {}

    impl MockServerObject {
        fn mock_object() -> AstarteObject {
            AstarteObject::from_iter([
                (
                    "double_endpoint".to_string(),
                    AstarteData::try_from(4.2).unwrap(),
                ),
                (
                    "string_endpoint".to_string(),
                    AstarteData::String("obj".to_string()),
                ),
                (
                    "boleanarray_endpoint".to_string(),
                    AstarteData::BooleanArray(vec![true, false, true]),
                ),
            ])
        }
    }

    pub(crate) trait InterfaceRequestUtils {
        fn match_interfaces(
            &self,
            interfaces: &[Interface],
        ) -> Result<bool, Box<dyn std::error::Error>>;
    }

    impl InterfaceRequestUtils for Request<InterfacesJson> {
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

    impl InterfaceRequestUtils for Request<InterfacesName> {
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
        interfaces: Interfaces,
        store: S,
    ) -> Result<(GrpcClient<S>, Grpc<S>), Box<dyn std::error::Error>>
    where
        S: PropertyStore,
    {
        let node_data = NodeData::try_from(&interfaces)?;
        let state = SharedState::new(
            Config::default(),
            interfaces,
            VolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY.get()),
        );

        let client = GrpcClient::new(message_hub_client_tx, store, Arc::new(state));

        let stream = Grpc::<S>::attach(&mut message_hub_client_rx, node_data).await?;
        let connection = Grpc::new(ID, message_hub_client_rx, stream);

        Ok((client, connection))
    }

    fn mock_stream<I, T>(v: I) -> Streaming<T>
    where
        I: IntoIterator<Item = Result<Option<T>, Status>>,
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

    fn check_individual_message(
        msg: &AstarteMessage,
        interface: &str,
        path: &str,
        value: AstarteData,
    ) -> bool {
        let Some(astarte_message_hub_proto::astarte_message::Payload::DatastreamIndividual(
            AstarteDatastreamIndividual {
                data: Some(data), ..
            },
        )) = &msg.payload
        else {
            return false;
        };

        msg.interface_name == interface
            && msg.path == path
            && AstarteData::try_from(data.clone()).is_ok_and(|data| data == value)
    }

    fn check_object_message(
        msg: &AstarteMessage,
        interface: &str,
        path: &str,
        value: AstarteObject,
    ) -> bool {
        let Some(astarte_message_hub_proto::astarte_message::Payload::DatastreamObject(data)) =
            &msg.payload
        else {
            return false;
        };

        let Ok((data, _timestamp)) = try_from_object(data.clone()) else {
            return false;
        };

        msg.interface_name == interface && msg.path == path && data == value
    }

    #[tokio::test]
    async fn test_attach_detach() {
        let mut seq = Sequence::new();
        // no expectations for the store
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(mock_store_client, MemoryStore::new());
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        // When the grpc connection gets created the attach methods is called
        mock_client_rx
            .expect_attach::<Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| {
                // no messages are read as responses by the server so we pass an empty vec
                Ok(tonic::Response::new(mock_stream([])))
            });
        // when disconnect is called detach gets called internally
        mock_client_tx
            .expect_detach::<Request<()>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));

        let (mut client, _connection) =
            mock_grpc(mock_client_tx, mock_client_rx, Interfaces::new(), store)
                .await
                .unwrap();

        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_server_error() {
        let mut seq = Sequence::new();
        // no expectations for the store
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(mock_store_client, MemoryStore::new());
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        // 2 attach and 2 error returned
        mock_client_rx
            .expect_attach::<Request<astarte_message_hub_proto::Node>>()
            .times(2)
            .in_sequence(&mut seq)
            .returning(|_i| {
                Ok(tonic::Response::new(mock_stream(
                    // send an Err response as the first message
                    [Err(tonic::Status::unknown("Test unknown reattach"))],
                )))
            });
        // attach no responses
        mock_client_rx
            .expect_attach::<Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| Ok(tonic::Response::new(mock_stream([]))));
        // expect detach
        mock_client_tx
            .expect_detach::<Request<()>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));

        // first attach is called when the connection is created
        let (mut client, mut connection) =
            mock_grpc(mock_client_tx, mock_client_rx, Interfaces::new(), store)
                .await
                .unwrap();
        // poll the next message (error)
        assert_eq!(connection.next_event().await.unwrap(), None);
        // reconnect (second attach)
        assert_eq!(
            connection.reconnect(&Interfaces::new()).await.unwrap(),
            AttemptStatus::Connected {
                session_present: false
            }
        );
        // poll the next message (second error)
        assert_eq!(connection.next_event().await.unwrap(), None);
        // after the second error we reconnect with no messages
        assert_eq!(
            connection.reconnect(&Interfaces::new()).await.unwrap(),
            AttemptStatus::Connected {
                session_present: false
            }
        );

        // manually calling detach
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_add_remove_interface() {
        let mut seq = Sequence::new();
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(mock_store_client, MemoryStore::new());
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        // When the grpc connection gets created the attach methods is called
        mock_client_rx
            .expect_attach::<Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| {
                // no messages are read as responses by the server so we pass an empty vec
                Ok(tonic::Response::new(mock_stream(vec![])))
            });
        mock_client_tx
            .expect_add_interfaces::<Request<astarte_message_hub_proto::InterfacesJson>>()
            .times(1)
            .in_sequence(&mut seq)
            .with(predicate::function(|r: &Request<_>| {
                r.match_interfaces(&[Interface::from_str(DEVICE_PROPERTIES).unwrap()])
                    .unwrap()
            }))
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));
        mock_client_tx
            .expect_remove_interfaces::<Request<astarte_message_hub_proto::InterfacesName>>()
            .times(1)
            .in_sequence(&mut seq)
            .with(predicate::function(|r: &Request<_>| {
                r.match_interfaces(&[Interface::from_str(DEVICE_PROPERTIES).unwrap()])
                    .unwrap()
            }))
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));
        mock_client_tx
            .expect_add_interfaces::<Request<astarte_message_hub_proto::InterfacesJson>>()
            .times(1)
            .in_sequence(&mut seq)
            .with(predicate::function(|r: &Request<_>| {
                r.match_interfaces(&[
                    Interface::from_str(DEVICE_PROPERTIES).unwrap(),
                    Interface::from_str(E2E_DEVICE_PROPERTY).unwrap(),
                ])
                .unwrap()
            }))
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));
        mock_client_tx
            .expect_remove_interfaces::<Request<astarte_message_hub_proto::InterfacesName>>()
            .times(1)
            .in_sequence(&mut seq)
            .with(predicate::function(|r: &Request<_>| {
                r.match_interfaces(&[
                    Interface::from_str(DEVICE_PROPERTIES).unwrap(),
                    Interface::from_str(E2E_DEVICE_PROPERTY).unwrap(),
                ])
                .unwrap()
            }))
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));
        // when disconnect is called detach is called
        mock_client_tx
            .expect_detach::<Request<()>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));

        let (mut client, _connection) =
            mock_grpc(mock_client_tx, mock_client_rx, Interfaces::new(), store)
                .await
                .unwrap();

        let interfaces = Interfaces::new();
        let interface = Interface::from_str(DEVICE_PROPERTIES).unwrap();
        let validated = interfaces.validate(interface.clone()).unwrap().unwrap();
        client.add_interface(&interfaces, &validated).await.unwrap();
        client
            .remove_interface(&interfaces, &validated)
            .await
            .unwrap();

        let additional_interface: Interface = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();
        let list_to_add = Interfaces::new()
            .validate_many([interface.clone(), additional_interface.clone()])
            .unwrap();
        client
            .extend_interfaces(&interfaces, &list_to_add)
            .await
            .unwrap();

        let to_remove = HashMap::from([
            (DEVICE_PROPERTIES_NAME, &interface),
            (E2E_DEVICE_PROPERTY_NAME, &additional_interface),
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
        let store = GrpcStore::new(mock_store_client, MemoryStore::new());
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        const PATH: &str = "/string_endpoint";
        const STRING_VALUE: &str = "value";
        let interface = Interface::from_str(crate::test::E2E_DEVICE_DATASTREAM).unwrap();
        let interface_name = interface.interface_name().to_owned();

        mock_client_rx
            .expect_attach::<Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| {
                // no messages are read as responses by the server so we pass an empty vec
                Ok(tonic::Response::new(mock_stream(vec![])))
            });

        let interface_name_cl = interface_name.clone();
        mock_client_tx
            .expect_send::<Request<AstarteMessage>>()
            .times(1)
            .in_sequence(&mut seq)
            .with(predicate::function(move |r: &Request<AstarteMessage>| {
                check_individual_message(
                    r.get_ref(),
                    &interface_name_cl,
                    PATH,
                    AstarteData::String(STRING_VALUE.to_string()),
                )
            }))
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));

        mock_client_tx
            .expect_detach::<Request<()>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));

        let (mut client, _connection) =
            mock_grpc(mock_client_tx, mock_client_rx, Interfaces::new(), store)
                .await
                .unwrap();

        let path = MappingPath::try_from(PATH).unwrap();
        let interfaces = Interfaces::from_iter([interface]);
        let mapping_ref = interfaces.get_individual(&interface_name, &path).unwrap();
        let validated = ValidatedIndividual::validate(
            mapping_ref,
            AstarteData::String(STRING_VALUE.to_string()),
            Some(Utc::now()),
        )
        .unwrap();
        client.send_individual(validated).await.unwrap();
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_object_timestamp() {
        let mut seq = Sequence::new();
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(mock_store_client, MemoryStore::new());
        let mut mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        const PATH: &str = "/1";
        let interface = DatastreamObject::from_str(DEVICE_OBJECT).unwrap();
        let interface_name = interface.name().to_string();

        mock_client_rx
            .expect_attach::<Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_i| {
                // no messages are read as responses by the server so we pass an empty vec
                Ok(tonic::Response::new(mock_stream([])))
            });

        let interface_name_cl = interface_name.clone();
        mock_client_tx
            .expect_send::<Request<AstarteMessage>>()
            .times(1)
            .in_sequence(&mut seq)
            .withf(move |r: &Request<AstarteMessage>| {
                check_object_message(
                    r.get_ref(),
                    &interface_name_cl,
                    PATH,
                    MockDeviceObject::mock_object(),
                )
            })
            .returning(|_i: Request<_>| Ok(tonic::Response::new(())));

        let (mut client, _connection) =
            mock_grpc(mock_client_tx, mock_client_rx, Interfaces::new(), store)
                .await
                .unwrap();

        let path = MappingPath::try_from(PATH).unwrap();
        let validated = ValidatedObject::validate(
            &interface,
            &path,
            MockDeviceObject::mock_object(),
            Some(Utc::now()),
        )
        .unwrap();

        client.send_object(validated).await.unwrap();
    }

    #[tokio::test]
    async fn test_connection_receive_object() {
        let mut seq = Sequence::new();
        let mock_store_client = MsgHubClient::new();
        let store = GrpcStore::new(mock_store_client, MemoryStore::new());
        let mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        const PATH: &str = "/1";
        let interface = Interface::from_str(E2E_SERVER_DATASTREAM).unwrap();
        let interface_name = interface.interface_name().to_owned();
        let proto_payload = ProtoPayload::DatastreamObject(AstarteDatastreamObject {
            data: MockServerObject::mock_object()
                .into_key_values()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            timestamp: None,
        });

        let astarte_message = super::convert::test::new_astarte_message(
            interface_name.clone(),
            PATH.to_string(),
            proto_payload.clone(),
        );

        mock_client_rx
            .expect_attach::<Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_i| {
                Ok(tonic::Response::new(mock_stream([Ok(Some(
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
            mock_grpc(mock_client_tx, mock_client_rx, Interfaces::new(), store)
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
        let store = GrpcStore::new(mock_store_client, MemoryStore::new());
        let mock_client_tx = MsgHubClient::new();
        let mut mock_client_rx = MsgHubClient::new();

        const PATH: &str = "/1/enable";
        let interface = Interface::from_str(crate::test::SERVER_PROPERTIES).unwrap();
        let interface_name = interface.interface_name().to_owned();
        let proto_payload: ProtoPayload =
            ProtoPayload::PropertyIndividual(AstartePropertyIndividual { data: None });
        let astarte_message = super::convert::test::new_astarte_message(
            interface_name.clone(),
            PATH.to_string(),
            proto_payload.clone(),
        );

        mock_client_rx
            .expect_attach::<Request<astarte_message_hub_proto::Node>>()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_i| {
                Ok(tonic::Response::new(mock_stream([Ok(Some(
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
            mock_grpc(mock_client_tx, mock_client_rx, Interfaces::new(), store)
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
}
