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

//! # Astarte MQTT Transport Module
//!
//! This module provides an implementation of the Astarte transport layer using the MQTT protocol.
//! It defines the `Mqtt` struct, which represents an MQTT connection, along with traits for publishing,
//! receiving, and registering interfaces.

pub(crate) mod client;
mod config;
mod connection;
pub mod crypto;
pub mod error;
pub(crate) mod pairing;
pub(crate) mod payload;
pub mod registration;
mod retention;
pub mod topic;

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    future::{Future, IntoFuture},
    sync::Arc,
};

use bytes::Bytes;
use futures::future::Either;
use itertools::Itertools;
use rumqttc::{AckOfPub, ClientError, QoS, SubAck, SubscribeFilter, Token, TokenError};
use tracing::{debug, error, info, trace};

use super::{
    Connection, Disconnect, Publish, Receive, ReceivedEvent, Reconnect, Register, TransportError,
    ValidatedProperty,
};

pub use self::config::Credential;
pub use self::config::MqttConfig;
pub use self::pairing::PairingError;
pub use self::payload::PayloadError;
use crate::{
    aggregate::AstarteObject,
    client::RecvError,
    error::Report,
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
        Ownership, Reliability,
    },
    interfaces::{self, Interfaces, Introspection},
    properties,
    retention::{
        memory::VolatileStore, PublishInfo, RetentionId, StoredRetention, StoredRetentionExt,
    },
    state::SharedState,
    store::{error::StoreError, wrapper::StoreWrapper, PropertyStore, StoreCapabilities},
    validate::{ValidatedIndividual, ValidatedObject, ValidatedUnset},
    AstarteType, Error, Interface, Timestamp,
};
use crate::{retention::RetentionError, store::OptStoredProp};

use self::{
    client::AsyncClient,
    connection::MqttConnection,
    error::MqttError,
    retention::{MqttRetention, RetSender},
    topic::ParsedTopic,
};

/// Default keep alive interval in seconds for the MQTT connection.
pub const DEFAULT_KEEP_ALIVE: u64 = 30;
/// Default connection timeout in seconds for the MQTT connection.
pub const DEFAULT_CONNECTION_TIMEOUT: u64 = 5;

/// Borrowing wrapper for the client id
///
/// To avoid directly allocating and returning a [`String`] each time
/// the client id is needed this trait implements [`Display`]
/// while only borrowing the field needed to construct the client id.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClientId<S = String> {
    pub(crate) realm: S,
    pub(crate) device_id: S,
}

impl ClientId<String> {
    fn as_ref(&self) -> ClientId<&str> {
        ClientId {
            realm: &self.realm,
            device_id: &self.device_id,
        }
    }
}

impl<S> ClientId<S>
where
    S: Display,
{
    /// Create a topic to subscribe on an interface
    fn make_interface_wildcard<T>(&self, interface_name: T) -> String
    where
        T: Display,
    {
        format!("{self}/{interface_name}/#")
    }
}

impl<S> Display for ClientId<S>
where
    S: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.realm, self.device_id)
    }
}

impl From<ClientId<&str>> for ClientId<String> {
    fn from(value: ClientId<&str>) -> Self {
        ClientId {
            realm: value.realm.to_owned(),
            device_id: value.device_id.to_owned(),
        }
    }
}

/// Struct representing an MQTT connection handler for an Astarte device.
///
/// It manages the interaction with the MQTT broker, handling connections, subscriptions, and
/// message publishing following the Astarte protocol.
#[derive(Clone, Debug)]
pub struct MqttClient<S> {
    client_id: ClientId,
    client: AsyncClient,
    retention: RetSender,
    store: StoreWrapper<S>,
    state: Arc<SharedState>,
}

impl<S> MqttClient<S> {
    /// Create a new client.
    pub(crate) fn new(
        client_id: ClientId,
        client: AsyncClient,
        retention: RetSender,
        store: StoreWrapper<S>,
        state: Arc<SharedState>,
    ) -> Self {
        Self {
            client_id,
            client,
            retention,
            store,
            state,
        }
    }

    /// Send a binary payload over this mqtt connection.
    async fn send(
        &self,
        interface: &str,
        path: &str,
        reliability: rumqttc::QoS,
        payload: Vec<u8>,
    ) -> Result<Token<AckOfPub>, MqttError> {
        self.client
            .publish(
                format!("{}/{interface}{path}", self.client_id),
                reliability,
                false,
                payload,
            )
            .await
            .map_err(|err| MqttError::publish("send", err))
    }

    async fn subscribe(&self, interface_name: &str) -> Result<(), MqttError> {
        self.client
            .subscribe(
                self.client_id.make_interface_wildcard(interface_name),
                rumqttc::QoS::ExactlyOnce,
            )
            .await
            .map_err(MqttError::Subscribe)
            .map(drop)
    }

    async fn unsubscribe(&self, interface_name: &str) -> Result<(), MqttError> {
        self.client
            .unsubscribe(self.client_id.make_interface_wildcard(interface_name))
            .await
            .map_err(MqttError::Unsubscribe)
            .map(drop)
    }

    async fn mark_received(&self, id: &RetentionId) -> Result<(), Error>
    where
        S: StoreCapabilities,
    {
        match id {
            RetentionId::Volatile(id) => {
                self.state.volatile_store.mark_received(id).await;
            }
            RetentionId::Stored(id) => {
                if let Some(retention) = self.store.get_retention() {
                    retention.mark_received(id).await?;
                }
            }
        }

        Ok(())
    }

    async fn mark_as_sent(&self, id: &RetentionId) -> Result<(), Error>
    where
        S: StoreCapabilities,
    {
        match id {
            RetentionId::Volatile(id) => {
                self.state.volatile_store.mark_sent(id, true).await;
            }
            RetentionId::Stored(id) => {
                if let Some(retention) = self.store.get_retention() {
                    retention.mark_as_sent(id).await?;
                }
            }
        }

        Ok(())
    }
}

impl<S> Publish for MqttClient<S>
where
    S: StoreCapabilities + Send + Sync,
{
    async fn send_individual(&mut self, validated: ValidatedIndividual) -> Result<(), Error> {
        let buf = payload::serialize_individual(&validated.data, validated.timestamp)
            .map_err(MqttError::Payload)?;

        self.send(
            &validated.interface,
            &validated.path,
            validated.reliability.into(),
            buf,
        )
        .await
        .map(drop)
        .map_err(Error::Mqtt)
    }

    async fn send_property(&mut self, validated: ValidatedProperty) -> Result<(), Error> {
        let buf =
            payload::serialize_individual(&validated.data, None).map_err(MqttError::Payload)?;

        self.send(&validated.interface, &validated.path, QoS::ExactlyOnce, buf)
            .await
            .map(drop)
            .map_err(Error::Mqtt)
    }

    async fn send_object(&mut self, validated: ValidatedObject) -> Result<(), Error> {
        let buf = payload::serialize_object(&validated.data, validated.timestamp)
            .map_err(MqttError::Payload)?;

        self.send(
            &validated.interface,
            &validated.path,
            validated.reliability.into(),
            buf,
        )
        .await
        .map(drop)
        .map_err(Error::Mqtt)
    }

    async fn send_individual_stored(
        &mut self,
        id: RetentionId,
        validated: ValidatedIndividual,
    ) -> Result<(), crate::Error> {
        let buf = payload::serialize_individual(&validated.data, validated.timestamp)
            .map_err(MqttError::Payload)?;

        let notice = self
            .send(
                &validated.interface,
                &validated.path,
                validated.reliability.into(),
                buf,
            )
            .await?;

        debug_assert!(
            !validated.retention.is_discard(),
            "send stored called for retention discard"
        );

        match validated.reliability {
            // Since it's Unreliable we will never know the broker received it
            Reliability::Unreliable => {
                self.mark_received(&id).await?;
            }
            Reliability::Guaranteed | Reliability::Unique => {
                self.mark_as_sent(&id).await?;

                self.retention
                    .send((id, notice))
                    .map_err(|_| Error::Disconnected)?;
            }
        }

        Ok(())
    }

    async fn send_object_stored(
        &mut self,
        id: RetentionId,
        validated: ValidatedObject,
    ) -> Result<(), crate::Error> {
        let buf = payload::serialize_object(&validated.data, validated.timestamp)
            .map_err(MqttError::Payload)?;

        let notice = self
            .send(
                &validated.interface,
                &validated.path,
                validated.reliability.into(),
                buf,
            )
            .await?;

        self.retention
            .send((id, notice))
            .map_err(|_| Error::Disconnected)?;

        Ok(())
    }

    async fn resend_stored(
        &mut self,
        id: RetentionId,
        data: PublishInfo<'_>,
    ) -> Result<(), crate::Error> {
        let notice = self
            .send(
                &data.interface,
                &data.path,
                data.reliability.into(),
                data.value.into(),
            )
            .await?;

        debug_assert!(
            self.store.get_retention().is_some(),
            "resend stored called without store that supports retention"
        );
        match data.reliability {
            // Since it's Unreliable we will never know the broker received it
            Reliability::Unreliable => {
                self.mark_received(&id).await?;
            }
            Reliability::Guaranteed | Reliability::Unique => {
                self.mark_as_sent(&id).await?;

                self.retention
                    .send((id, notice))
                    .map_err(|_| Error::Disconnected)?;
            }
        }

        Ok(())
    }

    async fn unset(&mut self, validated: ValidatedUnset) -> Result<(), Error> {
        // We send an empty vector as payload to unset the property, https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#payload-format
        self.send(
            &validated.interface,
            &validated.path,
            Reliability::Unique.into(),
            Vec::new(),
        )
        .await
        .map(drop)
        .map_err(Error::Mqtt)
    }

    fn serialize_individual(
        &self,
        validated: &ValidatedIndividual,
    ) -> Result<Vec<u8>, crate::Error> {
        payload::serialize_individual(&validated.data, validated.timestamp)
            .map_err(|err| Error::Mqtt(MqttError::Payload(err)))
    }

    fn serialize_object(&self, validated: &ValidatedObject) -> Result<Vec<u8>, crate::Error> {
        payload::serialize_object(&validated.data, validated.timestamp)
            .map_err(|err| Error::Mqtt(MqttError::Payload(err)))
    }
}

impl<S> Register for MqttClient<S>
where
    S: Send + Sync,
{
    async fn add_interface(
        &mut self,
        interfaces: &Interfaces,
        added: &interfaces::Validated,
    ) -> Result<(), Error> {
        if added.ownership().is_server() {
            self.subscribe(added.interface_name()).await?
        }

        let introspection = Introspection::new(interfaces.iter_with_added(added)).to_string();

        self.client
            .send_introspection(self.client_id.as_ref(), introspection)
            .await
            .map_err(|err| MqttError::publish("send introspection", err))?;

        Ok(())
    }

    async fn remove_interface(
        &mut self,
        interfaces: &Interfaces,
        removed: &Interface,
    ) -> Result<(), Error> {
        let iter = interfaces.iter_without_removed(removed);
        let introspection = Introspection::new(iter).to_string();

        self.client
            .send_introspection(self.client_id.as_ref(), introspection)
            .await
            .map_err(|err| MqttError::publish("send introspection", err))?;

        if removed.ownership().is_server() {
            self.unsubscribe(removed.interface_name()).await?;
        }

        Ok(())
    }

    /// Called when multiple interfaces are added.
    ///
    /// This method should convey to the server that one or more interfaces have been added.
    async fn extend_interfaces(
        &mut self,
        interfaces: &Interfaces,
        added: &interfaces::ValidatedCollection,
    ) -> Result<(), crate::Error> {
        let server_interfaces = added
            .values()
            .filter_map(|i| {
                if i.ownership().is_server() {
                    Some(i.interface_name())
                } else {
                    None
                }
            })
            .collect_vec();

        self.client
            .subscribe_interfaces(self.client_id.as_ref(), &server_interfaces)
            .await
            .map_err(MqttError::Subscribe)?;

        let introspection = Introspection::new(interfaces.iter_with_added_many(added)).to_string();

        let res = self
            .client
            .send_introspection(self.client_id.as_ref(), introspection)
            .await
            .map(drop)
            .map_err(|err| MqttError::publish("send introspection", err).into());

        // Cleanup the already subscribed interfaces
        if res.is_err() {
            error!("error while subscribing to interfaces");

            for srv_interface in server_interfaces {
                if let Err(err) = self.unsubscribe(srv_interface).await {
                    error!(
                        error = %Report::new(&err),
                        interface = srv_interface,
                        "failed to unsubscribing to server interface"
                    );
                }
            }
        }

        res
    }

    async fn remove_interfaces(
        &mut self,
        interfaces: &Interfaces,
        removed: &HashMap<&str, &Interface>,
    ) -> Result<(), Error> {
        let interfaces = interfaces.iter_without_removed_many(removed);
        let introspection = Introspection::new(interfaces).to_string();

        self.client
            .send_introspection(self.client_id.as_ref(), introspection)
            .await
            .map_err(|err| MqttError::publish("send introspection", err))?;

        for iface in removed.values() {
            if iface.ownership().is_server() {
                self.unsubscribe(iface.interface_name()).await?;
            }
        }

        Ok(())
    }
}

impl<S> Disconnect for MqttClient<S>
where
    S: Send,
{
    async fn disconnect(&mut self) -> Result<(), crate::Error> {
        self.client
            .disconnect()
            .await
            .map(drop)
            .map_err(MqttError::Disconnect)?;

        info!("disconnect packet sent");

        Ok(())
    }
}

impl<S> Display for MqttClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Mqtt Client {}", self.client_id)
    }
}

/// Handles the MQTT connection between a device and Astarte.
///
///  It manages the interaction with the MQTT broker, handling connections, subscriptions, and
///  message publishing following the Astarte protocol.
pub struct Mqtt<S> {
    client_id: ClientId,
    connection: MqttConnection,
    retention: MqttRetention,
    store: StoreWrapper<S>,
    state: Arc<SharedState>,
}

impl<S> Mqtt<S> {
    /// Creates a new MQTT connection struct.
    fn new(
        client_id: ClientId,
        connection: MqttConnection,
        retention: MqttRetention,
        store: StoreWrapper<S>,
        state: Arc<SharedState>,
    ) -> Self {
        Self {
            client_id,
            connection,
            retention,
            store,
            state,
        }
    }

    /// Marks the packets as received for the retention.
    async fn mark_packet_received(
        volatile: &VolatileStore,
        stored: &impl StoreCapabilities,
        res_id: Result<RetentionId, TokenError>,
    ) -> Result<(), RetentionError>
    where
        S: StoreCapabilities,
    {
        let id = match res_id {
            Ok(id) => id,
            Err(err) => {
                error!(error=%Report::new(err), "notice error while waiting for packet");

                return Ok(());
            }
        };

        trace!("received packet {id}");

        match id {
            RetentionId::Volatile(id) => {
                volatile.mark_received(&id).await;
            }
            RetentionId::Stored(id) => {
                if let Some(retention) = stored.get_retention() {
                    retention.mark_received(&id).await?;
                }
            }
        }

        debug!("marked {id} as received");

        Ok(())
    }

    async fn poll(&mut self) -> Result<Option<rumqttc::Publish>, TransportError>
    where
        S: StoreCapabilities,
    {
        if self.retention.is_empty() {
            return Ok(self.connection.next_publish().await);
        }

        loop {
            let mut conn_future = std::pin::pin!(self.connection.next_publish());

            match futures::future::select(self.retention.into_future(), &mut conn_future).await {
                Either::Left((res, _)) => {
                    Self::mark_packet_received(&self.state.volatile_store, &self.store, res)
                        .await
                        .map_err(|err| TransportError::Transport(Error::Retention(err)))?;
                }
                // the retention future can be dropped safely
                Either::Right((publish, _)) => {
                    return Ok(publish);
                }
            };
        }
    }
}

/// Trait to implement functionality on the store.
trait MqttStoreExt: PropertyStore
where
    Error: From<Self::Err>,
{
    /// This function deletes all the stored server owned properties after receiving a publish on
    /// `/control/consumer/properties`
    async fn purge_server_properties(&self, bdata: &[u8]) -> Result<(), Error> {
        let paths = properties::extract_set_properties(bdata)?;

        let stored_props = self.server_props().await?;

        for ref stored_prop in stored_props {
            if paths.contains(&format!("{}{}", stored_prop.interface, stored_prop.path)) {
                continue;
            }

            self.delete_prop(&stored_prop.into()).await?;
        }

        Ok(())
    }
}

impl<S> MqttStoreExt for StoreWrapper<S> where S: PropertyStore {}

impl<S> Receive for Mqtt<S>
where
    S: StoreCapabilities + PropertyStore,
{
    type Payload = Bytes;

    async fn next_event(&mut self) -> Result<Option<ReceivedEvent<Self::Payload>>, TransportError>
    where
        S: PropertyStore,
    {
        // Wait for next data or until it's disconnected
        while let Some(publish) = self.poll().await? {
            debug!("Incoming publish = {} {:x}", publish.topic, publish.payload);

            let publish_topic = ParsedTopic::try_parse(self.client_id.as_ref(), &publish.topic)
                .map_err(|err| RecvError::mqtt_connection_error(MqttError::Topic(err)))?;

            match publish_topic {
                ParsedTopic::PurgeProperties => {
                    debug!("Purging properties");

                    self.store
                        .purge_server_properties(&publish.payload)
                        .await
                        .map_err(TransportError::Transport)?;
                }
                ParsedTopic::InterfacePath { interface, path } => {
                    return Ok(Some(ReceivedEvent {
                        interface: interface.to_string(),
                        path: path.to_string(),
                        payload: publish.payload,
                    }));
                }
            }
        }

        Ok(None)
    }

    fn deserialize_property(
        &self,
        mapping: &MappingRef<'_, &Interface>,
        payload: Self::Payload,
    ) -> Result<Option<AstarteType>, TransportError> {
        payload::deserialize_property(mapping, &payload).map_err(|err| {
            TransportError::Recv(RecvError::mqtt_connection_error(MqttError::Payload(err)))
        })
    }

    fn deserialize_individual(
        &self,
        mapping: &MappingRef<'_, &Interface>,
        payload: Self::Payload,
    ) -> Result<(AstarteType, Option<Timestamp>), TransportError> {
        payload::deserialize_individual(mapping, &payload).map_err(|err| {
            TransportError::Recv(RecvError::mqtt_connection_error(MqttError::Payload(err)))
        })
    }

    fn deserialize_object(
        &self,
        object: &ObjectRef,
        path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(AstarteObject, Option<Timestamp>), TransportError> {
        payload::deserialize_object(object, path, &payload).map_err(|err| {
            TransportError::Recv(RecvError::mqtt_connection_error(MqttError::Payload(err)))
        })
    }
}

impl<S> Reconnect for Mqtt<S>
where
    S: StoreCapabilities + PropertyStore,
{
    async fn reconnect(&mut self, interfaces: &Interfaces) -> Result<bool, crate::Error> {
        self.connection
            .reconnect(self.client_id.as_ref(), interfaces, &self.store)
            .await
            .map_err(|err| Error::Mqtt(MqttError::Poll(err)))
    }
}

impl<S> Connection for Mqtt<S>
where
    S: StoreCapabilities,
{
    type Sender = MqttClient<S>;
    type Store = S;
}

/// Wrapper structs that holds data used when connecting/reconnecting
pub(crate) struct SessionData {
    interfaces: String,
    server_interfaces: Vec<String>,
    device_properties: Vec<OptStoredProp>,
}

impl SessionData {
    fn filter_server_interfaces(interfaces: &Interfaces) -> Vec<String> {
        interfaces
            .iter()
            .filter(|interface| interface.ownership() == Ownership::Server)
            .map(|interface| interface.interface_name().to_owned())
            .collect()
    }

    pub(crate) async fn try_from_props<S>(
        interfaces: &Interfaces,
        store: &S,
    ) -> Result<Self, StoreError>
    where
        S: PropertyStore<Err = StoreError>,
    {
        let mut device_properties = store.device_props_with_unset().await?;
        // Filter interfaces that are missing or have been updated
        device_properties.retain(|prop| {
            interfaces
                .get(&prop.interface)
                .is_some_and(|interface| interface.version_major() == prop.interface_major)
        });

        let server_interfaces = Self::filter_server_interfaces(interfaces);

        Ok(Self {
            interfaces: interfaces.get_introspection_string(),
            server_interfaces,
            device_properties,
        })
    }
}

trait AsyncClientExt {
    /// Sends the introspection [`String`].
    fn send_introspection(
        &self,
        client_id: ClientId<&str>,
        introspection: String,
    ) -> impl Future<Output = Result<Token<AckOfPub>, ClientError>> + Send;

    /// Subscribe to many interfaces
    fn subscribe_interfaces<S>(
        &self,
        client_id: ClientId<&str>,
        interfaces_names: &[S],
    ) -> impl Future<Output = Result<Option<Token<SubAck>>, ClientError>> + Send
    where
        S: Display + Debug + Send + Sync;
}

impl AsyncClientExt for AsyncClient {
    async fn send_introspection(
        &self,
        client_id: ClientId<&str>,
        introspection: String,
    ) -> Result<Token<AckOfPub>, ClientError> {
        debug!("sending introspection: {introspection}");

        let path = client_id.to_string();

        self.publish(path, QoS::ExactlyOnce, false, introspection)
            .await
    }

    /// Subscribe to many interfaces
    async fn subscribe_interfaces<S>(
        &self,
        client_id: ClientId<&str>,
        interfaces_names: &[S],
    ) -> Result<Option<Token<SubAck>>, ClientError>
    where
        S: Display + Debug + Send + Sync,
    {
        // should not subscribe if there are no interfaces
        if interfaces_names.is_empty() {
            debug!("empty subscribe many");

            return Ok(None);
        } else if interfaces_names.len() == 1 {
            trace!("subscribing on single interface");

            let name = &interfaces_names[0];

            return self
                .subscribe(
                    client_id.make_interface_wildcard(name),
                    rumqttc::QoS::ExactlyOnce,
                )
                .await
                .map(Some);
        }

        trace!("subscribing on {interfaces_names:?}");

        let topics = interfaces_names
            .iter()
            .map(|name| SubscribeFilter {
                path: client_id.make_interface_wildcard(name),
                qos: rumqttc::QoS::ExactlyOnce,
            })
            .collect_vec();

        debug!("topics {topics:?}");

        self.subscribe_many(topics).await.map(Some)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{str::FromStr, time::Duration};

    use mockall::predicate;
    use mockito::Server;
    use properties::extract_set_properties;
    use rumqttc::{
        ClientError, ConnAck, ConnectReturnCode, ConnectionError, Event as MqttEvent, Packet, QoS,
        Resolver,
    };
    use tempfile::TempDir;
    use test::{
        client::NEW_LOCK,
        pairing::tests::{mock_create_certificate, mock_get_broker_url},
    };

    use crate::{
        builder::{BuildConfig, ConnectionConfig, DeviceBuilder, DEFAULT_VOLATILE_CAPACITY},
        store::memory::MemoryStore,
    };

    use self::{
        client::{AsyncClient, EventLoop},
        config::transport::TransportProvider,
    };

    use super::*;

    pub(crate) fn notify_success<T, E>(out: T) -> Result<Token<T>, E> {
        let (tx, token) = Resolver::new();

        tx.resolve(out);

        Ok(token)
    }

    pub(crate) async fn mock_mqtt_connection<S>(
        client: AsyncClient,
        eventloop: EventLoop,
        store: S,
    ) -> (MqttClient<S>, Mqtt<S>)
    where
        S: Clone,
    {
        let client_id: ClientId = ClientId {
            realm: "realm",
            device_id: "device_id",
        }
        .into();

        let (ret_tx, ret_rx) = flume::unbounded();

        let store = StoreWrapper::new(store);

        let transport_provider = TransportProvider::configure(
            "http://api.astarte.localhost/pairing".parse().unwrap(),
            "secret".to_string(),
            None,
            true,
        )
        .await
        .expect("failed to configure transport provider");

        let state = Arc::new(SharedState::new(
            Interfaces::new(),
            VolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY),
        ));

        let mqtt = Mqtt::new(
            client_id.clone(),
            MqttConnection::new(
                client.clone(),
                eventloop,
                transport_provider,
                self::connection::Connected,
            ),
            MqttRetention::new(ret_rx),
            store.clone(),
            Arc::clone(&state),
        );

        let mqtt_client = MqttClient::new(client_id, client, ret_tx, store, state);

        (mqtt_client, mqtt)
    }

    #[tokio::test]
    async fn should_extend_interfaces() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = [
            Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(crate::test::DEVICE_OBJECT).unwrap(),
            Interface::from_str(crate::test::SERVER_INDIVIDUAL).unwrap(),
        ];

        let mut introspection = Introspection::new(to_add.iter())
            .to_string()
            .split(';')
            .map(ToOwned::to_owned)
            .collect_vec();

        introspection.sort_unstable();

        let interfaces = Interfaces::new();

        let to_add = interfaces.validate_many(to_add).unwrap();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .once()
            .returning(AsyncClient::default);

        client
            .expect_subscribe::<String>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()),
                predicate::eq( QoS::ExactlyOnce)
            )
            .in_sequence(&mut seq)
            .returning(|_, _| notify_success(SubAck::new(0, Vec::new())));

        client
            .expect_publish::<String, String>()
            .once()
            .in_sequence(&mut seq)
            .withf(move |publish, _, _, payload| {
                let mut intro = payload.split(';').collect_vec();

                intro.sort_unstable();

                publish == "realm/device_id" && intro == introspection
            })
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let (mut client, _mqtt_connection) =
            mock_mqtt_connection(client, eventl, MemoryStore::new()).await;

        client
            .extend_interfaces(&interfaces, &to_add)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn should_not_subscribe_many() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        // no server owned interfaces are present
        let to_add = [
            Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(crate::test::DEVICE_OBJECT).unwrap(),
        ];

        let mut introspection = Introspection::new(to_add.iter())
            .to_string()
            .split(';')
            .map(ToOwned::to_owned)
            .collect_vec();

        introspection.sort_unstable();

        let interfaces = Interfaces::new();

        let to_add = interfaces.validate_many(to_add).unwrap();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .once()
            .returning(AsyncClient::default);

        // in this case, no client.subscribe_many() is expected
        client
            .expect_publish::<String, String>()
            .once()
            .in_sequence(&mut seq)
            .withf(move |publish, qos, _, payload| {
                let mut intro = payload.split(';').collect_vec();

                intro.sort_unstable();

                publish == "realm/device_id" && intro == introspection && *qos == QoS::ExactlyOnce
            })
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let (mut client, _connection) =
            mock_mqtt_connection(client, eventl, MemoryStore::new()).await;

        client
            .extend_interfaces(&interfaces, &to_add)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn should_unsubscribe_on_extend_err() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = [Interface::from_str(crate::test::SERVER_PROPERTIES).unwrap()];

        let introspection = Introspection::new(to_add.iter()).to_string();

        let interfaces = Interfaces::new();

        let to_add = interfaces.validate_many(to_add).unwrap();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .once()
            .returning(AsyncClient::default);

        client
            .expect_subscribe::<String>()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.ServerProperties/#".to_string()),
                predicate::eq(QoS::ExactlyOnce ),
            )
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| notify_success(SubAck::new(0, Vec::new())));

        client
            .expect_publish::<String, String>()
            .once()
            .withf(move |publish, _, _, payload| {
                publish == "realm/device_id" && *payload == introspection
            })
            .returning(|_, _, _, _| {
                // Random error
                Err(ClientError::Request(rumqttc::Request::Disconnect(
                    Resolver::new().0,
                )))
            });

        client
            .expect_unsubscribe::<String>()
            .once()
            .withf(move |topic| topic == "realm/device_id/org.astarte-platform.rust.examples.individual-properties.ServerProperties/#")
            .returning(|_| {
                // We are disconnected so we cannot unsubscribe
                Err(ClientError::Request(rumqttc::Request::Disconnect(Resolver::new().0)))
            });

        let (mut mqtt_client, _mqtt_connection) =
            mock_mqtt_connection(client, eventl, MemoryStore::new()).await;

        mqtt_client
            .extend_interfaces(&interfaces, &to_add)
            .await
            .expect_err("Didn't return the error");
    }

    #[tokio::test]
    async fn should_reconnect() {
        let _m = NEW_LOCK.lock().await;

        let dir = TempDir::new().unwrap();

        let ctx = AsyncClient::new_context();
        ctx.expect().once().returning(|_, _| {
            let mut client = AsyncClient::default();
            let mut ev_loop = EventLoop::default();

            let mut seq = mockall::Sequence::new();

            ev_loop
                .expect_set_network_options()
                .once()
                .in_sequence(&mut seq)
                .returning(|_| EventLoop::default());

            client
                .expect_clone()
                .once()
                .in_sequence(&mut seq)
                .returning(|| {
                    let mut client = AsyncClient::default();

                    let mut seq = mockall::Sequence::new();

                    client
                        .expect_clone()
                        .once()
                        .in_sequence(&mut seq)
                        .returning(|| {
                            let mut client = AsyncClient::default();

                            let mut seq = mockall::Sequence::new();

                            client
                                .expect_subscribe::<String>()
                                .with(
                                    predicate::eq(
                                        "realm/device_id/control/consumer/properties".to_string(),
                                    ),
                                    predicate::eq(QoS::ExactlyOnce),
                                )
                                .once()
                                .in_sequence(&mut seq)
                                .returning(|_, _| notify_success(SubAck::new(0, Vec::new())));

                            client
                                .expect_publish::<String, String>()
                                .with(
                                    predicate::eq("realm/device_id".to_string()),
                                    predicate::eq(QoS::ExactlyOnce),
                                    predicate::eq(false),
                                    predicate::eq(String::new()),
                                )
                                .once()
                                .in_sequence(&mut seq)
                                .returning(|_, _, _, _| notify_success(AckOfPub::None));

                            client
                                .expect_publish::<String, &str>()
                                .once()
                                .in_sequence(&mut seq)
                                .with(
                                    predicate::eq("realm/device_id/control/emptyCache".to_string()),
                                    predicate::eq(QoS::ExactlyOnce),
                                    predicate::eq(false),
                                    predicate::eq("1"),
                                )
                                .returning(|_, _, _, _| notify_success(AckOfPub::None));

                            client
                                .expect_publish::<String, Vec<u8>>()
                                .with(
                                    predicate::eq(
                                        "realm/device_id/control/producer/properties".to_string(),
                                    ),
                                    predicate::eq(QoS::ExactlyOnce),
                                    predicate::eq(false),
                                    predicate::function(|payload: &Vec<u8>| {
                                        extract_set_properties(payload).unwrap().is_empty()
                                    }),
                                )
                                .once()
                                .in_sequence(&mut seq)
                                .returning(|_, _, _, _| notify_success(AckOfPub::None));

                            client
                        });

                    client
                });

            ev_loop
                .expect_poll()
                .once()
                .in_sequence(&mut seq)
                .returning(|| {
                    Box::pin(async {
                        Err(ConnectionError::Tls(rumqttc::TlsError::TLS(
                            rustls::Error::AlertReceived(
                                rustls::AlertDescription::CertificateExpired,
                            ),
                        )))
                    })
                });

            // First clean for the good connection
            ev_loop
                .expect_clean()
                .once()
                .in_sequence(&mut seq)
                .return_const(());

            ev_loop
                .expect_poll()
                .once()
                .in_sequence(&mut seq)
                .returning(|| {
                    Box::pin(async {
                        tokio::task::yield_now().await;

                        Ok(MqttEvent::Incoming(Packet::ConnAck(ConnAck {
                            session_present: false,
                            code: ConnectReturnCode::Success,
                        })))
                    })
                });

            // This guaranties we can keep polling while we are waiting for the ACKs.
            ev_loop.expect_poll().returning(|| {
                Box::pin(async {
                    tokio::task::yield_now().await;

                    Ok(MqttEvent::Outgoing(rumqttc::Outgoing::Publish(0)))
                })
            });

            (client, ev_loop)
        });

        let mut server = Server::new_async().await;

        let mock_url = mock_get_broker_url(&mut server).create_async().await;
        let mock_cert = mock_create_certificate(&mut server)
            .expect(2)
            .create_async()
            .await;

        let builder = DeviceBuilder::new().store_dir(dir.path()).await.unwrap();

        let config = MqttConfig::new(
            "realm",
            "device_id",
            Credential::secret("secret"),
            server.url(),
        );

        tokio::time::timeout(
            Duration::from_secs(3),
            config.connect(BuildConfig {
                store: builder.store,
                channel_size: builder.channel_size,
                writable_dir: builder.writable_dir,
                state: Arc::new(SharedState::new(
                    builder.interfaces,
                    VolatileStore::with_capacity(builder.volatile_retention),
                )),
            }),
        )
        .await
        .expect("timeout expired")
        .unwrap();

        mock_url.assert_async().await;
        mock_cert.assert_async().await;
    }
}
