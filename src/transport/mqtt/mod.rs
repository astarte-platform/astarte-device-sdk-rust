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

//! # Astarte MQTT Transport Module
//!
//! This module provides an implementation of the Astarte transport layer using the MQTT protocol.
//! It defines the `Mqtt` struct, which represents an MQTT connection, along with traits for publishing,
//! receiving, and registering interfaces.

pub(crate) mod client;
pub(crate) mod components;
pub(crate) mod config;
pub(crate) mod connection;
pub mod crypto;
pub mod error;
pub(crate) mod payload;
pub(crate) mod retention;
pub mod topic;

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::{Future, IntoFuture};
use std::ops::ControlFlow;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use astarte_interfaces::schema::{Ownership, Reliability};
use astarte_interfaces::{
    DatastreamIndividual, DatastreamObject, Interface, MappingPath, Properties,
};
use bytes::Bytes;
use futures::{TryFutureExt, future::Either};
use itertools::Itertools;
use rumqttc::{AckOfPub, ClientError, QoS, Token, TokenError};
use tracing::{debug, error, info, trace};

use super::{
    Connection, Disconnect, Publish, Receive, ReceivedEvent, Register, TransportError,
    ValidatedProperty,
};

use self::config::transport::TransportProvider;
use self::connection::context::Ctx;
use self::connection::{ConnError, MqttState};
use self::payload::PayloadError;
use crate::aggregate::AstarteObject;
use crate::client::RecvError;
use crate::error::Report;
use crate::interfaces::{self, DeviceIntrospection, Interfaces, MappingRef};
use crate::pairing::Pairing;
use crate::pairing::api::PairingError;
use crate::properties;
use crate::retention::RetentionError;
use crate::retention::{PublishInfo, RetentionId, StoredRetention, memory::VolatileStore};
use crate::session::{IntrospectionInterface, StoredSession};
use crate::state::SharedState;
use crate::store::{
    OptStoredProp, PropertyState, PropertyStore, StoreCapabilities, wrapper::StoreWrapper,
};
use crate::transport::AttemptStatus;
use crate::validate::{ValidatedIndividual, ValidatedObject, ValidatedUnset};
use crate::{AstarteData, Error, Timestamp};

use self::{
    client::AsyncClient,
    components::{ClientId, to_qos},
    error::MqttError,
    retention::{MqttRetention, RetSender},
    topic::ParsedTopic,
};

pub use self::config::Credential;
pub use self::config::MqttArgs;
pub use self::config::MqttConfig;

/// Default keep alive interval in seconds for the MQTT connection.
pub const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub(crate) struct ClientSender {
    id: ClientId,
    client: AsyncClient,
}

/// Struct representing an MQTT connection handler for an Astarte device.
///
/// It manages the interaction with the MQTT broker, handling connections, subscriptions, and
/// message publishing following the Astarte protocol.
#[derive(Debug, Clone)]
pub struct MqttClient<S> {
    pub(crate) sender: Arc<OnceLock<ClientSender>>,
    retention: RetSender,
    store: StoreWrapper<S>,
    state: Arc<SharedState>,
}

impl<S> MqttClient<S> {
    /// Creates a new client that is missing the transport
    pub(crate) fn new(
        retention: RetSender,
        store: StoreWrapper<S>,
        state: Arc<SharedState>,
    ) -> Self {
        Self {
            sender: Arc::new(OnceLock::new()),
            retention,
            store,
            state,
        }
    }

    fn get_client(&self) -> Result<&ClientSender, MqttError> {
        self.sender.get().ok_or(MqttError::NoClient)
    }

    /// Send a binary payload over this mqtt connection.
    async fn send(
        &self,
        interface: &str,
        path: &str,
        reliability: rumqttc::QoS,
        payload: Vec<u8>,
    ) -> Result<Token<AckOfPub>, MqttError> {
        let sender = self.get_client()?;

        self.apply_timeout(
            sender
                .client
                .publish(
                    format!("{}/{interface}{path}", &sender.id),
                    reliability,
                    false,
                    payload,
                )
                .map_err(|err| MqttError::publish("send", err)),
        )
        .await
    }

    async fn subscribe(&self, interface_name: &str) -> Result<(), MqttError> {
        let sender = self.get_client()?;

        self.apply_timeout(
            sender
                .client
                .subscribe(
                    sender.id.make_interface_wildcard(interface_name),
                    rumqttc::QoS::ExactlyOnce,
                )
                .map_err(MqttError::Subscribe),
        )
        .await
        .map(drop)
    }

    async fn unsubscribe(&self, interface_name: &str) -> Result<(), MqttError> {
        let sender = self.get_client()?;

        self.apply_timeout(
            sender
                .client
                .unsubscribe(sender.id.make_interface_wildcard(interface_name))
                .map_err(MqttError::Unsubscribe),
        )
        .await
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

    async fn mark_sent(
        &self,
        id: RetentionId,
        reliability: Reliability,
        notice: Token<AckOfPub>,
    ) -> Result<(), crate::Error>
    where
        S: StoreCapabilities,
    {
        match reliability {
            // Since it's Unreliable we will never know the broker received it
            Reliability::Unreliable => {
                self.mark_received(&id).await?;
            }
            Reliability::Guaranteed | Reliability::Unique => {
                self.retention
                    .send((id, notice))
                    .await
                    .map_err(|_| Error::Disconnected)?;
            }
        }

        Ok(())
    }

    async fn extend_interfaces_await_pub(
        &self,
        res: Result<Token<AckOfPub>, MqttError>,
    ) -> Result<(), MqttError> {
        let Ok(token) = res else {
            error!("error while subscribing to interfaces");
            return res.map(drop);
        };

        let ack_result = self
            .apply_timeout(token.map_err(MqttError::PubAckToken))
            .await;
        let Ok(_) = ack_result else {
            error!("error in ack reception while subscribing to interfaces");
            return ack_result.map(drop);
        };

        Ok(())
    }

    #[inline]
    async fn apply_timeout<F, T>(&self, fut: F) -> Result<T, MqttError>
    where
        F: Future<Output = Result<T, MqttError>>,
    {
        tokio::time::timeout(self.state.config.send_timeout, fut)
            .await
            .map_err(MqttError::Timeout)?
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
            to_qos(validated.reliability),
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
            to_qos(validated.reliability),
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
        debug_assert!(
            !validated.retention.is_discard(),
            "send stored called for retention discard"
        );

        let buf = payload::serialize_individual(&validated.data, validated.timestamp)
            .map_err(MqttError::Payload)?;

        let notice = self
            .send(
                &validated.interface,
                &validated.path,
                to_qos(validated.reliability),
                buf,
            )
            .await?;

        self.mark_sent(id, validated.reliability, notice).await?;

        Ok(())
    }

    async fn send_object_stored(
        &mut self,
        id: RetentionId,
        validated: ValidatedObject,
    ) -> Result<(), crate::Error> {
        debug_assert!(
            !validated.retention.is_discard(),
            "send stored called for retention discard"
        );

        let buf = payload::serialize_object(&validated.data, validated.timestamp)
            .map_err(MqttError::Payload)?;

        let notice = self
            .send(
                &validated.interface,
                &validated.path,
                to_qos(validated.reliability),
                buf,
            )
            .await?;

        self.mark_sent(id, validated.reliability, notice).await?;

        Ok(())
    }

    async fn resend_stored(
        &mut self,
        id: RetentionId,
        data: PublishInfo<'_>,
    ) -> Result<(), crate::Error> {
        debug_assert!(
            self.store.get_retention().is_some(),
            "resend stored called without store that supports retention"
        );

        let notice = self
            .send(
                &data.interface,
                &data.path,
                to_qos(data.reliability),
                data.value.into(),
            )
            .await?;

        self.mark_sent(id, data.reliability, notice).await?;

        Ok(())
    }

    /// Resend previously stored property.
    async fn resend_stored_property(
        &mut self,
        property_data: OptStoredProp,
    ) -> Result<(), crate::Error> {
        let buf = property_data
            .value
            .as_ref()
            .map(|d| payload::serialize_individual(d, None))
            .unwrap_or(Ok(Vec::new()))
            .map_err(MqttError::Payload)?;

        self.send(
            &property_data.interface,
            &property_data.path,
            QoS::ExactlyOnce,
            buf,
        )
        .await
        .map(drop)
        .map_err(Error::Mqtt)
    }

    async fn unset(&mut self, validated: ValidatedUnset) -> Result<(), Error> {
        // We send an empty vector as payload to unset the property, https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#payload-format
        self.send(
            &validated.interface,
            &validated.path,
            QoS::ExactlyOnce,
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
    S: StoreCapabilities + Send + Sync,
{
    async fn add_interface(
        &mut self,
        interfaces: &Interfaces,
        added: &interfaces::Validated,
    ) -> Result<(), Error> {
        if added.ownership().is_server() {
            self.subscribe(added.interface_name()).await?
        }

        let introspection = DeviceIntrospection::new(interfaces.iter_with_added(added)).to_string();

        let sender = self.get_client()?;

        self.apply_timeout(
            sender
                .client
                .send_introspection(sender.id.as_ref(), introspection)
                .map_err(|err| MqttError::publish("send introspection", err)),
        )
        .await?
        .await
        .map_err(MqttError::PubAckToken)?;

        if let Some(session) = self.store.get_session() {
            let interface: IntrospectionInterface<&str> = added.interface().into();
            session.add_interfaces(&[interface]).await?;
        }

        Ok(())
    }

    async fn remove_interface(
        &mut self,
        interfaces: &Interfaces,
        removed: &Interface,
    ) -> Result<(), Error> {
        let iter = interfaces.iter_without_removed(removed);
        let introspection = DeviceIntrospection::new(iter).to_string();

        let sender = self.get_client()?;
        self.apply_timeout(
            sender
                .client
                .send_introspection(sender.id.as_ref(), introspection)
                .map_err(|err| MqttError::publish("send introspection", err)),
        )
        .await?
        .await
        .map_err(MqttError::PubAckToken)?;

        if let Some(session) = self.store.get_session() {
            let interface: IntrospectionInterface<&str> = removed.into();
            session.remove_interfaces(&[interface]).await?;
        }

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

        let sender = self.get_client()?;
        self.apply_timeout(
            sender
                .client
                .subscribe_interfaces(sender.id.as_ref(), &server_interfaces)
                .map_err(MqttError::Subscribe),
        )
        .await?;

        let introspection =
            DeviceIntrospection::new(interfaces.iter_with_added_many(added)).to_string();

        let sender = self.get_client()?;
        let res = self
            .apply_timeout(
                sender
                    .client
                    .send_introspection(sender.id.as_ref(), introspection)
                    .map_err(|err| MqttError::publish("send introspection", err)),
            )
            .await;

        let res = self
            .extend_interfaces_await_pub(res)
            .await
            .map_err(Into::into);

        if res.is_ok() {
            if let Some(session) = self.store.get_session() {
                let added: Vec<IntrospectionInterface<&str>> =
                    added.iter_interfaces().map(|i| i.into()).collect();

                session.add_interfaces(&added).await?;
            }
        } else {
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
        let introspection = DeviceIntrospection::new(interfaces).to_string();

        let sender = self.get_client()?;
        self.apply_timeout(
            sender
                .client
                .send_introspection(sender.id.as_ref(), introspection)
                .map_err(|err| MqttError::publish("send introspection", err)),
        )
        .await?
        .await
        .map_err(MqttError::PubAckToken)?;

        if let Some(session) = self.store.get_session() {
            let removed: Vec<IntrospectionInterface<&str>> =
                removed.values().map(|&i| i.into()).collect();

            session.remove_interfaces(&removed).await?;
        }

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
        let Some(client) = self.sender.get() else {
            info!("disconnecting while never connected, the mqtt client was not created");
            return Ok(());
        };

        client
            .client
            .disconnect()
            .await
            .map(drop)
            .map_err(MqttError::Disconnect)?;

        info!("disconnect packet sent");

        Ok(())
    }
}

/// Handles the MQTT connection between a device and Astarte.
///
///  It manages the interaction with the MQTT broker, handling connections, subscriptions, and
///  message publishing following the Astarte protocol.
pub struct Mqtt<S, P> {
    pub(crate) connection: MqttState<P>,
    pub(crate) client_sender: Arc<OnceLock<ClientSender>>,
    pub(crate) provider: TransportProvider,
    pub(crate) retention: MqttRetention,
    pub(crate) store: StoreWrapper<S>,
    pub(crate) state: Arc<SharedState>,
}

impl<S, P> Mqtt<S, P> {
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

    /// This function deletes all the stored server owned properties after receiving a publish on
    /// `/control/consumer/properties`
    async fn purge_server_properties(&self, bdata: &[u8]) -> Result<(), Error>
    where
        S: PropertyStore,
    {
        let paths = properties::extract_set_properties(bdata)?;

        let stored_props = self.store.server_props().await?;

        for stored_prop in &stored_props {
            if paths.contains(&format!("{}{}", stored_prop.interface, stored_prop.path)) {
                continue;
            }

            self.store.delete_prop(&stored_prop.into()).await?;
        }

        Ok(())
    }

    async fn handle_publish(
        &self,
        publish: rumqttc::Publish,
    ) -> Result<Option<ReceivedEvent<Bytes>>, TransportError>
    where
        S: PropertyStore,
        P: Pairing,
    {
        let sender = self
            .client_sender
            .get()
            .ok_or(TransportError::Transport(Error::Mqtt(
                MqttError::Connection(ConnError::State),
            )))?;

        let publish_topic = ParsedTopic::try_parse(sender.id.as_ref(), &publish.topic)
            .map_err(|err| RecvError::mqtt_connection_error(MqttError::Topic(err)))?;

        match publish_topic {
            ParsedTopic::PurgeProperties => {
                debug!("Purging properties");

                self.purge_server_properties(&publish.payload)
                    .await
                    .map_err(TransportError::Transport)?;

                Ok(None)
            }
            ParsedTopic::InterfacePath { interface, path } => Ok(Some(ReceivedEvent {
                interface: interface.to_string(),
                path: path.to_string(),
                payload: publish.payload,
            })),
        }
    }
}

impl<S, P> Receive for Mqtt<S, P>
where
    S: StoreCapabilities + PropertyStore,
    P: Pairing + Send,
{
    type Payload = Bytes;

    async fn next_event(&mut self) -> Result<Option<ReceivedEvent<Self::Payload>>, TransportError>
    where
        S: PropertyStore,
    {
        // Wait for next data or until it's disconnected
        while let Some(publish) = self.poll().await? {
            debug!("Incoming publish = {} {:x}", publish.topic, publish.payload);

            if let Some(event) = self.handle_publish(publish).await? {
                return Ok(Some(event));
            }
        }

        Ok(None)
    }

    async fn reconnect(
        &mut self,
        interfaces: &Interfaces,
    ) -> Result<AttemptStatus<Self::Payload>, TransportError> {
        let mut ctx = Ctx {
            sender: &self.client_sender,
            state: &self.state,
            provider: &self.provider,
            store: &self.store,
            interfaces,
            session_synced: false,
        };

        loop {
            let result = self.connection.reconnect(&mut ctx).await;

            match result {
                Ok(ControlFlow::Continue(publish)) => {
                    if let Some(event) = self.handle_publish(publish).await? {
                        return Ok(AttemptStatus::ReceivedEvent(event));
                    }
                }
                Ok(ControlFlow::Break(session_present)) => {
                    if !session_present {
                        // if the session is not present we discard previously stored packets notices
                        let received = self.retention.drain_filter_acked();

                        // mark received packets
                        for id in received {
                            Self::mark_packet_received(
                                &self.state.volatile_store,
                                &self.store,
                                Ok(id),
                            )
                            .await
                            .map_err(|error| TransportError::Transport(Error::Retention(error)))?;
                        }
                    }

                    return Ok(AttemptStatus::Connected { session_present });
                }
                Err(error) => {
                    error!(%error, "couldn't connect to Astarte");

                    return Ok(AttemptStatus::Disconnected);
                }
            }
        }
    }

    fn deserialize_property(
        &self,
        mapping: &MappingRef<'_, Properties>,
        payload: Self::Payload,
    ) -> Result<Option<AstarteData>, TransportError> {
        payload::deserialize_property(mapping, &payload).map_err(|err| {
            TransportError::Recv(RecvError::mqtt_connection_error(MqttError::Payload(err)))
        })
    }

    fn deserialize_individual(
        &self,
        mapping: &MappingRef<'_, DatastreamIndividual>,
        payload: Self::Payload,
    ) -> Result<(AstarteData, Option<Timestamp>), TransportError> {
        payload::deserialize_individual(mapping, &payload).map_err(|err| {
            TransportError::Recv(RecvError::mqtt_connection_error(MqttError::Payload(err)))
        })
    }

    fn deserialize_object(
        &self,
        object: &DatastreamObject,
        path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(AstarteObject, Option<Timestamp>), TransportError> {
        payload::deserialize_object(object, path, &payload).map_err(|err| {
            TransportError::Recv(RecvError::mqtt_connection_error(MqttError::Payload(err)))
        })
    }
}

impl<S, P> Connection for Mqtt<S, P>
where
    S: StoreCapabilities,
    P: Pairing,
{
    type Sender = MqttClient<S>;
    type Store = S;
}

/// Wrapper structs that holds data used when connecting/reconnecting
pub(crate) struct SessionData {
    interfaces: String,
    interfaces_stored: Vec<IntrospectionInterface>,
    server_interfaces: Vec<String>,
    device_properties: Vec<String>,
}

impl SessionData {
    fn filter_server_interfaces(interfaces: &Interfaces) -> Vec<String> {
        interfaces
            .iter()
            .filter(|interface| interface.ownership() == Ownership::Server)
            .map(|interface| interface.interface_name().to_owned())
            .collect()
    }

    async fn load_set_device_properties<S>(interfaces: &Interfaces, store: &S) -> Vec<String>
    where
        S: PropertyStore,
    {
        // NOTE get only the completed properties (already received by the server)
        // the properties changed while offline will be resent after reconnecting
        let set_properties = store
            .device_props_with_unset(PropertyState::Completed, i64::MAX as usize, 0)
            .await
            .map(|p| {
                // Filter interfaces that are missing or have been updated
                p.iter()
                    .filter(|prop| {
                        prop.value
                            .as_ref()
                            .and_then(|_| interfaces.get(&prop.interface))
                            .is_some_and(|interface| {
                                interface.version_major() == prop.interface_major
                            })
                    })
                    .map(|val| format!("{}{}", val.interface, val.path))
                    .collect::<Vec<String>>()
            });

        match set_properties {
            Ok(p) => p,
            Err(e) => {
                error!(error = %Report::new(e), "error while loading device properties from the store");
                Vec::new()
            }
        }
    }

    pub(crate) async fn from_props<S>(interfaces: &Interfaces, store: &S) -> Self
    where
        S: PropertyStore,
    {
        let server_interfaces = Self::filter_server_interfaces(interfaces);
        let interfaces_stored: Vec<IntrospectionInterface> = interfaces.into();
        let device_properties = Self::load_set_device_properties(interfaces, store).await;

        Self {
            interfaces: interfaces.get_introspection_string(),
            server_interfaces,
            device_properties,
            interfaces_stored,
        }
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
    ) -> impl Future<Output = Result<(), ClientError>> + Send
    where
        S: AsRef<str> + Send + Sync;
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
    ) -> Result<(), ClientError>
    where
        S: AsRef<str> + Send + Sync,
    {
        for if_name in interfaces_names {
            let if_name = if_name.as_ref();

            debug!(interface = if_name, "subscribing on interface");

            self.subscribe(client_id.make_interface_wildcard(if_name), QoS::ExactlyOnce)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{str::FromStr, time::Duration};

    use astarte_interfaces::AggregationIndividual;
    use chrono::Utc;
    use mockall::{Sequence, predicate};
    use rumqttc::{ClientError, QoS, Resolver, SubAck, UnsubAck};
    use tempfile::TempDir;

    use crate::{
        builder::{Config, DEFAULT_VOLATILE_CAPACITY},
        retention::Context,
        session::SessionError,
        store::{SqliteStore, memory::MemoryStore, mock::MockStore},
        test::{
            DEVICE_OBJECT, DEVICE_PROPERTIES, DEVICE_PROPERTIES_NAME, E2E_DEVICE_DATASTREAM,
            E2E_DEVICE_DATASTREAM_NAME, SERVER_INDIVIDUAL, SERVER_INDIVIDUAL_NAME,
            SERVER_PROPERTIES,
        },
        transport::mqtt::payload::Payload,
    };

    use self::{
        client::{AsyncClient, EventLoop},
        config::transport::TransportProvider,
    };

    use super::connection::tests::mock_mqtt_state_connected;
    use super::*;
    use crate::pairing::api::PairingApi;

    const CLIENT_ID: ClientId<&str> = ClientId {
        realm: "realm",
        device_id: "device_id",
    };

    pub(crate) fn notify_success<T, E>(out: T) -> Result<Token<T>, E> {
        let (tx, token) = Resolver::new();

        tx.resolve(out);

        Ok(token)
    }

    pub(crate) async fn mock_mqtt_connection(
        client: AsyncClient,
        eventloop: EventLoop,
        interfaces: &[&str],
    ) -> (MqttClient<MemoryStore>, Mqtt<MemoryStore, PairingApi>) {
        mock_mqtt_connection_with_store(client, eventloop, interfaces, MemoryStore::new()).await
    }

    pub(crate) async fn mock_mqtt_connection_with_store<S>(
        client: AsyncClient,
        eventloop: EventLoop,
        interfaces: &[&str],
        store: S,
    ) -> (MqttClient<S>, Mqtt<S, PairingApi>)
    where
        S: Clone,
    {
        let client_id: ClientId = CLIENT_ID.into();

        let (ret_tx, ret_rx) = async_channel::unbounded();

        let store = StoreWrapper::new(store);

        let transport_provider = TransportProvider::configure(None, true)
            .await
            .expect("failed to configure transport provider");

        let state = Arc::new(mock_state(interfaces));

        let mqtt_config = MqttConfig {
            realm: client_id.realm.clone(),
            device_id: client_id.device_id.clone(),
            credential: Credential::Secret {
                credentials_secret: "credentials_secret".to_string(),
            },
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
            ignore_ssl_errors: true,
            keepalive: DEFAULT_KEEP_ALIVE,
        };

        let mqtt_state = mock_mqtt_state_connected(client.clone(), eventloop, mqtt_config);

        let client_sender = Arc::new(OnceLock::from(ClientSender {
            id: client_id,
            client,
        }));

        let mqtt = Mqtt {
            connection: mqtt_state,
            client_sender,
            provider: transport_provider,
            retention: MqttRetention::new(ret_rx),
            store: store.clone(),
            state: Arc::clone(&state),
        };

        let mqtt_client = MqttClient {
            sender: Arc::clone(&mqtt.client_sender),
            store,
            state,
            retention: ret_tx,
        };

        (mqtt_client, mqtt)
    }

    pub(crate) fn mock_state(interfaces: &[&str]) -> SharedState {
        let interfaces = interfaces.iter().map(|i| Interface::from_str(i).unwrap());

        SharedState::new(
            Config::default(),
            Interfaces::from_iter(interfaces),
            VolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY.get()),
        )
    }

    #[tokio::test]
    async fn should_extend_interfaces() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = [
            Interface::from_str(DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(DEVICE_OBJECT).unwrap(),
            Interface::from_str(SERVER_INDIVIDUAL).unwrap(),
        ];

        let mut introspection = DeviceIntrospection::new(to_add.iter())
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

        let (mut client, _mqtt_connection) = mock_mqtt_connection(client, eventl, &[]).await;

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
            Interface::from_str(DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(DEVICE_OBJECT).unwrap(),
        ];

        let mut introspection = DeviceIntrospection::new(to_add.iter())
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

        let (mut client, _connection) = mock_mqtt_connection(client, eventl, &[]).await;

        client
            .extend_interfaces(&interfaces, &to_add)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn should_unsubscribe_on_extend_err() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = [Interface::from_str(SERVER_PROPERTIES).unwrap()];

        let introspection = DeviceIntrospection::new(to_add.iter()).to_string();

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

        let (mut mqtt_client, _mqtt_connection) = mock_mqtt_connection(client, eventl, &[]).await;

        mqtt_client
            .extend_interfaces(&interfaces, &to_add)
            .await
            .expect_err("Didn't return the error");
    }

    #[tokio::test]
    async fn should_add_interface() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let introspection = DeviceIntrospection::new([to_add.clone()].iter()).to_string();

        let interfaces = Interfaces::new();

        let to_add = interfaces.validate(to_add).unwrap().unwrap();

        let mut store = MockStore::new();
        // enable session
        store.expect_return_session().return_const(true);

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .once()
            .returning(AsyncClient::default);

        store
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockStore::new);

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
            .with(
                predicate::eq("realm/device_id".to_owned()),
                predicate::always(),
                predicate::always(),
                predicate::eq(introspection),
            )
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let expected: [IntrospectionInterface; 1] = [to_add.interface().into()];
        store
            .expect_add_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf(move |actual| actual == expected)
            .returning(|_| Ok(()));

        let (mut client, _mqtt_connection) =
            mock_mqtt_connection_with_store(client, eventl, &[], store).await;

        client.add_interface(&interfaces, &to_add).await.unwrap()
    }

    #[tokio::test]
    async fn add_interface_error() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let introspection = DeviceIntrospection::new([to_add.clone()].iter()).to_string();

        let interfaces = Interfaces::new();

        let to_add = interfaces.validate(to_add).unwrap().unwrap();

        let mut store = MockStore::new();
        // enable session
        store.expect_return_session().return_const(true);

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .once()
            .returning(AsyncClient::default);

        store
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockStore::new);

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
            .with(
                predicate::eq("realm/device_id".to_owned()),
                predicate::always(),
                predicate::always(),
                predicate::eq(introspection),
            )
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let expected: [IntrospectionInterface; 1] = [to_add.interface().into()];
        store
            .expect_add_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf(move |actual| actual == expected)
            .returning(|_| Err(SessionError::add_interfaces("mock error add interfaces")));

        let (mut client, _mqtt_connection) =
            mock_mqtt_connection_with_store(client, eventl, &[], store).await;

        let result = client.add_interface(&interfaces, &to_add).await;

        assert!(matches!(
            result,
            Err(Error::Session(SessionError::AddInterfaces(..)))
        ));
    }

    #[tokio::test]
    async fn should_remove_interface() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_remove = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let mut interfaces = Interfaces::new();
        interfaces.add(interfaces.validate(to_remove.clone()).unwrap().unwrap());

        let mut store = MockStore::new();
        // enable session
        store.expect_return_session().return_const(true);

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .once()
            .returning(AsyncClient::default);

        store
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockStore::new);

        client
            .expect_publish::<String, String>()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::eq("realm/device_id".to_owned()),
                predicate::always(),
                predicate::always(),
                predicate::eq(String::new()),
            )
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let expected: [IntrospectionInterface; 1] = [(&to_remove).into()];
        store
            .expect_remove_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf(move |actual| actual == expected)
            .returning(|_| Ok(()));

        client
            .expect_unsubscribe::<String>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()),
            )
            .in_sequence(&mut seq)
            .returning(|_| notify_success(UnsubAck::new(0)));
        let (mut client, _mqtt_connection) =
            mock_mqtt_connection_with_store(client, eventl, &[], store).await;

        client
            .remove_interface(&interfaces, &to_remove)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn remove_interface_error() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_remove = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let mut interfaces = Interfaces::new();
        interfaces.add(interfaces.validate(to_remove.clone()).unwrap().unwrap());

        let mut store = MockStore::new();
        // enable session
        store.expect_return_session().return_const(true);

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .once()
            .returning(AsyncClient::default);

        store
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockStore::new);

        client
            .expect_publish::<String, String>()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::eq("realm/device_id".to_owned()),
                predicate::always(),
                predicate::always(),
                predicate::eq(String::new()),
            )
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let expected: [IntrospectionInterface; 1] = [(&to_remove).into()];
        store
            .expect_remove_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf(move |actual| actual == expected)
            .returning(|_| Err(SessionError::remove_interfaces("remove interface error")));

        // NOTE when a store error is thrown in the remove interface operation
        // no unsusbscribe is performed

        let (mut client, _mqtt_connection) =
            mock_mqtt_connection_with_store(client, eventl, &[], store).await;

        let result = client.remove_interface(&interfaces, &to_remove).await;

        assert!(matches!(
            result,
            Err(crate::Error::Session(SessionError::RemoveInterfaces(..)))
        ))
    }

    #[tokio::test]
    async fn should_remove_interfaces() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let device_properties = Interface::from_str(DEVICE_PROPERTIES).unwrap();
        let server_properties = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let to_remove: HashMap<&str, &Interface> = [
            (DEVICE_PROPERTIES_NAME, &device_properties),
            (SERVER_INDIVIDUAL_NAME, &server_properties),
        ]
        .into_iter()
        .collect();

        let remaining = Interface::from_str(DEVICE_OBJECT).unwrap();

        let introspection = DeviceIntrospection::new([remaining.clone()].iter()).to_string();

        let mut interfaces = Interfaces::new();
        interfaces.extend(
            interfaces
                .validate_many(
                    to_remove
                        .values()
                        .copied()
                        .chain(std::iter::once(&remaining))
                        .cloned(),
                )
                .unwrap(),
        );

        let mut store = MockStore::new();
        // enable session
        store.expect_return_session().return_const(true);

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .once()
            .returning(AsyncClient::default);

        store
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockStore::new);

        client
            .expect_publish::<String, String>()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::eq("realm/device_id".to_owned()),
                predicate::always(),
                predicate::always(),
                predicate::eq(introspection),
            )
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let expected: Vec<IntrospectionInterface> = to_remove.values().map(|&i| i.into()).collect();
        store
            .expect_remove_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf(move |actual| actual == expected)
            .returning(|_| Ok(()));

        client
            .expect_unsubscribe::<String>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()),
            )
            .in_sequence(&mut seq)
            .returning(|_| notify_success(UnsubAck::new(0)));

        let (mut client, _mqtt_connection) =
            mock_mqtt_connection_with_store(client, eventl, &[], store).await;

        client
            .remove_interfaces(&interfaces, &to_remove)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn should_extend_interfaces_store_introspection() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = [
            Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(crate::test::DEVICE_OBJECT).unwrap(),
            Interface::from_str(crate::test::SERVER_INDIVIDUAL).unwrap(),
        ];

        let mut introspection = DeviceIntrospection::new(to_add.iter())
            .to_string()
            .split(';')
            .map(ToOwned::to_owned)
            .collect_vec();

        introspection.sort_unstable();

        let interfaces = Interfaces::new();

        let to_add = interfaces.validate_many(to_add).unwrap();

        let mut mock_store = MockStore::new();
        // enable session
        mock_store.expect_return_session().return_const(true);

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .once()
            .returning(AsyncClient::default);

        mock_store
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockStore::new);

        client
            .expect_subscribe::<String>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()),
                predicate::eq( QoS::ExactlyOnce)
            )
            .in_sequence(&mut seq)
            .returning(|_, _| notify_success(SubAck::new(0, Vec::new())));

        let expected: Vec<IntrospectionInterface> =
            to_add.values().map(|i| i.interface().into()).collect();

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

        mock_store
            .expect_add_interfaces()
            .once()
            .withf(move |actual| actual == expected)
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));

        let (mut client, _mqtt_connection) =
            mock_mqtt_connection_with_store(client, eventl, &[], mock_store).await;

        client
            .extend_interfaces(&interfaces, &to_add)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn should_send_individual_success() {
        let mut client = AsyncClient::default();
        let eventloop = EventLoop::default();

        let mut seq = Sequence::new();

        let path = MappingPath::try_from("/integer_endpoint").unwrap();
        let interface = DatastreamIndividual::from_str(E2E_DEVICE_DATASTREAM).unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();
        let timestamp = Utc::now();
        let value = AstarteData::Integer(42);

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(AsyncClient::default);

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::eq(format!("{CLIENT_ID}/{E2E_DEVICE_DATASTREAM_NAME}{path}",)),
                predicate::eq(QoS::AtMostOnce),
                predicate::eq(false),
                predicate::eq(
                    Payload::with_timestamp(value.clone(), Some(timestamp))
                        .to_vec()
                        .unwrap(),
                ),
            )
            .returning(|_, _, _, _| Ok(Resolver::new().1));

        let (mut client, _connection) =
            mock_mqtt_connection(client, eventloop, &[E2E_DEVICE_DATASTREAM]).await;

        let data = ValidatedIndividual::validate(mapping, value, Some(timestamp)).unwrap();

        client.send_individual(data).await.unwrap();
    }

    #[tokio::test]
    async fn should_not_enqueue_sent_individual() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let interface_str = crate::test::STORED_DEVICE_DATASTREAM;
        let interface = Interface::from_str(interface_str).unwrap();

        let context = Context::new();

        client.expect_clone().once().returning(AsyncClient::default);

        client
            .expect_publish::<String, Vec<u8>>()
            .with(
                predicate::eq(format!(
                    "realm/device_id/{}/endpoint1",
                    crate::test::STORED_DEVICE_DATASTREAM_NAME
                )),
                predicate::eq(QoS::AtLeastOnce),
                predicate::always(),
                predicate::always(),
            )
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let (mut client, mut mqtt_connection) =
            mock_mqtt_connection_with_store(client, eventl, &[interface_str], store.clone()).await;

        let endpoint = "/endpoint1";
        let mapping_path = MappingPath::try_from(endpoint).unwrap();
        let individual = interface.as_datastream_individual().unwrap();
        let mapping = individual.mapping(&mapping_path).unwrap();
        let mapping_ref = MappingRef::new(individual, &mapping_path).unwrap();
        let validated_individual =
            ValidatedIndividual::validate(mapping_ref, AstarteData::LongInteger(10), None).unwrap();
        let id = context.next();
        let store_id = RetentionId::Stored(id);
        let buf = payload::serialize_individual(
            &validated_individual.data,
            validated_individual.timestamp,
        )
        .unwrap();
        let publish = PublishInfo::from_ref(
            interface.interface_name(),
            endpoint,
            interface.version_major(),
            mapping.reliability(),
            mapping.retention(),
            false,
            &buf,
        );
        // force a store
        store.store_publish(&id, publish.clone()).await.unwrap();
        // we should mark the message as sent externally
        store.update_sent_flag(&id, true).await.unwrap();
        client
            .send_individual_stored(store_id, validated_individual)
            .await
            .unwrap();
        // resend not sent (should not do anything)
        let mut unsent = Vec::with_capacity(1);
        store.unsent_publishes(1, &mut unsent).await.unwrap();
        for (id, publish) in unsent {
            store.update_sent_flag(&id, true).await.unwrap();
            client
                .resend_stored(RetentionId::Stored(id), publish)
                .await
                .unwrap();
        }

        tokio::time::timeout(
            Duration::from_secs(10),
            // this panics if the send_individual_stored didn't mark the publish as sent
            mqtt_connection.retention.into_future(),
        )
        .await
        .unwrap()
        .unwrap();
    }

    fn sample_object() -> AstarteObject {
        [
            ("longinteger".to_string(), AstarteData::LongInteger(10)),
            ("boolean".to_string(), AstarteData::Boolean(false)),
        ]
        .into_iter()
        .collect()
    }

    #[tokio::test]
    async fn should_not_enqueue_sent_object() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let interface_str = crate::test::STORED_DEVICE_OBJECT;
        let interface = Interface::from_str(interface_str).unwrap();

        let context = Context::new();

        client.expect_clone().once().returning(AsyncClient::default);

        client
            .expect_publish::<String, Vec<u8>>()
            .with(
                predicate::eq(format!(
                    "realm/device_id/{}/endpoint",
                    crate::test::STORED_DEVICE_OBJECT_NAME
                )),
                predicate::eq(QoS::AtLeastOnce),
                predicate::always(),
                predicate::always(),
            )
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let (mut client, mut mqtt_connection) =
            mock_mqtt_connection_with_store(client, eventl, &[interface_str], store.clone()).await;

        let endpoint = "/endpoint";
        let mapping_path = MappingPath::try_from(endpoint).unwrap();
        let mapping_ref = interface.as_datastream_object().unwrap();
        let validated_object =
            ValidatedObject::validate(mapping_ref, &mapping_path, sample_object(), None).unwrap();

        let id = context.next();
        let stored_id = RetentionId::Stored(id);
        let buf =
            payload::serialize_object(&validated_object.data, validated_object.timestamp).unwrap();
        let publish = PublishInfo::from_ref(
            interface.interface_name(),
            endpoint,
            interface.version_major(),
            mapping_ref.reliability(),
            mapping_ref.retention(),
            false,
            &buf,
        );
        // force a store
        store.store_publish(&id, publish).await.unwrap();
        // send object
        // we should mark the message as sent externally
        store.update_sent_flag(&id, true).await.unwrap();
        client
            .send_object_stored(stored_id, validated_object)
            .await
            .unwrap();
        // resend stored objects (this shouldn't do anything)
        let mut unsent = Vec::with_capacity(1);
        store.unsent_publishes(1, &mut unsent).await.unwrap();
        for (id, publish) in unsent {
            store.update_sent_flag(&id, true).await.unwrap();
            client
                .resend_stored(RetentionId::Stored(id), publish)
                .await
                .unwrap();
        }

        tokio::time::timeout(
            Duration::from_secs(10),
            // this panics if the send_individual_stored already marked the publish as sent
            mqtt_connection.retention.into_future(),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn message_enqueued_twice_panics() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let interface_str = crate::test::STORED_DEVICE_OBJECT;
        let interface = Interface::from_str(interface_str).unwrap();

        let context = Context::new();

        client.expect_clone().once().returning(AsyncClient::default);

        client
            .expect_publish::<String, Vec<u8>>()
            .with(
                predicate::eq(format!(
                    "realm/device_id/{}/endpoint",
                    crate::test::STORED_DEVICE_OBJECT_NAME
                )),
                predicate::eq(QoS::AtLeastOnce),
                predicate::always(),
                predicate::always(),
            )
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        let (mut client, mut mqtt_connection) =
            mock_mqtt_connection_with_store(client, eventl, &[interface_str], store.clone()).await;

        let endpoint = "/endpoint";
        let mapping_path = MappingPath::try_from(endpoint).unwrap();
        let mapping_ref = interface.as_datastream_object().unwrap();
        let validated_object =
            ValidatedObject::validate(mapping_ref, &mapping_path, sample_object(), None).unwrap();

        let id = context.next();
        let stored_id = RetentionId::Stored(id);
        let buf =
            payload::serialize_object(&validated_object.data, validated_object.timestamp).unwrap();
        let publish = PublishInfo::from_ref(
            interface.interface_name(),
            endpoint,
            interface.version_major(),
            mapping_ref.reliability(),
            mapping_ref.retention(),
            false,
            &buf,
        );
        // force a store
        store.store_publish(&id, publish.clone()).await.unwrap();
        // send object (should mark message as sent)
        client
            .send_object_stored(stored_id, validated_object)
            .await
            .unwrap();
        // force resend stored objects
        client.resend_stored(stored_id, publish).await.unwrap();

        tokio::time::timeout(
            Duration::from_secs(10),
            // this panics because two messages where enqueued
            mqtt_connection.retention.into_future(),
        )
        .await
        .unwrap()
        .unwrap();
    }
}
