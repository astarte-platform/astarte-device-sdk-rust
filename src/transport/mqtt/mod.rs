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

pub(crate) mod pairing;
pub(crate) mod payload;

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    ops::Deref,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, error, info, trace, warn};
use once_cell::sync::OnceCell;
use rumqttc::{Event as MqttEvent, Packet};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

pub use self::pairing::PairingError;
pub use self::payload::PayloadError;

#[cfg(test)]
pub(crate) use crate::mock::{MockAsyncClient as AsyncClient, MockEventLoop as EventLoop};
use crate::{
    builder::{ConnectionConfig, DeviceBuilder, DEFAULT_CHANNEL_SIZE},
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
        InterfaceError, Ownership,
    },
    interfaces::Interfaces,
    properties,
    retry::DelayedPoll,
    shared::SharedDevice,
    store::{error::StoreError, wrapper::StoreWrapper, PropertyStore, StoredProp},
    topic::ParsedTopic,
    types::AstarteType,
    validate::{ValidatedIndividual, ValidatedObject},
    Interface, Timestamp,
};

#[cfg(not(test))]
pub(crate) use rumqttc::{AsyncClient, EventLoop};

use payload::Payload;

use super::{Publish, Receive, ReceivedEvent, Register};

/// Borrowing wrapper for the client id
///
/// To avoid directly allocating and returning a [`String`] each time
/// the client id is needed this trait implements [`Display`]
/// while only borrowing the field needed to construct the client id.
#[derive(Debug, Clone, Copy)]
struct ClientId<'a> {
    realm: &'a str,
    device_id: &'a str,
}

impl<'a> Display for ClientId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.realm, self.device_id)
    }
}

/// Shared data of the connection, this struct is internal to the [`Mqtt`] connection
/// where is wrapped in an arc to share an immutable reference across tasks.
pub struct SharedMqtt {
    realm: String,
    device_id: String,
    eventloop: Mutex<EventLoop>,
}

/// This struct represents an MQTT connection handler for an Astarte device. It manages the
/// interaction with the MQTT broker, handling connections, subscriptions, and message publishing
/// following the Astarte protocol.
#[derive(Clone)]
pub struct Mqtt {
    shared: Arc<SharedMqtt>,
    client: AsyncClient,
}

impl Mqtt {
    /// Initializes values for this struct
    ///
    /// This method should only be used for testing purposes since it does not fully
    /// connect to the mqtt broker as described by the astarte protocol.
    /// This struct should be constructed with the [`Mqtt::connected`] associated function.
    fn new(realm: String, device_id: String, eventloop: EventLoop, client: AsyncClient) -> Self {
        let shared = Arc::new(SharedMqtt {
            realm,
            device_id,
            eventloop: Mutex::new(eventloop),
        });

        Self { shared, client }
    }

    /// Waits for mqtt connack to correctly initialize connection to astarte
    /// by sending session data.
    ///
    /// The session parameter holds data that will be sent during the
    /// connection to the astarte server.
    pub(crate) async fn wait_for_connack(&self, session: SessionData) -> Result<(), crate::Error> {
        loop {
            match self.poll().await? {
                rumqttc::Packet::ConnAck(connack) => {
                    self.connack(session, connack).await?;

                    return Ok(());
                }
                packet => warn!("Received incoming packet while waiting for connack: {packet:?}"),
            }
        }
    }

    /// Returns a wrapper for the client id
    fn client_id(&self) -> ClientId {
        ClientId {
            realm: &self.realm,
            device_id: &self.device_id,
        }
    }

    /// Method that gets called when a [`rumqttc::ConnAck`] is received.
    /// Following the astarte protocol it performs the following tasks:
    ///  - Subscribes to the server owned interfaces in the interface list
    ///  - Sends the introspection
    ///  - Sends the emptycache command
    ///  - Sends the device owned properties stored locally
    async fn connack(
        &self,
        SessionData {
            interfaces,
            server_interfaces,
            device_properties,
        }: SessionData,
        connack: rumqttc::ConnAck,
    ) -> Result<(), crate::Error> {
        if connack.session_present {
            return Ok(());
        }

        self.subscribe_server_interfaces(&server_interfaces).await?;
        self.send_introspection(interfaces).await?;
        self.send_emptycache().await?;
        self.send_device_properties(&device_properties).await?;

        info!("connack done");

        Ok(())
    }

    /// Subscribes to the passed list of interfaces
    async fn subscribe_server_interfaces(
        &self,
        server_interfaces: &[String],
    ) -> Result<(), crate::Error> {
        self.client
            .subscribe(
                format!("{}/control/consumer/properties", self.client_id()),
                rumqttc::QoS::ExactlyOnce,
            )
            .await?;

        for iface in server_interfaces {
            self.client
                .subscribe(
                    format!("{}/{iface}/#", self.client_id()),
                    rumqttc::QoS::ExactlyOnce,
                )
                .await?;
        }

        Ok(())
    }

    /// Sends the emptycache command as per the astarte protocol definition
    async fn send_emptycache(&self) -> Result<(), crate::Error> {
        let url = format!("{}/control/emptyCache", self.client_id());
        debug!("sending emptyCache to {}", url);

        self.client
            .publish(url, rumqttc::QoS::ExactlyOnce, false, "1")
            .await?;

        Ok(())
    }

    /// Sends the passed device owned properties
    async fn send_device_properties(
        &self,
        device_properties: &[StoredProp],
    ) -> Result<(), crate::Error> {
        for prop in device_properties {
            let topic = format!("{}/{}{}", self.client_id(), prop.interface, prop.path);

            debug!(
                "sending device-owned property = {}{}",
                prop.interface, prop.path
            );

            let payload = Payload::new(&prop.value).to_vec()?;

            self.client
                .publish(topic, rumqttc::QoS::ExactlyOnce, false, payload)
                .await?;
        }

        Ok(())
    }

    /// Purges local properties defined in the passed binary data
    async fn purge_properties<S>(
        &self,
        device: &SharedDevice<S>,
        bdata: &[u8],
    ) -> Result<(), crate::Error>
    where
        S: PropertyStore,
    {
        let stored_props = device.store.load_all_props().await?;

        let paths = properties::extract_set_properties(bdata)?;

        for stored_prop in stored_props {
            if paths.contains(&format!("{}{}", stored_prop.interface, stored_prop.path)) {
                continue;
            }

            device
                .store
                .delete_prop(&stored_prop.interface, &stored_prop.path)
                .await?;
        }

        Ok(())
    }

    /// Polls mqtt events from the [`rumqttc:EventLoop`].
    ///
    /// Errors are handled using the [`DelayedPoll::retry_poll_event`] method.
    async fn poll_mqtt_event(&self) -> Result<MqttEvent, crate::Error> {
        let mut lock = self.eventloop.lock().await;

        match lock.poll().await {
            Ok(event) => Ok(event),
            Err(err) => {
                error!("couldn't poll the event loop: {err:#?}");

                DelayedPoll::retry_poll_event(&mut lock).await
            }
        }
    }

    /// Polls mqtt events from the [`rumqttc:EventLoop`].
    ///
    /// This method internally calls [`Mqtt::poll_mqtt_event`] but ignores
    /// outgoing packets by logging them.
    async fn poll(&self) -> Result<Packet, crate::Error> {
        loop {
            match self.poll_mqtt_event().await? {
                MqttEvent::Incoming(packet) => return Ok(packet),
                MqttEvent::Outgoing(outgoing) => trace!("MQTT Outgoing = {:?}", outgoing),
            }
        }
    }

    /// Send a binary payload over this mqtt connection.
    async fn send(
        &self,
        interface: &Interface,
        path: &str,
        reliability: rumqttc::QoS,
        payload: Vec<u8>,
    ) -> Result<(), crate::Error> {
        self.client
            .publish(
                format!(
                    "{}/{}{}",
                    self.client_id(),
                    interface.interface_name(),
                    path
                ),
                reliability,
                false,
                payload,
            )
            .await?;

        Ok(())
    }

    async fn subscribe(&self, interface_name: &str) -> Result<(), crate::Error> {
        self.client
            .subscribe(
                format!("{}/{interface_name}/#", self.client_id()),
                rumqttc::QoS::ExactlyOnce,
            )
            .await
            .map_err(crate::Error::from)
    }

    async fn unsubscribe(&self, interface_name: &str) -> Result<(), crate::Error> {
        self.client
            .unsubscribe(format!("{}/{interface_name}/#", self.client_id()))
            .await
            .map_err(crate::Error::from)
    }

    /// Sends the introspection [`String`].
    async fn send_introspection(&self, introspection: String) -> Result<(), crate::Error> {
        debug!("sending introspection = {}", introspection);

        let path = self.client_id().to_string();

        self.client
            .publish(path, rumqttc::QoS::ExactlyOnce, false, introspection)
            .await
            .map_err(crate::Error::from)
    }
}

impl Deref for Mqtt {
    type Target = SharedMqtt;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

#[async_trait]
impl Publish for Mqtt {
    async fn send_individual(
        &self,
        validated: ValidatedIndividual<'_>,
    ) -> Result<(), crate::Error> {
        let buf = payload::serialize_individual(validated.data(), validated.timestamp())?;

        self.send(
            validated.mapping().interface(),
            validated.path().as_str(),
            validated.mapping().reliability().into(),
            buf,
        )
        .await
    }

    async fn send_object(&self, validated: ValidatedObject<'_>) -> Result<(), crate::Error> {
        let buf = payload::serialize_object(validated.data(), validated.timestamp())?;

        self.send(
            validated.object().interface,
            validated.path().as_str(),
            validated.object().reliability().into(),
            buf,
        )
        .await
    }
}

#[async_trait]
impl Receive for Mqtt {
    type Payload = Bytes;

    async fn next_event<S>(
        &self,
        device: &SharedDevice<S>,
    ) -> Result<ReceivedEvent<Self::Payload>, crate::Error>
    where
        S: PropertyStore,
    {
        static PURGE_PROPERTIES_TOPIC: OnceCell<String> = OnceCell::new();
        static CLIENT_ID: OnceCell<String> = OnceCell::new();

        // Keep consuming packets until we have an actual "data" event
        loop {
            match self.poll().await? {
                rumqttc::Packet::ConnAck(connack) => {
                    self.connack(
                        SessionData::try_from(&device.interfaces, &device.store).await?,
                        connack,
                    )
                    .await?
                }
                rumqttc::Packet::Publish(publish) => {
                    let purge_topic = PURGE_PROPERTIES_TOPIC.get_or_init(|| {
                        format!("{}/control/consumer/properties", self.client_id())
                    });

                    debug!("Incoming publish = {} {:x}", publish.topic, publish.payload);

                    if purge_topic == &publish.topic {
                        debug!("Purging properties");

                        self.purge_properties(device, &publish.payload).await?;
                    } else {
                        let client_id = CLIENT_ID.get_or_init(|| self.client_id().to_string());
                        let ParsedTopic { interface, path } =
                            ParsedTopic::try_parse(client_id, &publish.topic)?;

                        return Ok(ReceivedEvent {
                            interface: interface.to_string(),
                            path: path.to_string(),
                            payload: publish.payload,
                        });
                    }
                }
                packet => {
                    trace!("packet received {packet:?}");
                }
            }
        }
    }

    fn deserialize_individual(
        &self,
        mapping: MappingRef<'_, &Interface>,
        payload: &Self::Payload,
    ) -> Result<(AstarteType, Option<Timestamp>), crate::Error> {
        payload::deserialize_individual(mapping, payload).map_err(|err| err.into())
    }

    fn deserialize_object(
        &self,
        object: ObjectRef,
        path: &MappingPath<'_>,
        payload: &Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), crate::Error> {
        payload::deserialize_object(object, path, payload).map_err(|err| err.into())
    }
}

#[async_trait]
impl Register for Mqtt {
    async fn add_interface<S>(
        &self,
        device: &SharedDevice<S>,
        added_interface: &str,
    ) -> Result<(), crate::Error>
    where
        S: Send + Sync,
    {
        let interfaces = device.interfaces.read().await;
        let interface_ownership = interfaces
            .get(added_interface)
            .ok_or(InterfaceError::InterfaceNotFound {
                name: added_interface.to_string(),
            })?
            .ownership();
        let introspection_string = interfaces.get_introspection_string();

        if interface_ownership == Ownership::Server {
            self.subscribe(added_interface).await?
        }

        self.send_introspection(introspection_string).await
    }

    async fn remove_interface(
        &self,
        interfaces: &Interfaces,
        removed_interface: Interface,
    ) -> Result<(), crate::Error> {
        self.send_introspection(interfaces.get_introspection_string())
            .await?;

        if removed_interface.ownership() == Ownership::Server {
            self.unsubscribe(removed_interface.interface_name()).await?;
        }

        Ok(())
    }
}

/// Wrapper structs that holds data used when connecting/reconnecting
pub(crate) struct SessionData {
    interfaces: String,
    server_interfaces: Vec<String>,
    device_properties: Vec<StoredProp>,
}

impl SessionData {
    fn filter_server_interfaces(interfaces: &Interfaces) -> Vec<String> {
        interfaces
            .iter_interfaces()
            .filter(|interface| interface.ownership() == Ownership::Server)
            .map(|interface| interface.interface_name().to_owned())
            .collect()
    }

    /// Constructs this struct from a tokio [`RwLock`] that stores the interfaces
    pub(crate) async fn try_from<S>(
        interfaces: &RwLock<Interfaces>,
        store: &StoreWrapper<S>,
    ) -> Result<Self, StoreError>
    where
        S: PropertyStore,
    {
        let device_properties = store.device_props().await?;
        let interfaces = interfaces.read().await;

        let server_interfaces = Self::filter_server_interfaces(&interfaces);

        Ok(Self {
            interfaces: interfaces.get_introspection_string(),
            server_interfaces,
            device_properties,
        })
    }

    pub(crate) async fn try_from_unlocked<S>(
        interfaces: &Interfaces,
        store: &S,
    ) -> Result<Self, StoreError>
    where
        S: PropertyStore<Err = StoreError>,
    {
        let device_properties = store.device_props().await?;
        let server_interfaces = Self::filter_server_interfaces(interfaces);

        Ok(Self {
            interfaces: interfaces.get_introspection_string(),
            server_interfaces,
            device_properties,
        })
    }
}

/// Errors raised during construction of the [`Mqtt`] struct
#[derive(Debug, thiserror::Error)]
pub enum MqttConnectionError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error(transparent)]
    Pairing(#[from] PairingError),
    #[error(transparent)]
    Pki(#[from] webpki::Error),
}

/// Configuration for the mqtt connection
#[derive(Serialize, Deserialize)]
pub struct MqttConfig {
    pub(crate) realm: String,
    pub(crate) device_id: String,
    pub(crate) credentials_secret: String,
    pub(crate) pairing_url: String,
    pub(crate) ignore_ssl_errors: bool,
    pub(crate) keepalive: std::time::Duration,
    pub(crate) bounded_channel_size: usize,
}

impl MqttConfig {
    /// Create a new instance of MqttConfig
    ///
    /// As a default this configuration:
    ///    - does not ignore SSL errors.
    ///    - has a keepalive of 30 seconds
    ///    - has a default bounded channel size of [`crate::builder::DEFAULT_CHANNEL_SIZE`]
    ///
    /// ```no_run
    /// use astarte_device_sdk::transport::mqtt::MqttConfig;
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let realm = "realm_name";
    ///     let device_id = "device_id";
    ///     let credentials_secret = "device_credentials_secret";
    ///     let pairing_url = "astarte_cluster_pairing_url";
    ///
    ///     let mut mqtt_options =
    ///         MqttConfig::new(&realm, &device_id, &credentials_secret, &pairing_url);
    /// }
    /// ```
    pub fn new(realm: &str, device_id: &str, credentials_secret: &str, pairing_url: &str) -> Self {
        Self {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            ignore_ssl_errors: false,
            keepalive: std::time::Duration::from_secs(30),
            bounded_channel_size: DEFAULT_CHANNEL_SIZE,
        }
    }

    /// Configure the keep alive timeout.
    ///
    /// The MQTT broker will be pinged when no data exchange has append
    /// for the duration of the keep alive timeout.
    pub fn keepalive(&mut self, duration: std::time::Duration) -> &mut Self {
        self.keepalive = duration;

        self
    }

    /// Ignore TLS/SSL certificate errors.
    pub fn ignore_ssl_errors(&mut self) -> &mut Self {
        self.ignore_ssl_errors = true;

        self
    }

    /// Sets the size for the underlying bounded channel used by the eventloop of [`rumqttc`].
    pub fn bounded_channel_size(&mut self, bounded_channel_size: usize) -> &mut Self {
        self.bounded_channel_size = bounded_channel_size;

        self
    }
}

impl Debug for MqttConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttOptions")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            .field("credentials_secret", &"REDACTED")
            .field("pairing_url", &self.pairing_url)
            .field("ignore_ssl_errors", &self.ignore_ssl_errors)
            .field("keepalive", &self.keepalive)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ConnectionConfig for MqttConfig {
    type Con = Mqtt;

    async fn connect<S, C>(self, builder: &DeviceBuilder<S, C>) -> Result<Self::Con, crate::Error>
    where
        S: PropertyStore,
        C: Send + Sync,
    {
        let mqtt_options = pairing::get_transport_config(&self).await?;

        debug!("{:#?}", mqtt_options);

        let (client, eventloop) = AsyncClient::new(mqtt_options, self.bounded_channel_size);

        let session_data =
            SessionData::try_from_unlocked(&builder.interfaces, &builder.store).await?;
        let connection = Mqtt::new(self.realm, self.device_id, eventloop, client);
        // to correctly initialize the connection to astarte we should wait for the connack
        connection.wait_for_connack(session_data).await?;

        Ok(connection)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{str::FromStr, time::Duration};

    use mockall::predicate;
    use rumqttc::Packet;
    use tokio::sync::{mpsc, RwLock};

    use crate::{
        interfaces::Interfaces,
        shared::SharedDevice,
        store::{memory::MemoryStore, wrapper::StoreWrapper, PropertyStore, StoredProp},
        types::AstarteType,
        Interface,
    };

    use super::{AsyncClient, EventLoop, Mqtt, MqttConfig, MqttEvent, SessionData};

    pub(crate) fn mock_mqtt_connection(client: AsyncClient, eventl: EventLoop) -> Mqtt {
        Mqtt::new("realm".to_string(), "device_id".to_string(), eventl, client)
    }

    #[tokio::test]
    async fn test_poll_server_connack() {
        let mut eventl = EventLoop::default();
        let client = AsyncClient::default();

        eventl.expect_poll().once().returning(|| {
            Ok(MqttEvent::Incoming(rumqttc::Packet::ConnAck(
                rumqttc::ConnAck {
                    session_present: false,
                    code: rumqttc::ConnectReturnCode::Success,
                },
            )))
        });

        let mqtt_connection = mock_mqtt_connection(client, eventl);

        let ack = mqtt_connection
            .poll()
            .await
            .expect("Error while receiving the connack");

        if let Packet::ConnAck(ack) = ack {
            assert!(!ack.session_present);
            assert_eq!(ack.code, rumqttc::ConnectReturnCode::Success);
        }
    }

    #[tokio::test]
    async fn test_connect_client_response() {
        let mut eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        // Connak response for loop in connect method
        eventl.expect_poll().returning(|| {
            Ok(MqttEvent::Incoming(rumqttc::Packet::ConnAck(
                rumqttc::ConnAck {
                    session_present: false,
                    code: rumqttc::ConnectReturnCode::Success,
                },
            )))
        });

        client
            .expect_subscribe::<String>()
            .with(
                predicate::eq("realm/device_id/control/consumer/properties".to_string()),
                predicate::always(),
            )
            .returning(|_topic, _qos| Ok(()));

        client
            .expect_subscribe()
            .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()), predicate::always())
            .returning(|_: String, _| Ok(()));

        // Client id
        client
            .expect_publish::<String, String>()
            .with(
                predicate::eq("realm/device_id".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::always(),
            )
            .returning(|_, _, _, _| Ok(()));

        // empty cache
        client
            .expect_publish::<String, &str>()
            .with(
                predicate::eq("realm/device_id/control/emptyCache".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::eq("1"),
            )
            .returning(|_, _, _, _| Ok(()));

        // device property publish
        client
            .expect_publish::<String, Vec<u8>>()
            .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/sensor1/name".to_string()), predicate::always(), predicate::always(), predicate::always())
            .returning(|_, _, _, _| Ok(()));

        let interfaces = [
            Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap(),
            Interface::from_str(crate::test::INDIVIDUAL_SERVER_DATASTREAM).unwrap(),
        ];

        let (tx, _rx) = mpsc::channel(2);

        let mqtt_connection = mock_mqtt_connection(client, eventl);
        let shared_device = SharedDevice {
            interfaces: RwLock::new(Interfaces::from_iter(interfaces)),
            store: StoreWrapper {
                store: MemoryStore::new(),
            },
            tx,
        };

        let interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();

        let prop = StoredProp {
            interface: interface.interface_name(),
            path: "/sensor1/name",
            value: &AstarteType::String("temperature".to_string()),
            interface_major: 0,
            ownership: interface.ownership(),
        };

        shared_device
            .store
            .store_prop(prop)
            .await
            .expect("Error while storing test property");

        let introspection = SessionData::try_from(&shared_device.interfaces, &shared_device.store)
            .await
            .unwrap();

        mqtt_connection
            .connack(
                introspection,
                rumqttc::ConnAck {
                    session_present: false,
                    code: rumqttc::ConnectReturnCode::Success,
                },
            )
            .await
            .unwrap();
    }

    #[test]
    fn test_default_mqtt_config() {
        let mqtt_config = MqttConfig::new("test", "test", "test", "test");

        assert_eq!(mqtt_config.realm, "test");
        assert_eq!(mqtt_config.device_id, "test");
        assert_eq!(mqtt_config.credentials_secret, "test");
        assert_eq!(mqtt_config.pairing_url, "test");
        assert_eq!(mqtt_config.keepalive, Duration::from_secs(30));
        assert!(!mqtt_config.ignore_ssl_errors);
    }

    #[test]
    fn test_override_mqtt_config() {
        let mut mqtt_config = MqttConfig::new("test", "test", "test", "test");

        mqtt_config
            .ignore_ssl_errors()
            .keepalive(Duration::from_secs(60));

        assert_eq!(mqtt_config.realm, "test");
        assert_eq!(mqtt_config.device_id, "test");
        assert_eq!(mqtt_config.credentials_secret, "test");
        assert_eq!(mqtt_config.pairing_url, "test");
        assert_eq!(mqtt_config.keepalive, Duration::from_secs(60));
        assert!(mqtt_config.ignore_ssl_errors);
    }

    #[test]
    fn test_redacted_credentials_secret() {
        let mqtt_config = MqttConfig::new("test", "test", "secret=", "test");

        let debug_string = format!("{:?}", mqtt_config);

        assert!(!debug_string.contains("secret="));
        assert!(debug_string.contains("REDACTED"));
    }
}
