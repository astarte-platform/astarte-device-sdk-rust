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
mod topic;

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use tracing::{debug, error};

use crate::{
    error::Report,
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
        Ownership, Reliability,
    },
    interfaces::{self, Interfaces, Introspection},
    properties,
    store::{error::StoreError, wrapper::StoreWrapper, PropertyStore, StoredProp},
    types::AstarteType,
    validate::{ValidatedIndividual, ValidatedObject, ValidatedUnset},
    Error, Interface, Timestamp,
};

use super::{Publish, Receive, ReceivedEvent, Register};

pub use self::config::Credential;
pub use self::config::MqttConfig;
pub use self::pairing::PairingError;
pub use self::payload::PayloadError;

use self::{connection::MqttState, error::MqttError, topic::ParsedTopic};

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
struct ClientId<'a> {
    realm: &'a str,
    device_id: &'a str,
}

impl<'a> Display for ClientId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.realm, self.device_id)
    }
}

/// This struct represents an MQTT connection handler for an Astarte device. It manages the
/// interaction with the MQTT broker, handling connections, subscriptions, and message publishing
/// following the Astarte protocol.
pub struct Mqtt {
    realm: String,
    device_id: String,
    connection: MqttState,
}

impl Mqtt {
    /// Initializes values for this struct
    ///
    /// This method should only be used for testing purposes since it does not fully
    /// connect to the mqtt broker as described by the astarte protocol.
    /// This struct should be constructed with the [`Mqtt::connected`] associated function.
    fn new(realm: String, device_id: String, connection: MqttState) -> Self {
        Self {
            realm,
            device_id,
            connection,
        }
    }

    /// Returns a wrapper for the client id
    fn client_id(&self) -> ClientId {
        ClientId {
            realm: &self.realm,
            device_id: &self.device_id,
        }
    }

    /// This function deletes all the stored server owned properties after receiving a publish on
    /// `/control/consumer/properties`
    async fn purge_server_properties<S>(
        &self,
        store: &StoreWrapper<S>,
        bdata: &[u8],
    ) -> Result<(), Error>
    where
        S: PropertyStore,
    {
        let paths = properties::extract_set_properties(bdata)?;

        let stored_props = store.server_props().await?;

        for stored_prop in stored_props {
            if paths.contains(&format!("{}{}", stored_prop.interface, stored_prop.path)) {
                continue;
            }

            store
                .delete_prop(&stored_prop.interface, &stored_prop.path)
                .await?;
        }

        Ok(())
    }

    /// Send a binary payload over this mqtt connection.
    async fn send(
        &self,
        interface: &str,
        path: &str,
        reliability: rumqttc::QoS,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        self.connection
            .client()
            .publish(
                format!("{}/{}{}", self.client_id(), interface, path),
                reliability,
                false,
                payload,
            )
            .await
            .map_err(|err| MqttError::publish("send", err))?;

        Ok(())
    }

    /// Create a topic to subscribe on an interface
    fn make_interface_wildcard(&self, interface_name: &str) -> String {
        format!("{}/{interface_name}/#", self.client_id())
    }

    async fn subscribe(&self, interface_name: &str) -> Result<(), MqttError> {
        self.connection
            .client()
            .subscribe(
                self.make_interface_wildcard(interface_name),
                rumqttc::QoS::ExactlyOnce,
            )
            .await
            .map_err(MqttError::Subscribe)
            .map(drop)
    }

    async fn unsubscribe(&self, interface_name: &str) -> Result<(), MqttError> {
        self.connection
            .client()
            .unsubscribe(self.make_interface_wildcard(interface_name))
            .await
            .map_err(MqttError::Unsubscribe)
            .map(drop)
    }
}

#[async_trait]
impl Publish for Mqtt {
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
    }
}

#[async_trait]
impl Receive for Mqtt {
    type Payload = Bytes;

    async fn next_event<S>(
        &mut self,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> Result<ReceivedEvent<Self::Payload>, Error>
    where
        S: PropertyStore,
    {
        static PURGE_PROPERTIES_TOPIC: OnceCell<String> = OnceCell::new();

        // We need to construct the ClientId ourselves to not borrow self wail calling the method.
        let client_id = ClientId {
            realm: &self.realm,
            device_id: &self.device_id,
        };

        // Wait until some data is received
        loop {
            let publish = self
                .connection
                .next_publish(client_id, interfaces, store)
                .await?;

            let purge_topic = PURGE_PROPERTIES_TOPIC
                .get_or_init(|| format!("{}/control/consumer/properties", client_id));

            debug!("Incoming publish = {} {:x}", publish.topic, publish.payload);

            if purge_topic == &publish.topic {
                debug!("Purging properties");

                self.purge_server_properties(store, &publish.payload)
                    .await?;
            } else {
                let ParsedTopic { interface, path } =
                    ParsedTopic::try_parse(client_id, &publish.topic).map_err(MqttError::Topic)?;

                return Ok(ReceivedEvent {
                    interface: interface.to_string(),
                    path: path.to_string(),
                    payload: publish.payload,
                });
            }
        }
    }

    fn deserialize_individual(
        &self,
        mapping: &MappingRef<'_, &Interface>,
        payload: Self::Payload,
    ) -> Result<Option<(AstarteType, Option<Timestamp>)>, Error> {
        payload::deserialize_individual(mapping, &payload)
            .map_err(|err| MqttError::Payload(err).into())
    }

    fn deserialize_object(
        &self,
        object: &ObjectRef,
        path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), Error> {
        payload::deserialize_object(object, path, &payload)
            .map_err(|err| MqttError::Payload(err).into())
    }
}

#[async_trait]
impl Register for Mqtt {
    async fn add_interface(
        &mut self,
        interfaces: &Interfaces,
        added: &interfaces::Validated,
    ) -> Result<(), Error> {
        if added.ownership().is_server() {
            self.subscribe(added.interface_name()).await?
        }

        let introspection = Introspection::new(interfaces.iter_with_added(added)).to_string();

        self.connection
            .send_introspection(self.client_id(), introspection)
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

        self.connection
            .send_introspection(self.client_id(), introspection)
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

        self.connection
            .subscribe_many(self.client_id(), &server_interfaces)
            .await
            .map_err(MqttError::Subscribe)?;

        let introspection = Introspection::new(interfaces.iter_with_added_many(added)).to_string();

        let res = self
            .connection
            .send_introspection(self.client_id(), introspection)
            .await
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

        self.connection
            .send_introspection(self.client_id(), introspection)
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

/// Wrapper structs that holds data used when connecting/reconnecting
pub(crate) struct SessionData {
    interfaces: String,
    server_interfaces: Vec<String>,
    device_properties: Vec<StoredProp>,
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
        let device_properties = store.device_props().await?;
        let server_interfaces = Self::filter_server_interfaces(interfaces);

        Ok(Self {
            interfaces: interfaces.get_introspection_string(),
            server_interfaces,
            device_properties,
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::str::FromStr;

    use mockall::predicate;
    use mockito::Server;
    use rumqttc::{
        ClientError, ConnAck, ConnectReturnCode, ConnectionError, Event as MqttEvent, Packet, QoS,
    };
    use tempfile::TempDir;
    use test::{
        client::NEW_LOCK,
        pairing::tests::{mock_create_certificate, mock_get_broker_url},
    };

    use crate::{
        builder::{ConnectionConfig, DeviceBuilder},
        store::memory::MemoryStore,
    };

    use self::{
        client::{AsyncClient, EventLoop},
        config::transport::TransportProvider,
    };

    use super::*;

    pub(crate) fn mock_mqtt_connection(client: AsyncClient, eventloop: EventLoop) -> Mqtt {
        Mqtt::new(
            "realm".to_string(),
            "device_id".to_string(),
            MqttState::new(
                client,
                eventloop,
                TransportProvider::new(
                    "http://api.astarte.localhost/pairing".parse().unwrap(),
                    "secret".to_string(),
                    None,
                    true,
                ),
            ),
        )
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

        let mut mqtt_connection = mock_mqtt_connection(client, eventl);

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

        let mqtt_connection = mock_mqtt_connection(client, eventl);
        let interfaces = Interfaces::from_iter(interfaces);
        let store = StoreWrapper::new(MemoryStore::new());

        let interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();

        let prop = StoredProp {
            interface: interface.interface_name(),
            path: "/sensor1/name",
            value: &AstarteType::String("temperature".to_string()),
            interface_major: 0,
            ownership: interface.ownership(),
        };

        store
            .store_prop(prop)
            .await
            .expect("Error while storing test property");

        let introspection = SessionData::try_from_props(&interfaces, &store)
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

    #[tokio::test]
    async fn should_extend_interfaces() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = [
            Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap(),
            Interface::from_str(crate::test::INDIVIDUAL_SERVER_DATASTREAM).unwrap(),
        ];

        let mut introspection = Introspection::new(to_add.iter())
            .to_string()
            .split(';')
            .map(ToOwned::to_owned)
            .collect_vec();

        introspection.sort_unstable();

        let interfaces = Interfaces::new();

        let to_add = interfaces.validate_many(to_add).unwrap();

        client
            .expect_subscribe_many::<Vec<SubscribeFilter>>()
            .once()
            .returning(|_| Ok(()));

        client
            .expect_publish::<String, String>()
            .once()
            .withf(move |publish, _, _, payload| {
                let mut intro = payload.split(';').collect_vec();

                intro.sort_unstable();

                publish == "realm/device_id" && intro == introspection
            })
            .returning(|_, _, _, _| Ok(()));

        let mut mqtt_connection = mock_mqtt_connection(client, eventl);

        mqtt_connection
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
            Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap(),
        ];

        let mut introspection = Introspection::new(to_add.iter())
            .to_string()
            .split(';')
            .map(ToOwned::to_owned)
            .collect_vec();

        introspection.sort_unstable();

        let interfaces = Interfaces::new();

        let to_add = interfaces.validate_many(to_add).unwrap();

        // in this case, no client.subscribe_many() is expected
        client
            .expect_publish::<String, String>()
            .once()
            .withf(move |publish, _, _, payload| {
                let mut intro = payload.split(';').collect_vec();

                intro.sort_unstable();

                publish == "realm/device_id" && intro == introspection
            })
            .returning(|_, _, _, _| Ok(()));

        let mut mqtt_connection = mock_mqtt_connection(client, eventl);

        mqtt_connection
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

        client
            .expect_subscribe_many::<Vec<SubscribeFilter>>()
            .once()
            .returning(|_| Ok(()));

        client
            .expect_publish::<String, String>()
            .once()
            .withf(move |publish, _, _, payload| {
                publish == "realm/device_id" && *payload == introspection
            })
            .returning(|_, _, _, _| {
                // Random error
                Err(ClientError::Request(rumqttc::Request::Disconnect(
                    rumqttc::Disconnect,
                )))
            });

        client
            .expect_unsubscribe::<String>()
            .once()
            .withf(move |topic| topic == "realm/device_id/org.astarte-platform.rust.examples.individual-properties.ServerProperties/#")
            .returning(|_| {
                // We are disconnected so we cannot unsubscribe
                Err(ClientError::Request(rumqttc::Request::Disconnect(rumqttc::Disconnect)))
            });

        let mut mqtt_connection = mock_mqtt_connection(client, eventl);

        mqtt_connection
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
                .returning(|_| EventLoop::default())
                .once()
                .in_sequence(&mut seq);

            ev_loop
                .expect_poll()
                .returning(|| {
                    Err(ConnectionError::Tls(rumqttc::TlsError::TLS(
                        rustls::Error::AlertReceived(rustls::AlertDescription::CertificateExpired),
                    )))
                })
                .once()
                .in_sequence(&mut seq);

            ev_loop
                .expect_clean()
                .return_const(())
                .once()
                .in_sequence(&mut seq);

            ev_loop
                .expect_poll()
                .returning(|| {
                    Ok(MqttEvent::Incoming(Packet::ConnAck(ConnAck {
                        session_present: false,
                        code: ConnectReturnCode::Success,
                    })))
                })
                .once()
                .in_sequence(&mut seq);

            client
                .expect_subscribe::<String>()
                .withf(|topic, qos| {
                    topic == "realm/device_id/control/consumer/properties"
                        && *qos == QoS::ExactlyOnce
                })
                .returning(|_, _| Ok(()))
                .once()
                .in_sequence(&mut seq);

            client
                .expect_publish::<String, String>()
                .withf(|topic, qos, _, introspection| {
                    topic == "realm/device_id"
                        && *qos == QoS::ExactlyOnce
                        && introspection.is_empty()
                })
                .returning(|_, _, _, _| Ok(()))
                .once()
                .in_sequence(&mut seq);

            client
                .expect_publish::<String, &str>()
                .withf(|topic, qos, _, payload| {
                    topic == "realm/device_id/control/emptyCache"
                        && *qos == QoS::ExactlyOnce
                        && *payload == "1"
                })
                .returning(|_, _, _, _| Ok(()))
                .once()
                .in_sequence(&mut seq);

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

        config.connect(&builder).await.unwrap();

        mock_url.assert_async().await;
        mock_cert.assert_async().await;
    }
}
