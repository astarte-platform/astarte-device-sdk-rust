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
use rumqttc::{ClientError, NoticeFuture, QoS, SubscribeFilter};
use tracing::{debug, error, trace};

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

use super::{Connection, Publish, Receive, ReceivedEvent, Reconnect, Register};

pub use self::config::Credential;
pub use self::config::MqttConfig;
pub use self::pairing::PairingError;
pub use self::payload::PayloadError;

use self::{client::AsyncClient, connection::MqttConnection, error::MqttError, topic::ParsedTopic};

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

/// This struct represents an MQTT connection handler for an Astarte device. It manages the
/// interaction with the MQTT broker, handling connections, subscriptions, and message publishing
/// following the Astarte protocol.
#[derive(Clone, Debug)]
pub struct MqttClient {
    client_id: ClientId,
    client: AsyncClient,
}

impl MqttClient {
    /// Create a new client.
    pub(crate) fn new(client_id: ClientId, client: AsyncClient) -> Self {
        Self { client_id, client }
    }

    /// Send a binary payload over this mqtt connection.
    async fn send(
        &self,
        interface: &str,
        path: &str,
        reliability: rumqttc::QoS,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        self.client
            .publish(
                format!("{}/{interface}{path}", self.client_id),
                reliability,
                false,
                payload,
            )
            .await
            .map_err(|err| MqttError::publish("send", err))?;

        Ok(())
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
}

#[async_trait]
impl Publish for MqttClient {
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
impl Register for MqttClient {
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

impl Display for MqttClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Mqtt Client {}", self.client_id)
    }
}

/// This struct represents an MQTT connection handler for an Astarte device. It manages the
/// interaction with the MQTT broker, handling connections, subscriptions, and message publishing
/// following the Astarte protocol.
pub struct Mqtt {
    client_id: ClientId,
    connection: MqttConnection,
}

impl Mqtt {
    /// Initializes values for this struct
    ///
    /// This method should only be used for testing purposes since it does not fully
    /// connect to the mqtt broker as described by the astarte protocol.
    /// This struct should be constructed with the [`Mqtt::connected`] associated function.
    fn new(client_id: ClientId, connection: MqttConnection) -> Self {
        Self {
            client_id,
            connection,
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
}

#[async_trait]
impl Receive for Mqtt {
    type Payload = Bytes;

    async fn next_event<S>(
        &mut self,
        store: &StoreWrapper<S>,
    ) -> Result<Option<ReceivedEvent<Self::Payload>>, Error>
    where
        S: PropertyStore,
    {
        static PURGE_PROPERTIES_TOPIC: OnceCell<String> = OnceCell::new();

        // We need to construct the ClientId ourselves to not borrow self wail calling the method.
        let client_id = self.client_id.as_ref();

        // Wait for next data or until its disconnected
        while let Some(publish) = self.connection.next_publish().await {
            let purge_topic = PURGE_PROPERTIES_TOPIC
                .get_or_init(|| format!("{}/control/consumer/properties", self.client_id));

            debug!("Incoming publish = {} {:x}", publish.topic, publish.payload);

            if purge_topic == &publish.topic {
                debug!("Purging properties");

                self.purge_server_properties(store, &publish.payload)
                    .await?;
            } else {
                let ParsedTopic { interface, path } =
                    ParsedTopic::try_parse(client_id, &publish.topic).map_err(MqttError::Topic)?;

                return Ok(Some(ReceivedEvent {
                    interface: interface.to_string(),
                    path: path.to_string(),
                    payload: publish.payload,
                }));
            }
        }

        Ok(None)
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
impl Reconnect for Mqtt {
    async fn reconnect<S>(
        &mut self,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> Result<(), crate::Error>
    where
        S: PropertyStore,
    {
        self.connection
            .connect(self.client_id.as_ref(), interfaces, store)
            .await?;

        Ok(())
    }
}

impl Connection for Mqtt {
    type Sender = MqttClient;
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

#[async_trait]
trait AsyncClientExt {
    /// Sends the introspection [`String`].
    async fn send_introspection(
        &self,
        client_id: ClientId<&str>,
        introspection: String,
    ) -> Result<NoticeFuture, ClientError>;

    /// Subscribe to many interfaces
    async fn subscribe_interfaces<S>(
        &self,
        client_id: ClientId<&str>,
        interfaces_names: &[S],
    ) -> Result<Option<NoticeFuture>, ClientError>
    where
        S: Display + Debug + Send + Sync;
}

#[async_trait]
impl AsyncClientExt for AsyncClient {
    async fn send_introspection(
        &self,
        client_id: ClientId<&str>,
        introspection: String,
    ) -> Result<NoticeFuture, ClientError> {
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
    ) -> Result<Option<NoticeFuture>, ClientError>
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

    use mockito::Server;
    use rumqttc::{
        ClientError, ConnAck, ConnectReturnCode, ConnectionError, Event as MqttEvent, Packet, QoS,
    };
    use tempfile::TempDir;
    use test::{
        client::NEW_LOCK,
        pairing::tests::{mock_create_certificate, mock_get_broker_url},
    };

    use crate::builder::{ConnectionConfig, DeviceBuilder};

    use self::{
        client::{AsyncClient, EventLoop},
        config::transport::TransportProvider,
    };

    use super::*;

    pub(crate) fn notify_success<E>() -> Result<NoticeFuture, E> {
        let (tx, notice) = rumqttc::NoticeTx::new();

        tx.success();

        Ok(notice)
    }

    pub(crate) fn mock_mqtt_connection(
        client: AsyncClient,
        eventloop: EventLoop,
    ) -> (MqttClient, Mqtt) {
        let client_id: ClientId = ClientId {
            realm: "realm",
            device_id: "device_id",
        }
        .into();

        let mqtt = Mqtt::new(
            client_id.clone(),
            MqttConnection::new(
                client.clone(),
                eventloop,
                TransportProvider::new(
                    "http://api.astarte.localhost/pairing".parse().unwrap(),
                    "secret".to_string(),
                    None,
                    true,
                ),
                self::connection::Connected,
            ),
        );

        let mqtt_client = MqttClient::new(client_id, client);

        (mqtt_client, mqtt)
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
            .withf(|s, qos| {
                s == "realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#"
                && *qos == QoS::ExactlyOnce
            })
            .in_sequence(&mut seq)
            .returning(|_, _| notify_success());

        client
            .expect_publish::<String, String>()
            .once()
            .in_sequence(&mut seq)
            .withf(move |publish, _, _, payload| {
                let mut intro = payload.split(';').collect_vec();

                intro.sort_unstable();

                publish == "realm/device_id" && intro == introspection
            })
            .returning(|_, _, _, _| notify_success());

        let (mut client, _mqtt_connection) = mock_mqtt_connection(client, eventl);

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
            .returning(|_, _, _, _| notify_success());

        let (mut client, _connection) = mock_mqtt_connection(client, eventl);

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
            .once()
            .withf(|s, qos| {
                *qos == QoS::ExactlyOnce && s == "realm/device_id/org.astarte-platform.rust.examples.individual-properties.ServerProperties/#"
            })
            .in_sequence(&mut seq)
            .returning(|_, _| notify_success());

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

        let (mut mqtt_client, _mqtt_connection) = mock_mqtt_connection(client, eventl);

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
                                .withf(|topic, qos| {
                                    topic == "realm/device_id/control/consumer/properties"
                                        && *qos == QoS::ExactlyOnce
                                })
                                .returning(|_, _| notify_success())
                                .once()
                                .in_sequence(&mut seq);

                            client
                                .expect_publish::<String, String>()
                                .once()
                                .in_sequence(&mut seq)
                                .withf(|topic, qos, _, introspection| {
                                    topic == "realm/device_id"
                                        && *qos == QoS::ExactlyOnce
                                        && introspection.is_empty()
                                })
                                .returning(|_, _, _, _| notify_success());

                            client
                                .expect_publish::<String, &str>()
                                .once()
                                .in_sequence(&mut seq)
                                .withf(|topic, qos, _, payload| {
                                    topic == "realm/device_id/control/emptyCache"
                                        && *qos == QoS::ExactlyOnce
                                        && *payload == "1"
                                })
                                .returning(|_, _, _, _| notify_success());

                            client
                        });

                    client
                });

            ev_loop
                .expect_poll()
                .once()
                .in_sequence(&mut seq)
                .returning(|| {
                    Err(ConnectionError::Tls(rumqttc::TlsError::TLS(
                        rustls::Error::AlertReceived(rustls::AlertDescription::CertificateExpired),
                    )))
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
                    Ok(MqttEvent::Incoming(Packet::ConnAck(ConnAck {
                        session_present: false,
                        code: ConnectReturnCode::Success,
                    })))
                });

            // This guaranties we can keep polling while we are waiting for the ACKs.
            ev_loop
                .expect_poll()
                .returning(|| Ok(MqttEvent::Outgoing(rumqttc::Outgoing::Publish(0))));

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

        tokio::time::timeout(Duration::from_secs(3), config.connect(&builder))
            .await
            .expect("timeout expired")
            .unwrap();

        mock_url.assert_async().await;
        mock_cert.assert_async().await;
    }
}
