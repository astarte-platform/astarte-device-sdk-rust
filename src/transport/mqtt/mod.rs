// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

use std::time::Duration;

use super::{Introspection, Sender, ValidatedProperty};

use self::components::ClientId;

pub use self::config::Credential;
pub use self::config::Mqtt;
pub use self::config::MqttArgs;

pub(crate) mod client;
pub(crate) mod components;
pub(crate) mod config;
pub(crate) mod connection;
pub mod crypto;
pub(crate) mod deps;
pub mod error;
#[cfg(feature = "fdo")]
pub mod fdo;
pub mod pairing;
pub(crate) mod payload;
pub(crate) mod retention;
pub mod topic;

/// Default keep alive interval in seconds for the MQTT connection.
pub const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(15);

#[cfg(test)]
pub(crate) mod test {
    use std::str::FromStr;
    use std::sync::Arc;

    use astarte_device_error::Error;
    use astarte_interfaces::{DatastreamIndividual, Interface, MappingPath};
    use chrono::Utc;
    use mockall::{Sequence, predicate};
    use rumqttc::{AckOfPub, QoS, Resolver, SubAck, Token, UnsubAck};
    use sync_wrapper::SyncWrapper;

    use crate::AstarteData;
    use crate::builder::DEFAULT_CHANNEL_SIZE;
    use crate::error::ErrorKind;
    use crate::interfaces::{DeviceIntrospection, Interfaces, MappingRef};
    use crate::session::{IntrospectionInterface, SessionError};
    use crate::state::tests::mock_state;
    use crate::state::{ConnStatus, ConnectionState};
    use crate::store::mock::MockStore;
    use crate::test::{
        DEVICE_OBJECT, DEVICE_PROPERTIES, E2E_DEVICE_DATASTREAM, E2E_DEVICE_DATASTREAM_NAME,
        SERVER_INDIVIDUAL,
    };
    use crate::transport::RemovedInterface;
    use crate::transport::mqtt::payload::Payload;
    use crate::validate::individual::ValidatedIndividual;

    use self::deps::{AsyncClient, EventLoop};

    use super::client::MqttClient;
    use super::connection::{Connection, MqttConnection};
    use super::*;

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
    ) -> (MqttClient, MqttConnection) {
        let client_id: ClientId = CLIENT_ID.into();

        let (ret_tx, _ret_rx) = tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE.get());

        let mqtt_config = Mqtt {
            client_id: ClientId {
                realm: client_id.realm.clone(),
                device_id: client_id.device_id.clone(),
            },
            credential: Credential::Secret {
                credentials_secret: "credentials_secret".to_string(),
            },
            pairing_url: "http://api.astarte.localhost/pairing".parse().unwrap(),
            keepalive: DEFAULT_KEEP_ALIVE,
        };

        let mqtt = MqttConnection {
            config: mqtt_config,
            connection: Connection {
                eventloop: SyncWrapper::new(eventloop),
                retention: tokio::spawn(std::future::pending()),
                retention_joined: false,
            },
        };

        let mqtt_client = MqttClient {
            id: client_id,
            sender: client,
            retention: ret_tx,
            session_synced: false,
        };

        (mqtt_client, mqtt)
    }

    #[tokio::test]
    async fn should_extend_interfaces() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();
        let mut store = MockStore::new();

        let to_add = [
            Interface::from_str(DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(DEVICE_OBJECT).unwrap(),
            Interface::from_str(SERVER_INDIVIDUAL).unwrap(),
        ];

        let mut introspection = DeviceIntrospection::new(to_add.iter())
            .to_string()
            .split(';')
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();

        introspection.sort_unstable();

        let mut seq = mockall::Sequence::new();

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
                let mut intro: Vec<&str> = payload.split(';').collect();

                intro.sort_unstable();

                publish == "realm/device_id" && intro == introspection
            })
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        store
            .expect_return_session()
            .once()
            .in_sequence(&mut seq)
            .with()
            .return_const(true);

        store
            .expect_add_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let to_add = to_add.clone();

                move |interfaces| {
                    let mut interfaces = interfaces.to_vec();
                    interfaces.sort_unstable();

                    let mut to_add = to_add
                        .iter()
                        .map(IntrospectionInterface::from)
                        .collect::<Vec<_>>();
                    to_add.sort_unstable();

                    interfaces == to_add
                }
            })
            .returning(|_| Ok(()));

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);
        let state = ConnectionState::new(Arc::new(mock_state(store, status_tx, Interfaces::new())));

        let interfaces = state.interfaces().read().await;

        let (mut client, _mqtt_connection) = mock_mqtt_connection(client, eventl).await;

        let to_add = interfaces.validate_many(to_add).unwrap();

        client
            .extend_interfaces(&state, &interfaces, &to_add)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn should_not_subscribe_many_device() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();
        let mut store = MockStore::new();

        // no server owned interfaces are present
        let to_add = [
            Interface::from_str(DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(DEVICE_OBJECT).unwrap(),
        ];

        let mut introspection = DeviceIntrospection::new(to_add.iter())
            .to_string()
            .split(';')
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();

        introspection.sort_unstable();

        let mut seq = mockall::Sequence::new();

        // in this case, no client.subscribe_many() is expected
        client
            .expect_publish::<String, String>()
            .once()
            .in_sequence(&mut seq)
            .withf(move |publish, qos, _, payload| {
                let mut intro: Vec<&str> = payload.split(';').collect();

                intro.sort_unstable();

                publish == "realm/device_id" && intro == introspection && *qos == QoS::ExactlyOnce
            })
            .returning(|_, _, _, _| notify_success(AckOfPub::None));

        store
            .expect_return_session()
            .once()
            .in_sequence(&mut seq)
            .with()
            .return_const(true);

        store
            .expect_add_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let to_add = to_add.clone();

                move |interfaces| {
                    let mut interfaces = interfaces.to_vec();
                    interfaces.sort_unstable();

                    let mut to_add = to_add
                        .iter()
                        .map(IntrospectionInterface::from)
                        .collect::<Vec<_>>();
                    to_add.sort_unstable();

                    interfaces == to_add
                }
            })
            .returning(|_| Ok(()));

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);
        let state = ConnectionState::new(Arc::new(mock_state(store, status_tx, Interfaces::new())));

        let (mut client, _connection) = mock_mqtt_connection(client, eventl).await;

        let interfaces = state.interfaces().read().await;
        let to_add = interfaces.validate_many(to_add).unwrap();

        client
            .extend_interfaces(&state, &interfaces, &to_add)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn should_add_interface() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let introspection = DeviceIntrospection::new([to_add.clone()].iter()).to_string();

        let mut store = MockStore::new();
        // enable session
        store.expect_return_session().return_const(true);

        let mut seq = mockall::Sequence::new();

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

        store
            .expect_add_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let to_add = to_add.clone();

                move |actual| actual == [IntrospectionInterface::from(&to_add)]
            })
            .returning(|_| Ok(()));

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);
        let state = ConnectionState::new(Arc::new(mock_state(store, status_tx, Interfaces::new())));

        let interfaces = state.interfaces().read().await;

        let (mut client, _mqtt_connection) = mock_mqtt_connection(client, eventl).await;

        client
            .add_interface(&state, &interfaces, &to_add)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn add_interface_error() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let introspection = DeviceIntrospection::new([to_add.clone()].iter()).to_string();

        let mut store = MockStore::new();
        let mut seq = mockall::Sequence::new();

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

        // enable session
        store
            .expect_return_session()
            .once()
            .in_sequence(&mut seq)
            .return_const(true);

        store
            .expect_add_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let to_add = to_add.clone();
                move |actual| actual == [IntrospectionInterface::from(&to_add)]
            })
            .returning(|_| Err(Error::new(SessionError::AddInterfaces)));

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);
        let state = ConnectionState::new(Arc::new(mock_state(store, status_tx, Interfaces::new())));
        let interfaces = state.interfaces().read().await;

        let (mut client, _mqtt_connection) = mock_mqtt_connection(client, eventl).await;

        let result = client.add_interface(&state, &interfaces, &to_add).await;

        let err = result.unwrap_err();
        assert_eq!(*err.kind(), ErrorKind::Session(SessionError::AddInterfaces));
    }

    #[tokio::test]
    async fn should_remove_interface() {
        let to_remove = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();
        let mut store = MockStore::new();

        let mut seq = mockall::Sequence::new();

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

        client
            .expect_unsubscribe::<String>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()),
            )
            .in_sequence(&mut seq)
            .returning(|_| notify_success(UnsubAck::new(0)));

        store
            .expect_return_session()
            .once()
            .in_sequence(&mut seq)
            .with()
            .return_const(true);

        store
            .expect_remove_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let to_remove = to_remove.clone();
                move |interfaces| interfaces == [IntrospectionInterface::from(&to_remove)]
            })
            .returning(|_| Ok(()));

        let (mut client, _mqtt_connection) = mock_mqtt_connection(client, eventl).await;

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);
        let state = ConnectionState::new(Arc::new(mock_state(
            store,
            status_tx,
            Interfaces::from_iter([to_remove.clone()]),
        )));

        let interfaces = state.interfaces().read().await;

        client
            .remove_interface(&state, &interfaces, &RemovedInterface::from(&to_remove))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn remove_interface_error() {
        let to_remove = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let mut store = MockStore::new();

        let mut seq = mockall::Sequence::new();

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

        client
            .expect_unsubscribe::<String>()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(format!(
                "realm/device_id/{}/#",
                to_remove.interface_name()
            )))
            .returning(|_| notify_success(UnsubAck::new(0)));

        store
            .expect_return_session()
            .in_sequence(&mut seq)
            .once()
            .return_const(true);

        store
            .expect_remove_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let to_remove = to_remove.clone();
                move |actual| actual == [IntrospectionInterface::from(&to_remove)]
            })
            .returning(|_| Err(Error::new(SessionError::RemoveInterfaces)));

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);
        let state = ConnectionState::new(Arc::new(mock_state(
            store,
            status_tx,
            Interfaces::from_iter([to_remove.clone()]),
        )));

        let (mut client, _mqtt_connection) = mock_mqtt_connection(client, eventl).await;

        let interfaces = state.interfaces().read().await;
        let err = client
            .remove_interface(&state, &interfaces, &RemovedInterface::from(&to_remove))
            .await
            .unwrap_err();

        assert_eq!(
            *err.kind(),
            ErrorKind::Session(SessionError::RemoveInterfaces)
        );
    }

    #[tokio::test]
    async fn should_remove_interfaces() {
        let device_properties = Interface::from_str(DEVICE_PROPERTIES).unwrap();
        let server_properties = Interface::from_str(SERVER_INDIVIDUAL).unwrap();
        let remaining = Interface::from_str(DEVICE_OBJECT).unwrap();

        let to_remove = [device_properties.clone(), server_properties.clone()];

        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let introspection = DeviceIntrospection::new([remaining.clone()].iter()).to_string();

        let mut store = MockStore::new();

        let mut seq = mockall::Sequence::new();

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

        client
            .expect_unsubscribe::<String>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()),
            )
            .in_sequence(&mut seq)
            .returning(|_| notify_success(UnsubAck::new(0)));

        store
            .expect_return_session()
            .once()
            .in_sequence(&mut seq)
            .with()
            .return_const(true);

        store
            .expect_remove_interfaces()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let to_remove = to_remove.clone();

                move |actual| {
                    let to_remove = to_remove
                        .iter()
                        .map(IntrospectionInterface::from)
                        .collect::<Vec<_>>();

                    actual == to_remove
                }
            })
            .returning(|_| Ok(()));

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);
        let state = ConnectionState::new(Arc::new(mock_state(
            store,
            status_tx,
            Interfaces::from_iter([
                device_properties.clone(),
                server_properties.clone(),
                remaining.clone(),
            ]),
        )));

        let (mut client, _mqtt_connection) = mock_mqtt_connection(client, eventl).await;

        let interfaces = state.interfaces().read().await;
        let to_remove = to_remove
            .iter()
            .map(RemovedInterface::from)
            .collect::<Vec<_>>();
        client
            .remove_interfaces(&state, &interfaces, &to_remove)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn should_send_individual_success() {
        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);
        let _state = ConnectionState::new(Arc::new(mock_state(
            MockStore::new(),
            status_tx,
            Interfaces::new(),
        )));

        let mut client = AsyncClient::default();
        let eventloop = EventLoop::default();

        let mut seq = Sequence::new();

        let path = MappingPath::try_from("/integer_endpoint").unwrap();
        let interface = DatastreamIndividual::from_str(E2E_DEVICE_DATASTREAM).unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();
        let timestamp = Utc::now();
        let value = AstarteData::Integer(42);

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

        let (mut client, _connection) = mock_mqtt_connection(client, eventloop).await;

        let data = ValidatedIndividual::validate(mapping, value, Some(timestamp)).unwrap();

        client.send_individual(data).await.unwrap();
    }
}
