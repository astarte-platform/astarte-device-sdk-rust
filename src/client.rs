// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Client to send data to astarte, add interfaces or access properties.

use std::{path::Path, str::FromStr, sync::Arc};

use async_trait::async_trait;
use log::{debug, error, trace};
use tokio::{
    fs,
    sync::{mpsc, oneshot, RwLock},
};

use crate::{
    connection::ClientMessage,
    event::DeviceEvent,
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, PropertyRef},
        Aggregation as InterfaceAggregation, InterfaceTypeDef,
    },
    interfaces::Interfaces,
    introspection::{AddInterfaceError, DeviceIntrospection, DynamicIntrospection},
    store::{wrapper::StoreWrapper, PropertyStore},
    types::{AstarteType, TypeError},
    validate::{ValidatedIndividual, ValidatedObject, ValidatedUnset},
    AstarteAggregate, Error, Interface,
};

/// A trait representing the behavior of an Astarte device client. A device client is responsible
/// for interacting with the Astarte platform by sending properties and datastreams, handling events, and managing
/// device interfaces.
#[async_trait]
pub trait Client {
    /// Send an object datastream on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{
    ///     store::memory::MemoryStore, builder::DeviceBuilder,
    ///     transport::mqtt::MqttConfig, types::AstarteType, prelude::*,
    /// };
    /// #[cfg(not(feature = "derive"))]
    /// use astarte_device_sdk_derive::AstarteAggregate;
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[derive(AstarteAggregate)]
    /// struct TestObject {
    ///     endpoint1: f64,
    ///     endpoint2: bool,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mqtt_config = MqttConfig::with_credential_secret("realm_id", "device_id", "credential_secret", "pairing_url");
    ///
    ///     let (mut client, connection) = DeviceBuilder::new().store(MemoryStore::new())
    ///         .connect(mqtt_config).await.unwrap().build();
    ///
    ///     let data = TestObject {
    ///         endpoint1: 1.34,
    ///         endpoint2: false
    ///     };
    ///     let timestamp = Utc.timestamp_opt(1537449422, 0).unwrap();
    ///     client.send_object_with_timestamp("my.interface.name", "/endpoint/path", data, timestamp)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    async fn send_object_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send;

    /// Send an object datastream on an interface.
    ///
    /// The usage is the same of
    /// [`send_object_with_timestamp`](crate::Client::send_object_with_timestamp),
    /// without the timestamp.
    async fn send_object<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send;

    /// Send an individual datastream/property on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{
    ///     store::memory::MemoryStore, builder::DeviceBuilder,
    ///     transport::mqtt::MqttConfig, types::AstarteType, prelude::*,
    /// };
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mqtt_config = MqttConfig::with_credential_secret("realm_id", "device_id", "credential_secret", "pairing_url");
    ///
    ///     let (mut client, connection) = DeviceBuilder::new().store(MemoryStore::new())
    ///         .connect(mqtt_config).await.unwrap().build();
    ///
    ///     let value: i32 = 42;
    ///     let timestamp = Utc.timestamp_opt(1537449422, 0).unwrap();
    ///     client.send_with_timestamp("my.interface.name", "/endpoint/path", value, timestamp)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    async fn send_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send;

    /// Send an individual datastream/property on an interface.
    ///
    /// The usage is the same of
    /// [`send_with_timestamp`](Client::send_with_timestamp), without the timestamp.
    async fn send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send;

    /// Unset a device property.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{
    ///     store::memory::MemoryStore, builder::DeviceBuilder,
    ///     transport::mqtt::MqttConfig, types::AstarteType, prelude::*,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mqtt_config = MqttConfig::with_credential_secret("realm_id", "device_id", "credential_secret", "pairing_url");
    ///
    ///     let (mut device, _connection) = DeviceBuilder::new().store(MemoryStore::new())
    ///         .connect(mqtt_config).await.unwrap().build();
    ///
    ///     device
    ///         .unset("my.interface.name", "/endpoint/path",)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error>;

    /// Receives an event from Astarte.
    ///
    /// # Note
    ///
    /// An event can only be received once, so if the client is cloned only one of the clients
    /// instances will receive the message.
    async fn recv(&self) -> Result<DeviceEvent, Error>;
}

/// Client to send and receive message to and form Astarte or access the Device properties.
///
/// ### Notes
///
/// Cloning the client will not broadcast the [`DeviceEvent`]. Each message can
/// only be received once.
#[derive(Debug, Clone)]
pub struct DeviceClient<S> {
    pub(crate) interfaces: Arc<RwLock<Interfaces>>,
    // We use flume instead of the mpsc channel for the DeviceEvents for the connection to che
    // client since we need the Receiver end to be clonable. Flume provides an async mpmc
    // channel/queue that fits our needs and doesn't suffer from the "slow receiver" problem.
    // Since it doesn't block the sender till all the receivers have read the msg. Unlike the
    // Tokio broadcast channel (another mpmc channel implementation).
    pub(crate) rx: flume::Receiver<Result<DeviceEvent, Error>>,
    pub(crate) connection: mpsc::Sender<ClientMessage>,
    pub(crate) store: StoreWrapper<S>,
}

impl<S> DeviceClient<S> {
    pub(crate) fn new(
        interfaces: Arc<RwLock<Interfaces>>,
        rx: flume::Receiver<Result<DeviceEvent, Error>>,
        connection: mpsc::Sender<ClientMessage>,
        store: StoreWrapper<S>,
    ) -> Self {
        Self {
            interfaces,
            rx,
            connection,
            store,
        }
    }

    async fn send_msg(&self, msg: ClientMessage) -> Result<(), Error> {
        self.connection
            .send(msg)
            .await
            .map_err(|_| Error::Disconnected)
    }

    /// Checks whether a passed interface is a property and if it is already stored with the same value.
    /// Useful to prevent sending a property twice with the same value.
    async fn is_prop_stored<'a>(
        &self,
        mapping: &MappingRef<'a, &'a Interface>,
        path: &MappingPath<'_>,
        new: &AstarteType,
    ) -> Result<bool, Error>
    where
        S: PropertyStore,
    {
        let Some(prop_mapping) = mapping.as_prop() else {
            return Ok(false);
        };

        // Check if this property is already in db
        let stored = self.try_load_prop(&prop_mapping, path).await?;

        Ok(stored.is_some_and(|val| val == *new))
    }

    /// Get a property or deletes it if a version or type miss-match happens.
    pub(crate) async fn try_load_prop(
        &self,
        mapping: &MappingRef<'_, PropertyRef<'_>>,
        path: &MappingPath<'_>,
    ) -> Result<Option<AstarteType>, Error>
    where
        S: PropertyStore,
    {
        let interface = mapping.interface().interface_name();
        let path = path.as_str();

        let value = self
            .store
            .load_prop(interface, path, mapping.interface().version_major())
            .await?;

        let value = match value {
            Some(value) if value != mapping.mapping_type() => {
                error!(
                    "stored property type mismatch, expected {} got {:?}",
                    mapping.mapping_type(),
                    value
                );
                self.store.delete_prop(interface, path).await?;

                None
            }

            Some(value) => Some(value),
            None => None,
        };

        Ok(value)
    }

    async fn send_individual_impl<D>(
        &self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error>
    where
        S: PropertyStore,
        D: TryInto<AstarteType> + Send,
    {
        let interfaces = self.interfaces.read().await;
        let mapping = interfaces.interface_mapping(interface_name, path)?;

        let individual = data.try_into().map_err(|_| TypeError::Conversion)?;

        trace!("sending individual type {}", individual.display_type());

        if self.is_prop_stored(&mapping, path, &individual).await? {
            debug!("property was already sent, no need to send it again");
            return Ok(());
        }

        let validated = ValidatedIndividual::validate(mapping, path, individual, timestamp)?;

        let msg = if mapping.interface().is_property() {
            ClientMessage::Property {
                data: validated,
                version_major: mapping.interface().version_major(),
            }
        } else {
            ClientMessage::Individual(validated)
        };

        self.send_msg(msg).await
    }

    async fn send_object_impl<'a, D>(
        &self,
        interface_name: &str,
        path: &MappingPath<'a>,
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send,
    {
        let interfaces = self.interfaces.read().await;
        let interface = interfaces
            .get(interface_name)
            .ok_or_else(|| Error::InterfaceNotFound {
                name: interface_name.to_string(),
            })?;

        let object = interface
            .as_object_ref()
            .ok_or_else(|| Error::Aggregation {
                exp: InterfaceAggregation::Object,
                got: interface.aggregation(),
            })?;

        debug!("sending {} {}", interface_name, path);

        let aggregate = data.astarte_aggregate()?;

        let validated = ValidatedObject::validate(object, path, aggregate, timestamp)?;

        self.send_msg(ClientMessage::Object(validated)).await
    }

    async fn unset_prop<'a>(
        &self,
        interface_name: &str,
        path: &MappingPath<'a>,
    ) -> Result<(), Error>
    where
        S: PropertyStore,
    {
        let interfaces = self.interfaces.read().await;
        let interface = interfaces
            .get(interface_name)
            .ok_or_else(|| Error::InterfaceNotFound {
                name: interface_name.to_string(),
            })?;

        let mapping = interface
            .as_mapping_ref(path)
            .ok_or_else(|| Error::MappingNotFound {
                interface: interface.to_string(),
                mapping: path.to_string(),
            })?;

        let mapping = mapping.as_prop().ok_or_else(|| Error::InterfaceType {
            exp: InterfaceTypeDef::Properties,
            got: interface.interface_type(),
        })?;

        let validated = ValidatedUnset::validate(mapping, path)?;

        debug!("unsetting property {interface_name}{path}");

        self.send_msg(ClientMessage::Unset(validated)).await
    }
}

#[async_trait]
impl<S> Client for DeviceClient<S>
where
    S: PropertyStore,
{
    async fn send_object_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_object_impl(interface_name, &path, data, Some(timestamp))
            .await
    }

    async fn send_object<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_object_impl(interface_name, &path, data, None)
            .await
    }

    async fn send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_individual_impl(interface_name, &path, data, None)
            .await
    }

    async fn send_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send,
    {
        let mapping = MappingPath::try_from(interface_path)?;

        self.send_individual_impl(interface_name, &mapping, data, Some(timestamp))
            .await
    }

    async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error> {
        trace!("unsetting {} {}", interface_name, interface_path);

        let path = MappingPath::try_from(interface_path)?;

        self.unset_prop(interface_name, &path).await
    }

    async fn recv(&self) -> Result<DeviceEvent, Error> {
        self.rx
            .recv_async()
            .await
            .map_err(|_| Error::Disconnected)?
    }
}

#[async_trait]
impl<S> DeviceIntrospection for DeviceClient<S>
where
    S: Send + Sync,
{
    async fn get_interface<F, O>(&self, interface_name: &str, mut f: F) -> O
    where
        F: FnMut(Option<&Interface>) -> O + Send,
    {
        let interfaces = self.interfaces.read().await;

        f(interfaces.get(interface_name))
    }
}

#[async_trait]
impl<S> DynamicIntrospection for DeviceClient<S>
where
    S: Send + Sync,
{
    async fn add_interface(&self, interface: Interface) -> Result<bool, Error> {
        let (tx, rx) = oneshot::channel();

        self.send_msg(ClientMessage::AddInterface {
            interface,
            response: tx,
        })
        .await?;

        rx.await.map_err(|_| Error::Disconnected)?
    }

    async fn extend_interfaces<I>(&self, interfaces: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = Interface> + Send,
    {
        let interfaces = interfaces.into_iter().collect();

        self.extend_interfaces_vec(interfaces).await
    }

    async fn extend_interfaces_vec(
        &self,
        interfaces: Vec<Interface>,
    ) -> Result<Vec<String>, Error> {
        let (tx, rx) = oneshot::channel();

        self.send_msg(ClientMessage::ExtendInterfaces {
            interfaces,
            response: tx,
        })
        .await?;

        rx.await.map_err(|_| Error::Disconnected)?
    }

    async fn add_interface_from_file<P>(&self, file_path: P) -> Result<bool, Error>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let interface =
            fs::read_to_string(&file_path)
                .await
                .map_err(|err| AddInterfaceError::Io {
                    path: file_path.as_ref().to_owned(),
                    backtrace: err,
                })?;

        let interface =
            Interface::from_str(&interface).map_err(|err| AddInterfaceError::InterfaceFile {
                path: file_path.as_ref().to_owned(),
                backtrace: err,
            })?;

        self.add_interface(interface).await
    }

    async fn add_interface_from_str(&self, json_str: &str) -> Result<bool, Error> {
        let interface = Interface::from_str(json_str).map_err(AddInterfaceError::Interface)?;

        self.add_interface(interface).await
    }

    async fn remove_interface(&self, interface_name: &str) -> Result<bool, Error> {
        let (tx, rx) = oneshot::channel();

        self.send_msg(ClientMessage::RemoveInterface {
            interface: interface_name.to_string(),
            response: tx,
        })
        .await?;

        rx.await.map_err(|_| Error::Disconnected)?
    }

    async fn remove_interfaces<I>(&self, interfaces_name: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = String> + Send,
        I::IntoIter: Send,
    {
        let (tx, rx) = oneshot::channel();

        self.send_msg(ClientMessage::RemoveInterfaces {
            interfaces: interfaces_name.into_iter().collect(),
            response: tx,
        })
        .await?;

        rx.await.map_err(|_| Error::Disconnected)?
    }
}
