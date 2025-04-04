// Copyright 2024 - 2025 SECO Mind Srl
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

//! Client to send data to astarte, add interfaces or access properties.

use std::{future::Future, sync::Arc};

use tracing::{error, trace};

use crate::{
    aggregate::AstarteObject,
    error::{AggregationError, InterfaceTypeError},
    interface::mapping::path::MappingError,
    retention::{RetentionId, StoredRetentionExt},
    state::SharedState,
    transport::{mqtt::error::MqttError, Connection, Publish},
    Timestamp,
};
use crate::{error::DynError, transport::Disconnect};
use crate::{
    event::DeviceEvent,
    interface::{mapping::path::MappingPath, reference::MappingRef},
    store::wrapper::StoreWrapper,
    types::AstarteType,
    validate::{ValidatedIndividual, ValidatedObject},
    Error,
};

mod individual;
mod introspection;
mod object;
mod property;

/// Error generated by or received from the connection.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum RecvError {
    /// A generic connection related error.
    ///
    /// Should be downcasted to access the underling specific connection error.
    #[error("connection error, {0:?}")]
    Connection(#[source] DynError),

    /// Couldn't parse the mapping path.
    #[error("invalid mapping path '{}'", .0.path())]
    InvalidEndpoint(#[from] MappingError),

    /// Couldn't find an interface with the given name.
    #[error("couldn't find interface '{name}'")]
    InterfaceNotFound {
        /// Name of the missing interface.
        name: String,
    },

    /// Couldn't find missing mapping in the interface.
    #[error("couldn't find mapping {mapping} in interface {interface}")]
    MappingNotFound {
        /// Name of the interface.
        interface: String,
        /// Path of the missing mapping.
        mapping: String,
    },
    /// Received a message without timestamp, on an interface with `explicit_timestamp`
    #[error("message received with missing explicit timestamp on {interface_name}{path}")]
    MissingTimestamp {
        /// The interface we received the data on.
        interface_name: String,
        /// The mapping path we received on.
        path: String,
    },
    /// Received unset on property without `allow_unset`
    #[error("unset received on property {interface_name}{path} without allow_unset")]
    Unset {
        /// The interface we received the data on.
        interface_name: String,
        /// The mapping path we received on.
        path: String,
    },
    /// Invalid aggregation between the interface and the data.
    #[error("invalid aggregation between interface and data")]
    Aggregation(#[from] AggregationError),
    /// Invalid aggregation between the interface and the data.
    #[error("invalid interface type between interface and data")]
    InterfaceType(#[from] InterfaceTypeError),
    /// Error when the Device is disconnected from Astarte or client.
    ///
    /// This is an unrecoverable error for the SDK.
    #[error("disconnected from astarte")]
    Disconnected,
}

// Safe conversion for connection error
impl RecvError {
    pub(crate) fn mqtt_connection_error(value: MqttError) -> Self {
        RecvError::Connection(value.into())
    }

    #[cfg(feature = "message-hub")]
    pub(crate) fn grpc_connection_error(value: crate::transport::grpc::GrpcError) -> Self {
        RecvError::Connection(value.into())
    }
}

/// A trait representing the behavior of an Astarte device client.
///
/// A device client is responsible for interacting with the Astarte platform by sending properties
/// and datastreams, handling events, and managing device interfaces.
pub trait Client: Clone {
    /// Send an individual datastream on an interface.
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
    ///         .connection(mqtt_config).build().await.unwrap();
    ///
    ///     let value: i32 = 42;
    ///     client.send_individual("my.interface.name", "/endpoint/path", value.into())
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    fn send_individual(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
        data: AstarteType,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Send an individual datastream on an interface, with an explicit timestamp.
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
    ///         .connection(mqtt_config).build().await.unwrap();
    ///
    ///     let value: i32 = 42;
    ///     let timestamp = Utc.timestamp_opt(1537449422, 0).unwrap();
    ///     client.send_individual_with_timestamp("my.interface.name", "/endpoint/path", value.into(), timestamp)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    fn send_individual_with_timestamp(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
        data: AstarteType,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Send an object datastream on an interface.
    ///
    /// The usage is the same of
    /// [`send_object_with_timestamp`](crate::Client::send_object_with_timestamp),
    /// without the timestamp.
    fn send_object(
        &mut self,
        interface_name: &str,
        base_path: &str,
        data: AstarteObject,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Send an object datastream on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{
    ///     store::memory::MemoryStore, builder::DeviceBuilder,
    ///     transport::mqtt::MqttConfig, types::AstarteType, prelude::*,
    /// };
    /// # #[cfg(feature = "derive")]
    /// use astarte_device_sdk::IntoAstarteObject;
    /// # #[cfg(not(feature = "derive"))]
    /// # use astarte_device_sdk_derive::IntoAstarteObject;
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[derive(IntoAstarteObject)]
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
    ///         .connection(mqtt_config).build().await.unwrap();
    ///
    ///     let data = TestObject {
    ///         endpoint1: 1.34,
    ///         endpoint2: false
    ///     };
    ///     let timestamp = Utc.timestamp_opt(1537449422, 0).unwrap();
    ///     client.send_object_with_timestamp("my.interface.name", "/endpoint/path", data.try_into().unwrap(), timestamp)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    fn send_object_with_timestamp(
        &mut self,
        interface_name: &str,
        base_path: &str,
        data: AstarteObject,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Send an individual datastream on an interface.
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
    ///         .connection(mqtt_config).build().await.unwrap();
    ///
    ///     let value: i32 = 42;
    ///     client.set_property("my.interface.name", "/endpoint/path", value.into())
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    fn set_property(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
        data: AstarteType,
    ) -> impl Future<Output = Result<(), Error>> + Send;

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
    ///         .connection(mqtt_config).build().await.unwrap();
    ///
    ///     device
    ///         .unset_property("my.interface.name", "/endpoint/path",)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    fn unset_property(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Receives an event from Astarte.
    ///
    /// # Note
    ///
    /// An event can only be received once, so if the client is cloned only one of the clients
    /// instances will receive the message.
    fn recv(&self) -> impl Future<Output = Result<DeviceEvent, RecvError>> + Send;
}

/// A trait representing the behavior of an Astarte device client to disconnect itself from Astarte.
pub trait ClientDisconnect {
    /// Cleanly disconnects the client consuming it.
    fn disconnect(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Client to send and receive message to and form Astarte or access the Device properties.
///
/// ### Notes
///
/// Cloning the client will not broadcast the [`DeviceEvent`]. Each message can
/// only be received once.
#[derive(Debug)]
pub struct DeviceClient<C>
where
    C: Connection,
{
    // Sender of the connection.
    sender: C::Sender,
    // We use flume instead of the mpsc channel for the DeviceEvents for the connection to che
    // client since we need the Receiver end to be cloneable. Flume provides an async mpmc
    // channel/queue that fits our needs and doesn't suffer from the "slow receiver" problem.
    // Since it doesn't block the sender till all the receivers have read the msg. Unlike the
    // Tokio broadcast channel (another mpmc channel implementation).
    rx: flume::Receiver<Result<DeviceEvent, RecvError>>,
    pub(crate) store: StoreWrapper<C::Store>,
    pub(crate) state: Arc<SharedState>,
}

impl<C> DeviceClient<C>
where
    C: Connection,
{
    pub(crate) fn new(
        sender: C::Sender,
        rx: flume::Receiver<Result<DeviceEvent, RecvError>>,
        store: StoreWrapper<C::Store>,
        state: Arc<SharedState>,
    ) -> Self {
        Self {
            sender,
            rx,
            store,
            state,
        }
    }
}

// Cannot be derived it has specific generic bounds.
impl<C> Clone for DeviceClient<C>
where
    C: Connection,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            rx: self.rx.clone(),
            store: self.store.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<C> Client for DeviceClient<C>
where
    C: Connection,
    C::Sender: Publish,
{
    async fn send_object_with_timestamp(
        &mut self,
        interface_name: &str,
        base_path: &str,
        data: AstarteObject,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error> {
        let path = MappingPath::try_from(base_path)?;

        self.send_datastream_object(interface_name, &path, data, Some(timestamp))
            .await
    }

    async fn send_object(
        &mut self,
        interface_name: &str,
        base_path: &str,
        data: AstarteObject,
    ) -> Result<(), Error> {
        let path = MappingPath::try_from(base_path)?;

        self.send_datastream_object(interface_name, &path, data, None)
            .await
    }

    async fn send_individual(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
        data: AstarteType,
    ) -> Result<(), Error> {
        let path = MappingPath::try_from(mapping_path)?;

        self.send_datastream_individual(interface_name, &path, data, None)
            .await
    }

    async fn send_individual_with_timestamp(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
        data: AstarteType,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error> {
        let mapping = MappingPath::try_from(mapping_path)?;

        self.send_datastream_individual(interface_name, &mapping, data, Some(timestamp))
            .await
    }

    async fn set_property(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
        data: AstarteType,
    ) -> Result<(), Error> {
        trace!("setting property {}{}", interface_name, mapping_path);

        let path = MappingPath::try_from(mapping_path)?;

        self.send_property(interface_name, &path, data).await
    }

    async fn unset_property(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
    ) -> Result<(), Error> {
        trace!("unsetting {}{}", interface_name, mapping_path);

        let path = MappingPath::try_from(mapping_path)?;

        self.unset_prop(interface_name, &path).await
    }

    async fn recv(&self) -> Result<DeviceEvent, RecvError> {
        self.rx
            .recv_async()
            .await
            .map_err(|_| RecvError::Disconnected)?
    }
}

impl<C> ClientDisconnect for DeviceClient<C>
where
    C: Connection,
    C::Sender: Disconnect,
{
    async fn disconnect(&mut self) -> Result<(), Error> {
        self.sender.disconnect().await?;

        self.state.status.close();

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;

    use crate::builder::{DEFAULT_CHANNEL_SIZE, DEFAULT_VOLATILE_CAPACITY};
    use crate::interfaces::Interfaces;
    use crate::retention::memory::VolatileStore;
    use crate::store::memory::MemoryStore;
    use crate::store::StoreCapabilities;
    use crate::transport::mock::{MockCon, MockSender};
    use crate::Interface;

    use super::*;

    pub(crate) fn mock_client(
        interfaces: &[&str],
    ) -> (
        DeviceClient<MockCon<MemoryStore>>,
        flume::Sender<Result<DeviceEvent, RecvError>>,
    ) {
        mock_client_with_store(interfaces, MemoryStore::new())
    }

    pub(crate) fn mock_client_with_store<S>(
        interfaces: &[&str],
        store: S,
    ) -> (
        DeviceClient<MockCon<S>>,
        flume::Sender<Result<DeviceEvent, RecvError>>,
    )
    where
        S: StoreCapabilities,
    {
        let interfaces = interfaces.iter().map(|i| Interface::from_str(i).unwrap());
        let interfaces = Interfaces::from_iter(interfaces);

        let sender = MockSender::new();
        let (tx, rx) = flume::bounded(DEFAULT_CHANNEL_SIZE);
        let state = SharedState::new(
            interfaces,
            VolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY),
        );

        let client = DeviceClient::new(sender, rx, StoreWrapper::new(store), Arc::new(state));

        (client, tx)
    }
}
