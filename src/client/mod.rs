// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
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

use std::future::Future;

use astarte_device_error::{ResultExt, WrapError};
use astarte_interfaces::MappingPath;
use astarte_interfaces::interface::Retention;
use chrono::{DateTime, Utc};
use tracing::{debug, error, info, trace, warn};

use crate::aggregate::AstarteObject;
use crate::error::{AstarteError, ErrorKind, InterfaceError};
use crate::event::DeviceEvent;
use crate::logging::security::{SecurityEvent, notify_security_event};
use crate::pairing::Pairing;
use crate::retention::memory::{ItemValue, VolatileItemError};
use crate::retention::{
    Id, RetentionId, StoredRetention, StoredRetentionExt, stored_mark_unsent, volatile_mark_unsent,
};
use crate::state::{ClientState, ConnStatus};
use crate::store::StoreCapabilities;
use crate::transport::mqtt::Mqtt;
use crate::transport::{Connection, Disconnect, Publish};
use crate::types::AstarteData;
use crate::validate::{ValidatedIndividual, ValidatedObject};

mod individual;
mod introspection;
mod object;
mod property;

/// A trait representing the behavior of an Astarte device client.
///
/// A device client is responsible for interacting with the Astarte platform by sending properties
/// and datastreams, handling events, and managing device interfaces.
pub trait Client: Clone {
    /// Send an individual datastream on an interface.
    ///
    /// ```no_run
    /// use astarte_device_sdk::builder::DeviceBuilder;
    /// use astarte_device_sdk::prelude::*;
    /// use astarte_device_sdk::store::memory::MemoryStore;
    /// use astarte_device_sdk::transport::mqtt::{MqttConfig, MqttArgs, Credential};
    /// use astarte_device_sdk::types::AstarteData;
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let args = MqttArgs{
    ///         realm: "realm_id".to_string(),
    ///         device_id: "device_id".to_string(),
    ///         credential: Credential::secret("credential_secret"),
    ///         pairing_url: "http://api.astarte.localhost/pairing".parse().expect("a valid URL")
    ///     };
    ///     let mqtt_config = MqttConfig::new(args);
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
        data: AstarteData,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send;

    /// Send an individual datastream on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// use astarte_device_sdk::builder::DeviceBuilder;
    /// use astarte_device_sdk::prelude::*;
    /// use astarte_device_sdk::store::memory::MemoryStore;
    /// use astarte_device_sdk::transport::mqtt::{MqttConfig, MqttArgs, Credential};
    /// use astarte_device_sdk::types::AstarteData;
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let args = MqttArgs{
    ///         realm: "realm_id".to_string(),
    ///         device_id: "device_id".to_string(),
    ///         credential: Credential::secret("credential_secret"),
    ///         pairing_url: "http://api.astarte.localhost/pairing".parse().expect("a valid URL")
    ///     };
    ///     let mqtt_config = MqttConfig::new(args);
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
        data: AstarteData,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send;

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
    ) -> impl Future<Output = Result<(), AstarteError>> + Send;

    /// Send an object datastream on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// use astarte_device_sdk::store::memory::MemoryStore;
    /// use astarte_device_sdk::builder::DeviceBuilder;
    /// use astarte_device_sdk::transport::mqtt::{MqttConfig, Credential, MqttArgs};
    /// use astarte_device_sdk::types::AstarteData;
    /// use astarte_device_sdk::prelude::*;
    /// # #[cfg(feature = "derive")]
    /// use astarte_device_sdk::IntoAstarteObject;
    /// # #[cfg(not(feature = "derive"))]
    /// # use astarte_device_sdk_derive::IntoAstarteObject;
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[derive(IntoAstarteObject)]
    /// struct TestObject {
    ///     #[astarte_object(failable)]
    ///     endpoint1: f64,
    ///     endpoint2: bool,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let args = MqttArgs {
    ///         realm: "realm_id".to_string(),
    ///         device_id: "device_id".to_string(),
    ///         credential: Credential::secret("credential_secret"),
    ///         pairing_url: "http://api.astarte.localhost/pairing".parse().expect("a valid URL")
    ///     };
    ///     let mqtt_config = MqttConfig::new(args);
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
    ) -> impl Future<Output = Result<(), AstarteError>> + Send;

    /// Send an individual datastream on an interface.
    ///
    /// ```no_run
    /// use astarte_device_sdk::builder::DeviceBuilder;
    /// use astarte_device_sdk::prelude::*;
    /// use astarte_device_sdk::store::memory::MemoryStore;
    /// use astarte_device_sdk::transport::mqtt::{MqttConfig, MqttArgs, Credential};
    /// use astarte_device_sdk::types::AstarteData;
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let args = MqttArgs{
    ///         realm: "realm_id".to_string(),
    ///         device_id: "device_id".to_string(),
    ///         credential: Credential::secret("credential_secret"),
    ///         pairing_url: "http://api.astarte.localhost/pairing".parse().expect("a valid URL")
    ///     };
    ///     let mqtt_config = MqttConfig::new(args);
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
        data: AstarteData,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send;

    /// Unset a device property.
    ///
    /// ```no_run
    /// use astarte_device_sdk::builder::DeviceBuilder;
    /// use astarte_device_sdk::prelude::*;
    /// use astarte_device_sdk::store::memory::MemoryStore;
    /// use astarte_device_sdk::transport::mqtt::{MqttConfig, MqttArgs, Credential};
    /// use astarte_device_sdk::types::AstarteData;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let args = MqttArgs {
    ///         realm: "realm_id".to_string(),
    ///         device_id: "device_id".to_string(),
    ///         credential: Credential::secret("credential_secret"),
    ///         pairing_url: "http://api.astarte.localhost/pairing".parse().expect("a valid URL")
    ///     };
    ///     let mqtt_config = MqttConfig::new(args);
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
    ) -> impl Future<Output = Result<(), AstarteError>> + Send;

    /// Receives an event from Astarte.
    ///
    /// When receiving a [`None`] event, the device is disconnect.
    ///
    /// # Note
    ///
    /// An event can only be received once, so if the client is cloned only one of the clients
    /// instances will receive the message.
    fn recv(&self) -> impl Future<Output = Option<DeviceEvent>> + Send;
}

/// Connection of the Client.
///
/// Manages introspects the connection and device status.
pub trait ClientConnection {
    /// Cleanly disconnects the client consuming it.
    fn disconnect(&mut self) -> impl Future<Output = Result<(), AstarteError>> + Send;

    /// Check if the client is already paired.
    fn is_paired(&self) -> bool;
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
    // We use multi producer multi consumer instead of the mpsc channel for the DeviceEvents for the connection to che
    // client since we need the Receiver end to be cloneable.
    // The tokio Broadcast channel provides an async mpmc, but suffer from the "slow receiver" problem.
    events: async_channel::Receiver<DeviceEvent>,
    pub(crate) disconnect: async_channel::Sender<()>,
    pub(crate) store: C::Store,
    pub(crate) state: ClientState,
}

impl<C> DeviceClient<C>
where
    C: Connection,
{
    pub(crate) fn new(
        sender: C::Sender,
        rx: async_channel::Receiver<DeviceEvent>,
        store: C::Store,
        state: ClientState,
        disconnect: async_channel::Sender<()>,
    ) -> Self {
        Self {
            sender,
            events: rx,
            store,
            state,
            disconnect,
        }
    }

    async fn send<T>(
        state: &ClientState,
        store: &C::Store,
        sender: &mut C::Sender,
        data: T,
    ) -> Result<(), AstarteError>
    where
        T: ClientPacket + TryInto<ItemValue, Error = VolatileItemError> + Clone,
        C::Store: StoreCapabilities,
        C::Sender: Publish,
    {
        match state.connection().await {
            ConnStatus::Connected => {
                trace!("publish while connection is connected");
            }
            ConnStatus::Disconnected => {
                trace!("publish while connection is offline");

                return Self::offline_send(state, store, sender, data).await;
            }
            ConnStatus::Closed => {
                trace!("publish while connection is closed");

                if let Err(error) = Self::offline_send(state, store, sender, data).await {
                    error!(%error, "couldn't store the send");
                }

                return Err(AstarteError::with(
                    ErrorKind::Disconnected,
                    "cannot send data",
                ));
            }
        }

        match data.get_retention() {
            Retention::Volatile { .. } => Self::send_volatile(state, sender, data).await,
            Retention::Stored { .. } => Self::send_stored(state, store, sender, data).await,
            Retention::Discard => data.send(sender).await,
        }
    }

    async fn offline_send<T>(
        state: &ClientState,
        store: &C::Store,
        sender: &mut C::Sender,
        data: T,
    ) -> Result<(), AstarteError>
    where
        T: ClientPacket + TryInto<ItemValue, Error = VolatileItemError>,
        C::Store: StoreCapabilities,
        C::Sender: Publish,
    {
        match data.get_retention() {
            Retention::Discard => {
                debug!("drop publish with retention discard since disconnected");
            }
            Retention::Volatile { .. } => {
                let id = state.retention_ctx().next();

                state.volatile_store().push_unsent(id, data).await;
            }
            Retention::Stored { .. } => {
                let id = state.retention_ctx().next();

                if let Some(retention) = store.get_retention() {
                    data.store_publish(&id, sender, retention, false).await?;
                } else {
                    warn!(
                        ?store,
                        "storing interface with retention 'Stored' in volatile store since the store doesn't support retention"
                    );
                    state.volatile_store().push_unsent(id, data).await;
                }
            }
        }

        Ok(())
    }

    async fn send_stored<T>(
        state: &ClientState,
        store: &C::Store,
        sender: &mut C::Sender,
        data: T,
    ) -> Result<(), AstarteError>
    where
        T: ClientPacket + TryInto<ItemValue, Error = VolatileItemError> + Clone,
        C::Store: StoreCapabilities,
        C::Sender: Publish,
    {
        let Some(retention) = store.get_retention() else {
            warn!(
                ?store,
                "storing interface with retention 'Stored' in volatile store since the store doesn't support retention"
            );
            return Self::send_volatile(state, sender, data).await;
        };

        // generate id after the check to avoid wasting an id generation in case it gets regenerated in send_volatile
        let id = state.retention_ctx().next();

        data.store_publish(&id, sender, retention, true).await?;

        let result = data.send_stored(RetentionId::Stored(id), sender).await;

        if result.is_err() {
            error!("error while sending stored marking unsent");
            stored_mark_unsent(store, &id).await;
        }

        result
    }

    async fn send_volatile<T>(
        state: &ClientState,
        sender: &mut C::Sender,
        data: T,
    ) -> Result<(), AstarteError>
    where
        T: ClientPacket + TryInto<ItemValue, Error = VolatileItemError> + Clone,
        C::Store: StoreCapabilities,
        C::Sender: Publish,
    {
        let id = state.retention_ctx().next();

        state.volatile_store().push_sent(id, data.clone()).await;

        let result = data.send_stored(RetentionId::Volatile(id), sender).await;

        if result.is_err() {
            error!("error while sending volatile marking unsent");
            volatile_mark_unsent(state.volatile_store(), &id).await;
        }

        result
    }
}

impl<S, P> DeviceClient<Mqtt<S, P>>
where
    S: StoreCapabilities,
    P: Pairing,
{
    /// Retrieve the expiry (not_after) timestamp of the current certificate
    pub async fn get_cert_expiry(&self) -> Option<DateTime<Utc>> {
        self.state.cert_expiry().await
    }

    /// Retrieve the expiry (not_after) timestamp of the current certificate
    /// Note that this function will log a security event if the feature is enabled
    /// when the certificate will expire at the passed datetime
    pub async fn is_valid_at(&self, check_dt: DateTime<Utc>) -> Option<bool> {
        let expiry = self.get_cert_expiry().await?;

        if check_dt < expiry {
            Some(true)
        } else {
            notify_security_event(SecurityEvent::CertificateAboutToExpire);

            Some(false)
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
            events: self.events.clone(),
            store: self.store.clone(),
            state: self.state.clone(),
            disconnect: self.disconnect.clone(),
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
    ) -> Result<(), AstarteError> {
        let path = MappingPath::try_from(base_path)
            .wrap_err(ErrorKind::Interface(InterfaceError::Path))?;

        self.send_datastream_object(interface_name, &path, data, Some(timestamp))
            .await
    }

    async fn send_object(
        &mut self,
        interface_name: &str,
        base_path: &str,
        data: AstarteObject,
    ) -> Result<(), AstarteError> {
        let path = MappingPath::try_from(base_path)
            .wrap_err(ErrorKind::Interface(InterfaceError::Path))?;

        self.send_datastream_object(interface_name, &path, data, None)
            .await
    }

    async fn send_individual(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
        data: AstarteData,
    ) -> Result<(), AstarteError> {
        let path = MappingPath::try_from(mapping_path)
            .wrap_err(ErrorKind::Interface(InterfaceError::Path))?;

        self.send_datastream_individual(interface_name, &path, data, None)
            .await
    }

    async fn send_individual_with_timestamp(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
        data: AstarteData,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), AstarteError> {
        let mapping = MappingPath::try_from(mapping_path)
            .wrap_err(ErrorKind::Interface(InterfaceError::Path))?;

        self.send_datastream_individual(interface_name, &mapping, data, Some(timestamp))
            .await
    }

    async fn set_property(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
        data: AstarteData,
    ) -> Result<(), AstarteError> {
        trace!("setting property {}{}", interface_name, mapping_path);

        let path = MappingPath::try_from(mapping_path)
            .wrap_err(ErrorKind::Interface(InterfaceError::Path))?;

        self.send_property(interface_name, &path, data).await
    }

    async fn unset_property(
        &mut self,
        interface_name: &str,
        mapping_path: &str,
    ) -> Result<(), AstarteError> {
        trace!("unsetting {}{}", interface_name, mapping_path);

        let path = MappingPath::try_from(mapping_path)
            .wrap_err(ErrorKind::Interface(InterfaceError::Path))?;

        self.send_unset(interface_name, &path).await
    }

    async fn recv(&self) -> Option<DeviceEvent> {
        self.events.recv().await.ok()
    }
}

impl<C> ClientConnection for DeviceClient<C>
where
    C: Connection,
    C::Sender: Disconnect,
{
    async fn disconnect(&mut self) -> Result<(), AstarteError> {
        if self.state.connection().await == ConnStatus::Closed {
            debug!("connection already closed");

            return Ok(());
        }

        self.sender.disconnect().await?;

        info!("device disconnected");

        if let Err(error) = self.disconnect.try_send(()) {
            error!(%error, "multiple clients trying to disconnect");
        }

        Ok(())
    }

    fn is_paired(&self) -> bool {
        self.state.is_device_paired()
    }
}

trait ClientPacket {
    fn get_retention(&self) -> Retention;

    fn serialize<S>(&self, sender: &S) -> Result<Vec<u8>, AstarteError>
    where
        S: Publish;

    fn send<S>(self, sender: &mut S) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: Publish + Send;

    fn send_stored<S>(
        self,
        id: RetentionId,
        sender: &mut S,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: Publish + Send;

    fn store_publish<S, R>(
        &self,
        id: &Id,
        sender: &S,
        retention: &R,
        sent: bool,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: Publish + Sync,
        R: StoredRetention + Sync;
}

impl ClientPacket for ValidatedIndividual {
    fn get_retention(&self) -> Retention {
        self.retention
    }

    fn serialize<S>(&self, sender: &S) -> Result<Vec<u8>, AstarteError>
    where
        S: Publish,
    {
        sender.serialize_individual(self)
    }

    async fn send<S>(self, sender: &mut S) -> Result<(), AstarteError>
    where
        S: Publish + Send,
    {
        sender.send_individual(self).await
    }

    async fn send_stored<S>(self, id: RetentionId, sender: &mut S) -> Result<(), AstarteError>
    where
        S: Publish,
    {
        sender.send_individual_stored(id, self).await
    }

    async fn store_publish<S, R>(
        &self,
        id: &Id,
        sender: &S,
        retention: &R,
        sent: bool,
    ) -> Result<(), AstarteError>
    where
        S: Publish + Sync,
        R: StoredRetention + Sync,
    {
        let serialized = self.serialize(sender)?;

        retention
            .store_publish_individual(id, self, &serialized, sent)
            .await
            .map_kind(ErrorKind::Retention)
    }
}

impl ClientPacket for ValidatedObject {
    fn get_retention(&self) -> Retention {
        self.retention
    }

    fn serialize<S>(&self, sender: &S) -> Result<Vec<u8>, AstarteError>
    where
        S: Publish,
    {
        sender.serialize_object(self)
    }

    async fn send<S>(self, sender: &mut S) -> Result<(), AstarteError>
    where
        S: Publish + Send,
    {
        sender.send_object(self).await
    }

    async fn send_stored<S>(self, id: RetentionId, sender: &mut S) -> Result<(), AstarteError>
    where
        S: Publish + Send,
    {
        sender.send_object_stored(id, self).await
    }

    async fn store_publish<S, R>(
        &self,
        id: &Id,
        sender: &S,
        retention: &R,
        sent: bool,
    ) -> Result<(), AstarteError>
    where
        S: Publish + Sync,
        R: StoredRetention + Sync,
    {
        let serialized = self.serialize(sender)?;

        retention
            .store_publish_object(id, self, &serialized, sent)
            .await
            .map_kind(ErrorKind::Retention)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ops::{Deref, DerefMut};
    use std::str::FromStr;
    use std::sync::Arc;

    use astarte_interfaces::Interface;
    use chrono::Utc;
    use mockall::Sequence;
    use pretty_assertions::assert_eq;

    use crate::Value;
    use crate::builder::{Config, DEFAULT_CHANNEL_SIZE, DEFAULT_VOLATILE_CAPACITY};
    use crate::interfaces::Interfaces;
    use crate::retention::memory::VolatileStore;
    use crate::state::SharedState;
    use crate::store::StoreCapabilities;
    use crate::store::memory::MemoryStore;
    use crate::transport::mock::{MockCon, MockSender};

    use super::*;

    pub(crate) struct TestClient<S>
    where
        S: StoreCapabilities,
    {
        client: DeviceClient<MockCon<S>>,
        pub(crate) disconnect: async_channel::Receiver<()>,
        pub(crate) events: async_channel::Sender<DeviceEvent>,
    }

    impl<S> Deref for TestClient<S>
    where
        S: StoreCapabilities,
    {
        type Target = DeviceClient<MockCon<S>>;

        fn deref(&self) -> &Self::Target {
            &self.client
        }
    }

    impl<S> DerefMut for TestClient<S>
    where
        S: StoreCapabilities,
    {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.client
        }
    }

    pub(crate) fn mock_client(
        interfaces: &[&str],
        initial_status: ConnStatus,
    ) -> TestClient<MemoryStore> {
        mock_client_with_store(interfaces, initial_status, MemoryStore::new())
    }

    pub(crate) fn mock_client_with_store<S>(
        interfaces: &[&str],
        initial_status: ConnStatus,
        store: S,
    ) -> TestClient<S>
    where
        S: StoreCapabilities,
    {
        let interfaces = interfaces.iter().map(|i| Interface::from_str(i).unwrap());
        let interfaces = Interfaces::from_iter(interfaces);

        let sender = MockSender::new();
        let (events_tx, events_rx) = async_channel::bounded(DEFAULT_CHANNEL_SIZE.get());
        let (disconnect_tx, disconnect_rx) = async_channel::bounded(1);

        let mut state = SharedState::new(
            Config::default(),
            interfaces,
            VolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY.get()),
        );

        *state.status.get_mut() = initial_status;

        let client = DeviceClient::new(
            sender,
            events_rx,
            store,
            ClientState::new(Arc::new(state)),
            disconnect_tx,
        );

        TestClient {
            client,
            disconnect: disconnect_rx,
            events: events_tx,
        }
    }

    #[test]
    fn client_must_be_clone() {
        let mut client = mock_client(&[], ConnStatus::Connected);

        let mut seq = Sequence::new();
        client
            .sender
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockSender::new);

        let _b = client.clone();
    }

    #[tokio::test]
    async fn client_recv() {
        let client = mock_client(&[], ConnStatus::Connected);

        let exp = DeviceEvent {
            interface: "interface".to_string(),
            path: "path".to_string(),
            data: Value::Individual {
                data: AstarteData::LongInteger(42),
                timestamp: Utc::now(),
            },
        };

        client.events.send(exp.clone()).await.unwrap();

        let event = client.recv().await.unwrap();

        assert_eq!(event, exp);
    }

    #[tokio::test]
    async fn client_disconnect_closed() {
        let mut client = mock_client(&[], ConnStatus::Disconnected);

        let mut seq = Sequence::new();
        client
            .sender
            .expect_disconnect()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Ok(()));

        client.disconnect().await.unwrap();

        client.disconnect.recv().await.unwrap();
    }
}
