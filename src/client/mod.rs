// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
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

//! Client to send data to astarte, add interfaces or access properties.

use std::future::Future;

use astarte_device_error::WrapError;
use astarte_interfaces::MappingPath;
use astarte_interfaces::interface::Retention;
use chrono::{DateTime, Utc};
use tracing::{debug, error, info, trace, warn};

use crate::aggregate::AstarteObject;
use crate::builder::ConnectionConfig;
use crate::error::{AstarteError, ErrorKind, InterfaceError};
use crate::event::DeviceEvent;
use crate::logging::security::{SecurityEvent, notify_security_event};
use crate::retention::memory::{ItemValue, VolatileItemError};
use crate::retention::{Id, RetentionId, StoredRetention};
use crate::state::{ClientState, ConnStatus};
use crate::store::StoreCapabilities;
use crate::transport::Encode;
use crate::types::AstarteData;
use crate::validate::Validated;

mod individual;
mod introspection;
mod object;
mod property;

/// A trait representing the behavior of an Astarte device client.
///
/// A device client is responsible for interacting with the Astarte platform by sending properties
/// and datastreams, handling events, and managing device interfaces.
pub trait Client: Send + Sync + Clone {
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
        &self,
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
        &self,
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
        &self,
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
    ///     #[astarte_object(fallible)]
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
        &self,
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
        &self,
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
        &self,
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

    /// Retrieve the expiry (not_after) timestamp of the current certificate
    fn get_cert_expiry(&self) -> impl Future<Output = Option<DateTime<Utc>>> + Send;

    /// Retrieve the expiry (not_after) timestamp of the current certificate
    /// Note that this function will log a security event if the feature is enabled
    /// when the certificate will expire at the passed datetime
    fn is_valid_at(&self, check_dt: DateTime<Utc>) -> impl Future<Output = Option<bool>> + Send {
        async move {
            let expiry = self.get_cert_expiry().await?;

            if check_dt < expiry {
                Some(true)
            } else {
                notify_security_event(SecurityEvent::CertificateAboutToExpire);

                Some(false)
            }
        }
    }

    /// Cleanly disconnects the client consuming it.
    fn disconnect(&self) -> impl Future<Output = Result<(), AstarteError>> + Send;

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
pub struct DeviceClient<C, S>
where
    C: ConnectionConfig,
{
    /// Sender of the connection.
    sender: tokio::sync::mpsc::Sender<Validated>,
    /// Astarte data events.
    ///
    /// We use multi producer multi consumer instead of the mpsc channel for the DeviceEvents for
    /// the connection to che client since we need the Receiver end to be cloneable. The tokio
    /// Broadcast channel provides an async mpmc, but suffer from the "slow receiver" problem.
    events: async_channel::Receiver<DeviceEvent>,
    pub(crate) state: ClientState<S>,
    /// Encoder for the client
    ///
    /// Useful for tests to not call static methods. It can be a ZST.
    pub(crate) encoder: C::Encoder,
}

impl<C, S> DeviceClient<C, S>
where
    C: ConnectionConfig,
{
    pub(crate) fn new(
        sender: tokio::sync::mpsc::Sender<Validated>,
        rx: async_channel::Receiver<DeviceEvent>,
        state: ClientState<S>,
        encoder: C::Encoder,
    ) -> Self {
        Self {
            sender,
            events: rx,
            state,
            encoder,
        }
    }

    /// Sends the data to the other task with a timeout
    pub(crate) async fn send_timeout(&self, value: Validated) -> Result<(), AstarteError> {
        self.sender
            .send_timeout(value, self.state.config().send_timeout)
            .await
            .wrap_err_msg(ErrorKind::Timeout, "while sending on channel")
    }

    async fn send<T>(&self, data: T) -> Result<(), AstarteError>
    where
        C: ConnectionConfig,
        C::Encoder: Encode,
        S: StoreCapabilities,
        T: ClientPacket,
    {
        match self.state.connection() {
            ConnStatus::Online => {
                trace!("publish while connection is connected");
            }
            ConnStatus::Offline | ConnStatus::Connected { .. } => {
                trace!("publish while connection is offline");

                return self.offline_send(data).await;
            }
            ConnStatus::Disconnect | ConnStatus::Closed => {
                trace!("publish while connection is closed");

                if let Err(error) = self.offline_send(data).await {
                    error!(%error, "couldn't store the send");
                }

                return Err(AstarteError::with(
                    ErrorKind::Disconnected,
                    "cannot send data",
                ));
            }
        }

        match data.get_retention() {
            Retention::Volatile { .. } => self.send_volatile(data).await,
            Retention::Stored { .. } => self.send_stored(data).await,
            Retention::Discard => {
                let data = data.validated(None);

                if let Err(error) = self.sender.try_send(data) {
                    warn!(%error, "message with retention discard dropped, queue full");
                } else {
                    trace!("message queued")
                }

                Ok(())
            }
        }
    }

    async fn offline_send<T>(&self, data: T) -> Result<(), AstarteError>
    where
        C: ConnectionConfig,
        C::Encoder: Encode,
        S: StoreCapabilities,
        T: ClientPacket,
    {
        match data.get_retention() {
            Retention::Discard => {
                debug!("drop publish with retention discard since disconnected");
            }
            Retention::Volatile { .. } => {
                let id = self.state.retention_ctx().next();

                self.state.volatile_store().push_unsent(id, data).await;
            }
            Retention::Stored { .. } => {
                let id = self.state.retention_ctx().next();

                if let Some(retention) = self.state.store().get_retention() {
                    data.store_publish(retention, &self.encoder, &id, false)
                        .await?;
                } else {
                    warn!(
                        "storing interface with retention 'Stored' in volatile store since the store doesn't support retention"
                    );

                    self.state.volatile_store().push_unsent(id, data).await;
                }
            }
        }

        Ok(())
    }

    async fn send_stored<T>(&self, data: T) -> Result<(), AstarteError>
    where
        C: ConnectionConfig,
        C::Encoder: Encode,
        S: StoreCapabilities,
        T: ClientPacket,
    {
        let Some(retention) = self.state.store().get_retention() else {
            warn!(
                "storing interface with retention 'Stored' in volatile store since the store doesn't support retention"
            );

            return self.send_volatile(data).await;
        };

        // generate id after the check to avoid wasting an id generation in case it gets regenerated in send_volatile
        let id = self.state.retention_ctx().next();

        data.store_publish(retention, &self.encoder, &id, false)
            .await?;

        self.send_timeout(data.validated(Some(RetentionId::Stored(id))))
            .await?;

        Ok(())
    }

    async fn send_volatile<T>(&self, data: T) -> Result<(), AstarteError>
    where
        C: ConnectionConfig,
        C::Encoder: Encode,
        S: StoreCapabilities,
        T: ClientPacket,
    {
        let id = self.state.retention_ctx().next();

        self.state
            .volatile_store()
            .push_sent(id, data.clone(), false)
            .await;

        self.send_timeout(data.validated(Some(RetentionId::Volatile(id))))
            .await?;

        Ok(())
    }
}

// Cannot be derived it has specific generic bounds.
impl<C, S> Clone for DeviceClient<C, S>
where
    C: ConnectionConfig,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            events: self.events.clone(),
            state: self.state.clone(),
            encoder: self.encoder.clone(),
        }
    }
}

impl<C, S> Client for DeviceClient<C, S>
where
    C: ConnectionConfig,
    S: StoreCapabilities,
    C::Encoder: Encode,
{
    async fn send_object_with_timestamp(
        &self,
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
        &self,
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
        &self,
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
        &self,
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
        &self,
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
        &self,
        interface_name: &str,
        mapping_path: &str,
    ) -> Result<(), AstarteError> {
        trace!("unsetting {}{}", interface_name, mapping_path);

        let path = MappingPath::try_from(mapping_path)
            .wrap_err(ErrorKind::Interface(InterfaceError::Path))?;

        self.send_unset(interface_name, &path).await
    }

    async fn recv(&self) -> Option<DeviceEvent> {
        match self.events.recv().await {
            Ok(event) => Some(event),
            Err(error) => {
                // Use the error message
                info!("{error}");

                None
            }
        }
    }

    /// Retrieve the expiry (not_after) timestamp of the current certificate
    async fn get_cert_expiry(&self) -> Option<DateTime<Utc>> {
        self.state.cert_expiry().await
    }

    async fn disconnect(&self) -> Result<(), AstarteError> {
        self.state.disconnect();

        info!("device disconnected");

        Ok(())
    }

    fn is_paired(&self) -> bool {
        self.state.is_device_paired()
    }
}

pub(crate) trait ClientPacket
where
    Self: TryInto<ItemValue, Error = VolatileItemError> + Clone,
{
    fn get_retention(&self) -> Retention;

    fn serialize<E>(&self, encodeer: &E) -> Result<Vec<u8>, AstarteError>
    where
        E: Encode;

    fn validated(self, retention: Option<RetentionId>) -> Validated;

    fn store_publish<S, E>(
        &self,
        retention: &S,
        encodeer: &E,
        id: &Id,
        sent: bool,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoredRetention,
        E: Encode;
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
    use crate::builder::DEFAULT_CHANNEL_SIZE;
    use crate::interfaces::Interfaces;
    use crate::state::tests::mock_state;
    use crate::store::StoreCapabilities;
    use crate::store::memory::MemoryStore;
    use crate::transport::mock::{MockConfig, MockEncoder};

    use super::*;

    pub(crate) struct TestClient<S>
    where
        S: StoreCapabilities,
    {
        client: DeviceClient<MockConfig, S>,
        pub(crate) client_rx: tokio::sync::mpsc::Receiver<Validated>,
        pub(crate) events: async_channel::Sender<DeviceEvent>,
        pub(crate) status: tokio::sync::watch::Receiver<ConnStatus>,
    }

    impl<S> Deref for TestClient<S>
    where
        S: StoreCapabilities,
    {
        type Target = DeviceClient<MockConfig, S>;

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

        let (client_tx, client_rx) = tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE.get());
        let (events_tx, events_rx) = async_channel::bounded(DEFAULT_CHANNEL_SIZE.get());
        let (status_tx, status_rx) = tokio::sync::watch::channel(initial_status);

        let state = mock_state(store, status_tx, interfaces);

        let client = DeviceClient::new(
            client_tx,
            events_rx,
            ClientState::new(Arc::new(state)),
            MockEncoder::new(),
        );

        TestClient {
            client,
            client_rx,
            events: events_tx,
            status: status_rx,
        }
    }

    #[test]
    fn client_must_be_clone() {
        let mut client = mock_client(&[], ConnStatus::Online);

        let mut seq = Sequence::new();
        client
            .encoder
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockEncoder::new);

        let _ = client.client.clone();
    }

    #[tokio::test]
    async fn client_recv() {
        let client = mock_client(&[], ConnStatus::Online);

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
        let mut client = mock_client(&[], ConnStatus::Offline);

        client.disconnect().await.unwrap();

        assert_eq!(*client.status.borrow_and_update(), ConnStatus::Disconnect);
    }
}
