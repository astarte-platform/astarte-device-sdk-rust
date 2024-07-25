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

//! Connection to Astarte, for handling events and reconnection on error.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{collections::HashMap, sync::atomic::AtomicBool};

use async_trait::async_trait;
use futures::future::Either;
use itertools::Itertools;
use tokio::sync::Notify;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task::JoinSet,
};
use tracing::{debug, error, info, trace, warn};

use crate::builder::DEFAULT_CHANNEL_SIZE;
use crate::interface::Retention;
use crate::retention::memory::ItemValue;
use crate::retention::{StoredRetention, StoredRetentionExt};
use crate::{
    error::Report,
    event::DeviceEvent,
    interface::{mapping::path::MappingPath, Aggregation as InterfaceAggregation, Ownership},
    interfaces::Interfaces,
    introspection::AddInterfaceError,
    retention::memory::VolatileRetention,
    store::{wrapper::StoreWrapper, PropertyStore, StoreCapabilities, StoredProp},
    transport::{Connection, Disconnect, Publish, Receive, ReceivedEvent, Reconnect, Register},
    validate::{ValidatedIndividual, ValidatedObject, ValidatedUnset},
    Error, Interface, Value,
};

/// Handles the messages from the device and astarte.
#[async_trait]
pub trait EventLoop {
    /// Poll updates from the connection implementation, can be placed in a loop to receive data.
    ///
    /// This is a blocking function. It should be placed on a dedicated thread/task or as the main
    /// thread.
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
    ///     let (client, mut connection) = DeviceBuilder::new()
    ///         .store(MemoryStore::new())
    ///         .connect(mqtt_config).await.unwrap()
    ///         .build();
    ///
    ///     tokio::spawn(async move {
    ///         loop {
    ///             let event = client.recv().await;
    ///             assert!(event.is_ok());
    ///         }
    ///     });
    ///
    ///     connection.handle_events().await;
    /// }
    /// ```
    async fn handle_events(self) -> Result<(), crate::Error>;
}

/// A trait representing the behavior of an Astarte device client to disconnect itself from Astarte.
#[async_trait]
pub trait ClientDisconnect {
    /// Cleanly disconnects the client consuming it.
    async fn disconnect(self);
}

/// Astarte device implementation.
pub struct DeviceConnection<S, C>
where
    C: Connection,
{
    pub(crate) sender: DeviceSender<S, C::Sender>,
    pub(crate) receiver: DeviceReceiver<S, C>,
}

impl<S, C> DeviceConnection<S, C>
where
    C: Connection,
{
    pub(crate) fn new(
        interfaces: Arc<RwLock<Interfaces>>,
        tx: flume::Sender<Result<DeviceEvent, Error>>,
        client: mpsc::Receiver<ClientMessage>,
        volatile_store: VolatileRetention,
        store: StoreWrapper<S>,
        connection: C,
        sender: C::Sender,
    ) -> Self
    where
        S: Clone,
    {
        let status = Arc::new(ConnectionStatus::new());

        Self {
            sender: DeviceSender {
                interfaces: Arc::clone(&interfaces),
                client,
                store: store.clone(),
                sender,
                volatile_store,
                status: Arc::clone(&status),
            },
            receiver: DeviceReceiver {
                interfaces,
                tx,
                store,
                connection,
                status,
            },
        }
    }
}

#[async_trait]
impl<S, C> EventLoop for DeviceConnection<S, C>
where
    C: Connection + Reconnect + Receive + Send + Sync + 'static,
    C::Sender: Send + Register + Publish + 'static,
    S: PropertyStore + StoreCapabilities,
{
    async fn handle_events(mut self) -> Result<(), crate::Error> {
        let Self {
            mut sender,
            mut receiver,
        } = self;

        if let Some(retention) = sender.store.get_retention() {
            let interfaces = sender.interfaces.read().await;

            retention.cleanup_introspection(&interfaces).await?;
        }

        let mut tasks: JoinSet<Result<(), Error>> = JoinSet::new();

        tasks.spawn(async move {
            sender.init_stored_retention().await?;

            // TODO: consider adding a cancellation token.
            loop {
                let either = sender.poll_next().await.ok_or(Error::Disconnected)?;

                match either {
                    Either::Left(msg) => {
                        sender.handle_client_msg(msg).await?;
                    }
                    Either::Right(()) => {
                        sender.resend_volatile_publishes().await?;

                        sender.resend_stored_publishes().await?;
                    }
                }
            }
        });

        tasks.spawn(async move {
            // The event is null, reconnect the device
            loop {
                let event = receiver.connection.next_event().await?;

                let Some(event) = event else {
                    debug!("reconnecting");

                    receiver.status.set_connected(false);

                    let interfaces = receiver.interfaces.read().await;

                    receiver.connection.reconnect(&interfaces).await?;

                    receiver.status.set_connected(true);

                    continue;
                };

                receiver.handle_connection_event(event).await?;
            }
        });

        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    error!(error = %Report::new(err), "task errored")
                }
                Err(err) if err.is_cancelled() => {
                    debug!("task cancelled");
                }
                Err(err) => {
                    error!(error = %Report::new(err), "failed to join task");
                }
            }
        }

        Err(Error::Disconnected)
    }
}

#[async_trait]
impl<S, C> ClientDisconnect for DeviceConnection<S, C>
where
    S: StoreCapabilities + Send,
    C: Connection + Disconnect + Send,
    C::Sender: Send,
{
    async fn disconnect(self) {
        if let Err(e) = self.receiver.connection.disconnect().await {
            error!(error = %Report::new(e), "Could not close the connection gracefully");
        }
    }
}

pub(crate) struct DeviceSender<S, T> {
    interfaces: Arc<RwLock<Interfaces>>,
    pub(crate) client: mpsc::Receiver<ClientMessage>,
    store: StoreWrapper<S>,
    sender: T,
    status: Arc<ConnectionStatus>,
    volatile_store: VolatileRetention,
}

impl<S, T> DeviceSender<S, T>
where
    S: StoreCapabilities,
{
    async fn poll_next(&mut self) -> Option<Either<ClientMessage, ()>> {
        let msg_fut = std::pin::pin!(self.client.recv());
        let connected_fut = std::pin::pin!(self.status.wait_reconnection());

        // drop the references to sender
        let either = match futures::future::select(msg_fut, connected_fut).await {
            Either::Left((msg, _)) => {
                let msg = msg?;

                Either::Left(msg)
            }
            Either::Right(((), _)) => Either::Right(()),
        };

        Some(either)
    }

    pub(crate) async fn handle_client_msg(&mut self, msg: ClientMessage) -> Result<(), Error>
    where
        T: Publish + Register,
        S: PropertyStore,
    {
        match msg {
            ClientMessage::Individual(data) => self.send_individual(data).await,
            ClientMessage::Property {
                data,
                version_major,
            } => {
                self.sender.send_individual(data.clone()).await?;

                let prop = StoredProp {
                    interface: data.interface.as_str(),
                    path: data.path.as_str(),
                    value: &data.data,
                    interface_major: version_major,
                    ownership: Ownership::Device,
                };

                self.store.store_prop(prop).await?;

                debug!(
                    "property stored {}{}:{version_major}",
                    data.interface, data.path,
                );

                Ok(())
            }
            ClientMessage::Object(data) => self.send_object(data).await,
            ClientMessage::Unset(data) => {
                self.sender.unset(data.clone()).await?;

                debug!(
                    "deleting property {}{} from store",
                    data.interface, data.path
                );

                self.store.delete_prop(&data.interface, &data.path).await?;

                Ok(())
            }
            ClientMessage::AddInterface {
                interface,
                response,
            } => {
                let res = self.add_interface(interface).await;

                if let Err(Err(err)) = response.send(res) {
                    error!(error = %Report::new(err), "client disconnected while failing to add interface");
                }

                Ok(())
            }
            ClientMessage::ExtendInterfaces {
                interfaces,
                response,
            } => {
                let res = self.extend_interfaces(interfaces).await;

                if let Err(Err(err)) = response.send(res) {
                    error!(error = %Report::new(err),"client disconnected while failing to extend interfaces");
                }

                Ok(())
            }
            ClientMessage::RemoveInterface {
                interface,
                response,
            } => {
                let res = self.remove_interface(&interface).await;

                if let Err(Err(err)) = response.send(res) {
                    error!(error = %Report::new(err), "client disconnected while failing to remove interfaces");
                }

                Ok(())
            }
            ClientMessage::RemoveInterfaces {
                interfaces,
                response,
            } => {
                let res = self
                    .remove_interfaces(interfaces.iter().map(|s| s.as_str()))
                    .await;

                if let Err(Err(err)) = response.send(res) {
                    error!(error = %Report::new(err), "client disconnected while failing to remove interfaces");
                }

                Ok(())
            }
        }
    }

    async fn send_individual(&mut self, data: ValidatedIndividual) -> Result<(), Error>
    where
        T: Publish,
    {
        if !self.status.is_connected() {
            trace!("publish individual while connection is offline");

            return self.offline_send_individual(data).await;
        }

        if data.retention.is_stored() {
            if let Some(retention) = self.store.get_retention() {
                let value = self.sender.serialize_individual(&data)?;

                let id = retention
                    .store_sent_publish_individual(&data, &value)
                    .await?;

                return self.sender.send_individual_stored(id, data).await;
            }

            warn!("not storing interface with retention stored sinc the store doesn't support retention");
        }

        self.sender.send_individual(data).await
    }

    async fn send_object(&mut self, data: ValidatedObject) -> Result<(), Error>
    where
        T: Publish,
    {
        if !self.status.is_connected() {
            trace!("publish individual while connection is offline");

            return self.offline_send_object(data).await;
        }

        if data.retention.is_stored() {
            if let Some(retention) = self.store.get_retention() {
                let value = self.sender.serialize_object(&data)?;

                let id = retention.store_sent_publish_obj(&data, &value).await?;

                return self.sender.send_object_stored(id, data).await;
            }

            warn!("not storing interface with retention stored sinc the store doesn't support retention");
        }

        self.sender.send_object(data).await
    }

    async fn offline_send_individual(&mut self, data: ValidatedIndividual) -> Result<(), Error>
    where
        T: Publish,
    {
        match data.retention {
            Retention::Discard => {
                debug!("drop publish with retention discard since disconnected");
            }
            Retention::Volatile { .. } => {
                self.volatile_store.push(data);
            }
            Retention::Stored { .. } => {
                if let Some(retention) = self.store.get_retention() {
                    let value = self.sender.serialize_individual(&data)?;

                    let _ = retention
                        .store_sent_publish_individual(&data, &value)
                        .await?;
                } else {
                    warn!("storing interface with retention stored in volatile since the store doesn't support retention");

                    self.volatile_store.push(data);
                }
            }
        }

        Ok(())
    }

    async fn offline_send_object(&mut self, data: ValidatedObject) -> Result<(), Error>
    where
        T: Publish,
    {
        match data.retention {
            Retention::Discard => {
                debug!("drop publish with retention discard since disconnected");
            }
            Retention::Volatile { .. } => {
                self.volatile_store.push(data);
            }
            Retention::Stored { .. } => {
                if let Some(retention) = self.store.get_retention() {
                    let value = self.sender.serialize_object(&data)?;

                    let _ = retention.store_sent_publish_obj(&data, &value).await?;
                } else {
                    warn!("storing interface with retention stored in volatile since the store doesn't support retention");

                    self.volatile_store.push(data);
                }
            }
        }

        Ok(())
    }

    /// Returns a boolean to check if the interface was added.
    async fn add_interface(&mut self, interface: Interface) -> Result<bool, Error>
    where
        T: Register,
    {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.interfaces.write().await;

        let map_err = interfaces
            .validate(interface)
            .map_err(AddInterfaceError::Interface)?;

        let Some(to_add) = map_err else {
            debug!("interfaces already present");

            return Ok(false);
        };

        self.sender.add_interface(&interfaces, &to_add).await?;

        interfaces.add(to_add);

        Ok(true)
    }

    /// Returns a [`Vec`] with the name of the interfaces that have been added.
    async fn extend_interfaces<I>(&mut self, added: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = Interface> + Send,
        T: Register,
    {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.interfaces.write().await;

        let to_add = interfaces
            .validate_many(added)
            .map_err(AddInterfaceError::Interface)?;

        if to_add.is_empty() {
            debug!("All interfaces already present");
            return Ok(Vec::new());
        }

        debug!("Adding {} interfaces", to_add.len());

        self.sender.extend_interfaces(&interfaces, &to_add).await?;

        let names = to_add.keys().cloned().collect_vec();

        interfaces.extend(to_add);

        debug!("Interfaces added");

        Ok(names)
    }

    /// Returns a bool to check if the interface was added.
    async fn remove_interface(&mut self, interface_name: &str) -> Result<bool, Error>
    where
        T: Register,
        S: PropertyStore,
    {
        let mut interfaces = self.interfaces.write().await;

        let Some(to_remove) = interfaces.get(interface_name) else {
            debug!("{interface_name} not found, skipping");
            return Ok(false);
        };

        self.sender.remove_interface(&interfaces, to_remove).await?;

        if let Some(prop) = to_remove.as_prop() {
            // We cannot error here since we have already unsubscribed from the interface
            if let Err(err) = self.store.delete_interface(prop.interface_name()).await {
                error!(error = %Report::new(err),"failed to remove property");
            }
        }

        interfaces.remove(interface_name);

        Ok(true)
    }

    async fn remove_interfaces<'a, I>(&mut self, interfaces_name: I) -> Result<Vec<String>, Error>
    where
        T: Register,
        S: PropertyStore,
        I: IntoIterator<Item = &'a str> + Send,
    {
        let mut interfaces = self.interfaces.write().await;

        let to_remove: HashMap<&str, &Interface> = interfaces_name
            .into_iter()
            .filter_map(|iface_name| {
                let interface = interfaces.get(iface_name).map(|i| (i.interface_name(), i));

                if interface.is_none() {
                    debug!("{iface_name} not found, skipping");
                }

                interface
            })
            .collect();

        if to_remove.is_empty() {
            return Ok(Vec::new());
        }

        self.sender
            .remove_interfaces(&interfaces, &to_remove)
            .await?;

        for (_, iface) in to_remove.iter() {
            // We cannot error here since we have already unsubscribed from the interface
            if let Some(prop) = iface.as_prop() {
                if let Err(err) = self.store.delete_interface(prop.interface_name()).await {
                    error!(error = %Report::new(err), "failed to remove property");
                }
            }
        }

        let removed_names = to_remove.keys().map(|k| k.to_string()).collect_vec();

        interfaces.remove_many(&removed_names);

        Ok(removed_names)
    }

    async fn resend_volatile_publishes(&mut self) -> Result<(), Error>
    where
        T: Publish,
    {
        while let Some(item) = self.volatile_store.pop_next() {
            match item {
                ItemValue::Individual(individual) => {
                    self.sender.send_individual(individual).await?;
                }
                ItemValue::Object(object) => {
                    self.sender.send_object(object).await?;
                }
            }

            // Let's check if we are still connected after the await
            if self.status.is_connected() {
                break;
            }
        }

        Ok(())
    }

    async fn resend_stored_publishes(&mut self) -> Result<(), Error>
    where
        T: Publish,
    {
        let Some(retention) = self.store.get_retention() else {
            return Ok(());
        };

        let mut buf = Vec::new();

        debug!("start sending store publishes");
        loop {
            let count = retention
                .unsent_publishes(DEFAULT_CHANNEL_SIZE, &mut buf)
                .await?;

            trace!("loaded {count} stored publishes");

            for (id, info) in buf.drain(..) {
                self.sender.resend_stored(id, info).await?;
            }

            if count == 0 || count < DEFAULT_CHANNEL_SIZE {
                info!("all stored publishes sent");

                break;
            }

            buf.clear();
        }

        Ok(())
    }

    /// This function is called once at the start to send all the stored packet.
    async fn init_stored_retention(&mut self) -> Result<(), Error>
    where
        T: Publish,
    {
        let Some(retention) = self.store.get_retention() else {
            return Ok(());
        };

        retention.reset_all_publishes().await?;

        self.resend_stored_publishes().await?;

        Ok(())
    }
}

pub(crate) struct DeviceReceiver<S, C> {
    interfaces: Arc<RwLock<Interfaces>>,
    tx: flume::Sender<Result<DeviceEvent, Error>>,
    store: StoreWrapper<S>,
    status: Arc<ConnectionStatus>,
    connection: C,
}

impl<S, C> DeviceReceiver<S, C> {
    async fn handle_event(
        &self,
        interface: &str,
        path: &str,
        payload: C::Payload,
    ) -> Result<Value, crate::Error>
    where
        S: PropertyStore,
        C: Receive + Sync,
    {
        let path = MappingPath::try_from(path)?;

        let interfaces = self.interfaces.read().await;
        let interface = interfaces.get(interface).ok_or_else(|| {
            warn!("publish on missing interface {interface} ({path})");
            Error::InterfaceNotFound {
                name: interface.to_string(),
            }
        })?;

        let (data, timestamp) = match interface.aggregation() {
            InterfaceAggregation::Individual => {
                self.handle_payload_individual(interface, &path, payload)
                    .await?
            }
            InterfaceAggregation::Object => {
                self.handle_payload_object(interface, &path, payload)
                    .await?
            }
        };

        debug!("received {{v: {data:?}, t: {timestamp:?}}}");

        Ok(data)
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Individual`]
    async fn handle_payload_individual<'a>(
        &self,
        interface: &Interface,
        path: &MappingPath<'a>,
        payload: C::Payload,
    ) -> Result<(Value, Option<chrono::DateTime<chrono::Utc>>), Error>
    where
        S: PropertyStore,
        C: Receive + Sync,
    {
        let mapping = interface
            .as_mapping_ref(path)
            .ok_or_else(|| Error::MappingNotFound {
                interface: interface.interface_name().to_string(),
                mapping: path.to_string(),
            })?;

        let individual = self.connection.deserialize_individual(&mapping, payload)?;

        match individual {
            Some((value, timestamp)) => {
                if let Some(prop) = mapping.as_prop() {
                    let prop = StoredProp::from_mapping(&prop, &value);

                    self.store.store_prop(prop).await?;

                    debug!(
                        "property stored {}{path}:{}",
                        interface.interface_name(),
                        interface.version_major()
                    );
                }

                Ok((Value::Individual(value), timestamp))
            }
            None => {
                // Unset can only be received for a property
                self.store
                    .delete_prop(interface.interface_name(), path.as_str())
                    .await?;

                debug!(
                    "property unset {}{path}:{}",
                    interface.interface_name(),
                    interface.version_major()
                );

                Ok((Value::Unset, None))
            }
        }
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Object`]
    async fn handle_payload_object<'a>(
        &self,
        interface: &Interface,
        path: &MappingPath<'a>,
        payload: C::Payload,
    ) -> Result<(Value, Option<chrono::DateTime<chrono::Utc>>), Error>
    where
        S: PropertyStore,
        C: Receive + Sync,
    {
        let object = interface.as_object_ref().ok_or(Error::Aggregation {
            exp: InterfaceAggregation::Object,
            got: InterfaceAggregation::Individual,
        })?;

        let (data, timestamp) = self.connection.deserialize_object(&object, path, payload)?;

        Ok((Value::Object(data), timestamp))
    }

    async fn handle_connection_event(&self, event: ReceivedEvent<C::Payload>) -> Result<(), Error>
    where
        C: Receive + Sync,
        S: PropertyStore,
    {
        let data = self
            .handle_event(&event.interface, &event.path, event.payload)
            .await
            .map(|aggregation| DeviceEvent {
                interface: event.interface,
                path: event.path,
                data: aggregation,
            });

        self.tx
            .send_async(data)
            .await
            .map_err(|_| Error::Disconnected)
    }
}

/// Message set from the [`DeviceClient`](crate::DeviceClient) to the [`DeviceConnection`].
#[derive(Debug)]
pub(crate) enum ClientMessage {
    Individual(ValidatedIndividual),
    Property {
        data: ValidatedIndividual,
        version_major: i32,
    },
    Object(ValidatedObject),
    Unset(ValidatedUnset),
    AddInterface {
        interface: Interface,
        response: oneshot::Sender<Result<bool, Error>>,
    },
    ExtendInterfaces {
        interfaces: Vec<Interface>,
        response: oneshot::Sender<Result<Vec<String>, Error>>,
    },
    RemoveInterface {
        interface: String,
        response: oneshot::Sender<Result<bool, Error>>,
    },
    RemoveInterfaces {
        interfaces: Vec<String>,
        response: oneshot::Sender<Result<Vec<String>, Error>>,
    },
}

#[derive(Debug)]
struct ConnectionStatus {
    connected: AtomicBool,
    reconnected: Notify,
}

impl ConnectionStatus {
    fn new() -> Self {
        // Assumes we are connected
        Self {
            connected: AtomicBool::new(true),
            reconnected: Notify::new(),
        }
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Release);

        if connected {
            self.reconnected.notify_waiters();
        }
    }

    async fn wait_reconnection(&self) {
        self.reconnected.notified().await;
    }
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        Self::new()
    }
}
