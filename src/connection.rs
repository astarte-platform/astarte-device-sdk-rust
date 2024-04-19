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

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use log::{debug, error, info, warn};
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
};

use crate::{
    event::DeviceEvent,
    interface::{mapping::path::MappingPath, Aggregation as InterfaceAggregation, Ownership},
    interfaces::Interfaces,
    introspection::AddInterfaceError,
    store::{wrapper::StoreWrapper, PropertyStore, StoredProp},
    transport::{Disconnect, Publish, Receive, ReceivedEvent, Register},
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
    async fn handle_events(&mut self) -> Result<(), crate::Error>;
}

/// A trait representing the behavior of an Astarte device client to disconnect itself from Astarte.
#[async_trait]
pub trait ClientDisconnect {
    /// Cleanly disconnects the client consuming it.
    async fn disconnect(self);
}

/// Astarte device implementation.
pub struct DeviceConnection<S, C> {
    interfaces: Arc<RwLock<Interfaces>>,
    tx: flume::Sender<Result<DeviceEvent, Error>>,
    pub(crate) client: mpsc::Receiver<ClientMessage>,
    store: StoreWrapper<S>,
    connection: C,
}

impl<S, C> DeviceConnection<S, C> {
    pub(crate) fn new(
        interfaces: Arc<RwLock<Interfaces>>,
        tx: flume::Sender<Result<DeviceEvent, Error>>,
        client: mpsc::Receiver<ClientMessage>,
        store: StoreWrapper<S>,
        connection: C,
    ) -> Self {
        Self {
            interfaces,
            tx,
            client,
            store,
            connection,
        }
    }

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

                    info!(
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

                info!(
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

    /// Returns a boolean to check if the interface was added.
    async fn add_interface(&mut self, interface: Interface) -> Result<bool, Error>
    where
        C: Register,
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

        self.connection.add_interface(&interfaces, &to_add).await?;

        interfaces.add(to_add);

        Ok(true)
    }

    /// Returns a [`Vec`] with the name of the interfaces that have been added.
    async fn extend_interfaces<I>(&mut self, added: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = Interface> + Send,
        C: Register,
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

        self.connection
            .extend_interfaces(&interfaces, &to_add)
            .await?;

        let names = to_add.keys().cloned().collect_vec();

        interfaces.extend(to_add);

        debug!("Interfaces added");

        Ok(names)
    }

    /// Returns a bool to check if the interface was added.
    async fn remove_interface(&mut self, interface_name: &str) -> Result<bool, Error>
    where
        C: Register,
        S: PropertyStore,
    {
        let mut interfaces = self.interfaces.write().await;

        let to_remove = interfaces.get(interface_name);

        // only in debug mode, if the interface is not found, it has been probably misspelled
        debug_assert!(to_remove.is_some(), "interface {interface_name} not found");
        let to_remove = match to_remove {
            Some(i) => i,
            None => {
                warn!("{interface_name} not found, skipping");
                return Ok(false);
            }
        };

        self.connection
            .remove_interface(&interfaces, to_remove)
            .await?;

        if let Some(prop) = to_remove.as_prop() {
            // We cannot error here since we already unsubscribed to the interface
            if let Err(err) = self.store.delete_interface(prop.interface_name()).await {
                error!("failed to remove property {err}");
            }
        }

        interfaces.remove(interface_name);

        Ok(true)
    }

    async fn remove_interfaces<'a, I>(&mut self, interfaces_name: I) -> Result<Vec<String>, Error>
    where
        C: Register,
        S: PropertyStore,
        I: IntoIterator<Item = &'a str> + Clone + Send,
    {
        let mut interfaces = self.interfaces.write().await;

        let to_remove = interfaces_name
            .clone()
            .into_iter()
            .filter_map(|iface_name| {
                let interface = interfaces.get(iface_name).map(|i| (i.interface_name(), i));

                // only in debug mode, if the interface is not found, it has been probably misspelled
                debug_assert!(interface.is_some(), "interface {iface_name} not found");
                if interface.is_none() {
                    warn!("{iface_name} not found, skipping");
                }

                interface
            })
            .collect();

        self.connection
            .remove_interfaces(&interfaces, &to_remove)
            .await?;

        for (iface_name, _) in to_remove.iter() {
            // We cannot error here since we already unsubscribed to the interface
            if let Err(err) = self.store.delete_interface(iface_name).await {
                error!("failed to remove property {err}");
            }
        }

        let removed_names = to_remove.keys().map(|k| k.to_string()).collect_vec();

        interfaces.remove_many(&HashSet::from_iter(interfaces_name));

        Ok(removed_names)
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

    pub(crate) async fn handle_client_msg(&mut self, msg: ClientMessage) -> Result<(), Error>
    where
        C: Publish + Register,
        S: PropertyStore,
    {
        match msg {
            ClientMessage::Individual(data) => self.connection.send_individual(data).await,
            ClientMessage::Property {
                data,
                version_major,
            } => {
                self.connection.send_individual(data.clone()).await?;

                let prop = StoredProp {
                    interface: data.interface.as_str(),
                    path: data.path.as_str(),
                    value: &data.data,
                    interface_major: version_major,
                    ownership: Ownership::Device,
                };

                self.store.store_prop(prop).await?;

                info!(
                    "property stored {}{}:{version_major}",
                    data.interface, data.path,
                );

                Ok(())
            }
            ClientMessage::Object(data) => self.connection.send_object(data).await,
            ClientMessage::Unset(data) => {
                self.connection.unset(data.clone()).await?;

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
                    error!("client disconnected while failing to add interface: {err}");
                }

                Ok(())
            }
            ClientMessage::ExtendInterfaces {
                interfaces,
                response,
            } => {
                let res = self.extend_interfaces(interfaces).await;

                if let Err(Err(err)) = response.send(res) {
                    error!("client disconnected while failing to extend interfaces: {err}");
                }

                Ok(())
            }
            ClientMessage::RemoveInterface {
                interface,
                response,
            } => {
                let res = self.remove_interface(&interface).await;

                if let Err(Err(err)) = response.send(res) {
                    error!("client disconnected while failing to remove interfaces: {err}");
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
                    error!("client disconnected while failing to remove interfaces: {err}");
                }

                Ok(())
            }
        }
    }
}

#[async_trait]
impl<S, C> EventLoop for DeviceConnection<S, C>
where
    C: Receive + Register + Publish + Sync + Send,
    S: PropertyStore,
{
    async fn handle_events(&mut self) -> Result<(), crate::Error> {
        loop {
            // Future to not hold multiple mutable reference to self, but only to connection and client
            let fut = async {
                let interfaces = self.interfaces.read().await;
                self.connection.next_event(&interfaces, &self.store).await
            };

            select! {
                event = fut => {
                    let event = event?;

                    self.handle_connection_event(event).await?;
                    // recreate the future
                    continue;
                }
                event = self.client.recv() => {
                    let msg = event.ok_or(Error::Disconnected)?;

                    self.handle_client_msg(msg).await?;
                }
            }
        }
    }
}

#[async_trait]
impl<S, C> ClientDisconnect for DeviceConnection<S, C>
where
    S: Send,
    C: Disconnect + Send,
{
    async fn disconnect(self) {
        if let Err(e) = self.connection.disconnect().await {
            error!("Could not close the connection gracefully: {}", e);
        }
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
