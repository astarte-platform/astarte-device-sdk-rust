/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
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
#![doc = include_str!("../README.md")]

pub mod crypto;
pub mod database;
pub mod error;
pub mod interface;
mod interfaces;
#[cfg(test)]
mod mock;
pub mod options;
pub mod pairing;
pub mod payload;
pub mod properties;
pub mod registration;
mod topic;
pub mod types;

#[cfg(test)]
use mock::{MockAsyncClient as AsyncClient, MockEventLoop as EventLoop};
#[cfg(not(test))]
use rumqttc::{AsyncClient, EventLoop};

use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{self, Debug};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

// Re-export rumqttc since we return its types in some methods
pub use chrono;
pub use rumqttc;

use log::{debug, error, info, trace, warn};
use rumqttc::Event;

/// Re-exported internal structs
pub use crate::interface::Interface;

use crate::database::AstarteDatabase;
use crate::database::StoredProp;
use crate::error::Error;
use crate::interface::mapping::path::MappingPath;
use crate::interface::{InterfaceError, Ownership};
use crate::interfaces::PropertyRef;
use crate::options::AstarteOptions;
use crate::topic::parse_topic;
use crate::types::{AstarteType, TypeError};

/// A **trait** required by all data to be sent using
/// [send_object()][crate::AstarteDeviceSdk::send_object] and
/// [send_object_with_timestamp()][crate::AstarteDeviceSdk::send_object_with_timestamp].
/// It ensures correct parsing of the data.
///
/// The returned hash map should have as values the data to transmit for each
/// object endpoint and as keys the endpoints themselves.
///
/// The Astarte Device SDK provides a procedural macro that can be used to automatically
/// generate `AstarteAggregate` implementations for Structs.
/// To use the procedural macro enable the feature `derive`.
pub trait AstarteAggregate {
    /// Parse this data structure into a `HashMap` compatible with transmission of Astarte objects.
    /// ```
    /// use std::collections::HashMap;
    /// use std::convert::TryInto;
    ///
    /// use astarte_device_sdk::{types::AstarteType, error::Error, AstarteAggregate};
    ///
    /// struct Person {
    ///     name: String,
    ///     age: i32,
    ///     phones: Vec<String>,
    /// }
    ///
    /// // This is what #[derive(AstarteAggregate)] would generate.
    /// impl AstarteAggregate for Person {
    ///     fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error>
    ///     {
    ///         let mut r = HashMap::new();
    ///         r.insert("name".to_string(), self.name.try_into()?);
    ///         r.insert("age".to_string(), self.age.try_into()?);
    ///         r.insert("phones".to_string(), self.phones.try_into()?);
    ///         Ok(r)
    ///     }
    /// }
    /// ```
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error>;
}

impl AstarteAggregate for HashMap<String, AstarteType> {
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error> {
        Ok(self)
    }
}

// Re-export #[derive(AstarteAggregate)].
//
// The reason re-exporting is not enabled by default is that disabling it would
// be annoying for crates that provide handwritten impls or data formats. They
// would need to disable default features and then explicitly re-enable std.
#[cfg(feature = "derive")]
#[allow(unused_imports)]
#[macro_use]
extern crate astarte_device_sdk_derive;

/// Derive macro to implement `AstarteAggregate` trait with `feature = ["derive"]`.
#[cfg(feature = "derive")]
pub use astarte_device_sdk_derive::AstarteAggregate;

/// Astarte device implementation.
///
/// Provides functionality to transmit and receive individual and object datastreams as well
/// as properties.
#[derive(Clone)]
pub struct AstarteDeviceSdk {
    realm: String,
    device_id: String,
    client: AsyncClient,
    eventloop: Arc<tokio::sync::Mutex<EventLoop>>,
    interfaces: Arc<tokio::sync::RwLock<interfaces::Interfaces>>,
    database: Option<Arc<dyn AstarteDatabase + Sync + Send>>,
}

/// Payload format for an Astarte device event data.
#[derive(Debug, Clone, PartialEq)]
pub enum Aggregation {
    /// Individual data, can be both from a datastream or property.
    Individual(AstarteType),
    /// Object data, also called aggregate. Can only be from a datastream.
    Object(HashMap<String, AstarteType>),
}

/// Astarte device event data structure.
///
/// Data structure returned when an instance of [`AstarteDeviceSdk`] polls a valid event.
#[derive(Debug, Clone)]
pub struct AstarteDeviceDataEvent {
    /// Interface on which the event has been triggered
    pub interface: String,
    /// Path to the endpoint for which the event has been triggered
    pub path: String,
    /// Payload of the event
    pub data: Aggregation,
}

impl AstarteDeviceSdk {
    /// Create a new instance of the Astarte Device SDK.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{AstarteDeviceSdk, options::AstarteOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let sdk_options = AstarteOptions::new("", "", "", "")
    ///     .interface_directory("")
    ///     .unwrap()
    ///     .ignore_ssl_errors();
    ///
    ///     let mut device = AstarteDeviceSdk::new(sdk_options).await.unwrap();
    /// }
    /// ```
    pub async fn new(opts: AstarteOptions) -> Result<AstarteDeviceSdk, Error> {
        let mqtt_options = pairing::get_transport_config(&opts).await?;

        debug!("{:#?}", mqtt_options);

        let (client, eventloop) = AsyncClient::new(mqtt_options, 50);

        let mut device = AstarteDeviceSdk {
            realm: opts.realm,
            device_id: opts.device_id,
            client,
            eventloop: Arc::new(tokio::sync::Mutex::new(eventloop)),
            interfaces: Arc::new(tokio::sync::RwLock::new(opts.interfaces)),
            database: opts.database,
        };

        device.wait_for_connack().await?;

        Ok(device)
    }

    async fn wait_for_connack(&mut self) -> Result<(), Error> {
        loop {
            // keep consuming and processing packets until we have data for the user
            match self.eventloop.lock().await.poll().await? {
                Event::Incoming(i) => {
                    trace!("MQTT Incoming = {i:?}");

                    if let rumqttc::Packet::ConnAck(p) = i {
                        return self.connack(p).await;
                    } else {
                        error!("BUG: not connack inside poll_connack {i:?}");
                    }
                }
                Event::Outgoing(i) => {
                    error!("BUG: not connack inside poll_connack {i:?}");
                }
            }
        }
    }

    async fn connack(&self, p: rumqttc::ConnAck) -> Result<(), Error> {
        if !p.session_present {
            self.subscribe().await?;
            self.send_introspection().await?;
            self.send_emptycache().await?;
            self.send_device_owned_properties().await?;
            info!("connack done");
        }

        Ok(())
    }

    async fn subscribe(&self) -> Result<(), Error> {
        let ifaces = &self.interfaces.read().await;
        let server_owned_ifaces = ifaces
            .iter_interfaces()
            .filter(|interface| interface.ownership() == interface::Ownership::Server);

        self.client
            .subscribe(
                self.client_id() + "/control/consumer/properties",
                rumqttc::QoS::ExactlyOnce,
            )
            .await?;

        for iface in server_owned_ifaces {
            self.subscribe_server_owned_interface(iface).await?;
        }

        Ok(())
    }

    async fn subscribe_server_owned_interface(&self, iface: &Interface) -> Result<(), Error> {
        if iface.ownership() != interface::Ownership::Server {
            warn!("Unable to subscribe to {} as it is not server owned", iface);
        } else {
            self.client
                .subscribe(
                    self.client_id() + "/" + iface.interface_name() + "/#",
                    rumqttc::QoS::ExactlyOnce,
                )
                .await?;
        }
        Ok(())
    }

    async fn unsubscribe_server_owned_interface(&self, iface: &Interface) -> Result<(), Error> {
        if iface.ownership() != interface::Ownership::Server {
            warn!(
                "Unable to unsubscribe to {} as it is not server owned",
                iface
            );
        } else {
            self.client
                .unsubscribe(self.client_id() + "/" + iface.interface_name() + "/#")
                .await?;
        }
        Ok(())
    }

    /// Add a new interface from the provided file.
    pub async fn add_interface_from_file(&self, file_path: &str) -> Result<(), Error> {
        let path = Path::new(file_path);
        let interface = Interface::from_file(path)?;
        self.add_interface(interface).await
    }

    /// Add a new interface from a string. The string should contain a valid json formatted
    /// interface.
    pub async fn add_interface_from_str(&self, json_str: &str) -> Result<(), Error> {
        let interface: Interface = Interface::from_str(json_str)?;
        self.add_interface(interface).await
    }

    /// Add a new [`Interface`] to the device interfaces.
    pub async fn add_interface(&self, interface: Interface) -> Result<(), Error> {
        if interface.ownership() == interface::Ownership::Server {
            self.subscribe_server_owned_interface(&interface).await?;
        }
        self.add_interface_to_introspection(interface).await?;
        self.send_introspection().await?;
        Ok(())
    }

    async fn add_interface_to_introspection(&self, interface: Interface) -> Result<(), Error> {
        self.interfaces.write().await.add(interface)?;

        Ok(())
    }

    /// Remove the interface with the name specified as argument.
    pub async fn remove_interface(&self, interface_name: &str) -> Result<(), Error> {
        let interface = self.remove_interface_from_map(interface_name).await?;
        self.remove_properties_from_store(interface_name).await?;
        self.send_introspection().await?;
        if interface.ownership() == interface::Ownership::Server {
            self.unsubscribe_server_owned_interface(&interface).await?;
        }
        Ok(())
    }

    async fn remove_interface_from_map(&self, interface_name: &str) -> Result<Interface, Error> {
        self.interfaces
            .write()
            .await
            .remove(interface_name)
            .ok_or_else(|| {
                Error::Interface(InterfaceError::InterfaceNotFound {
                    name: interface_name.to_string(),
                })
            })
    }

    /// Poll updates from mqtt, can be placed in a loop to receive data.
    ///
    /// This is a blocking function. It should be placed on a dedicated thread/task.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{AstarteDeviceSdk, options::AstarteOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut sdk_options = AstarteOptions::new("_","_","_","_");
    ///     let mut device = AstarteDeviceSdk::new(sdk_options).await.unwrap();
    ///
    ///     loop {
    ///         match device.handle_events().await {
    ///             Ok(data) => {
    ///                 // React to received data
    ///             }
    ///             Err(err) => {
    ///                 // React to reception error
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn handle_events(&mut self) -> Result<AstarteDeviceDataEvent, Error> {
        loop {
            // keep consuming and processing packets until we have data for the user
            match self.eventloop.lock().await.poll().await? {
                Event::Incoming(incoming) => {
                    trace!("MQTT Incoming = {:?}", incoming);

                    match incoming {
                        rumqttc::Packet::ConnAck(conn_ack) => {
                            self.connack(conn_ack).await?;
                        }
                        rumqttc::Packet::Publish(publish) => {
                            let (_, _, interface, path) = parse_topic(&publish.topic)?;

                            // It can be borrowed as a &[u8]
                            let bdata = publish.payload;

                            if interface == "control" && path == "/consumer/properties" {
                                debug!("Purging properties");

                                self.purge_properties(&bdata).await?;

                                continue;
                            }

                            debug!("Incoming publish = {} {:?}", publish.topic, bdata);

                            let data = payload::deserialize(&bdata)?;

                            self.handle_payload(interface, &path, &data).await?;

                            if cfg!(debug_assertions) {
                                self.interfaces
                                    .read()
                                    .await
                                    .validate_receive(interface, &path, &bdata)?;
                            }

                            return Ok(AstarteDeviceDataEvent {
                                interface: interface.to_string(),
                                path: path.to_string(),
                                data,
                            });
                        }
                        _ => {}
                    }
                }
                Event::Outgoing(o) => trace!("MQTT Outgoing = {:?}", o),
            }
        }
    }

    /// Handles a payload received from the broker.
    async fn handle_payload<'a>(
        &self,
        interface: &str,
        path: &MappingPath<'a>,
        payload: &Aggregation,
    ) -> Result<(), Error> {
        match payload {
            Aggregation::Object(_) => Ok(()),
            Aggregation::Individual(ref data) => {
                let r_interfaces = self.interfaces.read().await;
                let interface = r_interfaces
                    .get_property(interface)
                    .filter(|interface| interface.mapping(path).is_some());

                if let Some(property) = interface {
                    self.store_property(property, path, data).await?;
                }

                Ok(())
            }
        }
    }

    /// Store the property.
    async fn store_property<'a>(
        &self,
        interface: PropertyRef<'a>,
        path: &MappingPath<'a>,
        data: &AstarteType,
    ) -> Result<(), Error> {
        debug_assert!(interface.is_property());
        debug_assert!(interface.mapping(path).is_some());

        //if database is loaded
        if let Some(database) = &self.database {
            let interface_name = interface.interface_name();
            let version_major = interface.version_major();

            database
                .store_prop(interface_name, path.as_str(), data, version_major)
                .await?;

            // database selftest / sanity check for debug builds
            if cfg!(debug_assertions) {
                let stored_prop = database
                    .load_prop(interface_name, path.as_str(), version_major)
                    .await
                    .expect("load_prop failed")
                    .expect("property wasn't correctly saved in the database");

                assert_eq!(*data, stored_prop);
                let prop = self
                    .property(interface_name, path)
                    .await?
                    .expect("property wasn't correctly saved in the database");
                assert_eq!(*data, prop);
                trace!("database test ok");
            }
        }

        Ok(())
    }

    fn client_id(&self) -> String {
        format!("{}/{}", self.realm, self.device_id)
    }

    async fn purge_properties(&self, bdata: &[u8]) -> Result<(), Error> {
        if let Some(db) = &self.database {
            let stored_props = db.load_all_props().await?;

            let paths = properties::extract_set_properties(bdata)?;

            for stored_prop in stored_props {
                if paths.contains(&(stored_prop.interface.clone() + &stored_prop.path)) {
                    continue;
                }

                db.delete_prop(&stored_prop.interface, &stored_prop.path)
                    .await?;
            }
        }

        Ok(())
    }

    async fn send_emptycache(&self) -> Result<(), Error> {
        let url = self.client_id() + "/control/emptyCache";
        debug!("sending emptyCache to {}", url);

        self.client
            .publish(url, rumqttc::QoS::ExactlyOnce, false, "1")
            .await?;

        Ok(())
    }

    async fn send_introspection(&self) -> Result<(), Error> {
        let interfaces = self.interfaces.read().await;
        let introspection = interfaces.get_introspection_string();

        debug!("sending introspection = {}", introspection);

        self.client
            .publish(
                self.client_id(),
                rumqttc::QoS::ExactlyOnce,
                false,
                introspection,
            )
            .await?;
        Ok(())
    }

    async fn send_device_owned_properties(&self) -> Result<(), Error> {
        if let Some(database) = &self.database {
            let properties = database.load_all_props().await?;
            // publish only device-owned properties...
            let interfaces = self.interfaces.read().await;
            let device_owned_properties: Vec<StoredProp> = properties
                .into_iter()
                .filter(|prop| match interfaces.get_property(&prop.interface) {
                    Some(interface) => {
                        interface.ownership() == Ownership::Device
                            && interface.version_major() == prop.interface_major
                    }
                    None => false,
                })
                .collect();
            for prop in device_owned_properties {
                let topic = format!("{}/{}{}", self.client_id(), prop.interface, prop.path);
                if let Some(version_major) = self.interfaces.read().await.get_property_major(
                    &prop.interface,
                    &MappingPath::try_from(prop.path.as_str())?,
                ) {
                    // ..and only if they are up-to-date
                    if version_major == prop.interface_major {
                        debug!(
                            "sending device-owned property = {}{}",
                            prop.interface, prop.path
                        );
                        self.client
                            .publish(topic, rumqttc::QoS::ExactlyOnce, false, prop.value)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Unset a device property.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{AstarteDeviceSdk, options::AstarteOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut sdk_options = AstarteOptions::new("_","_","_","_");
    ///     let mut device = AstarteDeviceSdk::new(sdk_options).await.unwrap();
    ///
    ///     device
    ///         .unset("my.interface.name", "/endpoint/path",)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error> {
        trace!("unsetting {} {}", interface_name, interface_path);

        let path = MappingPath::try_from(interface_path)?;

        if cfg!(debug_assertions) {
            self.interfaces
                .read()
                .await
                .validate_send(interface_name, &path, &[], &None)?;
        }

        self.send_with_timestamp_impl(interface_name, &path, AstarteType::Unset, None)
            .await?;

        Ok(())
    }

    /// When present get property from the allocated database (if allocated).
    ///
    /// ```no_run
    /// use astarte_device_sdk::{
    ///     AstarteDeviceSdk, database::AstarteSqliteDatabase, options::AstarteOptions,
    ///     types::AstarteType
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let database = AstarteSqliteDatabase::new("path/to/database/file.sqlite")
    ///         .await
    ///         .unwrap();
    ///     let mut sdk_options = AstarteOptions::new("_","_","_","_").database(database);
    ///     let mut device = AstarteDeviceSdk::new(sdk_options).await.unwrap();
    ///
    ///     let property_value: Option<AstarteType> = device
    ///         .get_property("my.interface.name", "/endpoint/path",)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn get_property(
        &self,
        interface: &str,
        path: &str,
    ) -> Result<Option<AstarteType>, Error> {
        let path_mappings = MappingPath::try_from(path)?;

        self.property(interface, &path_mappings).await
    }

    /// When present get property from the allocated database (if allocated).
    ///
    /// This will use a [`MappingPath`] to get the property, which is an parsed endpoint.
    pub(crate) async fn property<'a>(
        &self,
        interface: &str,
        path: &MappingPath<'a>,
    ) -> Result<Option<AstarteType>, Error> {
        let db = match &self.database {
            Some(db) => db,
            None => return Ok(None),
        };

        let major = self
            .interfaces
            .read()
            .await
            .get_property_major(interface, path);

        match major {
            Some(major) => db.load_prop(interface, path.as_str(), major).await,
            None => Ok(None),
        }
    }

    // ------------------------------------------------------------------------
    // individual types
    // ------------------------------------------------------------------------

    /// Send an individual datastream/property on an interface.
    ///
    /// The usage is the same of
    /// [send_with_timestamp()][crate::AstarteDeviceSdk::send_with_timestamp],
    /// without the timestamp.
    pub async fn send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType>,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_with_timestamp_impl(interface_name, &path, data, None)
            .await
    }

    /// Send an individual datastream/property on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{AstarteDeviceSdk, options::AstarteOptions};
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut sdk_options = AstarteOptions::new("_","_","_","_");
    ///     let mut device = AstarteDeviceSdk::new(sdk_options).await.unwrap();
    ///
    ///     let value: i32 = 42;
    ///     let timestamp = Utc.timestamp_opt(1537449422, 0).unwrap();
    ///     device.send_with_timestamp("my.interface.name", "/endpoint/path", value, timestamp)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn send_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType>,
    {
        let mapping = MappingPath::try_from(interface_path)?;

        self.send_with_timestamp_impl(interface_name, &mapping, data, Some(timestamp))
            .await
    }

    async fn send_with_timestamp_impl<'a, D>(
        &self,
        interface_name: &str,
        interface_path: &MappingPath<'a>,
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType>,
    {
        debug!("sending {} {}", interface_name, interface_path);

        let data = data.try_into().map_err(|_| TypeError::Conversion)?;

        let buf = payload::serialize_individual(&data, timestamp)?;

        if cfg!(debug_assertions) {
            self.interfaces.read().await.validate_send(
                interface_name,
                interface_path,
                &buf,
                &timestamp,
            )?;
        }

        let interfaces = self.interfaces.read().await;
        let opt_property = interfaces.get_property(interface_name);
        if let Some(property) = opt_property {
            let stored = self
                .check_property_already_stored(property, interface_path, &data)
                .await?;

            if stored {
                debug!("property was already sent, no need to send it again");
                return Ok(());
            }
        }

        self.client
            .publish(
                self.client_id() + "/" + interface_name.trim_matches('/') + interface_path.as_str(),
                self.interfaces
                    .read()
                    .await
                    .get_mqtt_reliability(interface_name, interface_path),
                false,
                buf,
            )
            .await?;

        // we store the property in the database after it has been successfully sent
        if let Some(property) = opt_property {
            self.store_property_on_send(property, interface_path, &data)
                .await?;
        }

        Ok(())
    }

    /// Check if a property is already stored in the database with the same value.
    /// Useful to prevent sending a property twice with the same value.
    async fn check_property_already_stored<'a>(
        &self,
        interface: PropertyRef<'a>,
        interface_path: &MappingPath<'a>,
        data: &AstarteType,
    ) -> Result<bool, Error> {
        let Some(ref db) = self.database else {
            return Ok(false);
        };

        // Check the mapping exists
        interface
            .mapping(interface_path)
            .ok_or_else(|| Error::SendError(format!("Mapping {interface_path} doesn't exist")))?;

        // Check if already in db
        let stored = db
            .load_prop(interface.interface_name(), interface_path.as_str(), 0)
            .await?;

        match stored {
            Some(value) => Ok(value.eq(data)),
            None => Ok(false),
        }
    }

    async fn store_property_on_send<'a>(
        &self,
        property: PropertyRef<'a>,
        interface_path: &MappingPath<'a>,
        data: &AstarteType,
    ) -> Result<(), Error> {
        let Some(ref db) = self.database else {
            return Ok(());
        };

        property.mapping(interface_path).ok_or_else(|| {
            Error::Interface(InterfaceError::MappingNotFound {
                path: interface_path.to_string(),
            })
        })?;

        db.store_prop(
            property.interface_name(),
            interface_path.as_str(),
            data,
            property.version_major(),
        )
        .await?;

        debug!("Stored new property in database");

        Ok(())
    }

    async fn remove_properties_from_store(&self, interface_name: &str) -> Result<(), Error> {
        let db = match self.database {
            Some(ref db) => db,
            None => return Ok(()),
        };

        let interfaces = self.interfaces.read().await;
        let mappings = interfaces.get_property(interface_name);

        let property = match mappings {
            Some(property) => property,
            None => return Ok(()),
        };

        for mapping in property.iter_mappings() {
            let path = mapping.endpoint();
            db.delete_prop(interface_name, path).await?;
            debug!("Stored property {}{} deleted", interface_name, path);
        }

        Ok(())
    }

    // ------------------------------------------------------------------------
    // object types
    // ------------------------------------------------------------------------

    async fn send_object_with_timestamp_impl<'a, T>(
        &self,
        interface_name: &str,
        interface_path: &MappingPath<'a>,
        data: T,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error>
    where
        T: AstarteAggregate,
    {
        let aggregate = data.astarte_aggregate()?;
        let buf = payload::serialize_object(&aggregate, timestamp)?;

        if cfg!(debug_assertions) {
            self.interfaces.read().await.validate_send(
                interface_name,
                interface_path,
                &buf,
                &timestamp,
            )?;
        }

        let topic =
            self.client_id() + "/" + interface_name.trim_matches('/') + interface_path.as_str();
        let qos = self
            .interfaces
            .read()
            .await
            .get_mqtt_reliability(interface_name, interface_path);

        self.client.publish(topic, qos, false, buf).await?;

        Ok(())
    }

    /// Send an object datastreamy on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// # use astarte_device_sdk::{AstarteDeviceSdk, options::AstarteOptions, AstarteAggregate};
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
    ///     let mut sdk_options = AstarteOptions::new("_","_","_","_");
    ///     let mut device = AstarteDeviceSdk::new(sdk_options).await.unwrap();
    ///
    ///     let data = TestObject {
    ///         endpoint1: 1.34,
    ///         endpoint2: false
    ///     };
    ///     let timestamp = Utc.timestamp_opt(1537449422, 0).unwrap();
    ///     device.send_object_with_timestamp("my.interface.name", "/endpoint/path", data, timestamp)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn send_object_with_timestamp<T>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: T,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        T: AstarteAggregate,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_object_with_timestamp_impl(interface_name, &path, data, Some(timestamp))
            .await
    }

    /// Send an object datastream on an interface.
    ///
    /// The usage is the same of
    /// [send_object_with_timestamp()][crate::AstarteDeviceSdk::send_object_with_timestamp],
    /// without the timestamp.
    pub async fn send_object<T>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: T,
    ) -> Result<(), Error>
    where
        T: AstarteAggregate,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_object_with_timestamp_impl(interface_name, &path, data, None)
            .await
    }
}

impl fmt::Debug for AstarteDeviceSdk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AstarteDeviceSdk")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            .field("interfaces", &self.interfaces)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod test {
    use base64::Engine;
    use mockall::predicate;
    use rumqttc::Event;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::sync::{Mutex, RwLock};

    use crate::interfaces::Interfaces;
    use crate::properties::tests::PROPERTIES_PAYLOAD;
    use crate::{self as astarte_device_sdk, payload, Interface};
    use astarte_device_sdk::AstarteAggregate;
    use astarte_device_sdk::{types::AstarteType, Aggregation, AstarteDeviceSdk};
    use astarte_device_sdk_derive::astarte_aggregate;
    #[cfg(not(feature = "derive"))]
    use astarte_device_sdk_derive::AstarteAggregate;

    use super::{AsyncClient, EventLoop};

    // Interfaces
    const OBJECT_DEVICE_DATASTREAM: &str = include_str!("../examples/object_datastream/interfaces/org.astarte-platform.rust.examples.object-datastream.DeviceDatastream.json");
    const INDIVIDUAL_SERVER_DATASTREAM: &str = include_str!("../examples/individual_datastream/interfaces/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream.json");
    const DEVICE_PROPERTIES: &str = include_str!("../examples/individual_properties/interfaces/org.astarte-platform.rust.examples.individual-properties.DeviceProperties.json");
    const SERVER_PROPERTIES: &str = include_str!("../examples/individual_properties/interfaces/org.astarte-platform.rust.examples.individual-properties.ServerProperties.json");

    fn mock_astarte_device<I>(
        client: AsyncClient,
        eventloop: EventLoop,
        interfaces: I,
    ) -> AstarteDeviceSdk
    where
        I: IntoIterator<Item = Interface>,
    {
        AstarteDeviceSdk {
            realm: "realm".to_string(),
            device_id: "device_id".to_string(),
            client,
            database: None,
            interfaces: Arc::new(RwLock::new(Interfaces::from(interfaces).unwrap())),
            eventloop: Arc::new(Mutex::new(eventloop)),
        }
    }

    #[derive(AstarteAggregate)]
    #[astarte_aggregate(rename_all = "lowercase")]
    struct MyLowerCasedAggregate {
        endpoint01: f64,
        endpoint02: i32,
        endpoint03: bool,
        endpoint04: i64,
        endpoint05: String,
        endpoint06: Vec<u8>,
        endpoint07: chrono::DateTime<chrono::Utc>,
        endpoint08: Vec<f64>,
        endpoint09: Vec<i32>,
        endpoint10: Vec<bool>,
        endpoint11: Vec<i64>,
        endpoint12: Vec<String>,
        endpoint13: Vec<Vec<u8>>,
        endpoint14: Vec<chrono::DateTime<chrono::Utc>>,
    }

    #[test]
    fn test_astarte_aggregate_trait_lower_case_attribute() {
        let my_aggregate = MyLowerCasedAggregate {
            endpoint01: 4.34,
            endpoint02: 1,
            endpoint03: true,
            endpoint04: 45543543534,
            endpoint05: "Hello".to_string(),
            endpoint06: base64::engine::general_purpose::STANDARD
                .decode("aGVsbG8=")
                .unwrap(),
            endpoint07: chrono::offset::Utc::now(),
            endpoint08: Vec::from([43.5, 10.5, 11.9]),
            endpoint09: Vec::from([-4, 123, -2222, 30]),
            endpoint10: Vec::from([true, false]),
            endpoint11: Vec::from([53267895478, 53267895428, 53267895118]),
            endpoint12: Vec::from(["Test ".to_string(), "String".to_string()]),
            endpoint13: Vec::from([
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
            ]),
            endpoint14: Vec::from([chrono::offset::Utc::now(), chrono::offset::Utc::now()]),
        };
        let expected_res = HashMap::from([
            (
                "endpoint01".to_string(),
                AstarteType::Double(my_aggregate.endpoint01),
            ),
            (
                "endpoint02".to_string(),
                AstarteType::Integer(my_aggregate.endpoint02),
            ),
            (
                "endpoint03".to_string(),
                AstarteType::Boolean(my_aggregate.endpoint03),
            ),
            (
                "endpoint04".to_string(),
                AstarteType::LongInteger(my_aggregate.endpoint04),
            ),
            (
                "endpoint05".to_string(),
                AstarteType::String(my_aggregate.endpoint05.clone()),
            ),
            (
                "endpoint06".to_string(),
                AstarteType::BinaryBlob(my_aggregate.endpoint06.clone()),
            ),
            (
                "endpoint07".to_string(),
                AstarteType::DateTime(my_aggregate.endpoint07),
            ),
            (
                "endpoint08".to_string(),
                AstarteType::DoubleArray(my_aggregate.endpoint08.clone()),
            ),
            (
                "endpoint09".to_string(),
                AstarteType::IntegerArray(my_aggregate.endpoint09.clone()),
            ),
            (
                "endpoint10".to_string(),
                AstarteType::BooleanArray(my_aggregate.endpoint10.clone()),
            ),
            (
                "endpoint11".to_string(),
                AstarteType::LongIntegerArray(my_aggregate.endpoint11.clone()),
            ),
            (
                "endpoint12".to_string(),
                AstarteType::StringArray(my_aggregate.endpoint12.clone()),
            ),
            (
                "endpoint13".to_string(),
                AstarteType::BinaryBlobArray(my_aggregate.endpoint13.clone()),
            ),
            (
                "endpoint14".to_string(),
                AstarteType::DateTimeArray(my_aggregate.endpoint14.clone()),
            ),
        ]);
        assert_eq!(expected_res, my_aggregate.astarte_aggregate().unwrap());
        println!("{expected_res:?}");
    }

    #[derive(AstarteAggregate)]
    #[astarte_aggregate(rename_all = "UPPERCASE")]
    struct MyUpperCasedAggregate {
        first_endpoint: f64,
        second_endpoint: f64,
    }

    #[test]
    fn test_astarte_aggregate_trait_upper_case_attribute() {
        let my_aggregate = MyUpperCasedAggregate {
            first_endpoint: 4.34,
            second_endpoint: 23.0,
        };
        let expected_res = HashMap::from([
            (
                "FIRST_ENDPOINT".to_string(),
                AstarteType::Double(my_aggregate.first_endpoint),
            ),
            (
                "SECOND_ENDPOINT".to_string(),
                AstarteType::Double(my_aggregate.second_endpoint),
            ),
        ]);
        assert_eq!(expected_res, my_aggregate.astarte_aggregate().unwrap());
    }

    #[derive(AstarteAggregate)]
    #[astarte_aggregate(rename_all = "PascalCase")]
    struct MyPascalCasedAggregate {
        first_endpoint: f64,
        second_endpoint: f64,
    }

    #[test]
    fn test_astarte_aggregate_trait_pascal_case_attribute() {
        let my_aggregate = MyPascalCasedAggregate {
            first_endpoint: 4.34,
            second_endpoint: 23.0,
        };
        let expected_res = HashMap::from([
            (
                "FirstEndpoint".to_string(),
                AstarteType::Double(my_aggregate.first_endpoint),
            ),
            (
                "SecondEndpoint".to_string(),
                AstarteType::Double(my_aggregate.second_endpoint),
            ),
        ]);
        assert_eq!(expected_res, my_aggregate.astarte_aggregate().unwrap());
    }

    #[tokio::test]
    async fn test_wait_for_connack() {
        let mut eventloope = EventLoop::default();

        eventloope.expect_poll().once().returning(|| {
            Ok(Event::Incoming(rumqttc::Packet::ConnAck(
                rumqttc::ConnAck {
                    session_present: false,
                    code: rumqttc::ConnectReturnCode::Success,
                },
            )))
        });

        let mut client = AsyncClient::default();

        client
            .expect_subscribe()
            .once()
            .returning(|_: String, _| Ok(()));

        client
            .expect_publish::<String, String>()
            .returning(|topic, _, _, _| {
                // Client id
                assert_eq!(topic, "realm/device_id");

                Ok(())
            });

        client
            .expect_publish::<String, &str>()
            .returning(|topic, _, _, payload| {
                // empty cache
                assert_eq!(topic, "realm/device_id/control/emptyCache");

                assert_eq!(payload, "1");

                Ok(())
            });

        client.expect_subscribe::<String>().returning(|topic, _qos| {
            assert_eq!(topic, "realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#");

            Ok(())
        });

        let interfaces = [
            Interface::from_str(OBJECT_DEVICE_DATASTREAM).unwrap(),
            Interface::from_str(INDIVIDUAL_SERVER_DATASTREAM).unwrap(),
        ];

        let mut astarte = mock_astarte_device(client, eventloope, interfaces);

        astarte.wait_for_connack().await.unwrap();
    }

    #[tokio::test]
    async fn test_add_remove_interface() {
        let eventloope = EventLoop::default();

        let mut client = AsyncClient::default();

        client
            .expect_subscribe::<String>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()),
                predicate::always()
            )
            .returning(|_, _| { Ok(()) });

        client
            .expect_publish::<String, String>()
            .with(
                predicate::eq("realm/device_id".to_string()),
                predicate::always(),
                predicate::eq(false),
                predicate::eq(
                    "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream:0:1"
                        .to_string(),
                ),
            )
            .returning(|_, _, _, _| Ok(()));

        client
            .expect_publish::<String, String>()
            .with(
                predicate::eq("realm/device_id".to_string()),
                predicate::always(),
                predicate::eq(false),
                predicate::eq(String::new()),
            )
            .returning(|_, _, _, _| Ok(()));

        client
            .expect_unsubscribe::<String>()
            .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()))
            .returning(|_| Ok(()));

        let astarte = mock_astarte_device(client, eventloope, []);

        astarte
            .add_interface_from_str(INDIVIDUAL_SERVER_DATASTREAM)
            .await
            .unwrap();

        astarte
            .remove_interface(
                "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_handle_event() {
        let mut client = AsyncClient::default();

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/1/name".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::function(|buf: &Vec<u8>| {
                    let doc= bson::Document::from_reader(buf.as_slice()).unwrap();

                    let value = doc.get("v").unwrap().as_str().unwrap();

                    value == "name number 1"
                }),
            )
            .returning(|_, _, _, _| Ok(()));

        let mut eventloope = EventLoop::default();

        let data = bson::doc! {
            "v": true
        };

        // Purge properties
        eventloope.expect_poll().once().returning(|| {
            Ok(Event::Incoming(rumqttc::Packet::Publish(
                rumqttc::Publish::new(
                    "realm/device_id/control/consumer/properties",
                    rumqttc::QoS::AtLeastOnce,
                    PROPERTIES_PAYLOAD,
                ),
            )))
        });

        // Send properties
        eventloope.expect_poll().once().returning(move || {
            Ok(Event::Incoming(rumqttc::Packet::Publish(
                rumqttc::Publish::new(
                    "realm/device_id/org.astarte-platform.rust.examples.individual-properties.ServerProperties/1/enable",
                    rumqttc::QoS::AtLeastOnce,
                    bson::to_vec(&data).unwrap()
                ),
            )))
        });

        let mut astarte = mock_astarte_device(
            client,
            eventloope,
            [
                Interface::from_str(DEVICE_PROPERTIES).unwrap(),
                Interface::from_str(SERVER_PROPERTIES).unwrap(),
            ],
        );

        astarte
            .send(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
                "name number 1".to_string(),
            )
            .await
            .unwrap();

        let event = astarte.handle_events().await;

        assert!(
            event.is_ok(),
            "Error handling event {:?}",
            event.unwrap_err()
        );

        let event = event.unwrap();

        assert_eq!("/1/enable", event.path);

        match event.data {
            Aggregation::Individual(AstarteType::Boolean(val)) => {
                assert!(val);
            }
            _ => panic!("Wrong data type {:?}", event.data),
        }
    }

    #[tokio::test]
    async fn test_unset_property() {
        let mut client = AsyncClient::default();

        let value = AstarteType::String(String::from("name number 1"));
        let buf = payload::serialize_individual(&value, None).unwrap();

        let unset = payload::serialize_individual(&AstarteType::Unset, None).unwrap();

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/1/name".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::eq(buf),
            )
            .returning(|_, _, _, _| Ok(()));

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/1/name".to_string()), predicate::always(), predicate::always(), predicate::eq(unset))
            .returning(|_, _, _, _| Ok(()));

        let eventloope = EventLoop::default();

        let astarte = mock_astarte_device(
            client,
            eventloope,
            [Interface::from_str(DEVICE_PROPERTIES).unwrap()],
        );

        astarte
            .send(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
                "name number 1".to_string(),
            )
            .await
            .unwrap();

        astarte
            .unset(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_receive_object() {
        let client = AsyncClient::default();

        let mut eventloope = EventLoop::default();

        let data = bson::doc! {
            "v": {
                "endpoint1": 4.2,
                "endpoint2": "obj",
                "endpoint3": [true],
            }
        };

        // Send object
        eventloope.expect_poll().returning(move || {
            Ok(Event::Incoming(rumqttc::Packet::Publish(
                rumqttc::Publish::new(
                    "realm/device_id/org.astarte-platform.rust.examples.object-datastream.DeviceDatastream/1",
                    rumqttc::QoS::AtLeastOnce,
                    bson::to_vec(&data).unwrap()
                ),
            )))
        });

        let mut astarte = mock_astarte_device(
            client,
            eventloope,
            [Interface::from_str(OBJECT_DEVICE_DATASTREAM).unwrap()],
        );

        let event = astarte.handle_events().await;

        assert!(
            event.is_ok(),
            "Error handling event {:?}",
            event.unwrap_err()
        );

        let event = event.unwrap();

        let mut obj = HashMap::new();
        obj.insert("endpoint1".to_string(), AstarteType::Double(4.2));
        obj.insert(
            "endpoint2".to_string(),
            AstarteType::String("obj".to_string()),
        );
        obj.insert(
            "endpoint3".to_string(),
            AstarteType::BooleanArray(vec![true]),
        );
        let expected = Aggregation::Object(obj);

        assert_eq!(
            "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream",
            event.interface
        );
        assert_eq!("/1", event.path);
        assert_eq!(expected, event.data);
    }
}
