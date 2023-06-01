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
pub mod interface;
mod interfaces;
#[cfg(test)]
mod mock;
pub mod options;
pub mod pairing;
pub mod registration;
mod topic;
pub mod types;

#[cfg(test)]
use mock::{MockAsyncClient as AsyncClient, MockEventLoop as EventLoop};
#[cfg(not(test))]
use rumqttc::{AsyncClient, EventLoop};

// Re-export rumqttc since we return its types in some methods
use bson::Bson;
pub use chrono;
use database::AstarteDatabase;
use database::StoredProp;
use log::{debug, error, info, trace, warn};
use options::AstarteOptions;
pub use rumqttc;
use rumqttc::{Event, MqttOptions};
use std::collections::HashMap;
use std::convert::Infallible;
use std::convert::TryInto;
use std::fmt::{self, Debug};
use std::iter::FromIterator;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use types::AstarteType;

use crate::interface::mapping::path::{Error as MappingError, MappingPath};
use crate::topic::{parse_topic, TopicError};
use interface::error::ValidationError;
pub use interface::Interface;

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
    /// use astarte_device_sdk::{types::AstarteType, AstarteError, AstarteAggregate};
    ///
    /// struct Person {
    ///     name: String,
    ///     age: i32,
    ///     phones: Vec<String>,
    /// }
    ///
    /// // This is what #[derive(AstarteAggregate)] would generate.
    /// impl AstarteAggregate for Person {
    ///     fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, AstarteError>
    ///     {
    ///         let mut r = HashMap::new();
    ///         r.insert("name".to_string(), self.name.try_into()?);
    ///         r.insert("age".to_string(), self.age.try_into()?);
    ///         r.insert("phones".to_string(), self.phones.try_into()?);
    ///         Ok(r)
    ///     }
    /// }
    /// ```
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, AstarteError>;
}

impl AstarteAggregate for HashMap<String, AstarteType> {
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, AstarteError> {
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
    mqtt_options: MqttOptions,
    client: AsyncClient,
    eventloop: Arc<tokio::sync::Mutex<EventLoop>>,
    interfaces: Arc<tokio::sync::RwLock<interfaces::Interfaces>>,
    database: Option<Arc<dyn AstarteDatabase + Sync + Send>>,
}

/// Astarte error.
///
/// Possible errors returned by functions of the Astarte device SDK.
#[derive(thiserror::Error, Debug)]
pub enum AstarteError {
    #[error("bson serialize error")]
    BsonSerError(#[from] bson::ser::Error),

    #[error("bson client error")]
    BsonClientError(#[from] rumqttc::ClientError),

    #[error("mqtt connection error")]
    ConnectionError(#[from] rumqttc::ConnectionError),

    #[error("malformed input from Astarte backend")]
    DeserializationError,

    #[error("error converting from Bson to AstarteType ({0})")]
    FromBsonError(String),

    #[error("type mismatch in bson array from astarte, something has gone very wrong here")]
    FromBsonArrayError,

    #[error("forbidden floating point number")]
    FloatError,

    #[error("send error ({0})")]
    SendError(String),

    #[error("receive error ({0})")]
    ReceiveError(String),

    #[error("database error")]
    DbError(#[from] sqlx::Error),

    #[error("options error")]
    OptionsError(#[from] options::AstarteOptionsError),

    #[error(transparent)]
    InterfaceError(#[from] interface::Error),

    #[error("generic error ({0})")]
    Reported(String),

    #[error("generic error")]
    Unreported,

    #[error("conversion error")]
    Conversion,

    #[error("infallible error")]
    Infallible(#[from] Infallible),

    #[error("invalid topic {}",.0.topic())]
    InvalidTopic(#[from] TopicError),

    #[error("invalid mapping path '{}'", .0.path())]
    InvalidEndpoint(#[from] MappingError),

    #[error("invalid interface added")]
    InvalidInterface(#[from] ValidationError),
}

/// Payload format for an Astarte device event data.
#[derive(Debug, Clone)]
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
    ///     let mut device = AstarteDeviceSdk::new(&sdk_options).await.unwrap();
    /// }
    /// ```
    pub async fn new(opts: &AstarteOptions) -> Result<AstarteDeviceSdk, AstarteError> {
        let mqtt_options = pairing::get_transport_config(opts).await?;

        debug!("{:#?}", mqtt_options);

        let (client, eventloop) = AsyncClient::new(mqtt_options.clone(), 50);

        let mut device = AstarteDeviceSdk {
            realm: opts.realm.to_owned(),
            device_id: opts.device_id.to_owned(),
            mqtt_options,
            client,
            eventloop: Arc::new(tokio::sync::Mutex::new(eventloop)),
            interfaces: Arc::new(tokio::sync::RwLock::new(opts.interfaces.clone())),
            database: opts.database.clone(),
        };

        device.wait_for_connack().await?;

        Ok(device)
    }

    async fn wait_for_connack(&mut self) -> Result<(), AstarteError> {
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

    async fn connack(&self, p: rumqttc::ConnAck) -> Result<(), AstarteError> {
        if !p.session_present {
            self.subscribe().await?;
            self.send_introspection().await?;
            self.send_emptycache().await?;
            self.send_device_owned_properties().await?;
            info!("connack done");
        }

        Ok(())
    }

    async fn subscribe(&self) -> Result<(), AstarteError> {
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

    async fn subscribe_server_owned_interface(
        &self,
        iface: &Interface,
    ) -> Result<(), AstarteError> {
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

    async fn unsubscribe_server_owned_interface(
        &self,
        iface: &Interface,
    ) -> Result<(), AstarteError> {
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
    pub async fn add_interface_from_file(&self, file_path: &str) -> Result<(), AstarteError> {
        let path = Path::new(file_path);
        let interface = Interface::from_file(path)?;
        self.add_interface(interface).await
    }

    /// Add a new interface from a string. The string should contain a valid json formatted
    /// interface.
    pub async fn add_interface_from_str(&self, json_str: &str) -> Result<(), AstarteError> {
        let interface: Interface = Interface::from_str(json_str)?;
        self.add_interface(interface).await
    }

    /// Add a new [`Interface`] to the device interfaces.
    pub async fn add_interface(&self, interface: Interface) -> Result<(), AstarteError> {
        if interface.ownership() == interface::Ownership::Server {
            self.subscribe_server_owned_interface(&interface).await?;
        }
        self.add_interface_to_introspection(interface).await?;
        self.send_introspection().await?;
        Ok(())
    }

    async fn add_interface_to_introspection(
        &self,
        interface: Interface,
    ) -> Result<(), AstarteError> {
        self.interfaces.write().await.add(interface)?;

        Ok(())
    }

    /// Remove the interface with the name specified as argument.
    pub async fn remove_interface(&self, interface_name: &str) -> Result<(), AstarteError> {
        let interface = self.remove_interface_from_map(interface_name).await?;
        self.remove_properties_from_store(interface_name).await?;
        self.send_introspection().await?;
        if interface.ownership() == interface::Ownership::Server {
            self.unsubscribe_server_owned_interface(&interface).await?;
        }
        Ok(())
    }

    async fn remove_interface_from_map(
        &self,
        interface_name: &str,
    ) -> Result<Interface, AstarteError> {
        self.interfaces
            .write()
            .await
            .remove(interface_name)
            .ok_or(AstarteError::InterfaceError(
                interface::Error::InterfaceNotFound,
            ))
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
    ///     let mut device = AstarteDeviceSdk::new(&sdk_options).await.unwrap();
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
    pub async fn handle_events(&mut self) -> Result<AstarteDeviceDataEvent, AstarteError> {
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

                            let bdata = publish.payload.to_vec();

                            if interface == "control" && path == "/consumer/properties" {
                                self.purge_properties(bdata).await?;
                                continue;
                            }

                            debug!("Incoming publish = {} {:?}", publish.topic, bdata);

                            self.store_property_in_database(interface, &path, &bdata)
                                .await?;

                            if cfg!(debug_assertions) {
                                self.interfaces
                                    .read()
                                    .await
                                    .validate_receive(interface, &path, &bdata)?;
                            }

                            let data = AstarteDeviceSdk::deserialize(&bdata)?;

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

    async fn store_property_in_database<'a>(
        &self,
        interface: &str,
        path: &MappingPath<'a>,
        bdata: &[u8],
    ) -> Result<(), AstarteError> {
        //if database is loaded
        if let Some(database) = &self.database {
            //if it's a property
            if let Some(major_version) = self
                .interfaces
                .read()
                .await
                .get_property_major(interface, path)
            {
                database
                    .store_prop(interface, path.as_str(), bdata, major_version)
                    .await?;

                // database selftest / sanity check for debug builds
                if cfg!(debug_assertions) {
                    let original = crate::AstarteDeviceSdk::deserialize(bdata)?;
                    if let Aggregation::Individual(data) = original {
                        let db = database
                            .load_prop(interface, path.as_str(), major_version)
                            .await
                            .expect("load_prop failed")
                            .expect("property wasn't correctly saved in the database");
                        assert!(data == db);
                        let prop = self
                            .property(interface, path)
                            .await?
                            .expect("property wasn't correctly saved in the database");
                        assert!(data == prop);
                        trace!("database test ok");
                    } else {
                        panic!("This should be impossible, can't have object properties");
                    }
                }
            }
        }
        Ok(())
    }

    fn client_id(&self) -> String {
        format!("{}/{}", self.realm, self.device_id)
    }

    async fn purge_properties(&self, bdata: Vec<u8>) -> Result<(), AstarteError> {
        if let Some(db) = &self.database {
            let stored_props = db.load_all_props().await?;

            let paths = utils::extract_set_properties(&bdata);

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

    async fn send_emptycache(&self) -> Result<(), AstarteError> {
        let url = self.client_id() + "/control/emptyCache";
        debug!("sending emptyCache to {}", url);

        self.client
            .publish(url, rumqttc::QoS::ExactlyOnce, false, "1")
            .await?;

        Ok(())
    }

    async fn send_introspection(&self) -> Result<(), AstarteError> {
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

    async fn send_device_owned_properties(&self) -> Result<(), AstarteError> {
        if let Some(database) = &self.database {
            let properties = database.load_all_props().await?;
            // publish only device-owned properties...
            let interfaces = self.interfaces.read().await;
            let device_owned_properties: Vec<StoredProp> = properties
                .into_iter()
                .filter(|prop| {
                    interfaces.get_ownership(&prop.interface)
                        == Some(crate::interface::Ownership::Device)
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
    ///     let mut device = AstarteDeviceSdk::new(&sdk_options).await.unwrap();
    ///
    ///     device
    ///         .unset("my.interface.name", "/endpoint/path",)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn unset(
        &self,
        interface_name: &str,
        interface_path: &str,
    ) -> Result<(), AstarteError> {
        trace!("unsetting {} {}", interface_name, interface_path);

        if cfg!(debug_assertions) {
            self.interfaces.read().await.validate_send(
                interface_name,
                interface_path,
                &[],
                &None,
            )?;
        }

        self.send_with_timestamp_impl(interface_name, interface_path, AstarteType::Unset, None)
            .await?;

        Ok(())
    }

    fn serialize(
        data: Bson,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError> {
        if let Bson::Null = data {
            return Ok(Vec::new());
        }

        let doc = if let Some(timestamp) = timestamp {
            bson::doc! {
               "t": timestamp,
               "v": data
            }
        } else {
            bson::doc! {
               "v": data,
            }
        };

        let mut buf = Vec::new();
        doc.to_writer(&mut buf)?;
        trace!("serialized {:#?}", doc);
        Ok(buf)
    }

    fn deserialize(bdata: &[u8]) -> Result<Aggregation, AstarteError> {
        if bdata.is_empty() {
            return Ok(Aggregation::Individual(AstarteType::Unset));
        }
        if let Ok(deserialized) = bson::Document::from_reader(&mut std::io::Cursor::new(bdata)) {
            trace!("{:?}", deserialized);
            if let Some(v) = deserialized.get("v") {
                if let Bson::Document(doc) = v {
                    let strings = doc.iter().map(|f| f.0.clone());

                    let data = doc.iter().map(|f| f.1.clone().try_into());
                    let data: Result<Vec<AstarteType>, AstarteError> = data.collect();
                    let data = data?;

                    let hmap: HashMap<String, AstarteType> = strings.zip(data).collect();

                    Ok(Aggregation::Object(hmap))
                } else if let Ok(v) = v.clone().try_into() {
                    Ok(Aggregation::Individual(v))
                } else {
                    Err(AstarteError::DeserializationError)
                }
            } else {
                Err(AstarteError::DeserializationError)
            }
        } else {
            Err(AstarteError::DeserializationError)
        }
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
    ///     let mut device = AstarteDeviceSdk::new(&sdk_options).await.unwrap();
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
    ) -> Result<Option<AstarteType>, AstarteError> {
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
    ) -> Result<Option<AstarteType>, AstarteError> {
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
    ) -> Result<(), AstarteError>
    where
        D: TryInto<AstarteType>,
    {
        self.send_with_timestamp_impl(interface_name, interface_path, data, None)
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
    ///     let mut device = AstarteDeviceSdk::new(&sdk_options).await.unwrap();
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
    ) -> Result<(), AstarteError>
    where
        D: TryInto<AstarteType>,
    {
        self.send_with_timestamp_impl(interface_name, interface_path, data, Some(timestamp))
            .await
    }

    async fn send_with_timestamp_impl<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), AstarteError>
    where
        D: TryInto<AstarteType>,
    {
        debug!("sending {} {}", interface_name, interface_path);

        let data = data.try_into().map_err(|_| AstarteError::Conversion)?;

        let buf = AstarteDeviceSdk::serialize_individual(data.clone(), timestamp)?;

        if cfg!(debug_assertions) {
            self.interfaces.read().await.validate_send(
                interface_name,
                interface_path,
                &buf,
                &timestamp,
            )?;
        }

        if self
            .check_property_on_send(interface_name, interface_path, data.clone())
            .await?
        {
            debug!("property was already sent, no need to send it again");
            return Ok(());
        }

        self.client
            .publish(
                self.client_id() + "/" + interface_name.trim_matches('/') + interface_path,
                self.interfaces
                    .read()
                    .await
                    .get_mqtt_reliability(interface_name, &interface_path.try_into()?),
                false,
                buf,
            )
            .await?;

        // we store the property in the database after it has been successfully sent
        self.store_property_on_send(interface_name, interface_path, data)
            .await?;
        Ok(())
    }

    /// Check if a property is already stored in the database with the same value.
    /// Useful to prevent sending a property twice with the same value.
    async fn check_property_on_send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<bool, AstarteError>
    where
        D: TryInto<AstarteType>,
    {
        let db = match self.database {
            Some(ref db) => db,
            None => return Ok(false),
        };
        //if database is present

        let data: AstarteType = data.try_into().map_err(|_| AstarteError::Conversion)?;

        let interfaces = self.interfaces.read().await;

        interfaces
            .get_propertiy_mapping(interface_name, &interface_path.try_into()?)
            .ok_or_else(|| {
                AstarteError::SendError(format!("Mapping {interface_path} doesn't exist"))
            })?;

        // Check if already in db
        let is_present = db
            .load_prop(interface_name, interface_path, 0)
            .await?
            .map(|db_data| db_data == data)
            .unwrap_or(false);

        Ok(is_present)
    }

    async fn store_property_on_send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), AstarteError>
    where
        D: TryInto<AstarteType>,
    {
        let db = match self.database {
            Some(ref db) => db,
            None => return Ok(()),
        };
        //if database is present

        let data: AstarteType = data.try_into().map_err(|_| AstarteError::Conversion)?;

        let interfaces = self.interfaces.read().await;

        let interface = interfaces
            .get(interface_name)
            .ok_or(AstarteError::InterfaceError(
                interface::Error::InterfaceNotFound,
            ))?;

        let interface_major = interface.version_major();

        let path = MappingPath::try_from(interface_path)?;

        interface
            .properties()
            .and_then(|properties| properties.mapping(&path))
            .ok_or(AstarteError::InterfaceError(
                interface::Error::MappingNotFound,
            ))?;

        let bin = AstarteDeviceSdk::serialize_individual(data, None)?;

        db.store_prop(interface_name, interface_path, &bin, interface_major)
            .await?;

        debug!("Stored new property in database");

        Ok(())
    }

    async fn remove_properties_from_store(&self, interface_name: &str) -> Result<(), AstarteError> {
        let db = match self.database {
            Some(ref db) => db,
            None => return Ok(()),
        };

        let interfaces = self.interfaces.read().await;
        let mappings = interfaces
            .get(interface_name)
            .and_then(|interface| interface.properties());

        let property = match mappings {
            Some(property) => property,
            None => return Ok(()),
        };

        for mapping in property.iter_mappings() {
            //if mapping is a property
            let path = mapping.endpoint();
            db.delete_prop(interface_name, path).await?;
            debug!("Stored property {}{} deleted", interface_name, path);
        }

        Ok(())
    }

    fn serialize_individual<D>(
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError>
    where
        D: TryInto<AstarteType>,
    {
        let data: AstarteType = data.try_into().map_err(|_| AstarteError::Conversion)?;
        AstarteDeviceSdk::serialize(data.into(), timestamp)
    }

    // ------------------------------------------------------------------------
    // object types
    // ------------------------------------------------------------------------

    fn serialize_object<T>(
        data: T,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError>
    where
        T: AstarteAggregate,
    {
        let iter_d = data
            .astarte_aggregate()?
            .into_iter()
            .map(|(k, v)| (k, v.into()));
        let doc_d: bson::Document = bson::Document::from_iter(iter_d);
        let bson_d: bson::Bson = bson::Bson::Document(doc_d);
        AstarteDeviceSdk::serialize(bson_d, timestamp)
    }

    async fn send_object_with_timestamp_impl<T>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: T,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), AstarteError>
    where
        T: AstarteAggregate,
    {
        let buf = AstarteDeviceSdk::serialize_object(data, timestamp)?;

        if cfg!(debug_assertions) {
            self.interfaces.read().await.validate_send(
                interface_name,
                interface_path,
                &buf,
                &timestamp,
            )?;
        }

        let topic = self.client_id() + "/" + interface_name.trim_matches('/') + interface_path;
        let qos = self
            .interfaces
            .read()
            .await
            .get_mqtt_reliability(interface_name, &interface_path.try_into()?);

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
    ///     let mut device = AstarteDeviceSdk::new(&sdk_options).await.unwrap();
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
    ) -> Result<(), AstarteError>
    where
        T: AstarteAggregate,
    {
        self.send_object_with_timestamp_impl(interface_name, interface_path, data, Some(timestamp))
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
    ) -> Result<(), AstarteError>
    where
        T: AstarteAggregate,
    {
        self.send_object_with_timestamp_impl(interface_name, interface_path, data, None)
            .await
    }
}

impl fmt::Debug for AstarteDeviceSdk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Device")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            .field("mqtt_options", &self.mqtt_options)
            .finish()
    }
}

mod utils {
    pub(crate) fn extract_set_properties(bdata: &[u8]) -> Vec<String> {
        use flate2::read::ZlibDecoder;
        use std::io::prelude::*;

        let mut d = ZlibDecoder::new(&bdata[4..]);
        let mut s = String::new();
        d.read_to_string(&mut s).unwrap();

        s.split(';').map(|x| x.to_owned()).collect()
    }
}

#[cfg(test)]
mod test {
    use base64::Engine;
    use chrono::{TimeZone, Utc};
    use rumqttc::{Event, MqttOptions};
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::sync::{Mutex, RwLock};

    use crate::interfaces::Interfaces;
    use crate::{self as astarte_device_sdk, Interface};
    use astarte_device_sdk::interface::MappingType;
    use astarte_device_sdk::AstarteAggregate;
    use astarte_device_sdk::{types::AstarteType, Aggregation, AstarteDeviceSdk};
    use astarte_device_sdk_derive::astarte_aggregate;
    #[cfg(not(feature = "derive"))]
    use astarte_device_sdk_derive::AstarteAggregate;

    use super::{AsyncClient, EventLoop};

    fn mock_astarte<I>(client: AsyncClient, eventloop: EventLoop, interfaces: I) -> AstarteDeviceSdk
    where
        I: IntoIterator<Item = Interface>,
    {
        AstarteDeviceSdk {
            realm: "realm".to_string(),
            device_id: "device_id".to_string(),
            mqtt_options: MqttOptions::new("device_id", "localhost", 1883),
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

    #[test]
    fn test_individual_serialization() {
        let alltypes: Vec<AstarteType> = vec![
            AstarteType::Double(4.5),
            AstarteType::Integer(-4),
            AstarteType::Boolean(true),
            AstarteType::LongInteger(45543543534_i64),
            AstarteType::String("hello".into()),
            AstarteType::BinaryBlob(b"hello".to_vec()),
            AstarteType::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteType::DoubleArray(vec![1.2, 3.4, 5.6, 7.8]),
            AstarteType::IntegerArray(vec![1, 3, 5, 7]),
            AstarteType::BooleanArray(vec![true, false, true, true]),
            AstarteType::LongIntegerArray(vec![45543543534_i64, 45543543535_i64, 45543543536_i64]),
            AstarteType::StringArray(vec!["hello".to_owned(), "world".to_owned()]),
            AstarteType::BinaryBlobArray(vec![b"hello".to_vec(), b"world".to_vec()]),
            AstarteType::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]),
        ];

        for ty in alltypes {
            println!("checking {ty:?}");

            let buf = AstarteDeviceSdk::serialize_individual(ty.clone(), None).unwrap();

            let ty2 = AstarteDeviceSdk::deserialize(&buf).unwrap();

            if let Aggregation::Individual(data) = ty2 {
                assert!(ty == data);
            } else {
                panic!();
            }
        }
    }

    #[test]
    fn test_serialize_object() {
        let alltypes: Vec<AstarteType> = vec![
            AstarteType::Double(4.5),
            AstarteType::Integer(-4),
            AstarteType::Boolean(true),
            AstarteType::LongInteger(45543543534_i64),
            AstarteType::String("hello".into()),
            AstarteType::BinaryBlob(b"hello".to_vec()),
            AstarteType::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteType::DoubleArray(vec![1.2, 3.4, 5.6, 7.8]),
            AstarteType::IntegerArray(vec![1, 3, 5, 7]),
            AstarteType::BooleanArray(vec![true, false, true, true]),
            AstarteType::LongIntegerArray(vec![45543543534_i64, 45543543535_i64, 45543543536_i64]),
            AstarteType::StringArray(vec!["hello".to_owned(), "world".to_owned()]),
            AstarteType::BinaryBlobArray(vec![b"hello".to_vec(), b"world".to_vec()]),
            AstarteType::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]),
        ];

        let allendpoints = vec![
            "double".to_string(),
            "integer".to_string(),
            "boolean".to_string(),
            "longinteger".to_string(),
            "string".to_string(),
            "binaryblob".to_string(),
            "datetime".to_string(),
            "doublearray".to_string(),
            "integerarray".to_string(),
            "booleanarray".to_string(),
            "longintegerarray".to_string(),
            "stringarray".to_string(),
            "binaryblobarray".to_string(),
            "datetimearray".to_string(),
        ];

        let mut data = std::collections::HashMap::new();

        for i in allendpoints.iter().zip(alltypes.iter()) {
            data.insert(i.0.clone(), i.1.clone());
        }

        let bytes = AstarteDeviceSdk::serialize_object(data.clone(), None).unwrap();

        let data_processed = AstarteDeviceSdk::deserialize(&bytes).unwrap();

        println!("\nComparing {data:?}\nto {data_processed:?}");

        if let Aggregation::Object(data_processed) = data_processed {
            assert_eq!(data, data_processed);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_deflate() {
        let example = b"com.example.MyInterface/some/path;org.example.DraftInterface/otherPath";

        let bdata: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x46, 0x78, 0x9c, 0x4b, 0xce, 0xcf, 0xd5, 0x4b, 0xad, 0x48, 0xcc,
            0x2d, 0xc8, 0x49, 0xd5, 0xf3, 0xad, 0xf4, 0xcc, 0x2b, 0x49, 0x2d, 0x4a, 0x4b, 0x4c,
            0x4e, 0xd5, 0x2f, 0xce, 0xcf, 0x4d, 0xd5, 0x2f, 0x48, 0x2c, 0xc9, 0xb0, 0xce, 0x2f,
            0x4a, 0x87, 0xab, 0x70, 0x29, 0x4a, 0x4c, 0x2b, 0x41, 0x28, 0xca, 0x2f, 0xc9, 0x48,
            0x2d, 0x0a, 0x00, 0x2a, 0x02, 0x00, 0xb2, 0x0c, 0x1a, 0xc9,
        ];

        let s = astarte_device_sdk::utils::extract_set_properties(&bdata);

        assert!(s.join(";").as_bytes() == example);
    }

    #[test]
    fn test_integer_longinteger_compatibility() {
        let integer_buf =
            AstarteDeviceSdk::deserialize(&[12, 0, 0, 0, 16, 118, 0, 16, 14, 0, 0, 0]).unwrap();
        if let Aggregation::Individual(astarte_type) = integer_buf {
            assert_eq!(astarte_type, MappingType::LongInteger);
        } else {
            panic!("Deserialization in not individual");
        }
    }

    #[test]
    fn test_bson_serialization() {
        let og_value: i64 = 3600;
        let buf = AstarteDeviceSdk::serialize_individual(og_value, None).unwrap();
        if let Aggregation::Individual(astarte_type) = AstarteDeviceSdk::deserialize(&buf).unwrap()
        {
            assert_eq!(astarte_type, AstarteType::LongInteger(3600));
            if let AstarteType::LongInteger(value) = astarte_type {
                assert_eq!(value, 3600);
            } else {
                panic!("Astarte Type is not LongInteger");
            }
        } else {
            panic!("Deserialization in not individual");
        }
    }

    #[tokio::test]
    async fn wait_for_connack() {
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
            Interface::from_str(include_str!("../examples/object_datastream/interfaces/org.astarte-platform.rust.examples.object-datastream.DeviceDatastream.json")).unwrap(),
            Interface::from_str(include_str!("../examples/individual_datastream/interfaces/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream.json")).unwrap()
        ];

        let mut astarte = mock_astarte(client, eventloope, interfaces);

        astarte.wait_for_connack().await.unwrap();
    }
}
