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

pub mod builder;
mod crypto;
pub mod database;
mod interface;
mod interfaces;
mod pairing;
pub mod registration;
pub mod types;

use bson::{to_document, Bson};
use builder::AstarteOptions;
use database::AstarteDatabase;
use database::StoredProp;
use itertools::Itertools;
use log::{debug, error, info, trace};
use rumqttc::{AsyncClient, Event};
use rumqttc::{EventLoop, MqttOptions};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{self, Debug};
use std::sync::Arc;
use types::AstarteType;

pub use interface::Interface;
pub use interfaces::Interfaces;

/// Astarte client
#[derive(Clone)]
pub struct AstarteSdk {
    realm: String,
    device_id: String,
    mqtt_options: MqttOptions,
    client: AsyncClient,
    eventloop: Arc<tokio::sync::Mutex<EventLoop>>,
    interfaces: Arc<interfaces::Interfaces>,
    database: Option<Arc<dyn AstarteDatabase + Sync + Send>>,
}

#[derive(thiserror::Error, Debug)]
pub enum AstarteError {
    #[error("configuration error")]
    ConfigError(String),

    #[error("pairing error")]
    PairingError(#[from] pairing::PairingError),

    #[error("bson serialize error")]
    BsonSerError(#[from] bson::ser::Error),

    #[error("bson client error")]
    BsonClientError(#[from] rumqttc::ClientError),

    #[error("mqtt connection error")]
    ConnectionError(#[from] rumqttc::ConnectionError),

    #[error("malformed input from Astarte backend")]
    DeserializationError,

    #[error("error converting from Bson to AstarteType")]
    FromBsonError,

    #[error("type mismatch in bson array from astarte, something has gone very wrong here")]
    FromBsonArrayError,

    #[error("forbidden floating point number")]
    FloatError,

    #[error("error creating interface")]
    InterfaceError(#[from] interface::Error),

    #[error("send error")]
    SendError(String),

    #[error("receive error")]
    ReceiveError(String),

    #[error("database error")]
    DbError(#[from] sqlx::Error),

    #[error("io error")]
    IoError(#[from] std::io::Error),

    #[error("generic error")]
    Reported(String),

    #[error("generic error")]
    Unreported,
}

#[derive(Debug)]
pub enum Aggregation {
    Individual(AstarteType),
    Object(HashMap<String, AstarteType>),
}

/// data from astarte to device
#[derive(Debug)]
pub struct Clientbound {
    pub interface: String,
    pub path: String,
    pub data: Aggregation,
}

fn parse_topic(topic: &str) -> Option<(String, String, String, String)> {
    let mut parts = topic.split('/');

    let realm = parts.next()?.to_owned();
    let device = parts.next()?.to_owned();
    let interface = parts.next()?.to_owned();
    let path = String::from("/") + &parts.join("/");
    Some((realm, device, interface, path))
}

impl AstarteSdk {
    pub async fn new(opts: &AstarteOptions) -> Result<AstarteSdk, AstarteError> {
        let mqtt_options = pairing::get_transport_config(opts).await?;

        // TODO: make cap configurable
        let (client, eventloop) = AsyncClient::new(mqtt_options.clone(), 50);

        let mut device = AstarteSdk {
            realm: opts.realm.to_owned(),
            device_id: opts.device_id.to_owned(),
            mqtt_options,
            client,
            eventloop: Arc::new(tokio::sync::Mutex::new(eventloop)),
            interfaces: Arc::new(opts.interfaces.clone()),
            database: opts.database.clone(),
        };

        device.subscribe().await?;

        device.poll_connack().await?;

        Ok(device)
    }

    async fn subscribe(&self) -> Result<(), AstarteError> {
        let cn = format!("{}/{}", self.realm, self.device_id);

        let ifaces = self
            .interfaces
            .interfaces
            .clone()
            .into_iter()
            .filter(|i| i.1.get_ownership() == interface::Ownership::Server);

        self.client
            .subscribe(
                cn.to_owned() + "/control/consumer/properties",
                rumqttc::QoS::ExactlyOnce,
            )
            .await?;

        for i in ifaces {
            self.client
                .subscribe(
                    cn.to_owned() + "/" + interface::traits::Interface::name(&i.1) + "/#",
                    rumqttc::QoS::ExactlyOnce,
                )
                .await?;
        }

        Ok(())
    }

    async fn poll_connack(&mut self) -> Result<(), AstarteError> {
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
            self.send_introspection().await?;
            self.send_emptycache().await?;
            self.send_device_owned_properties().await?;
            info!("connack done");
        }
        Ok(())
    }
    pub async fn update_interfaces(
        &mut self,
        interfaces: interfaces::Interfaces,
    ) -> Result<(), AstarteError> {
        self.interfaces = Arc::new(interfaces);
        self.subscribe().await?;
        self.send_introspection().await?;

        Ok(())
    }

    /// Poll updates from mqtt, this is where you receive data
    /// ```no_run
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut sdk_options = astarte_sdk::builder::AstarteOptions::new("_","_","_","_")
    ///                           .build();
    ///     let mut d = astarte_sdk::AstarteSdk::new(&sdk_options).await.unwrap();
    ///
    ///     loop {
    ///         if let Ok(data) = d.poll().await {
    ///             println!("incoming: {:?}", data);
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn poll(&mut self) -> Result<Clientbound, AstarteError> {
        loop {
            // keep consuming and processing packets until we have data for the user
            match self.eventloop.lock().await.poll().await? {
                Event::Incoming(i) => {
                    trace!("MQTT Incoming = {:?}", i);

                    match i {
                        rumqttc::Packet::ConnAck(p) => {
                            self.connack(p).await?;
                        }
                        rumqttc::Packet::Publish(p) => {
                            let topic = parse_topic(&p.topic);

                            if let Some((_, _, interface, path)) = topic {
                                let bdata = p.payload.to_vec();

                                if interface == "control" && path == "/consumer/properties" {
                                    self.purge_properties(bdata).await?;
                                    continue;
                                }

                                debug!("Incoming publish = {} {:?}", p.topic, bdata);

                                if let Some(database) = &self.database {
                                    //if database is loaded

                                    if let Some(major_version) =
                                        self.interfaces.get_property_major(&interface, &path)
                                    //if it's a property
                                    {
                                        database
                                            .store_prop(&interface, &path, &bdata, major_version)
                                            .await?;

                                        if cfg!(debug_assertions) {
                                            // database selftest / sanity check for debug builds
                                            let original = crate::AstarteSdk::deserialize(&bdata)?;
                                            if let Aggregation::Individual(data) = original {
                                                let db = database
                                                .load_prop(&interface, &path, major_version)
                                                .await
                                                .expect("load_prop failed")
                                                .expect(
                                                    "property wasn't correctly saved in the database",
                                                );
                                                assert!(data == db);
                                                let prop = self
                                                .get_property(&interface, &path)
                                                .await?
                                                .expect(
                                                "property wasn't correctly saved in the database",
                                            );
                                                assert!(data == prop);
                                                trace!("database test ok");
                                            } else {
                                                panic!("This should be impossible, can't have object properties");
                                            }
                                        }
                                    }
                                }

                                if cfg!(debug_assertions) {
                                    self.interfaces
                                        .validate_receive(&interface, &path, &bdata)?;
                                }

                                let data = AstarteSdk::deserialize(&bdata)?;
                                return Ok(Clientbound {
                                    interface,
                                    path,
                                    data,
                                });
                            }
                        }
                        _ => {}
                    }
                }
                Event::Outgoing(o) => trace!("MQTT Outgoing = {:?}", o),
            }
        }
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
        let introspection = self.interfaces.get_introspection_string();

        debug!("sending introspection = {}", introspection);

        self.client
            .publish(
                self.client_id(),
                rumqttc::QoS::ExactlyOnce,
                false,
                introspection.clone(),
            )
            .await?;
        Ok(())
    }

    async fn send_device_owned_properties(&self) -> Result<(), AstarteError> {
        if let Some(database) = &self.database {
            let properties = database.load_all_props().await?;
            // publish only device-owned properties...
            let device_owned_properties: Vec<StoredProp> = properties
                .into_iter()
                .filter(|prop| {
                    self.interfaces.get_ownership(&prop.interface)
                        == Some(crate::interface::Ownership::Device)
                })
                .collect();
            for prop in device_owned_properties {
                let topic = format!("{}/{}{}", self.client_id(), prop.interface, prop.path);
                if let Some(version_major) = self
                    .interfaces
                    .get_property_major(&prop.interface, &prop.path)
                {
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

    /// unset a device property
    pub async fn unset<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
    ) -> Result<(), AstarteError>
    where
        D: Into<AstarteType>,
    {
        trace!("unsetting {} {}", interface_name, interface_path);

        if cfg!(debug_assertions) {
            self.interfaces
                .validate_send(interface_name, interface_path, &[], &None)?;
        }

        self.send_with_timestamp_impl(interface_name, interface_path, AstarteType::Unset, None)
            .await?;

        Ok(())
    }

    /// Serialize data directly from Bson
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

    /// get property from database, if present
    pub async fn get_property(
        &self,
        interface: &str,
        path: &str,
    ) -> Result<Option<AstarteType>, AstarteError> {
        if let Some(database) = &self.database {
            if let Some(major) = self.interfaces.get_property_major(interface, path) {
                let prop = database.load_prop(interface, path, major).await?;
                return Ok(prop);
            }
        }

        Ok(None)
    }

    // ------------------------------------------------------------------------
    // individual types
    // ------------------------------------------------------------------------

    /// Send data to an astarte interface
    /// ```no_run
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut sdk_options = astarte_sdk::builder::AstarteOptions::new("_","_","_","_")
    ///                           .build();
    ///     let mut d = astarte_sdk::AstarteSdk::new(&sdk_options).await.unwrap();
    ///
    ///     d.send("com.test.interface", "/data", 45).await.unwrap();
    /// }
    /// ```

    pub async fn send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), AstarteError>
    where
        D: Into<AstarteType>,
    {
        self.send_with_timestamp_impl(interface_name, interface_path, data, None)
            .await
    }

    /// Send data to an astarte interface, with timestamp
    /// ```no_run
    /// #[tokio::main]
    /// async fn main() {
    ///     use chrono::Utc;
    ///     use chrono::TimeZone;
    ///     let mut sdk_options = astarte_sdk::builder::AstarteOptions::new("_","_","_","_")
    ///                           .build();
    ///     let mut d = astarte_sdk::AstarteSdk::new(&sdk_options).await.unwrap();
    ///
    ///     d.send_with_timestamp("com.test.interface", "/data", 45, Utc.timestamp(1537449422, 0) ).await.unwrap();
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
        D: Into<AstarteType>,
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
        D: Into<AstarteType>,
    {
        debug!("sending {} {}", interface_name, interface_path);

        let data: AstarteType = data.into();

        let buf = AstarteSdk::serialize_individual(data.clone(), timestamp)?;

        if cfg!(debug_assertions) {
            self.interfaces
                .validate_send(interface_name, interface_path, &buf, &timestamp)?;
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
                    .get_mqtt_reliability(interface_name, interface_path),
                false,
                buf,
            )
            .await?;

        // we store the property in the database after it has been successfully sent
        self.store_property_on_send(interface_name, interface_path, data)
            .await?;
        Ok(())
    }

    /// checks if a property mapping has alredy been sent, so we don't have to send the same thing again
    /// returns true if property was already sent
    async fn check_property_on_send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<bool, AstarteError>
    where
        D: Into<AstarteType>,
    {
        if let Some(db) = &self.database {
            //if database is present

            let data: AstarteType = data.into();

            let mapping = self
                .interfaces
                .get_mapping(interface_name, interface_path)
                .ok_or_else(|| AstarteError::SendError("Mapping doesn't exist".into()))?;

            if let crate::interface::Mapping::Properties(_) = mapping {
                //if mapping is a property
                let db_data = db.load_prop(interface_name, interface_path, 0).await?;

                if let Some(db_data) = db_data {
                    // if already in db
                    if db_data == data {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    async fn store_property_on_send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<bool, AstarteError>
    where
        D: Into<AstarteType>,
    {
        if let Some(db) = &self.database {
            //if database is present

            let data: AstarteType = data.into();

            let mapping = self
                .interfaces
                .get_mapping(interface_name, interface_path)
                .ok_or_else(|| AstarteError::SendError("Mapping doesn't exist".into()))?;

            if let crate::interface::Mapping::Properties(_) = mapping {
                //if mapping is a property
                let bin = AstarteSdk::serialize_individual(data, None)?;
                db.store_prop(interface_name, interface_path, &bin, 0)
                    .await?;
                debug!("Stored new property in database");
            }
        }

        Ok(false)
    }

    /// Serialize an astarte type into a vec of bytes
    fn serialize_individual<D>(
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError>
    where
        D: Into<AstarteType>,
    {
        AstarteSdk::serialize(data.into().into(), timestamp)
    }

    // ------------------------------------------------------------------------
    // object types
    // ------------------------------------------------------------------------

    /// helper function to convert from an HashMap of AstarteType to an HashMap of Bson
    pub fn to_bson_map(data: HashMap<&str, AstarteType>) -> HashMap<&str, Bson> {
        data.into_iter().map(|f| (f.0, f.1.into())).collect()
    }

    /// Serialize a group of astarte types to a vec of bytes, representing an object
    fn serialize_object<T>(
        data: T,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError>
    where
        T: serde::Serialize,
    {
        let doc = to_document(&data)?;

        AstarteSdk::serialize(Bson::Document(doc), timestamp)
    }

    async fn send_object_with_timestamp_impl<T>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: T,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), AstarteError>
    where
        T: serde::Serialize,
    {
        let buf = AstarteSdk::serialize_object(data, timestamp)?;

        if cfg!(debug_assertions) {
            self.interfaces
                .validate_send(interface_name, interface_path, &buf, &timestamp)?;
        }

        self.client
            .publish(
                self.client_id() + "/" + interface_name.trim_matches('/') + interface_path,
                self.interfaces
                    .get_mqtt_reliability(interface_name, interface_path),
                false,
                buf,
            )
            .await?;

        Ok(())
    }

    /// Send data to an object interface. with timestamp
    pub async fn send_object_with_timestamp<T>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: T,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), AstarteError>
    where
        T: serde::Serialize,
    {
        self.send_object_with_timestamp_impl(interface_name, interface_path, data, Some(timestamp))
            .await
    }

    /// Send data to an object interface. with timestamp
    pub async fn send_object<T>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: T,
    ) -> Result<(), AstarteError>
    where
        T: serde::Serialize,
    {
        self.send_object_with_timestamp_impl(interface_name, interface_path, data, None)
            .await
    }
}

impl fmt::Debug for AstarteSdk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Device")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            //.field("credentials_secret", &self.credentials_secret)
            //.field("pairing_url", &self.pairing_url)
            .field("mqtt_options", &self.mqtt_options)
            .finish()
    }
}

mod utils {
    pub fn extract_set_properties(bdata: &[u8]) -> Vec<String> {
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
    use chrono::{TimeZone, Utc};

    use crate::{types::AstarteType, AstarteSdk};

    fn do_vecs_match(a: &[u8], b: &[u8]) -> bool {
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();

        println!("matching {:?}\nwith     {:?}\n", a, b);
        matching == a.len() && matching == b.len()
    }

    #[test]
    fn serialize_individual() {
        assert!(do_vecs_match(
            &AstarteSdk::serialize_individual(false, None).unwrap(),
            &[0x09, 0x00, 0x00, 0x00, 0x08, 0x76, 0x00, 0x00, 0x00]
        ));
        assert!(do_vecs_match(
            &AstarteSdk::serialize_individual(AstarteType::Double(16.73), None).unwrap(),
            &[
                0x10, 0x00, 0x00, 0x00, 0x01, 0x76, 0x00, 0x7b, 0x14, 0xae, 0x47, 0xe1, 0xba, 0x30,
                0x40, 0x00
            ]
        ));
        assert!(do_vecs_match(
            &AstarteSdk::serialize_individual(
                AstarteType::Double(16.73),
                Some(Utc.timestamp(1537449422, 890000000))
            )
            .unwrap(),
            &[
                0x1b, 0x00, 0x00, 0x00, 0x09, 0x74, 0x00, 0x2a, 0x70, 0x20, 0xf7, 0x65, 0x01, 0x00,
                0x00, 0x01, 0x76, 0x00, 0x7b, 0x14, 0xae, 0x47, 0xe1, 0xba, 0x30, 0x40, 0x00
            ]
        ));
    }

    #[test]
    fn test_parse_topic() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/com.interface.test/led/red".to_owned();
        let (realm, device, interface, path) = crate::parse_topic(&topic).unwrap();
        assert!(realm == "test");
        assert!(device == "u-WraCwtK_G_fjJf63TiAw");
        assert!(interface == "com.interface.test");
        assert!(path == "/led/red");
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

        let s = crate::utils::extract_set_properties(&bdata);

        assert!(s.join(";").as_bytes() == example);
    }
}
