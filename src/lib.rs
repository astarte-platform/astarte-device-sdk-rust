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
mod mqtt_client;
mod pairing;
pub mod registration;
pub mod types;

use bson::Bson;
use builder::AstarteOptions;
use database::AstarteDatabase;
use database::StoredProp;
use itertools::Itertools;
use log::{debug, error, info, trace};
use rumqttc::Event;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::Receiver as MpscReceiver;
use types::AstarteType;

use crate::mqtt_client::*;
use crate::private::ISubject as privISubject;
pub use interface::Interface;

pub trait IObserver {
    fn update(&self, clientbound: &Clientbound);
}

pub trait ISubject<'a, T: IObserver> {
    fn attach(&mut self, observer: &'a T);
    fn detach(&mut self, observer: &'a T);
}
pub(crate) mod private {
    use crate::{Clientbound, IObserver};

    pub trait ISubject<'a, T: IObserver> {
        fn notify_observers(&self, clientbound: &Clientbound);
    }
}

/// Astarte client
#[derive(Clone)]
pub struct AstarteSdk<'a, T: IObserver> {
    realm: String,
    device_id: String,
    interfaces: interfaces::Interfaces,
    database: Option<Arc<dyn AstarteDatabase + Sync + Send>>,
    mqtt_outbound_channel: Sender<MqttOutboundType>,
    observers: Vec<&'a T>,
}

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

    #[error("error converting from Bson to AstarteType")]
    FromBsonError(String),

    #[error("type mismatch in bson array from astarte, something has gone very wrong here")]
    FromBsonArrayError,

    #[error("forbidden floating point number")]
    FloatError,

    #[error("send error")]
    SendError(String),

    #[error("receive error")]
    ReceiveError(String),

    #[error("database error")]
    DbError(#[from] sqlx::Error),

    #[error("builder error")]
    BuilderError(#[from] builder::AstarteBuilderError),

    #[error("generic error")]
    Reported(String),

    #[error("generic error")]
    Unreported,
}

#[derive(Debug, Clone)]
pub enum Aggregation {
    Individual(AstarteType),
    Object(HashMap<String, AstarteType>),
}

/// data from astarte to device
#[derive(Debug, Clone)]
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

impl<T: IObserver + Clone + PartialEq + std::marker::Sync> AstarteSdk<'static, T> {
    pub async fn new(opts: &AstarteOptions) -> Result<AstarteSdk<'static, T>, AstarteError>
    where
        T: 'static,
    {
        let mqtt_client = MQTTClient::new(opts).await?;

        let device = AstarteSdk {
            realm: opts.realm.to_owned(),
            device_id: opts.device_id.to_owned(),
            interfaces: interfaces::Interfaces::new(opts.interfaces.clone()),
            database: opts.database.clone(),
            mqtt_outbound_channel: mqtt_client.outbound_tx_channel,
            observers: Vec::new(),
        };

        let _ = device
            .start_mqtt_event_handler(mqtt_client.inbound_rx_channel)
            .await;

        Ok(device)
    }

    async fn subscribe(&self) -> Result<(), AstarteError> {
        let ifaces = self
            .interfaces
            .interfaces
            .clone()
            .into_iter()
            .filter(|i| i.1.get_ownership() == interface::Ownership::Server);

        let _ = self
            .mqtt_outbound_channel
            .send(MqttOutboundType::Subscription(
                self.client_id() + "/control/consumer/properties",
            ));

        for i in ifaces {
            let _ = self
                .mqtt_outbound_channel
                .send(MqttOutboundType::Subscription(
                    self.client_id() + "/" + interface::traits::Interface::name(&i.1) + "/#",
                ));
        }

        Ok(())
    }

    async fn connack(&self, p: rumqttc::ConnAck) -> Result<(), AstarteError> {
        if !p.session_present {
            self.subscribe().await?;
            self.send_introspection().await?;
            self.send_emptycache().await?;
            self.send_device_owned_properties().await?;
            self.mqtt_outbound_channel.send(MqttOutboundType::Ready);
            info!("connack done");
        }

        Ok(())
    }

    async fn start_mqtt_event_handler(&self, mut inbound_rx_channel: MpscReceiver<Event>) {
        let self_clone = self.to_owned();
        tokio::spawn(async move {
            loop {
                match inbound_rx_channel.recv().await.unwrap() {
                    Event::Incoming(i) => {
                        trace!("MQTT Incoming = {:?}", i);

                        match i {
                            rumqttc::Packet::ConnAck(p) => {
                                let _ = self_clone.connack(p).await;
                            }
                            rumqttc::Packet::Publish(p) => {
                                let topic = parse_topic(&p.topic);

                                if let Some((_, _, interface, path)) = topic {
                                    let bdata = p.payload.to_vec();

                                    if interface == "control" && path == "/consumer/properties" {
                                        self_clone.purge_properties(bdata).await.unwrap();
                                        continue;
                                    }

                                    debug!("Incoming publish = {} {:?}", p.topic, bdata);

                                    if let Some(database) = &self_clone.database {
                                        //if database is loaded

                                        if let Some(major_version) = self_clone
                                            .interfaces
                                            .get_property_major(&interface, &path)
                                        //if it's a property
                                        {
                                            database
                                                .store_prop(
                                                    &interface,
                                                    &path,
                                                    &bdata,
                                                    major_version,
                                                )
                                                .await
                                                .unwrap();

                                            if cfg!(debug_assertions) {
                                                // database self test / sanity check for debug builds
                                                let original = utils::deserialize(&bdata).unwrap();
                                                if let Aggregation::Individual(data) = original {
                                                    let db = database
                                                    .load_prop(&interface, &path, major_version)
                                                    .await
                                                    .expect("load_prop failed")
                                                    .expect(
                                                        "property wasn't correctly saved in the database",
                                                    );
                                                    assert!(data == db);
                                                    let prop = self_clone
                                                    .get_property(&interface, &path)
                                                    .await.unwrap()
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
                                        self_clone
                                            .interfaces
                                            .validate_receive(&interface, &path, &bdata)
                                            .unwrap();
                                    }

                                    let data = utils::deserialize(&bdata).unwrap();
                                    self_clone.notify_observers(&Clientbound {
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
        });
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

        let _ = self
            .mqtt_outbound_channel
            .send(MqttOutboundType::EmptyCache(EmptyCacheMessage {
                client_id: self.client_id(),
            }));

        Ok(())
    }

    async fn send_introspection(&self) -> Result<(), AstarteError> {
        let introspection = self.interfaces.get_introspection_string();

        debug!("sending introspection = {}", introspection);

        let _ = self
            .mqtt_outbound_channel
            .send(MqttOutboundType::Introspection(IntrospectionMessage {
                client_id: self.client_id(),
                introspection,
            }));
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
                if let Some(_) = self
                    .interfaces
                    .get_property_major(&prop.interface, &prop.path)
                {
                    let msg = AstarteOutboundMessage {
                        client_id: self.client_id(),
                        interface: prop.interface,
                        path: prop.path,
                        buf: prop.value,
                        qos: rumqttc::QoS::ExactlyOnce,
                    };
                    let _ = self
                        .mqtt_outbound_channel
                        .send(MqttOutboundType::OutboundMessage(msg));
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
    ///
    /// use astarte_sdk::Clientbound;
    /// use astarte_sdk::ISubject;
    /// #[derive(Clone, PartialEq)]
    /// struct EventHandler{}
    /// impl astarte_sdk::IObserver for EventHandler{
    ///     fn update(&self, clientbound: &Clientbound) {
    ///         let data = clientbound.to_owned();
    ///         println!("incoming: {:?}", data);
    ///     }
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut sdk_options = astarte_sdk::builder::AstarteOptions::new("_","_","_","_")
    ///                           .build();
    ///     let mut d = astarte_sdk::AstarteSdk::new(&sdk_options).await.unwrap();
    ///     d.attach(&EventHandler{});
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

    /// Send data to an astarte interface
    /// ```no_run
    ///
    /// use astarte_sdk::Clientbound;
    /// use astarte_sdk::ISubject;
    /// use astarte_sdk::types::AstarteType;
    /// use chrono::Utc;
    /// use chrono::TimeZone;
    ///
    ///
    /// #[derive(Clone, PartialEq)]
    /// struct EventHandler{}
    /// impl astarte_sdk::IObserver for EventHandler{
    ///     fn update(&self, clientbound: &Clientbound) {
    ///         let data = clientbound.to_owned();
    ///         println!("incoming: {:?}", data);
    ///     }
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut sdk_options = astarte_sdk::builder::AstarteOptions::new("_","_","_","_")
    ///                           .build();
    ///     let mut d = astarte_sdk::AstarteSdk::new(&sdk_options).await.unwrap();
    ///     d.attach(&EventHandler{});
    ///
    ///     d.send_with_timestamp("com.test.interface", "/data", AstarteType::Integer(45), Utc.timestamp(1537449422, 0) ).await.unwrap();
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

        let buf = utils::serialize_individual(data.clone(), timestamp)?;

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

        let msg = AstarteOutboundMessage {
            client_id: self.client_id(),
            interface: interface_name.to_owned(),
            path: interface_path.to_owned(),
            buf: utils::serialize_individual(data.clone(), timestamp)?,
            qos: self
                .interfaces
                .get_mqtt_reliability(interface_name, interface_path),
        };
        let _ = self
            .mqtt_outbound_channel
            .send(MqttOutboundType::OutboundMessage(msg));

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
                .ok_or_else(|| {
                    AstarteError::SendError(format!("Mapping {} doesn't exist", interface_path))
                })?;

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
                let bin = utils::serialize_individual(data, None)?;
                db.store_prop(interface_name, interface_path, &bin, 0)
                    .await?;
                debug!("Stored new property in database");
            }
        }

        Ok(false)
    }

    async fn send_object_with_timestamp_impl<S>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: S,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), AstarteError>
    where
        S: serde::Serialize,
    {
        let buf = utils::serialize_object(data, timestamp)?;

        if cfg!(debug_assertions) {
            self.interfaces
                .validate_send(interface_name, interface_path, &buf, &timestamp)?;
        }

        let msg = AstarteOutboundMessage {
            client_id: self.client_id(),
            interface: interface_name.to_owned(),
            path: interface_path.to_owned(),
            buf,
            qos: self
                .interfaces
                .get_mqtt_reliability(interface_name, interface_path),
        };
        let _ = self
            .mqtt_outbound_channel
            .send(MqttOutboundType::OutboundMessage(msg));

        Ok(())
    }

    /// Send data to an object interface. with timestamp
    pub async fn send_object_with_timestamp<S>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: S,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), AstarteError>
    where
        S: serde::Serialize,
    {
        self.send_object_with_timestamp_impl(interface_name, interface_path, data, Some(timestamp))
            .await
    }

    /// Send data to an object interface. with timestamp
    pub async fn send_object<S>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: S,
    ) -> Result<(), AstarteError>
    where
        S: serde::Serialize,
    {
        self.send_object_with_timestamp_impl(interface_name, interface_path, data, None)
            .await
    }
}

impl<'a, T: IObserver + PartialEq> ISubject<'a, T> for AstarteSdk<'a, T> {
    fn attach(&mut self, observer: &'a T) {
        self.observers.push(observer);
    }
    fn detach(&mut self, observer: &'a T) {
        if let Some(idx) = self.observers.iter().position(|x| *x == observer) {
            self.observers.remove(idx);
        }
    }
}
impl<'a, T: IObserver + PartialEq> privISubject<'a, T> for AstarteSdk<'a, T> {
    fn notify_observers(&self, clientbound: &Clientbound) {
        for item in self.observers.iter() {
            item.update(clientbound);
        }
    }
}

impl<'a, T: IObserver + PartialEq> fmt::Debug for AstarteSdk<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Device")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            //.field("credentials_secret", &self.credentials_secret)
            //.field("pairing_url", &self.pairing_url)
            // .field("mqtt_options", &self.mqtt_options)
            .finish()
    }
}

mod utils {
    use crate::{Aggregation, AstarteError, AstarteType};
    use bson::{to_document, Bson};
    use log::trace;
    use std::collections::HashMap;
    use std::convert::TryInto;

    pub(crate) fn extract_set_properties(bdata: &[u8]) -> Vec<String> {
        use flate2::read::ZlibDecoder;
        use std::io::prelude::*;

        let mut d = ZlibDecoder::new(&bdata[4..]);
        let mut s = String::new();
        d.read_to_string(&mut s).unwrap();

        s.split(';').map(|x| x.to_owned()).collect()
    }

    /// Serialize data directly from Bson
    pub(crate) fn serialize(
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

    pub(crate) fn deserialize(bdata: &[u8]) -> Result<Aggregation, AstarteError> {
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

    /// Serialize a group of astarte types to a vec of bytes, representing an object
    pub(crate) fn serialize_object<S>(
        data: S,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError>
    where
        S: serde::Serialize,
    {
        let doc = to_document(&data)?;

        serialize(Bson::Document(doc), timestamp)
    }

    /// Serialize an astarte type into a vec of bytes
    pub(crate) fn serialize_individual<D>(
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError>
    where
        D: Into<AstarteType>,
    {
        serialize(data.into().into(), timestamp)
    }

    // ------------------------------------------------------------------------
    // object types
    // ------------------------------------------------------------------------

    /// helper function to convert from an HashMap of AstarteType to an HashMap of Bson
    pub fn to_bson_map(data: HashMap<&str, AstarteType>) -> HashMap<&str, Bson> {
        data.into_iter().map(|f| (f.0, f.1.into())).collect()
    }
}

#[cfg(test)]
mod test {
    use chrono::{TimeZone, Utc};

    use crate::interface::MappingType;
    use crate::{types::AstarteType, utils, Aggregation, AstarteSdk};

    fn do_vecs_match(a: &[u8], b: &[u8]) -> bool {
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();

        println!("matching {:?}\nwith     {:?}\n", a, b);
        matching == a.len() && matching == b.len()
    }

    #[test]
    fn serialize_individual() {
        assert!(do_vecs_match(
            &utils::serialize_individual(false, None).unwrap(),
            &[0x09, 0x00, 0x00, 0x00, 0x08, 0x76, 0x00, 0x00, 0x00]
        ));
        assert!(do_vecs_match(
            &utils::serialize_individual(AstarteType::Double(16.73), None).unwrap(),
            &[
                0x10, 0x00, 0x00, 0x00, 0x01, 0x76, 0x00, 0x7b, 0x14, 0xae, 0x47, 0xe1, 0xba, 0x30,
                0x40, 0x00
            ]
        ));
        assert!(do_vecs_match(
            &utils::serialize_individual(
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

    #[test]
    fn test_integer_longinteger_compatibility() {
        let integer_buf = utils::deserialize(&[12, 0, 0, 0, 16, 118, 0, 16, 14, 0, 0, 0]).unwrap();
        if let Aggregation::Individual(astarte_type) = integer_buf {
            assert_eq!(astarte_type, MappingType::LongInteger);
        } else {
            panic!("Deserialization in not individual");
        }
    }

    #[test]
    fn test_bson_serialization() {
        let og_value: i64 = 3600;
        let buf = utils::serialize_individual(og_value, None).unwrap();
        if let Aggregation::Individual(astarte_type) = utils::deserialize(&buf).unwrap() {
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
}
