#![doc = include_str!("../README.md")]

mod crypto;
mod database;
mod interface;
mod pairing;
pub mod types;

use bson::{to_document, Bson};
use crypto::Bundle;
use database::AstarteDatabase;
use itertools::Itertools;
use log::{debug, trace};
use openssl::error::ErrorStack;
use pairing::PairingError;
use rumqttc::EventLoop;
use rumqttc::{AsyncClient, ClientConfig, Event, MqttOptions, Transport};
use rustls::{internal::pemfile, Certificate, PrivateKey};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use types::AstarteType;
use url::Url;

use interface::traits::Interface as InterfaceTrait;
pub use interface::Interface;

use crate::database::Database;
use crate::interface::Ownership;

/// Astarte client
#[derive(Clone)]
pub struct AstarteSdk {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
    build_options: BuildOptions,
    client: AsyncClient,
    eventloop: Arc<tokio::sync::Mutex<EventLoop>>,
    interfaces: HashMap<String, Interface>,
    database: database::Database,
}

/// Builder for Astarte client
///
/// ```
/// use astarte_sdk::AstarteOptions;
///
/// let realm = "test";
/// let device_id = "xxxxxxxxxxxxxxxxxxxxxxx";
/// let credentials_secret = "xxxxxxxxxxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxxxx";
/// let pairing_url = "https://api.example.com/pairing";
///
/// let mut sdk_options = AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url);
///
/// sdk_options.add_interface_files("path/to/interfaces");
///
///
/// ```

pub struct AstarteOptions {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
    interfaces: HashMap<String, Interface>,
    build_options: Option<BuildOptions>,
}

#[derive(thiserror::Error, Debug)]
pub enum AstarteBuilderError {
    #[error("private key or CSR creation failed")]
    CryptoGeneration(#[from] ErrorStack),

    #[error("device must have at least an interface")]
    MissingInterfaces,

    #[error("error creating interface")]
    InterfaceError(#[from] interface::Error),

    #[error("io error")]
    IoError(#[from] std::io::Error),

    #[error("configuration error")]
    ConfigError(String),

    #[error("mqtt error")]
    MqttError(#[from] rumqttc::ClientError),

    #[error("pairing error")]
    PairingError(#[from] PairingError),

    #[error("database error")]
    DbError(#[from] sqlx::Error),
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
    FromBsonError,

    #[error("type mismatch in bson array from astarte, something has gone very wrong here")]
    FromBsonArrayError,

    #[error("database error")]
    DbError(#[from] sqlx::Error),

    #[error("generic error")]
    Unreported,
}

#[derive(Debug, Clone)]
struct BuildOptions {
    private_key: PrivateKey,
    csr: String,
    certificate_pem: Vec<Certificate>,
    broker_url: Url,
    mqtt_opts: MqttOptions,
}

impl AstarteOptions {
    pub fn new(realm: &str, device_id: &str, credentials_secret: &str, pairing_url: &str) -> Self {
        AstarteOptions {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            interfaces: HashMap::new(),
            build_options: None,
        }
    }

    /// Add an interface from a json file
    pub fn add_interface_file(
        &mut self,
        file_path: &Path,
    ) -> Result<&mut Self, AstarteBuilderError> {
        let interface = Interface::from_file(file_path)?;
        let name = interface.name();
        debug!("Added interface {}", name);
        self.interfaces.insert(name.to_owned(), interface);
        Ok(self)
    }

    /// Add all json interface description inside a specified directory
    pub fn add_interface_files(
        &mut self,
        interfaces_directory: &str,
    ) -> Result<&mut Self, AstarteBuilderError> {
        let interface_files = std::fs::read_dir(Path::new(interfaces_directory))?;
        let it = interface_files.filter_map(Result::ok).filter(|f| {
            if let Some(ext) = f.path().extension() {
                ext == "json"
            } else {
                false
            }
        });

        for f in it {
            self.add_interface_file(&f.path())?;
        }

        Ok(self)
    }

    async fn populate_credentials(&mut self, csr: &str) -> Result<Vec<Certificate>, PairingError> {
        let cert_pem = pairing::fetch_credentials(self, csr).await?;
        let mut cert_pem_bytes = cert_pem.as_bytes();
        let certs =
            pemfile::certs(&mut cert_pem_bytes).map_err(|_| PairingError::InvalidCredentials)?;
        Ok(certs)
    }

    async fn populate_broker_url(&mut self) -> Result<Url, PairingError> {
        let broker_url = pairing::fetch_broker_url(self).await?;
        let parsed_broker_url = Url::parse(&broker_url)?;
        Ok(parsed_broker_url)
    }

    fn build_mqtt_opts(
        &self,
        certificate_pem: &[Certificate],
        broker_url: &Url,
        private_key: &PrivateKey,
    ) -> Result<MqttOptions, AstarteBuilderError> {
        let AstarteOptions {
            realm, device_id, ..
        } = self;

        let client_id = format!("{}/{}", realm, device_id);
        let host = broker_url
            .host_str()
            .ok_or_else(|| AstarteBuilderError::ConfigError("bad broker url".into()))?;
        let port = broker_url
            .port()
            .ok_or_else(|| AstarteBuilderError::ConfigError("bad broker url".into()))?;
        let mut tls_client_config = ClientConfig::new();
        tls_client_config.root_store = rustls_native_certs::load_native_certs().map_err(|_| {
            AstarteBuilderError::ConfigError("could not load platform certs".into())
        })?;
        tls_client_config
            .set_single_client_cert(certificate_pem.to_owned(), private_key.to_owned())
            .map_err(|_| AstarteBuilderError::ConfigError("cannot setup client auth".into()))?;

        let mut mqtt_opts = MqttOptions::new(client_id, host, port);
        mqtt_opts.set_keep_alive(30);

        mqtt_opts.set_transport(Transport::tls_with_config(tls_client_config.into()));

        Ok(mqtt_opts)
    }

    async fn subscribe(
        &mut self,
        client: &AsyncClient,
        cn: &str,
    ) -> Result<(), AstarteBuilderError> {
        let ifaces = self
            .interfaces
            .clone()
            .into_iter()
            .filter(|i| i.1.get_ownership() == Ownership::Server);

        client
            .subscribe(
                cn.to_owned() + "/control/consumer/properties",
                rumqttc::QoS::AtLeastOnce,
            )
            .await?;

        for i in ifaces {
            client
                .subscribe(
                    cn.to_owned() + "/" + i.1.name() + "/#",
                    rumqttc::QoS::AtLeastOnce,
                )
                .await?;
        }

        Ok(())
    }

    /// build Astarte client, call this before `connect`
    pub async fn build(&mut self) -> Result<(), AstarteBuilderError> {
        let cn = format!("{}/{}", self.realm, self.device_id);

        if self.interfaces.is_empty() {
            return Err(AstarteBuilderError::MissingInterfaces);
        }

        let Bundle(pkey_bytes, csr_bytes) = Bundle::new(&cn)?;

        let private_key = pemfile::pkcs8_private_keys(&mut pkey_bytes.as_slice())
            .map_err(|_| AstarteBuilderError::ConfigError("failed pkcs8 key extraction".into()))?
            .remove(0);

        let csr = String::from_utf8(csr_bytes)
            .map_err(|_| AstarteBuilderError::ConfigError("bad csr bytes format".into()))?;

        let certificate_pem = self.populate_credentials(&csr).await?;

        let broker_url = self.populate_broker_url().await?;

        let mqtt_opts = self.build_mqtt_opts(&certificate_pem, &broker_url, &private_key)?;

        self.build_options = Some(BuildOptions {
            private_key,
            csr,
            certificate_pem,
            broker_url,
            mqtt_opts,
        });

        Ok(())
    }

    /// Creates and connects an Astarte client
    pub async fn connect(&mut self) -> Result<AstarteSdk, AstarteBuilderError> {
        let cn = format!("{}/{}", self.realm, self.device_id);

        let build_options = self
            .build_options
            .clone()
            .ok_or_else(|| AstarteBuilderError::ConfigError("Missing or failed build".into()))?;

        // TODO: make cap configurable
        let (client, eventloop) = AsyncClient::new(build_options.mqtt_opts.clone(), 50);

        self.subscribe(&client, &cn).await?;

        let device = AstarteSdk {
            realm: self.realm.to_owned(),
            device_id: self.device_id.to_owned(),
            credentials_secret: self.credentials_secret.to_owned(),
            pairing_url: self.pairing_url.to_owned(),
            build_options,
            client,
            eventloop: Arc::new(tokio::sync::Mutex::new(eventloop)),
            interfaces: self.interfaces.to_owned(),
            database: Database::new("/tmp/astarte.db").await?,
        };

        Ok(device)
    }
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
    /// Poll updates from mqtt, this is where you receive data
    pub async fn poll(&mut self) -> Result<Clientbound, AstarteError> {
        loop {
            // keep consuming and processing packets until we have data for the user
            match self.eventloop.lock().await.poll().await? {
                Event::Incoming(i) => {
                    trace!("Incoming = {:?}", i);

                    match i {
                        rumqttc::Packet::ConnAck(p) => {
                            if !p.session_present {
                                self.send_introspection().await;
                                self.send_emptycache().await;
                                self.database.clear().await?;
                            }
                        }
                        rumqttc::Packet::Publish(p) => {
                            // if we have data for the user, return
                            let (_, _, interface, path) =
                                parse_topic(&p.topic).ok_or(AstarteError::DeserializationError)?;

                            let bdata = p.payload.to_vec();

                            debug!("Incoming publish = {} {:?}", p.topic, bdata);

                            let ifpath = interface.clone() + &path;

                            if let Some(major_version) = self.get_property_major(&ifpath) {
                                self.database
                                    .store_prop(&ifpath, &bdata, major_version)
                                    .await?;

                                if cfg!(debug_assertions) {
                                    let original = crate::AstarteSdk::deserialize(&bdata)?;
                                    if let Aggregation::Individual(data) = original {
                                        let db = self
                                            .database
                                            .load_prop(&ifpath, major_version)
                                            .await
                                            .expect("load_prop failed")
                                            .expect(
                                                "property wasn't correctly saved in the database",
                                            );
                                        assert!(data == db);
                                        trace!("database test ok");
                                    } else {
                                        panic!("This should be impossible");
                                    }
                                }
                            }

                            let data = AstarteSdk::deserialize(&bdata)?;

                            return Ok(Clientbound {
                                interface,
                                path,
                                data,
                            });
                        }
                        _ => {}
                    }
                }
                Event::Outgoing(o) => debug!("Outgoing = {:?}", o),
            }
        }
    }

    /// returns major version if the property exists, None otherwise
    fn get_property_major(&self, ifpath: &str) -> Option<i32> {
        // todo: this could be optimized
        self.interfaces
            .iter()
            .map(|f| f.1.get_properties_paths())
            .flatten()
            .filter(|f| f.0 == *ifpath)
            .map(|f| f.1)
            .next()
    }

    fn client_id(&self) -> String {
        format!("{}/{}", self.realm, self.device_id)
    }

    async fn send_emptycache(&self) {
        let url = self.client_id() + "/control/emptyCache";
        debug!("sending emptyCache to {}", url);

        let err = self
            .client
            .publish(url, rumqttc::QoS::ExactlyOnce, false, "1")
            .await;
        debug!("emptyCache = {:?}", err);
    }

    async fn send_introspection(&self) {
        let mut introspection: String = self
            .interfaces
            .iter()
            .map(|f| format!("{}:{}:{};", f.0, f.1.version().0, f.1.version().1))
            .collect();
        introspection.pop(); // remove last ";"
        let introspection = introspection; // drop mutability

        debug!("introspection string = {}", introspection);

        let err = self
            .client
            .publish(
                self.client_id(),
                rumqttc::QoS::ExactlyOnce,
                false,
                introspection.clone(),
            )
            .await;
        debug!("introspection = {:?}", err);
    }

    /// Send data to an astarte interface
    /// ```ignore
    /// d.send("com.test.interface", "/data", 4.5).await?;
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
    /// ```ignore
    /// d.send_with_timestamp("com.test.interface", "/data", 4.5, Utc.timestamp(1537449422, 0) ).await?;
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
        let buf = AstarteSdk::serialize_individual(data, timestamp)?;

        self.client
            .publish(
                self.client_id() + "/" + interface_name.trim_matches('/') + interface_path,
                rumqttc::QoS::AtLeastOnce,
                false,
                buf,
            )
            .await?;

        Ok(())
    }

    /// Send data to an object interface
    pub async fn send_object(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: HashMap<&str, AstarteType>,
    ) -> Result<(), AstarteError> {
        self.send_object_timestamp(interface_name, interface_path, data, None)
            .await
    }

    /// Send data to an object interface. with timestamp
    pub async fn send_object_timestamp(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: HashMap<&str, AstarteType>,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), AstarteError> {
        let buf = AstarteSdk::serialize_object(data, timestamp)?;

        self.client
            .publish(
                self.client_id() + "/" + interface_name.trim_matches('/') + interface_path,
                rumqttc::QoS::ExactlyOnce,
                false,
                buf,
            )
            .await?;

        Ok(())
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

    /// Serialize a group of astarte types to a vec of bytes, representing an object
    fn serialize_object(
        data: HashMap<&str, AstarteType>,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError> {
        let data: HashMap<&str, Bson> = data.into_iter().map(|f| (f.0, f.1.into())).collect();

        let doc = to_document(&data)?;

        AstarteSdk::serialize(Bson::Document(doc), timestamp)
    }

    /// Serialize data directly from Bson
    fn serialize(
        data: Bson,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError> {
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

    async fn _get_property(&self, key: &str) -> Result<Option<AstarteType>, AstarteError> {
        //todo
        let prop = self.database.load_prop(key, 1).await?;

        Ok(prop)
    }
}

impl fmt::Debug for AstarteSdk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Device")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            .field("credentials_secret", &self.credentials_secret)
            .field("pairing_url", &self.pairing_url)
            .field("build_options", &self.build_options)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};

    use crate::{types::AstarteType, AstarteSdk};

    fn do_vecs_match(a: &Vec<u8>, b: &Vec<u8>) -> bool {
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();

        println!("matching {:?}\nwith     {:?}\n", a, b);
        matching == a.len() && matching == b.len()
    }

    //#[test] test is disabled because for some reason it serializes objects in random order
    fn _serialize_object() {
        let mut map: HashMap<&str, AstarteType> = HashMap::new();
        map.insert("temp", 25.3123.into());
        map.insert("hum", 67.112.into());

        let buf = AstarteSdk::serialize_object(
            map.clone(),
            None, /*Some(Utc.timestamp(1537449422890,0))*/
        )
        .unwrap(); // allow_panic

        assert!(do_vecs_match(
            &buf,
            &vec![
                0x28, 0x00, 0x00, 0x00, 0x03, 0x76, 0x00, 0x20, 0x00, 0x00, 0x00, 0x01, 0x68, 0x75,
                0x6d, 0x00, 0xba, 0x49, 0x0c, 0x02, 0x2b, 0xc7, 0x50, 0x40, 0x01, 0x74, 0x65, 0x6d,
                0x70, 0x00, 0x72, 0x8a, 0x8e, 0xe4, 0xf2, 0x4f, 0x39, 0x40, 0x00, 0x00
            ]
        ));

        let buf =
            AstarteSdk::serialize_object(map, Some(Utc.timestamp(1537449422, 890000000))).unwrap(); // allow_panic

        assert!(do_vecs_match(
            &buf,
            &vec![
                0x33, 0x00, 0x00, 0x00, 0x09, 0x74, 0x00, 0xfb, 0x9d, 0x4f, 0xf7, 0x65, 0x01, 0x00,
                0x00, 0x03, 0x76, 0x00, 0x20, 0x00, 0x00, 0x00, 0x01, 0x68, 0x75, 0x6d, 0x00, 0xba,
                0x49, 0x0c, 0x02, 0x2b, 0xc7, 0x50, 0x40, 0x01, 0x74, 0x65, 0x6d, 0x70, 0x00, 0x72,
                0x8a, 0x8e, 0xe4, 0xf2, 0x4f, 0x39, 0x40, 0x00, 0x00
            ]
        ));
    }

    #[test]
    fn serialize_individual() {
        assert!(do_vecs_match(
            &AstarteSdk::serialize_individual(false, None).unwrap(), // allow_panic
            &vec![0x09, 0x00, 0x00, 0x00, 0x08, 0x76, 0x00, 0x00, 0x00]
        )); // allow_panic
        assert!(do_vecs_match(
            &AstarteSdk::serialize_individual(16.73, None).unwrap(), // allow_panic
            &vec![
                0x10, 0x00, 0x00, 0x00, 0x01, 0x76, 0x00, 0x7b, 0x14, 0xae, 0x47, 0xe1, 0xba, 0x30,
                0x40, 0x00
            ]
        )); // allow_panic
        assert!(do_vecs_match(
            &AstarteSdk::serialize_individual(16.73, Some(Utc.timestamp(1537449422, 890000000)))
                .unwrap(), // allow_panic
            &vec![
                0x1b, 0x00, 0x00, 0x00, 0x09, 0x74, 0x00, 0x2a, 0x70, 0x20, 0xf7, 0x65, 0x01, 0x00,
                0x00, 0x01, 0x76, 0x00, 0x7b, 0x14, 0xae, 0x47, 0xe1, 0xba, 0x30, 0x40, 0x00
            ]
        ));
    }

    #[test]
    fn test_parse_topic() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/com.interface.test/led/red".to_owned();
        let (realm, device, interface, path) = crate::parse_topic(&topic).unwrap(); // allow_panic

        assert!(realm == "test");
        assert!(device == "u-WraCwtK_G_fjJf63TiAw");
        assert!(interface == "com.interface.test");
        assert!(path == "/led/red");
    }
}
