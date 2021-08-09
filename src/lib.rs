//! Astarte is an Open Source IoT platform focused on Data management.
//! It takes care of everything from collecting data from devices to delivering data to end-user applications.
//! To achieve such a thing, it uses a mixture of mechanisms and paradigm to store organized data, perform live queries.
//!
//! <https://docs.astarte-platform.org/>

mod crypto;
mod interface;
mod pairing;
pub mod types;

use bson::{to_document, Bson};
use crypto::Bundle;
use log::{debug, trace};
use openssl::error::ErrorStack;
use pairing::PairingError;
use rumqttc::EventLoop;
use rumqttc::{AsyncClient, ClientConfig, Event, MqttOptions, Transport};
use rustls::{internal::pemfile, Certificate, PrivateKey};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use types::AstarteType;
use url::Url;

use interface::traits::Interface as InterfaceTrait;
pub use interface::Interface;

use crate::interface::Ownership;

/// Astarte client
#[derive(Clone)]
pub struct AstarteSdk {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
    private_key: PrivateKey,
    csr: String,
    certificate_pem: Vec<Certificate>,
    broker_url: Url,
    client: AsyncClient,
    eventloop: Arc<tokio::sync::Mutex<EventLoop>>,
    interfaces: HashMap<String, Interface>,
}

/// Builder for Astarte client
///
/// ```ignore
/// let mut sdk_options = AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url);
/// ```
///
/// Interfaces should be added before usage
/// ```ignore
/// sdk_options.add_interface_files("path/to/interfaces")
/// ```
/// or
/// ```ignore
/// sdk_options.add_interface_file("path/to/interfaces/interface.json")
/// ```

pub struct AstarteOptions {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
    interfaces: HashMap<String, Interface>,
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
}

#[derive(thiserror::Error, Debug)]
pub enum AstarteError {
    #[error("bson serialize error")]
    BsonSerError(#[from] bson::ser::Error),

    #[error("bson client error")]
    BsonClientError(#[from] rumqttc::ClientError),

    #[error("mqtt connection error")]
    ConnectionError(#[from] rumqttc::ConnectionError),

    #[error("generic error")]
    Unreported,
}

impl AstarteOptions {
    pub fn new(realm: &str, device_id: &str, credentials_secret: &str, pairing_url: &str) -> Self {
        AstarteOptions {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            interfaces: HashMap::new(),
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

    pub async fn build(&mut self) -> Result<AstarteSdk, AstarteBuilderError> {
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

        let mqtt_opts = self.build_mqtt_opts(&certificate_pem, &broker_url, &private_key);

        // TODO: make cap configurable
        let (client, eventloop) = AsyncClient::new(mqtt_opts?, 50);

        self.subscribe(&client, &cn).await?;

        let device = AstarteSdk {
            realm: self.realm.to_owned(),
            device_id: self.device_id.to_owned(),
            credentials_secret: self.credentials_secret.to_owned(),
            pairing_url: self.pairing_url.to_owned(),
            private_key,
            csr: csr.clone(),
            certificate_pem,
            broker_url,
            client,
            eventloop: Arc::new(tokio::sync::Mutex::new(eventloop)),
            interfaces: self.interfaces.to_owned(),
        };

        Ok(device)
    }
}

#[derive(Debug)]
pub enum Aggregation {
    Individual(AstarteType),
    Object(HashMap<String, AstarteType>),
}

/// packet from astarte to device
#[derive(Debug)]
pub struct Clientbound {
    pub path: String,
    pub data: Aggregation,
}

impl AstarteSdk {
    /// Poll updates from mqtt, this is where you receive data
    pub async fn poll(&mut self) -> Result<Option<Clientbound>, AstarteError> {
        match self.eventloop.lock().await.poll().await? {
            Event::Incoming(i) => {
                debug!("Incoming = {:?}", i);

                match i {
                    rumqttc::Packet::ConnAck(p) => {
                        if !p.session_present {
                            self.send_introspection().await;
                            self.send_emptycache().await;
                        }
                    }
                    rumqttc::Packet::Publish(p) => {
                        let topic = p.topic;
                        let data = p.payload.to_vec();
                        debug!("Incoming publish = {} {:?}", topic, data);

                        if let Ok(deserialized) =
                            bson::Document::from_reader(&mut std::io::Cursor::new(data))
                        {
                            trace!("{:?}", deserialized);
                            if let Some(v) = deserialized.get("v") {
                                if let Bson::Document(doc) = v {
                                    let strings = doc.iter().map(|f| f.0.clone());

                                    let data =
                                        doc.iter().map(|f| AstarteType::from_bson(f.1.clone()));
                                    let data: Option<Vec<AstarteType>> = data.collect();
                                    let data = data.ok_or(AstarteError::Unreported)?; //TODO

                                    let hmap: HashMap<String, AstarteType> =
                                        strings.zip(data).collect();

                                    let reply = Clientbound {
                                        path: topic,
                                        data: Aggregation::Object(hmap),
                                    };

                                    return Ok(Some(reply));
                                } else if let Some(v) = AstarteType::from_bson(v.clone()) {
                                    //TODO if the device id is not in the topic, it's probably an error
                                    let reply = Clientbound {
                                        path: topic,
                                        data: Aggregation::Individual(v),
                                    };

                                    return Ok(Some(reply));
                                } else {
                                    return Err(AstarteError::Unreported); //TODO
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Event::Outgoing(o) => debug!("Outgoing = {:?}", o),
        }

        Ok(None)
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
        self.send_timestamp(interface_name, interface_path, data, None)
            .await
    }

    /// Send data to an astarte interface, with timestamp
    /// ```ignore
    /// d.send("com.test.interface", "/data", 4.5, Some(Utc.timestamp(1537449422, 0)) ).await?;
    /// ```
    pub async fn send_timestamp<D>(
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

    /// Serialize an astarte type into a vec of bytes
    pub fn serialize_individual<D>(
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError>
    where
        D: Into<AstarteType>,
    {
        let doc = if let Some(timestamp) = timestamp {
            bson::doc! {
               "t": timestamp,
               "v": data.into()
            }
        } else {
            bson::doc! {
               "v": data.into(),
            }
        };

        let mut buf = Vec::new();
        doc.to_writer(&mut buf)?;
        Ok(buf)
    }

    /// Deserialize an astarte type from a vec of bytes
    pub fn deserialize_individual(data: Vec<u8>) -> Option<AstarteType> {
        if let Ok(deserialized) = bson::Document::from_reader(&mut std::io::Cursor::new(data)) {
            trace!("deserialized {:?}", deserialized);

            if let Some(v) = deserialized.get("v") {
                return AstarteType::from_bson(v.clone());
            }
        }

        None
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

    /// Serialize a group of astarte types to a vec of bytes, representing an object
    pub fn serialize_object(
        data: HashMap<&str, AstarteType>,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<u8>, AstarteError> {
        let data: HashMap<&str, Bson> = data.into_iter().map(|f| (f.0, f.1.into())).collect();

        let doc = to_document(&data)?;

        let doc = if let Some(timestamp) = timestamp {
            bson::doc! {
               "t": timestamp,
               "v": doc
            }
        } else {
            bson::doc! {
               "v": doc,
            }
        };

        let mut buf = Vec::new();
        doc.to_writer(&mut buf)?;
        println!("{:#?}", doc);
        Ok(buf)
    }

    /// deserialize an astarte object from a vec of bytes
    pub fn deserialize_object(data: Vec<u8>) -> Result<HashMap<String, AstarteType>, AstarteError> {
        if let Ok(deserialized) = bson::Document::from_reader(&mut std::io::Cursor::new(data)) {
            trace!("deserialized {:?}", deserialized);

            if let Some(v) = deserialized.get("v") {
                if let Bson::Document(doc) = v {
                    println!("\n\nDeserialized {:?}", doc);

                    let mut ret = HashMap::new();

                    for i in doc {
                        ret.insert(i.0.clone(), AstarteType::from_bson(i.1.clone()).unwrap());
                    }

                    return Ok(ret);
                }
            }
        }

        return Ok(HashMap::new());
    }
}

impl fmt::Debug for AstarteSdk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Device")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            .field("credentials_secret", &self.credentials_secret)
            .field("pairing_url", &self.pairing_url)
            .field("private_key", &self.private_key)
            .field("csr", &self.csr)
            .field("certificate_pem", &self.certificate_pem)
            .field("broker_url", &self.broker_url)
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
            &AstarteSdk::serialize_individual(false, None).unwrap(),
            &vec![0x09, 0x00, 0x00, 0x00, 0x08, 0x76, 0x00, 0x00, 0x00]
        )); // allow_panic
        assert!(do_vecs_match(
            &AstarteSdk::serialize_individual(16.73, None).unwrap(),
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
}
