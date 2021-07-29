mod crypto;
mod interface;
mod pairing;
pub mod types;

use bson::{Bson, to_document};
use crypto::Bundle;
use log::{debug, trace};
use openssl::error::ErrorStack;
use pairing::PairingError;
use rumqttc::EventLoop;
use rumqttc::{AsyncClient, ClientConfig, Event, MqttOptions, Transport};
use rustls::{internal::pemfile, Certificate, PrivateKey};
use std::collections::HashMap;
use std::{fmt};
use std::path::Path;
use std::sync::{Arc};
use url::Url;
use types::AstarteType;

use interface::traits::Interface as InterfaceTrait;
pub use interface::Interface;

use crate::interface::Ownership;

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

pub struct AstarteOptions {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
    interfaces: HashMap<String, Interface>,
}

#[derive(thiserror::Error, Debug)]
pub enum DeviceBuilderError {
    #[error("private key or CSR creation failed")]
    CryptoGeneration(#[from] ErrorStack),
    #[error("device must have a credentials secret")]
    MissingCredentialsSecret,
    #[error("device must have a pairing URL")]
    MissingPairingUrl,
    #[error("device must have at least an interface")]
    MissingInterfaces,
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("error while obtaining credentials")]
    Credentials(#[from] PairingError),
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

    pub fn add_interface_file(&mut self, file_path: &Path) -> &mut Self {
        // TODO: don't unwrap here. Builder returning Result?
        let interface = Interface::from_file(file_path).unwrap();
        let name = interface.name();
        debug!("Added interface {}", name);
        self.interfaces.insert(name.to_owned(), interface);
        self
    }

    pub fn add_interface_files(&mut self, interfaces_directory: &str) -> &mut Self {
        let interface_files = std::fs::read_dir(Path::new(interfaces_directory)).unwrap();
        interface_files
            .filter_map(Result::ok)
            .filter(|f| {
                if let Some(ext) = f.path().extension() {
                    ext == "json"
                } else {
                    false
                }
            })
            .for_each(|f| {
                self.add_interface_file(&f.path());
            });

        self
    }

    async fn populate_credentials(&mut self, csr: &str) -> Result<Vec<Certificate>, PairingError> {
        let cert_pem = pairing::fetch_credentials(&self, csr).await?;
        let mut cert_pem_bytes = cert_pem.as_bytes();
        let certs = pemfile::certs(&mut cert_pem_bytes).unwrap();
        Ok(certs)
    }

    async fn populate_broker_url(&mut self) -> Result<Url, PairingError> {
        let broker_url = pairing::fetch_broker_url(&self).await?;
        let parsed_broker_url = Url::parse(&broker_url)?;
        Ok(parsed_broker_url)
    }

    fn build_mqtt_opts(&self, certificate_pem: &Vec<Certificate>, broker_url: &Url, private_key: &PrivateKey) -> MqttOptions {
        let AstarteOptions {
            realm,
            device_id,
            ..
        } = self;

        let client_id = format!("{}/{}", realm, device_id);
        let host = broker_url.host_str().unwrap();
        let port = broker_url.port().unwrap();
        let mut tls_client_config = ClientConfig::new();
        tls_client_config.root_store =
            rustls_native_certs::load_native_certs().expect("could not load platform certs");
        tls_client_config
            .set_single_client_cert(certificate_pem.to_owned(), private_key.to_owned())
            .expect("cannot setup client auth");

        let mut mqtt_opts = MqttOptions::new(client_id, host, port);
        mqtt_opts
            .set_keep_alive(30);

        mqtt_opts.set_transport(Transport::tls_with_config(tls_client_config.into()));

        mqtt_opts
    }


    pub async fn subscribe(&mut self, client: &AsyncClient, cn: &String) {
        let ifaces = self.interfaces.clone().into_iter()
            .filter(|i| i.1.get_ownership() == Ownership::Server );

        client.subscribe(cn.clone()+ "/control/consumer/properties", rumqttc::QoS::AtLeastOnce).await.unwrap();

        for i in ifaces {
            client.subscribe(cn.clone()+ "/" + i.1.name() + "/#", rumqttc::QoS::AtLeastOnce).await.unwrap();
        }
    }

    pub async fn build(&mut self) -> Result<AstarteSdk, DeviceBuilderError> {
        let cn = format!("{}/{}", self.realm, self.device_id);


        if self.interfaces.is_empty() {
            return Err(DeviceBuilderError::MissingInterfaces);
        }

        let Bundle(pkey_bytes, csr_bytes) = Bundle::new(&cn)?;

        let private_key = pemfile::pkcs8_private_keys(&mut pkey_bytes.as_slice())
            .unwrap()
            .remove(0);
        let csr = String::from_utf8(csr_bytes).unwrap();


        let certificate_pem = self.populate_credentials(&csr).await.unwrap();
        let broker_url =  self.populate_broker_url().await.unwrap();

        let mqtt_opts = self.build_mqtt_opts(&certificate_pem, &broker_url, &private_key);

        // TODO: make cap configurable
        let (client, eventloop) = AsyncClient::new(mqtt_opts, 50);

        self.subscribe(&client, &cn).await;

        let device = AstarteSdk {
            realm: self.realm.to_owned(),
            device_id: self.device_id.to_owned(),
            credentials_secret: self.credentials_secret.to_owned(),
            pairing_url: self.pairing_url.to_owned(),
            private_key,
            csr: csr.clone(),
            certificate_pem,
            broker_url,
            client: client,
            eventloop: Arc::new(tokio::sync::Mutex::new(eventloop)),
            interfaces: self.interfaces.to_owned(),
        };

        Ok(device)
    }
}



impl AstarteSdk {

    pub async fn poll(&mut self ) -> Option<(String, AstarteType)> {
        match self.eventloop.lock().await.poll().await.unwrap() {
            Event::Incoming(i) => {
                debug!("Incoming = {:?}", i);

                match i {
                    rumqttc::Packet::ConnAck(p) => {
                        if p.session_present == false {
                            self.send_introspection().await;
                            //self.send_emptycache().await;
                        }
                    },
                    rumqttc::Packet::Publish(p) => {
                        let topic = p.topic;
                        let data = p.payload.to_vec();
                        debug!("Incoming publish = {} {:?}", topic, data);

                        if let Ok(deserialized) = bson::Document::from_reader(&mut  std::io::Cursor::new(data)) {
                            trace!("{:?}", deserialized);
                            if let Some(v) = deserialized.get("v") {
                                if let Some(v) = AstarteType::from_bson(v.clone()){
                                    return Some((topic, v));
                                }
                            }

                        }

                    }
                    _ => {

                    }
                }
        },
            Event::Outgoing(o) => debug!("Outgoing = {:?}", o),
        }

        None
    }

    fn client_id(&self) -> String {
        format!("{}/{}", self.realm, self.device_id)
    }

    async fn send_emptycache(&self){
        let url =  self.client_id() + "/control/emptyCache";
        debug!("sending emptyCache to {}", url);

        let err = self.client.publish(url, rumqttc::QoS::ExactlyOnce, false, "1").await;
        debug!("emptyCache = {:?}", err);
    }

    async fn send_introspection(& self){
        let mut introspection: String = self.interfaces.iter().map(|f| format!("{}:{}:{};", f.0, f.1.version().0, f.1.version().1)).collect();
        introspection.pop(); // remove last ";"
        let introspection = introspection; // drop mutability

        debug!("introspection string = {}", introspection);

        let err = self.client.publish(self.client_id(), rumqttc::QoS::ExactlyOnce, false, introspection.clone()).await;
        debug!("introspection = {:?}", err);
    }

    pub async fn publish <V> (&self, interface: &str, payload: V ) -> Result<(), rumqttc::ClientError>
    where
        V: Into<Vec<u8>>,
    {
        let pay: Vec<u8> = payload.into();
        debug!("publishing {} {:?}", interface, pay  );


        self.client.publish(self.client_id() + interface, rumqttc::QoS::ExactlyOnce, false, pay).await
    }


    pub async fn send<D> (&self, interface_name: &str, interface_path: &str, data: D)
    where
    D: Into<AstarteType>,{
        self.send_timestamp(interface_name, interface_path, data, None).await
    }

    pub fn serialize_object(data: HashMap<&str,AstarteType>, timestamp: Option<chrono::DateTime<chrono::Utc>>) -> Vec<u8> {

        let data: HashMap<&str,Bson> = data.into_iter().map(|f| (f.0, f.1.into())).collect();

        let doc = to_document(&data).unwrap();

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
        doc.to_writer(&mut buf).unwrap();
        println!("{:#?}", doc);
        println!("{:X?}", buf);

        buf
    }


    pub fn serialize_individual<D>(data: D, timestamp: Option<chrono::DateTime<chrono::Utc>>) -> Vec<u8>
    where
    D: Into<AstarteType> {
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
        doc.to_writer(&mut buf).unwrap();
        println!("{:X?}", buf);

        buf
    }

    pub async fn send_timestamp<D>(&self, interface_name: &str, interface_path: &str, data: D, timestamp: Option<chrono::DateTime<chrono::Utc>>)
    where
    D: Into<AstarteType> {

        let buf = AstarteSdk::serialize_individual(data, timestamp);

        self.client.publish(self.client_id() + "/" + interface_name.trim_matches('/') + interface_path, rumqttc::QoS::ExactlyOnce, false, buf).await.unwrap();
    }


    pub async fn send_object_timestamp(&self, interface_name: &str, interface_path: &str, data: HashMap<&str,AstarteType>, timestamp: Option<chrono::DateTime<chrono::Utc>>){

        let buf = AstarteSdk::serialize_object(data, timestamp);

        self.client.publish(self.client_id() + "/" + interface_name.trim_matches('/') + interface_path, rumqttc::QoS::ExactlyOnce, false, buf).await.unwrap();
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
    use std::{collections::HashMap};

    use chrono::{TimeZone, Utc};

    use crate::{AstarteSdk, types::AstarteType};

    // https://stackoverflow.com/questions/29504514/whats-the-best-way-to-compare-2-vectors-or-strings-element-by-element
    fn do_vecs_match<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
        matching == a.len() && matching == b.len()
    }


    #[test]
    fn serialize_object() {

        let mut lol: HashMap<&str, AstarteType> = HashMap::new();
        lol.insert("temp", 25.3123.into());
        lol.insert("hum", 67.112.into());

        let buf = AstarteSdk::serialize_object(lol, None/*Some(Utc.timestamp(1537449422890,0))*/);

        assert!(do_vecs_match(&buf, &vec![  0x28, 0x00, 0x00, 0x00, 0x03, 0x76, 0x00, 0x20, 0x00, 0x00, 0x00, 0x01,
            0x68, 0x75, 0x6d, 0x00, 0xba, 0x49, 0x0c, 0x02, 0x2b, 0xc7, 0x50, 0x40,
            0x01, 0x74, 0x65, 0x6d, 0x70, 0x00, 0x72, 0x8a, 0x8e, 0xe4, 0xf2, 0x4f,
            0x39, 0x40, 0x00, 0x00
          ]));
    }

    #[test]
    fn serialize_individual() {
        assert!(do_vecs_match(&AstarteSdk::serialize_individual(false, None), &vec![0x09, 0x00, 0x00, 0x00, 0x08, 0x76, 0x00, 0x00, 0x00]));
        assert!(do_vecs_match(&AstarteSdk::serialize_individual(16.73, None), &vec![0x10, 0x00, 0x00, 0x00, 0x01, 0x76, 0x00, 0x7b, 0x14, 0xae, 0x47, 0xe1, 0xba, 0x30, 0x40, 0x00]));
        assert!(do_vecs_match(&AstarteSdk::serialize_individual(16.73, Some(Utc.timestamp(1537449422890,0))), 
            &vec![0x1b, 0x00, 0x00, 0x00, 0x09, 0x74, 0x00, 0x2a, 0x70, 0x20, 0xf7, 0x65, 0x01, 0x00, 0x00, 0x01,
                0x76, 0x00, 0x7b, 0x14, 0xae, 0x47, 0xe1, 0xba, 0x30, 0x40, 0x00]));

    }
}