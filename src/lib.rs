mod crypto;
mod interface;
mod pairing;

use crypto::Bundle;
use log::debug;
use openssl::error::ErrorStack;
use pairing::PairingError;
use rumqttc::{AsyncClient, ClientConfig, Event, MqttOptions, Transport};
use rustls::{internal::pemfile, Certificate, PrivateKey};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinHandle;
use url::Url;

use interface::traits::Interface as InterfaceTrait;
pub use interface::Interface;

#[derive(Clone)]
pub struct Device {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
    private_key: PrivateKey,
    csr: String,
    certificate_pem: Option<Vec<Certificate>>,
    broker_url: Option<Url>,
    client: Option<AsyncClient>,
    eventloop_task: Arc<Option<JoinHandle<()>>>,
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


impl Device {

    pub fn new(realm: &str, device_id: &str, credentials_secret: &str, pairing_url: &str) -> Device {
        let cn = format!("{}/{}", realm, device_id);

        let Bundle(pkey_bytes, csr_bytes) = Bundle::new(&cn).unwrap();

        let private_key = pemfile::pkcs8_private_keys(&mut pkey_bytes.as_slice())
            .unwrap()
            .remove(0);
        let csr = String::from_utf8(csr_bytes).unwrap();

        Device {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            private_key,
            csr,
            certificate_pem: None,
            broker_url: None,
            client: None,
            eventloop_task: Arc::new(None),
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

    pub async fn connect(&mut self) -> Result<(), ConnectionError> {
        self.ensure_ready_for_connection().await?;

        let mqtt_opts = self.build_mqtt_opts();

        // TODO: make cap configurable
        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 50);

        self.client = Some(client);

        let device2 = self.clone();

        let eventloop_task = tokio::spawn(async move {
            loop {
                match eventloop.poll().await.unwrap() {
                    Event::Incoming(i) => {
                        debug!("Incoming = {:?}", i);

                        match i {
                            rumqttc::Packet::ConnAck(p) => {
                                if p.session_present == false {
                                    device2.send_introspection().await;
                                    //device2.send_emptycache().await;
                                }
                            },
                            _ => {

                            }
                        }
                },
                    Event::Outgoing(o) => debug!("Outgoing = {:?}", o),
                }
            }
        });

        self.eventloop_task = Arc::new(Some(eventloop_task));

        Ok(())
    }

    fn client_id(&self) -> String {
        format!("{}/{}", self.realm, self.device_id)
    }

    async fn send_emptycache(&self){
        if let Some(client) = self.client.clone() {
            let url =  self.client_id() + "/control/emptyCache";
            debug!("sending emptyCache to {}", url);

            let err = client.publish(url, rumqttc::QoS::ExactlyOnce, false, "1").await;
            debug!("emptyCache = {:?}", err);
        }
    }

    async fn send_introspection(& self){
        let mut introspection: String = self.interfaces.iter().map(|f| format!("{}:{}:{};", f.0, f.1.version().0, f.1.version().1)).collect();
        introspection.pop(); // remove last ;
        let introspection = introspection; // drop mutability

        debug!("introspection string = {}", introspection);

        if let Some(client) = self.client.clone() {
            let err = client.publish(self.client_id(), rumqttc::QoS::ExactlyOnce, false, introspection.clone()).await;
            debug!("introspection = {:?}", err);



        } else {
            panic!("Ma sono senza client!")
        }


    }

    pub async fn publish <V> (&self, interface: &str, payload: V ) -> Result<(), rumqttc::ClientError>
    where
        V: Into<Vec<u8>>,
    {
        let pay: Vec<u8> = payload.into();
        debug!("publishing {} {:?}", interface, pay  );

        if let Some(client) = self.client.clone() {

            return client.publish(self.client_id() + interface, rumqttc::QoS::ExactlyOnce, false, pay).await;
        }else {
            panic!("Cosa ci faccio qua");
        }
    }

    async fn ensure_ready_for_connection(&mut self) -> Result<(), ConnectionError> {
        if let None = self.certificate_pem {
            self.populate_credentials().await?;
        }

        if let None = self.broker_url {
            self.populate_broker_url().await?;
        }

        Ok(())
    }

    fn build_mqtt_opts(&self) -> MqttOptions {
        // Now we're sure to have these since we populated them above,
        // so we can unwrap
        let certificate_pem = self.certificate_pem.as_ref().unwrap();
        let broker_url = self.broker_url.as_ref().unwrap();
        let Device {
            realm,
            device_id,
            private_key,
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

    async fn populate_credentials(&mut self) -> Result<(), PairingError> {
        let cert_pem = pairing::fetch_credentials(&self).await?;
        let mut cert_pem_bytes = cert_pem.as_bytes();
        let certs = pemfile::certs(&mut cert_pem_bytes).unwrap();
        self.certificate_pem = Some(certs);
        Ok(())
    }

    async fn populate_broker_url(&mut self) -> Result<(), PairingError> {
        let broker_url = pairing::fetch_broker_url(&self).await?;
        let parsed_broker_url = Url::parse(&broker_url)?;
        self.broker_url = Some(parsed_broker_url);
        Ok(())
    }
}

impl fmt::Debug for Device {
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
