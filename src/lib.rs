mod crypto;
mod pairing;

use crypto::Bundle;
use openssl::error::ErrorStack;
use pairing::PairingError;
use rumqttc::{AsyncClient, ClientConfig, Event, MqttOptions};
use rustls::{internal::pemfile, Certificate, PrivateKey};
use std::fmt;
use std::sync::Arc;
use tokio::task::JoinHandle;
use url::Url;

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
    eventloop_task: Option<JoinHandle<()>>,
}

pub struct DeviceBuilder {
    realm: String,
    device_id: String,
    credentials_secret: Option<String>,
    pairing_url: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum DeviceBuilderError {
    #[error("private key or CSR creation failed")]
    CryptoGeneration(#[from] ErrorStack),
    #[error("device must have a credentials secret")]
    MissingCredentialsSecret,
    #[error("device must have a pairing URL")]
    MissingPairingUrl,
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("error while obtaining credentials")]
    Credentials(#[from] PairingError),
}

impl DeviceBuilder {
    pub fn new(realm: &str, device_id: &str) -> Self {
        DeviceBuilder {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: None,
            pairing_url: None,
        }
    }

    pub fn credentials_secret(&mut self, credentials_secret: &str) -> &mut Self {
        self.credentials_secret = Some(credentials_secret.to_owned());
        self
    }

    pub fn pairing_url(&mut self, pairing_url: &str) -> &mut Self {
        self.pairing_url = Some(pairing_url.to_owned());
        self
    }

    pub fn build(&self) -> Result<Device, DeviceBuilderError> {
        let cn = format!("{}/{}", self.realm, self.device_id);

        let credentials_secret = self
            .credentials_secret
            .as_ref()
            .ok_or(DeviceBuilderError::MissingCredentialsSecret)?;
        let pairing_url = self
            .pairing_url
            .as_ref()
            .ok_or(DeviceBuilderError::MissingPairingUrl)?;

        let Bundle(pkey_bytes, csr_bytes) = Bundle::new(&cn)?;

        let private_key = pemfile::pkcs8_private_keys(&mut pkey_bytes.as_slice())
            .unwrap()
            .remove(0);
        let csr = String::from_utf8(csr_bytes).unwrap();

        let device = Device {
            realm: self.realm.to_owned(),
            device_id: self.device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            private_key,
            csr,
            certificate_pem: None,
            broker_url: None,
            client: None,
            eventloop_task: None,
        };

        Ok(device)
    }
}

impl Device {
    pub async fn connect(&mut self) -> Result<(), ConnectionError> {
        self.ensure_ready_for_connection().await?;

        let mqtt_opts = self.build_mqtt_opts();

        // TODO: make cap configurable
        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 50);

        let eventloop_task = tokio::spawn(async move {
            loop {
                match eventloop.poll().await.unwrap() {
                    Event::Incoming(i) => println!("Incoming = {:?}", i),
                    Event::Outgoing(o) => println!("Outgoing = {:?}", o),
                }
            }
        });

        self.eventloop_task = Some(eventloop_task);
        self.client = Some(client);

        Ok(())
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
            .set_keep_alive(30)
            .set_tls_client_config(Arc::new(tls_client_config));

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
