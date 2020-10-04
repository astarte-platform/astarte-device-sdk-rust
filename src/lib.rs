mod crypto;
mod pairing;

use crypto::Bundle;
use openssl::error::ErrorStack;
use pairing::PairingError;
use rustls::{internal::pemfile, Certificate, PrivateKey};
use std::fmt;
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
            realm: String::from(realm),
            device_id: String::from(device_id),
            credentials_secret: None,
            pairing_url: None,
        }
    }

    pub fn credentials_secret(&mut self, credentials_secret: &str) -> &mut Self {
        self.credentials_secret = Some(String::from(credentials_secret));
        self
    }

    pub fn pairing_url(&mut self, pairing_url: &str) -> &mut Self {
        self.pairing_url = Some(String::from(pairing_url));
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
            realm: String::from(&self.realm),
            device_id: String::from(&self.device_id),
            credentials_secret: String::from(credentials_secret),
            pairing_url: String::from(pairing_url),
            private_key,
            csr,
            certificate_pem: None,
            broker_url: None,
        };

        Ok(device)
    }
}

impl Device {
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
