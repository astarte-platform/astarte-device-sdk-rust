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
 */

use log::debug;
use openssl::error::ErrorStack;
use pairing::PairingError;
use rumqttc::{ClientConfig, MqttOptions, Transport};
use rustls::ServerCertVerifier;
use rustls::{internal::pemfile, Certificate, PrivateKey};
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use url::Url;

use interface::traits::Interface as InterfaceTrait;
pub use interface::Interface;

use crate::crypto::Bundle;
use crate::database::AstarteDatabase;
use crate::interface::{self};
use crate::interfaces::Interfaces;
use crate::{pairing, AstarteSdk};

/// Options for astarte builder
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BuildOptions {
    pub(crate) private_key: PrivateKey,
    pub(crate) csr: String,
    pub(crate) certificate_pem: Vec<Certificate>,
    pub(crate) broker_url: Url,
    pub(crate) mqtt_opts: MqttOptions,
}

/// Builder for Astarte client
///
/// ```
/// use astarte_sdk::builder::AstarteBuilder;
///
/// let realm = "test";
/// let device_id = "xxxxxxxxxxxxxxxxxxxxxxx";
/// let credentials_secret = "xxxxxxxxxxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxxxx";
/// let pairing_url = "https://api.example.com/pairing";
///
/// let mut sdk_options = AstarteBuilder::new(&realm, &device_id, &credentials_secret, &pairing_url);
///
/// sdk_options.add_interface_files("path/to/interfaces");
///
///
/// ```

#[derive(Clone)]
pub struct AstarteBuilder {
    pub(crate) realm: String,
    pub(crate) device_id: String,
    pub(crate) credentials_secret: String,
    pub(crate) pairing_url: String,
    pub(crate) interfaces: HashMap<String, Interface>,
    pub(crate) database: Option<Arc<dyn AstarteDatabase + Sync + Send>>,
    pub(crate) ignore_ssl_errors: bool,
    pub(crate) keepalive: std::time::Duration,
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

impl AstarteBuilder {
    pub fn new(realm: &str, device_id: &str, credentials_secret: &str, pairing_url: &str) -> Self {
        AstarteBuilder {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            interfaces: HashMap::new(),
            database: None,
            ignore_ssl_errors: false,
            keepalive: std::time::Duration::from_secs(30),
        }
    }

    pub fn with_database<T: AstarteDatabase + 'static + Sync + Send>(&mut self, database: T) {
        self.database = Some(Arc::new(database));
    }

    /// Set time after which client should ping the broker
    /// if there is no other data exchange
    pub fn set_keep_alive(&mut self, duration: std::time::Duration) {
        self.keepalive = duration;
    }

    pub fn ignore_ssl_errors(&mut self) {
        self.ignore_ssl_errors = true;
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
        let AstarteBuilder {
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

        if self.keepalive.as_secs() < 5 {
            return Err(AstarteBuilderError::ConfigError(
                "Keepalive should be >= 5 secs".into(),
            ));
        }

        mqtt_opts.set_keep_alive(self.keepalive);

        if self.ignore_ssl_errors || std::env::var("IGNORE_SSL_ERRORS") == Ok("true".to_string()) {
            struct OkVerifier {}
            impl ServerCertVerifier for OkVerifier {
                fn verify_server_cert(
                    &self,
                    _: &rustls::RootCertStore,
                    _: &[Certificate],
                    _: webpki::DNSNameRef,
                    _: &[u8],
                ) -> Result<rustls::ServerCertVerified, rustls::TLSError> {
                    Ok(rustls::ServerCertVerified::assertion())
                }
            }

            let mut clientconfig = tls_client_config.dangerous();
            clientconfig.set_certificate_verifier(Arc::new(OkVerifier {}));

            let tls_config =
                rumqttc::TlsConfiguration::Rustls(Arc::new(clientconfig.cfg.to_owned()));
            let transport = Transport::tls_with_config(tls_config);

            mqtt_opts.set_transport(transport);
        } else {
            mqtt_opts.set_transport(Transport::tls_with_config(tls_client_config.into()));
        }

        Ok(mqtt_opts)
    }

    /// build Astarte client, call this before `connect`
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

        let mqtt_opts = self.build_mqtt_opts(&certificate_pem, &broker_url, &private_key)?;

        let build_options = BuildOptions {
            private_key,
            csr,
            certificate_pem,
            broker_url,
            mqtt_opts,
        };

        let device = AstarteSdk {
            realm: self.realm.to_owned(),
            device_id: self.device_id.to_owned(),
            credentials_secret: self.credentials_secret.to_owned(),
            pairing_url: self.pairing_url.to_owned(),
            build_options,
            transport: None,
            interfaces: Interfaces::new(self.interfaces.clone()),
            database: self.database.clone(),
        };

        Ok(device)
    }
}
