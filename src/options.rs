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
//! Provides functionality to configure an instance of the
//! [AstarteDeviceSdk][crate::AstarteDeviceSdk].
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use openssl::error::ErrorStack;
use pairing::PairingError;

use crate::database::AstarteDatabase;
use crate::interface::{self};
use crate::pairing;

use interface::traits::Interface as InterfaceTrait;
pub use interface::Interface;

/// Default keep alive interval in seconds for the MQTT connection.
pub const DEFAULT_KEEP_ALIVE: u64 = 30;
/// Default connection timeout in seconds for the MQTT connection.
pub const DEFAULT_CONNECTION_TIMEOUT: u64 = 5;

/// Astarte options error.
///
/// Possible errors used by the Astarte options module.
#[derive(thiserror::Error, Debug)]
pub enum AstarteOptionsError {
    #[error("private key or CSR creation failed")]
    CryptoGeneration(#[from] ErrorStack),

    #[error("device must have at least an interface")]
    MissingInterfaces,

    #[error("error creating interface")]
    InterfaceError(#[from] interface::Error),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("configuration error")]
    ConfigError(String),

    #[error(transparent)]
    MqttError(#[from] rumqttc::ClientError),

    #[error("pairing error")]
    PairingError(#[from] PairingError),

    #[error(transparent)]
    DbError(#[from] sqlx::Error),

    #[error(transparent)]
    PkiError(#[from] webpki::Error),
}

/// Structure used to store the configuration options for an instance of
/// [AstarteDeviceSdk][crate::AstarteDeviceSdk].
#[derive(Clone)]
pub struct AstarteOptions {
    pub(crate) realm: String,
    pub(crate) device_id: String,
    pub(crate) credentials_secret: String,
    pub(crate) pairing_url: String,
    pub(crate) interfaces: HashMap<String, Interface>,
    pub(crate) database: Option<Arc<dyn AstarteDatabase + Sync + Send>>,
    pub(crate) ignore_ssl_errors: bool,
    pub(crate) keepalive: Duration,
    pub(crate) conn_timeout: Duration,
}

impl AstarteOptions {
    /// Create a new instance of the astarte options.
    ///
    /// ```no_run
    /// use astarte_device_sdk::options::AstarteOptions;
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let realm = "realm_name";
    ///     let device_id = "device_id";
    ///     let credentials_secret = "device_credentials_secret";
    ///     let pairing_url = "astarte_cluster_pairing_url";
    ///
    ///     let mut sdk_options =
    ///         AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url)
    ///             .interface_directory("path/to/interfaces")
    ///             .unwrap()
    ///             .keepalive(std::time::Duration::from_secs(90));
    /// }
    /// ```
    pub fn new(realm: &str, device_id: &str, credentials_secret: &str, pairing_url: &str) -> Self {
        AstarteOptions {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            interfaces: HashMap::new(),
            database: None,
            ignore_ssl_errors: false,
            keepalive: Duration::from_secs(DEFAULT_KEEP_ALIVE),
            conn_timeout: Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT),
        }
    }

    /// Add a database to the astarte options.
    pub fn database<T: AstarteDatabase + 'static + Sync + Send>(mut self, database: T) -> Self {
        self.database = Some(Arc::new(database));
        self
    }

    /// Configure the keep alive timeout.
    ///
    /// The MQTT broker will be pinged when no data exchange has appened
    /// for the duration of the keep alive timeout.
    pub fn keepalive(mut self, duration: Duration) -> Self {
        self.keepalive = duration;

        self
    }

    /// Ignore TLS/SSL certificate errors.
    pub fn ignore_ssl_errors(mut self) -> Self {
        self.ignore_ssl_errors = true;

        self
    }

    /// Add a single interface from the provided `.json` file.
    pub fn interface_file(mut self, file_path: &Path) -> Result<Self, AstarteOptionsError> {
        let interface = Interface::from_file(file_path)?;
        let name = interface.name();
        debug!("Added interface {}", name);
        self.interfaces.insert(name.to_owned(), interface);
        Ok(self)
    }

    /// Add all the interfaces from the `.json` files contained in the specified folder.
    pub fn interface_directory(
        mut self,
        interfaces_directory: &str,
    ) -> Result<Self, AstarteOptionsError> {
        let interface_files = std::fs::read_dir(Path::new(interfaces_directory))?;
        let it = interface_files.filter_map(Result::ok).filter(|f| {
            if let Some(ext) = f.path().extension() {
                ext == "json"
            } else {
                false
            }
        });

        for f in it {
            self = self.interface_file(&f.path())?;
        }

        Ok(self)
    }

    /// Sets the MQTT connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.conn_timeout = timeout;

        self
    }
}
