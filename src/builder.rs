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

use log::debug;
use openssl::error::ErrorStack;
use pairing::PairingError;

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use interface::traits::Interface as InterfaceTrait;
pub use interface::Interface;

use crate::database::AstarteDatabase;
use crate::interface::{self};
use crate::pairing;

/// Builder for Astarte client
///
/// ```no_run
/// use astarte_device_sdk::builder::AstarteOptions;
///
/// #[tokio::main]
/// async fn main(){
/// let realm = "test";
/// let device_id = "xxxxxxxxxxxxxxxxxxxxxxx";
/// let credentials_secret = "xxxxxxxxxxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxxxx";
/// let pairing_url = "https://api.example.com/pairing";
///
/// let mut sdk_builder = AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url)
///                         .interface_directory("path/to/interfaces").unwrap()
///                         .keepalive(std::time::Duration::from_secs(90))
///                         .build();
///
/// }
///
///
/// ```

#[derive(Clone)]
pub struct AstarteOptions {
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

impl AstarteOptions {
    pub fn new(realm: &str, device_id: &str, credentials_secret: &str, pairing_url: &str) -> Self {
        AstarteOptions {
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

    pub fn database<T: AstarteDatabase + 'static + Sync + Send>(
        &mut self,
        database: T,
    ) -> &mut Self {
        self.database = Some(Arc::new(database));
        self
    }

    /// Set time after which client should ping the broker
    /// if there is no other data exchange
    pub fn keepalive(&mut self, duration: std::time::Duration) -> &mut Self {
        self.keepalive = duration;

        self
    }

    pub fn ignore_ssl_errors(&mut self) -> &mut Self {
        self.ignore_ssl_errors = true;

        self
    }

    /// Add an interface from a json file
    pub fn interface_file<'a>(
        &'a mut self,
        file_path: &Path,
    ) -> Result<&'a mut Self, AstarteBuilderError> {
        let interface = Interface::from_file(file_path)?;
        let name = interface.name();
        debug!("Added interface {}", name);
        self.interfaces.insert(name.to_owned(), interface);
        Ok(self)
    }

    /// Add all json interface description inside a specified directory
    pub fn interface_directory<'a>(
        &'a mut self,
        interfaces_directory: &str,
    ) -> Result<&'a mut Self, AstarteBuilderError> {
        let interface_files = std::fs::read_dir(Path::new(interfaces_directory))?;
        let it = interface_files.filter_map(Result::ok).filter(|f| {
            if let Some(ext) = f.path().extension() {
                ext == "json"
            } else {
                false
            }
        });

        for f in it {
            self.interface_file(&f.path())?;
        }

        Ok(self)
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}
