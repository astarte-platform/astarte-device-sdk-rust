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

use std::sync::Arc;

pub use interface::Interface;

use crate::database::AstarteDatabase;
use crate::interface::{self};
use crate::AstarteError;

/// Builder for Astarte client
///
/// ```no_run
/// use astarte_sdk::builder::AstarteOptions;
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
    pub(crate) interfaces: crate::interfaces::Interfaces,
    pub(crate) database: Option<Arc<dyn AstarteDatabase + Sync + Send>>,
    pub(crate) ignore_ssl_errors: bool,
    pub(crate) keepalive: std::time::Duration,
}

impl AstarteOptions {
    pub fn new(realm: &str, device_id: &str, credentials_secret: &str, pairing_url: &str) -> Self {
        AstarteOptions {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            interfaces: Default::default(),
            database: None,
            ignore_ssl_errors: false,
            keepalive: std::time::Duration::from_secs(30),
        }
    }

    pub fn database<'a, T: AstarteDatabase + 'static + Sync + Send>(
        &'a mut self,
        database: T,
    ) -> &'a mut Self {
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

    pub fn interface_directory(
        &mut self,
        interfaces_directory: &str,
    ) -> Result<&mut Self, AstarteError> {
        self.interfaces
            .add_interface_directory(interfaces_directory)?;

        Ok(self)
    }

    pub fn interface_file(&mut self, interface_file: &str) -> Result<&mut Self, AstarteError> {
        self.interfaces.add_interface_file(interface_file)?;
        Ok(self)
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}
