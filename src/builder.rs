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
//! [AstarteDeviceSdk].

use std::ffi::OsStr;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use log::debug;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::connection::mqtt::AsyncClient;
use crate::connection::mqtt::Mqtt;
use crate::connection::Connection;
use crate::connection::Introspection;
use crate::crypto::CryptoError;
use crate::interface::{Interface, InterfaceError};
use crate::interfaces::Interfaces;
use crate::pairing;
use crate::store::wrapper::StoreWrapper;
use crate::store::PropertyStore;
use crate::AstarteDeviceSdk;
use crate::EventReceiver;

/// Default capacity of the channels
///
/// This constant is the default bounded channel size for *both* the rumqttc AsyncClient and EventLoop
/// and the internal channel used by the [`AstarteDeviceSdk`] to send events data to the external receiver.
pub const MQTT_CHANNEL_SIZE: usize = 50;

/// Astarte options error.
///
/// Possible errors used by the Astarte options module.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum BuilderError {
    #[error("private key or CSR creation failed")]
    CryptoGeneration(#[from] CryptoError),

    #[error("device must have at least an interface")]
    MissingInterfaces,

    #[error("error creating interface")]
    Interface(#[from] InterfaceError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("configuration error")]
    ConfigError(String),

    #[error(transparent)]
    MqttError(#[from] rumqttc::ClientError),

    #[error("pairing error")]
    PairingError(#[from] pairing::PairingError),

    #[error(transparent)]
    DbError(#[from] sqlx::Error),

    #[error(transparent)]
    PkiError(#[from] webpki::Error),
}

#[async_trait]
pub trait DeviceBuilderConnect<S, C>
where
    C: ConnectionConfig + Send,
    S: PropertyStore,
{
    async fn connect(self, config: C) -> Result<DeviceBuilder<S, C::Con>, crate::Error>;
}

pub trait DeviceBuilderBuild<S, C> {
    fn build(self) -> (AstarteDeviceSdk<S, C>, EventReceiver);
}

#[async_trait]
impl<S, C, T> DeviceBuilderConnect<S, C> for DeviceBuilder<S, T>
where
    S: PropertyStore + Send,
    C: ConnectionConfig + Send + 'static,
    C::Con: Connection<S> + Send + Sync,
    T: Send,
{
    async fn connect(self, config: C) -> Result<DeviceBuilder<S, C::Con>, crate::Error> {
        let connection = config.build().await?;

        connection
            .connect(Introspection::from_unlocked(&self.interfaces, &self.store).await?)
            .await?;

        Ok(DeviceBuilder {
            channel_size: self.channel_size,
            interfaces: self.interfaces,
            connection,
            store: self.store,
        })
    }
}

impl<S, C> DeviceBuilderBuild<S, C> for DeviceBuilder<S, C>
where
    S: PropertyStore,
    C: Connection<S> + Send,
{
    fn build(self) -> (AstarteDeviceSdk<S, C>, EventReceiver) {
        let (tx, rx) = mpsc::channel(self.channel_size);

        let device = AstarteDeviceSdk::new(self.interfaces, self.store.store, self.connection, tx);

        (device, rx)
    }
}

/// Structure used to store the configuration options for an instance of
/// [AstarteDeviceSdk].
#[derive(Clone)]
pub struct DeviceBuilder<S, C> {
    pub(crate) channel_size: usize,
    pub(crate) interfaces: Interfaces,
    pub(crate) connection: C,
    pub(crate) store: StoreWrapper<S>,
}

impl DeviceBuilder<(), ()> {
    /// Create a new instance of the DeviceBuilder.
    /// Has a default [`DeviceBuilder::channel_size`] that equals to [`MQTT_CHANNEL_SIZE`].
    ///
    /// ```no_run
    /// use astarte_device_sdk::builder::{DeviceBuilder, MqttConfig};
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut builder =
    ///         DeviceBuilder::new()
    ///             .interface_directory("path/to/interfaces")
    ///             .unwrap();
    /// }
    /// ```
    pub fn new() -> Self {
        Self {
            channel_size: MQTT_CHANNEL_SIZE,
            interfaces: Interfaces::new(),
            connection: (),
            store: StoreWrapper::new(()),
        }
    }
}

impl<S, C> DeviceBuilder<S, C> {
    /// Add a single interface from the provided `.json` file.
    ///
    /// It will validate that the interfaces are the same, or a newer version of the interfaces
    /// with the same name that are already present.
    pub fn interface_file(mut self, file_path: &Path) -> Result<Self, BuilderError> {
        let interface = Interface::from_file(file_path)?;
        let name = interface.interface_name();

        debug!("Added interface {}", name);

        self.interfaces.add(interface)?;

        Ok(self)
    }

    /// Add all the interfaces from the `.json` files contained in the specified folder.
    pub fn interface_directory(self, interfaces_directory: &str) -> Result<Self, BuilderError> {
        walk_dir_json(interfaces_directory)?
            .iter()
            .try_fold(self, |acc, path| acc.interface_file(path))
    }

    /// This method configures the bounded channel size.
    ///
    pub fn channel_size(mut self, size: usize) -> Self {
        self.channel_size = size;

        self
    }

    /// Set the backing storage for the device.
    ///
    /// This will store and retrieve the device's properties.
    pub fn store<T>(self, store: T) -> DeviceBuilder<T, C>
    where
        T: PropertyStore,
    {
        DeviceBuilder {
            channel_size: self.channel_size,
            interfaces: self.interfaces,
            connection: self.connection,
            store: StoreWrapper::new(store),
        }
    }
}

impl<S, C> Debug for DeviceBuilder<S, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AstarteOptions")
            .field("interfaces", &self.interfaces)
            // We manually implement Debug for the store, so we can avoid have a trait bound on
            // `S` to implement [Display].
            .finish_non_exhaustive()
    }
}

impl Default for DeviceBuilder<(), ()> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Deserialize)]
pub struct MqttConfig {
    pub(crate) realm: String,
    pub(crate) device_id: String,
    pub(crate) credentials_secret: String,
    pub(crate) pairing_url: String,
    pub(crate) ignore_ssl_errors: bool,
    pub(crate) keepalive: std::time::Duration,
    pub(crate) bounded_channel_size: usize,
}

impl MqttConfig {
    /// Create a new instance of MqttConfig
    ///
    /// As a default this configuration:
    ///    - does not ignore SSL errors.
    ///    - has a keepalive of 30 seconds
    ///    - has a default bounded channel size of [`MQTT_CHANNEL_SIZE`]
    ///
    /// ```no_run
    /// use astarte_device_sdk::builder::MqttConfig;
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let realm = "realm_name";
    ///     let device_id = "device_id";
    ///     let credentials_secret = "device_credentials_secret";
    ///     let pairing_url = "astarte_cluster_pairing_url";
    ///
    ///     let mut mqtt_options =
    ///         MqttConfig::new(&realm, &device_id, &credentials_secret, &pairing_url);
    /// }
    /// ```
    pub fn new(realm: &str, device_id: &str, credentials_secret: &str, pairing_url: &str) -> Self {
        Self {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            ignore_ssl_errors: false,
            keepalive: std::time::Duration::from_secs(30),
            bounded_channel_size: MQTT_CHANNEL_SIZE,
        }
    }

    /// Configure the keep alive timeout.
    ///
    /// The MQTT broker will be pinged when no data exchange has appened
    /// for the duration of the keep alive timeout.
    pub fn keepalive(&mut self, duration: std::time::Duration) -> &mut Self {
        self.keepalive = duration;

        self
    }

    /// Ignore TLS/SSL certificate errors.
    pub fn ignore_ssl_errors(&mut self) -> &mut Self {
        self.ignore_ssl_errors = true;

        self
    }

    /// Sets the size for the underlying bounded channel used by the eventloop of [`rumqttc`]
    pub fn bounded_channel_size(&mut self, bounded_channel_size: usize) -> &mut Self {
        self.bounded_channel_size = bounded_channel_size;

        self
    }
}

impl Debug for MqttConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttOptions")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            .field("credentials_secret", &"REDACTED")
            .field("pairing_url", &self.pairing_url)
            .field("ignore_ssl_errors", &self.ignore_ssl_errors)
            .field("keepalive", &self.keepalive)
            .finish_non_exhaustive()
    }
}

#[async_trait]
pub trait ConnectionConfig {
    type Con;

    async fn build(self) -> Result<Self::Con, crate::Error>;
}

#[async_trait]
impl ConnectionConfig for MqttConfig {
    type Con = Mqtt;

    async fn build(self) -> Result<Self::Con, crate::Error> {
        let mqtt_options = pairing::get_transport_config(&self).await?;

        debug!("{:#?}", mqtt_options);

        let (client, eventloop) = AsyncClient::new(mqtt_options, self.bounded_channel_size);

        Ok(Mqtt::new(self.realm, self.device_id, eventloop, client))
    }
}

/// Walks a directory returning an array of json files
fn walk_dir_json<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>, io::Error> {
    std::fs::read_dir(path)?
        .map(|res| {
            res.and_then(|entry| {
                let path = entry.path();
                let metadata = entry.metadata()?;

                Ok((path, metadata))
            })
        })
        .filter_map(|res| match res {
            Ok((path, metadata)) => {
                if metadata.is_file() && path.extension() == Some(OsStr::new("json")) {
                    Some(Ok(path))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        })
        .collect()
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::{DeviceBuilder, MqttConfig};

    #[test]
    fn interface_directory() {
        let res =
            DeviceBuilder::new().interface_directory("examples/individual_datastream/interfaces");

        assert!(
            res.is_ok(),
            "Failed to load interfaces from directory: {:?}",
            res
        );
    }

    #[test]
    fn interface_existing_directory() {
        let res = DeviceBuilder::new()
            .interface_directory("examples/individual_datastream/interfaces")
            .unwrap()
            .interface_directory("examples/individual_datastream/interfaces");

        assert!(
            res.is_ok(),
            "Failed to load interfaces from directory: {:?}",
            res
        );
    }

    #[test]
    fn test_default_mqtt_config() {
        let mqtt_config = MqttConfig::new("test", "test", "test", "test");

        assert_eq!(mqtt_config.realm, "test");
        assert_eq!(mqtt_config.device_id, "test");
        assert_eq!(mqtt_config.credentials_secret, "test");
        assert_eq!(mqtt_config.pairing_url, "test");
        assert_eq!(mqtt_config.keepalive, Duration::from_secs(30));
        assert!(!mqtt_config.ignore_ssl_errors);
    }

    #[test]
    fn test_override_mqtt_config() {
        let mut mqtt_config = MqttConfig::new("test", "test", "test", "test");

        mqtt_config
            .ignore_ssl_errors()
            .keepalive(Duration::from_secs(60));

        assert_eq!(mqtt_config.realm, "test");
        assert_eq!(mqtt_config.device_id, "test");
        assert_eq!(mqtt_config.credentials_secret, "test");
        assert_eq!(mqtt_config.pairing_url, "test");
        assert_eq!(mqtt_config.keepalive, Duration::from_secs(60));
        assert!(mqtt_config.ignore_ssl_errors);
    }

    #[test]
    fn test_redacted_credentials_secret() {
        let mqtt_config = MqttConfig::new("test", "test", "secret=", "test");

        let debug_string = format!("{:?}", mqtt_config);

        assert!(!debug_string.contains("secret="));
        assert!(debug_string.contains("REDACTED"));
    }
}
