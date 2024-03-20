/*
 * This file is part of Astarte.
 *
 * Copyright 2021-2023 SECO Mind Srl
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
//! Provides functionality to configure an instance of the [`DeviceClient`] and
//! [`DeviceConnection`].

use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use crate::client::DeviceClient;
use crate::connection::DeviceConnection;
use crate::interface::Interface;
use crate::interfaces::Interfaces;
use crate::introspection::AddInterfaceError;
use crate::store::sqlite::SqliteError;
use crate::store::wrapper::StoreWrapper;
use crate::store::PropertyStore;
use crate::store::SqliteStore;
use crate::transport::{Publish, Receive, Register};

/// Default capacity of the channels
///
/// This constant is the default bounded channel size for *both* the rumqttc AsyncClient and
/// EventLoop and the internal channel used by the [`DeviceClient`] and [`DeviceConnection`] to send
/// events data to the external receiver and between each component.
pub const DEFAULT_CHANNEL_SIZE: usize = 50;

/// Astarte builder error.
///
/// Possible errors used by the Astarte builder module.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum BuilderError {
    /// Failed to read interface directory
    #[error("couldn't read interface path {}", .path.display())]
    Io {
        /// Path to the interface file.
        path: PathBuf,
        /// Reason why the file couldn't be read.
        #[source]
        backtrace: io::Error,
    },
    /// Couldn't get the metadata of the writable directory
    #[error("couldn't get metadata for {}", .path.display())]
    DirectoryMetadata {
        /// Path to the interface directory.
        path: PathBuf,
        /// Reason why the directory or file couldn't be read.
        #[source]
        backtrace: io::Error,
    },
    /// Provided path is not a directory
    #[error("invalid directory at {}", .0.display())]
    NotADirectory(PathBuf),
    /// The provided directory is read only
    #[error("directory is read only {}", .0.display())]
    DirectoryReadonly(PathBuf),
    /// Couldn't connect to the SQLite store
    #[error("couldn't connect to the SQLite store")]
    Sqlite(#[from] SqliteError),
}

/// Declares the conclusive operation of the device builder.
///
/// This trait is already implemented generically for the [`DeviceBuilder`]
/// and implementing it should be avoided since it has no practical use.
pub trait DeviceSdkBuild<S, C> {
    /// Method that consumes the builder and returns a working [`DeviceClient`] and
    /// [`DeviceConnection`] with the specified settings.
    fn build(self) -> (DeviceClient<S>, DeviceConnection<S, C>);
}

impl<S, C> DeviceSdkBuild<S, C> for DeviceBuilder<S, C>
where
    S: PropertyStore,
    C: Publish + Receive + Register + Send,
{
    fn build(self) -> (DeviceClient<S>, DeviceConnection<S, C>) {
        // We use the flume channel to have a clonable receiver, see the comment on the DeviceClient for more information.
        let (tx_connection, rx_client) = flume::bounded(self.channel_size);
        let (tx_client, rx_connection) = mpsc::channel(self.channel_size);

        let interfaces = Arc::new(RwLock::new(self.interfaces));

        let client = DeviceClient::new(
            Arc::clone(&interfaces),
            rx_client,
            tx_client,
            self.store.clone(),
        );
        let connection = DeviceConnection::new(
            interfaces,
            tx_connection,
            rx_connection,
            self.store,
            self.connection,
        );

        (client, connection)
    }
}

/// Structure used to store the configuration options for an instance of [`DeviceClient`] and
/// [`DeviceConnection`].
#[derive(Clone)]
pub struct DeviceBuilder<S, C> {
    pub(crate) channel_size: usize,
    pub(crate) interfaces: Interfaces,
    pub(crate) connection: C,
    pub(crate) store: StoreWrapper<S>,
    pub(crate) writable_dir: Option<PathBuf>,
}

impl DeviceBuilder<(), ()> {
    /// Create a new instance of the DeviceBuilder.
    /// Has a default [`DeviceBuilder::channel_size`] that equals to [`crate::builder::DEFAULT_CHANNEL_SIZE`].
    ///
    /// ```no_run
    /// use astarte_device_sdk::{builder::DeviceBuilder, transport::mqtt::MqttConfig};
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
            channel_size: DEFAULT_CHANNEL_SIZE,
            interfaces: Interfaces::new(),
            connection: (),
            store: StoreWrapper::new(()),
            writable_dir: None,
        }
    }
}

impl<S, C> DeviceBuilder<S, C> {
    /// Add a single interface from the provided `.json` file.
    ///
    /// If an interface with the same name is present, the code will validate
    /// the passed interface to ensure it has a newer version than the one stored.
    pub fn interface_file<P>(self, path: P) -> Result<Self, AddInterfaceError>
    where
        P: AsRef<Path>,
    {
        let interface = fs::read_to_string(path.as_ref()).map_err(|err| AddInterfaceError::Io {
            path: path.as_ref().to_path_buf(),
            backtrace: err,
        })?;

        self.interface_str(&interface)
            .map_err(|err| err.add_path_context(path.as_ref().to_owned()))
    }

    /// Add a single interface from the provided string.
    ///
    /// If an interface with the same name is present, the code will validate
    /// the passed interface to ensure it has a newer version than the one stored.
    pub fn interface_str(self, interface: &str) -> Result<Self, AddInterfaceError> {
        let interface = Interface::from_str(interface)?;

        self.interface(interface)
    }

    /// Add a single interface.
    ///
    /// If an interface with the same name is present, the code will validate
    /// the passed interface to ensure it has a newer version than the one stored.
    pub fn interface(mut self, interface: Interface) -> Result<Self, AddInterfaceError> {
        debug!("adding interface {}", interface.interface_name());

        let interface = self.interfaces.validate(interface)?;

        let Some(interface) = interface else {
            debug!("interface already present");

            return Ok(self);
        };

        self.interfaces.add(interface);

        Ok(self)
    }

    /// Add all the interfaces from the `.json` files contained in the specified folder.
    pub fn interface_directory<P>(self, interfaces_directory: P) -> Result<Self, AddInterfaceError>
    where
        P: AsRef<Path>,
    {
        walk_dir_json(&interfaces_directory)
            .map_err(|err| AddInterfaceError::Io {
                path: interfaces_directory.as_ref().to_path_buf(),
                backtrace: err,
            })?
            .iter()
            .try_fold(self, |acc, path| acc.interface_file(path))
    }

    /// This method configures the bounded channel size.
    pub fn channel_size(mut self, size: usize) -> Self {
        self.channel_size = size;

        self
    }

    /// Configure a writable directory for the device.
    pub fn writable_dir<P>(mut self, path: P) -> Result<Self, BuilderError>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let metadata = path
            .metadata()
            .map_err(|err| BuilderError::DirectoryMetadata {
                path: path.to_owned(),
                backtrace: err,
            })?;

        if !metadata.is_dir() {
            return Err(BuilderError::NotADirectory(path.to_owned()));
        }

        if metadata.permissions().readonly() {
            return Err(BuilderError::DirectoryReadonly(path.to_owned()));
        }

        self.writable_dir = Some(path.to_owned());

        Ok(self)
    }

    /// Configure a writable directory and initializes the [`SqliteStore`] in it.
    pub async fn store_dir<P>(
        mut self,
        path: P,
    ) -> Result<DeviceBuilder<SqliteStore, C>, BuilderError>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();

        self = self.writable_dir(path)?;

        let store = SqliteStore::connect(path).await?;

        Ok(self.store(store))
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
            writable_dir: self.writable_dir,
        }
    }

    /// Establishes the connection using the passed [`ConnectionConfig`].
    ///
    /// If the connection gets established correctly, the caller can than construct
    /// the [`DeviceClient`] and [`DeviceConnection`] using the [`DeviceSdkBuild::build`] method.
    pub async fn connect<T>(self, config: T) -> Result<DeviceBuilder<S, T::Con>, crate::Error>
    where
        S: PropertyStore,
        T: ConnectionConfig,
        crate::Error: From<<T as ConnectionConfig>::Err>,
        C: Send + Sync,
    {
        let connection = config.connect(&self).await?;

        Ok(DeviceBuilder {
            channel_size: self.channel_size,
            interfaces: self.interfaces,
            connection,
            store: self.store,
            writable_dir: self.writable_dir,
        })
    }
}

impl<S, C> Debug for DeviceBuilder<S, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AstarteOptions")
            .field("channel_size", &self.channel_size)
            .field("interfaces", &self.interfaces)
            .field("writable_dir", &self.writable_dir)
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

/// Generic connection configuration that enables the builder
/// to work with different types of transport.
/// You can pass types implementing this trait to the [`DeviceBuilder::connect`] method.
///
/// This trait is already implemented internally
/// and implementing it should be avoided since it has no practical use.
#[async_trait]
pub trait ConnectionConfig {
    /// Type of the constructed Connection
    type Con;
    /// Type of the error got while opening the connection
    type Err;

    /// Connect method that consumes self to construct a working connection
    /// This method is called internally by the builder.
    async fn connect<S, C>(self, builder: &DeviceBuilder<S, C>) -> Result<Self::Con, Self::Err>
    where
        S: PropertyStore,
        C: Send + Sync;
}

/// Walks a directory returning an array of json files
fn walk_dir_json<P>(path: P) -> Result<Vec<PathBuf>, io::Error>
where
    P: AsRef<Path>,
{
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
    use tempfile::TempDir;

    use super::DeviceBuilder;

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
    fn should_get_writable_path() {
        let dir = TempDir::new().unwrap();

        let builder = DeviceBuilder::new().writable_dir(dir.path()).unwrap();

        assert_eq!(builder.writable_dir, Some(dir.path().to_owned()))
    }

    #[tokio::test]
    async fn should_dir_with_store() {
        let dir = TempDir::new().unwrap();

        let builder = DeviceBuilder::new().store_dir(dir.path()).await.unwrap();

        assert_eq!(builder.writable_dir, Some(dir.path().to_owned()))
    }
}
