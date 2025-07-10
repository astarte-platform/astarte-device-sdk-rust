// This file is part of Astarte.
//
// Copyright 2021 - 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Provides functionality to configure an instance of the [`DeviceClient`] and
//! [`DeviceConnection`].

use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use astarte_interfaces::Interface;
use tracing::debug;

use crate::client::DeviceClient;
use crate::connection::DeviceConnection;
use crate::interfaces::Interfaces;
use crate::introspection::AddInterfaceError;
use crate::retention::memory::VolatileStore;
use crate::retention::RetentionError;
use crate::retention::StoredRetention;
use crate::state::SharedState;
use crate::store::sqlite::SqliteError;
use crate::store::wrapper::StoreWrapper;
use crate::store::PropertyStore;
use crate::store::SqliteStore;
use crate::store::StoreCapabilities;
use crate::transport::Connection;
use crate::Error;

/// Default capacity of the channels
///
/// This constant is the default bounded channel size for *both* the rumqttc AsyncClient and
/// EventLoop and the internal channel used by the [`DeviceClient`] and [`DeviceConnection`] to send
/// events data to the external receiver and between each component.
pub const DEFAULT_CHANNEL_SIZE: usize = 50;

/// Default capacity for the number of packets with retention volatile to store in memory.
pub const DEFAULT_VOLATILE_CAPACITY: usize = 1000;

/// Default capacity for the number of packets w ith retention store to store in memory.
pub const DEFAULT_STORE_CAPACITY: NonZeroUsize = const_non_zero_usize(1_000_000);

/// Necessary for rust 1.78 const compatibility
pub(crate) const fn const_non_zero_usize(v: usize) -> NonZeroUsize {
    let Some(v) = NonZeroUsize::new(v) else {
        panic!("value cannot be zero");
    };

    v
}

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
    /// Couldn't set the maximum number of items in the store
    #[error("couldn't set the maximum number of items in the store")]
    Retention(#[from] RetentionError),
}

/// Marker struct to identify a builder with no store configured
#[derive(Debug, Clone, Copy)]
pub struct NoStore;
/// Marker struct to identify a builder with no connection configured
#[derive(Debug, Clone, Copy)]
pub struct NoConnect;

/// Struct used to pass the connection configuration to the [`ConnectionConfig`]
#[derive(Debug)]
pub struct BuildConfig<S> {
    pub(crate) channel_size: usize,
    pub(crate) writable_dir: Option<PathBuf>,
    pub(crate) store: S,
    pub(crate) state: Arc<SharedState>,
}

/// Structure used to store the configuration options for an instance of [`DeviceClient`] and
/// [`DeviceConnection`].
#[derive(Debug)]
pub struct DeviceBuilder<C = NoConnect, S = NoStore> {
    pub(crate) channel_size: usize,
    pub(crate) volatile_retention: usize,
    pub(crate) stored_retention: NonZeroUsize,
    pub(crate) store: S,
    pub(crate) connection_config: C,
    pub(crate) interfaces: Interfaces,
    pub(crate) writable_dir: Option<PathBuf>,
}

impl DeviceBuilder<NoConnect, NoStore> {
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
            volatile_retention: DEFAULT_VOLATILE_CAPACITY,
            stored_retention: DEFAULT_STORE_CAPACITY,
            writable_dir: None,
            interfaces: Interfaces::new(),
            connection_config: NoConnect,
            store: NoStore,
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

    /// Set the maximum number of elements that will be kept in memory
    pub fn max_volatile_retention(mut self, items: NonZeroUsize) -> Self {
        self.volatile_retention = items.get();

        self
    }
}

impl<C> DeviceBuilder<C, NoStore> {
    /// Configure a writable directory and initializes the [`SqliteStore`] in it.
    pub async fn store_dir<P>(
        mut self,
        path: P,
    ) -> Result<DeviceBuilder<C, SqliteStore>, BuilderError>
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
    pub fn store<S>(self, store: S) -> DeviceBuilder<C, S>
    where
        S: PropertyStore,
    {
        DeviceBuilder {
            interfaces: self.interfaces,
            connection_config: self.connection_config,
            store,
            stored_retention: self.stored_retention,
            channel_size: self.channel_size,
            volatile_retention: self.volatile_retention,
            writable_dir: self.writable_dir,
        }
    }
}

impl<S> DeviceBuilder<NoConnect, S>
where
    S: StoredRetention,
{
    /// Set the maximum number of elements that will be kept in the store
    pub fn max_stored_retention(mut self, items: NonZeroUsize) -> Self {
        self.stored_retention = items;

        self
    }
}

impl<S> DeviceBuilder<NoConnect, S>
where
    S: PropertyStore,
{
    /// Configure the connection using the passed [`ConnectionConfig`].
    ///
    /// If the connection gets established correctly, the caller can than construct
    /// the [`DeviceClient`] and [`DeviceConnection`] using the [`DeviceBuilder::build`] method.
    pub fn connection<C>(self, connection_config: C) -> DeviceBuilder<C, S>
    where
        C: ConnectionConfig<S>,
    {
        DeviceBuilder {
            interfaces: self.interfaces,
            connection_config,
            store: self.store,
            channel_size: self.channel_size,
            volatile_retention: self.volatile_retention,
            stored_retention: self.stored_retention,
            writable_dir: self.writable_dir,
        }
    }
}

// NOTE: Currently volatile retention is not supported for the Grpc connection.
// Currently messages won't be retained or stored (see [`crate::transport::store::GrpcStore`])
impl<S> DeviceBuilder<S, crate::transport::mqtt::MqttConfig> {
    /// Sets the number of elements with retention volatile that will be kept in memory if
    /// disconnected.
    pub fn volatile_retention(mut self, items: usize) -> Self {
        self.volatile_retention = items;

        self
    }
}

type BuildRes<C> = (DeviceClient<C>, DeviceConnection<C>);

impl<C, S> DeviceBuilder<C, S>
where
    S: StoreCapabilities,
    C: ConnectionConfig<S>,
    Error: From<C::Err>,
{
    /// Method that consumes the builder and returns a working [`DeviceClient`] and
    /// [`DeviceConnection`] with the specified settings.
    pub async fn build(self) -> Result<BuildRes<C::Conn>, Error> {
        // We use the flume channel to have a cloneable receiver, see the comment on the DeviceClient for more information.
        let (tx_connection, rx_client) = flume::bounded(self.channel_size);

        let volatile_store = VolatileStore::with_capacity(self.volatile_retention);

        let state = Arc::new(SharedState::new(self.interfaces, volatile_store));

        let config = BuildConfig {
            store: self.store,
            channel_size: self.channel_size,
            writable_dir: self.writable_dir,
            state: Arc::clone(&state),
        };

        let DeviceTransport {
            connection,
            sender,
            store,
        } = self.connection_config.connect(config).await?;

        // set max retention items in the store
        if let Some(retention) = store.get_retention() {
            retention
                .set_max_retention_items(self.stored_retention)
                .await?;
        }

        let client =
            DeviceClient::new(sender.clone(), rx_client, store.clone(), Arc::clone(&state));

        let connection = DeviceConnection::new(tx_connection, store, state, connection, sender);

        Ok((client, connection))
    }
}

impl Default for DeviceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Structure that stores a successfully established connection
pub struct DeviceTransport<C>
where
    C: Connection,
{
    pub(crate) connection: C,
    pub(crate) sender: C::Sender,
    pub(crate) store: StoreWrapper<C::Store>,
}

/// Crate private connection implementation.
pub trait ConnectionConfig<S> {
    /// Type of the store used by the connection
    type Store: StoreCapabilities;
    /// Type of the constructed Connection
    type Conn: Connection;
    /// Type of the error got while opening the connection
    type Err;

    /// Connect method that consumes self to construct a working connection
    /// This method is called internally by the builder.
    fn connect(
        self,
        config: BuildConfig<S>,
    ) -> impl Future<Output = Result<DeviceTransport<Self::Conn>, Self::Err>> + Send
    where
        S: PropertyStore;
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
    use std::time::Duration;

    use mockall::Sequence;
    use tempfile::TempDir;

    use crate::test::DEVICE_PROPERTIES;
    use crate::transport::mock::{MockCon, MockConfig, MockSender};

    use super::*;

    #[test]
    fn interface_directory() {
        let res =
            DeviceBuilder::new().interface_directory("examples/individual_datastream/interfaces");

        assert!(
            res.is_ok(),
            "Failed to load interfaces from directory: {res:?}"
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
            "Failed to load interfaces from directory: {res:?}"
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

    #[tokio::test]
    async fn should_dir_with_store_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        DeviceBuilder::new()
            .store_dir(path.join("not existing "))
            .await
            .expect_err("Should error for non existing directory");
        let file = path.join("file");

        tokio::fs::write(&file, "content").await.unwrap();

        DeviceBuilder::new()
            .store_dir(file)
            .await
            .expect_err("Should error for file and not dir");

        let readonly = path.join("dir");
        tokio::fs::create_dir(&readonly).await.unwrap();

        let mut perm = readonly.metadata().unwrap().permissions();

        perm.set_readonly(true);

        tokio::fs::set_permissions(&readonly, perm).await.unwrap();

        DeviceBuilder::new()
            .store_dir(readonly)
            .await
            .expect_err("Should error for readonly");
    }

    #[tokio::test]
    async fn should_build() {
        let dir = TempDir::new().unwrap();

        // TODO: add expectations
        let mut config = MockConfig::<SqliteStore>::new();
        let mut seq = Sequence::new();

        let tmp_path = Some(dir.path().to_path_buf());

        config
            .expect_connect()
            .once()
            .in_sequence(&mut seq)
            .withf(
                move |BuildConfig {
                          channel_size,
                          writable_dir,
                          store: _,
                          state,
                      }| {
                    channel_size == channel_size
                        && *writable_dir == tmp_path
                        && state.interfaces.try_read().unwrap().get("org.astarte-platform.rust.examples.individual-properties.DeviceProperties").is_some()
                },
            )
            .returning(|config| {
                let mut sender = MockSender::new();
                let mut seq = Sequence::new();
                sender.expect_clone().once().in_sequence(&mut seq).returning(MockSender::new);

                Ok(DeviceTransport {
                    connection: MockCon::new(),
                    sender,
                    store: StoreWrapper::new(config.store),
                })
            });

        let (_client, _connection) = tokio::time::timeout(
            Duration::from_secs(3),
            DeviceBuilder::new()
                .store_dir(dir.path())
                .await
                .unwrap()
                .interface_str(DEVICE_PROPERTIES)
                .unwrap()
                .connection(config)
                .build(),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "value cannot be zero")]
    fn const_non_zero_usize_should_panic() {
        const_non_zero_usize(0);
    }
}
