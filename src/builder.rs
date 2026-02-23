// This file is part of Astarte.
//
// Copyright 2021-2026 SECO Mind Srl
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
use std::num::NonZero;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use astarte_interfaces::Interface;
use tracing::debug;
use tracing::info;

use crate::Error;
use crate::client::DeviceClient;
use crate::connection::DeviceConnection;
use crate::interfaces::Interfaces;
use crate::introspection::AddInterfaceError;
use crate::retention::RetentionError;
use crate::retention::StoredRetention;
use crate::retention::memory::VolatileStore;
use crate::retry::ExponentialIter;
use crate::retry::RandomExponentialIter;
use crate::state::SharedState;
use crate::store::PropertyStore;
use crate::store::StoreCapabilities;
use crate::store::sqlite::SqliteError;
use crate::store::wrapper::StoreWrapper;
use crate::transport::Connection;

/// Default capacity of the channels
///
/// This constant is the default bounded channel size for *both* the rumqttc AsyncClient and
/// EventLoop and the internal channel used by the [`DeviceClient`] and [`DeviceConnection`] to send
/// events data to the external receiver and between each component.
pub const DEFAULT_CHANNEL_SIZE: NonZero<usize> = NonZero::new(50).unwrap();

/// Default capacity for the number of packets with retention volatile to store in memory.
pub const DEFAULT_VOLATILE_CAPACITY: NonZero<usize> = NonZero::new(1000).unwrap();

/// Default capacity for the number of packets w ith retention store to store in memory.
pub const DEFAULT_STORE_CAPACITY: NonZero<usize> = NonZero::new(1_000_000).unwrap();

/// Default connection timeout.
///
/// This is the timeout for establishing a connection for the transport.
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Default send and receive timeout.
///
/// This is not the complete timeout of the whole connection process, it's a timeout applied per
/// request.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Default receiver warning threshold.
pub const DEFAULT_SLOW_RECEIVE_THRESHOLDS: Duration = Duration::from_secs(10);

/// Default maximum delay for the backoff.
pub const DEFAULT_BACKOFF_MAXIMUM_DELAY: Duration = Duration::from_secs(256);

/// Default reset interval for the backoff.
pub const DEFAULT_BACKOFF_RESET_INTERVAL: Duration = Duration::from_secs(256 * 4);

/// Default random jitter percentage to add/subtract to the delay.
pub const DEFAULT_BACKOFF_JITTER_PERCENTAGE: u8 = 50;

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

/// Configuration options and parameters for the connection.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// Optional writable directory
    pub writable_dir: Option<PathBuf>,
    /// Channel size
    pub channel_size: NonZero<usize>,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Send or write timeout
    pub send_timeout: Duration,
    /// Recv or read timeout
    pub recv_timeout: Duration,
    /// Slow receiver threshold for warning
    pub slow_receive: Duration,
    /// Maximum period the backoff will wait for.
    pub exponential_backoff_max: Duration,
    /// Period till we reset the exponential backoff to the default value.
    pub exponential_backoff_reset: Duration,
    /// Percentage jitter to add to the backoff.
    pub exponential_backoff_jitter: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            writable_dir: None,
            channel_size: DEFAULT_CHANNEL_SIZE,
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            send_timeout: DEFAULT_REQUEST_TIMEOUT,
            recv_timeout: DEFAULT_REQUEST_TIMEOUT,
            slow_receive: DEFAULT_SLOW_RECEIVE_THRESHOLDS,
            exponential_backoff_max: DEFAULT_BACKOFF_MAXIMUM_DELAY,
            exponential_backoff_reset: DEFAULT_BACKOFF_RESET_INTERVAL,
            exponential_backoff_jitter: RandomExponentialIter::DEFAULT_RANDOM_JITTER_RANGE,
        }
    }
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
    /// Store used for the connection.
    pub store: S,
    /// Shared state of the connection.
    pub state: Arc<SharedState>,
}

/// Structure used to store the configuration options for an instance of [`DeviceClient`] and
/// [`DeviceConnection`].
#[derive(Debug)]
pub struct DeviceBuilder<C = NoConnect, S = NoStore> {
    config: Config,
    interfaces: Interfaces,
    stored_retention: NonZero<usize>,
    volatile_retention: NonZero<usize>,
    store: S,
    connection_config: C,
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
            volatile_retention: DEFAULT_VOLATILE_CAPACITY,
            stored_retention: DEFAULT_STORE_CAPACITY,
            interfaces: Interfaces::new(),
            connection_config: NoConnect,
            store: NoStore,
            config: Config::default(),
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
    pub fn channel_size(mut self, size: NonZero<usize>) -> Self {
        self.config.channel_size = size;

        self
    }

    /// Configure a writable directory for the device.
    pub fn writable_dir<P>(mut self, path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();

        self.config.writable_dir = Some(path.to_owned());

        self
    }

    /// Set the maximum number of elements that will be kept in memory
    pub fn max_volatile_retention(mut self, items: NonZero<usize>) -> Self {
        self.volatile_retention = items;

        self
    }

    /// Set the timeout used while performing individual HTTP calls
    /// and used while waiting for a connection to the MQTT server.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;

        self
    }

    /// Set the timeout used while sending data on the connection transport
    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.config.send_timeout = timeout;

        self
    }

    /// Set the threshold for slow reception of the Astarte Device
    ///
    /// If the events are not dequeued in a timely manner this could impact the state of the Astarte
    /// connection.
    pub fn slow_receive_threshold(mut self, threshold: Duration) -> Self {
        self.config.slow_receive = threshold;

        self
    }

    /// Sets the maximum timeout generated by the exponential timeout generator.
    /// Used by the connection to wait in between connection retries.
    /// The timeout is an exponential timeout with a random jitter added, this duration limits the maximum
    /// of the exponential part of the timeout, the random jitter will be added on top of this maximum.
    ///
    /// ```text
    /// exponential + random(-jitter..+jitter)
    /// ```
    pub fn exponential_backoff_max(mut self, duration: Duration) -> Self {
        self.config.exponential_backoff_max = duration;

        self
    }

    /// Reset timeout interval, after this interval of time the exponential part of the timeout
    /// will be reset to 0.
    pub fn exponential_backoff_reset(mut self, interval: Duration) -> Self {
        self.config.exponential_backoff_reset = interval;

        self
    }

    /// Percentage of random jitter that will be added to the exponential retry timeout.
    /// From 0 to 100. As an example a jitter percentage of 50 will result in possible timeout values
    /// ranging from -50% to +50% of the exponential timeout.
    /// Values grater than 100 will be clamped to 100.
    pub fn exponential_backoff_jitter(mut self, percentage: u8) -> Self {
        self.config.exponential_backoff_jitter = percentage;

        self
    }
}

impl<C> DeviceBuilder<C, NoStore> {
    /// Set the backing storage for the device.
    ///
    /// This will store and retrieve the device's properties.
    pub fn store<S>(self, store: S) -> DeviceBuilder<C, S>
    where
        S: PropertyStore,
    {
        DeviceBuilder {
            volatile_retention: self.volatile_retention,
            config: self.config,
            stored_retention: self.stored_retention,
            connection_config: self.connection_config,
            interfaces: self.interfaces,
            store,
        }
    }
}

impl<S> DeviceBuilder<NoConnect, S>
where
    S: StoredRetention,
{
    /// Set the maximum number of elements that will be kept in the store
    pub fn max_stored_retention(mut self, items: NonZero<usize>) -> Self {
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
            store: self.store,
            volatile_retention: self.volatile_retention,
            stored_retention: self.stored_retention,
            config: self.config,
            connection_config,
        }
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
        if let Some(path) = &self.config.writable_dir {
            tokio::fs::create_dir_all(path)
                .await
                .map_err(|backtrace| BuilderError::Io {
                    path: path.clone(),
                    backtrace,
                })?;
        }

        let (events_tx, events_rx) = async_channel::bounded(self.config.channel_size.get());
        let (disconnect_tx, disconnect_rx) = async_channel::bounded(1);

        let volatile_store = VolatileStore::with_capacity(self.volatile_retention.get());

        let backoff = RandomExponentialIter::with_jitter(
            ExponentialIter::new(
                self.config.exponential_backoff_max,
                self.config.exponential_backoff_reset,
            ),
            self.config.exponential_backoff_jitter,
        );

        let state = Arc::new(SharedState::new(
            self.config,
            self.interfaces,
            volatile_store,
        ));

        let config = BuildConfig {
            store: self.store,
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

            // NOTE also reset the stored items since this is the first connection and we have no in memory data
            // (this won't be done again until the device is in memory)
            info!("resetting all publish sent flags");
            retention.reset_all_publishes().await?;
        }

        let (client_state, connection_state) = state.split();

        let client = DeviceClient::new(
            sender.clone(),
            events_rx,
            store.clone(),
            client_state,
            disconnect_tx,
        );

        let connection = DeviceConnection::new(
            events_tx,
            disconnect_rx,
            store,
            connection_state,
            connection,
            sender,
            backoff,
        );

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

    use crate::store::memory::MemoryStore;
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

        let builder = DeviceBuilder::new().writable_dir(dir.path());

        assert_eq!(builder.config.writable_dir, Some(dir.path().to_owned()))
    }

    #[tokio::test]
    async fn should_build() {
        let dir = TempDir::new().unwrap();

        // TODO: add expectations
        let mut config = MockConfig::<MemoryStore>::new();
        let mut seq = Sequence::new();

        let tmp_path = Some(dir.path().to_path_buf());

        config
            .expect_connect()
            .once()
            .in_sequence(&mut seq)
            .withf(
                move |BuildConfig {
                          state,
                          ..
                      }| {
                        state.config.writable_dir == tmp_path
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
                .writable_dir(dir.path())
                .store(MemoryStore::default())
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
    fn test_get_introspection_string() {
        let options = DeviceBuilder::new()
            .interface_directory("examples/individual_datastream/interfaces")
            .expect("Failed to set interface directory");

        let ifa = options.interfaces;

        let expected = [
            "org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream:0:1",
            "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream:0:1",
        ];

        let intro = ifa.get_introspection_string();
        let mut res: Vec<&str> = intro.split(';').collect();

        res.sort_unstable();

        assert_eq!(res, expected);
    }
}
