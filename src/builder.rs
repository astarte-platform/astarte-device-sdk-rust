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
use std::future::Future;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::debug;

use crate::client::DeviceClient;
use crate::connection::DeviceConnection;
use crate::interface::Interface;
use crate::interfaces::Interfaces;
use crate::introspection::AddInterfaceError;
use crate::retention::memory::SharedVolatileStore;
use crate::store::sqlite::SqliteError;
use crate::store::wrapper::StoreWrapper;
use crate::store::PropertyStore;
use crate::store::SqliteStore;
use crate::transport::Connection;

/// Default capacity of the channels
///
/// This constant is the default bounded channel size for *both* the rumqttc AsyncClient and
/// EventLoop and the internal channel used by the [`DeviceClient`] and [`DeviceConnection`] to send
/// events data to the external receiver and between each component.
pub const DEFAULT_CHANNEL_SIZE: usize = 50;

/// Default capacity for the number of packets with retention volatile to store in memory.
pub const DEFAULT_VOLATILE_CAPACITY: usize = 1000;

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

/// Marker struct to identify a builder with no store configured
#[derive(Debug, Clone, Copy)]
pub struct NoStore;
/// Marker struct to identify a builder with no connection configured
#[derive(Debug, Clone, Copy)]
pub struct NoConnect;

/// Configuration struct for the device, used internally by the builder
#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) channel_size: usize,
    pub(crate) volatile_retention: usize,
    pub(crate) volatile: SharedVolatileStore,
    pub(crate) writable_dir: Option<PathBuf>,
}

/// Struct used to pass the connection configuration to the [`ConnectionConfig`]
#[derive(Debug, Clone)]
pub struct ConnectionBuildConfig<'a, S> {
    pub(crate) store: S,
    pub(crate) interfaces: &'a Interfaces,
    pub(crate) config: Config,
}

/// Structure used to store the configuration options for an instance of [`DeviceClient`] and
/// [`DeviceConnection`].
#[derive(Debug, Clone)]
pub struct DeviceBuilder<S = NoStore, C = NoConnect> {
    pub(crate) store: S,
    pub(crate) connection_config: C,
    pub(crate) interfaces: Interfaces,
    pub(crate) config: Config,
}

impl DeviceBuilder<NoStore, NoConnect> {
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
            config: Config {
                channel_size: DEFAULT_CHANNEL_SIZE,
                volatile_retention: DEFAULT_VOLATILE_CAPACITY,
                volatile: SharedVolatileStore::new(),
                writable_dir: None,
            },
            interfaces: Interfaces::new(),
            store: NoStore,
            connection_config: NoConnect,
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
        self.config.channel_size = size;

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

        self.config.writable_dir = Some(path.to_owned());

        Ok(self)
    }
}

impl<C> DeviceBuilder<NoStore, C> {
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
    pub fn store<S>(self, store: S) -> DeviceBuilder<S, C>
    where
        S: PropertyStore,
    {
        DeviceBuilder {
            config: self.config,
            interfaces: self.interfaces,
            connection_config: self.connection_config,
            store,
        }
    }
}

impl<S> DeviceBuilder<S, NoConnect>
where
    S: PropertyStore,
{
    /// Configure the connection using the passed [`ConnectionConfig`].
    ///
    /// If the connection gets established correctly, the caller can than construct
    /// the [`DeviceClient`] and [`DeviceConnection`] using the [`DeviceBuilder::build`] method.
    pub fn connection<C>(self, connection_config: C) -> DeviceBuilder<S, C>
    where
        C: ConnectionConfig<S>,
        // required to call [`DeviceBuilder::build`]
        crate::Error: From<C::Err>,
    {
        DeviceBuilder {
            interfaces: self.interfaces,
            connection_config,
            store: self.store,
            config: self.config,
        }
    }
}

// NOTE: Currently volatile retention is not supported for the Grpc connection.
// Currently messages won't be retained or stored (see [`crate::transport::store::GrpcStore`])
impl<S> DeviceBuilder<S, crate::transport::mqtt::MqttConfig> {
    /// Sets the number of elements with retention volatile that will be kept in memory if
    /// disconnected.
    pub fn volatile_retention(mut self, items: usize) -> Self {
        self.config.volatile_retention = items;

        self
    }
}

impl<S, C> DeviceBuilder<S, C>
where
    S: PropertyStore,
    C: ConnectionConfig<S>,
    crate::Error: From<C::Err>,
{
    /// Method that consumes the builder and returns a working [`DeviceClient`] and
    /// [`DeviceConnection`] with the specified settings.
    pub async fn build(
        self,
    ) -> Result<(DeviceClient<C::Store>, DeviceConnection<C::Store, C::Conn>), crate::Error> {
        let channel_size = self.config.channel_size;
        // We use the flume channel to have a clonable receiver, see the comment on the DeviceClient for more information.
        let (tx_connection, rx_client) = flume::bounded(channel_size);
        let (tx_client, rx_connection) = mpsc::channel(channel_size);

        let volatile = self.config.volatile.clone();
        volatile
            .set_capacity(C::volatile_capacity_override().unwrap_or(self.config.volatile_retention))
            .await;

        let transport = self
            .connection_config
            .connect(ConnectionBuildConfig {
                store: self.store.clone(),
                interfaces: &self.interfaces,
                config: self.config,
            })
            .await?;
        let DeviceTransport {
            connection,
            sender,
            store,
        } = transport;
        let interfaces = Arc::new(RwLock::new(self.interfaces));

        let client =
            DeviceClient::new(Arc::clone(&interfaces), rx_client, tx_client, store.clone());

        let connection = DeviceConnection::new(
            interfaces,
            tx_connection,
            rx_connection,
            volatile,
            store,
            connection,
            sender,
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
pub struct DeviceTransport<S, C: Connection> {
    pub(crate) connection: C,
    pub(crate) sender: C::Sender,
    pub(crate) store: StoreWrapper<S>,
}

/// Crate private connection implementation.
pub trait ConnectionConfig<S> {
    /// Type of the store used by the connection
    type Store: PropertyStore;
    /// Type of the constructed Connection
    type Conn: Connection + Send + Sync;
    /// Type of the error got while opening the connection
    type Err;

    /// Connect method that consumes self to construct a working connection
    /// This method is called internally by the builder.
    fn connect(
        self,
        config: ConnectionBuildConfig<S>,
    ) -> impl Future<Output = Result<DeviceTransport<Self::Store, Self::Conn>, Self::Err>> + Send
    where
        S: PropertyStore;

    /// This method allows the connection config to modify the retention capacity.
    /// The default implmenetation returns a [`None`] to avoid editing the default
    /// retention config.
    // NOTE: Used by the GrpcConfig to disable retention of messages.
    fn volatile_capacity_override() -> Option<usize> {
        None
    }
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

    use mockall::predicate;
    use mockito::Server;
    use rumqttc::{AckOfPub, ConnAck, ConnectReturnCode, Event, Packet, QoS, SubAck};
    use tempfile::TempDir;

    use crate::{
        properties::extract_set_properties,
        transport::mqtt::{
            client::{AsyncClient, EventLoop, NEW_LOCK},
            pairing::tests::{mock_create_certificate, mock_get_broker_url},
            test::notify_success,
            MqttConfig,
        },
    };

    use super::*;

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

        assert_eq!(builder.config.writable_dir, Some(dir.path().to_owned()))
    }

    #[tokio::test]
    async fn should_dir_with_store() {
        let dir = TempDir::new().unwrap();

        let builder = DeviceBuilder::new().store_dir(dir.path()).await.unwrap();

        assert_eq!(builder.config.writable_dir, Some(dir.path().to_owned()))
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
        let _m = NEW_LOCK.lock().await;

        let dir = TempDir::new().unwrap();

        let ctx = AsyncClient::new_context();
        ctx.expect().once().returning(move |_, _| {
            let mut client = AsyncClient::default();
            let mut ev_loop = EventLoop::default();

            let mut seq = mockall::Sequence::new();

            ev_loop
                .expect_set_network_options()
                .once()
                .in_sequence(&mut seq)
                .returning(|_| EventLoop::default());

            client
                .expect_clone()
                .once()
                .in_sequence(&mut seq)
                .returning(|| {
                    let mut client = AsyncClient::default();

                    let mut seq = mockall::Sequence::new();

                    client
                        .expect_clone()
                        .once()
                        .in_sequence(&mut seq)
                        .returning(|| {
                            let mut client = AsyncClient::default();

                            let mut seq = mockall::Sequence::new();

                            client
                                .expect_subscribe::<String>()
                                .with(
                                    predicate::eq(
                                        "realm/device_id/control/consumer/properties".to_string(),
                                    ),
                                    predicate::eq(QoS::ExactlyOnce),
                                )
                                .once()
                                .in_sequence(&mut seq)
                                .returning(|_, _| notify_success(SubAck::new(0, Vec::new())));

                            client
                                .expect_publish::<String, String>()
                                .once()
                                .in_sequence(&mut seq)
                                .with(
                                    predicate::eq("realm/device_id".to_string()),
                                    predicate::eq(QoS::ExactlyOnce),
                                    predicate::eq(false),
                                    predicate::eq(String::new()),
                                )
                                .returning(|_, _, _, _| notify_success(AckOfPub::None));

                            client
                                .expect_publish::<String, &str>()
                                .once()
                                .in_sequence(&mut seq)
                                .withf(|topic, qos, _, payload| {
                                    topic == "realm/device_id/control/emptyCache"
                                        && *qos == QoS::ExactlyOnce
                                        && *payload == "1"
                                })
                                .returning(|_, _, _, _| notify_success(AckOfPub::None));

                            client
                                .expect_publish::<String, Vec<u8>>()
                                .once()
                                .in_sequence(&mut seq)
                                .withf(|topic, qos, _, payload| {
                                    topic == "realm/device_id/control/producer/properties"
                                        && *qos == QoS::ExactlyOnce
                                        && extract_set_properties(payload).unwrap().is_empty()
                                })
                                .returning(|_, _, _, _| notify_success(AckOfPub::None));

                            client
                        });

                    client
                });

            ev_loop
                .expect_poll()
                .once()
                .in_sequence(&mut seq)
                .returning(|| {
                    Box::pin(async {
                        tokio::task::yield_now().await;

                        Ok(Event::Incoming(Packet::ConnAck(ConnAck {
                            session_present: false,
                            code: ConnectReturnCode::Success,
                        })))
                    })
                });

            // catch other calls to pool
            ev_loop.expect_poll().once().returning(|| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    Ok(Event::Incoming(Packet::PingReq))
                })
            });

            (client, ev_loop)
        });

        let mut server = Server::new_async().await;

        let mock_url = mock_get_broker_url(&mut server).create_async().await;
        let mock_cert = mock_create_certificate(&mut server).create_async().await;

        let (_client, _connection) = tokio::time::timeout(
            Duration::from_secs(3),
            DeviceBuilder::new()
                .store_dir(dir.path())
                .await
                .unwrap()
                .connection(MqttConfig::with_credential_secret(
                    "realm",
                    "device_id",
                    "secret",
                    server.url(),
                ))
                .build(),
        )
        .await
        .unwrap()
        .unwrap();

        mock_url.assert_async().await;
        mock_cert.assert_async().await;
    }
}
