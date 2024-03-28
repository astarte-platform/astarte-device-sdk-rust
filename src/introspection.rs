// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Handle the introspection for the device

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use log::{debug, error};

use crate::{interface::error::InterfaceError, Error, Interface};

/// Error while adding an [`Interface`] to the device introspection.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum AddInterfaceError {
    /// Couldn't add the interface
    #[error("error adding interface")]
    Interface(#[from] InterfaceError),
    /// Failed to read interface directory
    #[error("couldn't read interface path {}", .path.display())]
    Io {
        path: PathBuf,
        #[source]
        backtrace: std::io::Error,
    },
    /// Cannot read the interface file.
    #[error("invalid interface file {}", .path.display())]
    InterfaceFile {
        path: PathBuf,
        backtrace: InterfaceError,
    },
}

impl AddInterfaceError {
    // Add a path to the error context.
    pub(crate) fn add_path_context(self, path: PathBuf) -> Self {
        match self {
            AddInterfaceError::Interface(backtrace) => {
                AddInterfaceError::InterfaceFile { path, backtrace }
            }
            AddInterfaceError::Io {
                path: prev,
                backtrace,
            } => {
                debug!("overwriting previous path {}", prev.display());

                AddInterfaceError::Io { path, backtrace }
            }
            AddInterfaceError::InterfaceFile {
                path: prev,
                backtrace,
            } => {
                debug!("overwriting previous path {}", prev.display());

                AddInterfaceError::InterfaceFile { path, backtrace }
            }
        }
    }
}

/// Trait that permits a client to add and remove interfaces dynamically after being connected.
#[async_trait]
pub trait DynamicIntrospection {
    /// Add a new [`Interface`] to the device interfaces.
    async fn add_interface(&self, interface: Interface) -> Result<(), Error>;

    /// Add one ore more [`Interface`] to the device introspection.
    async fn extend_interfaces<I>(&self, interfaces: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = Interface> + Send;

    /// Add one or more [`Interface`] to the device introspection, specialized for a [`Vec`].
    async fn extend_interfaces_vec(&self, interfaces: Vec<Interface>) -> Result<(), Error>;

    /// Add a new interface from the provided file.
    async fn add_interface_from_file<P>(&self, file_path: P) -> Result<(), Error>
    where
        P: AsRef<Path> + Send + Sync;

    /// Add a new interface from a string. The string should contain a valid json formatted
    /// interface.
    async fn add_interface_from_str(&self, json_str: &str) -> Result<(), Error>;

    /// Remove the interface with the name specified as argument.
    async fn remove_interface(&self, interface_name: &str) -> Result<(), Error>;
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use itertools::Itertools;
    use mockall::predicate;
    use rumqttc::SubscribeFilter;

    use super::*;

    use crate::interfaces::Introspection;
    use crate::test::{mock_astarte_device, INDIVIDUAL_SERVER_DATASTREAM};
    use crate::transport::mqtt::{AsyncClient, EventLoop as MqttEventLoop};

    #[tokio::test]
    async fn test_add_remove_interface() {
        let eventloop = MqttEventLoop::default();
        let mut client = AsyncClient::default();

        client
            .expect_subscribe::<String>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()),
                predicate::always()
            )
            .returning(|_, _| { Ok(()) });

        client
            .expect_publish::<String, String>()
            .with(
                predicate::eq("realm/device_id".to_string()),
                predicate::always(),
                predicate::eq(false),
                predicate::eq(
                    "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream:0:1"
                        .to_string(),
                ),
            )
            .returning(|_, _, _, _| Ok(()));

        client
            .expect_publish::<String, String>()
            .with(
                predicate::eq("realm/device_id".to_string()),
                predicate::always(),
                predicate::eq(false),
                predicate::eq(String::new()),
            )
            .returning(|_, _, _, _| Ok(()));

        client
            .expect_unsubscribe::<String>()
            .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()))
            .returning(|_| Ok(()));

        let (client, mut connection) = mock_astarte_device(client, eventloop, []);

        let handle = tokio::spawn(async move {
            for _ in 0..2 {
                let msg = connection.client.recv().await.unwrap();
                connection.handle_client_msg(msg).await.unwrap();
            }
        });

        client
            .add_interface_from_str(INDIVIDUAL_SERVER_DATASTREAM)
            .await
            .unwrap();

        client
            .remove_interface(
                "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream",
            )
            .await
            .unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn should_extend_interfaces() {
        let eventloop = MqttEventLoop::default();
        let mut client = AsyncClient::default();

        let to_add = [
            Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap(),
            Interface::from_str(crate::test::INDIVIDUAL_SERVER_DATASTREAM).unwrap(),
        ];

        let mut introspection = Introspection::new(to_add.iter())
            .to_string()
            .split(';')
            .map(ToOwned::to_owned)
            .collect_vec();

        introspection.sort_unstable();

        client
            .expect_subscribe_many::<Vec<SubscribeFilter>>()
            .once()
            .returning(|_| Ok(()));

        client
            .expect_publish::<String, String>()
            .once()
            .withf(move |publish, _, _, payload| {
                let mut intro = payload.split(';').collect_vec();

                intro.sort_unstable();

                publish == "realm/device_id" && intro == introspection
            })
            .returning(|_, _, _, _| Ok(()));

        let (client, mut connection) = mock_astarte_device(client, eventloop, []);

        let handle = tokio::spawn(async move {
            let msg = connection.client.recv().await.unwrap();
            connection.handle_client_msg(msg).await.unwrap();
        });

        client.extend_interfaces(to_add).await.unwrap();

        handle.await.unwrap();
    }
}
