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

use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use async_trait::async_trait;
use log::{debug, error};

use crate::{
    interface::error::InterfaceError, store::PropertyStore, transport::Register, AstarteDeviceSdk,
    Error, Interface,
};

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

    /// Add a new interface from the provided file.
    async fn add_interface_from_file<P>(&self, file_path: P) -> Result<(), Error>
    where
        P: AsRef<Path> + Send;

    /// Add a new interface from a string. The string should contain a valid json formatted
    /// interface.
    async fn add_interface_from_str(&self, json_str: &str) -> Result<(), Error>;

    /// Remove the interface with the name specified as argument.
    async fn remove_interface(&self, interface_name: &str) -> Result<(), Error>;
}

#[async_trait]
impl<S, C> DynamicIntrospection for AstarteDeviceSdk<S, C>
where
    C: Register + Send + Sync,
    S: Send + PropertyStore,
{
    async fn add_interface(&self, interface: Interface) -> Result<(), Error> {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.interfaces.write().await;

        let map_err = interfaces
            .validate(interface)
            .map_err(AddInterfaceError::Interface)?;

        let Some(to_add) = map_err else {
            debug!("interfaces already present");

            return Ok(());
        };

        self.connection.add_interface(&interfaces, &to_add).await?;

        interfaces.add(to_add);

        Ok(())
    }

    async fn extend_interfaces<I>(&self, added: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = Interface> + Send,
    {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.interfaces.write().await;

        let to_add = interfaces
            .validate_many(added)
            .map_err(AddInterfaceError::Interface)?;

        if to_add.is_empty() {
            debug!("All interfaces already present");
            return Ok(());
        }

        debug!("Adding {} interfaces", to_add.len());

        self.connection
            .extend_interfaces(&interfaces, &to_add)
            .await?;

        interfaces.extend(to_add);

        debug!("Interfaces added");

        Ok(())
    }

    async fn add_interface_from_file<P>(&self, file_path: P) -> Result<(), Error>
    where
        P: AsRef<Path> + Send,
    {
        let interface = fs::read_to_string(&file_path).map_err(|err| AddInterfaceError::Io {
            path: file_path.as_ref().to_owned(),
            backtrace: err,
        })?;

        let interface =
            Interface::from_str(&interface).map_err(|err| AddInterfaceError::InterfaceFile {
                path: file_path.as_ref().to_owned(),
                backtrace: err,
            })?;

        self.add_interface(interface).await
    }

    async fn add_interface_from_str(&self, json_str: &str) -> Result<(), Error> {
        let interface = Interface::from_str(json_str).map_err(AddInterfaceError::Interface)?;

        self.add_interface(interface).await
    }

    async fn remove_interface(&self, interface_name: &str) -> Result<(), Error> {
        let mut interfaces = self.interfaces.write().await;

        let to_remove = interfaces
            .get(interface_name)
            .ok_or_else(|| Error::InterfaceNotFound {
                name: interface_name.to_string(),
            })?;

        self.connection
            .remove_interface(&interfaces, to_remove)
            .await?;

        if let Some(prop) = to_remove.as_prop() {
            // We cannot error here since we already unsubscribed to the interface
            if let Err(err) = self.remove_properties_from_store(prop).await {
                error!("failed to remove property {err}");
            }
        }

        interfaces.remove(interface_name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use mockall::predicate;
    use rumqttc::SubscribeFilter;

    use super::*;

    use crate::interfaces::Introspection;
    use crate::test::{mock_astarte_device, INDIVIDUAL_SERVER_DATASTREAM};
    use crate::transport::mqtt::{AsyncClient, EventLoop};

    #[tokio::test]
    async fn test_add_remove_interface() {
        let eventloop = EventLoop::default();
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

        let (astarte, _rx) = mock_astarte_device(client, eventloop, []);

        astarte
            .add_interface_from_str(INDIVIDUAL_SERVER_DATASTREAM)
            .await
            .unwrap();

        astarte
            .remove_interface(
                "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn should_extend_interfaces() {
        let eventloop = EventLoop::default();
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

        let (astarte, _rx) = mock_astarte_device(client, eventloop, []);

        astarte.extend_interfaces(to_add).await.unwrap()
    }
}
