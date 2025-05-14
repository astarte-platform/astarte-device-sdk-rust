// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use itertools::Itertools;
use tokio::fs;
use tracing::{debug, error};

use crate::error::Report;
use crate::introspection::{AddInterfaceError, DeviceIntrospection};
use crate::prelude::DynamicIntrospection;
use crate::retention::StoredRetention;
use crate::store::{PropertyStore, StoreCapabilities};
use crate::transport::{Connection, Register};
use crate::{Error, Interface};

use super::DeviceClient;

impl<C> DeviceIntrospection for DeviceClient<C>
where
    C: Connection,
{
    async fn get_interface<F, O>(&self, interface_name: &str, mut f: F) -> O
    where
        F: FnMut(Option<&Interface>) -> O + Send,
    {
        let interfaces = self.state.interfaces.read().await;

        f(interfaces.get(interface_name))
    }
}

impl<C> DynamicIntrospection for DeviceClient<C>
where
    C: Connection,
    C::Sender: Register,
{
    async fn add_interface(&mut self, interface: Interface) -> Result<bool, Error> {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.state.interfaces.write().await;

        let map_err = interfaces
            .validate(interface)
            .map_err(AddInterfaceError::Interface)?;

        let Some(to_add) = map_err else {
            debug!("interfaces already present");

            return Ok(false);
        };

        self.sender.add_interface(&interfaces, &to_add).await?;

        if to_add.is_major_change() {
            if let Some(retention) = self.store.get_retention() {
                if let Err(err) = retention.delete_interface(to_add.interface_name()).await {
                    error!(error = %Report::new(err),"failed to remove interface from retention");
                }
            }
        }

        interfaces.add(to_add);

        Ok(true)
    }

    async fn extend_interfaces<I>(&mut self, iter: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = Interface> + Send,
    {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.state.interfaces.write().await;

        let to_add = interfaces
            .validate_many(iter)
            .map_err(AddInterfaceError::Interface)?;

        if to_add.is_empty() {
            debug!("All interfaces already present");
            return Ok(Vec::new());
        }

        debug!("Adding {} interfaces", to_add.len());

        self.sender.extend_interfaces(&interfaces, &to_add).await?;

        if let Some(retention) = self.store.get_retention() {
            // TODO here stuff goes wrong if possible try to avoid using the boxed future
            let res = retention
                .delete_interface_many(
                    &to_add
                        .values()
                        .filter_map(|v| v.is_major_change().then_some(v.interface_name()))
                        .collect_vec(),
                )
                .await;
            if let Err(err) = res {
                error!(error = %Report::new(err),"failed to remove interfaces from retention");
            }
        }

        let names = to_add.keys().cloned().collect_vec();

        interfaces.extend(to_add);

        debug!("Interfaces added");

        Ok(names)
    }

    async fn add_interface_from_file<P>(&mut self, file_path: P) -> Result<bool, Error>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let interface =
            fs::read_to_string(&file_path)
                .await
                .map_err(|err| AddInterfaceError::Io {
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

    async fn add_interface_from_str(&mut self, json_str: &str) -> Result<bool, Error> {
        let interface = Interface::from_str(json_str).map_err(AddInterfaceError::Interface)?;

        self.add_interface(interface).await
    }

    async fn remove_interface(&mut self, interface_name: &str) -> Result<bool, Error> {
        let mut interfaces = self.state.interfaces.write().await;

        let Some(to_remove) = interfaces.get(interface_name) else {
            debug!("{interface_name} not found, skipping");
            return Ok(false);
        };

        self.sender.remove_interface(&interfaces, to_remove).await?;

        if let Some(ref prop) = to_remove.as_prop() {
            // We cannot error here since we have already unsubscribed from the interface
            if let Err(err) = self.store.delete_interface(&prop.into()).await {
                error!(error = %Report::new(err),"failed to remove property");
            }
        } else if let Some(retention) = self.store.get_retention() {
            if let Err(err) = retention.delete_interface(to_remove.interface_name()).await {
                error!(error = %Report::new(err),"failed to remove interface from retention");
            }
        }

        interfaces.remove(interface_name);

        Ok(true)
    }

    async fn remove_interfaces<I>(&mut self, interfaces_name: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = String> + Send,
        I::IntoIter: Send,
    {
        let mut interfaces = self.state.interfaces.write().await;

        let to_remove: HashMap<&str, &Interface> = interfaces_name
            .into_iter()
            .filter_map(|iface_name| {
                let interface = interfaces.get(&iface_name).map(|i| (i.interface_name(), i));

                if interface.is_none() {
                    debug!("{iface_name} not found, skipping");
                }

                interface
            })
            .collect();

        if to_remove.is_empty() {
            return Ok(Vec::new());
        }

        self.sender
            .remove_interfaces(&interfaces, &to_remove)
            .await?;

        if let Some(retention) = self.store.get_retention() {
            let res = retention
                .delete_interface_many(&to_remove.keys().collect_vec())
                .await;
            if let Err(err) = res {
                error!(error = %Report::new(err),"failed to remove interfaces from retention");
            }
        }

        for (_, iface) in to_remove.iter() {
            // We cannot error here since we have already unsubscribed from the interface
            if let Some(ref prop) = iface.as_prop() {
                if let Err(err) = self.store.delete_interface(&prop.into()).await {
                    error!(error = %Report::new(err), "failed to remove property");
                }
            }
        }

        let removed_names = to_remove.keys().map(|k| k.to_string()).collect_vec();

        interfaces.remove_many(&removed_names);

        Ok(removed_names)
    }
}
