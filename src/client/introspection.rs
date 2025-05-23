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

use tokio::fs;
use tracing::{debug, error};

use crate::error::Report;
use crate::interface::InterfaceTypeDef;
use crate::introspection::{AddInterfaceError, DeviceIntrospection};
use crate::prelude::DynamicIntrospection;
use crate::retention::memory::VolatileStore;
use crate::retention::StoredRetention;
use crate::store::wrapper::StoreWrapper;
use crate::store::{PropertyInterface, PropertyStore, StoreCapabilities};
use crate::transport::{Connection, Register};
use crate::{Error, Interface};

use super::DeviceClient;

impl<C> DeviceClient<C>
where
    C: Connection,
{
    // Cleans up an interface, it will remove the properties and retention values.
    //
    // For the datastream, we would have to check all the mappings for each retention type and then
    // delete them from the stores (volatile and non). Instead we remove all the values with the
    // given interface from each store.
    async fn cleanup_interface(
        volatile_store: &VolatileStore,
        store: &StoreWrapper<C::Store>,
        interface: &Interface,
    ) {
        match interface.interface_type() {
            InterfaceTypeDef::Datastream => {
                volatile_store
                    .delete_interface(interface.interface_name())
                    .await;

                if let Some(retention) = store.get_retention() {
                    let res = retention.delete_interface(interface.interface_name()).await;

                    if let Err(err) = res {
                        error!(error = %Report::new(err),"failed to remove interfaces from retention");
                    }
                }
            }
            InterfaceTypeDef::Properties => {
                let res = store
                    .delete_interface(&PropertyInterface::from(interface))
                    .await;

                if let Err(err) = res {
                    error!(error = %Report::new(err),"failed to remove interfaces from properties");
                }
            }
        }
    }
}

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
        let _permit = self
            .state
            .introspection
            .acquire()
            .await
            .map_err(|_| Error::Disconnected)?;

        let interfaces = self.state.interfaces.read().await;

        let map_err = interfaces
            .validate(interface)
            .map_err(AddInterfaceError::Interface)?;

        let Some(to_add) = map_err else {
            debug!("interfaces already present");

            return Ok(false);
        };

        self.sender.add_interface(&interfaces, &to_add).await?;

        if to_add.is_major_change() {
            Self::cleanup_interface(&self.state.volatile_store, &self.store, &to_add).await;
        }

        drop(interfaces);
        debug!("adding interface to introspection");
        let mut interfaces = self.state.interfaces.write().await;

        interfaces.add(to_add);

        Ok(true)
    }

    async fn extend_interfaces<I>(&mut self, iter: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = Interface> + Send,
    {
        // Lock for writing for the whole scope, even the checks
        let _permit = self
            .state
            .introspection
            .acquire()
            .await
            .map_err(|_| Error::Disconnected)?;
        let interfaces = self.state.interfaces.read().await;

        let to_add = interfaces
            .validate_many(iter)
            .map_err(AddInterfaceError::Interface)?;

        if to_add.is_empty() {
            debug!("All interfaces already present");
            return Ok(Vec::new());
        }

        debug!("Adding {} interfaces", to_add.len());

        self.sender.extend_interfaces(&interfaces, &to_add).await?;

        let major_changes = to_add
            .values()
            .filter(|interface| interface.is_major_change());

        for interface in major_changes {
            Self::cleanup_interface(&self.state.volatile_store, &self.store, interface).await;
        }

        let names = to_add.keys().cloned().collect();

        drop(interfaces);
        debug!("adding interfaces to introspection");
        let mut interfaces = self.state.interfaces.write().await;
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
        // Lock for writing for the whole scope, even the checks
        let _permit = self
            .state
            .introspection
            .acquire()
            .await
            .map_err(|_| Error::Disconnected)?;

        let interfaces = self.state.interfaces.read().await;

        let Some(to_remove) = interfaces.get(interface_name) else {
            debug!("{interface_name} not found, skipping");
            return Ok(false);
        };

        self.sender.remove_interface(&interfaces, to_remove).await?;

        Self::cleanup_interface(&self.state.volatile_store, &self.store, to_remove).await;

        drop(interfaces);
        debug!("removing interface from introspection");
        let mut interfaces = self.state.interfaces.write().await;
        interfaces.remove(interface_name);

        Ok(true)
    }

    async fn remove_interfaces<I>(&mut self, interfaces_name: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = String> + Send,
        I::IntoIter: Send,
    {
        // Lock for writing for the whole scope, even the checks
        let _permit = self
            .state
            .introspection
            .acquire()
            .await
            .map_err(|_| Error::Disconnected)?;

        let interfaces = self.state.interfaces.read().await;

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

        for interface in to_remove.values() {
            Self::cleanup_interface(&self.state.volatile_store, &self.store, interface).await;
        }

        let removed_names: Vec<String> = to_remove.keys().map(|k| k.to_string()).collect();

        drop(interfaces);
        debug!("removing interfaces from introspection");
        let mut interfaces = self.state.interfaces.write().await;
        interfaces.remove_many(&removed_names);

        Ok(removed_names)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use mockall::{predicate, Sequence};
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::*;

    use crate::client::tests::{mock_client, mock_client_with_store};
    use crate::interface::{Ownership, Reliability, Retention};
    use crate::interfaces::tests::{mock_validated_collection, mock_validated_interface};
    use crate::retention::StoredRetentionExt;
    use crate::store::{PropertyMapping, SqliteStore};
    use crate::test::{
        for_update, E2E_DEVICE_AGGREGATE, E2E_DEVICE_AGGREGATE_NAME, E2E_DEVICE_PROPERTY,
        E2E_DEVICE_PROPERTY_NAME,
    };
    use crate::validate::ValidatedIndividual;
    use crate::AstarteType;

    #[tokio::test]
    async fn get_interface() {
        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let (client, _tx) = mock_client(&[E2E_DEVICE_AGGREGATE]);

        client
            .get_interface(interface.interface_name(), |i| {
                assert_eq!(i, Some(&interface));
            })
            .await;

        client
            .get_interface(E2E_DEVICE_PROPERTY_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;
    }

    #[tokio::test]
    async fn add_interface_missing() {
        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let (mut client, _tx) = mock_client(&[]);

        let mut seq = Sequence::new();
        client
            .sender
            .expect_add_interface()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::always(),
                predicate::eq(mock_validated_interface(interface.clone(), false)),
            )
            .returning(|_, _| Ok(()));

        let added = client.add_interface(interface.clone()).await.unwrap();
        assert!(added);

        client
            .get_interface(interface.interface_name(), |i| {
                assert_eq!(i, Some(&interface));
            })
            .await;

        let added = client.add_interface(interface).await.unwrap();
        assert!(!added);
    }

    #[tokio::test]
    async fn add_interface_missing_from_str() {
        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let (mut client, _tx) = mock_client(&[]);

        let mut seq = Sequence::new();
        client
            .sender
            .expect_add_interface()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::always(),
                predicate::eq(mock_validated_interface(interface.clone(), false)),
            )
            .returning(|_, _| Ok(()));

        let added = client
            .add_interface_from_str(E2E_DEVICE_AGGREGATE)
            .await
            .unwrap();
        assert!(added);

        client
            .get_interface(interface.interface_name(), |i| {
                assert_eq!(i, Some(&interface));
            })
            .await;

        let added = client.add_interface(interface).await.unwrap();
        assert!(!added);
    }

    #[tokio::test]
    async fn add_interface_missing_from_file() {
        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let (mut client, _tx) = mock_client(&[]);

        let mut seq = Sequence::new();
        client
            .sender
            .expect_add_interface()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::always(),
                predicate::eq(mock_validated_interface(interface.clone(), false)),
            )
            .returning(|_, _| Ok(()));

        let dir = TempDir::new().unwrap();

        let path = dir.path().join("interface");
        std::fs::write(&path, E2E_DEVICE_AGGREGATE).unwrap();

        let added = client.add_interface_from_file(&path).await.unwrap();
        assert!(added);

        client
            .get_interface(interface.interface_name(), |i| {
                assert_eq!(i, Some(&interface));
            })
            .await;

        let added = client.add_interface(interface).await.unwrap();
        assert!(!added);
    }

    #[tokio::test]
    async fn add_interface_major_with_retention_volatile() {
        let updated = Interface::from_str(for_update::E2E_DEVICE_DATASTREAM_1_0).unwrap();

        let (mut client, _tx) = mock_client(&[for_update::E2E_DEVICE_DATASTREAM_0_1]);

        client
            .state
            .volatile_store
            .push(
                client.state.retention_ctx.next(),
                ValidatedIndividual {
                    interface: for_update::E2E_DEVICE_DATASTREAM_NAME.to_string(),
                    path: "/sensor_1/volatile".to_string(),
                    version_major: 0,
                    reliability: Reliability::Guaranteed,
                    retention: Retention::Volatile { expiry: None },
                    data: AstarteType::Double(42.0),
                    timestamp: Some(Utc::now()),
                },
            )
            .await;

        let mut seq = Sequence::new();
        client
            .sender
            .expect_add_interface()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::always(),
                predicate::eq(mock_validated_interface(updated.clone(), true)),
            )
            .returning(|_, _| Ok(()));

        let added = client.add_interface(updated.clone()).await.unwrap();
        assert!(added);

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;

        assert!(client.state.volatile_store.pop_next().await.is_none());
    }

    #[tokio::test]
    async fn add_interface_major_with_retention_stored() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let (mut client, _tx) =
            mock_client_with_store(&[for_update::E2E_DEVICE_DATASTREAM_0_1], store);

        let updated = Interface::from_str(for_update::E2E_DEVICE_DATASTREAM_1_0).unwrap();

        let id = client.state.retention_ctx.next();
        client
            .store
            .get_retention()
            .unwrap()
            .store_publish_individual(
                &id,
                &ValidatedIndividual {
                    interface: for_update::E2E_DEVICE_DATASTREAM_NAME.to_string(),
                    path: "/sensor_1/stored".to_string(),
                    version_major: 0,
                    reliability: Reliability::Guaranteed,
                    retention: Retention::Stored { expiry: None },
                    data: AstarteType::Double(42.0),
                    timestamp: Some(Utc::now()),
                },
                &[1, 2, 3, 4],
            )
            .await
            .unwrap();

        let mut seq = Sequence::new();
        client
            .sender
            .expect_add_interface()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::always(),
                predicate::eq(mock_validated_interface(updated.clone(), true)),
            )
            .returning(|_, _| Ok(()));

        let added = client.add_interface(updated.clone()).await.unwrap();
        assert!(added);

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;

        let packets = client
            .store
            .get_retention()
            .unwrap()
            .fetch_all_interfaces()
            .await
            .unwrap();
        assert!(packets.is_empty());
    }

    #[tokio::test]
    async fn extend_interfaces_major_with_retention_volatile() {
        let updated = Interface::from_str(for_update::E2E_DEVICE_DATASTREAM_1_0).unwrap();

        let (mut client, _tx) = mock_client(&[for_update::E2E_DEVICE_DATASTREAM_0_1]);

        client
            .state
            .volatile_store
            .push(
                client.state.retention_ctx.next(),
                ValidatedIndividual {
                    interface: for_update::E2E_DEVICE_DATASTREAM_NAME.to_string(),
                    path: "/sensor_1/volatile".to_string(),
                    version_major: 0,
                    reliability: Reliability::Guaranteed,
                    retention: Retention::Volatile { expiry: None },
                    data: AstarteType::Double(42.0),
                    timestamp: Some(Utc::now()),
                },
            )
            .await;

        let mut seq = Sequence::new();
        client
            .sender
            .expect_extend_interfaces()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::always(),
                predicate::eq(mock_validated_collection(&[mock_validated_interface(
                    updated.clone(),
                    true,
                )])),
            )
            .returning(|_, _| Ok(()));

        let added = client.extend_interfaces([updated.clone()]).await.unwrap();
        assert_eq!(added, vec![updated.interface_name()]);

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;

        assert!(client.state.volatile_store.pop_next().await.is_none());
    }

    #[tokio::test]
    async fn extend_interfaces_major_with_retention_stored() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let (mut client, _tx) =
            mock_client_with_store(&[for_update::E2E_DEVICE_DATASTREAM_0_1], store);

        let updated = Interface::from_str(for_update::E2E_DEVICE_DATASTREAM_1_0).unwrap();

        let id = client.state.retention_ctx.next();
        client
            .store
            .get_retention()
            .unwrap()
            .store_publish_individual(
                &id,
                &ValidatedIndividual {
                    interface: for_update::E2E_DEVICE_DATASTREAM_NAME.to_string(),
                    path: "/sensor_1/stored".to_string(),
                    version_major: 0,
                    reliability: Reliability::Guaranteed,
                    retention: Retention::Stored { expiry: None },
                    data: AstarteType::Double(42.0),
                    timestamp: Some(Utc::now()),
                },
                &[1, 2, 3, 4],
            )
            .await
            .unwrap();

        let mut seq = Sequence::new();
        client
            .sender
            .expect_extend_interfaces()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::always(),
                predicate::eq(mock_validated_collection(&[mock_validated_interface(
                    updated.clone(),
                    true,
                )])),
            )
            .returning(|_, _| Ok(()));

        let added = client.extend_interfaces([updated.clone()]).await.unwrap();
        assert_eq!(added, [updated.interface_name()]);

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;

        let packets = client
            .store
            .get_retention()
            .unwrap()
            .fetch_all_interfaces()
            .await
            .unwrap();
        assert!(packets.is_empty());
    }

    #[tokio::test]
    async fn extend_interfaces_nothing_to_add() {
        let (mut client, _tx) = mock_client(&[for_update::E2E_DEVICE_DATASTREAM_1_0]);

        let updated = Interface::from_str(for_update::E2E_DEVICE_DATASTREAM_1_0).unwrap();

        let added = client.extend_interfaces([updated.clone()]).await.unwrap();
        assert!(added.is_empty());

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;
    }

    #[tokio::test]
    async fn remove_interface_present() {
        let (mut client, _tx) = mock_client(&[E2E_DEVICE_AGGREGATE]);

        let to_remove = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let mut seq = Sequence::new();
        client
            .sender
            .expect_remove_interface()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::always(), predicate::eq(to_remove.clone()))
            .returning(|_, _| Ok(()));

        let removed = client
            .remove_interface(E2E_DEVICE_AGGREGATE_NAME)
            .await
            .unwrap();
        assert!(removed);

        client
            .get_interface(E2E_DEVICE_AGGREGATE_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;
    }

    #[tokio::test]
    async fn remove_interface_property() {
        let (mut client, _tx) = mock_client(&[E2E_DEVICE_PROPERTY]);

        let to_remove = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();

        client
            .store
            .store_prop(crate::store::StoredProp {
                interface: E2E_DEVICE_PROPERTY_NAME,
                path: "/sensor_1/double_endpoint",
                value: &AstarteType::LongInteger(2),
                interface_major: to_remove.version_major(),
                ownership: to_remove.ownership(),
            })
            .await
            .unwrap();

        let mut seq = Sequence::new();
        client
            .sender
            .expect_remove_interface()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::always(), predicate::eq(to_remove.clone()))
            .returning(|_, _| Ok(()));

        let removed = client
            .remove_interface(E2E_DEVICE_PROPERTY_NAME)
            .await
            .unwrap();
        assert!(removed);

        client
            .get_interface(E2E_DEVICE_PROPERTY_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;

        let res = client
            .store
            .load_prop(
                &PropertyMapping::new_unchecked(
                    PropertyInterface::new(E2E_DEVICE_PROPERTY_NAME, Ownership::Device),
                    "/sensor_1/double_endpoint",
                ),
                0,
            )
            .await
            .unwrap();
        assert_eq!(res, None);
    }

    #[tokio::test]
    async fn remove_interface_not_found() {
        let (mut client, _tx) = mock_client(&[]);

        let removed = client
            .remove_interface(E2E_DEVICE_AGGREGATE_NAME)
            .await
            .unwrap();
        assert!(!removed);

        client
            .get_interface(E2E_DEVICE_AGGREGATE_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;
    }

    #[tokio::test]
    async fn remove_interface_many_present() {
        let (mut client, _tx) = mock_client(&[E2E_DEVICE_AGGREGATE]);

        let mut seq = Sequence::new();
        client
            .sender
            .expect_remove_interfaces()
            .once()
            .in_sequence(&mut seq)
            // The hashmap require a reference that lives for static
            .withf(|_, a| a.len() == 1 && a.contains_key(E2E_DEVICE_AGGREGATE_NAME))
            .returning(|_, _| Ok(()));

        let removed = client
            .remove_interfaces([E2E_DEVICE_AGGREGATE_NAME.to_string()])
            .await
            .unwrap();
        assert_eq!(removed, [E2E_DEVICE_AGGREGATE_NAME]);

        client
            .get_interface(E2E_DEVICE_AGGREGATE_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;
    }

    #[tokio::test]
    async fn remove_interface_many_missing() {
        let (mut client, _tx) = mock_client(&[]);

        let removed = client
            .remove_interfaces([E2E_DEVICE_AGGREGATE_NAME.to_string()])
            .await
            .unwrap();
        assert!(removed.is_empty());

        client
            .get_interface(E2E_DEVICE_AGGREGATE_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;
    }
}
