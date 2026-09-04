// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

use astarte_device_error::{Error, ResultExt, WrapError};
use astarte_interfaces::interface::InterfaceTypeAggregation;
use astarte_interfaces::{Interface, Schema};
use tracing::debug;

use crate::builder::ConnectionConfig;
use crate::error::{AstarteError, ErrorKind, InterfaceError};
use crate::introspection::DeviceIntrospection;
use crate::retention::{RetentionError, StoredRetention};
use crate::store::StoreCapabilities;
use crate::transport::RemovedInterface;
use crate::validate::Validated;

use super::DeviceClient;

impl<C, S> DeviceClient<C, S>
where
    C: ConnectionConfig,
{
    // Cleans up an interface, it will remove the properties and retention values.
    //
    // For the datastream, we would have to check all the mappings for each retention type and then
    // delete them from the stores (volatile and non). Instead we remove all the values with the
    // given interface from each store.
    async fn cleanup_interface(&self, interface: &Interface) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        match interface.inner() {
            InterfaceTypeAggregation::DatastreamIndividual(interface) => {
                self.cleanup_retention(interface.name())
                    .await
                    .map_kind(ErrorKind::Retention)?;
            }
            InterfaceTypeAggregation::DatastreamObject(interface) => {
                self.cleanup_retention(interface.name())
                    .await
                    .map_kind(ErrorKind::Retention)?;
            }
            InterfaceTypeAggregation::Properties(properties) => self
                .state
                .store()
                .delete_interface(properties)
                .await
                .map_kind(ErrorKind::Store)?,
        }

        Ok(())
    }

    // Cleans up the volatile and store retention.
    async fn cleanup_retention(&self, interface_name: &str) -> Result<(), Error<RetentionError>>
    where
        S: StoreCapabilities,
    {
        self.state
            .volatile_store()
            .delete_interface(interface_name)
            .await;

        if let Some(retention) = self.state.store().get_retention() {
            retention.delete_interface(interface_name).await?;
        }

        Ok(())
    }
}

impl<C, S> DeviceIntrospection for DeviceClient<C, S>
where
    C: ConnectionConfig,
    S: StoreCapabilities,
{
    async fn get_interface<F, O>(&self, interface_name: &str, mut f: F) -> O
    where
        F: FnMut(Option<&Interface>) -> O + Send,
    {
        let interfaces = self.state.interfaces().read().await;

        f(interfaces.get(interface_name))
    }

    async fn add_interface(&self, interface: Interface) -> Result<bool, AstarteError> {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.state.interfaces().write().await;

        let map_err = interfaces.validate(interface).wrap_err_with(|_| {
            AstarteError::with(
                ErrorKind::Interface(InterfaceError::Invalid),
                "couldn't add interface",
            )
        })?;

        let Some(to_add) = map_err else {
            debug!("interfaces already present");

            return Ok(false);
        };

        if to_add.is_major_change() {
            self.cleanup_interface(&to_add).await?;
        }

        self.send_timeout(Validated::AddInterface(to_add.interface().clone()))
            .await?;

        debug!("adding interface to introspection");

        interfaces.add(to_add);

        Ok(true)
    }

    async fn extend_interfaces<I>(&self, iter: I) -> Result<Vec<String>, AstarteError>
    where
        I: IntoIterator<Item = Interface> + Send,
    {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.state.interfaces().write().await;

        let to_add = interfaces.validate_many(iter).wrap_err_with(|_| {
            AstarteError::with(
                ErrorKind::Interface(InterfaceError::Invalid),
                "couldn't add interfaces",
            )
        })?;

        if to_add.is_empty() {
            debug!("All interfaces already present");
            return Ok(Vec::new());
        }

        debug!("Adding {} interfaces", to_add.len());

        let major_changes = to_add
            .values()
            .filter(|interface| interface.is_major_change());

        for interface in major_changes {
            self.cleanup_interface(interface).await?;
        }

        self.send_timeout(Validated::ExtendInterfaces(to_add.clone()))
            .await?;

        let names = to_add.keys().cloned().collect();

        debug!("adding interfaces to introspection");

        interfaces.extend(to_add);

        debug!("Interfaces added");

        Ok(names)
    }

    async fn add_interface_from_file<P>(&self, file_path: P) -> Result<bool, AstarteError>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let interface = tokio::fs::read_to_string(&file_path)
            .await
            .wrap_err_with(|err| {
                AstarteError::with(ErrorKind::Io(err.kind()), "couldn't read interface")
                    .set_ctx(file_path.as_ref().display().to_string())
            })?;

        let interface = Interface::from_str(&interface).wrap_err_with(|_| {
            AstarteError::with(
                ErrorKind::Interface(InterfaceError::Invalid),
                "couldn't add interface",
            )
            .set_ctx(file_path.as_ref().display().to_string())
        })?;

        self.add_interface(interface).await
    }

    async fn add_interface_from_str(&self, json_str: &str) -> Result<bool, AstarteError> {
        let interface = Interface::from_str(json_str).wrap_err_msg(
            ErrorKind::Interface(InterfaceError::Invalid),
            "couldn't add interface",
        )?;

        self.add_interface(interface).await
    }

    async fn remove_interface(&self, interface_name: &str) -> Result<bool, AstarteError> {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.state.interfaces().write().await;

        let Some(to_remove) = interfaces.get(interface_name) else {
            debug!("{interface_name} not found, skipping");
            return Ok(false);
        };

        self.cleanup_interface(to_remove).await?;

        self.send_timeout(Validated::RemoveInterface(RemovedInterface::from(
            to_remove,
        )))
        .await?;

        debug!("removing interface from introspection");

        interfaces.remove(interface_name);

        Ok(true)
    }

    async fn remove_interfaces<I>(&self, interfaces_name: I) -> Result<Vec<String>, AstarteError>
    where
        I: IntoIterator<Item = String> + Send,
        I::IntoIter: Send,
    {
        // Lock for writing for the whole scope, even the checks
        let mut interfaces = self.state.interfaces().write().await;

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

        for interface in to_remove.values() {
            self.cleanup_interface(interface).await?;
        }

        let value = to_remove
            .values()
            .map(|&v| RemovedInterface::from(v))
            .collect();
        self.send_timeout(Validated::RemoveInterfaceMany(value))
            .await?;

        let removed_names: Vec<String> = to_remove.keys().map(|k| k.to_string()).collect();

        debug!("removing interfaces from introspection");

        interfaces.remove_many(&removed_names);

        Ok(removed_names)
    }
}

#[cfg(test)]
mod tests {
    use astarte_interfaces::interface::Retention;
    use astarte_interfaces::schema::Reliability;
    use astarte_interfaces::{MappingPath, Properties};
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::*;

    use crate::AstarteData;
    use crate::client::tests::{mock_client, mock_client_with_store};
    use crate::interfaces::MappingRef;
    use crate::interfaces::tests::{mock_validated_collection, mock_validated_interface};
    use crate::retention::StoredRetentionExt;
    use crate::state::ConnStatus;
    use crate::store::{Prop, PropertyMapping, PropertyStore, SqliteStore};
    use crate::test::{
        E2E_DEVICE_AGGREGATE, E2E_DEVICE_AGGREGATE_NAME, E2E_DEVICE_PROPERTY,
        E2E_DEVICE_PROPERTY_NAME, for_update,
    };
    use crate::validate::individual::ValidatedIndividual;

    #[tokio::test]
    async fn get_interface() {
        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let client = mock_client(&[E2E_DEVICE_AGGREGATE], ConnStatus::Online);

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
        let exp_interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let mut client = mock_client(&[], ConnStatus::Online);

        let added = client.add_interface(exp_interface.clone()).await.unwrap();
        assert!(added);

        let Validated::AddInterface(interface) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };

        assert_eq!(interface, exp_interface);

        client
            .get_interface(interface.interface_name(), |i| {
                assert_eq!(i, Some(&interface));
            })
            .await;

        let added = client.add_interface(interface).await.unwrap();
        assert!(!added);
        assert!(client.client_rx.is_empty());
    }

    #[tokio::test]
    async fn add_interface_missing_from_str() {
        let exp_interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let mut client = mock_client(&[], ConnStatus::Online);

        let added = client
            .add_interface_from_str(E2E_DEVICE_AGGREGATE)
            .await
            .unwrap();
        assert!(added);

        let Validated::AddInterface(interface) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };

        assert_eq!(interface, exp_interface);

        client
            .get_interface(exp_interface.interface_name(), |i| {
                assert_eq!(i, Some(&exp_interface));
            })
            .await;

        let added = client.add_interface(exp_interface).await.unwrap();
        assert!(!added);
        assert!(client.client_rx.is_empty());
    }

    #[tokio::test]
    async fn add_interface_missing_from_file() {
        let exp_interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let mut client = mock_client(&[], ConnStatus::Online);

        let dir = TempDir::new().unwrap();

        let path = dir.path().join("interface");
        std::fs::write(&path, E2E_DEVICE_AGGREGATE).unwrap();

        let added = client.add_interface_from_file(&path).await.unwrap();
        assert!(added);

        let Validated::AddInterface(interface) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };

        assert_eq!(interface, exp_interface);

        client
            .get_interface(exp_interface.interface_name(), |i| {
                assert_eq!(i, Some(&exp_interface));
            })
            .await;

        let added = client.add_interface(exp_interface).await.unwrap();
        assert!(!added);
        assert!(client.client_rx.is_empty());
    }

    #[tokio::test]
    async fn add_interface_major_with_retention_volatile() {
        let updated = Interface::from_str(for_update::E2E_DEVICE_DATASTREAM_1_0).unwrap();

        let mut client = mock_client(&[for_update::E2E_DEVICE_DATASTREAM_0_1], ConnStatus::Online);

        client
            .state
            .volatile_store()
            .push_sent(
                client.state.retention_ctx().next(),
                ValidatedIndividual {
                    interface: for_update::E2E_DEVICE_DATASTREAM_NAME.to_string(),
                    path: "/sensor_1/volatile".to_string(),
                    version_major: 0,
                    reliability: Reliability::Guaranteed,
                    retention: Retention::Volatile { expiry: None },
                    data: AstarteData::try_from(42.0).unwrap(),
                    timestamp: Some(Utc::now()),
                },
                true,
            )
            .await;

        let added = client.add_interface(updated.clone()).await.unwrap();
        assert!(added);

        let Validated::AddInterface(interface) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };

        assert_eq!(interface, updated);

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;

        assert!(client.state.volatile_store().pop_next().await.is_none());
    }

    #[tokio::test]
    async fn add_interface_major_with_retention_stored() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let mut client = mock_client_with_store(
            &[for_update::E2E_DEVICE_DATASTREAM_0_1],
            ConnStatus::Online,
            store,
        );

        let updated = Interface::from_str(for_update::E2E_DEVICE_DATASTREAM_1_0).unwrap();

        let id = client.state.retention_ctx().next();
        client
            .state
            .store()
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
                    data: AstarteData::try_from(42.0).unwrap(),
                    timestamp: Some(Utc::now()),
                },
                &[1, 2, 3, 4],
                true,
            )
            .await
            .unwrap();

        let added = client.add_interface(updated.clone()).await.unwrap();
        assert!(added);

        let Validated::AddInterface(interface) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };

        assert_eq!(interface, updated);

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;

        let packets = client
            .state
            .store()
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

        let mut client = mock_client(&[for_update::E2E_DEVICE_DATASTREAM_0_1], ConnStatus::Online);

        client
            .state
            .volatile_store()
            .push_sent(
                client.state.retention_ctx().next(),
                ValidatedIndividual {
                    interface: for_update::E2E_DEVICE_DATASTREAM_NAME.to_string(),
                    path: "/sensor_1/volatile".to_string(),
                    version_major: 0,
                    reliability: Reliability::Guaranteed,
                    retention: Retention::Volatile { expiry: None },
                    data: AstarteData::try_from(42.0).unwrap(),
                    timestamp: Some(Utc::now()),
                },
                true,
            )
            .await;

        let added = client.extend_interfaces([updated.clone()]).await.unwrap();
        assert_eq!(added, vec![updated.interface_name()]);

        let Validated::ExtendInterfaces(interfaces) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };

        let exp = mock_validated_collection(&[mock_validated_interface(updated.clone(), true)]);
        assert_eq!(interfaces, exp);

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;

        assert!(client.state.volatile_store().pop_next().await.is_none());
    }

    #[tokio::test]
    async fn extend_interfaces_major_with_retention_stored() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let mut client = mock_client_with_store(
            &[for_update::E2E_DEVICE_DATASTREAM_0_1],
            ConnStatus::Online,
            store,
        );

        let updated = Interface::from_str(for_update::E2E_DEVICE_DATASTREAM_1_0).unwrap();

        let id = client.state.retention_ctx().next();
        client
            .state
            .store()
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
                    data: AstarteData::try_from(42.0).unwrap(),
                    timestamp: Some(Utc::now()),
                },
                &[1, 2, 3, 4],
                true,
            )
            .await
            .unwrap();

        let added = client.extend_interfaces([updated.clone()]).await.unwrap();
        assert_eq!(added, [updated.interface_name()]);

        let Validated::ExtendInterfaces(interfaces) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };

        let exp = mock_validated_collection(&[mock_validated_interface(updated.clone(), true)]);
        assert_eq!(interfaces, exp);

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;

        let packets = client
            .state
            .store()
            .get_retention()
            .unwrap()
            .fetch_all_interfaces()
            .await
            .unwrap();
        assert!(packets.is_empty());
    }

    #[tokio::test]
    async fn extend_interfaces_nothing_to_add() {
        let client = mock_client(&[for_update::E2E_DEVICE_DATASTREAM_1_0], ConnStatus::Online);

        let updated = Interface::from_str(for_update::E2E_DEVICE_DATASTREAM_1_0).unwrap();

        let added = client.extend_interfaces([updated.clone()]).await.unwrap();
        assert!(added.is_empty());
        assert!(client.client_rx.is_empty());

        client
            .get_interface(updated.interface_name(), |i| {
                assert_eq!(i, Some(&updated));
            })
            .await;
    }

    #[tokio::test]
    async fn remove_interface_present() {
        let mut client = mock_client(&[E2E_DEVICE_AGGREGATE], ConnStatus::Online);

        let to_remove = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let removed = client
            .remove_interface(E2E_DEVICE_AGGREGATE_NAME)
            .await
            .unwrap();
        assert!(removed);

        let Validated::RemoveInterface(removed) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };
        let exp = RemovedInterface::from(&to_remove);
        assert_eq!(removed, exp);

        client
            .get_interface(E2E_DEVICE_AGGREGATE_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;
    }

    #[tokio::test]
    async fn remove_interface_property() {
        let mut client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Online);

        let to_remove = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();

        let path = "/sensor_1/double_endpoint";

        client
            .state
            .store()
            .store_prop(Prop {
                interface: E2E_DEVICE_PROPERTY_NAME.to_string(),
                path: path.to_string(),
                value: AstarteData::LongInteger(2),
                interface_major: to_remove.version_major(),
                ownership: to_remove.ownership(),
                updated_at: client.state.property_ctx().next_updated_at(),
            })
            .await
            .unwrap();

        let removed = client
            .remove_interface(E2E_DEVICE_PROPERTY_NAME)
            .await
            .unwrap();
        assert!(removed);

        let Validated::RemoveInterface(removed) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };
        let exp = RemovedInterface::from(&to_remove);
        assert_eq!(removed, exp);

        client
            .get_interface(E2E_DEVICE_PROPERTY_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;

        let prop = Properties::from_str(E2E_DEVICE_PROPERTY).unwrap();
        let path = MappingPath::try_from(path).unwrap();
        let mapping = MappingRef::new(&prop, &path).unwrap();

        let res = client
            .state
            .store()
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap();
        assert_eq!(res, None);
    }

    #[tokio::test]
    async fn remove_interface_not_found() {
        let client = mock_client(&[], ConnStatus::Online);

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
        let mut client = mock_client(&[E2E_DEVICE_AGGREGATE], ConnStatus::Online);

        let removed = client
            .remove_interfaces([E2E_DEVICE_AGGREGATE_NAME.to_string()])
            .await
            .unwrap();
        assert_eq!(removed, [E2E_DEVICE_AGGREGATE_NAME]);

        let Validated::RemoveInterfaceMany(removed) = client.client_rx.try_recv().unwrap() else {
            panic!()
        };
        let exp = RemovedInterface::from(&Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap());
        assert_eq!(removed, vec![exp]);

        client
            .get_interface(E2E_DEVICE_AGGREGATE_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;
    }

    #[tokio::test]
    async fn remove_interface_many_missing() {
        let client = mock_client(&[], ConnStatus::Online);

        let removed = client
            .remove_interfaces([E2E_DEVICE_AGGREGATE_NAME.to_string()])
            .await
            .unwrap();
        assert!(removed.is_empty());
        assert!(client.client_rx.is_empty());

        client
            .get_interface(E2E_DEVICE_AGGREGATE_NAME, |i| {
                assert_eq!(i, None);
            })
            .await;
    }
}
