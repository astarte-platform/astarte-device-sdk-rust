// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
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

//! In memory store for the properties.

use std::collections::hash_map::Entry;
use std::num::NonZero;
use std::{collections::HashMap, fmt::Display, sync::Arc};

use astarte_device_error::Error;
use astarte_interfaces::schema::Ownership;
use astarte_interfaces::{Properties, Schema};
use tokio::sync::RwLock;
use tracing::error;

use super::error::StoreError;
use super::{
    OptStoredProp, Prop, PropMetadata, PropertyMapping, PropertyStore, StoreCapabilities,
    StoredProp, UpdatedAt,
};
use crate::store::{MissingCapability, PropertyState};
use crate::types::AstarteData;

/// Data structure providing an implementation of an in memory Key Value Store.
///
/// Can be used by an Astarte device to store variables while the device is running.
#[derive(Debug, Clone, Default)]
pub struct MemoryStore {
    // Store the properties in memory
    // TODO: we could use a separate index struct to keep the sorted order by updated_at of elements
    store: Arc<RwLock<HashMap<Key, Value>>>,
}

impl MemoryStore {
    /// Creates an in memory Key Value Store for the Astarte device.
    pub fn new() -> Self {
        MemoryStore {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl StoreCapabilities for MemoryStore {
    type Retention = MissingCapability;
    type Session = MissingCapability;

    fn get_retention(&self) -> Option<&Self::Retention> {
        None
    }

    fn get_session(&self) -> Option<&Self::Session> {
        None
    }
}

impl PropertyStore for MemoryStore {
    async fn store_prop(
        &self,
        Prop {
            interface,
            path,
            value,
            interface_major,
            ownership,
            updated_at,
        }: Prop,
    ) -> Result<PropMetadata, Error<StoreError>> {
        let mut store = self.store.write().await;

        let key = Key { interface, path };

        match store.entry(key) {
            Entry::Occupied(mut occupied_entry) => {
                let entry = occupied_entry.get_mut();

                if entry.value.as_ref().is_some_and(|old| *old == value)
                    && entry.interface_major == interface_major
                {
                    Ok(PropMetadata { epoch: None })
                } else {
                    entry.value = Some(value);
                    entry.interface_major = interface_major;
                    entry.state = PropertyState::Changed;
                    entry.updated_at = updated_at;
                    entry.epoch = entry.epoch.wrapping_add(1);

                    Ok(PropMetadata {
                        epoch: Some(entry.epoch),
                    })
                }
            }
            Entry::Vacant(vacant_entry) => {
                let entry = vacant_entry.insert(Value::new(
                    Some(value),
                    interface_major,
                    ownership,
                    updated_at,
                ));

                Ok(PropMetadata {
                    epoch: Some(entry.epoch),
                })
            }
        }
    }

    async fn update_state(
        &self,
        interface_name: &str,
        path: &str,
        state: PropertyState,
        epoch: u8,
    ) -> Result<bool, Error<StoreError>> {
        let key = Key {
            interface: interface_name.to_string(),
            path: path.to_string(),
        };

        if let Some(val) = self.store.write().await.get_mut(&key)
            && val.epoch == epoch
        {
            val.state = state;

            if state == PropertyState::Completed {
                val.epoch = 0;
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn load_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> Result<Option<StoredProp>, Error<StoreError>> {
        let key = Key {
            interface: property.interface_name().to_string(),
            path: property.path().to_string(),
        };

        let mut store = self.store.write().await;

        // We need to drop the lock before calling delete_prop
        match store.entry(key.clone()) {
            Entry::Occupied(entry) => {
                if property.version_major() != entry.get().interface_major {
                    error!(
                        "Version mismatch for property {}{} (stored {}, interface {}). Deleting.",
                        property.interface_name(),
                        property.path(),
                        entry.get().interface_major,
                        property.version_major()
                    );

                    entry.remove();

                    Ok(None)
                } else {
                    let value = entry.get().as_prop(&key);

                    Ok(value)
                }
            }
            Entry::Vacant(_) => Ok(None),
        }
    }

    async fn unset_prop(
        &self,
        property: &PropertyMapping<'_>,
        updated_at: UpdatedAt,
    ) -> Result<PropMetadata, Error<StoreError>> {
        let key = Key {
            interface: property.interface_name().to_string(),
            path: property.path().to_string(),
        };

        let mut writer = self.store.write().await;

        let Some(value) = writer.get_mut(&key) else {
            return Ok(PropMetadata { epoch: None });
        };

        if value.value.is_none() {
            return Ok(PropMetadata { epoch: None });
        }

        value.value = None;
        value.epoch = value.epoch.wrapping_add(1);
        value.updated_at = updated_at;

        Ok(PropMetadata {
            epoch: Some(value.epoch),
        })
    }

    async fn delete_device_prop(
        &self,
        interface_name: &str,
        path: &str,
        epoch: u8,
    ) -> Result<bool, Error<StoreError>> {
        let key = Key {
            interface: interface_name.to_string(),
            path: path.to_string(),
        };

        let mut store = self.store.write().await;

        let Entry::Occupied(entry) = store.entry(key) else {
            return Ok(false);
        };

        if entry.get().epoch != epoch {
            return Ok(false);
        }

        entry.remove();

        Ok(true)
    }

    async fn delete_server_prop(
        &self,
        interface_name: &str,
        path: &str,
    ) -> Result<bool, Error<StoreError>> {
        let key = Key {
            interface: interface_name.to_string(),
            path: path.to_string(),
        };

        let mut store = self.store.write().await;

        let deleted = store.remove(&key).is_some();

        Ok(deleted)
    }

    async fn clear(&self) -> Result<(), Error<StoreError>> {
        let mut store = self.store.write().await;

        store.clear();

        Ok(())
    }

    async fn load_all_props(
        &self,
        limit: NonZero<usize>,
        last_update_at: Option<UpdatedAt>,
    ) -> Result<Vec<StoredProp>, Error<StoreError>> {
        let store = self.store.read().await;

        let mut props = store
            .iter()
            .filter_map(|(k, v)| {
                if last_update_at.is_some_and(|t| v.updated_at <= t) {
                    return None;
                }

                v.as_prop(k)
            })
            .collect::<Vec<_>>();

        props.sort_unstable_by_key(|p| p.updated_at);

        props.truncate(limit.get());

        Ok(props)
    }

    async fn server_props(
        &self,
        limit: NonZero<usize>,
        last_update_at: Option<UpdatedAt>,
    ) -> Result<Vec<StoredProp>, Error<StoreError>> {
        let store = self.store.read().await;

        let mut props = store
            .iter()
            .filter_map(|(k, v)| {
                if last_update_at.is_some_and(|t| v.updated_at <= t) {
                    return None;
                }

                match v.ownership {
                    Ownership::Device => None,
                    Ownership::Server => v.as_prop(k),
                }
            })
            .collect::<Vec<_>>();

        props.sort_unstable_by_key(|p| p.updated_at);

        props.truncate(limit.get());

        Ok(props)
    }

    async fn device_props(
        &self,
        limit: NonZero<usize>,
        last_update_at: Option<UpdatedAt>,
    ) -> Result<Vec<StoredProp>, Error<StoreError>> {
        let store = self.store.read().await;

        let mut props = store
            .iter()
            .filter_map(|(k, v)| {
                if last_update_at.is_some_and(|t| v.updated_at <= t) {
                    return None;
                }

                match v.ownership {
                    Ownership::Device => v.as_prop(k),
                    Ownership::Server => None,
                }
            })
            .collect::<Vec<_>>();

        props.sort_unstable_by_key(|p| p.updated_at);

        props.truncate(limit.get());

        Ok(props)
    }

    async fn interface_props(
        &self,
        interface: &Properties,
        limit: NonZero<usize>,
        last_update_at: Option<UpdatedAt>,
    ) -> Result<Vec<StoredProp>, Error<StoreError>> {
        let store = self.store.read().await;

        let mut props = store
            .iter()
            .filter_map(|(k, v)| {
                if last_update_at.is_some_and(|t| v.updated_at <= t) {
                    return None;
                }

                if k.interface == interface.name() {
                    v.as_prop(k)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        props.sort_unstable_by_key(|p| p.updated_at);

        props.truncate(limit.get());

        Ok(props)
    }

    async fn delete_interface(&self, interface: &Properties) -> Result<(), Error<StoreError>> {
        self.store
            .write()
            .await
            .retain(|k, _v| k.interface != interface.name());

        Ok(())
    }

    async fn device_props_with_unset(
        &self,
        state: PropertyState,
        limit: NonZero<usize>,
        last_update_at: Option<UpdatedAt>,
    ) -> Result<Vec<OptStoredProp>, Error<StoreError>> {
        let store = self.store.read().await;

        // TODO: this allocates all the props
        let mut props = store
            .iter()
            .filter_map(|(k, v)| {
                if v.state != state {
                    return None;
                }

                if last_update_at.is_some_and(|t| v.updated_at <= t) {
                    return None;
                }

                match v.ownership {
                    Ownership::Device => Some(OptStoredProp::from((k, v))),
                    Ownership::Server => None,
                }
            })
            .collect::<Vec<_>>();

        props.sort_unstable_by_key(|p| p.updated_at);

        props.truncate(limit.get());

        Ok(props)
    }

    async fn reset_session(&self) -> Result<(), Error<StoreError>> {
        let mut store = self.store.write().await;

        store.retain(|_k, v| v.value.is_some());

        store
            .values_mut()
            .filter(|v| v.ownership == Ownership::Device)
            .for_each(|v| {
                v.state = PropertyState::Changed;
                v.epoch = 0;
            });

        Ok(())
    }
}

/// Key for the in memory store, this let us customize the hash and equality, and use (&str, &str)
/// to access the store.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct Key {
    interface: String,
    path: String,
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.interface, self.path)
    }
}

/// Value for the memory store
#[derive(Debug, Clone)]
struct Value {
    value: Option<AstarteData>,
    interface_major: i32,
    ownership: Ownership,
    state: PropertyState,
    epoch: u8,
    updated_at: UpdatedAt,
}

impl Value {
    fn new(
        value: Option<AstarteData>,
        interface_major: i32,
        ownership: Ownership,
        updated_at: UpdatedAt,
    ) -> Self {
        Self {
            value,
            interface_major,
            ownership,
            state: PropertyState::Changed,
            epoch: 0,
            updated_at,
        }
    }

    fn as_prop(&self, key: &Key) -> Option<StoredProp> {
        let Some(value) = &self.value else {
            return None;
        };

        Some(StoredProp {
            interface: key.interface.clone(),
            path: key.path.clone(),
            value: value.clone(),
            interface_major: self.interface_major,
            ownership: self.ownership,
            epoch: self.epoch,
            updated_at: self.updated_at,
        })
    }
}

impl From<(&Key, &Value)> for OptStoredProp {
    fn from((key, value): (&Key, &Value)) -> Self {
        Self {
            interface: key.interface.clone(),
            path: key.path.clone(),
            value: value.value.clone(),
            interface_major: value.interface_major,
            ownership: value.ownership,
            epoch: value.epoch,
            updated_at: value.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::test_property_store;

    #[tokio::test]
    async fn test_memory_store() {
        let db = MemoryStore::new();

        test_property_store(db).await;
    }
}
