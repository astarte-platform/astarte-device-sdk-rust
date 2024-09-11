// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
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

//! In memory store for the properties.

use std::{collections::HashMap, fmt::Display, hash::Hash, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::error;

use super::{OptStoredProp, PropertyStore, StoreCapabilities, StoredProp};
use crate::{interface::Ownership, retention::Missing, types::AstarteType};

/// Error from the memory store.
///
/// This error has no variants, but it is defined to allow for future changes.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum MemoryError {}

/// Data structure providing an implementation of an in memory Key Value Store.
///
/// Can be used by an Astarte device to store variables while the device is running.
#[derive(Debug, Clone, Default)]
pub struct MemoryStore {
    // Store the properties in memory
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
    type Retention = Missing;

    fn get_retention(&self) -> Option<&Self::Retention> {
        None
    }
}

#[async_trait]
impl PropertyStore for MemoryStore {
    type Err = MemoryError;

    async fn store_prop(
        &self,
        StoredProp {
            interface,
            path,
            value,
            interface_major,
            ownership,
        }: StoredProp<&str, &AstarteType>,
    ) -> Result<(), Self::Err> {
        let key = Key::new(interface, path);
        let value = Value {
            value: Some(value.clone()),
            interface_major,
            ownership,
        };

        let mut store = self.store.write().await;

        store.insert(key, value);

        Ok(())
    }

    async fn load_prop(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, Self::Err> {
        let key = Key::new(interface, path);

        // We need to drop the lock before calling delete_prop
        let opt_val = {
            let store = self.store.read().await;

            store.get(&key).cloned()
        };

        match opt_val {
            Some(value) if value.interface_major != interface_major => {
                error!(
                    "Version mismatch for property {}{} (stored {}, interface {}). Deleting.",
                    interface, path, value.interface_major, interface_major
                );

                self.delete_prop(interface, path).await?;

                Ok(None)
            }
            Some(value) => Ok(value.value),
            None => Ok(None),
        }
    }

    async fn unset_prop(&self, interface: &str, path: &str) -> Result<(), Self::Err> {
        let key = Key::new(interface, path);

        let mut writer = self.store.write().await;

        if let Some(value) = writer.get_mut(&key) {
            value.value = None;
        }

        Ok(())
    }

    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), Self::Err> {
        let key = Key::new(interface, path);

        let mut store = self.store.write().await;

        store.remove(&key);

        Ok(())
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        let mut store = self.store.write().await;

        store.clear();

        Ok(())
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let store = self.store.read().await;

        let props = store.iter().filter_map(|(k, v)| v.as_prop(k)).collect();

        Ok(props)
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let store = self.store.read().await;

        let props = store
            .iter()
            .filter_map(|(k, v)| match v.ownership {
                Ownership::Device => None,
                Ownership::Server => v.as_prop(k),
            })
            .collect();

        Ok(props)
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let store = self.store.read().await;

        let props = store
            .iter()
            .filter_map(|(k, v)| match v.ownership {
                Ownership::Device => v.as_prop(k),
                Ownership::Server => None,
            })
            .collect();

        Ok(props)
    }

    async fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, Self::Err> {
        Ok(self
            .store
            .read()
            .await
            .iter()
            .filter_map(|(k, v)| {
                if k.interface == interface {
                    v.as_prop(k)
                } else {
                    None
                }
            })
            .collect())
    }

    async fn delete_interface(&self, interface: &str) -> Result<(), Self::Err> {
        self.store
            .write()
            .await
            .retain(|k, _v| k.interface != interface);

        Ok(())
    }

    async fn device_props_with_unset(&self) -> Result<Vec<OptStoredProp>, Self::Err> {
        let store = self.store.read().await;

        let props = store
            .iter()
            .filter_map(|(k, v)| match v.ownership {
                Ownership::Device => Some(OptStoredProp::from((k, v))),
                Ownership::Server => None,
            })
            .collect();

        Ok(props)
    }
}

/// Key for the in memory store, this let us customize the hash and equality, and use (&str, &str)
/// to access the store.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct Key {
    interface: String,
    path: String,
}

impl Key {
    /// Creates a new Key
    fn new(interface: &str, path: &str) -> Self {
        Key {
            interface: interface.to_string(),
            path: path.to_string(),
        }
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.interface, self.path)
    }
}

/// Value for the memory store
#[derive(Debug, Clone)]
struct Value {
    value: Option<AstarteType>,
    interface_major: i32,
    ownership: Ownership,
}

impl Value {
    fn as_prop(&self, key: &Key) -> Option<StoredProp> {
        self.value.as_ref().map(|value| StoredProp {
            interface: key.interface.clone(),
            path: key.path.clone(),
            value: value.clone(),
            interface_major: self.interface_major,
            ownership: self.ownership,
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
