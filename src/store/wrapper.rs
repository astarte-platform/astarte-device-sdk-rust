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

//! Provides functionality to wrap a generic Store to convert the error in Error.

use async_trait::async_trait;

use crate::types::AstarteType;

use super::{error::StoreError, PropertyStore, StoredProp};

/// Wrapper for a generic [`AstarteDatabase`] to convert the error in [`Error`].
#[derive(Debug, Clone)]
pub(crate) struct StoreWrapper<S> {
    pub(crate) store: S,
}

impl<S> StoreWrapper<S> {
    pub(crate) fn new(store: S) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> PropertyStore for StoreWrapper<S>
where
    S: PropertyStore,
{
    type Err = StoreError;

    async fn store_prop(&self, prop: StoredProp<&str, &AstarteType>) -> Result<(), Self::Err> {
        self.store.store_prop(prop).await.map_err(StoreError::store)
    }

    async fn load_prop(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, Self::Err> {
        self.store
            .load_prop(interface, path, interface_major)
            .await
            .map_err(StoreError::load)
    }

    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), Self::Err> {
        self.store
            .delete_prop(interface, path)
            .await
            .map_err(StoreError::delete)
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        self.store.clear().await.map_err(StoreError::clear)
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.store
            .load_all_props()
            .await
            .map_err(StoreError::load_all)
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.store
            .server_props()
            .await
            .map_err(StoreError::server_props)
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.store
            .device_props()
            .await
            .map_err(StoreError::device_props)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{memory::MemoryStore, tests::test_property_store, SqliteStore};

    #[tokio::test]
    async fn test_memory_wrapped() {
        let db = StoreWrapper::new(MemoryStore::new());

        test_property_store(db).await;
    }

    #[tokio::test]
    async fn test_sqlite_wrapped() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let path = db_path.as_path().to_str().unwrap();

        let db = SqliteStore::new(path).await.unwrap();

        test_property_store(db).await;
    }
}
