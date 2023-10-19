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

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::interface::Retention;
use crate::types::AstarteType;

use super::{error::StoreError, PropertyStore, RetentionMessage, RetentionStore, StoredProp};

/// Wrapper for a generic [`AstarteDatabase`] to convert the error in [`Error`].
#[derive(Debug, Clone)]
pub(crate) struct StoreWrapper<S> {
    pub(crate) store: S,
    retention_cache: Arc<RwLock<VecDeque<RetentionMessage>>>,
}

impl<S: PropertyStore + RetentionStore> StoreWrapper<S> {
    pub(crate) fn new(store: S) -> Self {
        Self {
            store,
            retention_cache: Arc::new(Default::default()),
        }
    }

    pub(crate) async fn store_retention_message(
        &self,
        retention: Retention,
        retention_message: RetentionMessage,
    ) -> Result<(), StoreError> {
        match retention {
            Retention::Discard => {}
            Retention::Volatile { .. } => {
                let mut retention_cache = self.retention_cache.write().await;
                retention_cache.push_back(retention_message);
            }
            Retention::Stored { .. } => {
                self.store
                    .persist(retention_message)
                    .await
                    .map_err(StoreError::store)?;
            }
        };

        Ok(())
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

    async fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, Self::Err> {
        self.store
            .interface_props(interface)
            .await
            .map_err(StoreError::interface_props)
    }
}

#[async_trait]
impl<S> RetentionStore for StoreWrapper<S>
where
    S: RetentionStore,
{
    type Err = StoreError;

    async fn persist(&self, retention_message: RetentionMessage) -> Result<(), Self::Err> {
        self.store
            .persist(retention_message)
            .await
            .map_err(StoreError::store)
    }

    async fn is_empty(&self) -> bool {
        self.store.is_empty().await
    }

    async fn peek_first(&self) -> Option<RetentionMessage> {
        self.store.peek_first().await
    }

    async fn ack_first(&self) {
        self.store.ack_first().await
    }

    async fn reject_first(&self) {
        self.store.reject_first().await
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
