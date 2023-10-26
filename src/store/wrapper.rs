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

use crate::interface::{Reliability, Retention};
use crate::store::error::RetentionStoreError;
use crate::store::RetentionMessageBuilder;
use crate::types::AstarteType;

use super::{error::StoreError, PropertyStore, RetentionMessage, RetentionStore, StoredProp};

/// Wrapper for a generic [`AstarteDatabase`] to convert the error in [`Error`].
#[derive(Debug, Clone)]
pub(crate) struct StoreWrapper<S> {
    /// A generic store type
    pub(crate) store: S,
    /// A cache queue of both volatile and stored type retention messages
    retention_cache: Arc<RwLock<VecDeque<RetentionMessage>>>,
    /// A count of all retention message stored  since the beginning of the instance or after a
    /// clear operation.
    retention_stored_messages: Arc<RwLock<i64>>,
}

impl<S: PropertyStore + RetentionStore> StoreWrapper<S> {
    pub(crate) async fn new(store: S) -> Self {
        let retention_cache: Arc<RwLock<VecDeque<RetentionMessage>>> = Arc::new(Default::default());
        let stored_messages: Arc<RwLock<i64>> = Arc::new(Default::default());
        let retentions_stored = store.load_all_retention_messages().await.unwrap();

        {
            let mut retention_cache_guard = retention_cache.write().await;
            {
                let mut stored_messages = stored_messages.write().await;
                *stored_messages = retentions_stored.len() as i64;
            }

            for retention_message in retentions_stored.into_iter() {
                retention_cache_guard.push_back(retention_message);
            }
        }

        Self {
            store,
            retention_cache,
            retention_stored_messages: stored_messages,
        }
    }

    pub(crate) async fn store_retention_message(
        &self,
        retention: Retention,
        topic: String,
        payload: Vec<u8>,
        reliability: Reliability,
    ) -> Result<(), StoreError> {
        let mut retention_message = RetentionMessageBuilder::new(payload, reliability, topic);

        match retention {
            Retention::Discard => {}
            Retention::Volatile { expiry } => {
                let mut retention_cache = self.retention_cache.write().await;
                let retention_message = retention_message.expiry(expiry).build();
                retention_cache.push_back(retention_message);
            }
            Retention::Stored { expiry } => {
                let mut stored_messages = self.retention_stored_messages.write().await;
                let stored_messages_tmp = *stored_messages + 1;

                let retention_message = retention_message
                    .id(stored_messages_tmp)
                    .expiry(expiry)
                    .build();
                self.persist_retention_message(retention_message)
                    .await
                    .map_err(|err| StoreError::Store(err.into()))?;

                *stored_messages = stored_messages_tmp;
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

    async fn clear_props(&self) -> Result<(), Self::Err> {
        self.store.clear_props().await.map_err(StoreError::clear)
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
    type Err = RetentionStoreError;

    async fn clear_retention_messages(&self) -> Result<(), Self::Err> {
        let mut retention_cache = self.retention_cache.write().await;
        retention_cache.clear();

        self.store
            .clear_retention_messages()
            .await
            .map_err(|err| RetentionStoreError::Clear(err.into()))?;

        let mut stored_messages = self.retention_stored_messages.write().await;
        *stored_messages = 0;

        Ok(())
    }

    async fn front_retention_message(&self) -> Result<Option<RetentionMessage>, Self::Err> {
        let retention_cache = self.retention_cache.read().await;
        let retention_message = retention_cache.front().cloned();

        Ok(retention_message)
    }

    async fn is_empty_retention_message(&self) -> Result<bool, Self::Err> {
        let retention_cache = self.retention_cache.read().await;

        Ok(retention_cache.is_empty())
    }

    async fn load_all_retention_messages(&self) -> Result<Vec<RetentionMessage>, Self::Err> {
        self.store
            .load_all_retention_messages()
            .await
            .map_err(|err| RetentionStoreError::LoadAll(err.into()))
    }

    async fn persist_retention_message(
        &self,
        retention_message: RetentionMessage,
    ) -> Result<(), Self::Err> {
        self.store
            .persist_retention_message(retention_message.clone())
            .await
            .map_err(|err| RetentionStoreError::Store(err.into()))?;
        let mut retention_cache = self.retention_cache.write().await;
        retention_cache.push_back(retention_message);

        Ok(())
    }

    async fn remove_front_retention_message(&self) -> Result<(), Self::Err> {
        let mut retention_cache = self.retention_cache.write().await;
        let retention_message = retention_cache
            .pop_front()
            .ok_or(RetentionStoreError::Remove)?;

        if retention_message.id > 0 {
            self.store
                .remove_retention_message(retention_message)
                .await
                .map_err(|err| RetentionStoreError::RemoveFront(err.into()))?;
        }

        Ok(())
    }

    async fn remove_retention_message(
        &self,
        retention_message: RetentionMessage,
    ) -> Result<(), Self::Err> {
        let mut retention_cache = self.retention_cache.write().await;
        for i in 0..retention_cache.len() {
            let Some(rt) = retention_cache.get(i) else {
                continue;
            };

            if retention_message.id == rt.id {
                retention_cache.remove(i);
                break;
            }
        }

        if retention_message.id > 0 {
            self.store
                .remove_retention_message(retention_message)
                .await
                .map_err(|err| RetentionStoreError::RemoveFront(err.into()))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::test_retention_store;
    use crate::store::{memory::MemoryStore, tests::test_property_store, SqliteStore};

    fn generate_rt_messages() -> Vec<RetentionMessage> {
        let rt_msgs: Vec<RetentionMessage> = vec![
            RetentionMessage {
                id: 1,
                topic: "/device/interface/com.test1".to_string(),
                payload: vec![14u8, 11u8],
                reliability: Reliability::Guaranteed,
                absolute_expiry: None,
            },
            RetentionMessage {
                id: 2,
                topic: "/device/interface/com.test2".to_string(),
                payload: vec![24u8, 21u8],
                reliability: Reliability::Guaranteed,
                absolute_expiry: None,
            },
            RetentionMessage {
                id: 3,
                topic: "/device/interface/com.test3".to_string(),
                payload: vec![34u8, 31u8],
                reliability: Reliability::Guaranteed,
                absolute_expiry: None,
            },
            RetentionMessage {
                id: 4,
                topic: "/device/interface/com.test4".to_string(),
                payload: vec![44u8, 41u8],
                reliability: Reliability::Guaranteed,
                absolute_expiry: None,
            },
        ];

        rt_msgs
    }

    #[tokio::test]
    async fn test_memory_wrapped() {
        let db = StoreWrapper::new(MemoryStore::new()).await;

        test_property_store(db.clone()).await;
        test_retention_store(db).await;
    }

    #[tokio::test]
    async fn test_sqlite_wrapped() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let path = db_path.as_path().to_str().unwrap();

        let db = StoreWrapper::new(SqliteStore::new(path).await.unwrap()).await;

        test_property_store(db.clone()).await;
        test_retention_store(db).await;
    }

    #[tokio::test]
    async fn test_memory_with_initial_messages() {
        let db = MemoryStore::new();

        let rt_msgs = generate_rt_messages();

        for i in 0..rt_msgs.len() / 2 {
            db.persist_retention_message(rt_msgs.get(i).cloned().unwrap())
                .await
                .unwrap();
        }

        let store = StoreWrapper::new(db).await;
        test_retention_store_with_initial_value(store, rt_msgs).await;
    }

    #[tokio::test]
    async fn test_sqlite_with_initial_messages() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let path = db_path.as_path().to_str().unwrap();

        let db = SqliteStore::new(path).await.unwrap();

        let rt_msgs = generate_rt_messages();

        for i in 0..rt_msgs.len() / 2 {
            db.persist_retention_message(rt_msgs.get(i).cloned().unwrap())
                .await
                .unwrap();
        }

        let store = StoreWrapper::new(db).await;
        test_retention_store_with_initial_value(store, rt_msgs).await;
    }

    async fn test_retention_store_with_initial_value<S>(
        store: StoreWrapper<S>,
        rt_msgs: Vec<RetentionMessage>,
    ) where
        S: RetentionStore + PropertyStore,
    {
        assert!(!store.is_empty_retention_message().await.unwrap());

        for i in rt_msgs.len() / 2..rt_msgs.len() {
            let retention = Retention::Stored { expiry: 100 };
            let rt_msg = rt_msgs.get(i).cloned().unwrap();
            store
                .store_retention_message(
                    retention,
                    rt_msg.topic,
                    rt_msg.payload,
                    rt_msg.reliability,
                )
                .await
                .unwrap();
        }

        assert!(!store.is_empty_retention_message().await.unwrap());

        for rt_message in rt_msgs.iter() {
            assert_eq!(
                store
                    .front_retention_message()
                    .await
                    .unwrap()
                    .unwrap()
                    .payload,
                rt_message.payload
            );

            store.remove_front_retention_message().await.unwrap();
        }

        assert!(store.is_empty_retention_message().await.unwrap());
        assert!(store
            .load_all_retention_messages()
            .await
            .unwrap()
            .is_empty());
    }
}
