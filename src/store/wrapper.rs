// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Provides functionality to wrap a generic Store to convert the error in Error.

use tracing::trace;

use astarte_interfaces::{Properties, schema::Ownership};

use crate::{store::PropertyState, types::AstarteData};

use super::{
    OptStoredProp, PropertyMapping, PropertyStore, StoreCapabilities, StoredProp, error::StoreError,
};

/// Wrapper for a generic [`PropertyStore`] to convert the error in [`Error`](crate::Error).
#[derive(Debug, Clone)]
pub(crate) struct StoreWrapper<S> {
    pub(crate) store: S,
}

impl<S> StoreWrapper<S> {
    pub(crate) fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S> StoreCapabilities for StoreWrapper<S>
where
    S: StoreCapabilities,
{
    type Retention = S::Retention;
    type Session = S::Session;

    fn get_retention(&self) -> Option<&Self::Retention> {
        let retention = self.store.get_retention();

        if retention.is_none() {
            trace!("no stored retention");
        }

        retention
    }

    fn get_session(&self) -> Option<&Self::Session> {
        let session = self.store.get_session();

        if session.is_none() {
            trace!("no persistent session");
        }

        session
    }
}

impl<S> PropertyStore for StoreWrapper<S>
where
    S: PropertyStore,
{
    type Err = StoreError;

    async fn store_prop(&self, prop: StoredProp<&str, &AstarteData>) -> Result<(), Self::Err> {
        self.store.store_prop(prop).await.map_err(StoreError::store)
    }

    async fn update_state(
        &self,
        property: &PropertyMapping<'_>,
        state: super::PropertyState,
        expected: Option<AstarteData>,
    ) -> Result<bool, Self::Err> {
        self.store
            .update_state(property, state, expected)
            .await
            .map_err(StoreError::update_state)
    }

    async fn load_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> Result<Option<AstarteData>, Self::Err> {
        self.store
            .load_prop(property)
            .await
            .map_err(StoreError::load)
    }

    async fn unset_prop(&self, property: &PropertyMapping<'_>) -> Result<(), Self::Err> {
        self.store
            .unset_prop(property)
            .await
            .map_err(StoreError::unset)
    }

    async fn delete_prop(&self, interface: &PropertyMapping<'_>) -> Result<(), Self::Err> {
        self.store
            .delete_prop(interface)
            .await
            .map_err(StoreError::delete)
    }

    async fn delete_expected_prop(
        &self,
        property: &PropertyMapping<'_>,
        expected: Option<AstarteData>,
    ) -> Result<bool, Self::Err> {
        self.store
            .delete_expected_prop(property, expected)
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

    async fn interface_props(&self, interface: &Properties) -> Result<Vec<StoredProp>, Self::Err> {
        self.store
            .interface_props(interface)
            .await
            .map_err(StoreError::interface_props)
    }

    async fn delete_interface(&self, interface: &Properties) -> Result<(), Self::Err> {
        self.store
            .delete_interface(interface)
            .await
            .map_err(StoreError::delete_interface)
    }

    async fn device_props_with_unset(
        &self,
        state: PropertyState,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<OptStoredProp>, Self::Err> {
        self.store
            .device_props_with_unset(state, limit, offset)
            .await
            .map_err(StoreError::device_props)
    }

    async fn reset_state(&self, ownership: Ownership) -> Result<(), Self::Err> {
        self.store
            .reset_state(ownership)
            .await
            .map_err(StoreError::reset_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{SqliteStore, memory::MemoryStore, tests::test_property_store};

    #[tokio::test]
    async fn test_memory_wrapped() {
        let db = StoreWrapper::new(MemoryStore::new());

        test_property_store(db).await;
    }

    #[tokio::test]
    async fn test_sqlite_wrapped() {
        let dir = tempfile::tempdir().unwrap();

        let db = SqliteStore::options()
            .with_writable_dir(dir.as_ref())
            .await
            .unwrap();

        test_property_store(db).await;
    }
}
