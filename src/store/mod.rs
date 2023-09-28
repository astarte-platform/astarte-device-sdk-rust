// This file is part of Astarte.
//
// Copyright 2021 SECO Mind Srl
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

//! Provides functionality for instantiating an Astarte sqlite database.

use std::{error::Error as StdError, fmt::Debug};

use async_trait::async_trait;

pub use self::sqlite::SqliteStore;
use crate::types::AstarteType;

pub mod error;
pub mod memory;
pub mod sqlite;
pub mod wrapper;

/// Trait providing compatibility with Astarte devices to databases.
///
/// Any database implementing this trait can be used as permanent storage for the properties
/// of an Astarte device.
///
/// This SDK provides an implementation of a sqlite database for which this trait has already
/// been implemented, see [`crate::store::sqlite::SqliteStore`].
#[async_trait]
pub trait PropertyStore: Debug + Send + Sync + 'static
where
    // NOTE: the bounds are required to be compatible with the tokio tasks, with an additional Sync
    //       bound to further restrict the error type.
    Self::Err: StdError + Send + Sync + 'static,
{
    type Err;

    /// Stores a property within the database.
    async fn store_prop(
        &self,
        interface: &str,
        path: &str,
        value: &AstarteType,
        interface_major: i32,
    ) -> Result<(), Self::Err>;
    /// Load a property from the database.
    ///
    /// The property store should delete the property from the database if the major version of the
    /// interface does not match the one provided.
    async fn load_prop(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, Self::Err>;
    /// Delete a property from the database.
    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), Self::Err>;
    /// Removes all saved properties from the database.
    async fn clear(&self) -> Result<(), Self::Err>;
    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err>;
}

/// Data structure used to return stored properties by a database implementing the AstarteDatabase
/// trait.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct StoredProp {
    pub interface: String,
    pub path: String,
    pub value: AstarteType,
    pub interface_major: i32,
}

#[cfg(test)]
mod tests {
    use crate::store::{memory::MemoryStore, wrapper::StoreWrapper};

    use super::*;

    pub(crate) async fn test_property_store<S>(store: S)
    where
        S: PropertyStore,
    {
        let ty = AstarteType::Integer(23);

        store.clear().await.unwrap();

        //non existing
        assert_eq!(store.load_prop("com.test", "/test", 1).await.unwrap(), None);

        store.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );

        //major version mismatch
        assert_eq!(store.load_prop("com.test", "/test", 2).await.unwrap(), None);

        // after mismatch the path should be deleted
        assert_eq!(store.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // delete
        store.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );
        store.delete_prop("com.test", "/test").await.unwrap();
        assert_eq!(store.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // unset
        store.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );
        store
            .store_prop("com.test", "/test", &AstarteType::Unset, 1)
            .await
            .unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            AstarteType::Unset
        );

        // clear
        store.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );
        store.clear().await.unwrap();
        assert_eq!(store.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // load all props
        let expected = [
            StoredProp {
                interface: "com.test".into(),
                path: "/test".into(),
                value: ty.clone(),
                interface_major: 1,
            },
            StoredProp {
                interface: "com.test2".into(),
                path: "/test".into(),
                value: ty.clone(),
                interface_major: 1,
            },
        ];

        store.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        store
            .store_prop("com.test2", "/test", &ty, 1)
            .await
            .unwrap();

        let mut props = store.load_all_props().await.unwrap();

        props.sort_unstable_by(|a, b| a.interface.cmp(&b.interface));

        assert_eq!(props, expected);
    }

    /// Test that the error is Send + Sync + 'static to be send across task boundaries.
    #[tokio::test]
    async fn erro_should_compatible_with_tokio() {
        let mem = StoreWrapper::new(MemoryStore::new());

        let exp = AstarteType::Integer(1);
        mem.store_prop("com.test", "/test", &exp, 1).await.unwrap();

        let res = tokio::spawn(async move { mem.load_prop("com.test", "/test", 1).await })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(res, Some(exp));
    }
}
