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

use self::error::Error;
pub use self::sqlite::SqliteStore;
use crate::types::AstarteType;

pub mod error;
pub mod memory;
pub mod sqlite;

/// Trait providing compatibility with Astarte devices to databases.
///
/// Any database implementing this trait can be used as permanent storage for the properties
/// of an Astarte device.
///
/// This SDK provides an implementation of a sqlite database for which this trait has already
/// been implemented, see [`crate::store::sqlite::SqliteStore`].
// NOTE: the 'static bound is required for the MSRV, in other version the error is not present
#[async_trait]
pub trait PropertyStore: Debug + Send + Sync + 'static
where
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

/// Wrapper for a generic [`AstarteDatabase`] to convert the error in [`Error`].
#[derive(Debug, Clone)]
pub(crate) struct StoreWrapper<S>
where
    S: PropertyStore,
{
    pub(crate) store: S,
}

impl<S> StoreWrapper<S>
where
    S: PropertyStore,
{
    pub(crate) fn new(store: S) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> PropertyStore for StoreWrapper<S>
where
    S: PropertyStore,
{
    type Err = Error;

    async fn store_prop(
        &self,
        interface: &str,
        path: &str,
        value: &AstarteType,
        interface_major: i32,
    ) -> Result<(), Self::Err> {
        self.store
            .store_prop(interface, path, value, interface_major)
            .await
            .map_err(Error::store)
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
            .map_err(Error::load)
    }

    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), Self::Err> {
        self.store
            .delete_prop(interface, path)
            .await
            .map_err(Error::delete)
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        self.store.clear().await.map_err(Error::clear)
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.store.load_all_props().await.map_err(Error::load_all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) async fn test_db<D>(db: D)
    where
        D: PropertyStore,
    {
        let ty = AstarteType::Integer(23);

        db.clear().await.unwrap();

        //non existing
        assert_eq!(db.load_prop("com.test", "/test", 1).await.unwrap(), None);

        db.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            ty
        );

        //major version mismatch
        assert_eq!(db.load_prop("com.test", "/test", 2).await.unwrap(), None);

        // after mismatch the path should be deleted
        assert_eq!(db.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // delete
        db.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            ty
        );
        db.delete_prop("com.test", "/test").await.unwrap();
        assert_eq!(db.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // unset
        db.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            ty
        );
        db.store_prop("com.test", "/test", &AstarteType::Unset, 1)
            .await
            .unwrap();
        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            AstarteType::Unset
        );

        // clear
        db.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            ty
        );
        db.clear().await.unwrap();
        assert_eq!(db.load_prop("com.test", "/test", 1).await.unwrap(), None);

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

        db.store_prop("com.test", "/test", &ty, 1).await.unwrap();
        db.store_prop("com.test2", "/test", &ty, 1).await.unwrap();

        let mut props = db.load_all_props().await.unwrap();

        props.sort_unstable_by(|a, b| a.interface.cmp(&b.interface));

        assert_eq!(props, expected);
    }
}
