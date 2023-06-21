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

use async_trait::async_trait;

pub use self::sqlite::SqliteStore;
use crate::{types::AstarteType, AstarteError};

pub mod memory;
pub mod sqlite;

/// Data structure used to return stored properties by a database implementing the AstarteDatabase
/// trait.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct StoredProp {
    pub interface: String,
    pub path: String,
    pub value: AstarteType,
    pub interface_major: i32,
}

/// Trait providing compatibility with Astarte devices to databases.
///
/// Any database implementing this trait can be used as permanent storage for the properties
/// of an Astarte device.
///
/// This SDK provides an implementation of a sqlite database for which this trait has already
/// been implemented, see [`AstarteSqliteDatabase`].
#[async_trait]
pub trait AstarteDatabase {
    /// Stores a property within the database.
    async fn store_prop(
        &self,
        interface: &str,
        path: &str,
        value: &AstarteType,
        interface_major: i32,
    ) -> Result<(), AstarteError>;
    /// Load a property from the database.
    async fn load_prop(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, AstarteError>;
    /// Delete a property from the database.
    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), AstarteError>;
    /// Removes all saved properties from the database.
    async fn clear(&self) -> Result<(), AstarteError>;
    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    async fn load_all_props(&self) -> Result<Vec<StoredProp>, AstarteError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) async fn test_db(db: impl AstarteDatabase) {
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
