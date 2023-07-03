/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
//! Provides functionality for instantiating an Astarte sqlite database.

use async_trait::async_trait;
use std::str::FromStr;

use log::{debug, trace};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::FromRow;

use crate::payload;
use crate::{types::AstarteType, Error};

/// Data structure providing an implementation of a sqlite database.
///
/// Can be used by an Astarte device to store permanently properties values.
#[derive(Clone, Debug)]
pub struct AstarteSqliteDatabase {
    db_conn: sqlx::Pool<sqlx::Sqlite>,
}

/// Data structure used to return stored properties by a database implementing the AstarteDatabase
/// trait.
#[derive(FromRow, Debug, PartialEq)]
pub struct StoredProp {
    pub interface: String,
    pub path: String,
    pub value: Vec<u8>,
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
        value: &[u8],
        interface_major: i32,
    ) -> Result<(), Error>;
    /// Load a property from the database.
    async fn load_prop(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, Error>;
    /// Delete a property from the database.
    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), Error>;
    /// Removes all saved properties from the database.
    async fn clear(&self) -> Result<(), Error>;
    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Error>;
}

#[async_trait]
impl AstarteDatabase for AstarteSqliteDatabase {
    async fn store_prop(
        &self,
        interface: &str,
        path: &str,
        value: &[u8],
        interface_major: i32,
    ) -> Result<(), Error> {
        debug!(
            "Storing property {} {} in db ({:?})",
            interface, path, value
        );

        if value.is_empty() {
            //if unset?
            debug!("Unsetting {} {}", interface, path);
        }

        sqlx::query(
                "insert or replace into propcache (interface, path, value, interface_major) VALUES (?,?,?,?)",
            )
            .bind(interface)
            .bind(path)
            .bind(value)
            .bind(interface_major)
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn load_prop(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, Error> {
        let res: Option<(Vec<u8>, i32)> = sqlx::query_as(
            "select value, interface_major from propcache where interface=? and path=?",
        )
        .bind(interface)
        .bind(path)
        .fetch_optional(&self.db_conn)
        .await?;

        if let Some(res) = res {
            trace!("Loaded property {} {} in db ({:?})", interface, path, res.0);

            //if version mismatch, delete
            if res.1 != interface_major {
                self.delete_prop(interface, path).await?;
                return Ok(None);
            }

            let data = payload::deserialize(&res.0)?;

            match data {
                crate::Aggregation::Individual(data) => Ok(Some(data)),
                crate::Aggregation::Object(_) => Err(Error::Reported(
                    "BUG: extracting an object from the database".into(),
                )),
            }
        } else {
            Ok(None)
        }
    }

    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), Error> {
        sqlx::query("delete from propcache where interface=? and path=?")
            .bind(interface)
            .bind(path)
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn clear(&self) -> Result<(), Error> {
        sqlx::query("delete from propcache")
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Error> {
        let res: Vec<StoredProp> = sqlx::query_as("select * from propcache")
            .fetch_all(&self.db_conn)
            .await?;

        return Ok(res);
    }
}

impl AstarteSqliteDatabase {
    /// Creates an sqlite database for the Astarte device.
    ///
    /// URI should follow sqlite's convention, read [SqliteConnectOptions] for more details.
    ///
    /// ```no_run
    /// use astarte_device_sdk::database::AstarteSqliteDatabase;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let database = AstarteSqliteDatabase::new("path/to/database/file.sqlite")
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn new(uri: &str) -> Result<Self, crate::options::OptionsError> {
        let options = SqliteConnectOptions::from_str(uri)?.create_if_missing(true);

        let conn = SqlitePoolOptions::new().connect_with(options).await?;

        sqlx::query("CREATE TABLE if not exists propcache (interface TEXT, path TEXT, value BLOB NOT NULL, interface_major INTEGER NOT NULL, PRIMARY KEY (interface, path))").execute(&conn).await?;

        Ok(AstarteSqliteDatabase { db_conn: conn })
    }
}

#[cfg(test)]
mod test {
    use crate::database::AstarteDatabase;
    use crate::payload;
    use crate::{database::AstarteSqliteDatabase, database::StoredProp, types::AstarteType};

    #[tokio::test]
    async fn test_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let path = db_path.as_path().to_str().unwrap();

        let db = AstarteSqliteDatabase::new(path).await.unwrap();

        let ty = AstarteType::Integer(23);
        let ser = payload::serialize_individual(&ty, None).unwrap();

        db.clear().await.unwrap();

        //non existing
        assert_eq!(db.load_prop("com.test", "/test", 1).await.unwrap(), None);

        db.store_prop("com.test", "/test", &ser, 1).await.unwrap();
        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            ty
        );

        //major version mismatch
        assert_eq!(db.load_prop("com.test", "/test", 2).await.unwrap(), None);

        // after mismatch the path should be deleted
        assert_eq!(db.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // delete

        db.store_prop("com.test", "/test", &ser, 1).await.unwrap();
        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            ty
        );

        db.delete_prop("com.test", "/test").await.unwrap();

        assert_eq!(db.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // unset

        db.store_prop("com.test", "/test", &ser, 1).await.unwrap();
        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            ty
        );

        db.store_prop("com.test", "/test", &[], 1).await.unwrap();

        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            AstarteType::Unset
        );
        // clear

        db.store_prop("com.test", "/test", &ser, 1).await.unwrap();
        assert_eq!(
            db.load_prop("com.test", "/test", 1).await.unwrap().unwrap(),
            ty
        );

        db.clear().await.unwrap();

        assert_eq!(db.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // load all props
        db.store_prop("com.test", "/test", &ser, 1).await.unwrap();
        db.store_prop("com.test2", "/test", &ser, 1).await.unwrap();
        assert_eq!(
            db.load_all_props().await.unwrap(),
            vec![
                StoredProp {
                    interface: "com.test".into(),
                    path: "/test".into(),
                    value: ser.clone(),
                    interface_major: 1,
                },
                StoredProp {
                    interface: "com.test2".into(),
                    path: "/test".into(),
                    value: ser.clone(),
                    interface_major: 1,
                }
            ]
        );
    }
}
