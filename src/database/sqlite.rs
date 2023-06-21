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

//! Provides functionality for instantiating an Astarte sqlite database.

use std::str::FromStr;

use async_trait::async_trait;
use log::{debug, error, trace};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
    FromRow, Row,
};

use super::{AstarteDatabase, StoredProp};
use crate::{mqtt::Payload, types::AstarteType, utils, AstarteError};

#[deprecated = "Use SqliteStore instead"]
pub type AstarteSqliteDatabase = SqliteStore;

/// Data structure providing an implementation of a sqlite database.
///
/// Can be used by an Astarte device to store permanently properties values.
///
/// The values are stored as a BSON serialized SQLite BLOB. That can be then deserialized in the
/// respective [`AstarteType`].
#[derive(Clone, Debug)]
pub struct SqliteStore {
    db_conn: sqlx::SqlitePool,
}

impl SqliteStore {
    /// Creates an sqlite database for the Astarte device.
    ///
    /// URI should follow sqlite's convention, read [SqliteConnectOptions] for more details.
    ///
    /// ```no_run
    /// use astarte_device_sdk::database::sqlite::SqliteStore;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let database = SqliteStore::new("path/to/database/file.sqlite")
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn new(uri: &str) -> Result<Self, crate::options::AstarteOptionsError> {
        let options = SqliteConnectOptions::from_str(uri)?.create_if_missing(true);

        let conn = SqlitePoolOptions::new().connect_with(options).await?;

        sqlx::query("CREATE TABLE if not exists propcache (interface TEXT, path TEXT, value BLOB NOT NULL, interface_major INTEGER NOT NULL, PRIMARY KEY (interface, path))").execute(&conn).await?;

        Ok(SqliteStore { db_conn: conn })
    }
}

#[async_trait]
impl AstarteDatabase for SqliteStore {
    async fn store_prop(
        &self,
        interface: &str,
        path: &str,
        value: &AstarteType,
        interface_major: i32,
    ) -> Result<(), AstarteError> {
        debug!(
            "Storing property {} {} in db ({:?})",
            interface, path, value
        );

        let ser = utils::serialize_individual(value, None)?;

        sqlx::query(
                "insert or replace into propcache (interface, path, value, interface_major) VALUES (?,?,?,?)",
            )
            .bind(interface)
            .bind(path)
            .bind(ser)
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
    ) -> Result<Option<AstarteType>, AstarteError> {
        let res: Option<(Vec<u8>, i32)> = sqlx::query_as(
            "select value, interface_major from propcache where interface=? and path=?",
        )
        .bind(interface)
        .bind(path)
        .fetch_optional(&self.db_conn)
        .await?;

        match res {
            Some((data, version)) => {
                trace!(
                    "Loaded property {} {} in db (version {}, data: {:x?})",
                    interface,
                    path,
                    version,
                    data
                );

                // if version mismatch, delete
                if version != interface_major {
                    error!(
                        "Version mismatch for property {}{} (stored {}, interface {}). Deleting.",
                        interface, path, version, interface_major
                    );

                    self.delete_prop(interface, path).await?;

                    return Ok(None);
                }

                utils::deserialize_individual(&data).map(Some)
            }
            None => Ok(None),
        }
    }

    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), AstarteError> {
        sqlx::query("delete from propcache where interface=? and path=?")
            .bind(interface)
            .bind(path)
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn clear(&self) -> Result<(), AstarteError> {
        sqlx::query("delete from propcache")
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, AstarteError> {
        let res: Vec<StoredProp> = sqlx::query_as("select * from propcache")
            .fetch_all(&self.db_conn)
            .await?;

        return Ok(res);
    }
}

/// Convert a SqliteRow into a StoredProp
impl<'r> FromRow<'r, SqliteRow> for StoredProp {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        let data: &[u8] = row.try_get("value")?;
        let Payload { value, .. } =
            bson::from_slice(data).map_err(|err| sqlx::Error::ColumnDecode {
                index: "value".to_string(),
                source: err.into(),
            })?;

        Ok(StoredProp {
            interface: row.try_get("interface")?,
            path: row.try_get("path")?,
            value,
            interface_major: row.try_get("interface_major")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::tests::test_db;

    #[tokio::test]
    async fn test_db_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let path = db_path.as_path().to_str().unwrap();

        let db = SqliteStore::new(path).await.unwrap();

        test_db(db).await;
    }
}
