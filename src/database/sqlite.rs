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
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::{AstarteDatabase, StoredProp};
use crate::{mqtt::Payload, types::AstarteType, utils, AstarteError};

#[deprecated = "Use SqliteStore instead"]
pub type AstarteSqliteDatabase = SqliteStore;

/// Error returned by the [`SqliteStore`]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("could not parse the database uri: {uri}")]
    Uri {
        #[source]
        err: sqlx::Error,
        uri: String,
    },
    #[error("could not connect to database")]
    Connection(#[source] sqlx::Error),
    #[error("could not run migration")]
    Migration(sqlx::migrate::MigrateError),
    #[error("could not execute query")]
    Query(#[from] sqlx::Error),
    #[error("could not decode property from bson")]
    Decode(#[from] bson::de::Error),

    // TODO: refactor errors
    #[error(transparent)]
    AstarteError(#[from] AstarteError),
}

/// Result of the load_prop query
#[derive(Debug, Clone)]
struct PropRecord {
    value: Vec<u8>,
    interface_major: i32,
}

/// Result of the load_prop query
#[derive(Debug, Clone)]
struct StoredRecord {
    interface: String,
    path: String,
    value: Vec<u8>,
    interface_major: i32,
}

impl TryFrom<StoredRecord> for StoredProp {
    type Error = bson::de::Error;

    fn try_from(value: StoredRecord) -> Result<Self, Self::Error> {
        let payload: Payload = bson::from_slice(&value.value)?;

        Ok(StoredProp {
            interface: value.interface,
            path: value.path,
            value: payload.value,
            interface_major: value.interface_major,
        })
    }
}

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
    pub async fn new(uri: &str) -> Result<Self, Error> {
        let options = SqliteConnectOptions::from_str(uri)
            .map_err(|err| Error::Uri {
                err,
                uri: uri.to_string(),
            })?
            .create_if_missing(true);

        let conn = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(Error::Connection)?;

        // Run the migrations if needed
        sqlx::migrate!()
            .run(&conn)
            .await
            .map_err(Error::Migration)?;

        Ok(SqliteStore { db_conn: conn })
    }
}

#[async_trait]
impl AstarteDatabase for SqliteStore {
    type Err = Error;

    async fn store_prop_impl(
        &self,
        interface: &str,
        path: &str,
        value: &AstarteType,
        interface_major: i32,
    ) -> Result<(), Self::Err> {
        debug!(
            "Storing property {} {} in db ({:?})",
            interface, path, value
        );

        let ser = utils::serialize_individual(value, None)?;

        sqlx::query_file!(
            "queries/store_prop.sql",
            interface,
            path,
            ser,
            interface_major
        )
        .execute(&self.db_conn)
        .await?;

        Ok(())
    }

    async fn load_prop_impl(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, Self::Err> {
        let res: Option<PropRecord> =
            sqlx::query_file_as!(PropRecord, "queries/load_prop.sql", interface, path)
                .fetch_optional(&self.db_conn)
                .await?;

        match res {
            Some(record) => {
                trace!("Loaded property {} {} in db {:?}", interface, path, record);

                // if version mismatch, delete
                if record.interface_major != interface_major {
                    error!(
                        "Version mismatch for property {}{} (stored {}, interface {}). Deleting.",
                        interface, path, record.interface_major, interface_major
                    );

                    self.delete_prop_impl(interface, path).await?;

                    return Ok(None);
                }

                utils::deserialize_individual(&record.value)
                    .map(Some)
                    .map_err(Error::from)
            }
            None => Ok(None),
        }
    }

    async fn delete_prop_impl(&self, interface: &str, path: &str) -> Result<(), Self::Err> {
        sqlx::query_file!("queries/delete_prop.sql", interface, path)
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn clear_impl(&self) -> Result<(), Self::Err> {
        sqlx::query!("delete from propcache")
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn load_all_props_impl(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let res: Vec<StoredProp> = sqlx::query_file_as!(StoredRecord, "queries/load_all_props.sql")
            .try_map(|row| StoredProp::try_from(row).map_err(|err| sqlx::Error::Decode(err.into())))
            .fetch_all(&self.db_conn)
            .await?;

        Ok(res)
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
