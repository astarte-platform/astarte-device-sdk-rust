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
 */

use async_trait::async_trait;
use std::str::FromStr;

use log::{info, trace, warn};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use crate::{types::AstarteType, AstarteError, AstarteSdk};

#[derive(Clone)]
pub struct Database {
    db_conn: sqlx::Pool<sqlx::Sqlite>,
}

#[async_trait]
pub trait AstarteDatabase {
    async fn store_prop(
        &self,
        key: &str,
        value: &[u8],
        interface_major: i32,
    ) -> Result<(), AstarteError>;
    async fn load_prop(
        &self,
        key: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, AstarteError>;
    async fn delete_prop(&self, key: &str) -> Result<(), AstarteError>;

    async fn clear(&self) -> Result<(), AstarteError>;
}

#[async_trait]
impl AstarteDatabase for Database {
    async fn store_prop(
        &self,
        key: &str,
        value: &[u8],
        interface_major: i32,
    ) -> Result<(), AstarteError> {
        trace!("Storing property {} in db ({:?})", key, value);

        if value.is_empty() {
            //if unset?
            trace!("Unsetting {}", key);
            self.delete_prop(key).await?;
        } else {
            //let serialized = crate::AstarteSdk::serialize_individual(value, None)?;
            sqlx::query(
                "insert or replace into propcache (path, value, interface_major) VALUES (?,?,?)",
            )
            .bind(key)
            .bind(value)
            .bind(interface_major)
            .execute(&self.db_conn)
            .await?;
        }

        Ok(())
    }

    async fn load_prop(
        &self,
        key: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, AstarteError> {
        let res: Option<(Vec<u8>, i32)> =
            sqlx::query_as("select value, interface_major from propcache where path=?")
                .bind(key)
                .fetch_optional(&self.db_conn)
                .await?;

        if let Some(res) = res {
            trace!("Loaded property {} in db ({:?})", key, res.0);

            //if version mismatch, delete
            if res.1 != interface_major {
                self.delete_prop(key).await?;
                return Ok(None);
            }

            let data = AstarteSdk::deserialize(&res.0)?;

            if let crate::Aggregation::Individual(data) = data {
                return Ok(Some(data));
            }

            warn!("why are we here?");

            Ok(None)
        } else {
            Ok(None)
        }
    }

    async fn delete_prop(&self, key: &str) -> Result<(), AstarteError> {
        sqlx::query("delete from propcache where path=?")
            .bind(key)
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
}

impl Database {
    #![allow(dead_code)]
    pub async fn new(path: &str) -> Result<Self, crate::AstarteBuilderError> {
        let exists = std::path::Path::new(&path).exists();

        info!("database exists: {} ({})", exists, path);

        let database_path = &format!("sqlite://{}", path);

        let options = SqliteConnectOptions::from_str(database_path)?.create_if_missing(true);

        let conn = SqlitePoolOptions::new().connect_with(options).await?;

        if !exists {
            info!("initializing db");
            sqlx::query("CREATE TABLE propcache (path TEXT PRIMARY KEY, value BLOB NOT NULL, interface_major INTEGER NOT NULL)").execute(&conn).await?;
        }

        Ok(Database { db_conn: conn })
    }
}

#[cfg(test)]
mod test {
    use crate::database::AstarteDatabase;
    use crate::AstarteSdk;
    use crate::{database::Database, types::AstarteType};

    /*
    #[test]
    fn test_ser() {
        let ty = AstarteType::Int32(23);

        let serialized = AstarteSdk::serialize_individual(ty, None);

        println!("{:?}", serialized);

        assert!(false);

    }*/

    #[tokio::test]
    async fn test_db() {
        let db = Database::new("/tmp/test-astarte-db.sqlite3").await.unwrap();

        let ty = AstarteType::Integer(23);
        let ser = AstarteSdk::serialize_individual(ty.clone(), None).unwrap();

        db.clear().await.unwrap();

        //non existing
        assert_eq!(db.load_prop("com.test/test", 1).await.unwrap(), None);

        db.store_prop("com.test/test", &ser, 1).await.unwrap();
        assert_eq!(db.load_prop("com.test/test", 1).await.unwrap().unwrap(), ty);

        //major version mismatch
        assert_eq!(db.load_prop("com.test/test", 2).await.unwrap(), None);

        // after mismatch the path should be deleted
        assert_eq!(db.load_prop("com.test/test", 1).await.unwrap(), None);

        // delete

        db.store_prop("com.test/test", &ser, 1).await.unwrap();
        assert_eq!(db.load_prop("com.test/test", 1).await.unwrap().unwrap(), ty);

        db.delete_prop("com.test/test").await.unwrap();

        assert_eq!(db.load_prop("com.test/test", 1).await.unwrap(), None);

        // unset

        db.store_prop("com.test/test", &ser, 1).await.unwrap();
        assert_eq!(db.load_prop("com.test/test", 1).await.unwrap().unwrap(), ty);

        db.store_prop("com.test/test", &[], 1).await.unwrap();

        assert_eq!(db.load_prop("com.test/test", 1).await.unwrap(), None);
        // clear

        db.store_prop("com.test/test", &ser, 1).await.unwrap();
        assert_eq!(db.load_prop("com.test/test", 1).await.unwrap().unwrap(), ty);

        db.clear().await.unwrap();

        assert_eq!(db.load_prop("com.test/test", 1).await.unwrap(), None);
    }
}
