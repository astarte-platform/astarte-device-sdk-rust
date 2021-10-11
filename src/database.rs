use async_trait::async_trait;
use std::str::FromStr;

use log::{debug, trace, warn};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use crate::{types::AstarteType, AstarteError, AstarteSdk};

/// Implementation of the [AstarteDatabase] trait for an sqlite database backend
#[derive(Clone, Debug)]
pub struct AstarteSqliteDatabase {
    db_conn: sqlx::Pool<sqlx::Sqlite>,
}

/// Database backend for the astarte client can be made by implementing this trait
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

    /// Removes all saved properties from the database
    async fn clear(&self) -> Result<(), AstarteError>;
}

#[async_trait]
impl AstarteDatabase for AstarteSqliteDatabase {
    async fn store_prop(
        &self,
        key: &str,
        value: &[u8],
        interface_major: i32,
    ) -> Result<(), AstarteError> {
        debug!("Storing property {} in db ({:?})", key, value);

        if value.is_empty() {
            //if unset?
            debug!("Unsetting {}", key);
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

impl AstarteSqliteDatabase {
    /// Creates an sqlite database for the astarte client
    /// URI should follow sqlite's convention, read [SqliteConnectOptions] for more details
    pub async fn new(uri: &str) -> Result<Self, crate::builder::AstarteBuilderError> {
        let options = SqliteConnectOptions::from_str(uri)?.create_if_missing(true);

        let conn = SqlitePoolOptions::new().connect_with(options).await?;

        sqlx::query("CREATE TABLE if not exists propcache (path TEXT PRIMARY KEY, value BLOB NOT NULL, interface_major INTEGER NOT NULL)").execute(&conn).await?;

        Ok(AstarteSqliteDatabase { db_conn: conn })
    }
}

#[cfg(test)]
mod test {
    use crate::database::AstarteDatabase;
    use crate::AstarteSdk;
    use crate::{database::AstarteSqliteDatabase, types::AstarteType};

    #[tokio::test]
    async fn test_db() {
        let db = AstarteSqliteDatabase::new("sqlite::memory:").await.unwrap();

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
