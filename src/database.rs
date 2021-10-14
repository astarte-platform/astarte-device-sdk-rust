use async_trait::async_trait;
use std::str::FromStr;

use log::{debug, trace};
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
        interface: &str,
        path: &str,
        value: &[u8],
        interface_major: i32,
    ) -> Result<(), AstarteError>;
    async fn load_prop(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, AstarteError>;
    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), AstarteError>;

    /// Removes all saved properties from the database
    async fn clear(&self) -> Result<(), AstarteError>;
}

#[async_trait]
impl AstarteDatabase for AstarteSqliteDatabase {
    async fn store_prop(
        &self,
        interface: &str,
        path: &str,
        value: &[u8],
        interface_major: i32,
    ) -> Result<(), AstarteError> {
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
    ) -> Result<Option<AstarteType>, AstarteError> {
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

            if res.0.is_empty() {
                return Ok(Some(AstarteType::Unset));
            }

            let data = AstarteSdk::deserialize(&res.0)?;

            match data {
                crate::Aggregation::Individual(data) => Ok(Some(data)),
                crate::Aggregation::Object(_) => Err(AstarteError::Reported(
                    "BUG: extracting an object from the database".into(),
                )),
            }
        } else {
            Ok(None)
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
}

impl AstarteSqliteDatabase {
    /// Creates an sqlite database for the astarte client
    /// URI should follow sqlite's convention, read [SqliteConnectOptions] for more details
    pub async fn new(uri: &str) -> Result<Self, crate::builder::AstarteBuilderError> {
        let options = SqliteConnectOptions::from_str(uri)?.create_if_missing(true);

        let conn = SqlitePoolOptions::new().connect_with(options).await?;

        sqlx::query("CREATE TABLE if not exists propcache (interface TEXT, path TEXT, value BLOB NOT NULL, interface_major INTEGER NOT NULL, PRIMARY KEY (interface, path))").execute(&conn).await?;

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
    }
}
