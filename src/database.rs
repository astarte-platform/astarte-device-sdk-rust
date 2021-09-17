use async_trait::async_trait;
use std::str::FromStr;

use log::{info, trace};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use crate::{types::AstarteType, AstarteError, AstarteSdk};

#[derive(Clone)]
pub struct Database {
    db_conn: sqlx::Pool<sqlx::Sqlite>,
}

#[async_trait]
pub trait AstarteDatabase {
    async fn store_prop(&self, key: &str, value: &[u8], version: i32) -> Result<(), AstarteError>;
    async fn load_prop(&self, key: &str, version: i32)
        -> Result<Option<AstarteType>, AstarteError>;
    async fn delete_prop(&self, key: &str) -> Result<(), AstarteError>;

    async fn clear(&self) -> Result<(), AstarteError>;
}

#[async_trait]
impl AstarteDatabase for Database {
    async fn store_prop(&self, key: &str, value: &[u8], version: i32) -> Result<(), AstarteError> {
        trace!("Storing property {} in db ({:?})", key, value);

        //let serialized = crate::AstarteSdk::serialize_individual(value, None)?;
        sqlx::query("insert or replace into propcache (path, value, version) VALUES (?,?,?)")
            .bind(key)
            .bind(value)
            .bind(version)
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn load_prop(
        &self,
        key: &str,
        version: i32,
    ) -> Result<Option<AstarteType>, AstarteError> {
        let res: (Vec<u8>, i32) =
            sqlx::query_as("select value, version from propcache where path=?")
                .bind(key)
                .fetch_one(&self.db_conn)
                .await?;

        trace!("Loaded property {} in db ({:?})", key, res.0);

        if res.1 != version {
            self.delete_prop(key).await?;
            return Ok(None);
        }

        let data = AstarteSdk::deserialize(&res.0)?;

        if let crate::Aggregation::Individual(data) = data {
            return Ok(Some(data));
        }

        Ok(None)
    }

    async fn delete_prop(&self, key: &str) -> Result<(), AstarteError> {
        sqlx::query("delete from propcache where value=?")
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
    pub async fn new(path: &str) -> Result<Self, crate::AstarteBuilderError> {
        let exists = std::path::Path::new(&path).exists();

        info!("database exists: {} ({})", exists, path);

        let database_path = &format!("sqlite://{}", path);

        let options = SqliteConnectOptions::from_str(database_path)?.create_if_missing(true);

        let conn = SqlitePoolOptions::new().connect_with(options).await?;

        if !exists {
            info!("initializing db");
            sqlx::query("CREATE TABLE propcache (path TEXT PRIMARY KEY, value BLOB NOT NULL, version INTEGER NOT NULL)").execute(&conn).await?;
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
        let db = Database::new("/tmp/test.sqlite3").await.unwrap();

        let ty = AstarteType::Integer(23);
        let ser = AstarteSdk::serialize_individual(ty.clone(), None).unwrap();
        db.store_prop("com.test/test", &ser, 1).await.unwrap();
        let res = db.load_prop("com.test/test", 1).await.unwrap().unwrap();

        assert_eq!(ty, res);

        let res = db.load_prop("com.test/test", 2).await.unwrap();

        assert_eq!(None, res);
    }
}
