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

use std::{fmt::Debug, str::FromStr};

use async_trait::async_trait;
use log::{debug, error, trace};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::{PropertyStore, StoredProp};
use crate::{
    interface::{MappingType, Ownership},
    transport::mqtt::payload::{Payload, PayloadError},
    types::{AstarteType, BsonConverter, TypeError},
};

/// Error returned by the [`SqliteStore`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    /// Error returned when the database uri is not valid
    #[error("could not parse the database uri: {uri}")]
    Uri {
        #[source]
        err: sqlx::Error,
        uri: String,
    },
    /// Error returned when the database connection fails
    #[error("could not connect to database")]
    Connection(#[source] sqlx::Error),
    /// Error returned when the database migration fails
    #[error("could not run migration")]
    Migration(sqlx::migrate::MigrateError),
    /// Error returned when the database query fails
    #[error("could not execute query")]
    Query(#[from] sqlx::Error),
    /// Couldn't convert the stored value.
    #[error("couldn't convert the stored value")]
    Value(#[from] ValueError),
    /// Error returned when the decode of the bson fails
    #[error("could not decode property from bson")]
    #[deprecated = "moved to the ValueError"]
    Decode(#[from] PayloadError),
    /// Couldn't convert ownership value
    #[error("could not deserialize ownership")]
    Ownership(#[from] OwnershipError),
}

/// Error when converting a u8 into the [`Ownership`] struct
#[derive(Debug, thiserror::Error)]
#[error("invalid ownership value {value}")]
pub struct OwnershipError {
    value: u8,
}

/// Ownership of a property.
///
/// The ownership is an enum stored as an single byte integer (u8) in the SQLite database, the values
/// for the enum are:
/// - **Device owned**: 0
/// - **Server owned**: 1
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum RecordOwnership {
    Device = 0,
    Server = 1,
}

impl TryFrom<u8> for RecordOwnership {
    type Error = OwnershipError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RecordOwnership::Device),
            1 => Ok(RecordOwnership::Server),
            _ => Err(OwnershipError { value }),
        }
    }
}

impl From<RecordOwnership> for Ownership {
    fn from(value: RecordOwnership) -> Self {
        match value {
            RecordOwnership::Device => Ownership::Device,
            RecordOwnership::Server => Ownership::Server,
        }
    }
}

impl From<Ownership> for RecordOwnership {
    fn from(value: Ownership) -> Self {
        match value {
            Ownership::Device => RecordOwnership::Device,
            Ownership::Server => RecordOwnership::Server,
        }
    }
}

/// Error when de/serializing a value stored in the [`SqliteStore`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ValueError {
    /// Couldn't convert to AstarteType.
    #[error("couldn't convert to AstarteType")]
    Conversion(#[from] TypeError),
    /// Couldn't decode the BSON buffer.
    #[error("couldn't decode property from bson")]
    Decode(#[from] PayloadError),
    /// Unsupported [`AstarteType`].
    #[error("unsupported property type {0}")]
    UnsupportedType(&'static str),
    /// Unsupported [`AstarteType`].
    #[error("unsupported stored type {0}, expected [0-13]")]
    StoredType(u8),
}

/// Result of the load_prop query
#[derive(Clone)]
struct PropRecord {
    value: Vec<u8>,
    stored_type: u8,
    interface_major: i32,
}

impl Debug for PropRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use itertools::Itertools;

        // Print the value as an hex string instead of an array of numbers
        let hex_value = self
            .value
            .iter()
            .format_with("", |element, f| f(&format_args!("{element:x}")));

        f.debug_struct("PropRecord")
            .field("interface_major", &self.interface_major)
            .field("value", &format_args!("{}", hex_value))
            .finish()
    }
}

impl TryFrom<PropRecord> for AstarteType {
    type Error = ValueError;

    fn try_from(record: PropRecord) -> Result<Self, Self::Error> {
        deserialize_prop(record.stored_type, &record.value)
    }
}

/// Result of the load_all_props query
#[derive(Debug, Clone)]
struct StoredRecord {
    interface: String,
    path: String,
    value: Vec<u8>,
    stored_type: u8,
    interface_major: i32,
    ownership: u8,
}

fn into_stored_type(value: &AstarteType) -> Result<u8, ValueError> {
    let mapping_type = match value {
        AstarteType::Unset => 0,
        AstarteType::Double(_) => 1,
        AstarteType::Integer(_) => 2,
        AstarteType::Boolean(_) => 3,
        AstarteType::LongInteger(_) => 4,
        AstarteType::String(_) => 5,
        AstarteType::BinaryBlob(_) => 6,
        AstarteType::DateTime(_) => 7,
        AstarteType::DoubleArray(_) => 8,
        AstarteType::IntegerArray(_) => 9,
        AstarteType::BooleanArray(_) => 10,
        AstarteType::LongIntegerArray(_) => 11,
        AstarteType::StringArray(_) => 12,
        AstarteType::BinaryBlobArray(_) => 13,
        AstarteType::DateTimeArray(_) => 14,
        #[allow(deprecated)]
        AstarteType::EmptyArray => {
            return Err(ValueError::UnsupportedType(value.display_type()));
        }
    };

    Ok(mapping_type)
}

fn from_stored_type(value: u8) -> Result<Option<MappingType>, ValueError> {
    let mapping_type = match value {
        // Property is unset
        0 => {
            return Ok(None);
        }
        1 => MappingType::Double,
        2 => MappingType::Integer,
        3 => MappingType::Boolean,
        4 => MappingType::LongInteger,
        5 => MappingType::String,
        6 => MappingType::BinaryBlob,
        7 => MappingType::DateTime,
        8 => MappingType::DoubleArray,
        9 => MappingType::IntegerArray,
        10 => MappingType::BooleanArray,
        11 => MappingType::LongIntegerArray,
        12 => MappingType::StringArray,
        13 => MappingType::BinaryBlobArray,
        14 => MappingType::DateTimeArray,
        15.. => {
            return Err(ValueError::StoredType(value));
        }
    };

    Ok(Some(mapping_type))
}

impl TryFrom<StoredRecord> for StoredProp {
    type Error = SqliteError;

    fn try_from(record: StoredRecord) -> Result<Self, Self::Error> {
        let value = deserialize_prop(record.stored_type, &record.value)?;

        Ok(StoredProp {
            interface: record.interface,
            path: record.path,
            value,
            interface_major: record.interface_major,
            ownership: RecordOwnership::try_from(record.ownership)?.into(),
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
    /// Creates a sqlite database for the Astarte device.
    ///
    /// URI should follow sqlite's convention, read [SqliteConnectOptions] for more details.
    ///
    /// ```no_run
    /// use astarte_device_sdk::store::sqlite::SqliteStore;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let database = SqliteStore::new("path/to/database/file.sqlite")
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn new(uri: &str) -> Result<Self, SqliteError> {
        let options = SqliteConnectOptions::from_str(uri)
            .map_err(|err| SqliteError::Uri {
                err,
                uri: uri.to_string(),
            })?
            .create_if_missing(true);

        let conn = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(SqliteError::Connection)?;

        // Run the migrations if needed
        sqlx::migrate!()
            .run(&conn)
            .await
            .map_err(SqliteError::Migration)?;

        Ok(SqliteStore { db_conn: conn })
    }
}

#[async_trait]
impl PropertyStore for SqliteStore {
    type Err = SqliteError;

    async fn store_prop(
        &self,
        StoredProp {
            interface,
            path,
            value,
            interface_major,
            ownership,
        }: StoredProp<&str, &AstarteType>,
    ) -> Result<(), Self::Err> {
        debug!(
            "Storing property {} {} in db ({:?})",
            interface, path, value
        );

        let mapping_type = into_stored_type(value)?;
        let buf = Payload::new(value).to_vec()?;

        let own = RecordOwnership::from(ownership) as u8;

        sqlx::query_file!(
            "queries/store_prop.sql",
            interface,
            path,
            buf,
            mapping_type,
            interface_major,
            own
        )
        .execute(&self.db_conn)
        .await?;

        Ok(())
    }

    async fn load_prop(
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

                    self.delete_prop(interface, path).await?;

                    return Ok(None);
                }

                let ast_ty = record.try_into()?;

                Ok(Some(ast_ty))
            }
            None => Ok(None),
        }
    }

    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), Self::Err> {
        sqlx::query_file!("queries/delete_prop.sql", interface, path)
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        sqlx::query_file!("queries/clear.sql")
            .execute(&self.db_conn)
            .await?;

        Ok(())
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let res: Vec<StoredProp> = sqlx::query_file_as!(StoredRecord, "queries/load_all_props.sql")
            .try_map(|row| StoredProp::try_from(row).map_err(|err| sqlx::Error::Decode(err.into())))
            .fetch_all(&self.db_conn)
            .await?;

        Ok(res)
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let res: Vec<StoredProp> = sqlx::query_file_as!(StoredRecord, "queries/device_props.sql")
            .try_map(|row| StoredProp::try_from(row).map_err(|err| sqlx::Error::Decode(err.into())))
            .fetch_all(&self.db_conn)
            .await?;

        Ok(res)
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let res: Vec<StoredProp> = sqlx::query_file_as!(StoredRecord, "queries/server_props.sql")
            .try_map(|row| StoredProp::try_from(row).map_err(|err| sqlx::Error::Decode(err.into())))
            .fetch_all(&self.db_conn)
            .await?;

        Ok(res)
    }

    async fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, Self::Err> {
        let res: Vec<StoredProp> =
            sqlx::query_file_as!(StoredRecord, "queries/interface_props.sql", interface)
                .try_map(|row| {
                    StoredProp::try_from(row).map_err(|err| sqlx::Error::Decode(err.into()))
                })
                .fetch_all(&self.db_conn)
                .await?;

        Ok(res)
    }
}

/// Deserialize a property from the store.
fn deserialize_prop(stored_type: u8, buf: &[u8]) -> Result<AstarteType, ValueError> {
    let Some(mapping_type) = from_stored_type(stored_type)? else {
        return Ok(AstarteType::Unset);
    };

    let payload = Payload::from_slice(buf)?;
    let value = BsonConverter::new(mapping_type, payload.value);

    value.try_into().map_err(ValueError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::test_property_store;

    #[tokio::test]
    async fn test_sqlite_store() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let path = db_path.as_path().to_str().unwrap();

        let db = SqliteStore::new(path).await.unwrap();

        test_property_store(db).await;
    }
}
