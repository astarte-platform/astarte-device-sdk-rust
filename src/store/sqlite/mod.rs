// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
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

use std::fmt::Debug;
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use astarte_interfaces::schema::{MappingType, Ownership};
use astarte_interfaces::{Properties, Schema};
use rusqlite::ToSql;
use rusqlite::types::{FromSql, FromSqlError};
use serde::{Deserialize, Serialize};
use statements::include_query;
use tracing::{debug, error, info, instrument, trace};

use self::connection::SqliteConnection;
use self::pool::Connections;
use super::{OptStoredProp, PropertyMapping, PropertyStore, StoreCapabilities, StoredProp};
use crate::store::sqlite::options::SqliteOptions;
use crate::{
    store::PropertyState,
    transport::mqtt::payload::{Payload, PayloadError},
    types::{AstarteData, TypeError, de::BsonConverter},
};

pub(crate) mod connection;
pub(crate) mod options;
pub(crate) mod pool;
pub(crate) mod statements;

/// Milliseconds for the busy timeout
///
/// <https://www.sqlite.org/c3ref/busy_timeout.html>
pub const SQLITE_BUSY_TIMEOUT: u16 = Duration::from_secs(5).as_millis() as u16;

/// Cache size in kibibytes
///
/// <https://www.sqlite.org/pragma.html#pragma_cache_size>
pub const SQLITE_CACHE_SIZE: i16 =
    -(Size::MiB(NonZero::<u64>::new(2).unwrap()).to_kibibytes_ceil() as i16);
/// Max journal size
///
/// The default value specidfied in <https://www.sqlite.org/pragma.html#pragma_journal_size_limit> is -1
/// which does not set an effective limit, therefore we assume a default size of 64 mebibytes
pub const SQLITE_JOURNAL_SIZE_LIMIT: Size = Size::MiB(NonZero::<u64>::new(64).unwrap());

/// Default database size
pub const SQLITE_DEFAULT_DB_MAX_SIZE: Size = Size::GiB(NonZero::<u64>::new(1).unwrap());

/// SQLite maximum number of pages in the database.
///
/// <https://www.sqlite.org/limits.html>
pub const SQLITE_MAX_PAGE_COUNT: u32 = 4294967294;

/// SQLite auto checkpoint limit, a checkpoint will run whenever the log
/// is equal or above this size.
///
/// <https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint>
pub const SQLITE_WAL_AUTOCHECKPOINT: u32 = 1000;

/// Maximum number of reader connections to create.
///
/// This is the default if we cannot access the available_parallelism
pub const DEFAULT_MAX_READERS: NonZero<usize> = NonZero::<usize>::new(4).unwrap();

/// Error returned by the [`SqliteStore`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    /// Error returned when the database connection fails.
    #[error("could not connect to database")]
    Connection(#[source] rusqlite::Error),
    /// Couldn't set SQLite option.
    #[error("couldn't set database option")]
    Option(#[source] rusqlite::Error),
    /// Couldn't prepare the SQLite statement.
    #[error("couldn't prepare sqlite statement")]
    Prepare(#[source] rusqlite::Error),
    /// Couldn't start a transaction.
    #[error("could not start a transaction database")]
    Transaction(#[source] rusqlite::Error),
    /// Couldn't run migration
    #[error("couldn't run migration")]
    Migration(#[source] rusqlite::Error),
    /// Error returned when the database query fails.
    #[error("could not execute query")]
    Query(#[from] rusqlite::Error),
    /// Couldn't convert the stored value.
    #[error("couldn't convert the stored value")]
    Value(#[from] ValueError),
    /// Couldn't convert ownership value
    #[error("could not deserialize ownership")]
    Ownership(#[from] OwnershipError),
    /// Couldn't set max size
    #[error("couldn't set max size {ctx}")]
    InvalidMaxSize {
        /// Context of the error
        ctx: &'static str,
    },
    /// Couldn't acquire a reader permit
    #[error("couldn't acquire a reader permit")]
    Reader,
    /// Couldn't join the connection task
    #[error("couldn't join the connection task")]
    Join,
    /// Couldn't convert passed input
    #[error("couldn't convert passed input")]
    Conversion(usize),
}

/// Error when converting a u8 into the [`Ownership`] struct.
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

impl ToSql for RecordOwnership {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok((*self as u8).into())
    }
}

impl FromSql for RecordOwnership {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = u8::column_result(value)?;

        match value {
            0 => Ok(RecordOwnership::Device),
            1 => Ok(RecordOwnership::Server),
            _ => Err(FromSqlError::Other(OwnershipError { value }.into())),
        }
    }
}

/// Error when de/serializing a value stored in the [`SqliteStore`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ValueError {
    /// Couldn't convert to AstarteData.
    #[error("couldn't convert to AstarteData")]
    Conversion(#[from] TypeError),
    /// Couldn't decode the BSON buffer.
    #[error("couldn't decode property from bson")]
    Decode(#[source] PayloadError),
    /// Couldn't encode the BSON buffer.
    #[error("couldn't encode property from bson")]
    Encode(#[source] PayloadError),
    /// Unsupported [`AstarteData`].
    #[error("unsupported property type {0}")]
    UnsupportedType(&'static str),
    /// Unsupported [`AstarteData`].
    #[error("unsupported stored type {0}, expected [0-13]")]
    StoredType(u8),
}

/// Dimension of the database
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(tag = "unit", content = "value")]
pub enum Size {
    /// Dimension expressed in KiloBytes
    #[serde(rename = "kb")]
    Kb(NonZero<u64>),
    /// Dimension expressed in MegaBytes
    #[serde(rename = "mb")]
    Mb(NonZero<u64>),
    /// Dimension expressed in GigaBytes
    #[serde(rename = "gb")]
    Gb(NonZero<u64>),
    /// Dimension expressed in KibiBytes
    #[serde(rename = "kib")]
    KiB(NonZero<u64>),
    /// Dimension expressed in MebiBytes
    #[serde(rename = "mib")]
    MiB(NonZero<u64>),
    /// Dimension expressed in GibiBytes
    #[serde(rename = "gib")]
    GiB(NonZero<u64>),
}

impl Size {
    const ONE: NonZero<u32> = NonZero::<u32>::new(1).unwrap();

    const KB: NonZero<u64> = NonZero::<u64>::new(1000).unwrap();
    const MB: NonZero<u64> = NonZero::<u64>::new(1000 * 1000).unwrap();
    const GB: NonZero<u64> = NonZero::<u64>::new(1000 * 1000 * 1000).unwrap();
    const KI_B: NonZero<u64> = NonZero::<u64>::new(1024).unwrap();
    const MI_B: NonZero<u64> = NonZero::<u64>::new(1024 * 1024).unwrap();
    const GI_B: NonZero<u64> = NonZero::<u64>::new(1024 * 1024 * 1024).unwrap();

    /// Convert the size to bytes
    const fn to_bytes(self) -> NonZero<u64> {
        match self {
            Size::Kb(kb) => kb.saturating_mul(Self::KB),
            Size::Mb(mb) => mb.saturating_mul(Self::MB),
            Size::Gb(gb) => gb.saturating_mul(Self::GB),
            Size::KiB(kib) => kib.saturating_mul(Self::KI_B),
            Size::MiB(mib) => mib.saturating_mul(Self::MI_B),
            Size::GiB(gib) => gib.saturating_mul(Self::GI_B),
        }
    }

    const fn to_kibibytes_ceil(self) -> u64 {
        self.to_bytes().get().div_ceil(1024)
    }

    /// Approximate the max page count with the page size, with a minimum of 1 page
    #[instrument]
    fn into_page_count(self, page_size: NonZero<u64>) -> NonZero<u32> {
        let value = u32::try_from(self.to_bytes().get().div_euclid(page_size.get()))
            // default value
            .unwrap_or(SQLITE_MAX_PAGE_COUNT);

        trace!(pages = value, "calculated pages");

        // we must have at least one page
        NonZero::<u32>::new(value).unwrap_or(Self::ONE)
    }

    /// Calculate the into_wall_autocheckpoint page count to be at 1/10 of the journal_size_limit
    ///  if it's less than 1000 pages.
    #[instrument]
    fn into_wall_autocheckpoint(self, page_size: NonZero<u64>) -> NonZero<u32> {
        let journal_pages = self.into_page_count(page_size);

        let pages = journal_pages
            .get()
            .div_euclid(10)
            // upper bound
            .min(SQLITE_WAL_AUTOCHECKPOINT);

        trace!(pages, "calculated pages");

        // we must have at least one page
        NonZero::<u32>::new(pages).unwrap_or(Self::ONE)
    }
}

/// Result of the load_prop query
#[derive(Clone)]
struct PropRecord {
    value: Option<Vec<u8>>,
    stored_type: u8,
    interface_major: i32,
}

impl PropRecord {
    fn try_into_value(self) -> Result<Option<AstarteData>, ValueError> {
        self.value
            .map(|value| deserialize_prop(self.stored_type, &value))
            .transpose()
    }
}

impl Debug for PropRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use itertools::Itertools;

        // Print the value as an hex string instead of an array of numbers

        let mut d = f.debug_struct("PropRecord");

        d.field("interface_major", &self.interface_major);

        match &self.value {
            Some(value) => {
                let hex_value = value
                    .iter()
                    .format_with("", |element, f| f(&format_args!("Some({element:x})")));

                d.field("value", &format_args!("{hex_value}"))
            }
            None => d.field("value", &self.value),
        }
        .finish()
    }
}

/// Error when converting a u8 into the [`PropertyState`] struct.
#[derive(Debug, thiserror::Error)]
#[error("invalid property state value {value}")]
pub struct PropertyStateError {
    value: u8,
}

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
enum RecordPropertyState {
    Changed = 0,
    Completed = 1,
}

impl From<PropertyState> for RecordPropertyState {
    fn from(value: PropertyState) -> Self {
        match value {
            PropertyState::Changed => Self::Changed,
            PropertyState::Completed => Self::Completed,
        }
    }
}

impl From<RecordPropertyState> for PropertyState {
    fn from(value: RecordPropertyState) -> Self {
        match value {
            RecordPropertyState::Changed => Self::Changed,
            RecordPropertyState::Completed => Self::Completed,
        }
    }
}

impl ToSql for RecordPropertyState {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok((*self as u8).into())
    }
}

impl FromSql for RecordPropertyState {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = u8::column_result(value)?;

        match value {
            0 => Ok(RecordPropertyState::Changed),
            1 => Ok(RecordPropertyState::Completed),
            _ => Err(FromSqlError::Other(PropertyStateError { value }.into())),
        }
    }
}

/// Result of the load_all_props query
#[derive(Debug, Clone)]
struct StoredRecord {
    interface: String,
    path: String,
    value: Option<Vec<u8>>,
    stored_type: u8,
    interface_major: i32,
    ownership: RecordOwnership,
}

impl StoredRecord {
    pub(crate) fn try_into_prop(self) -> Result<Option<StoredProp>, SqliteError> {
        let Some(value) = self.value else {
            return Ok(None);
        };

        let value = deserialize_prop(self.stored_type, &value)?;

        Ok(Some(StoredProp {
            interface: self.interface,
            path: self.path,
            value,
            interface_major: self.interface_major,
            ownership: self.ownership.into(),
        }))
    }
}

impl TryFrom<StoredRecord> for OptStoredProp {
    type Error = SqliteError;

    fn try_from(record: StoredRecord) -> Result<Self, Self::Error> {
        let value = record
            .value
            .map(|value| deserialize_prop(record.stored_type, &value))
            .transpose()?;

        Ok(Self {
            interface: record.interface,
            path: record.path,
            value,
            interface_major: record.interface_major,
            ownership: record.ownership.into(),
        })
    }
}

fn into_stored_type(value: &AstarteData) -> Result<u8, ValueError> {
    let mapping_type = match value {
        AstarteData::Double(_) => 1,
        AstarteData::Integer(_) => 2,
        AstarteData::Boolean(_) => 3,
        AstarteData::LongInteger(_) => 4,
        AstarteData::String(_) => 5,
        AstarteData::BinaryBlob(_) => 6,
        AstarteData::DateTime(_) => 7,
        AstarteData::DoubleArray(_) => 8,
        AstarteData::IntegerArray(_) => 9,
        AstarteData::BooleanArray(_) => 10,
        AstarteData::LongIntegerArray(_) => 11,
        AstarteData::StringArray(_) => 12,
        AstarteData::BinaryBlobArray(_) => 13,
        AstarteData::DateTimeArray(_) => 14,
    };

    Ok(mapping_type)
}

fn from_stored_type(value: u8) -> Result<MappingType, ValueError> {
    let mapping_type = match value {
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
        0 | 15.. => {
            return Err(ValueError::StoredType(value));
        }
    };

    Ok(mapping_type)
}

/// Data structure providing an implementation of a sqlite database.
///
/// Can be used by an Astarte device to store permanently properties values and published with
/// retention stored.
///
/// The properties are stored as a BSON serialized SQLite BLOB. That can be then deserialized in the
/// respective [`AstarteData`].
///
/// The retention is stored as a BLOB serialized by the connection.
#[derive(Clone, Debug)]
pub struct SqliteStore {
    pub(crate) pool: Arc<Connections>,
}

impl SqliteStore {
    /// Configures the SQLite connection
    pub fn options() -> SqliteOptions {
        SqliteOptions::default()
    }

    /// Creates a SQLite database for the Astarte device.
    async fn new(db_file: PathBuf, options: SqliteOptions) -> Result<Self, SqliteError> {
        let sqlite_store = SqliteStore {
            pool: Arc::new(Connections::new(db_file, options)),
        };

        sqlite_store.migrate().await?;

        debug!("vacuum the database");

        sqlite_store
            .pool
            .acquire_writer(|writer| {
                writer
                    .execute("PRAGMA incremental_vacuum", ())
                    .map_err(SqliteError::Option)
            })
            .await?;

        Ok(sqlite_store)
    }

    /// Connect to the SQLite database using the default db name in the writable path.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use astarte_device_sdk::store::sqlite::{SqliteStore, SqliteOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let store = SqliteStore::with_writable_dir("/val/lib/astarte/", SqliteOptions::default())
    ///         .await
    ///         .expect("should connect");
    /// }
    /// ```
    pub async fn with_writable_dir(
        writable_path: impl AsRef<Path>,
        options: SqliteOptions,
    ) -> Result<Self, SqliteError> {
        let path = writable_path.as_ref();

        if let Err(error) = tokio::fs::create_dir_all(path).await {
            error!(%error,path = %path.display(), "couldn't create writable path for database");
        }

        // TODO: rename this since it doesn't store only properties
        let db = path.join("prop-cache.db");

        Self::new(db, options).await
    }

    /// Connect to the SQLite database give as a filename.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use astarte_device_sdk::store::sqlite::{SqliteStore, SqliteOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let store = SqliteStore::with_db_file("/val/lib/astarte/store.db", SqliteOptions::default()).await.unwrap();
    /// }
    /// ```
    pub async fn with_db_file(
        database_file: impl AsRef<Path>,
        options: SqliteOptions,
    ) -> Result<Self, SqliteError> {
        Self::new(database_file.as_ref().to_path_buf(), options).await
    }

    #[instrument(skip(self))]
    async fn migrate(&self) -> Result<(), SqliteError> {
        // Order is important
        const MIGRATIONS: &[&str] = &[
            include_query!("migrations/0001_init.sql"),
            include_query!("migrations/0002_unset_property.sql"),
            include_query!("migrations/0003_session.sql"),
            include_query!("migrations/0004_sent_properties.sql"),
        ];
        const USER_VERSION: u32 = {
            assert!(MIGRATIONS.len() < (u32::MAX as usize));

            MIGRATIONS.len() as u32
        };

        self.pool
            .acquire_writer(|writer| -> Result<(), SqliteError> {
                // re-run migrations on error
                let version: usize = writer
                    .get_pragma::<u32>("user_version")
                    .ok()
                    .and_then(|value| usize::try_from(value).ok())
                    .unwrap_or(0);

                debug!(
                    current = version,
                    migrations = MIGRATIONS.len(),
                    "checking migrations"
                );

                if version >= MIGRATIONS.len() {
                    info!("no migration to run");

                    return Ok(());
                }

                for migration in &MIGRATIONS[version..] {
                    writer
                        .execute_batch(migration)
                        .map_err(SqliteError::Migration)?;
                }

                debug!(version = MIGRATIONS.len(), "setting new database version");

                writer.set_pragma("user_version", &USER_VERSION)?;

                info!("store migrated to new version");

                Ok(())
            })
            .await?;

        Ok(())
    }
}

impl StoreCapabilities for SqliteStore {
    type Retention = Self;
    type Session = Self;

    fn get_retention(&self) -> Option<&Self::Retention> {
        Some(self)
    }

    fn get_session(&self) -> Option<&Self::Session> {
        Some(self)
    }
}

impl PropertyStore for SqliteStore {
    type Err = SqliteError;

    async fn store_prop(&self, prop: StoredProp<&str, &AstarteData>) -> Result<(), Self::Err> {
        trace!(
            interface = prop.interface,
            path = prop.path,
            "storing property",
        );

        let buf = Payload::new(prop.value)
            .to_vec()
            .map_err(ValueError::Encode)?;

        let prop = StoredProp::<String, AstarteData>::from(prop);
        self.pool
            .acquire_writer(move |writer| writer.store_prop((&prop).into(), &buf))
            .await?;

        Ok(())
    }

    async fn update_state(
        &self,
        property: &PropertyMapping<'_>,
        state: PropertyState,
        expected: Option<AstarteData>,
    ) -> Result<bool, Self::Err> {
        let interface_name = property.interface_name().to_string();
        let path = property.path().to_string();

        let updated = self
            .pool
            .acquire_writer(move |writer| {
                writer.update_state(&interface_name, &path, expected.as_ref(), state)
            })
            .await?;

        Ok(updated > 0)
    }

    async fn load_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> Result<Option<AstarteData>, Self::Err> {
        let interface_name = property.interface_name().to_string();
        let path = property.path().to_string();

        let opt_record = self
            .pool
            .acquire_reader(move |reader| reader.load_prop(&interface_name, &path))
            .await?;

        match opt_record {
            Some(record) => {
                trace!(
                    interface = property.interface_name(),
                    path = property.path(),
                    "loaded property",
                );

                // if version mismatch, delete
                if record.interface_major != property.version_major() {
                    error!(
                        "Version mismatch for property {}{} (stored {}, interface {}). Deleting.",
                        property.interface_name(),
                        property.path(),
                        record.interface_major,
                        property.version_major()
                    );

                    self.delete_prop(property).await?;

                    return Ok(None);
                }

                record.try_into_value().map_err(SqliteError::Value)
            }
            None => Ok(None),
        }
    }

    async fn unset_prop(&self, property: &PropertyMapping<'_>) -> Result<(), Self::Err> {
        let interface_name = property.interface_name().to_string();
        let path = property.path().to_string();
        self.pool
            .acquire_writer(move |writer| writer.unset_prop(&interface_name, &path))
            .await
    }

    async fn delete_prop(&self, property: &PropertyMapping<'_>) -> Result<(), Self::Err> {
        let interface_name = property.interface_name().to_string();
        let path = property.path().to_string();
        self.pool
            .acquire_writer(move |writer| writer.delete_prop(&interface_name, &path))
            .await
    }

    async fn delete_expected_prop(
        &self,
        property: &PropertyMapping<'_>,
        expected: Option<AstarteData>,
    ) -> Result<bool, Self::Err> {
        let interface_name = property.interface_name().to_string();
        let path = property.path().to_string();

        let updated = self
            .pool
            .acquire_writer(move |writer| {
                writer.delete_expected_prop(&interface_name, &path, expected.as_ref())
            })
            .await?;

        Ok(updated > 0)
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        self.pool
            .acquire_writer(|writer| writer.clear_props())
            .await
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.pool
            .acquire_reader(|reader| reader.load_all_props())
            .await
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.pool
            .acquire_reader(|reader| reader.props_with_ownership(Ownership::Device))
            .await
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.pool
            .acquire_reader(|reader| reader.props_with_ownership(Ownership::Server))
            .await
    }

    async fn interface_props(&self, interface: &Properties) -> Result<Vec<StoredProp>, Self::Err> {
        let interface_name = interface.name().to_string();

        self.pool
            .acquire_reader(move |reader| reader.interface_props(&interface_name))
            .await
    }

    async fn delete_interface(&self, interface: &Properties) -> Result<(), Self::Err> {
        let interface_name = interface.name().to_string();

        self.pool
            .acquire_writer(move |writer| writer.delete_interface_props(&interface_name))
            .await
    }

    async fn device_props_with_unset(
        &self,
        state: PropertyState,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<OptStoredProp>, Self::Err> {
        self.pool
            .acquire_reader(move |reader| {
                reader.props_with_unset(Ownership::Device, state, limit, offset)
            })
            .await
    }
}

/// Deserialize a property from the store.
fn deserialize_prop(stored_type: u8, buf: &[u8]) -> Result<AstarteData, ValueError> {
    let mapping_type = from_stored_type(stored_type)?;

    let payload = Payload::from_slice(buf).map_err(ValueError::Decode)?;
    let value = BsonConverter::new(mapping_type, payload.value);

    value.try_into().map_err(ValueError::from)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::store::tests::test_property_store;

    #[tokio::test]
    async fn test_sqlite_store() {
        let dir = tempfile::tempdir().unwrap();

        let db = SqliteStore::options()
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        test_property_store(db).await;
    }

    #[tokio::test]
    async fn multiple_db_per_thread() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();

        let db1 = SqliteStore::options()
            .with_writable_dir(dir1.path())
            .await
            .unwrap();

        let test = |store: SqliteStore| async move {
            let value = AstarteData::Integer(42);
            let prop = StoredProp {
                interface: "com.test",
                path: "/test",
                value: &value,
                interface_major: 1,
                ownership: Ownership::Device,
            };
            let prop_interface_data = PropertyMapping::from(&prop);

            store.store_prop(prop).await.unwrap();
            assert_eq!(
                store
                    .load_prop(&prop_interface_data)
                    .await
                    .unwrap()
                    .unwrap(),
                value
            );
        };

        (test)(db1).await;

        let db2 = SqliteStore::options()
            .with_writable_dir(dir2.path())
            .await
            .unwrap();

        (test)(db2).await;
    }

    #[tokio::test]
    async fn set_max_pages_cannot_shrink() {
        let dir = tempfile::tempdir().unwrap();

        {
            let db = SqliteStore::options()
                .with_writable_dir(dir.path())
                .await
                .unwrap();

            let page_size: i64 = db
                .pool
                .acquire_writer(|writer| writer.get_pragma("page_size"))
                .await
                .unwrap();

            db.store_prop(StoredProp {
                interface: "interface",
                path: "/path",
                value: &AstarteData::BinaryBlob(vec![1; usize::try_from(page_size).unwrap() * 3]),
                interface_major: 0,
                ownership: Ownership::Device,
            })
            .await
            .unwrap();
        }

        let err = SqliteStore::options()
            .set_max_page_count(NonZero::new(1).unwrap())
            .with_writable_dir(dir.path())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            SqliteError::InvalidMaxSize {
                ctx,
            } if ctx == "cannot shrink the database"
        ));
    }

    #[tokio::test]
    async fn store_cannot_exceed_max_pages() {
        let dir = tempfile::tempdir().unwrap();

        let (page_size, page_count) = {
            let db = SqliteStore::options()
                .with_writable_dir(dir.path())
                .await
                .unwrap();

            db.pool
                .acquire_writer(|writer| -> Result<_, SqliteError> {
                    let size = writer.get_pragma::<i64>("page_size")?;
                    let count = writer.get_pragma::<i64>("page_count")?;
                    Ok((size, count))
                })
                .await
                .unwrap()
        };

        let max_page_count = u32::try_from(page_count)
            .ok()
            .and_then(NonZero::new)
            .unwrap();

        let db = SqliteStore::options()
            .set_max_page_count(max_page_count)
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let size = usize::try_from(page_size * page_count + 1).unwrap();

        let err = db
            .store_prop(StoredProp {
                interface: "interface",
                path: "/path",
                value: &AstarteData::BinaryBlob(vec![1; size]),
                interface_major: 0,
                ownership: Ownership::Device,
            })
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            SqliteError::Query(err) if err.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull)
        ));
    }

    #[tokio::test]
    async fn set_max_pages() {
        let exp_count = 15;
        let max = NonZero::new(exp_count).unwrap();

        let dir = tempfile::tempdir().unwrap();

        let db = SqliteStore::options()
            .set_max_page_count(max)
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let page_count: u32 = db
            .pool
            .acquire_writer(|writer| writer.get_pragma("max_page_count"))
            .await
            .unwrap();

        assert_eq!(page_count, exp_count);
    }

    #[tokio::test]
    async fn set_db_max_size() {
        let dir = tempfile::tempdir().unwrap();

        let db = SqliteStore::options()
            .set_db_max_size(Size::MiB(NonZero::new(4).unwrap()))
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let max_page_count: u32 = db
            .pool
            .acquire_writer(|writer| writer.get_pragma("max_page_count"))
            .await
            .unwrap();

        assert_eq!(max_page_count, 1024);
    }

    #[tokio::test]
    async fn set_db_max_size_min() {
        let dir = tempfile::tempdir().unwrap();

        // set the max size considering the default page size of 4096 bytes
        // NOTE since the limit is set after the database is created we can't shrink an
        // already created database this means that setting a 1KB limit is currently not supported
        // even a 1 page limit (4096B) would not work
        let size = Size::Kb(NonZero::<u64>::new(1).unwrap());

        SqliteStore::options()
            .set_db_max_size(size)
            .with_writable_dir(dir.path())
            .await
            .unwrap_err();
    }

    #[test]
    fn size_to_kibibytes_ceil_min() {
        let size = Size::Kb(NonZero::<u64>::new(1).unwrap());
        assert_eq!(size.to_kibibytes_ceil(), 1);
    }

    #[tokio::test]
    async fn set_journal_size_limit() {
        let dir = tempfile::tempdir().unwrap();

        let size = Size::MiB(NonZero::<u64>::new(1).unwrap());

        let db = SqliteStore::options()
            .set_journal_size_limit(size)
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let journal_size: i64 = db
            .pool
            .acquire_writer(|writer| writer.get_pragma("journal_size_limit"))
            .await
            .unwrap();
        assert_eq!(journal_size, 1024 * 1024);

        let wal_autocheckpoint: u32 = db
            .pool
            .acquire_writer(|writer| writer.get_pragma("wal_autocheckpoint"))
            .await
            .unwrap();

        // autocheckpoin is set to a fraction of the journal_size in pages (pages / 10)
        // in this case
        //
        // 1MiB / 4096 = 256
        // 256 / 10 = 25
        assert_eq!(wal_autocheckpoint, 25);
    }

    #[tokio::test]
    async fn set_journal_size_limit_min() {
        let dir = tempfile::tempdir().unwrap();

        let size = Size::Kb(NonZero::<u64>::new(1).unwrap());

        let db = SqliteStore::options()
            .set_journal_size_limit(size)
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        let journal_size: u32 = db
            .pool
            .acquire_writer(|writer| writer.get_pragma("journal_size_limit"))
            .await
            .unwrap();

        assert_eq!(journal_size, 1000);

        let wal_autocheckpoint: u32 = db
            .pool
            .acquire_writer(|writer| writer.get_pragma("wal_autocheckpoint"))
            .await
            .unwrap();

        // autocheckpoin is set to a fraction of the journal_size in pages
        // in this case the size in pages is 0
        //
        // (1KB / 4096 = 0) but the minimum we allow is 1
        assert_eq!(wal_autocheckpoint, 1);
    }

    #[test]
    fn size_to_bytes() {
        let size = Size::Kb(NonZero::<u64>::new(1).unwrap());
        assert_eq!(size.to_bytes().get(), 1000);

        let size = Size::Mb(NonZero::<u64>::new(1).unwrap());
        assert_eq!(size.to_bytes().get(), 1000 * 1000);

        let size = Size::Gb(NonZero::<u64>::new(1).unwrap());
        assert_eq!(size.to_bytes().get(), 1000 * 1000 * 1000);
    }

    #[test]
    fn size_to_kib() {
        let size = Size::KiB(NonZero::<u64>::new(1).unwrap());
        assert_eq!(size.to_bytes().get(), 1024);

        let size = Size::MiB(NonZero::<u64>::new(1).unwrap());
        assert_eq!(size.to_bytes().get(), 1024 * 1024);

        let size = Size::GiB(NonZero::<u64>::new(1).unwrap());
        assert_eq!(size.to_bytes().get(), 1024 * 1024 * 1024);
    }

    #[test]
    fn should_serialize_deserialize_size() {
        let expected = r#"{"unit":"gb","value":2}"#;

        let size = Size::Gb(2.try_into().unwrap());
        let out = serde_json::to_string(&size).unwrap();

        let deser_size: Size = serde_json::from_str(&out).unwrap();

        assert_eq!(out, expected);
        assert_eq!(size, deser_size);
    }
}
