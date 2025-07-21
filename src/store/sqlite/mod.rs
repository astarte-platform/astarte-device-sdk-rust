// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
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

use std::{cell::Cell, fmt::Debug, num::NonZeroU64, path::Path, sync::Arc, time::Duration};

use astarte_interfaces::{
    schema::{MappingType, Ownership},
    Properties, Schema,
};
use futures::lock::Mutex;
use options::{SizeLimit, SqlitePragmas, SqliteStoreOptions};
use rusqlite::{
    types::{FromSql, FromSqlError},
    Connection, OptionalExtension, ToSql,
};
use serde::{Deserialize, Serialize};
use statements::{include_query, ReadConnection, WriteConnection};
use tracing::{debug, error, trace, warn};

use super::{OptStoredProp, PropertyMapping, PropertyStore, StoreCapabilities, StoredProp};
use crate::{
    transport::mqtt::payload::{Payload, PayloadError},
    types::{de::BsonConverter, AstarteData, TypeError},
};

pub(crate) mod options;
pub(crate) mod statements;

/// Milliseconds for the busy timeout
///
/// <https://www.sqlite.org/c3ref/busy_timeout.html>
pub const SQLITE_BUSY_TIMEOUT: u16 = Duration::from_secs(5).as_millis() as u16;

/// Cache size in kibibytes
///
/// <https://www.sqlite.org/pragma.html#pragma_cache_size>
pub const SQLITE_CACHE_SIZE: i16 = -(Size::MiB(const_non_zero(2)).to_kibibytes_ceil() as i16);

/// Max journal size
///
/// The default value specidfied in <https://www.sqlite.org/pragma.html#pragma_journal_size_limit> is -1
/// which does not set an effective limit, therefore we assume a default size of 64 mebibytes
pub const SQLITE_JOURNAL_SIZE_LIMIT: Size = Size::MiB(const_non_zero(64));

/// Default database size
pub const SQLITE_DEFAULT_DB_MAX_SIZE: Size = Size::GiB(const_non_zero(1));

/// SQLite maximum number of pages in the database.
///
/// <https://www.sqlite.org/limits.html>
pub const SQLITE_MAX_PAGE_COUNT: u32 = 4294967294;

/// SQLite auto checkpoint limit, a checkpoint will run whenever the log
/// is equal or above this size.
///
/// <https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint>
pub const SQLITE_WAL_AUTOCHECKPOINT: u32 = 1000;

/// Error returned by the [`SqliteStore`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    /// Error returned when the database connection fails.
    #[error("could not connect to database")]
    Connection(#[source] rusqlite::Error),
    /// Couldn't set SQLite option.
    #[error("could not connect to database")]
    Option(#[source] rusqlite::Error),
    /// Couldn't prepare the SQLite statement.
    #[error("could not connect to database")]
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
    Kb(NonZeroU64),
    /// Dimension expressed in MegaBytes
    #[serde(rename = "mb")]
    Mb(NonZeroU64),
    /// Dimension expressed in GigaBytes
    #[serde(rename = "gb")]
    Gb(NonZeroU64),
    /// Dimension expressed in KibiBytes
    #[serde(rename = "kib")]
    KiB(NonZeroU64),
    /// Dimension expressed in MebiBytes
    #[serde(rename = "mib")]
    MiB(NonZeroU64),
    /// Dimension expressed in GibiBytes
    #[serde(rename = "gib")]
    GiB(NonZeroU64),
}

impl Size {
    /// Convert the size to bytes
    const fn to_bytes(self) -> u64 {
        match self {
            Size::Kb(kb) => kb.get().saturating_mul(1000),
            Size::Mb(mb) => mb.get().saturating_mul(1000 * 1000),
            Size::Gb(gb) => gb.get().saturating_mul(1000 * 1000 * 1000),
            Size::KiB(kib) => kib.get().saturating_mul(1024),
            Size::MiB(mib) => mib.get().saturating_mul(1024 * 1024),
            Size::GiB(gib) => gib.get().saturating_mul(1024 * 1024 * 1024),
        }
    }

    const fn to_kibibytes_ceil(self) -> u64 {
        self.to_bytes().div_ceil(1024)
    }

    fn calculate_max_page_count(&self, page_size: u64) -> u32 {
        self.to_bytes()
            .div_euclid(page_size)
            .try_into()
            .inspect_err(|_| warn!("max page count exceeded u32::MAX"))
            .unwrap_or(SQLITE_MAX_PAGE_COUNT)
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

thread_local! {
    /// Read only connection to the SQLite database.
    ///
    /// Since SQLite supports multiple readers concurrently to an exclusive writer, guarantied that
    /// a connection is used by a single thread at a time. We create a thread local read only
    /// connection and share the read handle behind a [`Mutex`].
    ///
    /// This is a [`Vec`] of connection to allow multiple connections with different paths.
    static READER: Cell<Vec<ReadConnection>> = const { Cell::new(Vec::new()) };
}

/// Return db pages information
///
/// Useful to retrieve data assocuiated to PRAGMA page_size, page_count, max_page_count
pub(crate) fn get_pragma<T>(connection: &Connection, pragma_name: &str) -> Result<T, SqliteError>
where
    T: FromSql,
{
    wrap_sync_call(|| connection.pragma_query_value(None, pragma_name, |row| row.get::<_, T>(0)))
        .map_err(SqliteError::Query)
}

pub(crate) fn set_pragma<V>(
    connection: &Connection,
    pragma_name: &str,
    pragma_value: V,
) -> Result<(), SqliteError>
where
    V: ToSql,
{
    wrap_sync_call(|| connection.pragma_update(None, pragma_name, pragma_value))
        .map_err(SqliteError::Option)
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
    pub(crate) db_file: Arc<Path>,
    pub(crate) writer: Arc<Mutex<WriteConnection>>,
    pub(crate) options: SqliteStoreOptions,
}

impl SqliteStore {
    /// Creates a sqlite database for the Astarte device.
    async fn new(
        db_path: impl AsRef<Path>,
        connection: WriteConnection,
        options: SqliteStoreOptions,
    ) -> Result<Self, SqliteError> {
        let sqlite_store = SqliteStore {
            db_file: db_path.as_ref().into(),
            writer: Arc::new(Mutex::new(connection)),
            options,
        };

        sqlite_store.migrate().await?;

        Ok(sqlite_store)
    }

    /// Connect to the SQLite database using the default db name in the writable path.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use astarte_device_sdk::store::sqlite::SqliteStore;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let store = SqliteStore::connect("/val/lib/astarte/").await.unwrap();
    /// }
    /// ```
    // FIXME in a later version we could accept SqliteStoreOptions here instead on relying on setters
    // this is to avoid setting the default pragmas before the user overrides them by using
    // the setters
    pub async fn connect(writable_path: impl AsRef<Path>) -> Result<Self, SqliteError> {
        // TODO: rename the database to store.db since it doesn't contain only  properties
        let db = writable_path.as_ref().join("prop-cache.db");

        Self::connect_db(db).await
    }

    /// Connect to the SQLite database give as a filename.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use astarte_device_sdk::store::sqlite::SqliteStore;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let store = SqliteStore::connect_db("/val/lib/astarte/store.db").await.unwrap();
    /// }
    /// ```
    pub async fn connect_db(database_file: impl AsRef<Path>) -> Result<Self, SqliteError> {
        let options = SqliteStoreOptions::default();
        let connection = WriteConnection::connect(&database_file, &options).await?;
        Self::new(database_file, connection, options).await
    }

    /// Pass the thread local reference to the read only connection.
    pub(crate) fn with_reader<F, O>(&self, f: F) -> Result<O, SqliteError>
    where
        F: FnOnce(&ReadConnection) -> Result<O, SqliteError>,
    {
        wrap_sync_call(|| {
            READER.with(|tlv| {
                let mut v = tlv.take();

                let res = self.get_or_init_reader(&mut v).and_then(f);

                tlv.set(v);

                res
            })
        })
    }

    fn get_or_init_reader<'a: 'b, 'b>(
        &self,
        v: &'a mut Vec<ReadConnection>,
    ) -> Result<&'b ReadConnection, SqliteError> {
        // get the index instead of the element to solve NLL error
        let idx = v.iter().enumerate().find_map(|(i, r)| {
            r.path()
                .is_some_and(|p| p == self.db_file.to_string_lossy())
                .then_some(i)
        });

        if let Some(idx) = idx {
            return Ok(&v[idx]);
        }

        let new_connection = ReadConnection::connect(&self.db_file, &self.options)?;

        let idx = v.len();
        v.push(new_connection);

        Ok(&v[idx])
    }

    async fn migrate(&self) -> Result<(), SqliteError> {
        const MIGRATIONS: &[&str] = &[
            include_query!("migrations/0001_init.sql"),
            include_query!("migrations/0002_unset_property.sql"),
            include_query!("migrations/0003_session.sql"),
        ];

        let writer = self.writer.lock().await;

        wrap_sync_call(|| -> Result<(), SqliteError> {
            let version = writer
                .query_row("PRAGMA user_version;", [], |row| row.get(0))
                .optional()
                .map_err(SqliteError::Query)?
                .unwrap_or(0usize);

            debug!(
                current = version,
                migrations = MIGRATIONS.len(),
                "checking migrations"
            );

            if version >= MIGRATIONS.len() {
                trace!("no migration to run");

                return Ok(());
            }

            for migration in &MIGRATIONS[version..] {
                writer
                    .execute_batch(migration)
                    .map_err(SqliteError::Migration)?;
            }

            debug!(version = MIGRATIONS.len(), "setting new database version");

            writer
                .pragma_update(None, "user_version", MIGRATIONS.len())
                .map_err(SqliteError::Option)?;

            Ok(())
        })?;

        Ok(())
    }

    /// Set the maximum number of pages
    ///
    /// The new database size cannot be lower than the actual one.
    // FIXME this method should be removed and this option should be configurable only during object construction
    pub async fn set_max_pages(&mut self, max: u32) -> Result<(), SqliteError> {
        let writer = self.writer.lock().await;

        let mut modified_options = self.options.clone();
        modified_options.db_size_limit = SizeLimit::Pages(max);
        writer.apply_pragmas(&modified_options)?;
        self.options = modified_options;

        Ok(())
    }

    /// Set the maximum number of pages based on the actual maximum size of the db file
    // FIXME this method should be removed and this option should be configurable only during object construction
    pub async fn set_db_max_size(&mut self, size: Size) -> Result<(), SqliteError> {
        let writer = self.writer.lock().await;

        let mut modified_options = self.options.clone();
        modified_options.db_size_limit = SizeLimit::Size(size);
        writer.apply_pragmas(&modified_options)?;
        self.options = modified_options;

        Ok(())
    }

    /// Set journal size limit for the current database connection.
    /// This will allow to set the limit a value as low as 1KB however
    /// the wal_autocheckpoint will be set to 1 page (4096 bytes) even if
    /// this is larger than the journal size.
    // FIXME this method should be removed and this option should be configurable only during object construction
    pub async fn set_journal_size_limit(&mut self, size: Size) -> Result<(), SqliteError> {
        let writer = self.writer.lock().await;

        let mut modified_options = self.options.clone();
        modified_options.journal_size_limit = size;
        writer.apply_pragmas(&modified_options)?;
        self.options = modified_options;

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
        debug!(
            "Storing property {} {} in db ({:?})",
            prop.interface, prop.path, prop.value
        );

        let buf = Payload::new(prop.value)
            .to_vec()
            .map_err(ValueError::Encode)?;

        self.writer.lock().await.store_prop(prop, &buf)?;

        Ok(())
    }

    async fn load_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> Result<Option<AstarteData>, Self::Err> {
        let opt_record = self
            .with_reader(|reader| reader.load_prop(property.interface_name(), property.path()))?;

        match opt_record {
            Some(record) => {
                trace!(
                    "Loaded property {} {} in db {:?}",
                    property.interface_name(),
                    property.path(),
                    record
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
        self.writer
            .lock()
            .await
            .unset_prop(property.interface_name(), property.path())?;

        Ok(())
    }

    async fn delete_prop(&self, property: &PropertyMapping<'_>) -> Result<(), Self::Err> {
        self.writer
            .lock()
            .await
            .delete_prop(property.interface_name(), property.path())?;

        Ok(())
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        self.writer.lock().await.clear_props()?;

        Ok(())
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.with_reader(|reader| reader.load_all_props())
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.with_reader(|reader| reader.props_with_ownership(Ownership::Device))
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.with_reader(|reader| reader.props_with_ownership(Ownership::Server))
    }

    async fn interface_props(&self, interface: &Properties) -> Result<Vec<StoredProp>, Self::Err> {
        self.with_reader(|reader| reader.interface_props(interface.name()))
    }

    async fn delete_interface(&self, interface: &Properties) -> Result<(), Self::Err> {
        self.writer
            .lock()
            .await
            .delete_interface_props(interface.name())?;

        Ok(())
    }

    async fn device_props_with_unset(&self) -> Result<Vec<OptStoredProp>, Self::Err> {
        self.with_reader(|reader| reader.props_with_unset(Ownership::Device))
    }
}

#[cfg(not(feature = "tokio-multi-thread"))]
/// Functions to wrap the sync calls to the database and not starve the other tasks.
pub(crate) fn wrap_sync_call<F, O>(f: F) -> O
where
    F: FnOnce() -> O,
{
    (f)()
}

#[cfg(feature = "tokio-multi-thread")]
/// Functions to wrap the sync calls to the database and not starve the other tasks.
pub(crate) fn wrap_sync_call<F, O>(f: F) -> O
where
    F: FnOnce() -> O,
{
    let Ok(current) = tokio::runtime::Handle::try_current() else {
        return (f)();
    };

    match current.runtime_flavor() {
        // We cannot block in place, so we execute the call directly
        tokio::runtime::RuntimeFlavor::CurrentThread => (f)(),
        tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
        // Matches tokio-unstable MultiThreadAlt
        _ => tokio::task::block_in_place(f),
    }
}

/// Deserialize a property from the store.
fn deserialize_prop(stored_type: u8, buf: &[u8]) -> Result<AstarteData, ValueError> {
    let mapping_type = from_stored_type(stored_type)?;

    let payload = Payload::from_slice(buf).map_err(ValueError::Decode)?;
    let value = BsonConverter::new(mapping_type, payload.value);

    value.try_into().map_err(ValueError::from)
}

/// Necessary for rust 1.78 const compatibility
const fn const_non_zero(v: u64) -> NonZeroU64 {
    let Some(v) = NonZeroU64::new(v) else {
        panic!("value cannot be zero");
    };

    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::test_property_store;

    #[tokio::test]
    async fn test_sqlite_store() {
        let dir = tempfile::tempdir().unwrap();

        let db = SqliteStore::connect(dir.path()).await.unwrap();

        test_property_store(db).await;
    }

    #[tokio::test]
    async fn multiple_db_per_thread() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();

        let db1 = SqliteStore::connect(dir1.path()).await.unwrap();

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

        let db2 = SqliteStore::connect(dir2.path()).await.unwrap();

        (test)(db2).await;
    }

    #[tokio::test]
    async fn set_max_pages_invalid_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = SqliteStore::connect(dir.path()).await.unwrap();

        let err = db.set_max_pages(0).await.unwrap_err();

        assert!(matches!(
            err,
            SqliteError::InvalidMaxSize {
                ctx,
            } if ctx == "max page count cannot be 0"
        ));
    }

    #[tokio::test]
    async fn skip_set_max_pages() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = SqliteStore::connect(dir.path()).await.unwrap();

        {
            let connection = db.writer.lock().await;
            set_pragma(&connection, "max_page_count", 1000).unwrap();
        }

        assert!(db.set_max_pages(1000).await.is_ok());
    }

    #[tokio::test]
    async fn set_max_pages_cannot_shrink() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = SqliteStore::connect(dir.path()).await.unwrap();

        let page_size: usize = {
            let connection = db.writer.lock().await;
            get_pragma(&connection, "page_size").unwrap()
        };

        db.store_prop(StoredProp {
            interface: "interface",
            path: "/path",
            value: &AstarteData::BinaryBlob(vec![1; page_size * 3]),
            interface_major: 0,
            ownership: Ownership::Device,
        })
        .await
        .unwrap();

        let err = db.set_max_pages(1).await.unwrap_err();

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
        let mut db = SqliteStore::connect(dir.path()).await.unwrap();

        let (page_size, page_count): (u32, u32) = {
            let connection = db.writer.lock().await;
            (
                get_pragma(&connection, "page_size").unwrap(),
                get_pragma(&connection, "page_count").unwrap(),
            )
        };

        db.set_max_pages(page_count).await.unwrap();

        let size = (page_size * page_count + 1) as usize;

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
        let dir = tempfile::tempdir().unwrap();
        let mut db = SqliteStore::connect(dir.path()).await.unwrap();

        assert!(db.set_max_pages(10).await.is_ok());

        let lock = db.writer.lock().await;
        let page_count: u32 = get_pragma(&lock, "max_page_count").unwrap();
        let exp_count = 10;

        assert_eq!(page_count, exp_count);
    }

    #[tokio::test]
    async fn set_db_max_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = SqliteStore::connect(dir.path()).await.unwrap();

        let size = Size::MiB(NonZeroU64::new(4).unwrap());

        // set the max size considering the default page size of 4096 bytes
        db.set_db_max_size(size).await.unwrap();

        let lock = db.writer.lock().await;
        let page_count: u32 = get_pragma(&lock, "max_page_count").unwrap();
        let exp_count = 1024; // 4MiB / 4096B = 1024 pages

        assert_eq!(page_count, exp_count);
    }

    #[tokio::test]
    async fn set_db_max_size_min() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = SqliteStore::connect(dir.path()).await.unwrap();

        let size = Size::Kb(NonZeroU64::new(1).unwrap());

        // set the max size considering the default page size of 4096 bytes
        // NOTE since the limit is set after the database is created we can't shrink an
        // already created database this means that settin a 1KB limit is currently not supported
        // even a 1 page limit (4096B) would not work
        let res = db.set_db_max_size(size).await;

        assert!(res.is_err());
    }

    #[test]
    fn size_to_kibibytes_ceil_min() {
        let size = Size::Kb(NonZeroU64::new(1).unwrap());
        assert_eq!(size.to_kibibytes_ceil(), 1);
    }

    #[tokio::test]
    async fn set_journal_size_limit() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = SqliteStore::connect(dir.path()).await.unwrap();

        let size = Size::MiB(NonZeroU64::new(1).unwrap());

        // set the max size considering the default page size of 4096 bytes
        assert!(db.set_journal_size_limit(size).await.is_ok());

        let lock = db.writer.lock().await;
        let journal_size: u32 = get_pragma(&lock, "journal_size_limit").unwrap();
        let exp_size: u32 = size.to_bytes().try_into().unwrap();
        assert_eq!(journal_size, exp_size);

        let wal_autocheckpoint: u32 = get_pragma(&lock, "wal_autocheckpoint").unwrap();
        // autocheckpoin is set to a fraction of the journal_size in pages (pages / 10)
        // in this case 1MiB / 4096 / 10 = 25
        let exp_autocheckpoint = 25;
        assert_eq!(wal_autocheckpoint, exp_autocheckpoint);
    }

    #[tokio::test]
    async fn set_journal_size_limit_min() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = SqliteStore::connect(dir.path()).await.unwrap();

        let size = Size::Kb(NonZeroU64::new(1).unwrap());

        // set the max size considering the default page size of 4096 bytes
        assert!(db.set_journal_size_limit(size).await.is_ok());

        let lock = db.writer.lock().await;
        let journal_size: u32 = get_pragma(&lock, "journal_size_limit").unwrap();
        let exp_size: u32 = size.to_bytes().try_into().unwrap();
        assert_eq!(journal_size, exp_size);

        let wal_autocheckpoint: u32 = get_pragma(&lock, "wal_autocheckpoint").unwrap();
        // autocheckpoin is set to a fraction of the journal_size in pages
        // in this case the size in pages is 0 (1KB / 4096 = 0) but the minimum we allow is 1
        let exp_autocheckpoint = 1;
        assert_eq!(wal_autocheckpoint, exp_autocheckpoint);
    }

    #[test]
    fn size_to_bytes() {
        let size = Size::Kb(NonZeroU64::new(1).unwrap());
        assert_eq!(size.to_bytes(), 1000);

        let size = Size::Mb(NonZeroU64::new(1).unwrap());
        assert_eq!(size.to_bytes(), 1000 * 1000);

        let size = Size::Gb(NonZeroU64::new(1).unwrap());
        assert_eq!(size.to_bytes(), 1000 * 1000 * 1000);
    }

    #[test]
    fn size_to_kib() {
        let size = Size::KiB(NonZeroU64::new(1).unwrap());
        assert_eq!(size.to_bytes(), 1024);

        let size = Size::MiB(NonZeroU64::new(1).unwrap());
        assert_eq!(size.to_bytes(), 1024 * 1024);

        let size = Size::GiB(NonZeroU64::new(1).unwrap());
        assert_eq!(size.to_bytes(), 1024 * 1024 * 1024);
    }

    #[test]
    #[should_panic(expected = "value cannot be zero")]
    fn const_non_zero_should_panic() {
        const_non_zero(0);
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
