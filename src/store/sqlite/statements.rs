// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
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

use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

use rusqlite::{Connection, OpenFlags, OptionalExtension, ToSql};

use crate::{
    interface::Ownership,
    store::{MaybeStoredProp, StoredProp},
    AstarteType,
};

use super::{
    into_stored_type, wrap_sync_call, PropRecord, RecordOwnership, SqliteError, StoredRecord,
    SQLITE_BUSY_TIMEOUT,
};

#[cfg(feature = "sqlite-trace")]
/// Logs the execution of SQLite statements
#[tracing::instrument(name = "statement", skip_all)]
fn trace_sqlite(event: &str) {
    tracing::trace!(event)
}

macro_rules! include_query {
    ($file:expr) => {
        include_str!(concat!("../../../", $file))
    };
}

pub(crate) use include_query;

#[derive(Debug)]
pub(crate) struct WriteConnection(Connection);

impl WriteConnection {
    pub(crate) async fn connect(db_file: impl AsRef<Path>) -> Result<Self, SqliteError> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;

        let connection = wrap_sync_call(|| Connection::open_with_flags(&db_file, flags))
            .map_err(SqliteError::Connection)?;

        #[cfg(feature = "sqlite-trace")]
        let mut connection = connection;

        #[cfg(feature = "sqlite-trace")]
        connection.trace(Some(trace_sqlite));

        let connection = Self(connection);

        connection.set_pragma("foreign_keys", true)?;
        connection.set_pragma("busy_timeout", SQLITE_BUSY_TIMEOUT)?;
        connection.set_pragma("journal_mode", "WALL")?;

        Ok(connection)
    }

    pub(crate) fn set_pragma<V>(
        &self,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<(), SqliteError>
    where
        V: ToSql,
    {
        wrap_sync_call(|| self.pragma_update(None, pragma_name, pragma_value))
            .map_err(SqliteError::Option)
    }

    pub(super) fn store_prop(
        &self,
        prop: StoredProp<&str, &AstarteType>,
        buf: &[u8],
    ) -> Result<(), SqliteError> {
        let mapping_type = into_stored_type(prop.value)?;

        let ownership = RecordOwnership::from(prop.ownership);

        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/properties/write/store_prop.sql"))
                .map_err(SqliteError::Prepare)?;

            statement
                .execute((
                    prop.interface,
                    prop.path,
                    buf,
                    mapping_type,
                    prop.interface_major,
                    ownership,
                ))
                .map_err(SqliteError::Query)?;

            Ok(())
        })
    }

    pub(super) fn unset_prop(&self, interface: &str, path: &str) -> Result<(), SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/properties/write/unset_prop.sql"))
                .map_err(SqliteError::Prepare)?;

            let updated = statement
                .execute((interface, path))
                .map_err(SqliteError::Query)?;

            debug_assert!((0..=1).contains(&updated));

            Ok(())
        })
    }

    pub(super) fn delete_prop(&self, interface: &str, path: &str) -> Result<(), SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/properties/write/delete_prop.sql"))
                .map_err(SqliteError::Prepare)?;

            let deleted = statement
                .execute((interface, path))
                .map_err(SqliteError::Query)?;

            debug_assert!((0..=1).contains(&deleted));

            Ok(())
        })
    }

    pub(super) fn clear_props(&self) -> Result<(), SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/properties/write/clear.sql"))
                .map_err(SqliteError::Prepare)?;

            statement.execute(()).map_err(SqliteError::Query)?;

            Ok(())
        })
    }

    pub(super) fn delete_interface_props(&self, interface: &str) -> Result<(), SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!(
                    "queries/properties/write/delete_interface.sql"
                ))
                .map_err(SqliteError::Prepare)?;

            statement.execute([interface]).map_err(SqliteError::Query)?;

            Ok(())
        })
    }
}

impl Deref for WriteConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WriteConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub(crate) struct ReadConnection(Connection);

impl ReadConnection {
    pub(crate) fn connect(db_file: impl AsRef<Path>) -> Result<Self, SqliteError> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;

        let mut connection =
            Connection::open_with_flags(db_file, flags).map_err(SqliteError::Connection)?;

        #[cfg(feature = "sqlite-trace")]
        connection.trace(Some(trace_sqlite));

        Ok(Self(connection))
    }

    pub(super) fn load_prop(
        &self,
        interface: &str,
        path: &str,
    ) -> Result<Option<PropRecord>, SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/properties/read/load_prop.sql"))
                .map_err(SqliteError::Prepare)?;

            statement
                .query_row((interface, path), |row| {
                    Ok(PropRecord {
                        value: row.get(0)?,
                        stored_type: row.get(1)?,
                        interface_major: row.get(2)?,
                    })
                })
                .optional()
                .map_err(SqliteError::Query)
        })
    }

    pub(super) fn load_all_props(&self) -> Result<Vec<StoredProp>, SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/properties/read/load_all_props.sql"))
                .map_err(SqliteError::Prepare)?;

            let v = statement
                .query_map((), |row| {
                    Ok(StoredRecord {
                        interface: row.get(0)?,
                        path: row.get(1)?,
                        value: row.get(2)?,
                        stored_type: row.get(3)?,
                        interface_major: row.get(4)?,
                        ownership: row.get(5)?,
                    })
                })
                .map_err(SqliteError::Query)?
                .filter_map(|e| {
                    e.map_err(SqliteError::Query)
                        .and_then(StoredRecord::try_into_prop)
                        .transpose()
                })
                .collect::<Result<Vec<StoredProp>, SqliteError>>()?;

            Ok(v)
        })
    }

    pub(super) fn props_with_ownership(
        &self,
        ownership: Ownership,
    ) -> Result<Vec<StoredProp>, SqliteError> {
        let ownership_par = RecordOwnership::from(ownership);

        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!(
                    "queries/properties/read/props_where_ownership.sql"
                ))
                .map_err(SqliteError::Prepare)?;

            let v = statement
                .query_map([ownership_par], |row| {
                    Ok(StoredRecord {
                        interface: row.get(0)?,
                        path: row.get(1)?,
                        value: row.get(2)?,
                        stored_type: row.get(3)?,
                        interface_major: row.get(4)?,
                        ownership: row.get(5)?,
                    })
                })
                .map_err(SqliteError::Query)?
                .filter_map(|res| {
                    let record = match res {
                        Ok(record) => record,
                        Err(err) => return Some(Err(SqliteError::Query(err))),
                    };

                    match record.try_into_prop() {
                        Ok(Some(prop)) => {
                            debug_assert_eq!(prop.ownership, ownership);

                            Some(Ok(prop))
                        }
                        Ok(None) => None,
                        Err(err) => Some(Err(err)),
                    }
                })
                .collect::<Result<Vec<StoredProp>, SqliteError>>()?;

            Ok(v)
        })
    }

    pub(super) fn props_with_unset(
        &self,
        ownership: Ownership,
    ) -> Result<Vec<MaybeStoredProp>, SqliteError> {
        let ownership_par = RecordOwnership::from(ownership);

        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!(
                    "queries/properties/read/props_with_unset.sql"
                ))
                .map_err(SqliteError::Prepare)?;

            let v = statement
                .query_map([ownership_par], |row| {
                    Ok(StoredRecord {
                        interface: row.get(0)?,
                        path: row.get(1)?,
                        value: row.get(2)?,
                        stored_type: row.get(3)?,
                        interface_major: row.get(4)?,
                        ownership: row.get(5)?,
                    })
                })
                .map_err(SqliteError::Query)?
                .map(|e| {
                    e.map_err(SqliteError::Query).and_then(|record| {
                        let prop = MaybeStoredProp::try_from(record)?;

                        debug_assert_eq!(prop.ownership, ownership);

                        Ok(prop)
                    })
                })
                .collect::<Result<Vec<MaybeStoredProp>, SqliteError>>()?;

            Ok(v)
        })
    }

    pub(super) fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!(
                    "queries/properties/read/interface_props.sql"
                ))
                .map_err(SqliteError::Prepare)?;

            let v = statement
                .query_map([interface], |row| {
                    Ok(StoredRecord {
                        interface: row.get(0)?,
                        path: row.get(1)?,
                        value: row.get(2)?,
                        stored_type: row.get(3)?,
                        interface_major: row.get(4)?,
                        ownership: row.get(5)?,
                    })
                })
                .map_err(SqliteError::Query)?
                .filter_map(|e| {
                    e.map_err(SqliteError::Query)
                        .and_then(StoredRecord::try_into_prop)
                        .transpose()
                })
                .collect::<Result<Vec<StoredProp>, SqliteError>>()?;

            Ok(v)
        })
    }
}

impl Deref for ReadConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
