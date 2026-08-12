// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use astarte_device_error::{Error, WrapError};
use astarte_interfaces::schema::Ownership;
use rusqlite::OptionalExtension;
use tracing::{instrument, warn};

use crate::store::{OptStoredProp, Prop, PropertyState, sqlite::RecordPropertyState};
use crate::store::{StoredProp, UpdatedAt};

use super::connection::{ReadConnection, WriteConnection};
use super::{RecordOwnership, SqliteError, StoredRecord, into_stored_type};

macro_rules! include_query {
    ($file:expr) => {
        include_str!(concat!("../../../", $file))
    };
}

pub(crate) use include_query;

impl WriteConnection {
    #[instrument(skip_all)]
    pub(super) fn store_prop(
        &mut self,
        prop: &Prop,
        buf: &[u8],
    ) -> Result<Option<u8>, Error<SqliteError>> {
        let mapping_type = into_stored_type(&prop.value);
        let ownership = RecordOwnership::from(prop.ownership);
        let (updated_at, nanos, counter) = updated_at_to_i64(prop.updated_at);

        let mut statement = self
            .prepare_cached(include_query!("queries/properties/write/store_prop.sql"))
            .wrap_err_msg(SqliteError::Prepare, "while storing property")?;

        let epoch = statement
            .query_one::<u8, _, _>(
                (
                    &prop.interface,
                    &prop.path,
                    buf,
                    mapping_type,
                    prop.interface_major,
                    ownership,
                    RecordPropertyState::Changed,
                    updated_at,
                    nanos,
                    counter,
                ),
                |r| r.get(0),
            )
            .optional()
            .wrap_err_msg(SqliteError::Query, "while storing property")?;

        Ok(epoch)
    }

    #[instrument(skip_all)]
    pub(super) fn update_state(
        &mut self,
        interface: &str,
        path: &str,
        state: PropertyState,
        epoch: u8,
    ) -> Result<usize, Error<SqliteError>> {
        let mut statement = self
            .prepare_cached(include_query!("queries/properties/write/update_state.sql"))
            .wrap_err_msg(SqliteError::Prepare, "while updating state")?;

        let new_epoch = if state == PropertyState::Completed {
            0
        } else {
            epoch
        };

        let result = statement
            .execute((
                RecordPropertyState::from(state),
                new_epoch,
                interface,
                path,
                epoch,
            ))
            .wrap_err_msg(SqliteError::Query, "while updating state")?;

        debug_assert!((0..=1usize).contains(&result));

        Ok(result)
    }

    #[instrument(skip(self))]
    pub(super) fn unset_prop(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
        updated_at: UpdatedAt,
    ) -> Result<Option<u8>, Error<SqliteError>> {
        let (updated_at, nanos, counter) = updated_at_to_i64(updated_at);

        let mut statement = self
            .prepare_cached(include_query!("queries/properties/write/unset_prop.sql"))
            .wrap_err(SqliteError::Prepare)?;

        let epoch = statement
            .query_one::<u8, _, _>(
                (
                    RecordPropertyState::Changed,
                    updated_at,
                    nanos,
                    counter,
                    interface,
                    path,
                    interface_major,
                ),
                |r| r.get(0),
            )
            .optional()
            .wrap_err(SqliteError::Query)?;

        Ok(epoch)
    }

    #[instrument(skip(self))]
    pub(super) fn delete_device_prop(
        &self,
        interface: &str,
        path: &str,
        epoch: u8,
    ) -> Result<bool, Error<SqliteError>> {
        let mut statement = self
            .prepare_cached(include_query!(
                "queries/properties/write/delete_device_prop.sql"
            ))
            .wrap_err(SqliteError::Prepare)?;

        let deleted = statement
            .execute((interface, path, epoch))
            .wrap_err(SqliteError::Query)?;

        debug_assert!((0..=1).contains(&deleted));

        Ok(deleted != 0)
    }

    #[instrument(skip(self))]
    pub(super) fn delete_server_prop(
        &self,
        interface: &str,
        path: &str,
    ) -> Result<bool, Error<SqliteError>> {
        let mut statement = self
            .prepare_cached(include_query!("queries/properties/write/delete_prop.sql"))
            .wrap_err(SqliteError::Prepare)?;

        let deleted = statement
            .execute((interface, path))
            .wrap_err(SqliteError::Query)?;

        debug_assert!((0..=1).contains(&deleted));

        Ok(deleted != 0)
    }

    #[instrument(skip(self))]
    pub(super) fn clear_props(&self) -> Result<(), Error<SqliteError>> {
        let mut statement = self
            .prepare_cached(include_query!("queries/properties/write/clear.sql"))
            .wrap_err(SqliteError::Prepare)?;

        statement.execute(()).wrap_err(SqliteError::Query)?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub(super) fn delete_interface_props(&self, interface: &str) -> Result<(), Error<SqliteError>> {
        let mut statement = self
            .prepare_cached(include_query!(
                "queries/properties/write/delete_interface.sql"
            ))
            .wrap_err(SqliteError::Prepare)?;

        statement
            .execute([interface])
            .wrap_err(SqliteError::Query)?;

        Ok(())
    }

    #[instrument(skip_all)]
    pub(super) fn reset_session(&self) -> Result<(), Error<SqliteError>> {
        let mut reset_statement = self
            .prepare_cached(include_query!("queries/properties/write/reset_state.sql"))
            .wrap_err(SqliteError::Prepare)?;

        reset_statement
            .execute((
                RecordPropertyState::Changed,
                RecordOwnership::from(Ownership::Device),
            ))
            .wrap_err(SqliteError::Query)?;

        let mut delete_statement = self
            .prepare_cached(include_query!(
                "queries/properties/write/delete_all_unset.sql"
            ))
            .wrap_err(SqliteError::Prepare)?;

        delete_statement.execute(()).wrap_err(SqliteError::Query)?;

        Ok(())
    }

    // Delete a property with a wrong version major
    pub(crate) fn delete_prop_with_major(
        &self,
        interface_name: &str,
        path: &str,
        version_major: i32,
    ) -> Result<usize, Error<SqliteError>> {
        let mut statement = self
            .prepare_cached(include_query!(
                "queries/properties/write/delete_prop_with_major.sql"
            ))
            .wrap_err(SqliteError::Prepare)?;

        let changed = statement
            .execute((interface_name, path, version_major))
            .wrap_err(SqliteError::Query)?;

        debug_assert!((0..=1).contains(&changed));

        Ok(changed)
    }
}

fn query_prop_row(
    connection: &rusqlite::Connection,
    interface: &str,
    path: &str,
) -> Result<Option<StoredRecord>, Error<SqliteError>> {
    let mut statement = connection
        .prepare_cached(include_query!("queries/properties/read/load_prop.sql"))
        .wrap_err_msg(SqliteError::Prepare, "while querying property")?;

    statement
        .query_row((interface, path), |row| {
            Ok(StoredRecord {
                interface: row.get(0)?,
                path: row.get(1)?,
                value: row.get(2)?,
                stored_type: row.get(3)?,
                interface_major: row.get(4)?,
                ownership: row.get(5)?,
                epoch: row.get(6)?,
                updated_at: row.get(7)?,
                updated_at_nanos: row.get(8)?,
                updated_at_counter: row.get(9)?,
            })
        })
        .optional()
        .wrap_err_msg(SqliteError::Prepare, "while querying property")
}

impl ReadConnection {
    #[instrument(skip(self))]
    pub(super) fn load_prop(
        &self,
        interface: &str,
        path: &str,
    ) -> Result<Option<StoredRecord>, Error<SqliteError>> {
        query_prop_row(self, interface, path)
    }

    #[instrument(skip(self))]
    pub(super) fn load_all_props(
        &self,
        limit: usize,
        last_updated_at: Option<UpdatedAt>,
    ) -> Result<Vec<StoredProp>, Error<SqliteError>> {
        let limit = i64::try_from(limit)
            .wrap_err_with(|_| Error::new(SqliteError::Conversion).set_ctx(limit))?;
        let (updated_at, nanos, counter) = opt_updated_at_to_i64(last_updated_at);

        let mut statement = self
            .prepare_cached(include_query!("queries/properties/read/load_all_props.sql"))
            .wrap_err(SqliteError::Prepare)?;

        let vec = statement
            .query_map((updated_at, nanos, counter, limit), |row| {
                Ok(StoredRecord {
                    interface: row.get(0)?,
                    path: row.get(1)?,
                    value: row.get(2)?,
                    stored_type: row.get(3)?,
                    interface_major: row.get(4)?,
                    ownership: row.get(5)?,
                    epoch: row.get(6)?,
                    updated_at: row.get(7)?,
                    updated_at_nanos: row.get(8)?,
                    updated_at_counter: row.get(9)?,
                })
            })
            .wrap_err(SqliteError::Query)?
            .filter_map(|e| {
                e.wrap_err(SqliteError::Query)
                    .and_then(StoredRecord::try_into_prop)
                    .transpose()
            })
            .collect::<Result<Vec<StoredProp>, Error<SqliteError>>>()?;

        Ok(vec)
    }

    #[instrument(skip(self))]
    pub(super) fn props_with_ownership(
        &self,
        ownership: Ownership,
        limit: usize,
        last_updated_at: Option<UpdatedAt>,
    ) -> Result<Vec<StoredProp>, Error<SqliteError>> {
        let ownership_par = RecordOwnership::from(ownership);
        let limit = i64::try_from(limit)
            .wrap_err_with(|_| Error::new(SqliteError::Conversion).set_ctx(limit))?;
        let (updated_at, nanos, counter) = opt_updated_at_to_i64(last_updated_at);

        let mut statement = self
            .prepare_cached(include_query!(
                "queries/properties/read/props_where_ownership.sql"
            ))
            .wrap_err(SqliteError::Prepare)?;

        let v = statement
            .query_map((ownership_par, updated_at, nanos, counter, limit), |row| {
                Ok(StoredRecord {
                    interface: row.get(0)?,
                    path: row.get(1)?,
                    value: row.get(2)?,
                    stored_type: row.get(3)?,
                    interface_major: row.get(4)?,
                    ownership: row.get(5)?,
                    epoch: row.get(6)?,
                    updated_at: row.get(7)?,
                    updated_at_nanos: row.get(8)?,
                    updated_at_counter: row.get(9)?,
                })
            })
            .wrap_err(SqliteError::Query)?
            .filter_map(|res| {
                let record = match res {
                    Ok(record) => record,
                    Err(err) => return Some(Err(Error::new(SqliteError::Query).set_source(err))),
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
            .collect::<Result<Vec<StoredProp>, Error<SqliteError>>>()?;

        Ok(v)
    }

    pub(super) fn props_with_unset(
        &self,
        ownership: Ownership,
        state: PropertyState,
        limit: usize,
        last_updated_at: Option<UpdatedAt>,
    ) -> Result<Vec<OptStoredProp>, Error<SqliteError>> {
        let limit = i64::try_from(limit)
            .wrap_err_with(|_| Error::new(SqliteError::Conversion).set_ctx(limit))?;
        let (updated_at, nanos, counter) = opt_updated_at_to_i64(last_updated_at);

        let ownership_par = RecordOwnership::from(ownership);

        let mut statement = self
            .prepare_cached(include_query!(
                "queries/properties/read/props_with_unset.sql"
            ))
            .wrap_err(SqliteError::Prepare)?;

        let v = statement
            .query_map(
                (
                    ownership_par,
                    RecordPropertyState::from(state),
                    updated_at,
                    nanos,
                    counter,
                    limit,
                ),
                |row| {
                    Ok(StoredRecord {
                        interface: row.get(0)?,
                        path: row.get(1)?,
                        value: row.get(2)?,
                        stored_type: row.get(3)?,
                        interface_major: row.get(4)?,
                        ownership: row.get(5)?,
                        epoch: row.get(6)?,
                        updated_at: row.get(7)?,
                        updated_at_nanos: row.get(8)?,
                        updated_at_counter: row.get(9)?,
                    })
                },
            )
            .wrap_err(SqliteError::Query)?
            .map(|e| {
                e.wrap_err(SqliteError::Query).and_then(|record| {
                    let prop = OptStoredProp::try_from(record)?;

                    debug_assert_eq!(prop.ownership, ownership);

                    Ok(prop)
                })
            })
            .collect::<Result<Vec<OptStoredProp>, Error<SqliteError>>>()?;

        Ok(v)
    }

    pub(super) fn interface_props(
        &self,
        interface: &str,
        limit: usize,
        last_updated_at: Option<UpdatedAt>,
    ) -> Result<Vec<StoredProp>, Error<SqliteError>> {
        let limit = i64::try_from(limit)
            .wrap_err_with(|_| Error::new(SqliteError::Conversion).set_ctx(limit))?;
        let (updated_at, nanos, counter) = opt_updated_at_to_i64(last_updated_at);

        let mut statement = self
            .prepare_cached(include_query!(
                "queries/properties/read/interface_props.sql"
            ))
            .wrap_err(SqliteError::Prepare)?;

        let v = statement
            .query_map((interface, updated_at, nanos, counter, limit), |row| {
                Ok(StoredRecord {
                    interface: row.get(0)?,
                    path: row.get(1)?,
                    value: row.get(2)?,
                    stored_type: row.get(3)?,
                    interface_major: row.get(4)?,
                    ownership: row.get(5)?,
                    epoch: row.get(6)?,
                    updated_at: row.get(7)?,
                    updated_at_nanos: row.get(8)?,
                    updated_at_counter: row.get(9)?,
                })
            })
            .wrap_err(SqliteError::Query)?
            .filter_map(|e| {
                e.wrap_err(SqliteError::Query)
                    .and_then(StoredRecord::try_into_prop)
                    .transpose()
            })
            .collect::<Result<Vec<StoredProp>, Error<SqliteError>>>()?;

        Ok(v)
    }
}

fn opt_updated_at_to_i64(opt: Option<UpdatedAt>) -> (Option<i64>, Option<i64>, Option<i64>) {
    let Some(updated_at) = opt else {
        return (None, None, None);
    };

    let (updated_at, nanos, counter) = updated_at_to_i64(updated_at);

    (Some(updated_at), Some(nanos), Some(counter))
}

fn updated_at_to_i64(updated_at: UpdatedAt) -> (i64, i64, i64) {
    (
        updated_at.timestamp().timestamp(),
        i64::from(updated_at.timestamp().timestamp_subsec_nanos()),
        i64::from(updated_at.counter()),
    )
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::store::SqliteStore;
    use crate::store::sqlite::connection::SqliteConnection;
    use crate::store::sqlite::{SQLITE_JOURNAL_SIZE_LIMIT, Size};

    #[tokio::test]
    async fn custom_journal_size_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteStore::options()
            .with_writable_dir(dir.as_ref())
            .await
            .unwrap();

        let journal_size: i64 = db
            .pool
            .acquire_writer(|writer| writer.get_pragma("journal_size_limit"))
            .await
            .unwrap();

        // check that journal size has been set to default
        assert_eq!(
            u64::try_from(journal_size).unwrap(),
            SQLITE_JOURNAL_SIZE_LIMIT.to_bytes().get()
        );

        let new_journal_size: i64 = Size::MiB(NonZeroU64::new(100).unwrap())
            .to_bytes()
            .get()
            .try_into()
            .unwrap();

        // change journal size
        db.pool
            .acquire_writer(move |writer| {
                writer.set_pragma("journal_size_limit", &new_journal_size)
            })
            .await
            .unwrap();

        assert!(dir.path().join("prop-cache.db").exists());

        drop(db);

        // reopen the db connection resets the journal size
        let db: SqliteStore = SqliteStore::options()
            .with_writable_dir(dir.as_ref())
            .await
            .unwrap();

        let journal_size: i64 = db
            .pool
            .acquire_writer(|writer| writer.get_pragma("journal_size_limit"))
            .await
            .unwrap();

        assert_eq!(
            journal_size,
            SQLITE_JOURNAL_SIZE_LIMIT.to_bytes().get() as i64
        );
    }
}
