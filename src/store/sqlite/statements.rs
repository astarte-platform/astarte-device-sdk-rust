// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
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

use astarte_interfaces::schema::Ownership;
use rusqlite::OptionalExtension;
use tracing::{instrument, warn};

use crate::{
    AstarteData,
    store::{OptStoredProp, PropertyState, StoredProp, sqlite::RecordPropertyState},
};

use super::connection::{ReadConnection, WriteConnection};
use super::{PropRecord, RecordOwnership, SqliteError, StoredRecord, into_stored_type};

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
        prop: StoredProp<&str, &AstarteData>,
        buf: &[u8],
    ) -> Result<(), SqliteError> {
        let mapping_type = into_stored_type(prop.value)?;

        let ownership = RecordOwnership::from(prop.ownership);

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
                // NOTE the state when we store a property will always be changed
                // the old value of the property has to be checked before calling store_prop
                // if the value gets changed after the check and before this store is executed by another task
                // we expect to send multiple time the data even if the value is the same
                RecordPropertyState::Changed,
            ))
            .map_err(SqliteError::Query)?;

        Ok(())
    }

    #[instrument(skip_all)]
    pub(super) fn update_state(
        &mut self,
        interface: &str,
        path: &str,
        expected: Option<&AstarteData>,
        state: PropertyState,
    ) -> Result<usize, SqliteError> {
        let transaction = self.transaction().map_err(SqliteError::Transaction)?;

        let result = {
            let value = query_prop_row(&transaction, interface, path)?
                .map(PropRecord::try_into_value)
                .transpose()?
                .flatten();

            if expected != value.as_ref() {
                // if the value is different from the expected one no records will be updated
                return Ok(0);
            }

            let mut statement = transaction
                .prepare_cached(include_query!("queries/properties/write/update_state.sql"))
                .map_err(SqliteError::Prepare)?;

            let result = statement
                .execute((RecordPropertyState::from(state), interface, path))
                .map_err(SqliteError::Query)?;

            debug_assert!(1 == result);

            result
        };

        transaction.commit()?;

        Ok(result)
    }

    pub(super) fn unset_prop(&self, interface: &str, path: &str) -> Result<(), SqliteError> {
        let mut statement = self
            .prepare_cached(include_query!("queries/properties/write/unset_prop.sql"))
            .map_err(SqliteError::Prepare)?;

        let updated = statement
            .execute((RecordPropertyState::Changed, interface, path))
            .map_err(SqliteError::Query)?;

        debug_assert!((0..=1).contains(&updated));

        Ok(())
    }

    pub(super) fn delete_prop(&self, interface: &str, path: &str) -> Result<(), SqliteError> {
        let mut statement = self
            .prepare_cached(include_query!("queries/properties/write/delete_prop.sql"))
            .map_err(SqliteError::Prepare)?;

        let deleted = statement
            .execute((interface, path))
            .map_err(SqliteError::Query)?;

        debug_assert!((0..=1).contains(&deleted));

        Ok(())
    }

    pub(super) fn delete_expected_prop(
        &mut self,
        interface: &str,
        path: &str,
        expected: Option<&AstarteData>,
    ) -> Result<usize, SqliteError> {
        let transaction = self.transaction().map_err(SqliteError::Transaction)?;

        let deleted = {
            let value = query_prop_row(&transaction, interface, path)?
                .map(PropRecord::try_into_value)
                .transpose()?
                .flatten();

            if expected != value.as_ref() {
                // if the value is different from the expected one no records will be updated
                return Ok(0);
            }

            let mut statement = transaction
                .prepare_cached(include_query!("queries/properties/write/delete_prop.sql"))
                .map_err(SqliteError::Prepare)?;

            let deleted = statement
                .execute((interface, path))
                .map_err(SqliteError::Query)?;

            debug_assert!((0..=1).contains(&deleted));

            deleted
        };

        transaction.commit()?;

        Ok(deleted)
    }

    pub(super) fn clear_props(&self) -> Result<(), SqliteError> {
        let mut statement = self
            .prepare_cached(include_query!("queries/properties/write/clear.sql"))
            .map_err(SqliteError::Prepare)?;

        statement.execute(()).map_err(SqliteError::Query)?;

        Ok(())
    }

    pub(super) fn delete_interface_props(&self, interface: &str) -> Result<(), SqliteError> {
        let mut statement = self
            .prepare_cached(include_query!(
                "queries/properties/write/delete_interface.sql"
            ))
            .map_err(SqliteError::Prepare)?;

        statement.execute([interface]).map_err(SqliteError::Query)?;

        Ok(())
    }

    #[instrument(skip_all)]
    pub(super) fn reset_state(&self, ownership: Ownership) -> Result<(), SqliteError> {
        let mut statement = self
            .prepare_cached(include_query!("queries/properties/write/reset_state.sql"))
            .map_err(SqliteError::Prepare)?;

        statement
            .execute((
                RecordPropertyState::Changed,
                RecordOwnership::from(ownership),
            ))
            .map_err(SqliteError::Query)?;

        Ok(())
    }
}

fn query_prop_row(
    connection: &rusqlite::Connection,
    interface: &str,
    path: &str,
) -> Result<Option<PropRecord>, SqliteError> {
    let mut statement = connection
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
}

impl ReadConnection {
    pub(super) fn load_prop(
        &self,
        interface: &str,
        path: &str,
    ) -> Result<Option<PropRecord>, SqliteError> {
        query_prop_row(self, interface, path)
    }

    pub(super) fn load_all_props(&self) -> Result<Vec<StoredProp>, SqliteError> {
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
    }

    pub(super) fn props_with_ownership(
        &self,
        ownership: Ownership,
    ) -> Result<Vec<StoredProp>, SqliteError> {
        let ownership_par = RecordOwnership::from(ownership);

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
    }

    pub(super) fn props_with_unset(
        &self,
        ownership: Ownership,
        state: PropertyState,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<OptStoredProp>, SqliteError> {
        let limit = i64::try_from(limit).map_err(|_| SqliteError::Conversion(offset))?;
        let offset = i64::try_from(offset).map_err(|_| SqliteError::Conversion(offset))?;

        let ownership_par = RecordOwnership::from(ownership);

        let mut statement = self
            .prepare_cached(include_query!(
                "queries/properties/read/props_with_unset.sql"
            ))
            .map_err(SqliteError::Prepare)?;

        let v = statement
            .query_map(
                (
                    ownership_par,
                    RecordPropertyState::from(state),
                    limit,
                    offset,
                ),
                |row| {
                    Ok(StoredRecord {
                        interface: row.get(0)?,
                        path: row.get(1)?,
                        value: row.get(2)?,
                        stored_type: row.get(3)?,
                        interface_major: row.get(4)?,
                        ownership: row.get(5)?,
                    })
                },
            )
            .map_err(SqliteError::Query)?
            .map(|e| {
                e.map_err(SqliteError::Query).and_then(|record| {
                    let prop = OptStoredProp::try_from(record)?;

                    debug_assert_eq!(prop.ownership, ownership);

                    Ok(prop)
                })
            })
            .collect::<Result<Vec<OptStoredProp>, SqliteError>>()?;

        Ok(v)
    }

    pub(super) fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, SqliteError> {
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
    }
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
