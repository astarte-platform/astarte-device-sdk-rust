// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

use std::fmt::Display;
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use rusqlite::types::FromSql;
use rusqlite::{Connection, OpenFlags, OptionalExtension, ToSql};
use tracing::{debug, error, instrument, trace, warn};

use crate::builder::DEFAULT_STORE_CAPACITY;
use crate::error::Report;

use super::options::{SqliteOptions, SqlitePragmas};
use super::{SqliteError, SQLITE_BUSY_TIMEOUT, SQLITE_CACHE_SIZE};

#[cfg(feature = "sqlite-trace")]
/// Logs the execution of SQLite statements
#[tracing::instrument(name = "statement", skip_all)]
fn trace_sqlite(event: &str) {
    tracing::trace!("{event}");
}

pub(crate) trait SqliteConnection: Sized + Deref<Target = Connection> {
    const CONNECTION_TYPE: &'static str;

    fn connect(db_file: &Path, options: &SqliteOptions) -> Result<Self, SqliteError>;

    fn take_connection(self) -> Connection;

    #[instrument(skip_all)]
    fn close(self) {
        trace!("closing writer connection");

        if let Err((_, err)) = self.take_connection().close() {
            error!(error = %Report::new(err), connection=Self::CONNECTION_TYPE,  "couldn't close the connection");
        }
    }

    /// Queries a pragma value
    fn get_pragma<T>(&self, pragma_name: &str) -> Result<T, SqliteError>
    where
        T: FromSql + Display,
    {
        self.pragma_query_value(None, pragma_name, |row| row.get::<_, T>(0))
            .inspect(|value| trace!(pragma_name, %value, "pragma returned"))
            .map_err(SqliteError::Option)
    }

    /// Sets a pragma value
    #[instrument(skip_all, fields(connection = Self::CONNECTION_TYPE, %pragma_name, %pragma_value))]
    fn set_pragma<V>(&self, pragma_name: &str, pragma_value: &V) -> Result<(), SqliteError>
    where
        V: ToSql + ToOwned + PartialEq<V::Owned> + Display + ToOwned + ?Sized,
        V::Owned: FromSql + Display,
    {
        trace!("setting pragma");

        self.pragma_update_and_check(None, pragma_name, pragma_value, |r| {
            let Some(actual) = r.get::<_, V::Owned>(0).optional()? else {
                debug!("no pragma returned");

                return Ok(());
            };

            if *pragma_value != actual {
                error!(%actual, "couldn't set pragam");
            }

            Ok(())
        })
        .optional()
        .map_err(SqliteError::Option)?;

        Ok(())
    }

    /// Applies default and configured pragmas to the passed connection.
    ///
    /// Set journal size limit to the limit configured.
    /// Set also the wal autocheckpoint to a value of pages lower than the
    /// size limit (in pages) so that the commit happens before surpassing the size.
    ///
    /// <https://www.sqlite.org/pragma.html#pragma_journal_size_limit>
    /// <https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint>
    ///
    /// Applies also max pages database limit to the passsed connection
    ///
    /// <https://www.sqlite.org/pragma.html#pragma_max_page_count>
    fn apply_pragmas(&self, options: &SqliteOptions) -> Result<(), SqliteError> {
        let pragmas = SqlitePragmas::try_from_options(self, options)?;

        self.set_pragma("journal_mode", "wal")?;
        self.set_pragma("max_page_count", &pragmas.max_page_count)?;
        self.set_pragma("journal_size_limit", &pragmas.journal_size_limit)?;
        self.set_pragma("wal_autocheckpoint", &pragmas.wal_autocheckpoint)?;
        self.set_pragma("foreign_keys", &true)?;
        self.set_pragma("busy_timeout", &SQLITE_BUSY_TIMEOUT)?;
        self.set_pragma("synchronous", "NORMAL")?;
        self.set_pragma("auto_vacuum", "INCREMENTAL")?;
        self.set_pragma("temp_store", "MEMORY")?;
        self.set_pragma("cache_size", &SQLITE_CACHE_SIZE)?;

        Ok(())
    }

    #[instrument(skip_all, fields(connection = Self::CONNECTION_TYPE))]
    fn lazy(
        value: Option<Self>,
        db_file: &Path,
        options: &SqliteOptions,
    ) -> Result<Self, SqliteError> {
        value
            .map_or_else(
                || {
                    debug!("connection missing, creating it");

                    Self::connect(db_file, options)
                },
                Ok,
            )
            .inspect_err(|err| error!(error = %Report::new(err), "couldn't create connection"))
    }
}

#[derive(Debug)]
pub(crate) struct WriteConnection {
    connection: Connection,
    /// Maximum number of retention item to store
    pub(crate) retention_capacity: NonZeroUsize,
}

impl Deref for WriteConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for WriteConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl SqliteConnection for WriteConnection {
    const CONNECTION_TYPE: &'static str = "writer";

    fn connect(db_file: &Path, options: &SqliteOptions) -> Result<Self, SqliteError> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;

        let connection =
            Connection::open_with_flags(db_file, flags).map_err(SqliteError::Connection)?;

        #[cfg(feature = "sqlite-trace")]
        let mut connection = connection;

        #[cfg(feature = "sqlite-trace")]
        connection.trace(Some(trace_sqlite));

        let connection = Self {
            connection,
            retention_capacity: DEFAULT_STORE_CAPACITY,
        };

        connection.apply_pragmas(options)?;

        Ok(connection)
    }

    fn take_connection(self) -> Connection {
        self.connection
    }
}

#[derive(Debug)]
pub(crate) struct ReadConnection(Connection);

impl SqliteConnection for ReadConnection {
    const CONNECTION_TYPE: &'static str = "reader";

    fn connect(db_file: &Path, options: &SqliteOptions) -> Result<Self, SqliteError> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;

        let connection =
            Connection::open_with_flags(db_file, flags).map_err(SqliteError::Connection)?;

        #[cfg(feature = "sqlite-trace")]
        let mut connection = connection;
        #[cfg(feature = "sqlite-trace")]
        connection.trace(Some(trace_sqlite));

        let conn = Self(connection);

        conn.apply_pragmas(options)?;

        Ok(conn)
    }

    fn take_connection(self) -> Connection {
        self.0
    }
}

impl Deref for ReadConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
