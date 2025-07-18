// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

//! # Sqlite Options module
//!
//! This module provides structures to configure some sqlite options.
//! It defines the `SqliteStoreOptions` struct which holds the editable options.

use rusqlite::Connection;

use super::{
    get_pragma, set_pragma, Size, SqliteError, SQLITE_BUSY_TIMEOUT, SQLITE_CACHE_SIZE,
    SQLITE_DEFAULT_DB_MAX_SIZE, SQLITE_JOURNAL_SIZE_LIMIT, SQLITE_WAL_AUTOCHECKPOINT,
};

/// Sqlite options that can be set externally to tweak the behaviour of sqlite.
pub struct SqliteStoreOptions {
    // Maximum number of pages in the sqlite db file
    db_size_limit: Size,
    /// Since we use WAL mode the size of our journal controls the size of the WAL
    /// Without the autocheckpoint set the journal still grows until an autocheckpoint is performed.
    /// The size limit only applies when the WAL journal is truncated. We set both options to
    /// correctly limit the size of the WAL file.
    journal_size_limit: Size,
}

impl Default for SqliteStoreOptions {
    fn default() -> Self {
        Self {
            db_size_limit: SQLITE_DEFAULT_DB_MAX_SIZE,
            journal_size_limit: SQLITE_JOURNAL_SIZE_LIMIT,
        }
    }
}

/// Sqlite pragmas that should be persisted across connections.
///
/// These pragmas are not persisted across connections so we need to set them every time
/// we create a new connection.
#[derive(Clone, Debug)]
pub(crate) struct SqlitePragmas {
    /// Maximum number of pages in the sqlite db file
    max_page_count: u32,
    /// Maximum size of the sqlite WAL journal   
    journal_size_limit: u64,
    /// Limit of number of pages to wait before committing the WAL
    wal_autocheckpoint: u32,
}

impl SqlitePragmas {
    pub(crate) fn try_from_options(
        connection: &Connection,
        options: SqliteStoreOptions,
    ) -> Result<Self, SqliteError> {
        let max_page_count = Self::compute_max_page_count(connection, options.db_size_limit)?;
        let wal_autocheckpoint =
            Self::compute_autocheckpoint_pages(connection, options.journal_size_limit)?;

        Ok(Self {
            max_page_count,
            journal_size_limit: options.journal_size_limit.to_bytes(),
            wal_autocheckpoint,
        })
    }

    pub(crate) fn compute_max_page_count(
        connection: &Connection,
        max_size: Size,
    ) -> Result<u32, SqliteError> {
        let page_size: u64 = get_pragma(connection, "page_size")?;

        // perform euclidean division to retrieve the correct number of pages
        // no need to perform checked div since the minimum page size is 512 bytes
        // <https://www.sqlite.org/pragma.html#pragma_page_size>
        Ok(max_size.calculate_max_page_count(page_size))
    }

    /// Applites max pages database limit to the passsed connection
    ///
    /// <https://www.sqlite.org/pragma.html#pragma_max_page_count>
    fn apply_max_page_count(
        connection: &Connection,
        max_page_count: u32,
    ) -> Result<(), SqliteError> {
        set_pragma(connection, "max_page_count", max_page_count)
    }

    pub(crate) fn set_max_page_count(
        &mut self,
        connection: &Connection,
        max: u32,
    ) -> Result<(), SqliteError> {
        if max == 0 {
            return Err(SqliteError::InvalidMaxSize {
                ctx: "max page count cannot be 0",
            });
        }

        // check if the number of pages provided in input is the same as the maximum one
        let current_max: u32 = get_pragma(connection, "max_page_count")?;

        if max == current_max {
            return Ok(());
        }

        // check if the new database size is lower than the actual one
        let current_pages: u32 = get_pragma(connection, "page_count")?;

        if max < current_pages {
            return Err(SqliteError::InvalidMaxSize {
                ctx: "cannot shrink the database",
            });
        }

        Self::apply_max_page_count(connection, max)?;
        self.max_page_count = max;

        Ok(())
    }

    fn compute_autocheckpoint_pages(
        connection: &Connection,
        journal_size_limit: Size,
    ) -> Result<u32, SqliteError> {
        let page_size: u64 = get_pragma(connection, "page_size")?;
        // we want a number of pages for autocheckpointing currently we divide by 10
        // to get a sensible number lower than the size limit
        // we also set a maximum of 1000 to avoid exceeding the default value for sqlite
        Ok(journal_size_limit
            .calculate_max_page_count(page_size)
            .div_euclid(10)
            .min(SQLITE_WAL_AUTOCHECKPOINT))
    }

    /// Applies journal size limit pragmas for the current database connection.
    ///
    /// Sets also the wal autocheckpoint to a value of pages lower than the
    /// size limit (in pages) so that the commit happens before surpassing the size.
    ///
    /// <https://www.sqlite.org/pragma.html#pragma_journal_size_limit>
    /// <https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint>
    fn apply_journal_size_limit(
        connection: &Connection,
        journal_size_limit: u64,
        wal_autocheckpoint: u32,
    ) -> Result<(), SqliteError> {
        set_pragma(connection, "journal_size_limit", journal_size_limit)?;
        set_pragma(connection, "wal_autocheckpoint", wal_autocheckpoint)?;

        Ok(())
    }

    pub(crate) fn set_journal_size_limit(
        &mut self,
        connection: &Connection,
        journal_size_limit: Size,
    ) -> Result<(), SqliteError> {
        let wal_autocheckpoint =
            Self::compute_autocheckpoint_pages(connection, journal_size_limit)?;
        let journal_size_limit = journal_size_limit.to_bytes();

        Self::apply_journal_size_limit(connection, journal_size_limit, wal_autocheckpoint)?;

        self.journal_size_limit = journal_size_limit;
        self.wal_autocheckpoint = wal_autocheckpoint;

        Ok(())
    }

    pub(crate) fn apply_pragmas(&self, connection: &Connection) -> Result<(), SqliteError> {
        Self::apply_max_page_count(connection, self.max_page_count)?;
        Self::apply_journal_size_limit(
            connection,
            self.journal_size_limit,
            self.wal_autocheckpoint,
        )?;
        set_pragma(connection, "foreign_keys", true)?;
        set_pragma(connection, "busy_timeout", SQLITE_BUSY_TIMEOUT)?;
        set_pragma(connection, "synchronous", "NORMAL")?;
        // Reduces the size of the database
        set_pragma(connection, "auto_vacuum", "INCREMENTAL")?;
        set_pragma(connection, "temp_store", "MEMORY")?;
        set_pragma(connection, "cache_size", SQLITE_CACHE_SIZE)?;
        set_pragma(connection, "journal_mode", "WAL")?;

        Ok(())
    }
}
