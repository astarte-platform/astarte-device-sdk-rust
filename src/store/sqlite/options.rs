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

use std::num::NonZero;

use serde::{Deserialize, Serialize};
use tracing::{error, instrument, trace, warn};

use crate::error::Report;

use super::connection::SqliteConnection;
use super::{
    DEFAULT_MAX_READERS, SQLITE_DEFAULT_DB_MAX_SIZE, SQLITE_JOURNAL_SIZE_LIMIT, Size, SqliteError,
};

/// Choices of limit of the size of the sqlite database
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SizeLimit {
    /// Set a size limit based on the number of pages
    MaxPageCount(NonZero<u32>),
    /// Set a size limit based on the actual size of the db file
    ///
    /// This value will be approximated if it's not a multiple of the database page size.
    DbMaxSize(Size),
}

/// SQLite options that can be set externally to tweak the behaviour of the connections.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub(crate) struct SqliteOptions {
    // Maximum number of read connection to open
    max_readers: Option<NonZero<usize>>,
    // Maximum size of the database file.
    //
    // This can be in pages or an actual file size.
    #[serde(flatten)]
    db_size_limit: Option<SizeLimit>,
    /// Maximum size of the journal file.
    ///
    /// Since we use WAL mode the size of our journal controls the size of the WAL Without the
    /// autocheckpoint set the journal still grows until an autocheckpoint is performed. The size
    /// limit only applies when the WAL journal is truncated. We set both options to correctly limit
    /// the size of the WAL file.
    journal_size_limit: Option<Size>,
}

impl SqliteOptions {
    /// Get the max number of readers.
    ///
    /// If the value is not set, it uses [`std::thread::available_parallelism`]  defaulting to
    /// [`DEFAULT_MAX_READERS`] on error.
    ///
    /// This is the max number of readers and they are lazily created, so this number it's safe to
    /// be more than the actual available_parallelism.
    #[instrument]
    pub(crate) fn max_readers(&self) -> NonZero<usize> {
        match self.max_readers {
            Some(readers) => readers,
            None => {
                std::thread::available_parallelism().unwrap_or_else(|err| {
                    error!(err = %Report::new(err), "couldn't get available_parallelism, defaulting to {DEFAULT_MAX_READERS}");

                    DEFAULT_MAX_READERS
                })
            }
        }
    }

    /// Returns the db_size_limit or the default one
    pub(crate) fn db_size_limit(&self) -> SizeLimit {
        self.db_size_limit
            .unwrap_or(SizeLimit::DbMaxSize(SQLITE_DEFAULT_DB_MAX_SIZE))
    }

    /// Returns the journal_size_limit or the default one
    fn journal_size_limit(&self) -> Size {
        self.journal_size_limit.unwrap_or(SQLITE_JOURNAL_SIZE_LIMIT)
    }

    /// Sets the database size limit
    pub(crate) fn set_db_max_size(&mut self, db_size_limit: Size) {
        self.db_size_limit = Some(SizeLimit::DbMaxSize(db_size_limit));
    }

    /// Sets the database size limit
    pub(crate) fn set_max_page_count(&mut self, max_page_count: NonZero<u32>) {
        self.db_size_limit = Some(SizeLimit::MaxPageCount(max_page_count));
    }

    /// Sets the journal size limit
    pub(crate) fn set_journal_size_limit(&mut self, journal_size_limit: Size) {
        self.journal_size_limit = Some(journal_size_limit);
    }
}

/// Sqlite pragmas that should be persisted across connections.
///
/// These pragmas are not persisted across connections so we need to set them every time
/// we create a new connection.
#[derive(Clone, Debug)]
pub(crate) struct SqlitePragmas {
    /// Maximum number of pages in the sqlite db file
    pub(crate) max_page_count: NonZero<u32>,
    /// Maximum size of the SQLite WAL journal
    pub(crate) journal_size_limit: NonZero<i64>,
    /// Limit of number of pages to wait before committing the WAL
    pub(crate) wal_autocheckpoint: NonZero<u32>,
}

impl SqlitePragmas {
    #[instrument(skip_all)]
    pub(crate) fn try_from_options<C>(
        connection: &C,
        options: &SqliteOptions,
    ) -> Result<Self, SqliteError>
    where
        C: SqliteConnection,
    {
        let page_size = connection
            .get_pragma::<i64>("page_size")
            .ok()
            .and_then(|page_size: i64| u64::try_from(page_size).ok())
            .and_then(NonZero::<u64>::new)
            .ok_or(SqliteError::InvalidMaxSize {
                ctx: "couldn't read the db page size (0)",
            })?;

        trace!(%page_size, "pragma page_size");

        let max_page_count = match options.db_size_limit() {
            SizeLimit::MaxPageCount(pages) => pages,
            SizeLimit::DbMaxSize(size) => size.into_page_count(page_size),
        };

        let journal_size_limit = options.journal_size_limit();

        let wal_autocheckpoint = journal_size_limit.into_wall_autocheckpoint(page_size);

        let journal_size_limit =
            journal_size_limit
                .to_bytes()
                .try_into()
                .map_err(|_| SqliteError::InvalidMaxSize {
                    ctx: "couldn't calculate journal si limit",
                })?;

        let pragma = Self {
            max_page_count,
            journal_size_limit,
            wal_autocheckpoint,
        };

        pragma.validate(connection)
    }

    /// Check if the PRAGMA are valid
    ///
    /// # Errors
    ///
    /// If the page count is less than the current page_count this call will fail.
    #[instrument(skip_all)]
    fn validate<C>(self, connection: &C) -> Result<Self, SqliteError>
    where
        C: SqliteConnection,
    {
        // check if the new database size is lower than the actual one
        let current_pages: u32 = connection.get_pragma("page_count")?;

        if self.max_page_count.get() < current_pages {
            return Err(SqliteError::InvalidMaxSize {
                ctx: "cannot shrink the database",
            });
        }

        Ok(self)
    }
}
