// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use astarte_device_error::{Error, WrapError};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tracing::{error, instrument, trace, warn};

use crate::error::Report;
use crate::store::sqlite::connection::SqliteConnection;

use super::SqliteError;
use super::connection::{ReadConnection, WriteConnection};
use super::options::SqliteOptions;

type HandleResult<C, O, E> = Result<(C, Result<O, E>), E>;

#[derive(Debug)]
pub(crate) struct Connections {
    db_file: Arc<Path>,
    options: RwLock<SqliteOptions>,
    writer: Mutex<Option<WriteConnection>>,
    reader_sem: Semaphore,
    /// Use a FIFO queue for the connections to cycle through them all.
    readers: Mutex<VecDeque<ReadConnection>>,
}

impl Connections {
    /// Create a new connection queue
    pub(crate) fn new(db_file: PathBuf, options: SqliteOptions) -> Self {
        let readers = options.max_readers();

        Self {
            db_file: db_file.into(),
            options: RwLock::new(options),
            writer: Mutex::new(None),
            reader_sem: Semaphore::new(readers.get()),
            readers: Mutex::new(VecDeque::with_capacity(readers.get())),
        }
    }

    /// Acquire the write connection to the database.
    ///
    /// It will call the closure in a [`tokio::task::spawn_blocking`] so all the SQLite operation
    /// will not block the runtime.
    #[instrument(skip_all, fields(db = %self.db_file.display()))]
    pub(crate) async fn acquire_writer<F, O>(&self, f: F) -> Result<O, Error<SqliteError>>
    where
        F: FnOnce(&mut WriteConnection) -> Result<O, Error<SqliteError>> + Send + 'static,
        O: Send + 'static,
    {
        trace!("locking writer connection");

        let mut writer_g = self.writer.lock().await;

        trace!("writer connection acquired");

        let writer = writer_g.take();
        let db_file = Arc::clone(&self.db_file);
        let options = { *self.options.read().await };

        // this need to be a spawn blocking to both support single and multi threaded runtimes
        let (writer, out) = tokio::task::spawn_blocking(
            move || -> HandleResult<WriteConnection, O, Error<SqliteError>> {
                let mut writer = WriteConnection::lazy(writer, &db_file, &options)?;

                let out = (f)(&mut writer);

                Ok((writer, out))
            },
        )
        .await
        .wrap_err_with(|err| {
            error!(error = %Report::new(err), "couldn't join sqlite task");

            Error::new(SqliteError::Join)
        })??;

        trace!("restoring writer connection");

        writer_g.replace(writer);

        out
    }

    /// Acquire one of the reader connection to the database.
    ///
    /// It will call the closure in a [`tokio::task::spawn_blocking`] so all the SQLite operation
    /// will not block the runtime.
    #[instrument(skip_all, fields(db = %self.db_file.display()))]
    pub(crate) async fn acquire_reader<F, O>(&self, f: F) -> Result<O, Error<SqliteError>>
    where
        F: FnOnce(&mut ReadConnection) -> Result<O, Error<SqliteError>> + Send + 'static,
        O: Send + 'static,
    {
        trace!("acquiring reader permit");

        let _permit = self
            .reader_sem
            .acquire()
            .await
            .wrap_err(SqliteError::Reader)?;

        trace!("reader permit acquired");

        // The number of connection is less than or equal to the number of permits. We pop the
        // interface and if the collection is empty, we need to add another one.
        let reader = { self.readers.lock().await.pop_front() };
        let db_file = Arc::clone(&self.db_file);
        let options = { *self.options.read().await };

        // this need to be a spawn blocking to both support single and multi threaded runtimes
        let (reader, out) = tokio::task::spawn_blocking(
            move || -> HandleResult<ReadConnection, O, Error<SqliteError>> {
                let mut reader = ReadConnection::lazy(reader, &db_file, &options)?;

                let out = (f)(&mut reader);

                Ok((reader, out))
            },
        )
        .await
        .wrap_err_with(|err| {
            error!(error = %Report::new(err), "couldn't join sqlite task");

            Error::new(SqliteError::Join)
        })??;

        {
            self.readers.lock().await.push_back(reader)
        }

        out
    }
}

// Drop to manually close all the readers before the writer.
impl Drop for Connections {
    #[instrument(skip_all, fields(db = %self.db_file.display()))]
    fn drop(&mut self) {
        trace!("pool dropped, closing the connections");

        let readers = std::mem::take(self.readers.get_mut());
        let writer = self.writer.get_mut().take();

        tokio::task::spawn_blocking(move || {
            for reader in readers {
                reader.close();
            }

            if let Some(writer) = writer {
                writer.close();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn should_survive_panic() {
        let tpm = TempDir::new().unwrap();

        let pool = Connections::new(tpm.path().join("sdk.db"), SqliteOptions::default());

        let res = pool
            .acquire_writer::<_, ()>(|_writer| panic!())
            .await
            .unwrap_err();

        assert_eq!(*res.kind(), SqliteError::Join);

        let pool = Connections::new(tpm.path().join("sdk.db"), SqliteOptions::default());

        let res = pool
            .acquire_reader::<_, ()>(|_reader| panic!())
            .await
            .unwrap_err();

        assert_eq!(*res.kind(), SqliteError::Join);
    }
}
