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

use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock, Semaphore};
use tracing::{debug, error, instrument, trace, warn};

use crate::error::Report;
use crate::store::sqlite::connection::SqliteConnection;

use super::connection::{ReadConnection, WriteConnection};
use super::options::SqliteOptions;
use super::{Size, SqliteError};

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
    pub(crate) async fn acquire_writer<F, O, E>(&self, f: F) -> Result<O, E>
    where
        F: FnOnce(&mut WriteConnection) -> Result<O, E> + Send + 'static,
        O: Send + 'static,
        E: From<SqliteError> + Send + 'static,
    {
        trace!("locking writer connection");

        let mut writer_g = self.writer.lock().await;

        trace!("writer connection acquired");

        let writer = writer_g.take();
        let db_file = Arc::clone(&self.db_file);
        let options = { *self.options.read().await };

        // this need to be a spawn blocking to both support single and multi threaded runtimes
        let (writer, out) =
            tokio::task::spawn_blocking(move || -> HandleResult<WriteConnection, O, E> {
                let mut writer = WriteConnection::lazy(writer, &db_file, &options)?;

                let out = (f)(&mut writer);

                Ok((writer, out))
            })
            .await
            .map_err(|err| {
                error!(error = %Report::new(err), "couldn't join sqlite task");

                SqliteError::Join
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
    pub(crate) async fn acquire_reader<F, O, E>(&self, f: F) -> Result<O, E>
    where
        F: FnOnce(&mut ReadConnection) -> Result<O, E> + Send + 'static,
        O: Send + 'static,
        E: From<SqliteError> + Send + 'static,
    {
        trace!("acquiring reader permit");

        let _permit = self
            .reader_sem
            .acquire()
            .await
            .map_err(|_| SqliteError::Reader)?;

        trace!("reader permit acquired");

        // The number of connection is less than or equal to the number of permits. We pop the
        // interface and if the collection is empty, we need to add another one.
        let reader = { self.readers.lock().await.pop_front() };
        let db_file = Arc::clone(&self.db_file);
        let options = { *self.options.read().await };

        // this need to be a spawn blocking to both support single and multi threaded runtimes
        let (reader, out) =
            tokio::task::spawn_blocking(move || -> HandleResult<ReadConnection, O, E> {
                let mut reader = ReadConnection::lazy(reader, &db_file, &options)?;

                let out = (f)(&mut reader);

                Ok((reader, out))
            })
            .await
            .map_err(|err| {
                error!(error = %Report::new(err), "couldn't join sqlite task");

                SqliteError::Join
            })??;

        {
            self.readers.lock().await.push_back(reader)
        }

        out
    }

    // FIXME: remove in next release.
    pub(crate) async fn set_max_page_count(&self, count: NonZeroU32) -> Result<(), SqliteError> {
        let mut writer_g = self.writer.lock().await;
        let mut readers_g = self.readers.lock().await;

        let mut options_g = self.options.write().await;
        let mut options = *options_g;

        options.set_max_page_count(count);

        apply_options(&mut writer_g, &mut readers_g, options).await?;

        *options_g = options;

        Ok(())
    }

    // FIXME: remove in next release.
    pub(crate) async fn set_db_max_size(&self, size: Size) -> Result<(), SqliteError> {
        let mut writer_g = self.writer.lock().await;
        let mut readers_g = self.readers.lock().await;

        let mut options_g = self.options.write().await;
        let mut options = *options_g;

        options.set_db_max_size(size);

        apply_options(&mut writer_g, &mut readers_g, options).await?;

        *options_g = options;

        Ok(())
    }

    // FIXME: remove in next release.
    pub(crate) async fn set_journal_size_limit(&self, size: Size) -> Result<(), SqliteError> {
        let mut writer_g = self.writer.lock().await;
        let mut readers_g = self.readers.lock().await;

        let mut options_g = self.options.write().await;
        let mut options = *options_g;

        options.set_journal_size_limit(size);

        apply_options(&mut writer_g, &mut readers_g, options).await?;

        *options_g = options;

        Ok(())
    }
}

#[instrument(skip(writer_g, readers_g))]
async fn apply_options(
    writer_g: &mut Option<WriteConnection>,
    readers_g: &mut VecDeque<ReadConnection>,
    options: SqliteOptions,
) -> Result<(), SqliteError> {
    trace!("applyign options");

    if let Some(writer) = writer_g.take() {
        let writer =
            tokio::task::spawn_blocking(move || -> Result<WriteConnection, SqliteError> {
                writer.apply_pragmas(&options)?;

                Ok(writer)
            })
            .await
            .map_err(|err| {
                error!(error = %Report::new(err), "couldn't join sqlite task");

                SqliteError::Join
            })??;

        writer_g.replace(writer);

        debug!("writer options applyed");
    } else {
        trace!("writer not connectd");
    }

    let len = readers_g.len();
    for i in 0..len {
        trace!(i, "applying to reader");

        let Some(reader) = readers_g.pop_front() else {
            warn!(i, "no more readers");

            break;
        };

        let reader = tokio::task::spawn_blocking(move || -> Result<ReadConnection, SqliteError> {
            reader.apply_pragmas(&options)?;

            Ok(reader)
        })
        .await
        .map_err(|err| {
            error!(error = %Report::new(err), "couldn't join sqlite task");

            SqliteError::Join
        })??;

        readers_g.push_back(reader);

        debug!(i, "reader options applyed");
    }

    Ok(())
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
    async fn should_survice_panic() {
        let tpm = TempDir::new().unwrap();

        let pool = Connections::new(tpm.path().join("sdk.db"), SqliteOptions::default());

        let res: Result<(), SqliteError> = pool.acquire_writer(|_writer| panic!()).await;

        assert!(matches!(res.unwrap_err(), SqliteError::Join));

        let pool = Connections::new(tpm.path().join("sdk.db"), SqliteOptions::default());

        let res: Result<(), SqliteError> = pool.acquire_reader(|_reader| panic!()).await;

        assert!(matches!(res.unwrap_err(), SqliteError::Join));
    }

    #[tokio::test]
    async fn apply_options() {
        let tpm = TempDir::new().unwrap();

        let pool = Connections::new(tpm.path().join("sdk.db"), SqliteOptions::default());

        // create connections
        pool.acquire_writer::<_, _, SqliteError>(|_writer| Ok(()))
            .await
            .unwrap();
        pool.acquire_reader::<_, _, SqliteError>(|_reader| Ok(()))
            .await
            .unwrap();

        pool.set_max_page_count(NonZeroU32::new(42).unwrap())
            .await
            .unwrap();
    }
}
