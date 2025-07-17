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
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, error, instrument, trace};

use crate::builder::const_non_zero_usize;
use crate::error::Report;

use super::statements::{ReadConnection, WriteConnection};
use super::SqliteError;

const DEFAULT_MAX_READERS: NonZeroUsize = const_non_zero_usize(4);

type HandleResult<C, O, E> = Result<(C, Result<O, E>), E>;

#[derive(Debug)]
pub(crate) struct Connections {
    db_file: Arc<Path>,
    writer: Mutex<Option<WriteConnection>>,
    reader_sem: Semaphore,
    /// Use a FIFO queue for the connections to cycle through them all.
    readers: Mutex<VecDeque<ReadConnection>>,
}

impl Connections {
    /// Create a new connection queue
    ///
    /// It uses one writer and the function [`std::thread::available_parallelism`] for the number of
    /// readers, defaulting to 10 on error. This is the max number of readers and they are lazily
    /// created, so this number it's safe to be more than the actual available_parallelism.
    #[instrument]
    pub(crate) fn new(db_file: PathBuf) -> Self {
        let count = std::thread::available_parallelism().unwrap_or_else(|err| {
            error!(err = %Report::new(err), "couldn't get available_parallelism, defaulting to {DEFAULT_MAX_READERS}");

            DEFAULT_MAX_READERS
        });

        trace!(count, "setting max readers");

        Self::with_readers(db_file, count)
    }

    pub(crate) fn with_readers(db_file: PathBuf, readers: NonZeroUsize) -> Self {
        Self {
            db_file: db_file.into(),
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

        // this need to be a spawn blocking to both support single and multi threaded runtimes
        let (writer, out) =
            tokio::task::spawn_blocking(move || -> HandleResult<WriteConnection, O, E> {
                let mut writer = writer
                    .map_or_else(
                        || {
                            debug!("writer connection missing, creating it");

                            WriteConnection::connect(&db_file)
                        },
                        Ok,
                    )
                    .inspect_err(
                        |err| error!(error = %Report::new(err),"couldn't create connection"),
                    )?;

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

        // this need to be a spawn blocking to both support single and multi threaded runtimes
        let (reader, out) =
            tokio::task::spawn_blocking(move || -> HandleResult<ReadConnection, O, E> {
                let mut reader = reader
                    .map_or_else(
                        || {
                            debug!("reader connection missing, creating it");
                            ReadConnection::connect(&db_file)
                        },
                        Ok,
                    )
                    .inspect_err(
                        |err| error!(error = %Report::new(&err), "couldn't open reader connection"),
                    )?;

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
