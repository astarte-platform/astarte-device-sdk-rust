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

use rusqlite::ToSql;

use crate::session::IntrospectionInterface;
use crate::store::sqlite::connection::{ReadConnection, WriteConnection};
use crate::store::sqlite::{SqliteError, statements::include_query};

impl WriteConnection {
    pub(crate) fn add_interfaces<S>(
        &mut self,
        interfaces: &[IntrospectionInterface<S>],
    ) -> Result<(), SqliteError>
    where
        S: ToSql,
    {
        let trn = self.transaction()?;

        {
            let mut statement = trn
                .prepare_cached(include_query!(
                    "queries/session/write/store_introspection.sql"
                ))
                .map_err(SqliteError::Prepare)?;

            for i in interfaces {
                statement.execute((i.name(), i.version_major(), i.version_minor()))?;
            }
        }

        trn.commit()?;

        Ok(())
    }

    pub(crate) fn clear_introspection(&self) -> Result<(), SqliteError> {
        let mut statement = self.prepare_cached(include_query!(
            "queries/session/write/clear_introspection.sql"
        ))?;

        statement.execute(())?;

        Ok(())
    }

    pub(crate) fn remove_interfaces(
        &mut self,
        interfaces: &[IntrospectionInterface],
    ) -> Result<(), SqliteError> {
        let trn = self.transaction()?;

        {
            let mut statement =
                trn.prepare_cached(include_query!("queries/session/write/remove_interface.sql"))?;

            for i in interfaces {
                statement.execute((i.name(), i.version_major(), i.version_minor()))?;
            }
        }

        trn.commit()?;

        Ok(())
    }
}

impl ReadConnection {
    pub(crate) fn load_introspection(&self) -> Result<Vec<IntrospectionInterface>, SqliteError> {
        let mut statement = self
            .prepare_cached(include_query!(
                "queries/session/read/load_introspection.sql"
            ))
            .map_err(SqliteError::Prepare)?;

        let interfaces = statement
            .query_map([], |row| {
                Ok(IntrospectionInterface {
                    name: row.get(0)?,
                    version_major: row.get(1)?,
                    version_minor: row.get(2)?,
                })
            })
            .map_err(SqliteError::Query)?
            .collect::<Result<Vec<IntrospectionInterface>, rusqlite::Error>>()
            .map_err(SqliteError::Query)?;

        Ok(interfaces)
    }
}
