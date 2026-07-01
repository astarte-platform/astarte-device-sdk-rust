// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
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

//! Error returned by the [`SqliteStore`](super::SqliteStore).

use std::fmt::Display;

/// Error returned by the [`SqliteStore`](super::SqliteStore).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SqliteError {
    /// Error returned when the database connection fails.
    Connection,
    /// Couldn't set SQLite option.
    Option,
    /// Couldn't prepare the SQLite statement.
    Prepare,
    /// Couldn't start a transaction.
    Transaction,
    /// Couldn't run migration
    Migration,
    /// Error returned when the database query fails.
    Query,
    /// Couldn't convert the stored value.
    Value(ValueError),
    /// Couldn't convert ownership value
    Ownership,
    /// Couldn't set max size
    InvalidMaxSize,
    /// Couldn't acquire a reader permit
    Reader,
    /// Couldn't join the connection task
    Join,
    /// Couldn't convert passed input
    Conversion,
}

impl Display for SqliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection => write!(f, "could not connect to database"),
            Self::Option => write!(f, "couldn't set database option"),
            Self::Prepare => write!(f, "couldn't prepare sqlite statement"),
            Self::Transaction => write!(f, "could not start a transaction database"),
            Self::Migration => write!(f, "couldn't run migration"),
            Self::Query => write!(f, "could not execute query"),
            Self::Value(error) => write!(f, "couldn't convert the stored value {error}"),
            Self::Ownership => write!(f, "could not deserialize ownership"),
            Self::InvalidMaxSize => write!(f, "couldn't set max size"),
            Self::Reader => write!(f, "couldn't acquire a reader permit"),
            Self::Join => write!(f, "couldn't join the connection task"),
            Self::Conversion => write!(f, "couldn't convert passed input"),
        }
    }
}

/// Error when de/serializing a value stored in the [`SqliteStore`](super::SqliteStore).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueError {
    /// Couldn't convert to AstarteData.
    Conversion,
    /// Couldn't decode the BSON buffer.
    Decode,
    /// Couldn't encode the BSON buffer.
    Encode,
    /// Unsupported AstarteData.
    UnsupportedType,
    /// Unsupported AstarteData.
    StoredType,
}

impl Display for ValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conversion => write!(f, "couldn't convert to AstarteData"),
            Self::Decode => write!(f, "couldn't decode property from bson"),
            Self::Encode => write!(f, "couldn't encode property from bson"),
            Self::UnsupportedType => write!(f, "unsupported property type"),
            Self::StoredType => write!(f, "unsupported stored type, expected [1-14]"),
        }
    }
}
