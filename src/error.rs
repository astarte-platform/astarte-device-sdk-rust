// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Error types for the Astarte SDK.

use std::convert::Infallible;

use crate::interface::error::ValidationError;
use crate::interface::mapping::path::Error as MappingError;
use crate::interface::Error as InterfaceError;
use crate::options::Error as OptionsError;
use crate::properties::Error as PropertiesError;
use crate::store::error::Error as StoreError;
use crate::topic::Error as TopicError;

/// Astarte error.
///
/// Possible errors returned by functions of the Astarte device SDK.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("bson serialize error")]
    BsonSerError(#[from] bson::ser::Error),

    #[error("bson client error")]
    BsonClientError(#[from] rumqttc::ClientError),

    #[error("mqtt connection error")]
    ConnectionError(#[from] rumqttc::ConnectionError),

    #[error("malformed input from Astarte backend")]
    DeserializationError(#[from] bson::de::Error),

    #[error("malformed input from Astarte backend, missing value 'v' in document: {0}")]
    DeserializationMissingValue(bson::Document),

    #[error("error converting from Bson to AstarteType ({0})")]
    FromBsonError(String),

    #[error("type mismatch in bson array from astarte, something has gone very wrong here")]
    FromBsonArrayError,

    #[error("forbidden floating point number")]
    FloatError,

    #[error("send error ({0})")]
    SendError(String),

    #[error("receive error ({0})")]
    ReceiveError(String),

    #[error("options error")]
    OptionsError(#[from] OptionsError),

    #[error(transparent)]
    InterfaceError(#[from] InterfaceError),

    #[error("generic error ({0})")]
    Reported(String),

    #[error("generic error")]
    Unreported,

    #[error("conversion error")]
    Conversion,

    #[error("infallible error")]
    Infallible(#[from] Infallible),

    #[error("invalid topic {}",.0.topic())]
    InvalidTopic(#[from] TopicError),

    #[error("invalid mapping path '{}'", .0.path())]
    InvalidEndpoint(#[from] MappingError),

    #[error("invalid interface added")]
    InvalidInterface(#[from] ValidationError),

    /// Error while parsing the /control/consumer/properties payload.
    #[error("failed to parse consumer/producer properties")]
    PurgeProperty(#[from] PropertiesError),

    /// Wraps a generic [`StoreError`] from the storage implementation.
    #[error(transparent)]
    Database(#[from] StoreError),
}
