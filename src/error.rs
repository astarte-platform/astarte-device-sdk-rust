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

use crate::interface::mapping::path::MappingError;
use crate::interface::InterfaceError;
use crate::options::OptionsError;
use crate::payload::PayloadError;
use crate::properties::PropertiesError;
use crate::topic::TopicError;
use crate::types::TypeError;

/// Astarte error.
///
/// Possible errors returned by functions of the Astarte device SDK.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("bson client error")]
    BsonClientError(#[from] rumqttc::ClientError),

    #[error("mqtt connection error")]
    ConnectionError(#[from] rumqttc::ConnectionError),

    #[error("send error ({0})")]
    SendError(String),

    #[error("receive error ({0})")]
    ReceiveError(String),

    #[error("database error")]
    DbError(#[from] sqlx::Error),

    #[error("options error")]
    OptionsError(#[from] OptionsError),

    #[error("invalid interface")]
    Interface(#[from] InterfaceError),

    #[error("generic error ({0})")]
    Reported(String),

    #[error("generic error")]
    Unreported,

    #[error("infallible error")]
    Infallible(#[from] Infallible),

    #[error("invalid topic {}",.0.topic())]
    InvalidTopic(#[from] TopicError),

    #[error("invalid mapping path '{}'", .0.path())]
    InvalidEndpoint(#[from] MappingError),

    /// Errors when converting between Astarte types.
    #[error("couldn't process payload")]
    Types(#[from] TypeError),

    /// Errors that can occur handling the payload.
    #[error("couldn't process payload")]
    Payload(#[from] PayloadError),

    #[error("couldn't handle properties")]
    Properties(#[from] PropertiesError),
}
