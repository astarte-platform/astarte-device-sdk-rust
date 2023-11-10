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

use crate::builder::BuilderError;
use crate::interface::mapping::path::MappingError;
use crate::interface::{Aggregation, InterfaceError, InterfaceTypeDef};
use crate::properties::PropertiesError;
use crate::store::error::StoreError;
use crate::topic::TopicError;
use crate::transport::mqtt::{payload::PayloadError, MqttConnectionError};
use crate::types::TypeError;
use crate::validate::UserValidationError;

/// Astarte error.
///
/// Possible errors returned by functions of the Astarte device SDK.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[deprecated = "The error is unused and will be removed in a future version"]
    #[error("bson client error")]
    BsonClientError(#[from] rumqttc::ClientError),

    #[error("mqtt connection error")]
    ConnectionError(#[from] rumqttc::ConnectionError),

    /// The connection poll reached the max number of retries.
    #[error("mqtt connection reached max retries")]
    ConnectionTimeout,

    #[deprecated = "The error is unused and will be removed in a future version"]
    #[error("send error ({0})")]
    SendError(String),

    #[error("receive error ({0})")]
    ReceiveError(String),

    #[error("options error")]
    Builder(#[from] BuilderError),

    #[error("invalid interface")]
    Interface(#[from] InterfaceError),

    #[deprecated = "The error is unused and will be removed in a future version"]
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
    #[error("couldn't convert to Astarte Type")]
    Types(#[from] TypeError),

    /// Errors that can occur handling the payload.
    #[error("couldn't process payload")]
    Payload(#[from] PayloadError),

    /// Error while parsing the /control/consumer/properties payload.
    #[error("couldn't handle properties")]
    Properties(#[from] PropertiesError),

    /// Error returned by a store operation.
    #[error("couldn't complete store operation")]
    Database(#[from] StoreError),

    /// Error missing interface
    #[error("couldn't find the interface {0}")]
    MissingInterface(String),

    /// Error missing mapping in interface
    #[error("couldn't find mapping {mapping} in interface {interface}")]
    MissingMapping { interface: String, mapping: String },

    /// Send or receive validation failed
    #[error("validation of the send payload failed")]
    Validation(#[from] UserValidationError),

    #[error("invalid aggregation, expected {exp} but got {got}")]
    Aggregation { exp: Aggregation, got: Aggregation },

    #[error("invalid interface type, expected {exp} but got {got}")]
    InterfaceType {
        exp: InterfaceTypeDef,
        got: InterfaceTypeDef,
    },

    #[error(transparent)]
    MqttConnection(#[from] MqttConnectionError),
}
