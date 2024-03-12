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
use crate::interface::error::InterfaceFileError;
use crate::interface::mapping::path::MappingError;
use crate::interface::{error::InterfaceError, Aggregation, InterfaceTypeDef};
use crate::properties::PropertiesError;
use crate::store::error::StoreError;
use crate::transport::mqtt::error::MqttError;
use crate::types::TypeError;
use crate::validate::UserValidationError;

/// Astarte error.
///
/// Possible errors returned by functions of the Astarte device SDK.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// The connection poll reached the max number of retries.
    #[error("connection reached max retries")]
    ConnectionTimeout,
    /// Error returned by the builder
    #[error("options error")]
    Builder(#[from] BuilderError),
    /// Failed to parse the interface
    #[error("couldn't add interface")]
    Interface(#[from] InterfaceError),
    /// Failed to parse interface file
    #[error("couldn't parse interface file")]
    InterfaceFile(#[from] InterfaceFileError),
    /// Invalid interface type when sending or receiving
    #[error("invalid interface type, expected {exp} but got {got}")]
    InterfaceType {
        exp: InterfaceTypeDef,
        got: InterfaceTypeDef,
    },
    /// Couldn't find an interface with the given name.
    #[error("couldn't find interface '{name}'")]
    InterfaceNotFound { name: String },
    /// Error missing mapping in interface
    #[error("couldn't find mapping {mapping} in interface {interface}")]
    MappingNotFound { interface: String, mapping: String },
    /// Couldn't parse the mapping path
    #[error("invalid mapping path '{}'", .0.path())]
    InvalidEndpoint(#[from] MappingError),
    /// Errors when converting between Astarte types.
    #[error("couldn't convert to Astarte Type")]
    Types(#[from] TypeError),
    /// Error while parsing the /control/consumer/properties payload.
    #[error("couldn't handle properties")]
    Properties(#[from] PropertiesError),
    /// Error returned by a store operation.
    #[error("couldn't complete store operation")]
    Store(#[from] StoreError),
    /// Send or receive validation failed
    #[error("validation of the send payload failed")]
    Validation(#[from] UserValidationError),
    #[error("invalid aggregation, expected {exp} but got {got}")]
    Aggregation { exp: Aggregation, got: Aggregation },
    /// Infallible conversion
    #[error(transparent)]
    Infallible(#[from] Infallible),
    /// Error returned by the MQTT connection
    #[error(transparent)]
    Mqtt(#[from] MqttError),
    /// Error returned by the GRpc transport
    #[cfg(feature = "message-hub")]
    #[error(transparent)]
    Grpc(#[from] crate::transport::grpc::GrpcError),
}
