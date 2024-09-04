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

use crate::interface::error::InterfaceError;
use crate::interface::mapping::path::MappingError;
use crate::interface::{Aggregation, InterfaceTypeDef};
use crate::introspection::AddInterfaceError;
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
    /// Error while parsing interface.
    #[error("couldn't parse interface")]
    Interface(#[from] InterfaceError),
    /// Error while operating on the device introspection.
    #[error("couldn't complete introspection operation")]
    AddInterface(#[from] AddInterfaceError),
    /// Invalid interface type when sending or receiving.
    #[error("invalid interface type, expected {exp} but got {got}")]
    InterfaceType {
        /// Expected interface type.
        exp: InterfaceTypeDef,
        /// Actual interface type.
        got: InterfaceTypeDef,
    },
    /// Couldn't find an interface with the given name.
    #[error("couldn't find interface '{name}'")]
    InterfaceNotFound {
        /// Name of the missing interface.
        name: String,
    },
    /// Couldn't find missing mapping in the interface.
    #[error("couldn't find mapping {mapping} in interface {interface}")]
    MappingNotFound {
        /// Name of the interface.
        interface: String,
        /// Path of the missing mapping.
        mapping: String,
    },
    /// Couldn't parse the mapping path.
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
    /// Invalid aggregation between the interface and the data.
    #[error("invalid aggregation, expected {exp} but got {got}")]
    Aggregation {
        /// Expected aggregation of the interface.
        exp: Aggregation,
        /// The actual aggregation.
        got: Aggregation,
    },
    /// Infallible conversion.
    #[error(transparent)]
    Infallible(#[from] Infallible),
    /// Error returned by the MQTT connection.
    #[error(transparent)]
    Mqtt(#[from] MqttError),
    /// Error when the Device is disconnected from Astarte or client.
    ///
    /// This is an unrecoverable error for the SDK.
    #[error("disconnected from astarte")]
    Disconnected,
    /// Error returned by the gRPC transport
    #[cfg(feature = "message-hub")]
    #[cfg_attr(docsrs, doc(cfg(feature = "message-hub")))]
    #[error(transparent)]
    Grpc(#[from] crate::transport::grpc::GrpcError),
}
