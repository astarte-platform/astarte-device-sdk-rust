// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
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

//! Error types for the Astarte SDK.

use std::convert::Infallible;
use std::fmt::{Display, Formatter};

use astarte_interfaces::error::Error as InterfaceError;
use astarte_interfaces::mapping::path::MappingPathError;
use astarte_interfaces::schema::{Aggregation, InterfaceType, Ownership};

use crate::introspection::AddInterfaceError;
use crate::properties::PropertiesError;
use crate::retention::RetentionError;
use crate::session::SessionError;
use crate::store::error::StoreError;
use crate::transport::mqtt::error::MqttError;
use crate::types::TypeError;
use crate::validate::UserValidationError;

/// Dynamic error type
pub(crate) type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

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
    #[error("invalid mapping path")]
    InvalidEndpoint(#[from] MappingPathError),
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
    #[error(transparent)]
    Aggregation(#[from] AggregationError),
    /// Invalid interface type between the interface and the data.
    #[error(transparent)]
    InterfaceType(#[from] InterfaceTypeError),
    /// Infallible conversion.
    #[error(transparent)]
    Infallible(#[from] Infallible),
    /// Error returned by the MQTT connection.
    #[error(transparent)]
    Mqtt(#[from] MqttError),
    /// Error when the Device is disconnected from Astarte or client.
    ///
    /// This is an unrecoverable error for the SDK.
    #[error("disconnected from Astarte")]
    Disconnected,
    /// Retention operation failed.
    #[error("retention operation failed")]
    Retention(#[from] RetentionError),
    /// Persistent session operation failed
    #[error("persistent session operation failed")]
    Session(#[from] SessionError),
    /// Error returned by the gRPC transport
    #[cfg(feature = "message-hub")]
    #[cfg_attr(docsrs, doc(cfg(feature = "message-hub")))]
    #[error(transparent)]
    Grpc(#[from] crate::transport::grpc::GrpcError),
}

/// Aggregation error.
///
/// This provides additional context in case of an aggregation error
#[derive(Debug, thiserror::Error)]
#[error("invalid aggregation for {interface}{path}, expected {exp} but got {got}")]
#[non_exhaustive]
pub struct AggregationError {
    /// Interface name
    interface: String,
    /// Path
    path: String,
    /// Expected aggregation of the interface.
    exp: Aggregation,
    /// The actual aggregation.
    got: Aggregation,
}

impl AggregationError {
    // Public to be used in the derive macro.
    #[doc(hidden)]
    pub fn new(
        interface: impl Into<String>,
        path: impl Into<String>,
        exp: Aggregation,
        got: Aggregation,
    ) -> Self {
        Self {
            interface: interface.into(),
            path: path.into(),
            exp,
            got,
        }
    }
}

/// Invalid interface type when sending or receiving.
#[derive(Debug, thiserror::Error)]
#[error("invalid interface type for {name}{}, expected {exp} but got {got}", path.as_deref().unwrap_or_default())]
pub struct InterfaceTypeError {
    /// Name of the interface.
    name: String,
    /// Optional path
    path: Option<String>,
    /// Expected interface type.
    exp: InterfaceType,
    /// Actual interface type.
    got: InterfaceType,
}

impl InterfaceTypeError {
    pub(crate) fn new(name: impl Into<String>, exp: InterfaceType, got: InterfaceType) -> Self {
        Self {
            name: name.into(),
            path: None,
            exp,
            got,
        }
    }

    // Public to be used in the derive macro.
    #[doc(hidden)]
    pub fn with_path(
        name: impl Into<String>,
        path: impl Into<String>,
        exp: InterfaceType,
        got: InterfaceType,
    ) -> Self {
        Self {
            name: name.into(),
            path: Some(path.into()),
            exp,
            got,
        }
    }
}

/// Sending data on an interface not owned by the device
#[derive(Debug, thiserror::Error)]
#[error("invalid ownership for interface {name}, expected {exp} but got {got}")]
pub struct OwnershipError {
    /// Name of the interface.
    name: String,
    /// Expected interface ownership.
    exp: Ownership,
    /// Actual interface ownership.
    got: Ownership,
}

impl OwnershipError {
    pub(crate) fn new(name: impl Into<String>, exp: Ownership, got: Ownership) -> Self {
        Self {
            name: name.into(),
            exp,
            got,
        }
    }
}

/// An error reporter that prints an error and its sources.
///
/// This is a stub until the std library implementation get stabilized[^1]
///
/// [^1]: https://doc.rust-lang.org/std/error/struct.Report.html
pub(crate) struct Report<E = Box<dyn std::error::Error>> {
    /// The error being reported.
    error: E,
}

impl<E> Report<E>
where
    Report<E>: From<E>,
{
    /// Create a new Report from an input error.
    pub(crate) fn new(error: E) -> Self {
        Self::from(error)
    }
}

impl<E> From<E> for Report<E>
where
    E: std::error::Error,
{
    fn from(value: E) -> Self {
        Self { error: value }
    }
}

impl<E> Display for Report<E>
where
    E: std::error::Error,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)?;

        let mut cause: &dyn std::error::Error = &self.error;

        while let Some(source) = cause.source() {
            cause = source;

            write!(f, ": {cause}")?;
        }

        Ok(())
    }
}

// This type intentionally outputs the same format for `Display` and `Debug`for
// situations where you unwrap a `Report` or return it from main.
impl<E> std::fmt::Debug for Report<E>
where
    Report<E>: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}
