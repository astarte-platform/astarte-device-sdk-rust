// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
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

use std::fmt::{Display, Formatter};

use astarte_device_error::Error;
use astarte_interfaces::schema::{Aggregation, InterfaceType};

use crate::retention::RetentionError;
use crate::session::SessionError;
use crate::store::error::StoreError;
use crate::transport::mqtt::error::MqttError;

/// Error returned by the SDK
pub type AstarteError = astarte_device_error::Error<ErrorKind>;

/// Astarte error.
///
/// Possible errors returned by functions of the Astarte device SDK.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Generic I/O error
    Io(std::io::ErrorKind),
    /// Invalid or malformed interface
    Interface(InterfaceError),
    /// Couldn't complete store operation
    Store(StoreError),
    /// Couldn't complete session operation
    Session(SessionError),
    /// Couldn't complete retention operation
    Retention(RetentionError),
    /// Mqtt transport error.
    Mqtt(MqttError),
    /// Grcp transport error
    #[cfg(feature = "message-hub")]
    Grpc(crate::transport::grpc::error::GrpcError),
    /// Device is disconnected
    Disconnected,
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Io(error_kind) => write!(f, "I/O error {error_kind}"),
            ErrorKind::Interface(error) => write!(f, "interface error {error}"),
            ErrorKind::Store(error) => {
                write!(f, "store operation failed {error}")
            }
            ErrorKind::Session(error) => {
                write!(f, "session operation failed {error}")
            }
            ErrorKind::Retention(retention_error) => {
                write!(f, "retention operation failed {retention_error}")
            }
            ErrorKind::Disconnected => write!(f, "device is disconnected"),
            ErrorKind::Mqtt(error) => write!(f, "MQTT transport error {error}"),
            #[cfg(feature = "message-hub")]
            ErrorKind::Grpc(grpc_error) => write!(f, "Message Hub gRPC returned {grpc_error}"),
        }
    }
}

/// Error for the Astarte Interfaces
///
/// Returned when an error is caused by an Interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InterfaceError {
    /// Invalid interface
    Invalid,
    /// Couldn't parse the mapping path.
    Path,
    /// Couldn't find an interface with the given name.
    InterfaceNotFound,
    /// Couldn't find missing mapping in the interface.
    MappingNotFound,
    /// Invalid aggregation between the interface and the data.
    Aggregation,
    /// Invalid aggregation between the interface and the data.
    InterfaceType,
    /// Received data on a device owned interface.
    Ownership,
    /// Invalid mapping type sent on endpoint.
    MappingType,
    /// Invalid `explicit_timestamp` usage.
    Timestamp,
    /// Invalid `allow_unset` usage for property.
    Unset,
    /// Invalid object path for interface
    ObjectPath,
    /// Invalid object with required mapping
    MappingRequired,
}

impl InterfaceError {
    #[doc(hidden)]
    pub fn interface_type(
        ctx: &'static str,
        interface: impl Display,
        path: impl Display,
        exp: InterfaceType,
        got: InterfaceType,
    ) -> Error<InterfaceError> {
        Error::with(InterfaceError::InterfaceType, ctx).set_ctx(format!(
            "for {interface}{path}, expected {exp} but got {got}"
        ))
    }

    #[doc(hidden)]
    pub fn aggregation(
        ctx: &'static str,
        interface: impl Display,
        path: impl Display,
        exp: Aggregation,
        got: Aggregation,
    ) -> Error<InterfaceError> {
        Error::with(InterfaceError::Aggregation, ctx).set_ctx(format!(
            "for {interface}{path}, expected {exp} but got {got}"
        ))
    }
}

impl Display for InterfaceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InterfaceError::Invalid => write!(f, "invalid interface"),
            InterfaceError::Path => write!(f, "couldn't parse mapping path"),
            InterfaceError::InterfaceNotFound => write!(f, "interface not found"),
            InterfaceError::MappingNotFound => write!(f, "mapping endpoint not found"),
            InterfaceError::Aggregation => write!(f, "invalid interface aggregation"),
            InterfaceError::InterfaceType => write!(f, "invalid interface type"),
            InterfaceError::Ownership => write!(f, "invalid interface ownership"),
            InterfaceError::MappingType => write!(f, "invalid mapping type"),
            InterfaceError::Timestamp => write!(f, "invalid explicit_timestamp"),
            InterfaceError::Unset => write!(f, "invalid `allow_unset`"),
            InterfaceError::ObjectPath => write!(f, "invalid object path for interface"),
            InterfaceError::MappingRequired => write!(f, "invalid required object mapping"),
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
