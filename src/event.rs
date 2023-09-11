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

//! Event returned form the loop.

use crate::AstarteDeviceDataEvent;

/// Conversion error from an [`AstarteDeviceDataEvent`].
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum FromEventError {
    /// couldn't parse request from interface
    #[error("couldn't parse request from interface {0}")]
    Interface(String),
    /// object has wrong base path
    #[error("object has wrong base path ({interface}) {object}")]
    Path {
        /// Interface that generated the error
        interface: &'static str,
        /// Base path of the interface
        object: String,
    },
    /// individual data passed to object
    #[error("individual data passed to object ({interface}/{object})")]
    Individual {
        /// Interface that generated the error
        interface: &'static str,
        /// Base path of the interface
        object: &'static str,
    },
    /// object missing field
    #[error("object missing field ({interface}/{object}) {path}")]
    MissingField {
        /// Interface that generated the error
        interface: &'static str,
        /// Base path of the interface
        object: &'static str,
        /// Path of the endpoint in error
        path: &'static str,
    },
    /// couldn't convert from [`AstarteType`](crate::types::AstarteType)
    #[error("couldn't convert from AstarteType")]
    Conversion(#[from] crate::types::TypeError),
}

/// Converts a struct form an [`AstarteDeviceDataEvent`].
///
/// # Example
///
/// ```rust
/// use astarte_device_sdk::{Aggregation, AstarteDeviceDataEvent};
/// use astarte_device_sdk::event::{FromEvent, FromEventError};
///
/// struct Sensor {
///     name: String,
///     value: i32,
/// }
///
/// impl FromEvent for Sensor {
///     type Err = FromEventError;
///
///     fn from_event(event: AstarteDeviceDataEvent) -> Result<Self, Self::Err> {
///         if event.interface != "com.example.Sensor" {
///             return Err(FromEventError::Interface(event.interface.clone()));
///         }
///
///         if event.path != "/sensor" {
///             return Err(FromEventError::Path {
///                 interface: "com.example.Person",
///                 object: event.path.clone(),
///             });
///         }
///
///         let Aggregation::Object(mut object) = event.data else {
///             return Err(FromEventError::Individual {
///                 interface: "com.example.Person",
///                 object: "/sensor",
///             });
///         };
///
///         let name = object
///             .remove("name")
///             .ok_or(FromEventError::MissingField {
///                 interface: "com.example.Person",
///                 object: "/sensor",
///                 path: "name",
///             })?
///             .try_into()?;
///         let value = object
///             .remove("value")
///             .ok_or(FromEventError::MissingField {
///                 interface: "com.example.Person",
///                 object: "/sensor",
///                 path: "value",
///             })?
///             .try_into()?;
///
///         Ok(Self { name, value })
///     }
/// }
/// ```
pub trait FromEvent: Sized {
    type Err;

    fn from_event(event: AstarteDeviceDataEvent) -> Result<Self, Self::Err>;
}
