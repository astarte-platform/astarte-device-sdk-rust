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

use crate::Value;

/// Astarte device event data structure.
///
/// Data structure received from the [`Client`](crate::DeviceClient) when the
/// [`Connection`](crate::DeviceConnection) polls a valid event.
#[derive(Debug, Clone)]
pub struct DeviceEvent {
    /// Interface on which the event has been triggered
    pub interface: String,
    /// Path to the endpoint for which the event has been triggered
    pub path: String,
    /// Payload of the event
    pub data: Value,
}

/// Conversion error from an [`DeviceEvent].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FromEventError {
    /// couldn't parse request from interface
    #[error("couldn't parse request from interface {0}")]
    Interface(String),
    /// couldn't parse the event path
    #[error("the interface {interface} has wrong path {base_path}")]
    Path {
        /// Interface that generated the error
        interface: &'static str,
        /// Base path of the interface
        base_path: String,
    },
    /// individual data passed to object
    #[error("individual data passed to object {interface}{base_path}")]
    Individual {
        /// Interface that generated the error
        interface: &'static str,
        /// Base path of the interface
        base_path: &'static str,
    },
    /// object data passed to individual
    #[error("object data passed to individual {interface}{endpoint}")]
    Object {
        /// Interface that generated the error
        interface: &'static str,
        /// endpoint
        endpoint: String,
    },
    /// unset passed to endpoint without allow unset
    #[error("unset passed to {interface}{endpoint} without allow unset")]
    Unset {
        /// Interface that generated the error
        interface: &'static str,
        /// endpoint
        endpoint: String,
    },
    /// object missing field
    #[error("object {interface} missing field {base_path}/{path}")]
    MissingField {
        /// Interface that generated the error
        interface: &'static str,
        /// Base path of the interface
        base_path: &'static str,
        /// Path of the endpoint in error
        path: &'static str,
    },
    /// couldn't convert from [`AstarteType`](crate::types::AstarteType)
    #[error("couldn't convert from AstarteType")]
    Conversion(#[from] crate::types::TypeError),
    /// couldn't parse the [`crate::interface::mapping::endpoint::Endpoint`]
    #[error("couldn't parse the endpoint")]
    Endpoint(#[from] crate::interface::mapping::endpoint::EndpointError),
}

/// Converts a struct form an [`DeviceEvent`].
///
/// # Example
///
/// ```rust
/// use astarte_device_sdk::{Value, DeviceEvent};
/// use astarte_device_sdk::event::{FromEvent, FromEventError};
/// use astarte_device_sdk::interface::mapping::endpoint::Endpoint;
///
/// use std::convert::TryFrom;
///
/// struct Sensor {
///     name: String,
///     value: i32,
/// }
///
/// impl FromEvent for Sensor {
///     type Err = FromEventError;
///
///     fn from_event(event: DeviceEvent) -> Result<Self, Self::Err> {
///         let base_path: Endpoint<&str> = Endpoint::try_from("/sensor")?;
///
///         if event.interface != "com.example.Sensor" {
///             return Err(FromEventError::Interface(event.interface.clone()));
///         }
///
///         if base_path.eq_mapping(&event.path) {
///             return Err(FromEventError::Path {
///                 interface: "com.example.Sensor",
///                 base_path: event.path.clone(),
///             });
///         }
///
///         let Value::Object(mut object) = event.data else {
///             return Err(FromEventError::Individual {
///                 interface: "com.example.Sensor",
///                 base_path: "sensor",
///             });
///         };
///
///         let name = object
///             .remove("name")
///             .ok_or(FromEventError::MissingField {
///                 interface: "com.example.Sensor",
///                 base_path: "sensor",
///                 path: "name",
///             })?
///             .try_into()?;
///
///         let value = object
///             .remove("value")
///             .ok_or(FromEventError::MissingField {
///                 interface: "com.example.Sensor",
///                 base_path: "sensor",
///                 path: "value",
///             })?
///             .try_into()?;
///
///         Ok(Self { name, value })
///     }
/// }
/// ```
pub trait FromEvent: Sized {
    /// Reason why the conversion failed.
    type Err;

    /// Perform the conversion from the event.
    fn from_event(event: DeviceEvent) -> Result<Self, Self::Err>;
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "derive")]
    #[test]
    fn should_derive_form_event_obj() {
        use std::collections::HashMap;

        // Alias the crate to the resulting macro
        use crate::{self as astarte_device_sdk};
        use crate::{DeviceEvent, FromEvent, Value};

        #[derive(Debug, FromEvent, PartialEq, Eq)]
        #[from_event(interface = "com.example.Sensor", path = "/sensor")]
        struct Sensor {
            name: String,
            value: i32,
        }

        let mut data = HashMap::new();
        data.insert("name".to_string(), "Foo".to_string().into());
        data.insert("value".to_string(), 42i32.into());

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor".to_string(),
            data: Value::Object(data),
        };

        let sensor = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor {
            name: "Foo".to_string(),
            value: 42,
        };

        assert_eq!(sensor, expected);
    }

    #[cfg(feature = "derive")]
    #[test]
    fn should_derive_form_event_individual() {
        // Alias the crate to the resulting macro
        use crate::{self as astarte_device_sdk, AstarteType, DeviceEvent, FromEvent, Value};

        #[derive(Debug, FromEvent, PartialEq)]
        #[from_event(interface = "com.example.Sensor", aggregation = "individual")]
        enum Sensor {
            #[mapping(endpoint = "/%{param}/luminosity")]
            Luminosity(i32),
            #[mapping(endpoint = "/sensor/temperature")]
            Temperature(f64),
            #[mapping(endpoint = "/sensor/unsettable", allow_unset = true)]
            Unsettable(Option<bool>),
        }

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/luminosity".to_string(),
            data: Value::Individual(42i32.into()),
        };

        let luminosity = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Luminosity(42);

        assert_eq!(luminosity, expected);

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/temperature".to_string(),
            data: Value::Individual(AstarteType::Double(3.)),
        };

        let temperature = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Temperature(3.);

        assert_eq!(temperature, expected);

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/unsettable".to_string(),
            data: Value::Individual(AstarteType::Boolean(true)),
        };

        let temperature = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Unsettable(Some(true));

        assert_eq!(temperature, expected);

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/unsettable".to_string(),
            data: Value::Unset,
        };

        let temperature = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Unsettable(None);

        assert_eq!(temperature, expected);
    }
}
