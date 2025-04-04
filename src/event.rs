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

use crate::aggregate::AstarteObject;
use crate::error::{AggregationError, InterfaceTypeError};
use crate::{AstarteType, Timestamp};

/// Astarte device event data structure.
///
/// Data structure received from the [`Client`](crate::DeviceClient) when the
/// [`Connection`](crate::DeviceConnection) polls a valid event.
#[derive(Debug, Clone, PartialEq)]
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
    /// Invalid interface aggregation for the event
    #[error("invalid interface aggregation for the event")]
    Aggregation(#[from] AggregationError),
    /// Invalid interface type for the event
    #[error("invalid interface type for the event")]
    InterfaceType(#[from] InterfaceTypeError),
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
    /// couldn't convert from [`AstarteType`]
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
/// use astarte_device_sdk::error::AggregationError;
/// use astarte_device_sdk::event::{FromEvent, FromEventError};
/// use astarte_device_sdk::interface::mapping::endpoint::Endpoint;
/// use astarte_device_sdk::interface::def::Aggregation;
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
///         let Value::Object{mut data, timestamp: _} = event.data else {
///             return Err(FromEventError::Aggregation(AggregationError::new(
///                 "com.example.Sensor",
///                 "sensor",
///                 Aggregation::Object,
///                 Aggregation::Individual,
///             )));
///         };
///
///         let name = data
///             .remove("name")
///             .ok_or(FromEventError::MissingField {
///                 interface: "com.example.Sensor",
///                 base_path: "sensor",
///                 path: "name",
///             })?
///             .try_into()?;
///
///         let value = data
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

/// Data for an [`Astarte data event`](DeviceEvent).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Data from an Individual Datastream interface.
    Individual {
        ///  The data publish on the endpoint.
        data: AstarteType,
        /// The timestamp of the data.
        ///
        /// If the interface has `explicit_timestamp` set to true, then this is the value we
        /// received from Astarte, otherwise it's the reception timestamp.
        timestamp: Timestamp,
    },
    /// Data from an Object Datastream interface.
    Object {
        /// The object data received.
        data: AstarteObject,
        /// The timestamp of the data.
        ///
        /// If the interface has `explicit_timestamp` set to true, then this is the value we
        /// received from Astarte, otherwise it's the reception timestamp.
        timestamp: Timestamp,
    },
    /// Property data, can also be an unset ,
    Property(Option<AstarteType>),
}

impl Value {
    /// Returns `true` if the aggregation is [`Individual`].
    ///
    /// [`Individual`]: Value::Individual
    #[must_use]
    pub fn is_individual(&self) -> bool {
        matches!(self, Self::Individual { .. })
    }

    /// Get a reference to the [`AstarteType`] if the aggregate is
    /// [`Individual`](Value::Individual).
    #[must_use]
    pub fn as_individual(&self) -> Option<(&AstarteType, &Timestamp)> {
        if let Self::Individual { data, timestamp } = self {
            Some((data, timestamp))
        } else {
            None
        }
    }

    /// Take out of the enum an [`AstarteType`] if the aggregate is
    /// [`Individual`](Value::Individual).
    pub fn try_into_individual(self) -> Result<(AstarteType, Timestamp), Self> {
        if let Self::Individual { data, timestamp } = self {
            Ok((data, timestamp))
        } else {
            Err(self)
        }
    }

    /// Returns `true` if the aggregation is [`Object`].
    ///
    /// [`Object`]: Value::Object
    #[must_use]
    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object { .. })
    }

    /// Get a reference to the [`AstarteObject`] if the aggregate is [`Object`](Value::Object).
    #[must_use]
    pub fn as_object(&self) -> Option<(&AstarteObject, &Timestamp)> {
        if let Self::Object { data, timestamp } = self {
            Some((data, timestamp))
        } else {
            None
        }
    }

    /// Take out of the enum an [`AstarteObject`] if the aggregate is [`Object`](Value::Object).
    pub fn try_into_object(self) -> Result<(AstarteObject, Timestamp), Self> {
        if let Self::Object { data, timestamp } = self {
            Ok((data, timestamp))
        } else {
            Err(self)
        }
    }

    /// Returns `true` if the aggregation is [`Property`].
    ///
    /// [`Property`]: Value::Property
    #[must_use]
    pub fn is_property(&self) -> bool {
        matches!(self, Self::Property(_))
    }

    /// Get a reference to the [`AstarteType`] if the type is [`Property`](Value::Property).
    #[must_use]
    pub fn as_property(&self) -> Option<&Option<AstarteType>> {
        if let Self::Property(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Take out of the enum an [`AstarteType`] if the type is [`Property`](Value::Property).
    pub fn try_into_property(self) -> Result<Option<AstarteType>, Self> {
        if let Self::Property(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn should_increase_coverage() {
        let timestamp = Utc::now();
        let individual = AstarteType::Integer(42);
        let val = Value::Individual {
            data: individual.clone(),
            timestamp,
        };
        assert!(val.is_individual());
        assert_eq!(val.as_individual(), Some((&individual, &timestamp)));
        assert_eq!(val.as_object(), None);
        assert_eq!(val.as_property(), None);
        assert_eq!(
            val.clone().try_into_individual(),
            Ok((individual, timestamp))
        );
        assert_eq!(val.clone().try_into_object(), Err(val.clone()));
        assert_eq!(val.clone().try_into_property(), Err(val));

        let val = Value::Object {
            data: AstarteObject::new(),
            timestamp,
        };
        assert!(val.is_object());
        assert_eq!(val.as_individual(), None);
        assert_eq!(val.as_property(), None);
        assert_eq!(val.as_object(), Some((&AstarteObject::new(), &timestamp)));
        assert_eq!(
            val.clone().try_into_object(),
            Ok((AstarteObject::new(), timestamp))
        );
        assert_eq!(val.clone().try_into_individual(), Err(val.clone()));
        assert_eq!(val.clone().try_into_property(), Err(val));

        let prop = Some(AstarteType::Integer(42));
        let val = Value::Property(prop.clone());
        assert!(val.is_property());
        assert_eq!(val.as_property(), Some(&prop));
        assert_eq!(val.as_object(), None);
        assert_eq!(val.as_individual(), None);
        assert_eq!(val.clone().try_into_individual(), Err(val.clone()));
        assert_eq!(val.clone().try_into_object(), Err(val.clone()));
        assert_eq!(val.clone().try_into_property(), Ok(prop));
    }

    #[test]
    #[cfg(feature = "derive")]
    fn should_derive_form_event_obj() {
        use crate::aggregate::AstarteObject;
        use crate::{DeviceEvent, FromEvent, Value};

        // Alias the crate to the resulting macro
        use crate::{self as astarte_device_sdk};

        #[derive(Debug, FromEvent, PartialEq, Eq)]
        #[from_event(
            interface = "com.example.Sensor",
            path = "/sensor",
            aggregation = "object"
        )]
        struct Sensor {
            name: String,
            value: i32,
        }

        let mut data = AstarteObject::new();
        data.insert("name".to_string(), "Foo".to_string().into());
        data.insert("value".to_string(), 42i32.into());

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor".to_string(),
            data: Value::Object {
                data,
                timestamp: Utc::now(),
            },
        };

        let sensor = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor {
            name: "Foo".to_string(),
            value: 42,
        };

        assert_eq!(sensor, expected);
    }

    #[test]
    #[cfg(feature = "derive")]
    fn should_derive_form_event_individual() {
        use crate::{AstarteType, DeviceEvent, FromEvent, Value};

        // Alias the crate to the resulting macro
        use crate::{self as astarte_device_sdk};

        #[derive(Debug, FromEvent, PartialEq)]
        #[from_event(interface = "com.example.Sensor", aggregation = "individual")]
        enum Sensor {
            #[mapping(endpoint = "/%{param}/luminosity")]
            Luminosity(i32),
            #[mapping(endpoint = "/sensor/temperature")]
            Temperature(f64),
        }

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/luminosity".to_string(),
            data: Value::Individual {
                data: 42i32.into(),
                timestamp: Utc::now(),
            },
        };

        let luminosity = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Luminosity(42);

        assert_eq!(luminosity, expected);

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/temperature".to_string(),
            data: Value::Individual {
                data: AstarteType::Double(3.),
                timestamp: Utc::now(),
            },
        };

        let temperature = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Temperature(3.);

        assert_eq!(temperature, expected);
    }

    #[test]
    #[cfg(feature = "derive")]
    fn should_derive_form_event_property() {
        use crate::{AstarteType, DeviceEvent, FromEvent, Value};

        // Alias the crate to the resulting macro
        use crate::{self as astarte_device_sdk};

        #[derive(Debug, FromEvent, PartialEq)]
        #[from_event(interface = "com.example.Sensor", interface_type = "properties")]
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
            data: Value::Property(Some(42i32.into())),
        };

        let luminosity = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Luminosity(42);

        assert_eq!(luminosity, expected);

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/temperature".to_string(),
            data: Value::Property(Some(AstarteType::Double(3.))),
        };

        let temperature = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Temperature(3.);

        assert_eq!(temperature, expected);

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/unsettable".to_string(),
            data: Value::Property(Some(AstarteType::Boolean(true))),
        };

        let temperature = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Unsettable(Some(true));

        assert_eq!(temperature, expected);

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/unsettable".to_string(),
            data: Value::Property(None),
        };

        let temperature = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Unsettable(None);

        assert_eq!(temperature, expected);
    }
}
