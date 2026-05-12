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

//! Event returned form the loop.

use std::fmt::Display;

use crate::aggregate::AstarteObject;
use crate::error::InterfaceError;
use crate::types::TypeError;
use crate::{AstarteData, Timestamp};

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

/// Conversion error from an [`DeviceEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FromEventError {
    /// couldn't parse request from interface
    Interface(InterfaceError),
    /// couldn't convert from [`AstarteData`]
    Conversion(TypeError),
}

impl Display for FromEventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FromEventError::Interface(interface_error) => {
                write!(f, "interface error {interface_error}")
            }
            FromEventError::Conversion(type_error) => write!(f, "conversion error {type_error}"),
        }
    }
}

/// Converts a struct form an [`DeviceEvent`].
///
/// You can use the derive macro to implement the trait for a struct (object DataStream) or an enum
/// (individual DataStream and Properties).
///
/// See [`astarte_device_sdk_derive`].
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
        data: AstarteData,
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
    Property(Option<AstarteData>),
}

impl Value {
    /// Returns `true` if the aggregation is [`Individual`].
    ///
    /// [`Individual`]: Value::Individual
    #[must_use]
    pub fn is_individual(&self) -> bool {
        matches!(self, Self::Individual { .. })
    }

    /// Get a reference to the [`AstarteData`] if the aggregate is
    /// [`Individual`](Value::Individual).
    #[must_use]
    pub fn as_individual(&self) -> Option<(&AstarteData, &Timestamp)> {
        if let Self::Individual { data, timestamp } = self {
            Some((data, timestamp))
        } else {
            None
        }
    }

    /// Take out of the enum an [`AstarteData`] if the aggregate is
    /// [`Individual`](Value::Individual).
    pub fn try_into_individual(self) -> Result<(AstarteData, Timestamp), Self> {
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

    /// Get a reference to the [`AstarteData`] if the type is [`Property`](Value::Property).
    #[must_use]
    pub fn as_property(&self) -> Option<&Option<AstarteData>> {
        if let Self::Property(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Take out of the enum an [`AstarteData`] if the type is [`Property`](Value::Property).
    pub fn try_into_property(self) -> Result<Option<AstarteData>, Self> {
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
        let individual = AstarteData::Integer(42);
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

        let prop = Some(AstarteData::Integer(42));
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
        use crate::{AstarteData, DeviceEvent, FromEvent, Value};

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
                data: AstarteData::try_from(3.0).unwrap(),
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
        use crate::{AstarteData, DeviceEvent, FromEvent, Value};

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
            data: Value::Property(Some(AstarteData::try_from(3.0).unwrap())),
        };

        let temperature = Sensor::from_event(event).expect("couldn't parse the event");

        let expected = Sensor::Temperature(3.0);

        assert_eq!(temperature, expected);

        let event = DeviceEvent {
            interface: "com.example.Sensor".to_string(),
            path: "/sensor/unsettable".to_string(),
            data: Value::Property(Some(AstarteData::Boolean(true))),
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
