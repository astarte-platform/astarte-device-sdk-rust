// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Contains conversion traits to convert the Astarte types in the protobuf format to the
//! Astarte types from the Astarte device SDK.

use std::collections::HashMap;
use std::fmt::Display;

use astarte_device_error::{Error, ResultExt};

use crate::validate::individual::ValidatedIndividual;
use crate::validate::object::ValidatedObject;
use crate::validate::properties::ValidatedUnset;
use astarte_interfaces::schema::Ownership;
use astarte_message_hub_proto::astarte_data::AstarteData as ProtoData;
use astarte_message_hub_proto::astarte_message::Payload as ProtoPayload;
use astarte_message_hub_proto::{
    AstarteData as ProtoDataWrapper, AstarteDatastreamIndividual, AstarteDatastreamObject,
    AstartePropertyIndividual, prost_types,
};
use chrono::{TimeZone, Utc};
use tracing::error;

use crate::Timestamp;
use crate::aggregate::AstarteObject;
use crate::store::{OptStoredProp, StoredProp, UpdatedAt};
use crate::types::AstarteData;
use crate::types::{Double, TypeError};

use super::ValidatedProperty;

/// Error returned by the Message Hub types conversions.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageHubProtoError {
    /// Expected field was not found
    ExpectedField,
    /// Date conversion error
    Timestamp,
    /// Expected set property got an unset
    ExpectedSetProperty,
    /// Couldn't convert proto to astarte type
    Conversion(TypeError),
}

impl Display for MessageHubProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExpectedField => write!(f, "missing the expected field"),
            Self::Timestamp => write!(f, "error while converting a proto timestamp"),
            Self::ExpectedSetProperty => write!(f, "expected set property got an unset"),
            Self::Conversion(error) => {
                write!(f, "couldn't convert proto to Astarte type {error}")
            }
        }
    }
}

/// Map a list of properties, unset properties will be ignored
pub(crate) fn map_set_stored_properties(
    message_hub_properties: astarte_message_hub_proto::StoredProperties,
) -> Result<Vec<StoredProp>, Error<MessageHubProtoError>> {
    message_hub_properties
        .properties
        .into_iter()
        .filter_map(|prop| {
            let ownership = match prop.ownership() {
                astarte_message_hub_proto::Ownership::Device => Ownership::Device,
                astarte_message_hub_proto::Ownership::Server => Ownership::Server,
            };

            let value = prop.data?;

            Some(AstarteData::try_from(value).map(|value| {
                StoredProp::from_server(
                    prop.interface_name,
                    prop.path,
                    value,
                    prop.version_major,
                    ownership,
                    // TODO: should be received from the server
                    UpdatedAt::new(Utc::now(), 0),
                )
            }))
        })
        .collect()
}

/// Converts a [`prost_types::Timestamp`] into a [`chrono::DateTime<Utc>`]
pub(crate) fn convert_timestamp(
    timestamp: prost_types::Timestamp,
) -> Result<crate::Timestamp, Error<MessageHubProtoError>> {
    let val = timestamp.normalized();

    let nanos = val
        .nanos
        .try_into()
        .inspect_err(|_| {
            error!(%timestamp, "couldn't convert sub nanoseconds");
        })
        .unwrap_or(0);

    chrono::Utc
        .timestamp_opt(val.seconds, nanos)
        .earliest()
        .ok_or(Error::new(MessageHubProtoError::Timestamp))
}

/// Converts a [`chrono::DateTime<Utc>`] into a [`prost_types::Timestamp`]
fn convert_chrono(timestamp: crate::Timestamp) -> prost_types::Timestamp {
    let nanos = i32::try_from(timestamp.timestamp_subsec_nanos()).unwrap_or(i32::MAX);

    prost_types::Timestamp {
        seconds: timestamp.timestamp(),
        // this is always less than i32::MAX
        nanos,
    }
}

impl TryFrom<ProtoDataWrapper> for AstarteData {
    type Error = Error<MessageHubProtoError>;

    fn try_from(value: ProtoDataWrapper) -> Result<Self, Self::Error> {
        let astarte_data = value
            .astarte_data
            .ok_or_else(|| Error::with(MessageHubProtoError::ExpectedField, "astarte_data"))?;

        match astarte_data {
            ProtoData::DateTime(v) => convert_timestamp(v).map(AstarteData::DateTime),
            ProtoData::Double(v) => {
                AstarteData::try_from(v).map_kind(MessageHubProtoError::Conversion)
            }
            ProtoData::Integer(v) => Ok(AstarteData::Integer(v)),
            ProtoData::Boolean(v) => Ok(AstarteData::Boolean(v)),
            ProtoData::LongInteger(v) => Ok(AstarteData::LongInteger(v)),
            ProtoData::String(v) => Ok(AstarteData::String(v)),
            ProtoData::BinaryBlob(v) => Ok(AstarteData::BinaryBlob(v)),
            ProtoData::DoubleArray(arr) => {
                AstarteData::try_from(arr.values).map_kind(MessageHubProtoError::Conversion)
            }
            ProtoData::IntegerArray(arr) => Ok(AstarteData::IntegerArray(arr.values)),
            ProtoData::BooleanArray(arr) => Ok(AstarteData::BooleanArray(arr.values)),
            ProtoData::LongIntegerArray(arr) => Ok(AstarteData::LongIntegerArray(arr.values)),
            ProtoData::StringArray(arr) => Ok(AstarteData::StringArray(arr.values)),
            ProtoData::BinaryBlobArray(arr) => Ok(AstarteData::BinaryBlobArray(arr.values)),
            ProtoData::DateTimeArray(arr) => arr
                .values
                .into_iter()
                .map(convert_timestamp)
                .collect::<Result<Vec<_>, Error<MessageHubProtoError>>>()
                .map(AstarteData::DateTimeArray),
        }
    }
}

impl From<AstarteData> for ProtoDataWrapper {
    fn from(value: AstarteData) -> Self {
        let astarte_data = match value {
            AstarteData::Double(value) => ProtoData::Double(*value),
            AstarteData::Integer(value) => ProtoData::Integer(value),
            AstarteData::Boolean(value) => ProtoData::Boolean(value),
            AstarteData::LongInteger(value) => ProtoData::LongInteger(value),
            AstarteData::String(value) => ProtoData::String(value),
            AstarteData::BinaryBlob(value) => ProtoData::BinaryBlob(value),
            AstarteData::DateTime(value) => ProtoData::DateTime(convert_chrono(value)),
            AstarteData::DoubleArray(values) => {
                ProtoData::DoubleArray(astarte_message_hub_proto::AstarteDoubleArray {
                    values: values.into_iter().map(Double::into).collect(),
                })
            }
            AstarteData::IntegerArray(values) => {
                ProtoData::IntegerArray(astarte_message_hub_proto::AstarteIntegerArray { values })
            }
            AstarteData::BooleanArray(values) => {
                ProtoData::BooleanArray(astarte_message_hub_proto::AstarteBooleanArray { values })
            }
            AstarteData::LongIntegerArray(values) => {
                ProtoData::LongIntegerArray(astarte_message_hub_proto::AstarteLongIntegerArray {
                    values,
                })
            }
            AstarteData::StringArray(values) => {
                ProtoData::StringArray(astarte_message_hub_proto::AstarteStringArray { values })
            }
            AstarteData::BinaryBlobArray(values) => {
                ProtoData::BinaryBlobArray(astarte_message_hub_proto::AstarteBinaryBlobArray {
                    values,
                })
            }
            AstarteData::DateTimeArray(values) => {
                let values = values.into_iter().map(convert_chrono).collect();

                ProtoData::DateTimeArray(astarte_message_hub_proto::AstarteDateTimeArray { values })
            }
        };

        Self {
            astarte_data: Some(astarte_data),
        }
    }
}

// For send individual
impl From<ValidatedIndividual> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: ValidatedIndividual) -> Self {
        let timestamp = value.timestamp.map(convert_chrono);

        let payload = Some(ProtoPayload::DatastreamIndividual(
            AstarteDatastreamIndividual {
                data: Some(value.data.into()),
                timestamp,
            },
        ));

        astarte_message_hub_proto::AstarteMessage {
            interface_name: value.interface,
            path: value.path,
            payload,
        }
    }
}

// For send object
impl From<ValidatedObject> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: ValidatedObject) -> Self {
        let timestamp = value.timestamp.map(convert_chrono);

        let data = value
            .data
            .into_key_values()
            .map(|(k, v)| {
                let v = ProtoDataWrapper::from(v);

                (k, v)
            })
            .collect::<HashMap<String, ProtoDataWrapper>>();

        let payload = Some(ProtoPayload::DatastreamObject(
            astarte_message_hub_proto::AstarteDatastreamObject { data, timestamp },
        ));

        astarte_message_hub_proto::AstarteMessage {
            interface_name: value.interface,
            path: value.path,
            payload,
        }
    }
}

// For send property
impl From<ValidatedProperty> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: ValidatedProperty) -> Self {
        let data = ProtoDataWrapper::from(value.data);

        Self {
            interface_name: value.interface,
            path: value.path,
            payload: Some(ProtoPayload::PropertyIndividual(
                astarte_message_hub_proto::AstartePropertyIndividual { data: Some(data) },
            )),
        }
    }
}

// To convert a stored property
impl From<OptStoredProp> for astarte_message_hub_proto::AstarteMessage {
    fn from(prop: OptStoredProp) -> Self {
        Self {
            interface_name: prop.interface,
            path: prop.path,
            payload: Some(ProtoPayload::PropertyIndividual(
                astarte_message_hub_proto::AstartePropertyIndividual {
                    data: prop.value.map(Into::into),
                },
            )),
        }
    }
}

// For sending unset
impl From<ValidatedUnset> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: ValidatedUnset) -> Self {
        Self {
            interface_name: value.interface,
            path: value.path,
            payload: Some(ProtoPayload::PropertyIndividual(
                astarte_message_hub_proto::AstartePropertyIndividual { data: None },
            )),
        }
    }
}

/// For deserialize individual
pub(crate) fn try_from_individual(
    individual: AstarteDatastreamIndividual,
) -> Result<(AstarteData, Option<Timestamp>), Error<MessageHubProtoError>> {
    let data = individual
        .data
        .ok_or(Error::with(MessageHubProtoError::ExpectedField, "data"))?
        .try_into()?;

    let timestamp = individual.timestamp.map(convert_timestamp).transpose()?;

    Ok((data, timestamp))
}

/// For deserialize object
pub(crate) fn try_from_object(
    value: AstarteDatastreamObject,
) -> Result<(AstarteObject, Option<Timestamp>), Error<MessageHubProtoError>> {
    let data = value
        .data
        .into_iter()
        .map(|(k, value)| AstarteData::try_from(value).map(|v| (k, v)))
        .collect::<Result<AstarteObject, Error<MessageHubProtoError>>>()?;

    let timestamp = value.timestamp.map(convert_timestamp).transpose()?;

    Ok((data, timestamp))
}

// For deserialize property
pub(crate) fn try_from_property(
    property: AstartePropertyIndividual,
) -> Result<Option<AstarteData>, Error<MessageHubProtoError>> {
    property.data.map(AstarteData::try_from).transpose()
}

#[cfg(test)]
pub(crate) mod test {
    use std::collections::HashMap;

    use astarte_message_hub_proto::{
        AstarteDatastreamObject, AstarteMessage, AstartePropertyIndividual, Property,
    };
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    use super::*;

    pub(crate) fn new_astarte_message(
        interface_name: String,
        path: String,
        payload: ProtoPayload,
    ) -> AstarteMessage {
        AstarteMessage {
            interface_name,
            path,
            payload: Some(payload),
        }
    }

    #[rstest]
    #[case(AstarteData::Double(12.21.try_into().unwrap()))]
    #[case(AstarteData::Integer(12))]
    #[case(AstarteData::Boolean(false))]
    #[case(AstarteData::LongInteger(42))]
    #[case(AstarteData::String("hello".to_string()))]
    #[case(AstarteData::BinaryBlob(vec![1, 2, 3, 4]))]
    #[case(AstarteData::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()))]
    #[case(AstarteData::DoubleArray(
        [1.3, 2.6, 3.1, 4.0]
            .map(|v| Double::try_from(v).unwrap())
            .to_vec(),
    ))]
    #[case(AstarteData::IntegerArray(vec![1, 2, 3, 4]))]
    #[case(AstarteData::BooleanArray(vec![true, false, true, true]))]
    #[case(AstarteData::LongIntegerArray(vec![32, 11, 33, 1]))]
    #[case(AstarteData::StringArray(vec!["Hello".to_string(), " world!".to_string()]))]
    #[case(AstarteData::BinaryBlobArray(vec![vec![1, 2, 3, 4], vec![4, 4, 1, 4]]))]
    #[case(AstarteData::DateTimeArray(vec![
            TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
            TimeZone::timestamp_opt(&Utc, 1611580808, 0).unwrap(),
    ]))]
    fn proto_conversions_success(#[case] exp: AstarteData) {
        let proto = ProtoDataWrapper::from(exp.clone());
        let astarte_type = AstarteData::try_from(proto).unwrap();

        assert_eq!(exp, astarte_type);
    }

    #[test]
    fn astarte_individual_to_proto() {
        let exp_value = AstarteData::Integer(42);
        let exp = ValidatedIndividual {
            interface: "com.foo".to_string(),
            path: "/path".to_string(),
            version_major: 1,
            reliability: astarte_interfaces::schema::Reliability::Unique,
            retention: astarte_interfaces::interface::Retention::Discard,
            data: exp_value.clone(),
            timestamp: Some(Utc::now()),
        };
        let astarte_message = AstarteMessage::from(exp.clone());
        assert_eq!(astarte_message.interface_name, exp.interface);
        assert_eq!(astarte_message.path, exp.path);

        let value = astarte_message.payload.and_then(take_individual).unwrap();

        let (data, timestamp) = try_from_individual(value).unwrap();
        assert_eq!(data, exp_value);
        assert_eq!(timestamp, exp.timestamp);
    }

    #[test]
    fn astarte_property_to_proto() {
        let exp_value = AstarteData::Integer(42);
        let exp = ValidatedProperty {
            interface: "com.foo".to_string(),
            path: "/path".to_string(),
            version_major: 1,
            data: exp_value.clone(),
        };
        let astarte_message = AstarteMessage::from(exp.clone());
        assert_eq!(astarte_message.interface_name, exp.interface);
        assert_eq!(astarte_message.path, exp.path);

        let value = astarte_message.payload.and_then(take_property).unwrap();

        let data = try_from_property(value).unwrap();
        assert_eq!(data, Some(exp_value));
    }

    #[test]
    fn astarte_unset_to_proto() {
        let exp = ValidatedUnset {
            interface: "com.foo".to_string(),
            path: "/path".to_string(),
        };
        let astarte_message = AstarteMessage::from(exp.clone());
        assert_eq!(astarte_message.interface_name, exp.interface);
        assert_eq!(astarte_message.path, exp.path);

        let value = astarte_message.payload.and_then(take_property).unwrap();

        let data = try_from_property(value).unwrap();
        assert_eq!(data, None);
    }

    #[test]
    fn astarte_object_to_proto() {
        let expected_map = AstarteObject::from_iter([
            (
                "Mercury".to_owned(),
                AstarteData::Double(0.4.try_into().unwrap()),
            ),
            (
                "Venus".to_owned(),
                AstarteData::Double(0.7.try_into().unwrap()),
            ),
            (
                "Earth".to_owned(),
                AstarteData::Double(1.0.try_into().unwrap()),
            ),
            (
                "Mars".to_owned(),
                AstarteData::Double(1.5.try_into().unwrap()),
            ),
        ]);

        let exp = ValidatedObject {
            interface: "com.foo".to_string(),
            path: "/path".to_string(),
            version_major: 1,
            reliability: astarte_interfaces::schema::Reliability::Unique,
            retention: astarte_interfaces::interface::Retention::Discard,
            data: expected_map.clone(),
            timestamp: Some(Utc::now()),
        };
        let astarte_message = AstarteMessage::from(exp.clone());
        assert_eq!(astarte_message.interface_name, exp.interface);
        assert_eq!(astarte_message.path, exp.path);

        let astarte_object = astarte_message.payload.and_then(take_object).unwrap();

        let (data, timestamp) = try_from_object(astarte_object).unwrap();
        assert_eq!(data, expected_map);
        assert_eq!(timestamp, exp.timestamp);
    }

    #[test]
    fn from_sdk_astarte_type_to_astarte_message_payload_success() {
        let expected_double_value: f64 = 15.5;
        let astarte_sdk_type_double = AstarteData::try_from(expected_double_value).unwrap();

        let payload: ProtoPayload =
            ProtoPayload::DatastreamIndividual(AstarteDatastreamIndividual {
                data: Some(astarte_sdk_type_double.into()),
                timestamp: None,
            });

        let double_value = take_individual(payload)
            .and_then(|data| data.data)
            .and_then(|data| data.astarte_data)
            .unwrap();

        assert_eq!(ProtoData::Double(expected_double_value), double_value);
    }

    fn take_object(payload: ProtoPayload) -> Option<AstarteDatastreamObject> {
        match payload {
            ProtoPayload::DatastreamObject(obj) => Some(obj),
            _ => None,
        }
    }

    fn take_individual(payload: ProtoPayload) -> Option<AstarteDatastreamIndividual> {
        match payload {
            ProtoPayload::DatastreamIndividual(i) => Some(i),
            _ => None,
        }
    }

    fn take_property(payload: ProtoPayload) -> Option<AstartePropertyIndividual> {
        match payload {
            ProtoPayload::PropertyIndividual(i) => Some(i),
            _ => None,
        }
    }

    #[test]
    fn from_sdk_astarte_aggregate_to_astarte_message_payload_success() {
        let expected_data: f64 = 15.5;

        let payload_result = ProtoPayload::DatastreamObject(AstarteDatastreamObject {
            data: HashMap::from([(
                "key1".to_string(),
                ProtoDataWrapper {
                    astarte_data: Some(ProtoData::Double(expected_data)),
                },
            )]),
            timestamp: None,
        });

        let double_data = take_object(payload_result)
            .and_then(|mut obj| obj.data.remove("key1"))
            .and_then(|data| data.astarte_data)
            .unwrap();

        assert_eq!(ProtoData::Double(expected_data), double_data);
    }

    #[test]
    fn map_property_to_astarte_type_none() {
        let prop = Property {
            interface_name: "com.test.interface".to_owned(),
            path: "/path11".to_owned(),
            version_major: 0,
            ownership: astarte_message_hub_proto::Ownership::Device.into(),
            data: None,
        };
        let empty_props = map_set_stored_properties(astarte_message_hub_proto::StoredProperties {
            properties: vec![prop],
        })
        .unwrap();

        assert!(empty_props.is_empty());
    }

    #[test]
    fn from_message_hub_stored_properties_to_internal_ok() {
        const INTERFACE_1: &str = "com.test.interface1";
        const INTERFACE_2: &str = "com.test.interface2";

        let prop11 = Property {
            interface_name: INTERFACE_1.to_owned(),
            path: "/path11".to_owned(),
            version_major: 0,
            ownership: astarte_message_hub_proto::Ownership::Device.into(),
            data: Some(ProtoDataWrapper {
                astarte_data: Some(ProtoData::String("test".to_owned())),
            }),
        };
        let prop12 = Property {
            interface_name: INTERFACE_1.to_owned(),
            path: "/path12".to_owned(),
            version_major: 0,
            ownership: astarte_message_hub_proto::Ownership::Device.into(),
            data: Some(ProtoDataWrapper {
                astarte_data: Some(ProtoData::Integer(0)),
            }),
        };
        let prop21 = Property {
            interface_name: INTERFACE_2.to_owned(),
            path: "/path21".to_owned(),
            version_major: 0,
            ownership: astarte_message_hub_proto::Ownership::Server.into(),
            data: Some(ProtoDataWrapper {
                astarte_data: Some(ProtoData::BinaryBlob(vec![0, 54, 0, 23])),
            }),
        };
        let prop22 = Property {
            interface_name: INTERFACE_2.to_owned(),
            path: "/path22".to_owned(),
            version_major: 0,
            ownership: astarte_message_hub_proto::Ownership::Server.into(),
            data: Some(ProtoDataWrapper {
                astarte_data: Some(ProtoData::Double(std::f64::consts::PI)),
            }),
        };

        let message_hub_stored_properties = astarte_message_hub_proto::StoredProperties {
            properties: vec![prop11, prop12, prop21, prop22],
        };

        let inner_vec = map_set_stored_properties(message_hub_stored_properties).unwrap();

        assert_eq!(inner_vec.len(), 4);

        assert_eq!(
            inner_vec
                .iter()
                .filter(
                    |p| p.interface == INTERFACE_1 && (p.path == "/path11" || p.path == "/path12")
                )
                .count(),
            2
        );

        assert_eq!(
            inner_vec
                .iter()
                .filter(
                    |p| p.interface == INTERFACE_2 && (p.path == "/path21" || p.path == "/path22")
                )
                .count(),
            2
        );
    }
}
