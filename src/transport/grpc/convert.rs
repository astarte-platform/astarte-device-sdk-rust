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

//! Contains conversion traits to convert the Astarte types in the protobuf format to the
//! Astarte types from the Astarte device SDK.

use astarte_interfaces::schema::Ownership;
use astarte_message_hub_proto::astarte_data::AstarteData as ProtoData;
use astarte_message_hub_proto::message_hub_event::Event;
use astarte_message_hub_proto::{astarte_message::Payload as ProtoPayload, pbjson_types};
use astarte_message_hub_proto::{
    AstarteData as ProtoDataWrapper, AstarteDatastreamIndividual, AstarteDatastreamObject,
    AstartePropertyIndividual, MessageHubEvent,
};
use chrono::TimeZone;
use itertools::Itertools;

use crate::aggregate::AstarteObject;
use crate::store::{OptStoredProp, StoredProp};
use crate::types::{Double, TypeError};
use crate::validate::ValidatedUnset;
use crate::{
    transport::ReceivedEvent, types::AstarteData, validate::ValidatedIndividual,
    validate::ValidatedObject,
};
use crate::{DeviceEvent, Timestamp, Value};

use super::{GrpcError, GrpcPayload, ValidatedProperty};

/// Error returned by the Message Hub types conversions.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum MessageHubProtoError {
    /// Expected field was not found
    #[error("missing the expected field '{0}'")]
    ExpectedField(&'static str),
    /// Date conversion error
    #[error("error while converting a proto date: {0:?}")]
    Timestamp(pbjson_types::Timestamp),
    /// Expected set property got an unset
    #[error("expected set property got an unset")]
    ExpectedSetProperty,
    /// Couldn't convert proto to astarte type
    #[error("couldn't convert proto to Astarte type")]
    Conversion(#[from] TypeError),
}

/// Map a received message hub property to an optional astarte type
pub(crate) fn map_property_to_astarte_type(
    value: astarte_message_hub_proto::Property,
) -> Result<Option<AstarteData>, MessageHubProtoError> {
    let astarte_message_hub_proto::Property { data, .. } = value;

    let Some(individual) = data else {
        return Ok(None);
    };

    Ok(Some(individual.try_into()?))
}

/// Map a list of properties, unset properties will be ignored
pub(crate) fn map_set_stored_properties(
    message_hub_properties: astarte_message_hub_proto::StoredProperties,
) -> Result<Vec<StoredProp>, MessageHubProtoError> {
    message_hub_properties
        .properties
        .into_iter()
        .filter_map(|prop| {
            let ownership = match prop.ownership() {
                astarte_message_hub_proto::Ownership::Device => Ownership::Device,
                astarte_message_hub_proto::Ownership::Server => Ownership::Server,
            };

            let value = match map_property_to_astarte_type(prop.clone()).transpose()? {
                Ok(s) => s,
                Err(e) => return Some(Err(e)),
            };

            let res = StoredProp {
                interface: prop.interface_name,
                path: prop.path,
                value,
                interface_major: prop.version_major,
                ownership,
            };

            Some(Ok(res))
        })
        .try_collect()
}

/// Converts a [`pbjson_types::Timestamp`] into a [`chrono::DateTime<Utc>`]
fn convert_timestamp(
    val: pbjson_types::Timestamp,
) -> Result<crate::Timestamp, MessageHubProtoError> {
    let nanos = val
        .nanos
        .try_into()
        .map_err(|_| MessageHubProtoError::Timestamp(val))?;

    chrono::Utc
        .timestamp_opt(val.seconds, nanos)
        .earliest()
        .ok_or(MessageHubProtoError::Timestamp(val))
}

impl TryFrom<ProtoDataWrapper> for AstarteData {
    type Error = MessageHubProtoError;

    fn try_from(value: ProtoDataWrapper) -> Result<Self, Self::Error> {
        let astarte_data = value
            .astarte_data
            .ok_or(MessageHubProtoError::ExpectedField("astarte_data"))?;

        match astarte_data {
            ProtoData::DateTime(v) => convert_timestamp(v).map(AstarteData::DateTime),
            ProtoData::Double(v) => {
                AstarteData::try_from(v).map_err(MessageHubProtoError::Conversion)
            }
            ProtoData::Integer(v) => Ok(AstarteData::Integer(v)),
            ProtoData::Boolean(v) => Ok(AstarteData::Boolean(v)),
            ProtoData::LongInteger(v) => Ok(AstarteData::LongInteger(v)),
            ProtoData::String(v) => Ok(AstarteData::String(v)),
            ProtoData::BinaryBlob(v) => Ok(AstarteData::BinaryBlob(v)),
            ProtoData::DoubleArray(arr) => {
                AstarteData::try_from(arr.values).map_err(MessageHubProtoError::Conversion)
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
                .try_collect()
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
            AstarteData::DateTime(value) => ProtoData::DateTime(value.into()),
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
                let values = values
                    .into_iter()
                    .map(pbjson_types::Timestamp::from)
                    .collect();

                ProtoData::DateTimeArray(astarte_message_hub_proto::AstarteDateTimeArray { values })
            }
        };

        Self {
            astarte_data: Some(astarte_data),
        }
    }
}

// The received payload from the connection
impl TryFrom<MessageHubEvent> for ReceivedEvent<GrpcPayload> {
    type Error = GrpcError;

    fn try_from(value: MessageHubEvent) -> Result<Self, Self::Error> {
        let event = value
            .event
            .ok_or(MessageHubProtoError::ExpectedField("event"))?;

        let message = match event {
            Event::Message(msg) => msg,
            Event::Error(err) => return Err(GrpcError::Server(err)),
        };

        let payload = message
            .payload
            .ok_or(MessageHubProtoError::ExpectedField("payload"))?;

        Ok(ReceivedEvent {
            interface: message.interface_name,
            path: message.path,
            payload: GrpcPayload::new(payload),
        })
    }
}

// For send individual
impl From<ValidatedIndividual> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: ValidatedIndividual) -> Self {
        let timestamp = value
            .timestamp
            .map(astarte_message_hub_proto::pbjson_types::Timestamp::from);

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
        let timestamp = value
            .timestamp
            .map(astarte_message_hub_proto::pbjson_types::Timestamp::from);

        let data = value
            .data
            .into_key_values()
            .map(|(k, v)| (k, v.into()))
            .collect();

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
        Self {
            interface_name: value.interface,
            path: value.path,
            payload: Some(ProtoPayload::PropertyIndividual(
                astarte_message_hub_proto::AstartePropertyIndividual {
                    data: Some(value.data.into()),
                },
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

// For deserialize object
impl TryFrom<AstarteDatastreamObject> for AstarteObject {
    type Error = MessageHubProtoError;

    fn try_from(value: AstarteDatastreamObject) -> Result<Self, Self::Error> {
        value
            .data
            .into_iter()
            .map(|(k, value)| AstarteData::try_from(value).map(|v| (k, v)))
            .collect()
    }
}

// For deserialize individual
pub(crate) fn try_from_individual(
    individual: AstarteDatastreamIndividual,
) -> Result<(AstarteData, Option<Timestamp>), MessageHubProtoError> {
    let data = individual
        .data
        .ok_or(MessageHubProtoError::ExpectedField("data"))?
        .try_into()?;

    let timestamp = individual.timestamp.map(convert_timestamp).transpose()?;

    Ok((data, timestamp))
}

// For deserialize property
pub(crate) fn try_from_property(
    property: AstartePropertyIndividual,
) -> Result<Option<AstarteData>, MessageHubProtoError> {
    property.data.map(AstarteData::try_from).transpose()
}

impl From<DeviceEvent> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: DeviceEvent) -> Self {
        let payload = match value.data {
            Value::Individual { data, timestamp } => {
                ProtoPayload::DatastreamIndividual(AstarteDatastreamIndividual {
                    data: Some(data.into()),
                    timestamp: Some(timestamp.into()),
                })
            }
            Value::Object { data, timestamp } => {
                let data = data.inner.into_iter().map(|(k, v)| (k, v.into())).collect();

                ProtoPayload::DatastreamObject(astarte_message_hub_proto::AstarteDatastreamObject {
                    data,
                    timestamp: Some(timestamp.into()),
                })
            }
            Value::Property(prop) => ProtoPayload::PropertyIndividual(
                astarte_message_hub_proto::AstartePropertyIndividual {
                    data: prop.map(ProtoDataWrapper::from),
                },
            ),
        };

        astarte_message_hub_proto::AstarteMessage {
            interface_name: value.interface,
            path: value.path,
            payload: Some(payload),
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::collections::HashMap;

    use astarte_message_hub_proto::{
        AstarteDatastreamObject, AstarteMessage, AstartePropertyIndividual, Property,
    };
    use chrono::Utc;
    use pretty_assertions::assert_eq;

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

    #[test]
    fn proto_conversions_success() {
        let cases = [
            AstarteData::Double(12.21.try_into().unwrap()),
            AstarteData::Integer(12),
            AstarteData::Boolean(false),
            AstarteData::LongInteger(42),
            AstarteData::String("hello".to_string()),
            AstarteData::BinaryBlob(vec![1, 2, 3, 4]),
            AstarteData::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteData::DoubleArray(
                [1.3, 2.6, 3.1, 4.0]
                    .map(|v| Double::try_from(v).unwrap())
                    .to_vec(),
            ),
            AstarteData::IntegerArray(vec![1, 2, 3, 4]),
            AstarteData::BooleanArray(vec![true, false, true, true]),
            AstarteData::LongIntegerArray(vec![32, 11, 33, 1]),
            AstarteData::StringArray(vec!["Hello".to_string(), " world!".to_string()]),
            AstarteData::BinaryBlobArray(vec![vec![1, 2, 3, 4], vec![4, 4, 1, 4]]),
            AstarteData::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1611580808, 0).unwrap(),
            ]),
        ];

        for exp in cases {
            let proto = ProtoDataWrapper::from(exp.clone());
            let astarte_type = AstarteData::try_from(proto).unwrap();

            assert_eq!(exp, astarte_type);
        }
    }

    #[test]
    fn convert_astarte_device_data_event_unset_to_astarte_message() {
        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Property(None),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);
        assert_eq!(
            ProtoPayload::PropertyIndividual(AstartePropertyIndividual { data: None }),
            astarte_message.payload.unwrap()
        );
    }

    fn get_astarte_data_from_payload(
        payload: ProtoPayload,
    ) -> Result<AstarteData, MessageHubProtoError> {
        let astarte_data = take_individual(payload)
            .expect("individual")
            .data
            .expect("data")
            .astarte_data
            .expect("astarte_data");

        ProtoDataWrapper {
            astarte_data: Some(astarte_data),
        }
        .try_into()
    }

    #[test]
    fn convert_astarte_device_data_event_individual_f64_to_astarte_message() {
        let expected_data = AstarteData::try_from(10.1).unwrap();

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i32_to_astarte_message() {
        let expected_data = AstarteData::Integer(10);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bool_to_astarte_message() {
        let expected_data = AstarteData::Boolean(true);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i64_to_astarte_message() {
        let expected_data = AstarteData::LongInteger(45);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_string_to_astarte_message() {
        let expected_data = AstarteData::String("test".to_owned());

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bytes_to_astarte_message() {
        let expected_data = AstarteData::BinaryBlob(vec![12, 48]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_date_time_to_astarte_message() {
        let expected_data = AstarteData::DateTime(Utc::now());

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_f64_array_to_astarte_message() {
        let expected_data = AstarteData::try_from(vec![13.5, 487.35]).unwrap();

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i32_array_to_astarte_message() {
        let expected_data = AstarteData::IntegerArray(vec![78, 45]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bool_array_to_astarte_message() {
        let expected_data = AstarteData::BooleanArray(vec![true, false, true]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i64_array_to_astarte_message() {
        let expected_data = AstarteData::LongIntegerArray(vec![658, 77845, 4444]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_string_array_to_astarte_message() {
        let expected_data =
            AstarteData::StringArray(vec!["test1".to_owned(), "test_098".to_string()]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bytes_array_to_astarte_message() {
        let expected_data = AstarteData::BinaryBlobArray(vec![vec![12, 48], vec![47, 55], vec![9]]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_date_time_array_to_astarte_message() {
        let expected_data = AstarteData::DateTimeArray(vec![Utc::now(), Utc::now()]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual {
                data: expected_data.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_astarte_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_object_to_astarte_message() {
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

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Object {
                data: expected_map.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let astarte_object = astarte_message.payload.and_then(take_object).unwrap();

        let object_data = astarte_object.data;
        for (k, v) in expected_map.into_key_values() {
            let astarte_type: AstarteData = object_data
                .get(&k)
                .and_then(|data| data.astarte_data.as_ref())
                .and_then(|data| {
                    ProtoDataWrapper {
                        astarte_data: Some(data.clone()),
                    }
                    .try_into()
                    .ok()
                })
                .unwrap();

            assert_eq!(v, astarte_type);
        }
    }

    #[test]
    fn convert_astarte_device_data_event_object2_to_astarte_message() {
        let expected_map = AstarteObject::from_iter([
            ("M".to_owned(), AstarteData::try_from(0.4).unwrap()),
            (
                "V".to_owned(),
                AstarteData::StringArray(vec!["test1".to_owned(), "test2".to_owned()]),
            ),
            ("R".to_owned(), AstarteData::Integer(112)),
            ("a".to_owned(), AstarteData::Boolean(false)),
        ]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Object {
                data: expected_map.clone(),
                timestamp: Utc::now(),
            },
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let object_data = astarte_message.payload.and_then(take_object).unwrap().data;

        for (k, v) in expected_map.inner.into_iter() {
            let astarte_type: AstarteData = object_data
                .get(&k)
                .and_then(|data| data.astarte_data.as_ref())
                .and_then(|data| {
                    ProtoDataWrapper {
                        astarte_data: Some(data.clone()),
                    }
                    .try_into()
                    .ok()
                })
                .unwrap();

            assert_eq!(v, astarte_type);
        }
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
    fn map_property_to_astarte_type_ok() {
        let prop = Property {
            interface_name: "com.test.interface".to_owned(),
            path: "/path11".to_owned(),
            version_major: 0,
            ownership: astarte_message_hub_proto::Ownership::Device.into(),
            data: Some(ProtoDataWrapper {
                astarte_data: Some(ProtoData::String("test".to_owned())),
            }),
        };

        let astarte_type = map_property_to_astarte_type(prop).unwrap().unwrap();

        assert_eq!(AstarteData::String("test".to_string()), astarte_type);
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
        let astarte_type_err = map_property_to_astarte_type(prop);

        assert!(matches!(astarte_type_err, Ok(None)));
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
