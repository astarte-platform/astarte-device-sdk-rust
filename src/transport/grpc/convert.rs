/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

//! Contains conversion traits to convert the Astarte types in the protobuf format to the
//! Astarte types from the Astarte device SDK.

use std::num::TryFromIntError;

use astarte_message_hub_proto::astarte_data::AstarteData as ProtoData;
use astarte_message_hub_proto::message_hub_event::Event;
use astarte_message_hub_proto::{astarte_message::Payload as ProtoPayload, pbjson_types};
use astarte_message_hub_proto::{
    AstarteDatastreamIndividual, AstarteDatastreamObject, MessageHubEvent,
};
use chrono::TimeZone;
use itertools::Itertools;

use crate::aggregate::AstarteObject;
use crate::interface::Ownership;
use crate::store::StoredProp;
use crate::validate::ValidatedUnset;
use crate::{
    transport::ReceivedEvent, types::AstarteType, validate::ValidatedIndividual,
    validate::ValidatedObject,
};
use crate::{DeviceEvent, Value};

use super::{GrpcError, GrpcPayload};

/// Error returned by the Message Hub types conversions.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum MessageHubProtoError {
    /// Wrapper for integer conversion errors
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),

    /// Expected field was not found
    #[error("Missing the expected field '{0}'")]
    ExpectedField(&'static str),

    /// Date conversion error
    #[error("Error while converting a proto date: {0}")]
    DateConversion(String),

    /// Expected set property got an unset
    #[error("Expected set property got an unset")]
    ExpectedSetProperty,
}

/// Map a received message hub property to an optional astarte type
pub(crate) fn map_property_to_astarte_type(
    value: astarte_message_hub_proto::Property,
) -> Result<Option<AstarteType>, MessageHubProtoError> {
    let astarte_message_hub_proto::Property { data, .. } = value;

    let Some(individual) = data else {
        return Ok(None);
    };

    Ok(Some(individual.try_into()?))
}

/// Map a list of properties, unset properties will be ignored
pub(crate) fn map_set_stored_properties(
    mut message_hub_properties: astarte_message_hub_proto::StoredProperties,
) -> Result<Vec<StoredProp>, MessageHubProtoError> {
    message_hub_properties
        .interface_properties
        .iter_mut()
        .flat_map(|(name, prop_data)| {
            prop_data.properties.iter().filter_map(|p| {
                let path = p.path.clone();
                let value: AstarteType =
                    match map_property_to_astarte_type(p.clone()).transpose()? {
                        Ok(s) => s,
                        Err(e) => return Some(Err(e)),
                    };

                let res = StoredProp {
                    interface: name.clone(),
                    path,
                    value,
                    interface_major: prop_data.version_major,
                    ownership: prop_data.ownership().into(),
                };

                Some(Ok(res))
            })
        })
        .try_collect()
}

impl From<astarte_message_hub_proto::Ownership> for Ownership {
    fn from(value: astarte_message_hub_proto::Ownership) -> Self {
        match value {
            astarte_message_hub_proto::Ownership::Device => Ownership::Device,
            astarte_message_hub_proto::Ownership::Server => Ownership::Server,
        }
    }
}

impl TryFrom<astarte_message_hub_proto::AstarteData> for AstarteType {
    type Error = MessageHubProtoError;

    fn try_from(value: astarte_message_hub_proto::AstarteData) -> Result<Self, Self::Error> {
        let astarte_data = value
            .astarte_data
            .ok_or(MessageHubProtoError::ExpectedField("astarte_data"))?;

        match astarte_data {
            ProtoData::DateTime(v) => convert_timestamp(v).map(AstarteType::DateTime),
            ProtoData::Double(v) => Ok(AstarteType::Double(v)),
            ProtoData::Integer(v) => Ok(AstarteType::Integer(v)),
            ProtoData::Boolean(v) => Ok(AstarteType::Boolean(v)),
            ProtoData::LongInteger(v) => Ok(AstarteType::LongInteger(v)),
            ProtoData::String(v) => Ok(AstarteType::String(v)),
            ProtoData::BinaryBlob(v) => Ok(AstarteType::BinaryBlob(v)),
            ProtoData::DoubleArray(arr) => Ok(AstarteType::DoubleArray(arr.values)),
            ProtoData::IntegerArray(arr) => Ok(AstarteType::IntegerArray(arr.values)),
            ProtoData::BooleanArray(arr) => Ok(AstarteType::BooleanArray(arr.values)),
            ProtoData::LongIntegerArray(arr) => Ok(AstarteType::LongIntegerArray(arr.values)),
            ProtoData::StringArray(arr) => Ok(AstarteType::StringArray(arr.values)),
            ProtoData::BinaryBlobArray(arr) => Ok(AstarteType::BinaryBlobArray(arr.values)),
            ProtoData::DateTimeArray(arr) => arr
                .values
                .into_iter()
                .map(convert_timestamp)
                .try_collect()
                .map(AstarteType::DateTimeArray),
        }
    }
}

/// Converts a [`pbjson_types::Timestamp`] into a [`chrono::DateTime<Utc>`]
fn convert_timestamp(
    val: pbjson_types::Timestamp,
) -> Result<crate::Timestamp, MessageHubProtoError> {
    let nanos = val
        .nanos
        .try_into()
        .map_err(|err: TryFromIntError| MessageHubProtoError::DateConversion(err.to_string()))?;

    chrono::Utc
        .timestamp_opt(val.seconds, nanos)
        .earliest()
        .ok_or_else(|| MessageHubProtoError::DateConversion(format!("{val:?}")))
}

impl From<AstarteType> for astarte_message_hub_proto::AstarteData {
    fn from(value: AstarteType) -> Self {
        let astarte_data = match value {
            AstarteType::Double(value) => ProtoData::Double(value),
            AstarteType::Integer(value) => ProtoData::Integer(value),
            AstarteType::Boolean(value) => ProtoData::Boolean(value),
            AstarteType::LongInteger(value) => ProtoData::LongInteger(value),
            AstarteType::String(value) => ProtoData::String(value),
            AstarteType::BinaryBlob(value) => ProtoData::BinaryBlob(value),
            AstarteType::DateTime(value) => ProtoData::DateTime(value.into()),
            AstarteType::DoubleArray(values) => {
                ProtoData::DoubleArray(astarte_message_hub_proto::AstarteDoubleArray { values })
            }
            AstarteType::IntegerArray(values) => {
                ProtoData::IntegerArray(astarte_message_hub_proto::AstarteIntegerArray { values })
            }
            AstarteType::BooleanArray(values) => {
                ProtoData::BooleanArray(astarte_message_hub_proto::AstarteBooleanArray { values })
            }
            AstarteType::LongIntegerArray(values) => {
                ProtoData::LongIntegerArray(astarte_message_hub_proto::AstarteLongIntegerArray {
                    values,
                })
            }
            AstarteType::StringArray(values) => {
                ProtoData::StringArray(astarte_message_hub_proto::AstarteStringArray { values })
            }
            AstarteType::BinaryBlobArray(values) => {
                ProtoData::BinaryBlobArray(astarte_message_hub_proto::AstarteBinaryBlobArray {
                    values,
                })
            }
            AstarteType::DateTimeArray(values) => {
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

/// This function can be used to convert a map of (String, AstarteDataTypeIndividual) into a
/// map of (String, AstarteType).
///
/// It's used also outside in the message hub.
impl TryFrom<AstarteDatastreamObject> for AstarteObject {
    type Error = MessageHubProtoError;

    fn try_from(value: AstarteDatastreamObject) -> Result<Self, Self::Error> {
        value
            .data
            .into_iter()
            .map(|(k, value)| AstarteType::try_from(value).map(|v| (k, v)))
            .collect()
    }
}

// This is needed for a conversion in the message hub, but cannot be implemented outside of the crate
// because the AstarteMessage is from the proto crate, while the AstarteDeviceDataEvent is ours.
impl TryFrom<astarte_message_hub_proto::AstarteMessage> for DeviceEvent {
    type Error = MessageHubProtoError;

    fn try_from(value: astarte_message_hub_proto::AstarteMessage) -> Result<Self, Self::Error> {
        let payload = value
            .payload
            .ok_or(MessageHubProtoError::ExpectedField("payload"))?;

        let data = payload.try_into()?;

        Ok(Self {
            interface: value.interface_name,
            path: value.path,
            data,
        })
    }
}

impl TryFrom<ProtoPayload> for Value {
    type Error = MessageHubProtoError;

    fn try_from(value: ProtoPayload) -> Result<Self, Self::Error> {
        use astarte_message_hub_proto::AstarteDatastreamIndividual;
        use astarte_message_hub_proto::AstartePropertyIndividual;

        match value {
            // Unset
            ProtoPayload::PropertyIndividual(AstartePropertyIndividual { data: None }) => {
                Ok(Value::Unset)
            }
            // Individual
            ProtoPayload::DatastreamIndividual(AstarteDatastreamIndividual {
                data: Some(data),
                timestamp: _,
            })
            | ProtoPayload::PropertyIndividual(AstartePropertyIndividual { data: Some(data) }) => {
                let value = data.try_into()?;

                Ok(Value::Individual(value))
            }
            // Individual error case
            ProtoPayload::DatastreamIndividual(AstarteDatastreamIndividual {
                data: None,
                timestamp: _,
            }) => Err(MessageHubProtoError::ExpectedField("data")),
            // Object
            ProtoPayload::DatastreamObject(object) => {
                let value = AstarteObject::try_from(object)?;

                Ok(Value::Object(value))
            }
        }
    }
}

// This is needed for a conversion in the message hub, but cannot be implemented outside of the crate
// because the AstarteMessage is from the proto crate, while the AstarteDeviceDataEvent is ours.
impl From<DeviceEvent> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: DeviceEvent) -> Self {
        let payload = match value.data {
            Value::Individual(val) => {
                ProtoPayload::DatastreamIndividual(AstarteDatastreamIndividual {
                    data: Some(val.into()),
                    // FIXME: how do i retrieve the timestamp if the Value type doesn't have it?
                    timestamp: None,
                })
            }
            Value::Object(val) => {
                let data = val.inner.into_iter().map(|(k, v)| (k, v.into())).collect();

                ProtoPayload::DatastreamObject(astarte_message_hub_proto::AstarteDatastreamObject {
                    data,
                    // FIXME: how do i retrieve the timestamp if the Value type doesn't have it?
                    timestamp: None,
                })
            }
            Value::Unset => ProtoPayload::PropertyIndividual(
                astarte_message_hub_proto::AstartePropertyIndividual { data: None },
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
        AstarteData, AstarteDatastreamObject, AstarteMessage, AstartePropertyIndividual,
        InterfaceProperties,
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

    pub(crate) fn new_property(
        path: String,
        data: Option<AstarteType>,
    ) -> astarte_message_hub_proto::Property {
        astarte_message_hub_proto::Property {
            path,
            data: data.map(|s| s.into()),
        }
    }

    #[test]
    fn proto_conversions_success() {
        let cases = [
            AstarteType::Double(12.21),
            AstarteType::Integer(12),
            AstarteType::Boolean(false),
            AstarteType::LongInteger(42),
            AstarteType::String("hello".to_string()),
            AstarteType::BinaryBlob(vec![1, 2, 3, 4]),
            AstarteType::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteType::DoubleArray(vec![1.3, 2.6, 3.1, 4.0]),
            AstarteType::IntegerArray(vec![1, 2, 3, 4]),
            AstarteType::BooleanArray(vec![true, false, true, true]),
            AstarteType::LongIntegerArray(vec![32, 11, 33, 1]),
            AstarteType::StringArray(vec!["Hello".to_string(), " world!".to_string()]),
            AstarteType::BinaryBlobArray(vec![vec![1, 2, 3, 4], vec![4, 4, 1, 4]]),
            AstarteType::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1611580808, 0).unwrap(),
            ]),
        ];

        for exp in cases {
            let proto = AstarteData::from(exp.clone());
            let astarte_type = AstarteType::try_from(proto).unwrap();

            assert_eq!(exp, astarte_type);
        }
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_individual_success() {
        let expected_data: f64 = 15.5;
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let astarte_type: AstarteType = expected_data.try_into().unwrap();
        let payload: ProtoPayload =
            ProtoPayload::DatastreamIndividual(AstarteDatastreamIndividual {
                data: Some(astarte_type.into()),
                timestamp: None,
            });

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            payload: Some(payload),
        };

        let astarte_device_data_event: DeviceEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        astarte_device_data_event.data.as_individual().unwrap();
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_unset_success() {
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let payload: ProtoPayload =
            ProtoPayload::PropertyIndividual(AstartePropertyIndividual { data: None });

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            payload: Some(payload),
        };

        let astarte_device_data_event: DeviceEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        assert_eq!(Value::Unset, astarte_device_data_event.data);
    }

    #[test]
    fn convert_astarte_device_data_event_unset_to_astarte_message() {
        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Unset,
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
    ) -> Result<AstarteType, MessageHubProtoError> {
        let astarte_data = take_individual(payload)
            .expect("individual")
            .data
            .expect("data")
            .astarte_data
            .expect("astarte_data");

        AstarteData {
            astarte_data: Some(astarte_data),
        }
        .try_into()
    }

    #[test]
    fn convert_astarte_device_data_event_individual_f64_to_astarte_message() {
        let expected_data = AstarteType::Double(10.1);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::Integer(10);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::Boolean(true);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::LongInteger(45);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::String("test".to_owned());

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::BinaryBlob(vec![12, 48]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::DateTime(Utc::now());

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::DoubleArray(vec![13.5, 487.35]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::IntegerArray(vec![78, 45]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::BooleanArray(vec![true, false, true]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::LongIntegerArray(vec![658, 77845, 4444]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
            AstarteType::StringArray(vec!["test1".to_owned(), "test_098".to_string()]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::BinaryBlobArray(vec![vec![12, 48], vec![47, 55], vec![9]]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
        let expected_data = AstarteType::DateTimeArray(vec![Utc::now(), Utc::now()]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Individual(expected_data.clone()),
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
            ("Mercury".to_owned(), AstarteType::Double(0.4)),
            ("Venus".to_owned(), AstarteType::Double(0.7)),
            ("Earth".to_owned(), AstarteType::Double(1.0)),
            ("Mars".to_owned(), AstarteType::Double(1.5)),
        ]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Object(expected_map.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let astarte_object = astarte_message.payload.and_then(take_object).unwrap();

        let object_data = astarte_object.data;
        for (k, v) in expected_map.into_key_values() {
            let astarte_type: AstarteType = object_data
                .get(&k)
                .and_then(|data| data.astarte_data.as_ref())
                .and_then(|data| {
                    AstarteData {
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
            ("M".to_owned(), AstarteType::Double(0.4)),
            (
                "V".to_owned(),
                AstarteType::StringArray(vec!["test1".to_owned(), "test2".to_owned()]),
            ),
            ("R".to_owned(), AstarteType::Integer(112)),
            ("a".to_owned(), AstarteType::Boolean(false)),
        ]);

        let astarte_device_data_event = DeviceEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Value::Object(expected_map.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().into();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let object_data = astarte_message.payload.and_then(take_object).unwrap().data;

        for (k, v) in expected_map.inner.into_iter() {
            let astarte_type: AstarteType = object_data
                .get(&k)
                .and_then(|data| data.astarte_data.as_ref())
                .and_then(|data| {
                    AstarteData {
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
        let astarte_sdk_type_double = AstarteType::Double(expected_double_value);

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
        use astarte_message_hub_proto::astarte_data::AstarteData as ProtoData;
        use astarte_message_hub_proto::AstarteData;

        let expected_data: f64 = 15.5;

        let payload_result = ProtoPayload::DatastreamObject(AstarteDatastreamObject {
            data: HashMap::from([(
                "key1".to_string(),
                AstarteData {
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

    fn make_messagehub_property(
        path: &str,
        value: Option<AstarteType>,
    ) -> astarte_message_hub_proto::Property {
        astarte_message_hub_proto::Property {
            path: path.to_owned(),
            data: value.map(|v| v.into()),
        }
    }

    #[test]
    fn map_property_to_astarte_type_ok() {
        let value: AstarteType = AstarteType::String("test".to_owned());

        let prop = make_messagehub_property("/path11", Some(value.clone()));

        let astarte_type = map_property_to_astarte_type(prop).unwrap().unwrap();

        assert_eq!(value, astarte_type);
    }

    #[test]
    fn map_property_to_astarte_type_none() {
        let prop = make_messagehub_property("/path11", None);

        let astarte_type_err = map_property_to_astarte_type(prop);

        assert!(matches!(astarte_type_err, Ok(None)));
    }

    #[test]
    fn from_message_hub_stored_properties_to_internal_ok() {
        const INTERFACE_1: &str = "com.test.interface1";
        const INTERFACE_2: &str = "com.test.interface2";

        let interface_properties_map = vec![
            (
                INTERFACE_1.to_owned(),
                InterfaceProperties {
                    ownership: astarte_message_hub_proto::Ownership::Device.into(),
                    version_major: 0,
                    properties: vec![
                        make_messagehub_property(
                            "/path11",
                            Some(AstarteType::String("test".to_owned())),
                        ),
                        make_messagehub_property("/path12", Some(AstarteType::Integer(0))),
                    ],
                },
            ),
            (
                INTERFACE_2.to_owned(),
                InterfaceProperties {
                    ownership: astarte_message_hub_proto::Ownership::Server.into(),
                    version_major: 0,
                    properties: vec![
                        make_messagehub_property(
                            "/path21",
                            Some(AstarteType::BinaryBlob(vec![0, 54, 0, 23])),
                        ),
                        make_messagehub_property(
                            "/path22",
                            Some(AstarteType::Double(std::f64::consts::PI)),
                        ),
                    ],
                },
            ),
        ]
        .into_iter()
        .collect();

        let message_hub_stored_properties: astarte_message_hub_proto::StoredProperties =
            astarte_message_hub_proto::StoredProperties {
                interface_properties: interface_properties_map,
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
