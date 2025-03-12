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

use std::collections::HashMap;
use std::num::TryFromIntError;
use std::str::FromStr;

use astarte_message_hub_proto::astarte_data_type::Data as ProtoData;
use astarte_message_hub_proto::message_hub_event::Event;
use astarte_message_hub_proto::types::InterfaceJson;
use astarte_message_hub_proto::{
    astarte_data_type_individual::IndividualData as ProtoIndividualData,
    astarte_message::Payload as ProtoPayload, pbjson_types,
};
use astarte_message_hub_proto::{AstarteDataTypeObject, MessageHubEvent};
use chrono::TimeZone;
use itertools::Itertools;

use crate::interface::Ownership;
use crate::store::StoredProp;
use crate::validate::ValidatedUnset;
use crate::{
    transport::ReceivedEvent, types::AstarteType, validate::ValidatedIndividual,
    validate::ValidatedObject,
};
use crate::{DeviceEvent, Interface, Value};

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
    let astarte_message_hub_proto::Property { value, .. } = value;

    let value = value.ok_or(MessageHubProtoError::ExpectedField("value"))?;
    let individual = match value {
        astarte_message_hub_proto::property::Value::AstarteProperty(p) => p
            .individual_data
            .ok_or(MessageHubProtoError::ExpectedField("individual_data"))?,
        astarte_message_hub_proto::property::Value::AstarteUnset(_) => return Ok(None),
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

impl TryFrom<ProtoIndividualData> for AstarteType {
    type Error = MessageHubProtoError;

    fn try_from(value: ProtoIndividualData) -> Result<Self, Self::Error> {
        match value {
            ProtoIndividualData::AstarteDouble(val) => Ok(AstarteType::Double(val)),
            ProtoIndividualData::AstarteDoubleArray(val) => {
                Ok(AstarteType::DoubleArray(val.values))
            }
            ProtoIndividualData::AstarteInteger(val) => Ok(AstarteType::Integer(val)),
            ProtoIndividualData::AstarteBoolean(val) => Ok(AstarteType::Boolean(val)),
            ProtoIndividualData::AstarteLongInteger(val) => Ok(AstarteType::LongInteger(val)),
            ProtoIndividualData::AstarteString(val) => Ok(AstarteType::String(val)),
            ProtoIndividualData::AstarteBinaryBlob(val) => Ok(AstarteType::BinaryBlob(val)),
            ProtoIndividualData::AstarteIntegerArray(val) => {
                Ok(AstarteType::IntegerArray(val.values))
            }
            ProtoIndividualData::AstarteBooleanArray(val) => {
                Ok(AstarteType::BooleanArray(val.values))
            }
            ProtoIndividualData::AstarteLongIntegerArray(val) => {
                Ok(AstarteType::LongIntegerArray(val.values))
            }
            ProtoIndividualData::AstarteStringArray(val) => {
                Ok(AstarteType::StringArray(val.values))
            }
            ProtoIndividualData::AstarteBinaryBlobArray(val) => {
                Ok(AstarteType::BinaryBlobArray(val.values))
            }
            ProtoIndividualData::AstarteDateTime(val) => {
                convert_timestamp(val).map(AstarteType::DateTime)
            }
            ProtoIndividualData::AstarteDateTimeArray(val) => val
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

impl TryFrom<astarte_message_hub_proto::AstarteDataTypeIndividual> for AstarteType {
    type Error = MessageHubProtoError;

    fn try_from(
        value: astarte_message_hub_proto::AstarteDataTypeIndividual,
    ) -> Result<Self, Self::Error> {
        value
            .individual_data
            .ok_or(MessageHubProtoError::ExpectedField("individual_data"))
            .and_then(TryInto::try_into)
    }
}

/// Implements the TryFrom trait for the AstarteDataTypeIndividual for any AstarteType.
impl From<AstarteType> for ProtoIndividualData {
    fn from(value: AstarteType) -> Self {
        use astarte_message_hub_proto::{
            AstarteBinaryBlobArray, AstarteBooleanArray, AstarteDateTimeArray, AstarteDoubleArray,
            AstarteIntegerArray, AstarteLongIntegerArray, AstarteStringArray,
        };

        match value {
            AstarteType::Double(value) => ProtoIndividualData::AstarteDouble(value),
            AstarteType::Integer(value) => ProtoIndividualData::AstarteInteger(value),
            AstarteType::Boolean(value) => ProtoIndividualData::AstarteBoolean(value),
            AstarteType::LongInteger(value) => ProtoIndividualData::AstarteLongInteger(value),
            AstarteType::String(value) => ProtoIndividualData::AstarteString(value),
            AstarteType::BinaryBlob(value) => ProtoIndividualData::AstarteBinaryBlob(value),
            AstarteType::DateTime(value) => ProtoIndividualData::AstarteDateTime(value.into()),
            AstarteType::DoubleArray(values) => {
                ProtoIndividualData::AstarteDoubleArray(AstarteDoubleArray { values })
            }
            AstarteType::IntegerArray(values) => {
                ProtoIndividualData::AstarteIntegerArray(AstarteIntegerArray { values })
            }
            AstarteType::BooleanArray(values) => {
                ProtoIndividualData::AstarteBooleanArray(AstarteBooleanArray { values })
            }
            AstarteType::LongIntegerArray(values) => {
                ProtoIndividualData::AstarteLongIntegerArray(AstarteLongIntegerArray { values })
            }
            AstarteType::StringArray(values) => {
                ProtoIndividualData::AstarteStringArray(AstarteStringArray { values })
            }
            AstarteType::BinaryBlobArray(values) => {
                ProtoIndividualData::AstarteBinaryBlobArray(AstarteBinaryBlobArray { values })
            }
            AstarteType::DateTimeArray(values) => {
                ProtoIndividualData::AstarteDateTimeArray(AstarteDateTimeArray {
                    values: values
                        .into_iter()
                        .map(pbjson_types::Timestamp::from)
                        .collect(),
                })
            }
        }
    }
}

impl From<AstarteType> for astarte_message_hub_proto::AstarteDataTypeIndividual {
    fn from(value: AstarteType) -> Self {
        Self {
            individual_data: Some(value.into()),
        }
    }
}

impl TryFrom<InterfaceJson> for Interface {
    type Error = crate::Error;

    fn try_from(interface: InterfaceJson) -> Result<Self, Self::Error> {
        Interface::from_str(&interface.0).map_err(Self::Error::Interface)
    }
}

impl From<AstarteType> for ProtoPayload {
    fn from(value: AstarteType) -> Self {
        ProtoPayload::AstarteData(astarte_message_hub_proto::AstarteDataType {
            data: Some(ProtoData::AstarteIndividual(value.into())),
        })
    }
}

impl TryFrom<MessageHubEvent> for ReceivedEvent<GrpcPayload> {
    type Error = GrpcError;

    fn try_from(value: MessageHubEvent) -> Result<Self, Self::Error> {
        let event = value
            .event
            .ok_or(MessageHubProtoError::ExpectedField("event"))?;

        match event {
            Event::Message(msg) => msg.try_into().map_err(GrpcError::MessageHubProtoConversion),
            Event::Error(err) => Err(GrpcError::Server(err)),
        }
    }
}

impl TryFrom<astarte_message_hub_proto::AstarteMessage> for ReceivedEvent<GrpcPayload> {
    type Error = MessageHubProtoError;

    fn try_from(message: astarte_message_hub_proto::AstarteMessage) -> Result<Self, Self::Error> {
        let payload = message
            .payload
            .ok_or(MessageHubProtoError::ExpectedField("payload"))?;

        let timestamp = message.timestamp.map(convert_timestamp).transpose()?;

        Ok(ReceivedEvent {
            interface: message.interface_name,
            path: message.path,
            payload: GrpcPayload::new(payload, timestamp),
        })
    }
}

impl From<ValidatedIndividual> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: ValidatedIndividual) -> Self {
        let timestamp = value.timestamp.map(|t| t.into());

        let payload = Some(value.data.into());

        astarte_message_hub_proto::AstarteMessage {
            interface_name: value.interface,
            path: value.path,
            timestamp,
            payload,
        }
    }
}

impl From<ValidatedObject> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: ValidatedObject) -> Self {
        let timestamp = value.timestamp.map(|t| t.into());

        let object_data = value.data.into_iter().map(|(k, v)| (k, v.into())).collect();

        let payload = Some(ProtoPayload::AstarteData(
            astarte_message_hub_proto::AstarteDataType {
                data: Some(
                    astarte_message_hub_proto::astarte_data_type::Data::AstarteObject(
                        astarte_message_hub_proto::AstarteDataTypeObject { object_data },
                    ),
                ),
            },
        ));

        astarte_message_hub_proto::AstarteMessage {
            interface_name: value.interface,
            path: value.path,
            timestamp,
            payload,
        }
    }
}

impl From<ValidatedUnset> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: ValidatedUnset) -> Self {
        Self {
            interface_name: value.interface,
            path: value.path,
            timestamp: None,
            payload: Some(ProtoPayload::AstarteUnset(
                astarte_message_hub_proto::AstarteUnset {},
            )),
        }
    }
}

/// This function can be used to convert a map of (String, AstarteDataTypeIndividual) into a
/// map of (String, AstarteType).
///
/// It's used also outside in the message hub.
pub fn map_values_to_astarte_type(
    value: AstarteDataTypeObject,
) -> Result<HashMap<String, AstarteType>, MessageHubProtoError> {
    // Cannot be implemented as TryFrom since the types are not from our crate
    value
        .object_data
        .into_iter()
        .map(|(k, value)| AstarteType::try_from(value).map(|v| (k, v)))
        .collect()
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

// This is needed for a conversion in the message hub, but cannot be implemented outside of the crate
// because the AstarteMessage is from the proto crate, while the AstarteDeviceDataEvent is ours.
impl TryFrom<ProtoPayload> for Value {
    type Error = MessageHubProtoError;

    fn try_from(value: ProtoPayload) -> Result<Self, Self::Error> {
        match value {
            // Unset
            ProtoPayload::AstarteUnset(astarte_message_hub_proto::AstarteUnset {}) => {
                Ok(Value::Unset)
            }
            // Individual
            ProtoPayload::AstarteData(astarte_message_hub_proto::AstarteDataType {
                data: Some(ProtoData::AstarteIndividual(individual)),
            }) => {
                let value = individual.try_into()?;

                Ok(Value::Individual(value))
            }
            // Object
            ProtoPayload::AstarteData(astarte_message_hub_proto::AstarteDataType {
                data: Some(ProtoData::AstarteObject(obj)),
            }) => {
                let value = map_values_to_astarte_type(obj)?;

                Ok(Value::Object(value))
            }
            // Error case
            ProtoPayload::AstarteData(astarte_message_hub_proto::AstarteDataType {
                data: None,
            }) => Err(MessageHubProtoError::ExpectedField("data")),
        }
    }
}

// This is needed for a conversion in the message hub, but cannot be implemented outside of the crate
// because the AstarteMessage is from the proto crate, while the AstarteDeviceDataEvent is ours.
impl From<DeviceEvent> for astarte_message_hub_proto::AstarteMessage {
    fn from(value: DeviceEvent) -> Self {
        let payload: ProtoPayload = value.data.into();

        astarte_message_hub_proto::AstarteMessage {
            interface_name: value.interface,
            path: value.path,
            timestamp: None,
            payload: Some(payload),
        }
    }
}

impl From<Value> for ProtoPayload {
    fn from(value: Value) -> Self {
        use astarte_message_hub_proto::astarte_data_type::Data;

        match value {
            Value::Individual(val) => {
                ProtoPayload::AstarteData(astarte_message_hub_proto::AstarteDataType {
                    data: Some(Data::AstarteIndividual(val.into())),
                })
            }
            Value::Object(val) => {
                let object_data = val.into_iter().map(|(k, v)| (k, v.into())).collect();

                ProtoPayload::AstarteData(astarte_message_hub_proto::AstarteDataType {
                    data: Some(Data::AstarteObject(AstarteDataTypeObject { object_data })),
                })
            }
            Value::Unset => ProtoPayload::AstarteUnset(astarte_message_hub_proto::AstarteUnset {}),
        }
    }
}

#[cfg(test)]
mod test {
    use core::panic;

    use astarte_message_hub_proto::{
        astarte_data_type_individual::IndividualData, AstarteMessage, InterfaceProperties,
    };
    use chrono::{DateTime, Utc};

    use crate::transport::grpc::convert::map_property_to_astarte_type;

    use super::*;

    #[test]
    fn proto_astarte_double_into_astarte_device_sdk_type_success() {
        let value = 15.5;
        let expected_double_value = IndividualData::AstarteDouble(value);
        let astarte_type = expected_double_value.try_into().unwrap();

        if let AstarteType::Double(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_integer_into_astarte_device_sdk_type_success() {
        let value: i32 = 15;
        let expected_integer_value = IndividualData::AstarteInteger(value);
        let astarte_type: AstarteType = expected_integer_value.try_into().unwrap();

        if let AstarteType::Integer(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_boolean_into_astarte_device_sdk_type_success() {
        let value: bool = true;
        let expected_boolean_value = IndividualData::AstarteBoolean(value);
        let astarte_type: AstarteType = expected_boolean_value.try_into().unwrap();

        if let AstarteType::Boolean(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_long_integer_into_astarte_device_sdk_type_success() {
        let value: i64 = 154;
        let expected_long_integer_value = IndividualData::AstarteLongInteger(value);
        let astarte_type: AstarteType = expected_long_integer_value.try_into().unwrap();

        if let AstarteType::LongInteger(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_string_into_astarte_device_sdk_type_success() {
        let value: String = "test".to_owned();
        let expected_string_value = IndividualData::AstarteString(value.clone());
        let astarte_type: AstarteType = expected_string_value.try_into().unwrap();

        if let AstarteType::String(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_binary_blob_into_astarte_device_sdk_type_success() {
        let value: Vec<u8> = vec![10, 34];
        let expected_binary_blob_value = IndividualData::AstarteBinaryBlob(value.clone());
        let astarte_type: AstarteType = expected_binary_blob_value.try_into().unwrap();

        if let AstarteType::BinaryBlob(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_date_time_into_astarte_device_sdk_type_success() {
        let value: DateTime<Utc> = Utc::now();
        let expected_date_time_value = IndividualData::AstarteDateTime(value.into());
        let astarte_type: AstarteType = expected_date_time_value.try_into().unwrap();

        if let AstarteType::DateTime(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_double_array_into_astarte_device_sdk_type_success() {
        let value: Vec<f64> = vec![15.5, 18.7];
        use astarte_message_hub_proto::AstarteDoubleArray;
        let expected_double_array_value = IndividualData::AstarteDoubleArray(AstarteDoubleArray {
            values: value.clone(),
        });
        let astarte_type: AstarteType = expected_double_array_value.try_into().unwrap();

        if let AstarteType::DoubleArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_integer_array_into_astarte_device_sdk_type_success() {
        let value: Vec<i32> = vec![15, 18];
        use astarte_message_hub_proto::AstarteIntegerArray;
        let expected_integer_array_value =
            IndividualData::AstarteIntegerArray(AstarteIntegerArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_integer_array_value.try_into().unwrap();

        if let AstarteType::IntegerArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_boolean_array_into_astarte_device_sdk_type_success() {
        let value: Vec<bool> = vec![false, true];
        use astarte_message_hub_proto::AstarteBooleanArray;
        let expected_boolean_array_value =
            IndividualData::AstarteBooleanArray(AstarteBooleanArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_boolean_array_value.try_into().unwrap();

        if let AstarteType::BooleanArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_long_integer_array_into_astarte_device_sdk_type_success() {
        let value: Vec<i64> = vec![1543, 18];
        use astarte_message_hub_proto::AstarteLongIntegerArray;
        let expected_long_integer_array_value =
            IndividualData::AstarteLongIntegerArray(AstarteLongIntegerArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_long_integer_array_value.try_into().unwrap();

        if let AstarteType::LongIntegerArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_string_array_into_astarte_device_sdk_type_success() {
        let value: Vec<String> = vec!["test1".to_owned(), "test2".to_owned()];
        use astarte_message_hub_proto::AstarteStringArray;
        let expected_string_array_value = IndividualData::AstarteStringArray(AstarteStringArray {
            values: value.clone(),
        });
        let astarte_type: AstarteType = expected_string_array_value.try_into().unwrap();

        if let AstarteType::StringArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_binary_blob_array_into_astarte_device_sdk_type_success() {
        let value: Vec<Vec<u8>> = vec![vec![11, 201], vec![1, 241]];
        use astarte_message_hub_proto::AstarteBinaryBlobArray;
        let expected_binary_blob_array_value =
            IndividualData::AstarteBinaryBlobArray(AstarteBinaryBlobArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_binary_blob_array_value.try_into().unwrap();

        if let AstarteType::BinaryBlobArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_date_time_array_into_astarte_device_sdk_type_success() {
        use astarte_message_hub_proto::AstarteDateTimeArray;
        use pbjson_types::Timestamp;

        let value: Vec<DateTime<Utc>> = vec![Utc::now(), Utc::now()];
        let expected_date_time_array_value =
            IndividualData::AstarteDateTimeArray(AstarteDateTimeArray {
                values: value
                    .clone()
                    .into_iter()
                    .map(|it| it.into())
                    .collect::<Vec<Timestamp>>(),
            });
        let astarte_type: AstarteType = expected_date_time_array_value.try_into().unwrap();

        if let AstarteType::DateTimeArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_individual_success() {
        let expected_data: f64 = 15.5;
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let astarte_type: AstarteType = expected_data.try_into().unwrap();
        let payload: ProtoPayload = astarte_type.into();

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            timestamp: None,
            payload: Some(payload),
        };

        let astarte_device_data_event: DeviceEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        astarte_device_data_event.data.as_individual().unwrap();
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_object_success() {
        use astarte_message_hub_proto::AstarteDataTypeIndividual;
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let expected_data_f64: f64 = 15.5;
        let expected_data_i32: i32 = 15;
        let mut object_map: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        object_map.insert("1".to_string(), expected_data_f64.into());
        object_map.insert("2".to_string(), expected_data_i32.into());

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            timestamp: None,
            payload: Some(ProtoPayload::AstarteData(object_map.into())),
        };

        let astarte_device_data_event: DeviceEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        let object_map = astarte_device_data_event.data.as_object().unwrap();

        assert_eq!(
            object_map.get("1").unwrap().clone(),
            AstarteType::try_from(expected_data_f64).unwrap()
        );
        assert_eq!(
            object_map.get("2").unwrap().clone(),
            AstarteType::from(expected_data_i32)
        );
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_unset_success() {
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let astarte_type = Value::Unset;
        let payload: ProtoPayload = astarte_type.into();

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            timestamp: None,
            payload: Some(payload),
        };

        let astarte_device_data_event: DeviceEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        assert_eq!(Value::Unset, astarte_device_data_event.data);
    }

    #[test]
    fn convert_map_values_to_astarte_astarte_data_type_individual_success() {
        let expected_data: f64 = 15.5;
        let astarte_type_map =
            HashMap::from([("key1".to_string(), AstarteType::Double(expected_data))]);

        let conversion_map_result: HashMap<String, ProtoIndividualData> = astarte_type_map
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect();

        let individual_data = conversion_map_result.get("key1").unwrap();
        assert_eq!(
            ProtoIndividualData::AstarteDouble(expected_data),
            *individual_data
        );
    }

    #[test]
    fn convert_proto_interface_to_astarte_interface() {
        const SERV_PROPS_IFACE: &str = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 1,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;

        let interface = InterfaceJson(SERV_PROPS_IFACE.into());

        let astarte_interface: Interface = interface.try_into().unwrap();

        assert_eq!(
            astarte_interface.interface_name(),
            "org.astarte-platform.test.test"
        );
        assert_eq!(astarte_interface.version_major(), 1);
    }

    #[tokio::test]
    async fn convert_proto_interface_with_special_chars_to_astarte_interface() {
        const IFACE_SPECIAL_CHARS: &str = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 1,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true,
                    "description": "Hello 你好 안녕하세요"
                }
            ]
        }
        "#;

        let interface = InterfaceJson(IFACE_SPECIAL_CHARS.into());

        let astarte_interface: Interface = interface.try_into().unwrap();

        assert_eq!(
            astarte_interface.interface_name(),
            "org.astarte-platform.test.test"
        );
        assert_eq!(astarte_interface.version_major(), 1);
    }

    #[tokio::test]
    async fn convert_bad_proto_interface_to_astarte_interface() {
        const IFACE_BAD: &str = r#"{"#;

        let interface = InterfaceJson(IFACE_BAD.into());

        let astarte_interface_bad_result: Result<Interface, crate::error::Error> =
            interface.try_into();

        assert!(astarte_interface_bad_result.is_err());
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
            ProtoPayload::AstarteUnset(astarte_message_hub_proto::AstarteUnset {}),
            astarte_message.payload.unwrap()
        );
    }

    fn get_individual_data_from_payload(
        payload: ProtoPayload,
    ) -> Result<AstarteType, MessageHubProtoError> {
        payload
            .take_data()
            .expect("data")
            .take_individual()
            .expect("individual")
            .individual_data
            .expect("individual_data")
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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

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
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_object_to_astarte_message() {
        let expected_map = HashMap::from([
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

        let astarte_object = astarte_message
            .take_data()
            .and_then(|data| data.take_object())
            .unwrap();

        let object_data = astarte_object.object_data;
        for (k, v) in expected_map.into_iter() {
            let astarte_type: AstarteType = object_data
                .get(&k)
                .and_then(|data| data.individual_data.as_ref())
                .and_then(|data| data.clone().try_into().ok())
                .unwrap();

            assert_eq!(v, astarte_type);
        }
    }

    #[test]
    fn convert_astarte_device_data_event_object2_to_astarte_message() {
        let expected_map = HashMap::from([
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

        let object_data = astarte_message
            .take_data()
            .and_then(|data| data.take_object())
            .unwrap()
            .object_data;

        for (k, v) in expected_map.into_iter() {
            let astarte_type: AstarteType = object_data
                .get(&k)
                .and_then(|data| data.individual_data.as_ref())
                .and_then(|data| data.clone().try_into().ok())
                .unwrap();

            assert_eq!(v, astarte_type);
        }
    }

    #[test]
    fn from_sdk_astarte_type_to_astarte_message_payload_success() {
        let expected_double_value: f64 = 15.5;
        let astarte_sdk_type_double = AstarteType::Double(expected_double_value);

        let payload: ProtoPayload = astarte_sdk_type_double.into();

        let double_value = payload
            .take_data()
            .and_then(astarte_message_hub_proto::AstarteDataType::take_individual)
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteDouble(expected_double_value),
            double_value
        );
    }

    #[test]
    fn from_sdk_astarte_aggregate_to_astarte_message_payload_success() {
        let expected_data: f64 = 15.5;
        use std::collections::HashMap;
        let astarte_type_map = Value::Object(HashMap::from([(
            "key1".to_string(),
            AstarteType::Double(expected_data),
        )]));

        let mut payload_result: ProtoPayload = astarte_type_map.into();

        let double_data = payload_result
            .data_mut()
            .and_then(astarte_message_hub_proto::AstarteDataType::object_mut)
            .and_then(|data| data.object_data.remove("key1"))
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(IndividualData::AstarteDouble(expected_data), double_data);
    }

    fn make_messagehub_property(
        path: &str,
        value: Option<AstarteType>,
    ) -> astarte_message_hub_proto::Property {
        astarte_message_hub_proto::Property {
            path: path.to_owned(),
            value: Some(value.map_or(
                astarte_message_hub_proto::property::Value::AstarteUnset(
                    astarte_message_hub_proto::AstarteUnset {},
                ),
                |v| astarte_message_hub_proto::property::Value::AstarteProperty(v.into()),
            )),
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
