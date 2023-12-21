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

//! Provides the structs for the Astarte MQTT Protocol.
//!
//! You can find more information about the protocol v1 in the [Astarte MQTT v1 Protocol](https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html).

use std::collections::HashMap;

use bson::Bson;

use log::{debug, trace};
use serde::{Deserialize, Serialize};

use crate::{
    interface::{
        mapping::path::{MappingError, MappingPath},
        reference::{MappingRef, ObjectRef},
    },
    types::{AstarteType, BsonConverter, TypeError},
    Interface, Timestamp,
};

/// Errors that can occur while handling the payload.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum PayloadError {
    /// Couldn't serialize the payload to bson.
    #[error("couldn't serialize the payload")]
    Serialize(#[from] bson::ser::Error),
    /// Couldn't deserialize the payload to bson.
    #[error("couldn't deserialize the payload")]
    Deserialize(#[from] bson::de::Error),
    /// Couldn't convert the value to [`AstarteType`]
    #[error("couldn't convert the value to AstarteType")]
    AstarteType(#[from] TypeError),
    /// Expected object, individual data deserialized
    #[error("expected object, individual data deserialized instead {0}")]
    Object(Bson),
    /// Couldn't parse a mapping
    #[error("couldn't parse the mapping")]
    Mapping(#[from] MappingError),
    /// Couldn't accept unset for mapping without `allow_unset`
    #[error("couldn't accept unset if the mapping isn't a property with `allow_unset`")]
    Unset,
}

/// The payload of an MQTT message.
///
/// It is serialized as a BSON object when sent over the wire.
///
/// The payload BSON specification can be found here: [BSON](https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#bson)
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub(crate) struct Payload<T> {
    #[serde(rename = "v")]
    pub(crate) value: T,
    #[serde(rename = "t", default, skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<Timestamp>,
}

impl<T> Payload<T> {
    /// Create a new payload with the given value.
    ///
    /// The time-stamp will be set to [`None`]
    pub(crate) fn new(value: T) -> Self {
        Self {
            value,
            timestamp: None,
        }
    }

    /// Create a new payload with the given value and time-stamp.
    pub(crate) fn with_timestamp(value: T, timestamp: Option<Timestamp>) -> Self {
        Self { value, timestamp }
    }

    /// Serialize the payload to a BSON vector of bytes.
    pub(crate) fn to_vec(&self) -> Result<Vec<u8>, PayloadError>
    where
        T: serde::Serialize,
    {
        let res = bson::to_vec(self)?;

        Ok(res)
    }

    /// Deserialize the payload from a BSON slice of bytes.
    pub(crate) fn from_slice<'a>(buf: &'a [u8]) -> Result<Payload<T>, PayloadError>
    where
        T: serde::de::Deserialize<'a>,
    {
        let res = bson::from_slice(buf)?;

        Ok(res)
    }
}

/// Serialize an [`AstarteType`] to a [`Bson`] buffer
pub(super) fn serialize_individual(
    individual: &AstarteType,
    timestamp: Option<Timestamp>,
) -> Result<Vec<u8>, PayloadError> {
    if &AstarteType::Unset == individual {
        // When sending/receiving an unset we should accept an empty payload.
        // https://docs.astarte-platform.org/latest/080-mqtt-v1-protocol.html#payload-format
        return Ok(Vec::new());
    }

    Payload::with_timestamp(individual, timestamp).to_vec()
}

/// Serialize an aggregate to a [`Bson`] buffer
pub(super) fn serialize_object(
    aggregate: &HashMap<&String, &AstarteType>,
    timestamp: Option<Timestamp>,
) -> Result<Vec<u8>, PayloadError> {
    Payload::with_timestamp(aggregate, timestamp).to_vec()
}

/// Deserialize an individual [`AstarteType`]
pub(super) fn deserialize_individual<'a>(
    mapping: MappingRef<'a, &'a Interface>,
    buf: &[u8],
) -> Result<(AstarteType, Option<Timestamp>), PayloadError> {
    if buf.is_empty() {
        debug!("unset received");

        if !mapping.allow_unset() {
            return Err(PayloadError::Unset);
        }

        return Ok((AstarteType::Unset, None));
    }

    let payload = Payload::<Bson>::from_slice(buf)?;

    let hint = BsonConverter::new(mapping.mapping_type(), payload.value);

    let ast_val = AstarteType::try_from(hint)?;

    Ok((ast_val, payload.timestamp))
}

pub(super) fn deserialize_object(
    object: ObjectRef,
    path: &MappingPath,
    buf: &[u8],
) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), PayloadError> {
    if buf.is_empty() {
        return Err(PayloadError::Unset);
    }

    let payload = Payload::<Bson>::from_slice(buf)?;

    let doc = match payload.value {
        Bson::Document(document) => document,
        data => return Err(PayloadError::Object(data)),
    };

    trace!("base path {path}");

    let aggregate = doc
        .into_iter()
        .filter_map(|(key, value)| {
            trace!("key {key}");

            let full_path = format!("{path}/{key}");
            let path = match MappingPath::try_from(full_path.as_str()) {
                Ok(path) => path,
                Err(err) => return Some(Err(PayloadError::from(err))),
            };

            let mapping = match object.mapping(&path) {
                Some(mapping) => mapping,
                None => {
                    debug!(
                        "unrecognized mapping {path} for interface {}",
                        object.interface.interface_name()
                    );

                    return None;
                }
            };

            let hint = BsonConverter::new(mapping.mapping_type(), value);

            let ast_val = match AstarteType::try_from(hint) {
                Ok(t) => t,
                Err(err) => return Some(Err(PayloadError::from(err))),
            };

            if ast_val.is_unset() {
                return Some(Err(PayloadError::Unset));
            }

            Some(Ok((key, ast_val)))
        })
        .collect::<Result<HashMap<String, AstarteType>, _>>()?;

    Ok((aggregate, payload.timestamp))
}

#[cfg(test)]
mod test {
    use chrono::Utc;
    use std::str::FromStr;

    use chrono::TimeZone;

    use crate::{
        interface::MappingType,
        mapping,
        validate::{validate_individual, validate_object},
    };

    use super::*;

    use crate::test::E2E_DEVICE_AGGREGATE;
    use crate::test::E2E_DEVICE_DATASTREAM;

    fn mapping_type(value: &AstarteType) -> MappingType {
        match value {
            AstarteType::Double(_) => MappingType::Double,
            AstarteType::Integer(_) => MappingType::Integer,
            AstarteType::Boolean(_) => MappingType::Boolean,
            AstarteType::LongInteger(_) => MappingType::LongInteger,
            AstarteType::String(_) => MappingType::String,
            AstarteType::BinaryBlob(_) => MappingType::BinaryBlob,
            AstarteType::DateTime(_) => MappingType::DateTime,
            AstarteType::DoubleArray(_) => MappingType::DoubleArray,
            AstarteType::IntegerArray(_) => MappingType::IntegerArray,
            AstarteType::BooleanArray(_) => MappingType::BooleanArray,
            AstarteType::LongIntegerArray(_) => MappingType::LongIntegerArray,
            AstarteType::StringArray(_) => MappingType::StringArray,
            AstarteType::BinaryBlobArray(_) => MappingType::BinaryBlobArray,
            AstarteType::DateTimeArray(_) => MappingType::DateTimeArray,
            #[allow(deprecated)]
            AstarteType::Unset | AstarteType::EmptyArray => {
                panic!("Mapping doesn't exists for those type")
            }
        }
    }

    #[test]
    fn test_individual_serialization() {
        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();

        let alltypes = [
            AstarteType::Double(4.5),
            AstarteType::Integer(-4),
            AstarteType::Boolean(true),
            AstarteType::LongInteger(45543543534_i64),
            AstarteType::String("hello".into()),
            AstarteType::BinaryBlob(b"hello".to_vec()),
            AstarteType::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteType::DoubleArray(vec![1.2, 3.4, 5.6, 7.8]),
            AstarteType::IntegerArray(vec![1, 3, 5, 7]),
            AstarteType::BooleanArray(vec![true, false, true, true]),
            AstarteType::LongIntegerArray(vec![45543543534_i64, 45543543535_i64, 45543543536_i64]),
            AstarteType::StringArray(vec!["hello".to_owned(), "world".to_owned()]),
            AstarteType::BinaryBlobArray(vec![b"hello".to_vec(), b"world".to_vec()]),
            AstarteType::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]),
        ];

        for ty in alltypes {
            println!("checking {ty:?}");
            let mapping_type = mapping_type(&ty);
            let endpoint = format!("/{mapping_type}_endpoint");
            let path = mapping!(endpoint.as_str());
            let mapping = interface.as_mapping_ref(path).unwrap();

            let validated = validate_individual(mapping, path, &ty, None).unwrap();
            let buf = serialize_individual(validated.data(), validated.timestamp()).unwrap();

            let (res, _) = deserialize_individual(mapping, &buf).unwrap();

            assert_eq!(res, ty);
        }
    }

    #[test]
    fn test_serialize_object() {
        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let alltypes = [
            AstarteType::Double(4.5),
            AstarteType::Integer(-4),
            AstarteType::Boolean(true),
            AstarteType::LongInteger(45543543534_i64),
            AstarteType::String("hello".into()),
            AstarteType::BinaryBlob(b"hello".to_vec()),
            AstarteType::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteType::DoubleArray(vec![1.2, 3.4, 5.6, 7.8]),
            AstarteType::IntegerArray(vec![1, 3, 5, 7]),
            AstarteType::BooleanArray(vec![true, false, true, true]),
            AstarteType::LongIntegerArray(vec![45543543534_i64, 45543543535_i64, 45543543536_i64]),
            AstarteType::StringArray(vec!["hello".to_owned(), "world".to_owned()]),
            AstarteType::BinaryBlobArray(vec![b"hello".to_vec(), b"world".to_vec()]),
            AstarteType::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]),
        ];

        let base_path = "/1";
        let data: HashMap<_, _> = alltypes
            .into_iter()
            .map(|ty| {
                let mapping_type = mapping_type(&ty);

                let endpoint = format!("{mapping_type}_endpoint");

                (endpoint, ty)
            })
            .collect();

        let object = interface.as_object_ref().unwrap();
        let path = mapping!(base_path);

        let validated = validate_object(object, path, &data, None).unwrap();
        let buf = serialize_object(validated.data(), validated.timestamp()).unwrap();

        let (res, _) = deserialize_object(object, path, &buf).unwrap();

        assert_eq!(res, data)
    }

    #[test]
    fn test_integer_longinteger_compatibility() {
        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();
        let mapping = interface
            .as_mapping_ref(mapping!("/longinteger_endpoint"))
            .unwrap();

        // 3600i32
        let longinteger_b = [12, 0, 0, 0, 16, 118, 0, 16, 14, 0, 0, 0];

        let (res, _) = deserialize_individual(mapping, &longinteger_b).unwrap();

        assert_eq!(res, AstarteType::LongInteger(3600i64));
    }

    #[test]
    fn test_bson_serialization() {
        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();
        let path = mapping!("/longinteger_endpoint");
        let mapping = interface.as_mapping_ref(path).unwrap();

        let og_value = AstarteType::LongInteger(3600);
        let validated = validate_individual(mapping, path, &og_value, None).unwrap();
        let buf = serialize_individual(validated.data(), validated.timestamp()).unwrap();

        let expected = [16, 0, 0, 0, 18, 118, 0, 16, 14, 0, 0, 0, 0, 0, 0, 0];

        assert_eq!(buf, expected);

        let (res, _) = deserialize_individual(mapping, &buf).unwrap();

        assert_eq!(res, og_value);
    }

    #[test]
    fn deserialize_mixed_array() {
        let buf = [
            49, 0, 0, 0, 4, 118, 0, 41, 0, 0, 0, 18, 48, 0, 238, 82, 155, 154, 10, 0, 0, 0, 16, 49,
            0, 10, 0, 0, 0, 16, 50, 0, 0, 0, 0, 0, 18, 51, 0, 238, 82, 155, 154, 10, 0, 0, 0, 0, 0,
        ];

        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();

        let mapping = interface
            .as_mapping_ref(mapping!("/longintegerarray_endpoint"))
            .unwrap();

        let (at, _) = deserialize_individual(mapping, &buf).unwrap();

        let expected = AstarteType::LongIntegerArray(vec![45543543534, 10, 0, 45543543534]);

        assert_eq!(at, expected);
    }

    #[test]
    fn deserialize_empty_array() {
        let buf = [13, 0, 0, 0, 4, 118, 0, 5, 0, 0, 0, 0, 0];

        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();

        let mapping = interface
            .as_mapping_ref(mapping!("/longintegerarray_endpoint"))
            .unwrap();

        let (at, _) = deserialize_individual(mapping, &buf).unwrap();

        let expected = AstarteType::LongIntegerArray(vec![]);

        assert_eq!(at, expected);
    }

    #[test]
    fn deserialize_unset_individual() {
        let buf = [];

        let interface = Interface::from_str(
            r#"{
    "interface_name": "org.astarte-platform.rust.e2etest.DeviceProperty",
    "version_major": 0,
    "version_minor": 1,
    "type": "properties",
    "ownership": "device",
    "mappings": [
        {
            "endpoint": "/%{sensor_id}/double_endpoint",
            "type": "double",
            "allow_unset": false
        }
]}"#,
        )
        .unwrap();

        let mapping = interface
            .as_mapping_ref(mapping!("/1/double_endpoint"))
            .unwrap();

        let at = deserialize_individual(mapping, &buf);

        assert!(matches!(at, Err(PayloadError::Unset)));
    }

    #[test]
    fn deserialize_unset_aggregate() {
        let buf = [];

        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();
        let object = interface.as_object_ref().unwrap();

        let at = deserialize_object(object, mapping!("/1"), &buf);

        assert!(matches!(at, Err(PayloadError::Unset)));
    }
}
