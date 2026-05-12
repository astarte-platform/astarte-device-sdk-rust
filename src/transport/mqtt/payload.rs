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

//! Provides the structs for the Astarte MQTT Protocol.
//!
//! You can find more information about the protocol v1 in the [Astarte MQTT v1 Protocol](https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html).

use std::fmt::Display;

use astarte_device_error::{Error, ResultExt, WrapError};
use astarte_interfaces::{
    DatastreamIndividual, DatastreamObject, InterfaceMapping, MappingPath, Properties, Schema,
};
use bson::Bson;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace};

use crate::Timestamp;
use crate::aggregate::AstarteObject;
use crate::interfaces::MappingRef;
use crate::types::de::BsonConverter;
use crate::types::{AstarteData, TypeError};

/// Errors that can occur while handling the payload.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadError {
    /// Couldn't serialize the payload to bson.
    Serialize,
    /// Couldn't deserialize the payload to bson.
    Deserialize,
    /// Couldn't convert the value to [`AstarteData`]
    Conversion(TypeError),
}

impl Display for PayloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialize => write!(f, "couldn't serialize the payload"),
            Self::Deserialize => write!(f, "couldn't deserialize the payload"),
            Self::Conversion(error) => write!(f, "couldn't convert to Astarte type {error}"),
        }
    }
}

/// Used to serialize to the correct type
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub(crate) struct BsonTimestamp(
    #[serde(with = "bson::serde_helpers::datetime::FromChrono04DateTime")] pub(crate) Timestamp,
);

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
    pub(crate) timestamp: Option<BsonTimestamp>,
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
        Self {
            value,
            timestamp: timestamp.map(BsonTimestamp),
        }
    }

    /// Serialize the payload to a BSON vector of bytes.
    pub(crate) fn to_vec(&self) -> Result<Vec<u8>, Error<PayloadError>>
    where
        T: serde::Serialize,
    {
        let res = bson::serialize_to_vec(self).wrap_err(PayloadError::Serialize)?;

        Ok(res)
    }

    /// Deserialize the payload from a BSON slice of bytes.
    pub(crate) fn from_slice<'a>(buf: &'a [u8]) -> Result<Payload<T>, Error<PayloadError>>
    where
        T: serde::de::Deserialize<'a>,
    {
        let res = bson::deserialize_from_slice(buf).wrap_err(PayloadError::Deserialize)?;

        Ok(res)
    }
}

/// Serialize an [`AstarteData`] to a [`Bson`] buffer
pub(super) fn serialize_individual(
    individual: &AstarteData,
    timestamp: Option<Timestamp>,
) -> Result<Vec<u8>, Error<PayloadError>> {
    Payload::with_timestamp(individual, timestamp).to_vec()
}

/// Serialize an aggregate to a [`Bson`] buffer
pub(super) fn serialize_object(
    aggregate: &AstarteObject,
    timestamp: Option<Timestamp>,
) -> Result<Vec<u8>, Error<PayloadError>> {
    Payload::with_timestamp(aggregate, timestamp).to_vec()
}

/// Deserialize an individual [`AstarteData`]
pub(super) fn deserialize_property(
    mapping: &MappingRef<'_, Properties>,
    buf: &[u8],
) -> Result<Option<AstarteData>, Error<PayloadError>> {
    if buf.is_empty() {
        return Ok(None);
    }

    let payload = Payload::<Bson>::from_slice(buf)?;

    let hint = BsonConverter::new(mapping.mapping().mapping_type(), payload.value);

    let ast_val = AstarteData::try_from(hint).map_kind(PayloadError::Conversion)?;

    Ok(Some(ast_val))
}

/// Deserialize an individual [`AstarteData`]
pub(super) fn deserialize_individual(
    mapping: &MappingRef<'_, DatastreamIndividual>,
    buf: &[u8],
) -> Result<(AstarteData, Option<Timestamp>), Error<PayloadError>> {
    let payload = Payload::<Bson>::from_slice(buf)?;

    let hint = BsonConverter::new(mapping.mapping().mapping_type(), payload.value);

    let ast_val = AstarteData::try_from(hint).map_kind(PayloadError::Conversion)?;

    Ok((ast_val, payload.timestamp.map(|v| v.0)))
}

pub(super) fn deserialize_object(
    object: &DatastreamObject,
    path: &MappingPath<'_>,
    buf: &[u8],
) -> Result<(AstarteObject, Option<Timestamp>), Error<PayloadError>> {
    let payload = Payload::<Bson>::from_slice(buf)?;

    let doc = match payload.value {
        Bson::Document(document) => document,
        data => {
            error!(
                data = ?data.element_type(),
                "expected bson document for object datastream"
            );

            return Err(Error::with(
                PayloadError::Deserialize,
                "object interface not a BSON document",
            ));
        }
    };

    trace!("base path {path}");

    let aggregate = doc
        .into_iter()
        .filter_map(|(key, value)| {
            trace!(key);

            let mapping = match object.mapping(&key) {
                Some(mapping) => mapping,
                None => {
                    debug!(
                        "unrecognized mapping {path} for interface {}",
                        object.interface_name()
                    );

                    return None;
                }
            };

            let hint = BsonConverter::new(mapping.mapping_type(), value);

            let res = AstarteData::try_from(hint)
                .map_kind(PayloadError::Conversion)
                .map(|value| (key, value));

            Some(res)
        })
        .collect::<Result<AstarteObject, _>>()?;

    Ok((aggregate, payload.timestamp.map(|v| v.0)))
}

#[cfg(test)]
mod test {
    use astarte_interfaces::Interface;
    use astarte_interfaces::schema::MappingType;
    use astarte_test_utils::Hexdump;
    use astarte_test_utils::with_insta;
    use chrono::{DateTime, Utc};
    use pretty_assertions::assert_eq;
    use std::str::FromStr;

    use chrono::TimeZone;

    use crate::types::Double;
    use crate::validate::ValidatedIndividual;
    use crate::validate::ValidatedObject;

    use super::*;

    use crate::test::E2E_DEVICE_AGGREGATE;
    use crate::test::E2E_DEVICE_DATASTREAM;

    fn mapping_type(value: &AstarteData) -> MappingType {
        match value {
            AstarteData::Double(_) => MappingType::Double,
            AstarteData::Integer(_) => MappingType::Integer,
            AstarteData::Boolean(_) => MappingType::Boolean,
            AstarteData::LongInteger(_) => MappingType::LongInteger,
            AstarteData::String(_) => MappingType::String,
            AstarteData::BinaryBlob(_) => MappingType::BinaryBlob,
            AstarteData::DateTime(_) => MappingType::DateTime,
            AstarteData::DoubleArray(_) => MappingType::DoubleArray,
            AstarteData::IntegerArray(_) => MappingType::IntegerArray,
            AstarteData::BooleanArray(_) => MappingType::BooleanArray,
            AstarteData::LongIntegerArray(_) => MappingType::LongIntegerArray,
            AstarteData::StringArray(_) => MappingType::StringArray,
            AstarteData::BinaryBlobArray(_) => MappingType::BinaryBlobArray,
            AstarteData::DateTimeArray(_) => MappingType::DateTimeArray,
        }
    }

    #[test]
    fn test_individual_serialization() {
        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();
        let interface = interface.as_datastream_individual().unwrap();

        let alltypes = [
            AstarteData::Double(4.5.try_into().unwrap()),
            AstarteData::Integer(-4),
            AstarteData::Boolean(true),
            AstarteData::LongInteger(45543543534_i64),
            AstarteData::String("hello".into()),
            AstarteData::BinaryBlob(b"hello".to_vec()),
            AstarteData::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteData::DoubleArray(
                [1.2, 3.4, 5.6, 7.8]
                    .map(|v| Double::try_from(v).unwrap())
                    .to_vec(),
            ),
            AstarteData::IntegerArray(vec![1, 3, 5, 7]),
            AstarteData::BooleanArray(vec![true, false, true, true]),
            AstarteData::LongIntegerArray(vec![45543543534_i64, 45543543535_i64, 45543543536_i64]),
            AstarteData::StringArray(vec!["hello".to_owned(), "world".to_owned()]),
            AstarteData::BinaryBlobArray(vec![b"hello".to_vec(), b"world".to_vec()]),
            AstarteData::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]),
        ];

        for ty in alltypes {
            let mapping_type = mapping_type(&ty);
            let endpoint = format!("/{mapping_type}_endpoint");

            let path = MappingPath::try_from(endpoint.as_str()).unwrap();
            let mapping = MappingRef::new(interface, &path).unwrap();

            let validated = ValidatedIndividual::validate(
                mapping,
                ty.clone(),
                Some(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            )
            .unwrap();

            let buf = serialize_individual(&validated.data, validated.timestamp).unwrap();

            let (res, _) = deserialize_individual(&mapping, &buf).unwrap();

            assert_eq!(res, ty);
        }
    }

    #[test]
    fn test_serialize_object() {
        let interface = DatastreamObject::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let alltypes = [
            AstarteData::try_from(4.5).unwrap(),
            AstarteData::Integer(-4),
            AstarteData::Boolean(true),
            AstarteData::LongInteger(45543543534_i64),
            AstarteData::String("hello".into()),
            AstarteData::BinaryBlob(b"hello".to_vec()),
            AstarteData::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteData::try_from(vec![1.2, 3.4, 5.6, 7.8]).unwrap(),
            AstarteData::IntegerArray(vec![1, 3, 5, 7]),
            AstarteData::BooleanArray(vec![true, false, true, true]),
            AstarteData::LongIntegerArray(vec![45543543534_i64, 45543543535_i64, 45543543536_i64]),
            AstarteData::StringArray(vec!["hello".to_owned(), "world".to_owned()]),
            AstarteData::BinaryBlobArray(vec![b"hello".to_vec(), b"world".to_vec()]),
            AstarteData::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]),
        ];

        let base_path = "/1";
        let data: AstarteObject = alltypes
            .into_iter()
            .map(|ty| {
                let mapping_type = mapping_type(&ty);

                let endpoint = format!("{mapping_type}_endpoint");

                (endpoint, ty)
            })
            .collect();

        let path = MappingPath::try_from(base_path).unwrap();

        let timestamp = Some(DateTime::from_timestamp_millis(42).unwrap());
        let validated =
            ValidatedObject::validate(&interface, &path, data.clone(), timestamp).unwrap();
        let buf = serialize_object(&validated.data, validated.timestamp).unwrap();

        with_insta!({
            insta::assert_snapshot!(Hexdump(buf.as_ref()));
        });

        let (res, res_timestamp) = deserialize_object(&interface, &path, &buf).unwrap();

        assert_eq!(res, data);
        assert_eq!(res_timestamp, timestamp);
    }

    #[test]
    fn test_integer_longinteger_compatibility() {
        let interface = DatastreamIndividual::from_str(E2E_DEVICE_DATASTREAM).unwrap();
        let path = MappingPath::try_from("/longinteger_endpoint").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        // 3600i32
        let longinteger_b = [12, 0, 0, 0, 16, 118, 0, 16, 14, 0, 0, 0];

        let (res, _) = deserialize_individual(&mapping, &longinteger_b).unwrap();

        assert_eq!(res, AstarteData::LongInteger(3600i64));
    }

    #[test]
    fn test_bson_serialization() {
        let interface = DatastreamIndividual::from_str(E2E_DEVICE_DATASTREAM).unwrap();
        let path = MappingPath::try_from("/longinteger_endpoint").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let og_value = AstarteData::LongInteger(3600);
        let timestamp = Some(DateTime::from_timestamp_millis(42).unwrap());
        let validated =
            ValidatedIndividual::validate(mapping, og_value.clone(), timestamp).unwrap();
        let buf = serialize_individual(&validated.data, validated.timestamp).unwrap();

        with_insta!({
            insta::assert_snapshot!(Hexdump(buf.as_slice()));
        });

        let (res, res_timestamp) = deserialize_individual(&mapping, &buf).unwrap();

        assert_eq!(res, og_value);
        assert_eq!(res_timestamp, timestamp);
    }

    #[test]
    fn deserialize_mixed_array() {
        let buf = [
            49, 0, 0, 0, 4, 118, 0, 41, 0, 0, 0, 18, 48, 0, 238, 82, 155, 154, 10, 0, 0, 0, 16, 49,
            0, 10, 0, 0, 0, 16, 50, 0, 0, 0, 0, 0, 18, 51, 0, 238, 82, 155, 154, 10, 0, 0, 0, 0, 0,
        ];

        let interface = DatastreamIndividual::from_str(E2E_DEVICE_DATASTREAM).unwrap();

        let path = MappingPath::try_from("/longintegerarray_endpoint").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let (at, _) = deserialize_individual(&mapping, &buf).unwrap();

        let expected = AstarteData::LongIntegerArray(vec![45543543534, 10, 0, 45543543534]);

        assert_eq!(at, expected);
    }

    #[test]
    fn deserialize_empty_array() {
        let buf = [13, 0, 0, 0, 4, 118, 0, 5, 0, 0, 0, 0, 0];

        let interface = DatastreamIndividual::from_str(E2E_DEVICE_DATASTREAM).unwrap();

        let path = MappingPath::try_from("/longintegerarray_endpoint").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let (at, _) = deserialize_individual(&mapping, &buf).unwrap();

        let expected = AstarteData::LongIntegerArray(vec![]);

        assert_eq!(at, expected);
    }

    /// This is validated outside of the payload.
    #[test]
    fn deserialize_unset_individual() {
        let buf = [];

        let interface = Properties::from_str(
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

        let path = MappingPath::try_from("/1/double_endpoint").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let opt = deserialize_property(&mapping, &buf).unwrap();

        assert!(opt.is_none());
    }

    #[test]
    fn deserialize_unset_aggregate() {
        let buf = [];

        let object = DatastreamObject::from_str(E2E_DEVICE_AGGREGATE).unwrap();
        let path = MappingPath::try_from("/1").unwrap();

        let err = deserialize_object(&object, &path, &buf).unwrap_err();
        assert_eq!(*err.kind(), PayloadError::Deserialize);
    }
}
