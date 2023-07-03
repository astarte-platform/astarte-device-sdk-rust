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
use chrono::{DateTime, Utc};
use log::trace;
use serde::{Deserialize, Serialize};

use crate::{
    types::{AstarteType, TypeError},
    Aggregation,
};

/// Errors that can occur handling the payload.
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
    pub(crate) timestamp: Option<DateTime<Utc>>,
}

impl<T> Payload<T>
where
    T: serde::Serialize,
{
    pub(crate) fn to_vec(&self) -> Result<Vec<u8>, PayloadError> {
        let res = bson::to_vec(self)?;

        Ok(res)
    }

    pub(crate) fn from_slice<'a>(buf: &'a [u8]) -> Result<Payload<T>, PayloadError>
    where
        T: serde::de::Deserialize<'a>,
    {
        let res = bson::from_slice(buf)?;

        Ok(res)
    }
}

/// Serialize an [`AstarteType`] to bson payload.
pub(crate) fn serialize_individual(
    data: &AstarteType,
    timestamp: Option<DateTime<Utc>>,
) -> Result<Vec<u8>, PayloadError> {
    let payload = Payload {
        value: data,
        timestamp,
    };

    payload.to_vec()
}

/// Serialize an Object passed as an [`HashMap`] of [`AstarteType`] to bson payload.
pub(crate) fn serialize_object(
    data: &HashMap<String, AstarteType>,
    timestamp: Option<DateTime<Utc>>,
) -> Result<Vec<u8>, PayloadError> {
    let payload = Payload {
        value: data,
        timestamp,
    };

    payload.to_vec()
}

/// Deserialize a bson payload to an individual [`AstarteType`] or an object as an [`HashMap`].
pub(crate) fn deserialize(bdata: &[u8]) -> Result<Aggregation, PayloadError> {
    if bdata.is_empty() {
        return Ok(Aggregation::Individual(AstarteType::Unset));
    }

    let payload = Payload::<Bson>::from_slice(bdata)?;

    trace!("{:?}", payload);

    match payload.value {
        Bson::Document(doc) => {
            let hmap = doc
                .into_iter()
                .map(|(name, value)| {
                    let v = AstarteType::try_from(value)?;

                    Ok((name, v))
                })
                .collect::<Result<HashMap<String, AstarteType>, PayloadError>>()?;

            Ok(Aggregation::Object(hmap))
        }
        value => {
            let individual = AstarteType::try_from(value)?;

            Ok(Aggregation::Individual(individual))
        }
    }
}

#[cfg(test)]
mod test {
    use chrono::TimeZone;

    use crate::interface::MappingType;

    use super::*;

    #[test]
    fn test_individual_serialization() {
        let alltypes: Vec<AstarteType> = vec![
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

            let buf = serialize_individual(&ty, None).unwrap();

            let ty2 = deserialize(&buf).unwrap();

            if let Aggregation::Individual(data) = ty2 {
                assert!(ty == data);
            } else {
                panic!();
            }
        }
    }

    #[test]
    fn test_serialize_object() {
        let alltypes: Vec<AstarteType> = vec![
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

        let allendpoints = vec![
            "double".to_string(),
            "integer".to_string(),
            "boolean".to_string(),
            "longinteger".to_string(),
            "string".to_string(),
            "binaryblob".to_string(),
            "datetime".to_string(),
            "doublearray".to_string(),
            "integerarray".to_string(),
            "booleanarray".to_string(),
            "longintegerarray".to_string(),
            "stringarray".to_string(),
            "binaryblobarray".to_string(),
            "datetimearray".to_string(),
        ];

        let mut data = std::collections::HashMap::new();

        for i in allendpoints.iter().zip(alltypes.iter()) {
            data.insert(i.0.clone(), i.1.clone());
        }

        let bytes = serialize_object(&data, None).unwrap();

        let data_processed = deserialize(&bytes).unwrap();

        println!("\nComparing {data:?}\nto {data_processed:?}");

        if let Aggregation::Object(data_processed) = data_processed {
            assert_eq!(data, data_processed);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_integer_longinteger_compatibility() {
        let longinteger_b = [12, 0, 0, 0, 16, 118, 0, 16, 14, 0, 0, 0];
        let integer_buf = deserialize(&longinteger_b).unwrap();
        if let Aggregation::Individual(astarte_type) = integer_buf {
            assert_eq!(astarte_type, MappingType::LongInteger);
        } else {
            panic!("Deserialization in not individual");
        }
    }

    #[test]
    fn test_bson_serialization() {
        let og_value = AstarteType::LongInteger(3600);
        let buf = serialize_individual(&og_value, None).unwrap();
        if let Aggregation::Individual(astarte_type) = deserialize(&buf).unwrap() {
            assert_eq!(astarte_type, AstarteType::LongInteger(3600));
            if let AstarteType::LongInteger(value) = astarte_type {
                assert_eq!(value, 3600);
            } else {
                panic!("Astarte Type is not LongInteger");
            }
        } else {
            panic!("Deserialization in not individual");
        }
    }
}
