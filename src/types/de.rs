// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

//! Deserialize the [`AstarteData`].

use bson::{Binary, Bson};

use astarte_interfaces::schema::MappingType;

use super::{AstarteData, TypeError};

impl From<AstarteData> for Bson {
    fn from(d: AstarteData) -> Self {
        match d {
            AstarteData::Double(d) => Bson::Double(d.0),
            AstarteData::Integer(d) => Bson::Int32(d),
            AstarteData::Boolean(d) => Bson::Boolean(d),
            AstarteData::LongInteger(d) => Bson::Int64(d),
            AstarteData::String(d) => Bson::String(d),
            AstarteData::BinaryBlob(d) => Bson::Binary(Binary {
                bytes: d,
                subtype: bson::spec::BinarySubtype::Generic,
            }),
            AstarteData::DateTime(d) => Bson::DateTime(d.into()),
            AstarteData::DoubleArray(d) => d.into_iter().map(f64::from).collect(),
            AstarteData::IntegerArray(d) => d.into_iter().collect(),
            AstarteData::BooleanArray(d) => d.into_iter().collect(),
            AstarteData::LongIntegerArray(d) => d.into_iter().collect(),
            AstarteData::StringArray(d) => d.into_iter().collect(),
            AstarteData::BinaryBlobArray(d) => d
                .into_iter()
                .map(|bytes| Binary {
                    bytes,
                    subtype: bson::spec::BinarySubtype::Generic,
                })
                .collect(),
            AstarteData::DateTimeArray(d) => d.into_iter().collect(),
        }
    }
}

/// Utility to convert a Bson array into an [`AstarteData`] array.
pub(crate) fn bson_array<T, F>(array: Vec<Bson>, f: F) -> Result<Vec<T>, TypeError>
where
    F: FnMut(Bson) -> Option<T>,
{
    array
        .into_iter()
        .map(f)
        .map(|item| item.ok_or(TypeError::FromBsonArrayError))
        .collect()
}

/// Type of astarte arrays.
///
/// It's used to deserialize a BSON array to the given [`AstarteData`] given this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArrayType {
    Double,
    Integer,
    Boolean,
    LongInteger,
    String,
    BinaryBlob,
    DateTime,
}

impl ArrayType {
    /// Create an empty [`AstarteData`] array.
    pub(crate) fn as_empty(&self) -> AstarteData {
        match self {
            ArrayType::Double => AstarteData::DoubleArray(Vec::new()),
            ArrayType::Integer => AstarteData::IntegerArray(Vec::new()),
            ArrayType::Boolean => AstarteData::BooleanArray(Vec::new()),
            ArrayType::LongInteger => AstarteData::LongIntegerArray(Vec::new()),
            ArrayType::String => AstarteData::StringArray(Vec::new()),
            ArrayType::BinaryBlob => AstarteData::BinaryBlobArray(Vec::new()),
            ArrayType::DateTime => AstarteData::DateTimeArray(Vec::new()),
        }
    }
}

/// Struct to convert a BSON to an [`AstarteData`] with the given [`MappingType`] as hint.
///
/// The struct is needed to convert an [`Bson`] value to an [`AstarteData`] since the Astarte's
/// arrays cannot be of mixed type.
///
/// We use this struct to deserialize a buffer into the [`Bson`] so we can have a better control of
/// the errors when converting it into the astarte type.
#[derive(Debug, Clone)]
pub(crate) struct BsonConverter {
    mapping_type: MappingType,
    bson: Bson,
}

impl BsonConverter {
    /// Create a new hint with the given type
    pub(crate) fn new(mapping_type: MappingType, bson: Bson) -> Self {
        Self { mapping_type, bson }
    }

    /// Tries to convert the [`ArrayType`] to an astarte array.
    fn try_into_array(self, item_type: ArrayType) -> Result<AstarteData, TypeError> {
        match self.bson {
            Bson::Array(val) => AstarteData::try_from_array(val, item_type),
            _ => Err(TypeError::InvalidType),
        }
    }
}

impl TryFrom<BsonConverter> for AstarteData {
    type Error = TypeError;

    fn try_from(value: BsonConverter) -> Result<Self, Self::Error> {
        match value.mapping_type {
            MappingType::Double => value
                .bson
                .as_f64()
                .ok_or(TypeError::InvalidType)
                .and_then(AstarteData::try_from),
            MappingType::Integer => value
                .bson
                .as_i32()
                .ok_or(TypeError::InvalidType)
                .map(AstarteData::from),
            MappingType::Boolean => value
                .bson
                .as_bool()
                .ok_or(TypeError::InvalidType)
                .map(AstarteData::from),
            MappingType::LongInteger => value
                .bson
                .as_i64()
                // Astarte can send different size integer
                .or_else(|| value.bson.as_i32().map(i64::from))
                .ok_or(TypeError::InvalidType)
                .map(AstarteData::from),
            MappingType::String => match value.bson {
                Bson::String(val) => Ok(AstarteData::from(val)),
                _ => Err(TypeError::InvalidType),
            },
            MappingType::BinaryBlob => match value.bson {
                Bson::Binary(val) => Ok(AstarteData::from(val.bytes)),
                _ => Err(TypeError::InvalidType),
            },
            MappingType::DateTime => match value.bson {
                Bson::DateTime(val) => Ok(AstarteData::from(val.to_chrono())),
                _ => Err(TypeError::InvalidType),
            },
            MappingType::DoubleArray => value.try_into_array(ArrayType::Double),
            MappingType::IntegerArray => value.try_into_array(ArrayType::Integer),
            MappingType::BooleanArray => value.try_into_array(ArrayType::Boolean),
            MappingType::LongIntegerArray => value.try_into_array(ArrayType::LongInteger),
            MappingType::StringArray => value.try_into_array(ArrayType::String),
            MappingType::BinaryBlobArray => value.try_into_array(ArrayType::BinaryBlob),
            MappingType::DateTimeArray => value.try_into_array(ArrayType::DateTime),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_array() {
        let astarte_type_double = AstarteData::DoubleArray(vec![]);

        let bson: Bson = astarte_type_double.into();

        assert_eq!(bson, Bson::Array(vec![]));

        let hint = BsonConverter {
            mapping_type: MappingType::DoubleArray,
            bson,
        };

        let astarte_type_double = AstarteData::try_from(hint).expect("failed to convert");

        assert_eq!(astarte_type_double, AstarteData::DoubleArray(vec![]));
    }
}
