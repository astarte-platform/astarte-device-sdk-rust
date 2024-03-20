/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
//! Provides Astarte specific types to be used by the
//! [AstarteDeviceSdk][crate::AstarteDeviceSdk] to transmit/receivedata to/from the Astarte cluster.

use bson::{Binary, Bson};
use chrono::{DateTime, Utc};
use log::debug;
use serde::Serialize;

use crate::interface::MappingType;

/// Astarte type conversion errors.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum TypeError {
    /// Invalid floating point value
    #[error("forbidden floating point number, Nan, Infinite or subnormals are invalid")]
    FloatError,
    /// Conversion error
    #[error("conversion error")]
    Conversion,
    /// Failed to convert from Bson value
    #[error("error converting from Bson to AstarteType ({0})")]
    FromBsonError(String),
    /// Failed to convert from Bson array
    #[error("type mismatch in bson array from astarte")]
    FromBsonArrayError,
    /// Invalid type convert between the BSON and [`AstarteType`]
    #[error("type mismatch for bson and mapping")]
    InvalidType,
}

/// Types supported by the Astarte device.
///
/// An implementation of the [From] or [TryFrom] trait is provided for the encapsulated base types.
///
/// ```
/// use astarte_device_sdk::types::AstarteType;
/// use std::convert::TryInto;
///
/// let btype: bool = true;
/// let astype: AstarteType = AstarteType::from(btype);
/// assert_eq!(AstarteType::Boolean(true), astype);
/// let btype: bool = astype.try_into().unwrap();
///
/// let dtype: f64 = 42.4;
/// let astype: AstarteType = AstarteType::try_from(dtype).unwrap();
/// assert_eq!(AstarteType::Double(42.4), astype);
/// let dtype: f64 = astype.try_into().unwrap();
/// ```
///
/// For more information about the types supported by Astarte see the
/// [documentation](https://docs.astarte-platform.org/latest/080-mqtt-v1-protocol.html#astarte-data-types-to-bson-types)
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
#[serde(into = "Bson")]
pub enum AstarteType {
    Double(f64),
    Integer(i32),
    Boolean(bool),
    LongInteger(i64),
    String(String),
    BinaryBlob(Vec<u8>),
    DateTime(DateTime<Utc>),

    DoubleArray(Vec<f64>),
    IntegerArray(Vec<i32>),
    BooleanArray(Vec<bool>),
    LongIntegerArray(Vec<i64>),
    StringArray(Vec<String>),
    BinaryBlobArray(Vec<Vec<u8>>),
    DateTimeArray(Vec<DateTime<Utc>>),

    Unset,

    #[deprecated = "this will never be constructed, was kept for API compatibility but will removed in future versions"]
    EmptyArray,
}

macro_rules! check_astype_match {
    ( $self:ident, $other:ident, {$( $astartetype:tt ,)*}) => {
        match ($self, $other) {
            $((AstarteType::$astartetype(_), $crate::interface::MappingType::$astartetype) => true,)*
            _ => false,
        }
    };
}

impl PartialEq<MappingType> for AstarteType {
    fn eq(&self, other: &MappingType) -> bool {
        if other == &MappingType::LongInteger || other == &MappingType::Double {
            if let AstarteType::Integer(_) = self {
                return true;
            }
        }

        // Will be removed in a future version.
        #[allow(deprecated)]
        if self == &AstarteType::EmptyArray {
            match other {
                // The empty array should be equal to any other array
                MappingType::DoubleArray
                | MappingType::IntegerArray
                | MappingType::BooleanArray
                | MappingType::LongIntegerArray
                | MappingType::StringArray
                | MappingType::BinaryBlobArray
                | MappingType::DateTimeArray => return true,
                // Not an array, continue
                MappingType::Double
                | MappingType::Integer
                | MappingType::Boolean
                | MappingType::LongInteger
                | MappingType::String
                | MappingType::BinaryBlob
                | MappingType::DateTime => {}
            }
        }

        check_astype_match!(self, other, {
            Double,
            Integer,
            Boolean,
            LongInteger,
            String,
            BinaryBlob,
            DateTime,
            DoubleArray,
            IntegerArray,
            BooleanArray,
            LongIntegerArray,
            StringArray,
            BinaryBlobArray,
            DateTimeArray,
        })
    }
}

// we implement From<T> and PartialEq<T> from all the base types to AstarteType, using this macro
macro_rules! impl_type_conversion_traits {
    ( {$( ($typ:ty, $astartetype:tt) ,)*}) => {

        $(
                impl From<$typ> for AstarteType {
                    fn from(d: $typ) -> Self {
                        AstarteType::$astartetype(d.into())
                    }
                }

                impl From<&$typ> for AstarteType {
                    fn from(d: &$typ) -> Self {
                        AstarteType::$astartetype(d.clone().into())
                    }
                }

                impl PartialEq<$typ> for AstarteType {
                    fn eq(&self, other: &$typ) -> bool {
                        let oth: AstarteType = other.into();
                        oth == *self
                    }
                }
        )*
    };
}

impl_type_conversion_traits!({
    (i32, Integer),
    (i64, LongInteger),
    (&str, String),
    (String, String),
    (bool, Boolean),
    (Vec<u8>, BinaryBlob),
    (chrono::DateTime<chrono::Utc>, DateTime),
    (Vec<i32>, IntegerArray),
    (Vec<i64>, LongIntegerArray),
    (Vec<bool>, BooleanArray),
    (Vec<String>, StringArray),
    (Vec<Vec<u8>>, BinaryBlobArray),
    (Vec<chrono::DateTime<chrono::Utc>>, DateTimeArray),
});

// We implement float types on the side since they have different requirements
impl TryFrom<f32> for AstarteType {
    type Error = TypeError;

    fn try_from(d: f32) -> Result<Self, Self::Error> {
        if d.is_nan() || d.is_infinite() || d.is_subnormal() {
            return Err(Self::Error::FloatError);
        }
        Ok(AstarteType::Double(d.into()))
    }
}

impl TryFrom<f64> for AstarteType {
    type Error = TypeError;
    fn try_from(d: f64) -> Result<Self, Self::Error> {
        if d.is_nan() || d.is_infinite() || d.is_subnormal() {
            return Err(Self::Error::FloatError);
        }
        Ok(AstarteType::Double(d))
    }
}

impl PartialEq<f64> for AstarteType {
    fn eq(&self, other: &f64) -> bool {
        if let AstarteType::Double(dself) = self {
            dself == other
        } else {
            false
        }
    }
}

impl TryFrom<Vec<f64>> for AstarteType {
    type Error = TypeError;
    fn try_from(d: Vec<f64>) -> Result<Self, Self::Error> {
        if d.iter()
            .any(|&x| x.is_nan() || x.is_infinite() || x.is_subnormal())
        {
            return Err(Self::Error::FloatError);
        }
        Ok(AstarteType::DoubleArray(d))
    }
}

impl PartialEq<Vec<f64>> for AstarteType {
    fn eq(&self, other: &Vec<f64>) -> bool {
        if let AstarteType::DoubleArray(dself) = self {
            if dself.len() == other.len() {
                dself.iter().zip(other).all(|(&x, &y)| x == y)
            } else {
                false
            }
        } else {
            false
        }
    }
}

// we implement TryFrom<AstarteType> to all the base types, using this macro
macro_rules! impl_reverse_type_conversion_traits {
    ($(($astartetype:tt, $typ:ty),)*) => {
        $(
            impl std::convert::TryFrom<AstarteType> for $typ {
                type Error = $crate::types::TypeError;

                fn try_from(var: AstarteType) -> Result<Self, Self::Error> {
                    if let AstarteType::$astartetype(val) = var {
                        Ok(val)
                    } else {
                        Err(Self::Error::Conversion)
                    }
                }
            }
        )*
    }
}

impl TryFrom<AstarteType> for f64 {
    type Error = TypeError;
    fn try_from(var: AstarteType) -> Result<Self, Self::Error> {
        match var {
            AstarteType::Double(val) => Ok(val),
            AstarteType::Integer(val) => Ok(val.into()),
            _ => Err(TypeError::Conversion),
        }
    }
}

impl TryFrom<AstarteType> for i64 {
    type Error = TypeError;
    fn try_from(var: AstarteType) -> Result<Self, Self::Error> {
        match var {
            AstarteType::LongInteger(val) => Ok(val),
            AstarteType::Integer(val) => Ok(val.into()),
            _ => Err(TypeError::Conversion),
        }
    }
}

impl_reverse_type_conversion_traits!(
    (Integer, i32),
    (Boolean, bool),
    (String, String),
    (BinaryBlob, Vec<u8>),
    (DateTime, DateTime<Utc>),
    (DoubleArray, Vec<f64>),
    (IntegerArray, Vec<i32>),
    (BooleanArray, Vec<bool>),
    (LongIntegerArray, Vec<i64>),
    (StringArray, Vec<String>),
    (BinaryBlobArray, Vec<Vec<u8>>),
    (DateTimeArray, Vec<DateTime<Utc>>),
);

impl From<AstarteType> for Bson {
    fn from(d: AstarteType) -> Self {
        match d {
            AstarteType::Double(d) => Bson::Double(d),
            AstarteType::Integer(d) => Bson::Int32(d),
            AstarteType::Boolean(d) => Bson::Boolean(d),
            AstarteType::LongInteger(d) => Bson::Int64(d),
            AstarteType::String(d) => Bson::String(d),
            AstarteType::BinaryBlob(d) => Bson::Binary(Binary {
                bytes: d,
                subtype: bson::spec::BinarySubtype::Generic,
            }),
            AstarteType::DateTime(d) => Bson::DateTime(d.into()),
            AstarteType::DoubleArray(d) => d.into_iter().collect(),
            AstarteType::IntegerArray(d) => d.into_iter().collect(),
            AstarteType::BooleanArray(d) => d.into_iter().collect(),
            AstarteType::LongIntegerArray(d) => d.into_iter().collect(),
            AstarteType::StringArray(d) => d.into_iter().collect(),
            AstarteType::BinaryBlobArray(d) => d
                .into_iter()
                .map(|bytes| Binary {
                    bytes,
                    subtype: bson::spec::BinarySubtype::Generic,
                })
                .collect(),
            AstarteType::DateTimeArray(d) => d.into_iter().collect(),
            AstarteType::Unset => Bson::Null,
            // Will be removed in a future version
            #[allow(deprecated)]
            AstarteType::EmptyArray => Bson::Array(Vec::new()),
        }
    }
}

/// Utility to convert a Bson array into an [`AstarteType`] array.
fn bson_array<T, F>(array: Vec<Bson>, f: F) -> Result<Vec<T>, TypeError>
where
    F: FnMut(Bson) -> Option<T>,
{
    array
        .into_iter()
        .map(f)
        .map(|item| item.ok_or(TypeError::FromBsonArrayError))
        .collect()
}

impl AstarteType {
    #[deprecated = "this method will be removed in the next major update"]
    pub fn from_bson_vec(d: Vec<Bson>) -> Result<Vec<Self>, TypeError> {
        d.into_iter()
            .map(|b| match b {
                Bson::Double(v) => v.try_into(),
                Bson::String(v) => Ok(v.into()),
                Bson::Boolean(v) => Ok(v.into()),
                Bson::Int32(v) => Ok(v.into()),
                Bson::Int64(v) => Ok(v.into()),
                Bson::Binary(v) => Ok(v.bytes.into()),
                Bson::DateTime(v) => Ok(v.to_chrono().into()),
                _ => Err(TypeError::FromBsonArrayError),
            })
            .collect()
    }

    /// Check if the variant is [`AstarteType::Unset`].
    pub fn is_unset(&self) -> bool {
        matches!(self, AstarteType::Unset)
    }

    /// Convert a non empty BSON array to astarte array with all the elements of the same type.
    pub(crate) fn try_from_array(
        array: Vec<Bson>,
        item_type: ArrayType,
    ) -> Result<Self, TypeError> {
        if array.is_empty() {
            return Ok(item_type.as_empty());
        }

        match item_type {
            ArrayType::Double => bson_array(array, |b| b.as_f64()).and_then(AstarteType::try_from),
            ArrayType::Integer => bson_array(array, |b| b.as_i32()).map(AstarteType::from),
            ArrayType::Boolean => bson_array(array, |b| b.as_bool()).map(AstarteType::from),
            ArrayType::LongInteger => {
                bson_array(array, |b| {
                    // Astarte can send different size integer
                    b.as_i64().or_else(|| b.as_i32().map(i64::from))
                })
                .map(AstarteType::from)
            }
            ArrayType::String => {
                // Take the same string and don't use as_str
                bson_array(array, |b| match b {
                    Bson::String(s) => Some(s),
                    _ => None,
                })
                .map(AstarteType::from)
            }
            ArrayType::BinaryBlob => {
                // Take the same buf allocation
                bson_array(array, |b| match b {
                    Bson::Binary(b) => Some(b.bytes),
                    _ => None,
                })
                .map(AstarteType::from)
            }
            ArrayType::DateTime => {
                // Manually convert to chrono
                bson_array(array, |b| match b {
                    Bson::DateTime(d) => Some(d.to_chrono()),
                    _ => None,
                })
                .map(AstarteType::from)
            }
        }
    }

    pub(crate) fn display_type(&self) -> &'static str {
        match self {
            AstarteType::Double(_) => "double",
            AstarteType::Integer(_) => "integer",
            AstarteType::Boolean(_) => "boolean",
            AstarteType::LongInteger(_) => "long integer",
            AstarteType::String(_) => "string",
            AstarteType::BinaryBlob(_) => "binary blob",
            AstarteType::DateTime(_) => "datetime",
            AstarteType::DoubleArray(_) => "double array",
            AstarteType::IntegerArray(_) => "integer array",
            AstarteType::BooleanArray(_) => "boolean array",
            AstarteType::LongIntegerArray(_) => "long integer array",
            AstarteType::StringArray(_) => "string array",
            AstarteType::BinaryBlobArray(_) => "binary blob array",
            AstarteType::DateTimeArray(_) => "datetime array",
            AstarteType::Unset => "unset",
            #[allow(deprecated)]
            AstarteType::EmptyArray => "empty array",
        }
    }
}

/// Type of astarte arrays.
///
/// It's used to deserialize a BSON array to the given [`AstarteType`] given this type.
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
    /// Create an empty [`AstarteType`] array.
    fn as_empty(&self) -> AstarteType {
        match self {
            ArrayType::Double => AstarteType::DoubleArray(Vec::new()),
            ArrayType::Integer => AstarteType::IntegerArray(Vec::new()),
            ArrayType::Boolean => AstarteType::BooleanArray(Vec::new()),
            ArrayType::LongInteger => AstarteType::LongIntegerArray(Vec::new()),
            ArrayType::String => AstarteType::StringArray(Vec::new()),
            ArrayType::BinaryBlob => AstarteType::BinaryBlobArray(Vec::new()),
            ArrayType::DateTime => AstarteType::DateTimeArray(Vec::new()),
        }
    }
}

/// Struct to convert a BSON to an [`AstarteType`] with the given [`MappingType`] as hint.
///
/// The struct is needed to convert an [`Bson`] value to an [`AstarteType`] since the Astarte's
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

    /// Tries to convert the [`BsonHint::bson`] to an astarte array.
    fn try_into_array(self, item_type: ArrayType) -> Result<AstarteType, TypeError> {
        match self.bson {
            Bson::Array(val) => AstarteType::try_from_array(val, item_type),
            _ => Err(TypeError::InvalidType),
        }
    }
}

impl TryFrom<BsonConverter> for AstarteType {
    type Error = TypeError;

    fn try_from(value: BsonConverter) -> Result<Self, Self::Error> {
        if value.bson == Bson::Null {
            debug!("bson null returning unset");

            return Ok(AstarteType::Unset);
        }

        match value.mapping_type {
            MappingType::Double => value
                .bson
                .as_f64()
                .ok_or(TypeError::InvalidType)
                .and_then(AstarteType::try_from),
            MappingType::Integer => value
                .bson
                .as_i32()
                .ok_or(TypeError::InvalidType)
                .map(AstarteType::from),
            MappingType::Boolean => value
                .bson
                .as_bool()
                .ok_or(TypeError::InvalidType)
                .map(AstarteType::from),
            MappingType::LongInteger => value
                .bson
                .as_i64()
                // Astarte can send different size integer
                .or_else(|| value.bson.as_i32().map(i64::from))
                .ok_or(TypeError::InvalidType)
                .map(AstarteType::from),
            MappingType::String => match value.bson {
                Bson::String(val) => Ok(AstarteType::from(val)),
                _ => Err(TypeError::InvalidType),
            },
            MappingType::BinaryBlob => match value.bson {
                Bson::Binary(val) => Ok(AstarteType::from(val.bytes)),
                _ => Err(TypeError::InvalidType),
            },
            MappingType::DateTime => match value.bson {
                Bson::DateTime(val) => Ok(AstarteType::from(val.to_chrono())),
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
mod test {
    use chrono::TimeZone;

    use crate::Aggregation;

    use super::*;

    #[test]
    fn test_eq() {
        assert!(AstarteType::Double(12.21) == 12.21_f64);
        assert!(AstarteType::Integer(12) == 12_i32);
        assert!(AstarteType::Boolean(false) == false);
        assert!(AstarteType::LongInteger(42) == 42_i64);
        assert!(AstarteType::String("hello".to_string()) == "hello");
        assert!(AstarteType::BinaryBlob(vec![1, 2, 3, 4]) == vec![1_u8, 2, 3, 4]);
        let data: chrono::DateTime<Utc> = TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap();
        assert!(AstarteType::DateTime(data) == data);
        let data: Vec<f64> = vec![1.3, 2.6, 3.1, 4.0];
        assert!(AstarteType::DoubleArray(data.clone()) == data);
        let data: Vec<i32> = vec![1, 2, 3, 4];
        assert!(AstarteType::IntegerArray(data.clone()) == data);
        let data: Vec<bool> = vec![true, false, true, true];
        assert!(AstarteType::BooleanArray(data.clone()) == data);
        let data: Vec<i64> = vec![32, 11, 33, 1];
        assert!(AstarteType::LongIntegerArray(data.clone()) == data);
        let data: Vec<String> = vec!["Hello".to_string(), " world!".to_string()];
        assert!(AstarteType::StringArray(data.clone()) == data);
        let data: Vec<Vec<u8>> = vec![vec![1, 2, 3, 4], vec![4, 4, 1, 4]];
        assert!(AstarteType::BinaryBlobArray(data.clone()) == data);
        let data: Vec<chrono::DateTime<Utc>> = vec![
            TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
            TimeZone::timestamp_opt(&Utc, 1611580808, 0).unwrap(),
        ];
        assert!(AstarteType::DateTimeArray(data.clone()) == data);
    }

    #[test]
    fn test_conversion_to_astarte_type() -> Result<(), TypeError> {
        let data: f64 = 42.24;
        let a_data: AstarteType = data.try_into()?;
        assert_eq!(AstarteType::Double(data), a_data);

        let data: f32 = 42.24;
        let a_data: AstarteType = data.try_into()?;
        assert_eq!(AstarteType::Double(data as f64), a_data);

        let data: i32 = 42;
        let a_data: AstarteType = data.into();
        assert_eq!(AstarteType::Integer(data), a_data);

        let data: i64 = 42;
        let a_data: AstarteType = data.into();
        assert_eq!(AstarteType::LongInteger(data), a_data);

        let data: &str = "Hello";
        let a_data: AstarteType = data.into();
        assert_eq!(AstarteType::String(data.to_string()), a_data);

        let data: String = String::from("Hello");
        let a_data: AstarteType = data.clone().into();
        assert_eq!(AstarteType::String(data), a_data);

        let data: bool = false;
        let a_data: AstarteType = data.into();
        assert_eq!(AstarteType::Boolean(data), a_data);

        let data: Vec<u8> = vec![100, 101];
        let a_data: AstarteType = data.clone().into();
        assert_eq!(AstarteType::BinaryBlob(data), a_data);

        let data: chrono::DateTime<chrono::Utc> =
            TimeZone::timestamp_opt(&Utc, 1627580808, 12).unwrap();
        let a_data: AstarteType = data.into();
        assert_eq!(AstarteType::DateTime(data), a_data);

        let data: Vec<f64> = vec![1.2, 11.6];
        let a_data: AstarteType = data.clone().try_into().unwrap();
        assert_eq!(AstarteType::DoubleArray(data), a_data);

        let data: Vec<i32> = vec![5, -4];
        let a_data: AstarteType = data.clone().into();
        assert_eq!(AstarteType::IntegerArray(data), a_data);

        let data: Vec<i64> = vec![11, 23234];
        let a_data: AstarteType = data.clone().into();
        assert_eq!(AstarteType::LongIntegerArray(data), a_data);

        let data: Vec<bool> = vec![true, false];
        let a_data: AstarteType = data.clone().into();
        assert_eq!(AstarteType::BooleanArray(data), a_data);

        let data: Vec<String> = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let a_data: AstarteType = data.clone().into();
        assert_eq!(AstarteType::StringArray(data), a_data);

        let data: Vec<Vec<u8>> = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
        let a_data: AstarteType = data.clone().into();
        assert_eq!(AstarteType::BinaryBlobArray(data), a_data);

        let data: Vec<chrono::DateTime<chrono::Utc>> = vec![
            TimeZone::timestamp_opt(&Utc, 1627580808, 12).unwrap(),
            TimeZone::timestamp_opt(&Utc, 3455667775, 42).unwrap(),
            TimeZone::timestamp_opt(&Utc, 4646841646, 11).unwrap(),
        ];
        let a_data: AstarteType = data.clone().into();
        assert_eq!(AstarteType::DateTimeArray(data), a_data);

        Ok(())
    }

    #[test]
    fn test_conversion_from_astarte_type() -> Result<(), TypeError> {
        let data = 42.24;
        let a_data = AstarteType::Double(data);
        assert_eq!(f64::try_from(a_data)?, data);

        let data = 43;
        let a_data = AstarteType::Integer(data);
        assert_eq!(i32::try_from(a_data)?, data);

        let data = true;
        let a_data = AstarteType::Boolean(data);
        assert_eq!(bool::try_from(a_data)?, data);

        let data = 62;
        let a_data = AstarteType::LongInteger(data);
        assert_eq!(i64::try_from(a_data)?, data);

        let data = "something".to_string();
        let a_data = AstarteType::String(data.clone());
        assert_eq!(String::try_from(a_data)?, data);

        let data = vec![1, 2, 3];
        let a_data = AstarteType::BinaryBlob(data.clone());
        assert_eq!(Vec::<u8>::try_from(a_data)?, data);

        let data = TimeZone::timestamp_opt(&Utc, 1627580808, 12).unwrap();
        let a_data = AstarteType::DateTime(data);
        assert_eq!(DateTime::<Utc>::try_from(a_data)?, data);

        let data = vec![1.4, 2.4, 3.1];
        let a_data = AstarteType::DoubleArray(data.clone());
        assert_eq!(Vec::<f64>::try_from(a_data)?, data);

        let data = vec![1, 2, 3];
        let a_data = AstarteType::IntegerArray(data.clone());
        assert_eq!(Vec::<i32>::try_from(a_data)?, data);

        let data = vec![true, false, true];
        let a_data = AstarteType::BooleanArray(data.clone());
        assert_eq!(Vec::<bool>::try_from(a_data)?, data);

        let data = vec![1, 2, 3];
        let a_data = AstarteType::LongIntegerArray(data.clone());
        assert_eq!(Vec::<i64>::try_from(a_data)?, data);

        let data = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let a_data = AstarteType::StringArray(data.clone());
        assert_eq!(Vec::<String>::try_from(a_data)?, data);

        let data = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
        let a_data = AstarteType::BinaryBlobArray(data.clone());
        assert_eq!(Vec::<Vec<u8>>::try_from(a_data)?, data);

        let data = vec![
            TimeZone::timestamp_opt(&Utc, 1627580808, 12).unwrap(),
            TimeZone::timestamp_opt(&Utc, 3455667775, 42).unwrap(),
            TimeZone::timestamp_opt(&Utc, 4646841646, 11).unwrap(),
        ];
        let a_data = AstarteType::DateTimeArray(data.clone());
        assert_eq!(Vec::<DateTime<Utc>>::try_from(a_data)?, data);

        Ok(())
    }

    #[test]
    fn test_eq_astarte_type_with_mapping_type() {
        assert_eq!(AstarteType::Double(0.0), MappingType::Double);
        assert_eq!(AstarteType::Integer(0), MappingType::Double);

        assert_eq!(AstarteType::Integer(0), MappingType::Integer);
        assert_ne!(AstarteType::Double(0.0), MappingType::Integer);
        assert_eq!(AstarteType::Integer(0), MappingType::LongInteger);

        assert_eq!(AstarteType::LongInteger(0), MappingType::LongInteger);
    }

    #[test]
    fn test_conversion_from_astarte_integer_to_f64() {
        let astarte_type_double = AstarteType::Integer(5);
        let astarte_ind = Aggregation::Individual(astarte_type_double);

        if let Aggregation::Individual(var) = astarte_ind {
            let value: f64 = var.try_into().unwrap();
            assert_eq!(5.0, value);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_conversion_from_astarte_integer_to_i64() {
        let astarte_type_double = AstarteType::Integer(5);
        let astarte_ind = Aggregation::Individual(astarte_type_double);

        if let Aggregation::Individual(var) = astarte_ind {
            let value: i64 = var.try_into().unwrap();
            assert_eq!(5, value);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_empty_array() {
        let astarte_type_double = AstarteType::DoubleArray(vec![]);

        let bson: Bson = astarte_type_double.into();

        assert_eq!(bson, Bson::Array(vec![]));

        let hint = BsonConverter {
            mapping_type: MappingType::DoubleArray,
            bson,
        };

        let astarte_type_double = AstarteType::try_from(hint).expect("failed to convert");

        assert_eq!(astarte_type_double, AstarteType::DoubleArray(vec![]));
    }

    #[test]
    fn tesat_float_validation() {
        assert_eq!(
            AstarteType::try_from(54.4).unwrap(),
            AstarteType::Double(54.4)
        );
        AstarteType::try_from(f64::NAN).unwrap_err();
        AstarteType::try_from(vec![1.0, 2.0, f64::NAN, 4.0]).unwrap_err();
    }
}
