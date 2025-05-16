// This file is part of Astarte.
//
// Copyright 2021 - 2025 SECO Mind Srl
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

//! Provides Astarte specific types to be used by the [Client][crate::DeviceClient] to
//! transmit/receive data to/from the Astarte cluster.

use std::borrow::Borrow;
use std::fmt::Display;
use std::ops::Deref;

use astarte_interfaces::schema::MappingType;
use bson::{Binary, Bson};
use serde::Serialize;

use crate::Timestamp;

macro_rules! check_astype_match {
    ( $self:ident, $other:ident, {$( $astartetype:tt ,)*}) => {
        match ($self, $other) {
            $((AstarteType::$astartetype(_), ::astarte_interfaces::schema::MappingType::$astartetype) => true,)*
            _ => false,
        }
    };
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

                impl PartialEq<$typ> for AstarteType {
                    fn eq(&self, other: &$typ) -> bool {
                        let AstarteType::$astartetype(value) = self else {
                            return false;
                        };

                        value == other
                    }
                }

                impl PartialEq<AstarteType> for $typ {
                    fn eq(&self, other: &AstarteType) -> bool {
                        other.eq(self)
                    }
                }
        )*
    };
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
                        Err(Self::Error::conversion(format!("from {} into $typ", var.display_type())))
                    }
                }
            }
        )*
    }
}

/// Astarte type conversion errors.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum TypeError {
    /// Invalid floating point value
    #[error("forbidden floating point number, Nan, Infinite or subnormal numbers are invalid")]
    Float,
    /// Conversion error
    #[error("couldn't convert value {ctx}")]
    Conversion {
        /// Context of the failed conversion
        ctx: String,
    },
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

impl TypeError {
    /// Error with context for the conversion
    pub(crate) const fn conversion(ctx: String) -> Self {
        Self::Conversion { ctx }
    }
}

/// Types supported by the Astarte device.
///
/// An implementation of the [From] or [TryFrom] trait is provided for the encapsulated base types.
///
/// ```
/// use astarte_device_sdk::types::AstarteType;
/// use std::convert::TryInto;
///
/// let b_type: bool = true;
/// let as_type: AstarteType = AstarteType::from(b_type);
/// assert_eq!(AstarteType::Boolean(true), as_type);
/// let b_type: bool = as_type.try_into().unwrap();
///
/// let d_type: f64 = 42.4;
/// let as_type: AstarteType = AstarteType::try_from(d_type).unwrap();
/// assert_eq!(as_type, AstarteType::Double(42.4.try_into().unwrap()));
/// let d_type: f64 = as_type.try_into().unwrap();
/// ```
///
/// For more information about the types supported by Astarte see the
/// [documentation](https://docs.astarte-platform.org/latest/080-mqtt-v1-protocol.html#astarte-data-types-to-bson-types)
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
#[serde(into = "Bson")]
pub enum AstarteType {
    /// Double value.
    ///
    /// This is guaranteed not to be a [subnormal](https://en.wikipedia.org/wiki/Subnormal_number) or
    /// `+inf`, `Nan`, etc...
    Double(Double),
    /// Singed integer value.
    Integer(i32),
    /// Boolean value.
    Boolean(bool),
    /// Long integer value.
    ///
    /// During transport this can be received from Astarte as a `i32` to save space.
    LongInteger(i64),
    /// String value.
    String(String),
    /// Binary value.
    BinaryBlob(Vec<u8>),
    /// Date time value.
    ///
    /// UTC date time.
    DateTime(Timestamp),
    /// Double array value.
    DoubleArray(Vec<Double>),
    /// Integer array value.
    IntegerArray(Vec<i32>),
    /// Boolean array value.
    BooleanArray(Vec<bool>),
    /// Long integer array value.
    ///
    /// During transport this can be received from Astarte as a mixed `i32` and `i64` array to save
    /// space.
    LongIntegerArray(Vec<i64>),
    /// String array value.
    StringArray(Vec<String>),
    /// Binary array value.
    BinaryBlobArray(Vec<Vec<u8>>),
    /// Date time array value.
    ///
    /// UTC date time.
    DateTimeArray(Vec<Timestamp>),
}

impl AstarteType {
    pub(crate) fn eq_mapping_type(&self, other: MappingType) -> bool {
        if other == MappingType::LongInteger || other == MappingType::Double {
            if let AstarteType::Integer(_) = self {
                return true;
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
        }
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

impl PartialEq<AstarteType> for f64 {
    fn eq(&self, other: &AstarteType) -> bool {
        other.eq(self)
    }
}

impl TryFrom<f64> for AstarteType {
    type Error = TypeError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Double::try_from(value).map(AstarteType::Double)
    }
}

impl TryFrom<Vec<f64>> for AstarteType {
    type Error = TypeError;

    fn try_from(value: Vec<f64>) -> Result<Self, Self::Error> {
        value
            .into_iter()
            .map(Double::try_from)
            .collect::<Result<Vec<Double>, Self::Error>>()
            .map(AstarteType::DoubleArray)
    }
}

impl TryFrom<AstarteType> for Vec<f64> {
    type Error = TypeError;

    fn try_from(value: AstarteType) -> Result<Self, Self::Error> {
        let AstarteType::DoubleArray(value) = value else {
            return Err(TypeError::conversion(format!(
                "from {} into Vec<f64>",
                value.display_type()
            )));
        };

        let vec = value.into_iter().map(Double::into).collect();

        Ok(vec)
    }
}

impl PartialEq<Vec<f64>> for AstarteType {
    fn eq(&self, other: &Vec<f64>) -> bool {
        let AstarteType::DoubleArray(this) = self else {
            return false;
        };

        if this.len() != other.len() {
            return false;
        }

        this.iter().zip(other).all(|(x, y)| x == y)
    }
}

impl PartialEq<AstarteType> for Vec<f64> {
    fn eq(&self, other: &AstarteType) -> bool {
        other.eq(self)
    }
}

impl TryFrom<AstarteType> for f64 {
    type Error = TypeError;

    fn try_from(value: AstarteType) -> Result<Self, Self::Error> {
        match value {
            AstarteType::Double(val) => Ok(val.into()),
            _ => Err(TypeError::conversion(format!(
                "from {} into f64",
                value.display_type()
            ))),
        }
    }
}

impl TryFrom<AstarteType> for i64 {
    type Error = TypeError;
    fn try_from(value: AstarteType) -> Result<Self, Self::Error> {
        match value {
            AstarteType::LongInteger(val) => Ok(val),
            AstarteType::Integer(val) => Ok(val.into()),
            _ => Err(TypeError::conversion(format!(
                "from {} into i64",
                value.display_type()
            ))),
        }
    }
}

impl_type_conversion_traits!({
    (i32, Integer),
    (i64, LongInteger),
    (&str, String),
    (String, String),
    (bool, Boolean),
    (Double, Double),
    (Vec<u8>, BinaryBlob),
    (chrono::DateTime<chrono::Utc>, DateTime),
    (Vec<i32>, IntegerArray),
    (Vec<i64>, LongIntegerArray),
    (Vec<bool>, BooleanArray),
    (Vec<String>, StringArray),
    (Vec<Vec<u8>>, BinaryBlobArray),
    (Vec<chrono::DateTime<chrono::Utc>>, DateTimeArray),
    (Vec<Double>, DoubleArray),
});

impl_reverse_type_conversion_traits!(
    (Double, Double),
    (Integer, i32),
    (Boolean, bool),
    (String, String),
    (BinaryBlob, Vec<u8>),
    (DateTime, Timestamp),
    (IntegerArray, Vec<i32>),
    (BooleanArray, Vec<bool>),
    (LongIntegerArray, Vec<i64>),
    (StringArray, Vec<String>),
    (BinaryBlobArray, Vec<Vec<u8>>),
    (DateTimeArray, Vec<Timestamp>),
);

impl From<AstarteType> for Bson {
    fn from(d: AstarteType) -> Self {
        match d {
            AstarteType::Double(d) => Bson::Double(d.0),
            AstarteType::Integer(d) => Bson::Int32(d),
            AstarteType::Boolean(d) => Bson::Boolean(d),
            AstarteType::LongInteger(d) => Bson::Int64(d),
            AstarteType::String(d) => Bson::String(d),
            AstarteType::BinaryBlob(d) => Bson::Binary(Binary {
                bytes: d,
                subtype: bson::spec::BinarySubtype::Generic,
            }),
            AstarteType::DateTime(d) => Bson::DateTime(d.into()),
            AstarteType::DoubleArray(d) => d.into_iter().map(f64::from).collect(),
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

/// A valid Astarte [`f64`].
///
/// It cannot be Nan, Infinite or subnormal.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Double(f64);

impl TryFrom<f64> for Double {
    type Error = TypeError;
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value.is_nan() || value.is_infinite() || value.is_subnormal() {
            return Err(Self::Error::Float);
        }

        Ok(Self(value))
    }
}

// NOTE: do not implement conversions from f32 into Double, since it looses precision. So it's better
//       handled by the final user.
impl From<Double> for f64 {
    fn from(value: Double) -> Self {
        value.0
    }
}

impl Display for Double {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Borrow<f64> for Double {
    fn borrow(&self) -> &f64 {
        &self.0
    }
}

impl Deref for Double {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<f64> for Double {
    fn eq(&self, other: &f64) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<Double> for f64 {
    fn eq(&self, other: &Double) -> bool {
        self.eq(&other.0)
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
    use chrono::{DateTime, TimeZone, Utc};
    use pretty_assertions::assert_eq;

    use super::*;

    fn all_astarte_types() -> Vec<AstarteType> {
        vec![
            AstarteType::Double(12.21.try_into().unwrap()),
            AstarteType::Integer(12),
            AstarteType::Boolean(false),
            AstarteType::LongInteger(42),
            AstarteType::String("hello".to_string()),
            AstarteType::BinaryBlob(vec![1, 2, 3, 4]),
            AstarteType::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteType::DoubleArray(
                [1.3, 2.6, 3.1, 4.0]
                    .map(|v| Double::try_from(v).unwrap())
                    .to_vec(),
            ),
            AstarteType::IntegerArray(vec![1, 2, 3, 4]),
            AstarteType::BooleanArray(vec![true, false, true, true]),
            AstarteType::LongIntegerArray(vec![32, 11, 33, 1]),
            AstarteType::StringArray(vec!["Hello".to_string(), " world!".to_string()]),
            AstarteType::BinaryBlobArray(vec![vec![1, 2, 3, 4], vec![4, 4, 1, 4]]),
            AstarteType::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1611580808, 0).unwrap(),
            ]),
        ]
    }

    #[test]
    fn test_eq() {
        for case in all_astarte_types() {
            match case.clone() {
                AstarteType::Double(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    let f = f64::from(value);
                    assert_eq!(case, f);
                    assert_eq!(f, case);
                    assert_ne!(case, false);
                }
                AstarteType::Integer(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::Boolean(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, 42f64);
                }
                AstarteType::LongInteger(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::String(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::BinaryBlob(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::DateTime(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::DoubleArray(value) => {
                    let value: Vec<f64> = value.into_iter().map(f64::from).collect();

                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::IntegerArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::BooleanArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::LongIntegerArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::StringArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::BinaryBlobArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteType::DateTimeArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
            }
        }
    }

    #[test]
    fn test_conversion_to_astarte_type() {
        for case in all_astarte_types() {
            match case.clone() {
                AstarteType::Double(value) => {
                    let inv: Double = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::Integer(value) => {
                    let inv: i32 = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::Boolean(value) => {
                    let inv: bool = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    f64::try_from(case).unwrap_err();
                }
                AstarteType::LongInteger(value) => {
                    let inv: i64 = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::String(value) => {
                    let inv: String = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::BinaryBlob(value) => {
                    let inv: Vec<u8> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::DateTime(value) => {
                    let inv: DateTime<Utc> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::DoubleArray(value) => {
                    let inv: Vec<f64> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::IntegerArray(value) => {
                    let inv: Vec<i32> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::BooleanArray(value) => {
                    let inv: Vec<bool> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::LongIntegerArray(value) => {
                    let inv: Vec<i64> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::StringArray(value) => {
                    let inv: Vec<String> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::BinaryBlobArray(value) => {
                    let inv: Vec<Vec<u8>> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteType::DateTimeArray(value) => {
                    let inv: Vec<DateTime<Utc>> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteType::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
            }
        }
    }

    #[test]
    fn test_eq_astarte_type_with_mapping_type() {
        assert!(AstarteType::Double(0.0.try_into().unwrap()).eq_mapping_type(MappingType::Double));
        assert!(AstarteType::Integer(0).eq_mapping_type(MappingType::Double));

        assert!(AstarteType::Integer(0).eq_mapping_type(MappingType::Integer));
        assert!(!AstarteType::Double(0.0.try_into().unwrap()).eq_mapping_type(MappingType::Integer));
        assert!(AstarteType::Integer(0).eq_mapping_type(MappingType::LongInteger));

        assert!(AstarteType::LongInteger(0).eq_mapping_type(MappingType::LongInteger));
    }

    #[test]
    fn test_conversion_from_astarte_integer_to_f64() {
        let value = AstarteType::Integer(5);

        f64::try_from(value).unwrap_err();
    }

    #[test]
    fn test_conversion_from_astarte_integer_and_long_integer_to_i64() {
        let value: i64 = AstarteType::Integer(5).try_into().unwrap();

        assert_eq!(value, 5);

        let value: i64 = AstarteType::LongInteger(5).try_into().unwrap();

        assert_eq!(value, 5);

        i64::try_from(AstarteType::Boolean(false)).unwrap_err();
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
            AstarteType::Double(54.4.try_into().unwrap())
        );
        AstarteType::try_from(f64::NAN).unwrap_err();
        AstarteType::try_from(vec![1.0, 2.0, f64::NAN, 4.0]).unwrap_err();
    }
}
