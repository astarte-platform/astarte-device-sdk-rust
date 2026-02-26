// This file is part of Astarte.
//
// Copyright 2021-2026 SECO Mind Srl
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
use bson::Bson;
use serde::Serialize;

use crate::Timestamp;

use self::de::{ArrayType, bson_array};

pub(crate) mod de;
mod display;

macro_rules! check_astype_match {
    ( $self:ident, $other:ident, {$( $variant:tt ,)*}) => {
        match ($self, $other) {
            $((AstarteData::$variant(_), ::astarte_interfaces::schema::MappingType::$variant) => true,)*
            _ => false,
        }
    };
}

// we implement From<T> and PartialEq<T> from all the base types to AstarteData, using this macro
macro_rules! impl_type_conversion_traits {
    ( {$( ($typ:ty, $variant:tt) ,)*}) => {
        $(
                impl From<$typ> for AstarteData {
                    fn from(d: $typ) -> Self {
                        AstarteData::$variant(d.into())
                    }
                }

                impl PartialEq<$typ> for AstarteData {
                    fn eq(&self, other: &$typ) -> bool {
                        let AstarteData::$variant(value) = self else {
                            return false;
                        };

                        value == other
                    }
                }

                impl PartialEq<AstarteData> for $typ {
                    fn eq(&self, other: &AstarteData) -> bool {
                        other.eq(self)
                    }
                }
        )*
    };
}

// we implement TryFrom<AstarteData> to all the base types, using this macro
macro_rules! impl_reverse_type_conversion_traits {
    ($(($variant:tt, $typ:ty),)*) => {
        $(
            impl std::convert::TryFrom<AstarteData> for $typ {
                type Error = $crate::types::TypeError;

                fn try_from(var: AstarteData) -> Result<Self, Self::Error> {
                    if let AstarteData::$variant(val) = var {
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
    #[error("error converting from Bson to AstarteData ({0})")]
    FromBsonError(String),
    /// Failed to convert from Bson array
    #[error("type mismatch in bson array from astarte")]
    FromBsonArrayError,
    /// Invalid type convert between the BSON and [`AstarteData`]
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
/// use astarte_device_sdk::types::AstarteData;
/// use std::convert::TryInto;
///
/// let b_type: bool = true;
/// let as_type: AstarteData = AstarteData::from(b_type);
/// assert_eq!(AstarteData::Boolean(true), as_type);
/// let b_type: bool = as_type.try_into().unwrap();
///
/// let d_type: f64 = 42.4;
/// let as_type: AstarteData = AstarteData::try_from(d_type).unwrap();
/// assert_eq!(as_type, AstarteData::Double(42.4.try_into().unwrap()));
/// let d_type: f64 = as_type.try_into().unwrap();
/// ```
///
/// For more information about the types supported by Astarte see the
/// [documentation](https://docs.astarte-platform.org/latest/080-mqtt-v1-protocol.html#astarte-data-types-to-bson-types)
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
#[serde(into = "Bson")]
pub enum AstarteData {
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

impl AstarteData {
    pub(crate) fn eq_mapping_type(&self, other: MappingType) -> bool {
        if other == MappingType::LongInteger || other == MappingType::Double {
            if let AstarteData::Integer(_) = self {
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
            ArrayType::Double => bson_array(array, |b| b.as_f64()).and_then(AstarteData::try_from),
            ArrayType::Integer => bson_array(array, |b| b.as_i32()).map(AstarteData::from),
            ArrayType::Boolean => bson_array(array, |b| b.as_bool()).map(AstarteData::from),
            ArrayType::LongInteger => {
                bson_array(array, |b| {
                    // Astarte can send different size integer
                    b.as_i64().or_else(|| b.as_i32().map(i64::from))
                })
                .map(AstarteData::from)
            }
            ArrayType::String => {
                // Take the same string and don't use as_str
                bson_array(array, |b| match b {
                    Bson::String(s) => Some(s),
                    _ => None,
                })
                .map(AstarteData::from)
            }
            ArrayType::BinaryBlob => {
                // Take the same buf allocation
                bson_array(array, |b| match b {
                    Bson::Binary(b) => Some(b.bytes),
                    _ => None,
                })
                .map(AstarteData::from)
            }
            ArrayType::DateTime => {
                // Manually convert to chrono
                bson_array(array, |b| match b {
                    Bson::DateTime(d) => Some(d.to_chrono()),
                    _ => None,
                })
                .map(AstarteData::from)
            }
        }
    }

    pub(crate) fn display_type(&self) -> &'static str {
        match self {
            AstarteData::Double(_) => "double",
            AstarteData::Integer(_) => "integer",
            AstarteData::Boolean(_) => "boolean",
            AstarteData::LongInteger(_) => "long integer",
            AstarteData::String(_) => "string",
            AstarteData::BinaryBlob(_) => "binary blob",
            AstarteData::DateTime(_) => "datetime",
            AstarteData::DoubleArray(_) => "double array",
            AstarteData::IntegerArray(_) => "integer array",
            AstarteData::BooleanArray(_) => "boolean array",
            AstarteData::LongIntegerArray(_) => "long integer array",
            AstarteData::StringArray(_) => "string array",
            AstarteData::BinaryBlobArray(_) => "binary blob array",
            AstarteData::DateTimeArray(_) => "datetime array",
        }
    }
}

impl PartialEq<f64> for AstarteData {
    fn eq(&self, other: &f64) -> bool {
        if let AstarteData::Double(dself) = self {
            dself == other
        } else {
            false
        }
    }
}

impl PartialEq<AstarteData> for f64 {
    fn eq(&self, other: &AstarteData) -> bool {
        other.eq(self)
    }
}

impl TryFrom<f64> for AstarteData {
    type Error = TypeError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Double::try_from(value).map(AstarteData::Double)
    }
}

impl TryFrom<Vec<f64>> for AstarteData {
    type Error = TypeError;

    fn try_from(value: Vec<f64>) -> Result<Self, Self::Error> {
        value
            .into_iter()
            .map(Double::try_from)
            .collect::<Result<Vec<Double>, Self::Error>>()
            .map(AstarteData::DoubleArray)
    }
}

impl TryFrom<AstarteData> for Vec<f64> {
    type Error = TypeError;

    fn try_from(value: AstarteData) -> Result<Self, Self::Error> {
        let AstarteData::DoubleArray(value) = value else {
            return Err(TypeError::conversion(format!(
                "from {} into Vec<f64>",
                value.display_type()
            )));
        };

        let vec = value.into_iter().map(Double::into).collect();

        Ok(vec)
    }
}

impl PartialEq<Vec<f64>> for AstarteData {
    fn eq(&self, other: &Vec<f64>) -> bool {
        let AstarteData::DoubleArray(this) = self else {
            return false;
        };

        if this.len() != other.len() {
            return false;
        }

        this.iter().zip(other).all(|(x, y)| x == y)
    }
}

impl PartialEq<AstarteData> for Vec<f64> {
    fn eq(&self, other: &AstarteData) -> bool {
        other.eq(self)
    }
}

impl TryFrom<AstarteData> for f64 {
    type Error = TypeError;

    fn try_from(value: AstarteData) -> Result<Self, Self::Error> {
        match value {
            AstarteData::Double(val) => Ok(val.into()),
            _ => Err(TypeError::conversion(format!(
                "from {} into f64",
                value.display_type()
            ))),
        }
    }
}

impl TryFrom<AstarteData> for i64 {
    type Error = TypeError;
    fn try_from(value: AstarteData) -> Result<Self, Self::Error> {
        match value {
            AstarteData::LongInteger(val) => Ok(val),
            AstarteData::Integer(val) => Ok(val.into()),
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

/// A valid Astarte float.
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

// NOTE: do not implement conversions from f32 into Float, since it looses precision. So it's better
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

#[cfg(test)]
pub(crate) mod test {
    use chrono::{DateTime, TimeZone, Utc};
    use pretty_assertions::assert_eq;

    use super::*;

    pub(crate) fn all_astarte_types() -> Vec<AstarteData> {
        vec![
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
        ]
    }

    #[test]
    fn test_eq() {
        for case in all_astarte_types() {
            match case.clone() {
                AstarteData::Double(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    let f = f64::from(value);
                    assert_eq!(case, f);
                    assert_eq!(f, case);
                    assert_ne!(case, false);
                }
                AstarteData::Integer(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::Boolean(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, 42f64);
                }
                AstarteData::LongInteger(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::String(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::BinaryBlob(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::DateTime(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::DoubleArray(value) => {
                    let value: Vec<f64> = value.into_iter().map(f64::from).collect();

                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::IntegerArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::BooleanArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::LongIntegerArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::StringArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::BinaryBlobArray(value) => {
                    assert_eq!(case, value);
                    assert_eq!(value, case);
                    assert_ne!(case, false);
                }
                AstarteData::DateTimeArray(value) => {
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
                AstarteData::Double(value) => {
                    let inv: Double = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::Integer(value) => {
                    let inv: i32 = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::Boolean(value) => {
                    let inv: bool = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    f64::try_from(case).unwrap_err();
                }
                AstarteData::LongInteger(value) => {
                    let inv: i64 = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::String(value) => {
                    let inv: String = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::BinaryBlob(value) => {
                    let inv: Vec<u8> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::DateTime(value) => {
                    let inv: DateTime<Utc> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::DoubleArray(value) => {
                    let inv: Vec<f64> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::IntegerArray(value) => {
                    let inv: Vec<i32> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::BooleanArray(value) => {
                    let inv: Vec<bool> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::LongIntegerArray(value) => {
                    let inv: Vec<i64> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::StringArray(value) => {
                    let inv: Vec<String> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::BinaryBlobArray(value) => {
                    let inv: Vec<Vec<u8>> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
                AstarteData::DateTimeArray(value) => {
                    let inv: Vec<DateTime<Utc>> = case.clone().try_into().unwrap();
                    assert_eq!(inv, value);
                    let conv = AstarteData::from(value);
                    assert_eq!(conv, case);
                    bool::try_from(case).unwrap_err();
                }
            }
        }
    }

    #[test]
    fn test_eq_astarte_type_with_mapping_type() {
        assert!(AstarteData::Double(0.0.try_into().unwrap()).eq_mapping_type(MappingType::Double));
        assert!(AstarteData::Integer(0).eq_mapping_type(MappingType::Double));

        assert!(AstarteData::Integer(0).eq_mapping_type(MappingType::Integer));
        assert!(
            !AstarteData::Double(0.0.try_into().unwrap()).eq_mapping_type(MappingType::Integer)
        );
        assert!(AstarteData::Integer(0).eq_mapping_type(MappingType::LongInteger));

        assert!(AstarteData::LongInteger(0).eq_mapping_type(MappingType::LongInteger));
    }

    #[test]
    fn test_conversion_from_astarte_integer_to_f64() {
        let value = AstarteData::Integer(5);

        f64::try_from(value).unwrap_err();
    }

    #[test]
    fn test_conversion_from_astarte_integer_and_long_integer_to_i64() {
        let value: i64 = AstarteData::Integer(5).try_into().unwrap();

        assert_eq!(value, 5);

        let value: i64 = AstarteData::LongInteger(5).try_into().unwrap();

        assert_eq!(value, 5);

        i64::try_from(AstarteData::Boolean(false)).unwrap_err();
    }

    #[test]
    fn tesat_float_validation() {
        assert_eq!(
            AstarteData::try_from(54.4).unwrap(),
            AstarteData::Double(54.4.try_into().unwrap())
        );
        AstarteData::try_from(f64::NAN).unwrap_err();
        AstarteData::try_from(vec![1.0, 2.0, f64::NAN, 4.0]).unwrap_err();
    }
}
