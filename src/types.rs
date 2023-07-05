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

use std::convert::TryFrom;

use bson::{Binary, Bson};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "Bson", into = "Bson")]
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

    /// A generic empty array. This is not part of the Astarte MQTT v1 protocol. However, it's
    /// required since we cannot know the type of an empty array. It should only be used when
    /// converting from a Bson array.
    EmptyArray,

    Unset,
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

// we implement float types on the side since they have different requirements
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
            AstarteType::EmptyArray => Bson::Array(Vec::new()),
        }
    }
}

impl TryFrom<Bson> for AstarteType {
    type Error = TypeError;

    fn try_from(d: Bson) -> Result<Self, Self::Error> {
        match d {
            Bson::Double(d) => AstarteType::try_from(d),
            Bson::String(d) => Ok(AstarteType::from(d)),
            Bson::Boolean(d) => Ok(AstarteType::from(d)),
            Bson::Int32(d) => Ok(AstarteType::from(d)),
            Bson::Int64(d) => Ok(AstarteType::from(d)),
            Bson::Binary(d) => Ok(AstarteType::from(d.bytes)),
            Bson::DateTime(d) => Ok(AstarteType::from(d.to_chrono())),
            Bson::Null => Ok(AstarteType::Unset),
            Bson::Array(arr) => AstarteType::from_bson_array(arr),
            _ => Err(Self::Error::FromBsonError(format!(
                "Can't convert {d:?} to astarte"
            ))),
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
    pub fn from_bson_vec(d: Vec<Bson>) -> Result<Vec<Self>, TypeError> {
        d.into_iter().map(AstarteType::try_from).collect()
    }

    /// Convert a non empty bson array to astarte array
    pub(crate) fn from_bson_array(array: Vec<Bson>) -> Result<Self, TypeError> {
        let Some(first) = array.first() else {
            return Ok(AstarteType::EmptyArray);
        };

        match first {
            Bson::Double(_) => bson_array(array, |b| b.as_f64()).and_then(AstarteType::try_from),
            Bson::String(_) => {
                // Take the same string and don't use as_str
                bson_array(array, |b| match b {
                    Bson::String(s) => Some(s),
                    _ => None,
                })
                .map(AstarteType::from)
            }
            Bson::Boolean(_) => bson_array(array, |b| b.as_bool()).map(AstarteType::from),
            Bson::Int32(_) => bson_array(array, |b| b.as_i32()).map(AstarteType::from),
            Bson::Int64(_) => bson_array(array, |b| b.as_i64()).map(AstarteType::from),
            Bson::Binary(_) => {
                // Take the same buf allocation
                bson_array(array, |b| match b {
                    Bson::Binary(b) => Some(b.bytes),
                    _ => None,
                })
                .map(AstarteType::from)
            }
            Bson::DateTime(_) => {
                // Manually convert to chrono
                bson_array(array, |b| match b {
                    Bson::DateTime(d) => Some(d.to_chrono()),
                    _ => None,
                })
                .map(AstarteType::from)
            }
            _ => Err(TypeError::FromBsonArrayError),
        }
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryFrom;
    use std::convert::TryInto;

    use chrono::{DateTime, TimeZone, Utc};

    use crate::interface::MappingType;
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

        let bson: Bson = astarte_type_double.try_into().expect("Failed to convert");

        assert_eq!(bson, Bson::Array(vec![]));

        let astarte_type_double: AstarteType = bson.try_into().expect("Failed to convert");

        assert_eq!(astarte_type_double, AstarteType::EmptyArray);
    }
}
