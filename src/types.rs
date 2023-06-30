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

use std::convert::TryInto;

use bson::{Binary, Bson};
use chrono::{DateTime, Utc};

use crate::interface::MappingType;
use crate::Error;

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
#[derive(Debug, Clone, PartialEq)]
pub enum AstarteType {
    Double(f64),
    Integer(i32),
    Boolean(bool),
    LongInteger(i64),
    String(String),
    BinaryBlob(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),

    DoubleArray(Vec<f64>),
    IntegerArray(Vec<i32>),
    BooleanArray(Vec<bool>),
    LongIntegerArray(Vec<i64>),
    StringArray(Vec<String>),
    BinaryBlobArray(Vec<Vec<u8>>),
    DateTimeArray(Vec<chrono::DateTime<chrono::Utc>>),

    Unset,
}

impl PartialEq<MappingType> for AstarteType {
    fn eq(&self, other: &MappingType) -> bool {
        macro_rules! check_astype_match {
            ( $self:ident, $other:ident, {$( $astartetype:tt ,)*}) => {
                match $other {
                    $(
                        crate::interface::MappingType::$astartetype => if let AstarteType::$astartetype(_) = $self {
                                true
                            } else {
                                false
                            }
                    )*
                }
            };
        }

        if other == &MappingType::LongInteger || other == &MappingType::Double {
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
impl std::convert::TryFrom<f32> for AstarteType {
    type Error = Error;

    fn try_from(d: f32) -> Result<Self, Self::Error> {
        if d.is_nan() || d.is_infinite() || d.is_subnormal() {
            return Err(Error::FloatError);
        }
        Ok(AstarteType::Double(d.into()))
    }
}

impl std::convert::TryFrom<f64> for AstarteType {
    type Error = Error;
    fn try_from(d: f64) -> Result<Self, Self::Error> {
        if d.is_nan() || d.is_infinite() || d.is_subnormal() {
            return Err(Error::FloatError);
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

impl std::convert::TryFrom<Vec<f64>> for AstarteType {
    type Error = Error;
    fn try_from(d: Vec<f64>) -> Result<Self, Self::Error> {
        if d.iter()
            .any(|&x| x.is_nan() || x.is_infinite() || x.is_subnormal())
        {
            return Err(Error::FloatError);
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
                type Error = $crate::error::Error;

                fn try_from(var: AstarteType) -> Result<Self, Self::Error> {
                    if let AstarteType::$astartetype(val) = var {
                        Ok(val)
                    } else {
                        Err($crate::error::Error::Conversion)
                    }
                }
            }
        )*
    }
}

impl std::convert::TryFrom<AstarteType> for f64 {
    type Error = Error;
    fn try_from(var: AstarteType) -> Result<Self, Self::Error> {
        if let AstarteType::Double(val) = var {
            Ok(val)
        } else if let AstarteType::Integer(val) = var {
            Ok(val.into())
        } else {
            Err(Error::Conversion)
        }
    }
}

impl std::convert::TryFrom<AstarteType> for i64 {
    type Error = Error;
    fn try_from(var: AstarteType) -> Result<Self, Self::Error> {
        if let AstarteType::LongInteger(val) = var {
            Ok(val)
        } else if let AstarteType::Integer(val) = var {
            Ok(val.into())
        } else {
            Err(Error::Conversion)
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
            AstarteType::DoubleArray(d) => d.iter().collect(),
            AstarteType::IntegerArray(d) => d.iter().collect(),
            AstarteType::BooleanArray(d) => d.iter().collect(),
            AstarteType::LongIntegerArray(d) => d.iter().collect(),
            AstarteType::StringArray(d) => d.iter().collect(),
            AstarteType::BinaryBlobArray(d) => d
                .iter()
                .map(|d| Binary {
                    bytes: d.clone(),
                    subtype: bson::spec::BinarySubtype::Generic,
                })
                .collect(),
            AstarteType::DateTimeArray(d) => d.iter().collect(),
            AstarteType::Unset => Bson::Null,
        }
    }
}

macro_rules! from_bson_array {
    // Bson::Binary is built different from the other types
    // we have to make a special case for it
    ($arr:ident, $astartetype:tt,Binary,$typ:ty) => {{
        let ret = $arr.iter().map(|x| {
            if let Bson::Binary(val) = x {
                Ok(val.bytes.clone())
            } else {
                Err($crate::error::Error::FromBsonArrayError)
            }
        });

        let ret: Result<Vec<$typ>, $crate::error::Error> = ret.collect();
        Ok(AstarteType::$astartetype(ret?))
    }};

    // We have to specialize for DateTimeArray too because bson has its own datetime type
    ($arr:ident, $astartetype:tt,DateTime,$typ:ty) => {{
        let ret = $arr.iter().map(|x| {
            if let Bson::DateTime(val) = x {
                Ok(val.clone())
            } else {
                Err($crate::error::Error::FromBsonArrayError)
            }
        });

        let ret: Result<Vec<bson::DateTime>, $crate::error::Error> = ret.collect();
        let ret: Vec<$typ> = ret?.iter().map(|f| f.to_chrono()).collect();

        Ok(AstarteType::$astartetype(ret))
    }};

    ($arr:ident, $astartetype:tt,$bsontype:tt,$typ:ty) => {{
        let ret = $arr.iter().map(|x| {
            if let Bson::$bsontype(val) = x {
                Ok(val.clone())
            } else {
                Err($crate::error::Error::FromBsonArrayError)
            }
        });

        let ret: Result<Vec<$typ>, $crate::error::Error> = ret.collect();
        Ok(AstarteType::$astartetype(ret?))
    }};
}

impl std::convert::TryFrom<Bson> for AstarteType {
    type Error = Error;

    fn try_from(d: Bson) -> Result<Self, Self::Error> {
        match d {
            Bson::Double(d) => Ok(AstarteType::Double(d)),
            Bson::String(d) => Ok(AstarteType::String(d)),
            Bson::Array(arr) => match arr[0] {
                Bson::Double(_) => from_bson_array!(arr, DoubleArray, Double, f64),
                Bson::Boolean(_) => from_bson_array!(arr, BooleanArray, Boolean, bool),
                Bson::Int32(_) => from_bson_array!(arr, IntegerArray, Int32, i32),
                Bson::Int64(_) => from_bson_array!(arr, LongIntegerArray, Int64, i64),
                Bson::DateTime(_) => {
                    from_bson_array!(arr, DateTimeArray, DateTime, chrono::DateTime<chrono::Utc>)
                }
                Bson::String(_) => from_bson_array!(arr, StringArray, String, String),
                Bson::Binary(_) => from_bson_array!(arr, BinaryBlobArray, Binary, Vec<u8>),
                _ => Err(Error::FromBsonError(format!(
                    "Can't convert array {arr:?} to astarte"
                ))),
            },
            Bson::Boolean(d) => Ok(AstarteType::Boolean(d)),
            Bson::Int32(d) => Ok(AstarteType::Integer(d)),
            Bson::Int64(d) => Ok(AstarteType::LongInteger(d)),
            Bson::Binary(d) => Ok(AstarteType::BinaryBlob(d.bytes)),
            Bson::DateTime(d) => Ok(AstarteType::DateTime(d.into())),
            _ => Err(Error::FromBsonError(format!(
                "Can't convert {d:?} to astarte"
            ))),
        }
    }
}

impl AstarteType {
    pub fn from_bson_vec(d: Vec<Bson>) -> Result<Vec<Self>, Error> {
        let vec = d.iter().map(|f| f.clone().try_into());
        vec.collect()
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryFrom;
    use std::convert::TryInto;

    use chrono::{DateTime, TimeZone, Utc};

    use crate::interface::MappingType;
    use crate::{types::AstarteType, Aggregation, Error};

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
    fn test_conversion_to_astarte_type() -> Result<(), Error> {
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
    fn test_conversion_from_astarte_type() -> Result<(), Error> {
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
}
