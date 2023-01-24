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
use std::convert::TryInto;

use bson::{Binary, Bson};
use chrono::{DateTime, Utc};

use crate::interface::MappingType;
use crate::AstarteError;

/// Types supported by astarte
///
/// <https://docs.astarte-platform.org/latest/080-mqtt-v1-protocol.html#astarte-data-types-to-bson-types>
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

        if other == &MappingType::LongInteger {
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

// we implement From<T> from all the base types to AstarteType, using this macro
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
    (Vec<f64>, DoubleArray),
    (Vec<i32>, IntegerArray),
    (Vec<i64>, LongIntegerArray),
    (Vec<bool>, BooleanArray),
    (Vec<String>, StringArray),
    (Vec<Vec<u8>>, BinaryBlobArray),
    (Vec<chrono::DateTime<chrono::Utc>>, DateTimeArray),
});

// we implement float types on the side since they have different requirements
impl std::convert::TryFrom<f64> for AstarteType {
    type Error = AstarteError;
    fn try_from(d: f64) -> Result<Self, Self::Error> {
        if d.is_nan() || d.is_infinite() || d.is_subnormal() {
            return Err(AstarteError::FloatError);
        }
        Ok(AstarteType::Double(d))
    }
}

impl std::convert::TryFrom<f32> for AstarteType {
    type Error = AstarteError;

    fn try_from(d: f32) -> Result<Self, Self::Error> {
        if d.is_nan() || d.is_infinite() || d.is_subnormal() {
            return Err(AstarteError::FloatError);
        }
        Ok(AstarteType::Double(d.into()))
    }
}

// we implement TryFrom<AstarteType> to all the base types, using this macro
macro_rules! impl_reverse_type_conversion_traits {
    ($(($astartetype:tt, $typ:ty),)*) => {
        $(
            impl std::convert::TryFrom<AstarteType> for $typ {
                type Error = AstarteError;

                fn try_from(var: AstarteType) -> Result<Self, Self::Error> {
                    if let AstarteType::$astartetype(val) = var {
                        Ok(val)
                    } else {
                        Err(AstarteError::Conversion)
                    }
                }
            }
        )*
    }
}

impl_reverse_type_conversion_traits!(
    (Double, f64),
    (Integer, i32),
    (Boolean, bool),
    (LongInteger, i64),
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
                Err(AstarteError::FromBsonArrayError)
            }
        });

        let ret: Result<Vec<$typ>, AstarteError> = ret.collect();
        Ok(AstarteType::$astartetype(ret?))
    }};

    // We have to specialize for DateTimeArray too because bson has its own datetime type
    ($arr:ident, $astartetype:tt,DateTime,$typ:ty) => {{
        let ret = $arr.iter().map(|x| {
            if let Bson::DateTime(val) = x {
                Ok(val.clone())
            } else {
                Err(AstarteError::FromBsonArrayError)
            }
        });

        let ret: Result<Vec<bson::DateTime>, AstarteError> = ret.collect();
        let ret: Vec<$typ> = ret?.iter().map(|f| f.to_chrono()).collect();

        Ok(AstarteType::$astartetype(ret))
    }};

    ($arr:ident, $astartetype:tt,$bsontype:tt,$typ:ty) => {{
        let ret = $arr.iter().map(|x| {
            if let Bson::$bsontype(val) = x {
                Ok(val.clone())
            } else {
                Err(AstarteError::FromBsonArrayError)
            }
        });

        let ret: Result<Vec<$typ>, AstarteError> = ret.collect();
        Ok(AstarteType::$astartetype(ret?))
    }};
}

impl std::convert::TryFrom<Bson> for AstarteType {
    type Error = AstarteError;

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
                _ => Err(AstarteError::FromBsonError(format!(
                    "Can't convert array {:?} to astarte",
                    arr
                ))),
            },
            Bson::Boolean(d) => Ok(AstarteType::Boolean(d)),
            Bson::Int32(d) => Ok(AstarteType::Integer(d)),
            Bson::Int64(d) => Ok(AstarteType::LongInteger(d)),
            Bson::Binary(d) => Ok(AstarteType::BinaryBlob(d.bytes)),
            Bson::DateTime(d) => Ok(AstarteType::DateTime(d.into())),
            _ => Err(AstarteError::FromBsonError(format!(
                "Can't convert {:?} to astarte",
                d
            ))),
        }
    }
}

impl AstarteType {
    pub fn from_bson_vec(d: Vec<Bson>) -> Result<Vec<Self>, AstarteError> {
        let vec = d.iter().map(|f| f.clone().try_into());
        vec.collect()
    }
}

#[cfg(test)]

mod test {
    use std::collections::HashMap;
    use std::convert::TryFrom;

    use chrono::{DateTime, TimeZone, Utc};

    use crate::{types::AstarteType, Aggregation, AstarteDeviceSdk, AstarteError};

    #[test]
    fn test_individual_serialization() {
        let alltypes: Vec<AstarteType> = vec![
            AstarteType::Double(4.5),
            (-4).into(),
            true.into(),
            45543543534_i64.into(),
            "hello".into(),
            b"hello".to_vec().into(),
            TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap().into(),
            vec![1.2, 3.4, 5.6, 7.8].into(),
            vec![1, 3, 5, 7].into(),
            vec![true, false, true, true].into(),
            vec![45543543534_i64, 45543543535_i64, 45543543536_i64].into(),
            vec!["hello".to_owned(), "world".to_owned()].into(),
            vec![b"hello".to_vec(), b"world".to_vec()].into(),
            vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]
            .into(),
            AstarteType::Unset,
        ];

        for ty in alltypes {
            println!("checking {:?}", ty);

            let buf = AstarteDeviceSdk::serialize_individual(ty.clone(), None).unwrap();

            let ty2 = AstarteDeviceSdk::deserialize(&buf).unwrap();

            if let Aggregation::Individual(data) = ty2 {
                assert!(ty == data);
            } else {
                panic!();
            }
        }
    }

    #[test]
    fn test_object_serialization() {
        let alltypes: Vec<AstarteType> = vec![
            AstarteType::Double(4.5),
            (-4).into(),
            true.into(),
            45543543534_i64.into(),
            "hello".into(),
            b"hello".to_vec().into(),
            TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap().into(),
            vec![1.2, 3.4, 5.6, 7.8].into(),
            vec![1, 3, 5, 7].into(),
            vec![true, false, true, true].into(),
            vec![45543543534_i64, 45543543535_i64, 45543543536_i64].into(),
            vec!["hello".to_owned(), "world".to_owned()].into(),
            vec![b"hello".to_vec(), b"world".to_vec()].into(),
            vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]
            .into(),
        ];

        let allendpoints = vec![
            "double",
            "integer",
            "boolean",
            "longinteger",
            "string",
            "binaryblob",
            "datetime",
            "doublearray",
            "integerarray",
            "booleanarray",
            "longintegerarray",
            "stringarray",
            "binaryblobarray",
            "datetimearray",
        ];

        let mut data = std::collections::HashMap::new();

        for i in allendpoints.iter().zip(alltypes.iter()) {
            data.insert(*i.0, i.1.clone());
        }

        let bytes =
            AstarteDeviceSdk::serialize_object(AstarteDeviceSdk::to_bson_map(data.clone()), None)
                .unwrap();

        let data2 = AstarteDeviceSdk::deserialize(&bytes).unwrap();

        fn hashmap_match(
            map1: &HashMap<&str, AstarteType>,
            map2: &HashMap<String, AstarteType>,
        ) -> bool {
            map1.len() == map2.len()
                && map1.keys().all(|k| {
                    map2.contains_key(<&str>::clone(k)) && map1[k] == map2[<&str>::clone(k)]
                })
        }

        println!("\nComparing {:?}\nto {:?}", data, data2);

        if let Aggregation::Object(data2) = data2 {
            assert!(hashmap_match(&data, &data2));
        } else {
            panic!();
        }
    }

    #[test]
    fn test_eq() {
        assert!(AstarteType::Integer(12) == 12);
        assert!(AstarteType::String("hello".to_owned()) == "hello");
        assert!(AstarteType::BinaryBlob(vec![1, 2, 3, 4]) == vec![1_u8, 2, 3, 4]);
    }

    #[test]
    fn test_conversion_from_astarte_type() -> Result<(), AstarteError> {
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
}
