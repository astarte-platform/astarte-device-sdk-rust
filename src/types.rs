use std::convert::TryInto;

use bson::{Binary, Bson};

use crate::AstarteError;

/// Types supported by astarte
///
/// <https://docs.astarte-platform.org/latest/080-mqtt-v1-protocol.html#astarte-data-types-to-bson-types>
#[derive(Debug, Clone, PartialEq)]
pub enum AstarteType {
    Double(f64),
    Int32(i32),
    Boolean(bool),
    Int64(i64),
    String(String),
    Blob(Vec<u8>),
    Datetime(chrono::DateTime<chrono::Utc>),

    DoubleArray(Vec<f64>),
    Int32Array(Vec<i32>),
    BooleanArray(Vec<bool>),
    Int64Array(Vec<i64>),
    StringArray(Vec<String>),
    BlobArray(Vec<Vec<u8>>),
    DatetimeArray(Vec<chrono::DateTime<chrono::Utc>>),
}

// we implement From<T> from all the base types to AstarteType, using this macro
macro_rules! impl_traits {
    ($typ:ty, $astartetype:tt) => {
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
    };
}

impl_traits!(f64, Double);
impl_traits!(f32, Double);
impl_traits!(i32, Int32);
impl_traits!(i64, Int64);
impl_traits!(&str, String);
impl_traits!(String, String);
impl_traits!(bool, Boolean);
impl_traits!(Vec<u8>, Blob);
impl_traits!(chrono::DateTime<chrono::Utc>, Datetime);
impl_traits!(Vec<f64>, DoubleArray);
impl_traits!(Vec<i32>, Int32Array);
impl_traits!(Vec<i64>, Int64Array);
impl_traits!(Vec<bool>, BooleanArray);
impl_traits!(Vec<String>, StringArray);
impl_traits!(Vec<Vec<u8>>, BlobArray);
impl_traits!(Vec<chrono::DateTime<chrono::Utc>>, DatetimeArray);

impl From<AstarteType> for Bson {
    fn from(d: AstarteType) -> Self {
        match d {
            AstarteType::Double(d) => Bson::Double(d),
            AstarteType::Int32(d) => Bson::Int32(d),
            AstarteType::Boolean(d) => Bson::Boolean(d),
            AstarteType::Int64(d) => Bson::Int64(d),
            AstarteType::String(d) => Bson::String(d),
            AstarteType::Blob(d) => Bson::Binary(Binary {
                bytes: d,
                subtype: bson::spec::BinarySubtype::Generic,
            }),
            AstarteType::Datetime(d) => Bson::DateTime(d),
            AstarteType::DoubleArray(d) => d.iter().collect(),
            AstarteType::Int32Array(d) => d.iter().collect(),
            AstarteType::BooleanArray(d) => d.iter().collect(),
            AstarteType::Int64Array(d) => d.iter().collect(),
            AstarteType::StringArray(d) => d.iter().collect(),
            AstarteType::BlobArray(d) => d
                .iter()
                .map(|d| Binary {
                    bytes: d.clone(),
                    subtype: bson::spec::BinarySubtype::Generic,
                })
                .collect(),
            AstarteType::DatetimeArray(d) => d.iter().collect(),
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
                Bson::Int32(_) => from_bson_array!(arr, Int32Array, Int32, i32),
                Bson::Int64(_) => from_bson_array!(arr, Int64Array, Int64, i64),
                Bson::DateTime(_) => {
                    from_bson_array!(arr, DatetimeArray, DateTime, chrono::DateTime<chrono::Utc>)
                }
                Bson::String(_) => from_bson_array!(arr, StringArray, String, String),
                Bson::Binary(_) => from_bson_array!(arr, BlobArray, Binary, Vec<u8>),
                _ => Err(AstarteError::FromBsonError),
            },
            Bson::Boolean(d) => Ok(AstarteType::Boolean(d)),
            Bson::Int32(d) => Ok(AstarteType::Int32(d)),
            Bson::Int64(d) => Ok(AstarteType::Int64(d)),
            Bson::Binary(d) => Ok(AstarteType::Blob(d.bytes)),
            Bson::DateTime(d) => Ok(AstarteType::Datetime(d)),
            _ => Err(AstarteError::FromBsonError),
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

    use crate::{types::AstarteType, Aggregation, AstarteSdk};

    #[test]
    fn test_individual_serialization() {
        let alltypes: Vec<AstarteType> = vec![
            (4.5).into(),
            (-4).into(),
            true.into(),
            45543543534_i64.into(),
            "hello".into(),
            b"hello".to_vec().into(),
            chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0).into(),
            vec![1.2, 3.4, 5.6, 7.8].into(),
            vec![1, 3, 5, 7].into(),
            vec![true, false, true, true].into(),
            vec![45543543534_i64, 45543543535_i64, 45543543536_i64].into(),
            vec!["hello".to_owned(), "world".to_owned()].into(),
            vec![b"hello".to_vec(), b"world".to_vec()].into(),
            vec![
                chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0),
                chrono::TimeZone::timestamp(&chrono::Utc, 1627580809, 0),
                chrono::TimeZone::timestamp(&chrono::Utc, 1627580810, 0),
            ]
            .into(),
        ];

        for ty in alltypes {
            println!("checking {:?}", ty);

            let buf = AstarteSdk::serialize_individual(ty.clone(), None).unwrap(); // allow_panic

            let ty2 = AstarteSdk::deserialize(buf).unwrap(); // allow_panic

            if let Aggregation::Individual(data) = ty2 {
                assert!(ty == data);
            } else {
                panic!(); // allow_panic
            }
        }
    }

    #[test]
    fn test_object_serialization() {
        let alltypes: Vec<AstarteType> = vec![
            (4.5).into(),
            (-4).into(),
            true.into(),
            45543543534_i64.into(),
            "hello".into(),
            b"hello".to_vec().into(),
            chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0).into(),
            vec![1.2, 3.4, 5.6, 7.8].into(),
            vec![1, 3, 5, 7].into(),
            vec![true, false, true, true].into(),
            vec![45543543534_i64, 45543543535_i64, 45543543536_i64].into(),
            vec!["hello".to_owned(), "world".to_owned()].into(),
            vec![b"hello".to_vec(), b"world".to_vec()].into(),
            vec![
                chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0),
                chrono::TimeZone::timestamp(&chrono::Utc, 1627580809, 0),
                chrono::TimeZone::timestamp(&chrono::Utc, 1627580810, 0),
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

        //let data: std::collections::HashMap<String,AstarteType> = allendpoints.iter().zip(alltypes.iter()).collect();

        let bytes = AstarteSdk::serialize_object(data.clone(), None).unwrap(); // allow_panic

        let data2 = AstarteSdk::deserialize(bytes).unwrap(); // allow_panic

        fn hashmap_match(
            map1: &HashMap<&str, AstarteType>,
            map2: &HashMap<String, AstarteType>,
        ) -> bool {
            map1.len() == map2.len()
                && map1
                    .keys()
                    .all(|k| map2.contains_key(k.clone()) && map1[k] == map2[k.clone()])
        }

        println!("\nComparing {:?}\nto {:?}", data, data2);

        if let Aggregation::Object(data2) = data2 {
            assert!(hashmap_match(&data, &data2));
        } else {
            panic!(); // allow_panic
        }
    }

    #[test]
    fn test_eq() {
        assert!(AstarteType::Int32(12) == 12);
        assert!(AstarteType::Double(12.0) == 12.0);
        assert!(AstarteType::String("hello".to_owned()) == "hello");
        assert!(AstarteType::Blob(vec![1, 2, 3, 4]) == vec![1_u8, 2, 3, 4]);
    }
}
