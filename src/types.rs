use bson::{Binary, Bson};

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

impl From<f64> for AstarteType {
    fn from(d: f64) -> Self {
        AstarteType::Double(d)
    }
}

impl From<f32> for AstarteType {
    fn from(d: f32) -> Self {
        AstarteType::Double(d.into())
    }
}

impl From<i32> for AstarteType {
    fn from(d: i32) -> Self {
        AstarteType::Int32(d)
    }
}

impl From<i64> for AstarteType {
    fn from(d: i64) -> Self {
        AstarteType::Int64(d)
    }
}

impl From<&str> for AstarteType {
    fn from(d: &str) -> Self {
        AstarteType::String(d.to_owned())
    }
}

impl From<String> for AstarteType {
    fn from(d: String) -> Self {
        AstarteType::String(d)
    }
}

impl From<bool> for AstarteType {
    fn from(d: bool) -> Self {
        AstarteType::Boolean(d)
    }
}

impl From<Vec<u8>> for AstarteType {
    fn from(d: Vec<u8>) -> Self {
        AstarteType::Blob(d)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for AstarteType {
    fn from(d: chrono::DateTime<chrono::Utc>) -> Self {
        AstarteType::Datetime(d)
    }
}

impl From<Vec<f64>> for AstarteType {
    fn from(d: Vec<f64>) -> Self {
        AstarteType::DoubleArray(d)
    }
}

impl From<Vec<i32>> for AstarteType {
    fn from(d: Vec<i32>) -> Self {
        AstarteType::Int32Array(d)
    }
}

impl From<Vec<i64>> for AstarteType {
    fn from(d: Vec<i64>) -> Self {
        AstarteType::Int64Array(d)
    }
}

impl From<Vec<bool>> for AstarteType {
    fn from(d: Vec<bool>) -> Self {
        AstarteType::BooleanArray(d)
    }
}

impl From<Vec<String>> for AstarteType {
    fn from(d: Vec<String>) -> Self {
        AstarteType::StringArray(d)
    }
}

impl From<Vec<Vec<u8>>> for AstarteType {
    fn from(d: Vec<Vec<u8>>) -> Self {
        AstarteType::BlobArray(d)
    }
}

impl From<Vec<chrono::DateTime<chrono::Utc>>> for AstarteType {
    fn from(d: Vec<chrono::DateTime<chrono::Utc>>) -> Self {
        AstarteType::DatetimeArray(d)
    }
}

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

impl AstarteType {
    pub fn from_bson(d: Bson) -> Option<Self> {
        match d {
            Bson::Double(d) => Some(AstarteType::Double(d)),
            Bson::String(d) => Some(AstarteType::String(d)),
            Bson::Array(arr) => match arr[0] {
                //TODO this is probably better served by a macro
                Bson::Double(_) => Some(AstarteType::DoubleArray(
                    arr.iter()
                        .map(|x| {
                            if let Bson::Double(val) = x {
                                *val
                            } else {
                                panic!("malformed input")
                            }
                        })
                        .collect::<Vec<f64>>(),
                )),
                Bson::Boolean(_) => Some(AstarteType::BooleanArray(
                    arr.iter()
                        .map(|x| {
                            if let Bson::Boolean(val) = x {
                                *val
                            } else {
                                panic!("malformed input")
                            }
                        })
                        .collect::<Vec<bool>>(),
                )),
                Bson::Int32(_) => Some(AstarteType::Int32Array(
                    arr.iter()
                        .map(|x| {
                            if let Bson::Int32(val) = x {
                                *val
                            } else {
                                panic!("malformed input")
                            }
                        })
                        .collect::<Vec<i32>>(),
                )),
                Bson::Int64(_) => Some(AstarteType::Int64Array(
                    arr.iter()
                        .map(|x| {
                            if let Bson::Int64(val) = x {
                                *val
                            } else {
                                panic!("malformed input")
                            }
                        })
                        .collect::<Vec<i64>>(),
                )),
                Bson::String(_) => Some(AstarteType::StringArray(
                    arr.iter()
                        .map(|x| {
                            if let Bson::String(val) = x {
                                val.clone()
                            } else {
                                panic!("malformed input")
                            }
                        })
                        .collect::<Vec<String>>(),
                )),
                Bson::Binary(_) => Some(AstarteType::BlobArray(
                    arr.iter()
                        .map(|x| {
                            if let Bson::Binary(val) = x {
                                val.bytes.clone()
                            } else {
                                panic!("malformed input")
                            }
                        })
                        .collect::<Vec<Vec<u8>>>(),
                )),
                Bson::DateTime(_) => Some(AstarteType::DatetimeArray(
                    arr.iter()
                        .map(|x| {
                            if let Bson::DateTime(val) = x {
                                *val
                            } else {
                                panic!("malformed input")
                            }
                        })
                        .collect::<Vec<chrono::DateTime<chrono::Utc>>>(),
                )),
                _ => None,
            },
            Bson::Boolean(d) => Some(AstarteType::Boolean(d)),
            Bson::Int32(d) => Some(AstarteType::Int32(d)),
            Bson::Int64(d) => Some(AstarteType::Int64(d)),
            Bson::Binary(d) => Some(AstarteType::Blob(d.bytes)),
            Bson::DateTime(d) => Some(AstarteType::Datetime(d)),
            _ => None,
        }
    }

    pub fn from_bson_vec(d: Vec<Bson>) -> Option<Vec<Self>> {
        let vec = d.iter().map(|f| AstarteType::from_bson(f.clone()));
        let vec: Option<Vec<AstarteType>> = vec.collect();

        vec
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
}
