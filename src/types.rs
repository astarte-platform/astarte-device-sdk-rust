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
            Bson::Array(_) => None,
            Bson::Document(_) => None,
            Bson::Boolean(d) => Some(AstarteType::Boolean(d)),
            Bson::Null => None,
            Bson::RegularExpression(_) => None,
            Bson::JavaScriptCode(_) => None,
            Bson::JavaScriptCodeWithScope(_) => None,
            Bson::Int32(d) => Some(AstarteType::Int32(d)),
            Bson::Int64(d) => Some(AstarteType::Int64(d)),
            Bson::Timestamp(_) => None,
            Bson::Binary(d) => Some(AstarteType::Blob(d.bytes)),
            Bson::ObjectId(_) => None,
            Bson::DateTime(d) => Some(AstarteType::Datetime(d)),
            Bson::Symbol(_) => None,
            Bson::Decimal128(_) => None,
            Bson::Undefined => None,
            Bson::MaxKey => None,
            Bson::MinKey => None,
            Bson::DbPointer(_) => None,
        }
    }


    pub fn from_bson_vec(d: Vec<Bson>) -> Option<Vec<Self>>{
        let vec = d.iter().map(|f| AstarteType::from_bson(f.clone()));
        let vec: Option<Vec<AstarteType>> = vec.collect();

        vec
    }
}


#[cfg(test)]

mod test {
    use crate::{AstarteSdk, types::AstarteType};

    #[test]
    fn test_types() {
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

            let buf = AstarteSdk::serialize_individual(ty.clone(), None).unwrap();

            let ty2 = AstarteSdk::deserialize_individual(buf).unwrap();

            assert!(ty == ty2);
        }
    }
}
