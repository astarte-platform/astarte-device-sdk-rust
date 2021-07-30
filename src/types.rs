use bson::{Binary, Bson};

#[derive(Debug, Clone)]
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
    BlobArray(Vec<Vec<u8>>)
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


impl From<AstarteType> for Bson {
    fn from(d: AstarteType) -> Self {
        match d {
            AstarteType::Double(d) => Bson::Double(d),
            AstarteType::Int32(d) => Bson::Int32(d),
            AstarteType::Boolean(d) => Bson::Boolean(d),
            AstarteType::Int64(d) => Bson::Int64(d),
            AstarteType::String(d) => Bson::String(d),
            AstarteType::Blob(d) => Bson::Binary(Binary { bytes: d, subtype: bson::spec::BinarySubtype::Generic}),
            AstarteType::Datetime(d) => Bson::DateTime(d),
            AstarteType::DoubleArray(d) => d.iter().collect(),
            AstarteType::Int32Array(d) => d.iter().collect(),
            AstarteType::BooleanArray(d) => d.iter().collect(),
            AstarteType::Int64Array(d) => d.iter().collect(),
            AstarteType::StringArray(d) => d.iter().collect(),
            AstarteType::BlobArray(d) => d.iter().map(|d| Binary { bytes: d.clone(), subtype: bson::spec::BinarySubtype::Generic}).collect(),
        }
    }
}

impl  AstarteType {
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
}