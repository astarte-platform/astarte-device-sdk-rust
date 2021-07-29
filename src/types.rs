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
