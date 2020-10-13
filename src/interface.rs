use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Interface {
    interface_name: String,
    version_major: i32,
    version_minor: i32,
    #[serde(rename = "type")]
    interface_type: InterfaceType,
    ownership: Ownership,
    #[serde(default, skip_serializing_if = "is_default")]
    aggregation: Aggregation,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
    mappings: Vec<Mapping>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
enum InterfaceType {
    Datastream,
    Properties,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Ownership {
    Device,
    Server,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Aggregation {
    Individual,
    Object,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Mapping {
    // Common fields
    endpoint: String,
    #[serde(rename = "type")]
    mapping_type: MappingType,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
    // Datastream only
    #[serde(default, skip_serializing_if = "is_default")]
    reliability: Reliability,
    #[serde(default, skip_serializing_if = "is_default")]
    retention: Retention,
    #[serde(default, skip_serializing_if = "is_default")]
    explicit_timestamp: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiry: Option<u32>,
    #[serde(default, skip_serializing_if = "is_default")]
    database_retention_policy: DatabaseRetentionPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    database_retention_ttl: Option<u32>,
    // Properties only
    #[serde(default, skip_serializing_if = "is_default")]
    allow_unset: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
enum MappingType {
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
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Reliability {
    Unreliable,
    Guaranteed,
    Unique,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Retention {
    Discard,
    Volatile,
    Stored,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
enum DatabaseRetentionPolicy {
    UseTtl,
    NoTtl,
}

impl Default for Aggregation {
    fn default() -> Self {
        Aggregation::Individual
    }
}

impl Default for Reliability {
    fn default() -> Self {
        Reliability::Unreliable
    }
}

impl Default for Retention {
    fn default() -> Self {
        Retention::Discard
    }
}

impl Default for DatabaseRetentionPolicy {
    fn default() -> Self {
        DatabaseRetentionPolicy::NoTtl
    }
}

fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    t == &T::default()
}

#[cfg(test)]
mod tests {
    use super::{
        Aggregation, DatabaseRetentionPolicy, Interface, InterfaceType, Mapping, MappingType,
        Ownership, Reliability, Retention,
    };

    #[test]
    fn datastream_interface_deserialization() {
        let interface_json = "
        {
            \"interface_name\": \"org.astarte-platform.genericsensors.Values\",
            \"version_major\": 1,
            \"version_minor\": 0,
            \"type\": \"datastream\",
            \"ownership\": \"device\",
            \"description\": \"Generic sensors sampled data.\",
            \"doc\": \"Values allows generic sensors to stream samples. It is usually used in combination with AvailableSensors, which makes API client aware of what sensors and what unit of measure they are reporting. sensor_id represents an unique identifier for an individual sensor, and should match sensor_id in AvailableSensors when used in combination.\",
            \"mappings\": [
                {
                    \"endpoint\": \"/%{sensor_id}/value\",
                    \"type\": \"double\",
                    \"explicit_timestamp\": true,
                    \"description\": \"Sampled real value.\",
                    \"doc\": \"Datastream of sampled real values.\"
                }
            ]
        }";

        let endpoint = "/%{sensor_id}/value".to_owned();
        let mapping_type = MappingType::Double;
        let explicit_timestamp = true;
        let description = Some("Sampled real value.".to_owned());
        let doc = Some("Datastream of sampled real values.".to_owned());

        let mapping = Mapping {
            endpoint,
            mapping_type,
            explicit_timestamp,
            description,
            doc,
            database_retention_policy: DatabaseRetentionPolicy::NoTtl,
            database_retention_ttl: None,
            expiry: None,
            retention: Retention::Discard,
            reliability: Reliability::Unreliable,
            allow_unset: false,
        };

        let interface_name = "org.astarte-platform.genericsensors.Values".to_owned();
        let version_major = 1;
        let version_minor = 0;
        let interface_type = InterfaceType::Datastream;
        let ownership = Ownership::Device;
        let description = Some("Generic sensors sampled data.".to_owned());
        let doc = Some("Values allows generic sensors to stream samples. It is usually used in combination with AvailableSensors, which makes API client aware of what sensors and what unit of measure they are reporting. sensor_id represents an unique identifier for an individual sensor, and should match sensor_id in AvailableSensors when used in combination.".to_owned());

        let interface = Interface {
            interface_name,
            version_major,
            version_minor,
            interface_type,
            ownership,
            description,
            doc,
            aggregation: Aggregation::Individual,
            mappings: vec![mapping],
        };

        let deser_interface = serde_json::from_str(interface_json).unwrap();

        assert_eq!(interface, deser_interface)
    }
}
