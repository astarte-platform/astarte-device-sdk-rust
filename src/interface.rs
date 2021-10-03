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
 */

pub(crate) mod traits;

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use traits::Interface as InterfaceTrait;
use traits::Mapping as MappingTrait;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("cannot parse interface JSON")]
    ParseError(#[from] serde_json::Error),
    #[error("cannot read interface file")]
    IoError(#[from] io::Error),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Interface {
    Datastream(DatastreamInterface),
    Properties(PropertiesInterface),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) struct BaseInterface {
    interface_name: String,
    version_major: i32,
    version_minor: i32,
    ownership: Ownership,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DatastreamInterface {
    #[serde(flatten)]
    base: BaseInterface,
    #[serde(default, skip_serializing_if = "is_default")]
    aggregation: Aggregation,
    mappings: Vec<DatastreamMapping>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct PropertiesInterface {
    #[serde(flatten)]
    base: BaseInterface,
    mappings: Vec<PropertiesMapping>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceType {
    Datastream,
    Properties,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Ownership {
    Device,
    Server,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Individual,
    Object,
}

#[derive(Debug)]
pub enum Mapping<'a> {
    Datastream(&'a DatastreamMapping),
    Properties(&'a PropertiesMapping),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct BaseMapping {
    endpoint: String,
    #[serde(rename = "type")]
    mapping_type: MappingType,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DatastreamMapping {
    #[serde(flatten)]
    base: BaseMapping,
    #[serde(default, skip_serializing_if = "is_default")]
    pub reliability: Reliability,
    #[serde(default, skip_serializing_if = "is_default")]
    pub retention: Retention,
    #[serde(default, skip_serializing_if = "is_default")]
    pub explicit_timestamp: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry: Option<u32>,
    // TODO: merge database_retention_policy and database_retention_ttl in a
    // single type (adjacently tagged enum works ok except when there's no
    // database_retention_policy key in JSON)
    #[serde(default, skip_serializing_if = "is_default")]
    pub database_retention_policy: DatabaseRetentionPolicy,
    #[serde(default)]
    pub database_retention_ttl: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct PropertiesMapping {
    #[serde(flatten)]
    base: BaseMapping,
    #[serde(default, skip_serializing_if = "is_default")]
    pub allow_unset: bool,
}

// TODO: investigate pro/cons of tagged enum like
// Scalar(InnerType)/Array(InnerType)
#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum MappingType {
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Reliability {
    Unreliable,
    Guaranteed,
    Unique,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    Discard,
    Volatile,
    Stored,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseRetentionPolicy {
    NoTtl,
    UseTtl,
}

impl Default for Aggregation {
    fn default() -> Self {
        Self::Individual
    }
}

impl Default for Reliability {
    fn default() -> Self {
        Self::Unreliable
    }
}

impl Default for Retention {
    fn default() -> Self {
        Self::Discard
    }
}

impl Default for DatabaseRetentionPolicy {
    fn default() -> Self {
        Self::NoTtl
    }
}

fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    t == &T::default()
}

impl Interface {
    pub fn from_file(path: &Path) -> Result<Self, Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let interface = serde_json::from_reader(reader)?;
        Ok(interface)
    }

    pub fn aggregation(&self) -> Aggregation {
        match &self {
            Self::Datastream(d) => d.aggregation,
            // Properties are always individual
            Self::Properties(_) => Aggregation::Individual,
        }
    }

    pub fn mapping(&self, path: &str) -> Option<Mapping> {
        match &self {
            Self::Datastream(d) => {
                for mapping in d.mappings.iter() {
                    if mapping.is_compatible(path) {
                        return Some(Mapping::Datastream(mapping));
                    }
                }
            }
            // Properties are always individual
            Self::Properties(p) => {
                for mapping in p.mappings.iter() {
                    if mapping.is_compatible(path) {
                        return Some(Mapping::Properties(mapping));
                    }
                }
            }
        }

        None
    }

    pub fn mappings_len(&self) -> usize {
        match &self {
            Self::Datastream(d) => d.mappings.len(),
            Self::Properties(p) => p.mappings.len(),
        }
    }

    pub fn get_ownership(&self) -> Ownership {
        match &self {
            Interface::Datastream(iface) => iface.base.ownership,
            Interface::Properties(iface) => iface.base.ownership,
        }
    }

    pub fn get_properties_paths(&self) -> Vec<(String, i32)> {
        if let Interface::Properties(iface) = self {
            let name = iface.base.interface_name.clone();

            let mappings = iface
                .mappings
                .iter()
                .map(|f| (name.clone() + &f.base.endpoint, iface.base.version_major))
                .collect();

            return mappings;
        }

        Vec::new()
    }
}

impl std::str::FromStr for Interface {
    fn from_str(s: &str) -> Result<Self, Error> {
        let interface = serde_json::from_str(s)?;
        Ok(interface)
    }

    type Err = Error;
}

impl InterfaceTrait for Interface {
    fn base_interface(&self) -> &BaseInterface {
        match &self {
            Self::Datastream(d) => &d.base,
            Self::Properties(p) => &p.base,
        }
    }
}

impl MappingTrait for Mapping<'_> {
    fn base_mapping(&self) -> &BaseMapping {
        match &self {
            Self::Datastream(d) => &d.base,
            Self::Properties(d) => &d.base,
        }
    }
}

impl MappingTrait for DatastreamMapping {
    fn base_mapping(&self) -> &BaseMapping {
        &self.base
    }
}

impl MappingTrait for PropertiesMapping {
    fn base_mapping(&self) -> &BaseMapping {
        &self.base
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::traits::Interface as InterfaceTrait;
    use super::traits::Mapping as MappingTrait;
    use super::{
        Aggregation, BaseInterface, BaseMapping, DatabaseRetentionPolicy, DatastreamInterface,
        DatastreamMapping, Interface, MappingType, Ownership, Reliability, Retention,
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
            \"description\": \"Interface description\",
            \"doc\": \"Interface doc\",
            \"mappings\": [
                {
                    \"endpoint\": \"/%{sensor_id}/value\",
                    \"type\": \"double\",
                    \"explicit_timestamp\": true,
                    \"description\": \"Mapping description\",
                    \"doc\": \"Mapping doc\"
                }
            ]
        }";

        let endpoint = "/%{sensor_id}/value".to_owned();
        let mapping_type = MappingType::Double;
        let explicit_timestamp = true;
        let description = Some("Mapping description".to_owned());
        let doc = Some("Mapping doc".to_owned());

        let base_mapping = BaseMapping {
            endpoint,
            mapping_type,
            description,
            doc,
        };

        let mapping = DatastreamMapping {
            base: base_mapping,
            explicit_timestamp,
            database_retention_policy: DatabaseRetentionPolicy::NoTtl,
            database_retention_ttl: None,
            expiry: None,
            retention: Retention::Discard,
            reliability: Reliability::Unreliable,
        };

        assert_eq!(mapping.is_compatible("/foo/value"), true);
        assert_eq!(mapping.is_compatible("/bar/value"), true);
        assert_eq!(mapping.is_compatible("/value"), false);
        assert_eq!(mapping.is_compatible("/foo/bar/value"), false);
        assert_eq!(mapping.endpoint(), "/%{sensor_id}/value");
        assert_eq!(mapping.mapping_type(), MappingType::Double);
        assert_eq!(mapping.description(), Some("Mapping description"));
        assert_eq!(mapping.doc(), Some("Mapping doc"));

        let interface_name = "org.astarte-platform.genericsensors.Values".to_owned();
        let version_major = 1;
        let version_minor = 0;
        let ownership = Ownership::Device;
        let description = Some("Interface description".to_owned());
        let doc = Some("Interface doc".to_owned());

        let base_interface = BaseInterface {
            interface_name,
            version_major,
            version_minor,
            ownership,
            description,
            doc,
        };

        let datastream_interface = DatastreamInterface {
            base: base_interface,
            aggregation: Aggregation::Individual,
            mappings: vec![mapping],
        };

        let interface = Interface::Datastream(datastream_interface);

        let deser_interface = Interface::from_str(interface_json).unwrap();

        assert_eq!(interface, deser_interface);

        assert_eq!(
            interface.name(),
            "org.astarte-platform.genericsensors.Values"
        );
        assert_eq!(interface.version(), (1, 0));
        assert_eq!(interface.ownership(), Ownership::Device);
        assert_eq!(interface.description(), Some("Interface description"));
        assert_eq!(interface.doc(), Some("Interface doc"));
    }
}
