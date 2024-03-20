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

pub(crate) mod traits;

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use traits::Interface as InterfaceTrait;
use traits::Mapping as MappingTrait;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("cannot parse interface JSON")]
    Parse(#[from] serde_json::Error),
    #[error("cannot read interface file")]
    Io(#[from] io::Error),
    #[error("wrong major and minor")]
    MajorMinor,
    #[error("interface not found")]
    InterfaceNotFound,
    #[error("mapping not found")]
    MappingNotFound,
}

/// Astarte interface implementation.
///
/// Should be used only through its methods, not instantiated directly.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Interface {
    #[doc(hidden)]
    Datastream(DatastreamInterface),
    #[doc(hidden)]
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
    /// Instantiate a new `Interface` from a file.
    pub fn from_file(path: &Path) -> Result<Self, Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let interface: Interface = serde_json::from_reader(reader)?;
        interface.validate()?;
        Ok(interface)
    }

    /// Getter function for the aggregation type of the interface.
    pub fn aggregation(&self) -> Aggregation {
        match &self {
            Self::Datastream(d) => d.aggregation,
            // Properties are always individual
            Self::Properties(_) => Aggregation::Individual,
        }
    }

    pub(crate) fn mapping(&self, path: &str) -> Option<Mapping> {
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

    pub(crate) fn mappings(&self) -> Vec<Mapping> {
        return match &self {
            Self::Datastream(d) => d.mappings.iter().map(Mapping::Datastream).collect(),
            Self::Properties(p) => p.mappings.iter().map(Mapping::Properties).collect(),
        };
    }

    pub(crate) fn mappings_len(&self) -> usize {
        match &self {
            Self::Datastream(d) => d.mappings.len(),
            Self::Properties(p) => p.mappings.len(),
        }
    }

    pub(crate) fn get_ownership(&self) -> Ownership {
        match &self {
            Interface::Datastream(iface) => iface.base.ownership,
            Interface::Properties(iface) => iface.base.ownership,
        }
    }

    /// Getter function for the the endpoint paths for each property contained in the interface.
    ///
    /// Return a vector of touples. Each tuple contains the endpoint as a `String` and the
    /// major version of the interface as a `i32`.
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

    /// Getter function for the interface name.
    pub fn get_name(&self) -> String {
        match &self {
            Interface::Datastream(iface) => iface.base.interface_name.clone(),
            Interface::Properties(iface) => iface.base.interface_name.clone(),
        }
    }

    /// Getter function for the interface major version.
    pub fn get_version_major(&self) -> i32 {
        match &self {
            Interface::Datastream(iface) => iface.base.version_major,
            Interface::Properties(iface) => iface.base.version_major,
        }
    }

    /// Getter function for the interface minor version.
    pub fn get_version_minor(&self) -> i32 {
        match &self {
            Interface::Datastream(iface) => iface.base.version_minor,
            Interface::Properties(iface) => iface.base.version_minor,
        }
    }

    fn validate(&self) -> Result<(), Error> {
        // TODO: add additional validation
        if self.get_version_major() == 0 && self.get_version_minor() == 0 {
            return Err(Error::MajorMinor);
        }
        Ok(())
    }
}

impl std::str::FromStr for Interface {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let interface: Interface = serde_json::from_str(s)?;
        interface.validate()?;
        Ok(interface)
    }
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

impl Display for Interface {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.get_name(),
            self.get_version_major(),
            self.get_version_minor()
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::interface::Error;
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
                },
                {
                    \"endpoint\": \"/%{sensor_id}/otherValue\",
                    \"type\": \"longinteger\",
                    \"explicit_timestamp\": true,
                    \"description\": \"Mapping description\",
                    \"doc\": \"Mapping doc\"
                }
            ]
        }";

        let value_base_mapping = BaseMapping {
            endpoint: "/%{sensor_id}/value".to_owned(),
            mapping_type: MappingType::Double,
            description: Some("Mapping description".to_owned()),
            doc: Some("Mapping doc".to_owned()),
        };

        let other_value_base_mapping = BaseMapping {
            endpoint: "/%{sensor_id}/otherValue".to_owned(),
            mapping_type: MappingType::LongInteger,
            description: Some("Mapping description".to_owned()),
            doc: Some("Mapping doc".to_owned()),
        };

        let value_mapping = DatastreamMapping {
            base: value_base_mapping,
            explicit_timestamp: true,
            database_retention_policy: DatabaseRetentionPolicy::NoTtl,
            database_retention_ttl: None,
            expiry: None,
            retention: Retention::Discard,
            reliability: Reliability::Unreliable,
        };

        let other_value_mapping = DatastreamMapping {
            base: other_value_base_mapping,
            explicit_timestamp: true,
            database_retention_policy: DatabaseRetentionPolicy::NoTtl,
            database_retention_ttl: None,
            expiry: None,
            retention: Retention::Discard,
            reliability: Reliability::Unreliable,
        };

        assert!(value_mapping.is_compatible("/foo/value"));
        assert!(value_mapping.is_compatible("/bar/value"));
        assert!(!value_mapping.is_compatible("/value"));
        assert!(!value_mapping.is_compatible("/foo/bar/value"));
        assert!(!value_mapping.is_compatible("/foo/value/bar"));
        assert!(other_value_mapping.is_compatible("/foo/otherValue"));
        assert!(other_value_mapping.is_compatible("/bar/otherValue"));
        assert!(!other_value_mapping.is_compatible("/otherValue"));
        assert!(!other_value_mapping.is_compatible("/foo/bar/otherValue"));
        assert!(!other_value_mapping.is_compatible("/foo/value/otherValue"));
        assert_eq!(value_mapping.endpoint(), "/%{sensor_id}/value");
        assert_eq!(value_mapping.mapping_type(), MappingType::Double);
        assert_eq!(
            value_mapping.base.description,
            Some("Mapping description".to_string())
        );
        assert_eq!(value_mapping.base.doc, Some("Mapping doc".to_string()));

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
            mappings: vec![value_mapping, other_value_mapping],
        };

        let interface = Interface::Datastream(datastream_interface);

        let deser_interface = Interface::from_str(interface_json).unwrap();

        assert_eq!(interface, deser_interface);

        assert_eq!(
            interface.name(),
            "org.astarte-platform.genericsensors.Values"
        );
        assert_eq!(interface.version(), (1, 0));
        assert_eq!(interface.base_interface().ownership, Ownership::Device);
        assert_eq!(
            interface.base_interface().description,
            Some("Interface description".to_string())
        );
        assert_eq!(
            interface.base_interface().doc,
            Some("Interface doc".to_string())
        );
    }

    #[test]
    fn validation_test() {
        let interface_json = "
        {
            \"interface_name\": \"org.astarte-platform.genericsensors.Values\",
            \"version_major\": 0,
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
                },
                {
                    \"endpoint\": \"/%{sensor_id}/otherValue\",
                    \"type\": \"longinteger\",
                    \"explicit_timestamp\": true,
                    \"description\": \"Mapping description\",
                    \"doc\": \"Mapping doc\"
                }
            ]
        }";

        let deser_interface = Interface::from_str(interface_json);

        assert!(deser_interface.is_err());
        assert!(match deser_interface {
            Err(Error::MajorMinor) => true,
            Err(e) => panic!("expected Error::MajorMinor, got {e:?}"),
            Ok(_) => panic!("Expected Err, got Ok"),
        });
    }
}
