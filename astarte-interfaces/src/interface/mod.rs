// This file is part of Astarte.
//
// Copyright 2021 - 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Provides the functionalities to parse and validate an Astarte interface.

use std::borrow::Cow;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

use name::InterfaceName;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use self::datastream::individual::DatastreamIndividual;
use self::datastream::object::DatastreamObject;
use self::properties::Properties;
use self::validation::VersionChange;
use self::version::InterfaceVersion;
use crate::error::Error;
use crate::mapping::{collection::MappingVec, path::MappingPath};
use crate::schema::{InterfaceJson, Mapping};

// Re export the schema types
pub use crate::schema::{Aggregation, DatabaseRetentionPolicy, InterfaceType, Ownership};

pub mod datastream;
pub mod name;
pub mod properties;
pub mod validation;
pub mod version;

/// Maximum number of mappings an interface can have
///
/// See the [Astarte interface scheme](https://docs.astarte-platform.org/latest/040-interface_schema.html#astarte-interface-schema-mappings)
pub const MAX_INTERFACE_MAPPINGS: usize = 1024;

/// Astarte interface implementation.
///
/// Should be used only through its conversion methods, not instantiated directly.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
#[serde(try_from = "InterfaceJson<std::borrow::Cow<str>>")]
pub struct Interface {
    inner: InterfaceTypeAggregation,
}

impl Interface {
    /// Returns the inner enum of the [`InterfaceTypeAggregation`]
    pub fn inner(&self) -> &InterfaceTypeAggregation {
        &self.inner
    }

    /// Returns the interface name.
    #[must_use]
    pub fn interface_name(&self) -> &str {
        match &self.inner {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                datastream_individual.name()
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                datastream_object.name()
            }
            InterfaceTypeAggregation::Properties(property) => property.name(),
        }
    }

    /// Returns the interface version.
    #[must_use]
    pub fn version(&self) -> InterfaceVersion {
        match &self.inner {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                datastream_individual.version()
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                datastream_object.version()
            }
            InterfaceTypeAggregation::Properties(property) => property.version(),
        }
    }

    /// Returns the interface major version.
    #[must_use]
    pub fn version_major(&self) -> i32 {
        match &self.inner {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                datastream_individual.version_major()
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                datastream_object.version_major()
            }
            InterfaceTypeAggregation::Properties(property) => property.version_major(),
        }
    }

    /// Returns the interface minor version.
    #[must_use]
    pub fn version_minor(&self) -> i32 {
        match &self.inner {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                datastream_individual.version_minor()
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                datastream_object.version_minor()
            }
            InterfaceTypeAggregation::Properties(property) => property.version_minor(),
        }
    }

    /// Returns the interface type.
    #[must_use]
    pub fn interface_type(&self) -> InterfaceType {
        match &self.inner {
            InterfaceTypeAggregation::DatastreamIndividual(_)
            | InterfaceTypeAggregation::DatastreamObject(_) => InterfaceType::Datastream,
            InterfaceTypeAggregation::Properties(_) => InterfaceType::Properties,
        }
    }

    /// Returns the interface ownership.
    #[must_use]
    pub fn ownership(&self) -> Ownership {
        match &self.inner {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                datastream_individual.ownership()
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                datastream_object.ownership()
            }
            InterfaceTypeAggregation::Properties(property) => property.ownership(),
        }
    }

    /// Returns the interface aggregation.
    #[must_use]
    pub fn aggregation(&self) -> Aggregation {
        match &self.inner {
            InterfaceTypeAggregation::Properties(_)
            | InterfaceTypeAggregation::DatastreamIndividual(_) => Aggregation::Individual,
            InterfaceTypeAggregation::DatastreamObject(_) => Aggregation::Object,
        }
    }

    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    /// Returns the interface description
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        match &self.inner {
            InterfaceTypeAggregation::Properties(interface) => interface.description(),
            InterfaceTypeAggregation::DatastreamIndividual(interface) => interface.description(),
            InterfaceTypeAggregation::DatastreamObject(interface) => interface.description(),
        }
    }

    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    /// Returns the interface documentation.
    #[must_use]
    pub fn doc(&self) -> Option<&str> {
        match &self.inner {
            InterfaceTypeAggregation::Properties(interface) => interface.doc(),
            InterfaceTypeAggregation::DatastreamIndividual(interface) => interface.doc(),
            InterfaceTypeAggregation::DatastreamObject(interface) => interface.doc(),
        }
    }

    /// Validate an interface given the previous version `prev`.
    ///
    /// It will check whether:
    ///
    /// - Both the versions are valid
    /// - The name of the interface is the same
    /// - The new version is a valid successor of the previous version.
    pub fn validate_with(&self, prev: &Self) -> Result<&Self, Error> {
        // If the interfaces are the same, they are valid
        if self == prev {
            return Ok(self);
        }

        // Check if the wrong interface was passed
        let name = self.interface_name();
        let prev_name = prev.interface_name();
        if name != prev_name {
            return Err(Error::NameMismatch {
                name: name.to_string(),
                prev_name: prev_name.to_string(),
            });
        }

        // Validate the new interface version
        VersionChange::try_new(self, prev)
            .map_err(Error::VersionChange)
            .map(|change| {
                info!("Interface {} version changed: {}", name, change);

                self
            })
    }

    /// Return a reference to a [`DatastreamIndividual`].
    #[must_use]
    pub fn as_datastream_individual(&self) -> Option<&DatastreamIndividual> {
        if let InterfaceTypeAggregation::DatastreamIndividual(v) = &self.inner {
            Some(v)
        } else {
            None
        }
    }

    /// Return a reference to a [`DatastreamObject`].
    #[must_use]
    pub fn as_datastream_object(&self) -> Option<&DatastreamObject> {
        if let InterfaceTypeAggregation::DatastreamObject(v) = &self.inner {
            Some(v)
        } else {
            None
        }
    }

    /// Return a reference to a [`Properties`].
    #[must_use]
    pub fn as_properties(&self) -> Option<&Properties> {
        if let InterfaceTypeAggregation::Properties(v) = &self.inner {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the interface type is [`DatastreamIndividual`].
    ///
    /// [`DatastreamIndividual`]: InterfaceTypeAggregation::DatastreamIndividual
    #[must_use]
    pub fn is_datastream_individual(&self) -> bool {
        matches!(
            self.inner,
            InterfaceTypeAggregation::DatastreamIndividual(..)
        )
    }

    /// Returns `true` if the interface type is [`DatastreamObject`].
    ///
    /// [`DatastreamObject`]: InterfaceTypeAggregation::DatastreamObject
    #[must_use]
    pub fn is_datastream_object(&self) -> bool {
        matches!(self.inner, InterfaceTypeAggregation::DatastreamObject(..))
    }

    /// Returns `true` if the interface type is [`Properties`].
    ///
    /// [`Properties`]: InterfaceTypeAggregation::Properties
    #[must_use]
    pub fn is_properties(&self) -> bool {
        matches!(self.inner, InterfaceTypeAggregation::Properties(..))
    }
}

impl<T> TryFrom<InterfaceJson<T>> for Interface
where
    T: AsRef<str> + Into<String>,
{
    type Error = Error;

    fn try_from(value: InterfaceJson<T>) -> Result<Self, Self::Error> {
        let inner = match (value.interface_type, value.aggregation.unwrap_or_default()) {
            (InterfaceType::Datastream, Aggregation::Individual) => {
                let interface = DatastreamIndividual::try_from(value)?;

                InterfaceTypeAggregation::DatastreamIndividual(interface)
            }
            (InterfaceType::Datastream, Aggregation::Object) => {
                let interface = DatastreamObject::try_from(value)?;

                InterfaceTypeAggregation::DatastreamObject(interface)
            }
            (InterfaceType::Properties, Aggregation::Individual) => {
                let interface = Properties::try_from(value)?;

                InterfaceTypeAggregation::Properties(interface)
            }
            (InterfaceType::Properties, Aggregation::Object) => return Err(Error::PropertyObject),
        };

        Ok(Interface { inner })
    }
}

impl<'a> From<&'a Interface> for InterfaceJson<Cow<'a, str>> {
    fn from(value: &'a Interface) -> Self {
        match &value.inner {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                Self::from(datastream_individual)
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                Self::from(datastream_object)
            }
            InterfaceTypeAggregation::Properties(properties) => Self::from(properties),
        }
    }
}

impl Serialize for Interface {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.inner {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                datastream_individual.serialize(serializer)
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                datastream_object.serialize(serializer)
            }
            InterfaceTypeAggregation::Properties(properties) => properties.serialize(serializer),
        }
    }
}

impl FromStr for Interface {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let interface: InterfaceJson<Cow<str>> = serde_json::from_str(s)?;

        Interface::try_from(interface)
    }
}

impl Display for Interface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                write!(f, "{datastream_individual}")
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                write!(f, "{datastream_object}")
            }
            InterfaceTypeAggregation::Properties(property) => {
                write!(f, "{property}")
            }
        }
    }
}

/// Enum of all the types and aggregation of interfaces
///
/// This is not a direct representation of only the mapping to permit extensibility of specific
/// properties present only in some aggregations.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum InterfaceTypeAggregation {
    /// Interface with type datastream and aggregations individual.
    DatastreamIndividual(DatastreamIndividual),
    /// Interface with type datastream and aggregations object.
    DatastreamObject(DatastreamObject),
    /// A property interface.
    Properties(Properties),
}

/// Defines the retention of a data stream.
///
/// Describes what to do with the sent data if the transport is incapable of delivering it.
///
/// See [Retention](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html#astarte-mapping-schema-retention)
/// for more information.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Retention {
    /// Data is discarded.
    Discard,
    /// Data is kept in a cache in memory.
    Volatile {
        /// Duration for the data to expire.
        ///
        /// If it's [`None`] it will never expire.
        expiry: Option<Duration>,
    },
    /// Data is kept on disk.
    Stored {
        /// Duration for the data to expire.
        ///
        /// If it's [`None`] it will never expire.
        expiry: Option<Duration>,
    },
}

impl Retention {
    /// Returns `true` if the retention is [`Stored`].
    ///
    /// [`Stored`]: Retention::Stored
    #[must_use]
    pub const fn is_stored(&self) -> bool {
        matches!(self, Self::Stored { .. })
    }

    /// Returns the expiry for the retention.
    ///
    /// For the [`Discard`](Retention::Discard) will always return [`None`], while for
    /// [`Volatile`](Retention::Volatile) or [`Stored`](Retention::Stored) returns the inner expiry
    /// only if set.
    #[must_use]
    pub const fn as_expiry(&self) -> Option<&Duration> {
        match self {
            Retention::Discard => None,
            // Duration is copy
            Retention::Volatile { expiry } | Retention::Stored { expiry } => expiry.as_ref(),
        }
    }

    /// Returns the expiry for the retention in seconds.
    ///
    /// For the [`Discard`](Retention::Discard) will always return [`None`], while for
    /// [`Volatile`](Retention::Volatile) or [`Stored`](Retention::Stored) returns the inner expiry
    /// only if set.
    #[must_use]
    pub fn as_expiry_seconds(&self) -> Option<i64> {
        self.as_expiry().map(|duration| {
            // The expiry duration was created from a i64, but we cap at i64::MAX to be sure
            i64::try_from(duration.as_secs())
                .inspect_err(|err| warn!(%err, "expiry conversion error"))
                .unwrap_or(i64::MAX)
        })
    }

    /// Returns `true` if the retention is [`Volatile`].
    ///
    /// [`Volatile`]: Retention::Volatile
    #[must_use]
    pub const fn is_volatile(&self) -> bool {
        matches!(self, Self::Volatile { .. })
    }

    /// Returns `true` if the retention is [`Discard`].
    ///
    /// [`Discard`]: Retention::Discard
    #[must_use]
    pub const fn is_discard(&self) -> bool {
        matches!(self, Self::Discard)
    }
}

impl Default for Retention {
    fn default() -> Self {
        Self::Discard
    }
}

impl From<Retention> for crate::schema::Retention {
    fn from(value: Retention) -> Self {
        match value {
            Retention::Discard => crate::schema::Retention::Discard,
            Retention::Volatile { .. } => crate::schema::Retention::Volatile,
            Retention::Stored { .. } => crate::schema::Retention::Stored,
        }
    }
}

/// Defines if data should be expired from the database after a given interval.
///
/// See [Database Retention Policy](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html#astarte-mapping-schema-database_retention_policy)
/// for more information.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DatabaseRetention {
    /// Data will never expire.
    NoTtl,
    /// Data will live for the ttl.
    UseTtl {
        /// Time to live int the database.
        ttl: Duration,
    },
}

impl DatabaseRetention {
    /// Returns `true` if the database retention is [`NoTtl`].
    ///
    /// [`NoTtl`]: DatabaseRetention::NoTtl
    #[must_use]
    pub fn is_no_ttl(&self) -> bool {
        matches!(self, Self::NoTtl)
    }

    /// Returns `true` if the database retention is [`UseTtl`].
    ///
    /// [`UseTtl`]: DatabaseRetention::UseTtl
    #[must_use]
    pub fn is_use_ttl(&self) -> bool {
        matches!(self, Self::UseTtl { .. })
    }

    /// Returns the duration of the ttl if the policy is [`UseTtl`]
    ///
    /// [`UseTtl`]: DatabaseRetention::UseTtl
    #[must_use]
    pub fn as_ttl(&self) -> Option<&Duration> {
        match self {
            DatabaseRetention::NoTtl => None,
            DatabaseRetention::UseTtl { ttl } => Some(ttl),
        }
    }

    /// Returns the duration of the ttl if the policy is [`UseTtl`]
    ///
    /// [`UseTtl`]: DatabaseRetention::UseTtl
    #[must_use]
    pub fn as_ttl_secs(&self) -> Option<i64> {
        self.as_ttl().map(|ttl| {
            // The expiry duration was created from a i64, but we cap at i64::MAX to be sure
            i64::try_from(ttl.as_secs())
                .inspect_err(|err| warn!(%err, "ttl conversion error"))
                .unwrap_or(i64::MAX)
        })
    }
}

impl Default for DatabaseRetention {
    fn default() -> Self {
        Self::NoTtl
    }
}

impl From<DatabaseRetention> for DatabaseRetentionPolicy {
    fn from(value: DatabaseRetention) -> Self {
        match value {
            DatabaseRetention::NoTtl => DatabaseRetentionPolicy::NoTtl,
            DatabaseRetention::UseTtl { .. } => DatabaseRetentionPolicy::UseTtl,
        }
    }
}

/// Access to the information of an interface.
pub trait Schema {
    /// Mapping specific for the interface type and aggregation.
    type Mapping: Sized;

    /// Returns the interface name.
    fn name(&self) -> &str;
    /// Returns the interface name.
    fn interface_name(&self) -> &InterfaceName;
    /// Returns the interface major version.
    fn version_major(&self) -> i32;
    /// Returns the interface minor version.
    fn version_minor(&self) -> i32;
    /// Returns the interface version.
    fn version(&self) -> InterfaceVersion;
    /// Returns the interface type.
    fn interface_type(&self) -> InterfaceType;
    /// Returns the interface ownership.
    fn ownership(&self) -> Ownership;
    /// Returns the interface aggregation.
    fn aggregation(&self) -> Aggregation;

    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    /// Returns the interface description
    fn description(&self) -> Option<&str>;

    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    /// Returns the interface documentation.
    fn doc(&self) -> Option<&str>;

    /// Returns an iterator over the interface's mappings.
    fn iter_mappings(&self) -> impl Iterator<Item = &Self::Mapping>;

    /// Returns the number of Mappings in the interface.
    fn mappings_len(&self) -> usize;

    /// Returns an iterator over the interface's mappings.
    fn iter_interface_mappings(&self) -> impl Iterator<Item = Mapping<Cow<'_, str>>>;
}

/// Access information of an interface with [`Aggregation`] individual.
pub trait AggregationIndividual: Schema {
    /// Returns the mapping with the given path.
    fn mapping(&self, path: &MappingPath) -> Option<&Self::Mapping>;
}

impl<'a, T> From<&'a T> for InterfaceJson<Cow<'a, str>>
where
    T: Schema,
{
    fn from(value: &'a T) -> Self {
        InterfaceJson {
            interface_name: value.interface_name().as_str().into(),
            version_major: value.version_major(),
            version_minor: value.version_minor(),
            interface_type: value.interface_type(),
            ownership: value.ownership(),
            aggregation: Some(value.aggregation()),
            #[cfg(feature = "doc-fields")]
            description: value.description().map(Cow::from),
            #[cfg(feature = "doc-fields")]
            doc: value.doc().map(Cow::from),
            #[cfg(not(feature = "doc-fields"))]
            description: None,
            #[cfg(not(feature = "doc-fields"))]
            doc: None,
            mappings: value.iter_interface_mappings().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use pretty_assertions::assert_eq;

    use super::*;
    use crate::{
        mapping::{InterfaceMapping, MappingError},
        schema::{InterfaceType, MappingType, Ownership, Reliability},
        DatastreamIndividual, DatastreamIndividualMapping, Endpoint, Interface, MappingPath,
        Schema,
    };

    const E2E_DEVICE_PROPERTY: &str= include_str!("../../../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.DeviceProperty.json");
    const E2E_DEVICE_AGGREGATE: &str = include_str!(
        "../../../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceAggregate.json"
    );
    const E2E_DEVICE_DATASTREAM: &str = include_str!(
        "../../../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceDatastream.json"
    );

    // The mappings are sorted alphabetically by endpoint, so we can confront them
    #[cfg(feature = "doc-fields")]
    const INTERFACE_JSON: &str = r#"{
            "interface_name": "org.astarte-platform.genericsensors.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "datastream",
            "ownership": "device",
            "description": "Interface description",
            "doc": "Interface doc",
            "mappings": [
                {
                    "endpoint": "/%{sensor_id}/otherValue",
                    "type": "longinteger",
                    "explicit_timestamp": true,
                    "description": "Mapping description",
                    "doc": "Mapping doc"
                },
                {
                    "endpoint": "/%{sensor_id}/value",
                    "type": "double",
                    "explicit_timestamp": true,
                    "description": "Mapping description",
                    "doc": "Mapping doc"
                }
            ]
        }"#;

    #[cfg(not(feature = "doc-fields"))]
    const INTERFACE_JSON: &str = r#"{
            "interface_name": "org.astarte-platform.genericsensors.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/%{sensor_id}/otherValue",
                    "type": "longinteger",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/%{sensor_id}/value",
                    "type": "double",
                    "explicit_timestamp": true
                }
            ]
        }"#;

    // The mappings are sorted alphabetically by endpoint, so we can confront them
    const PROPERTIES_JSON: &str = r#"{
            "interface_name": "org.astarte-platform.genericproperties.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "properties",
            "ownership": "server",
            "description": "Interface description",
            "doc": "Interface doc",
            "mappings": [
                {
                    "endpoint": "/%{sensor_id}/aaaa",
                    "type": "longinteger",
                    "allow_unset": true
                },
                {
                    "endpoint": "/%{sensor_id}/bbbb",
                    "type": "double",
                    "allow_unset": false
                }
            ]
        }"#;

    #[test]
    fn datastream_interface_deserialization() {
        let value_mapping = DatastreamIndividualMapping {
            endpoint: Endpoint::try_from("/%{sensor_id}/value").unwrap(),
            mapping_type: MappingType::Double,
            reliability: Reliability::default(),
            retention: Retention::default(),
            #[cfg(feature = "server-fields")]
            database_retention: DatabaseRetention::default(),
            explicit_timestamp: true,
            #[cfg(feature = "doc-fields")]
            description: Some("Mapping description".to_string()),
            #[cfg(feature = "doc-fields")]
            doc: Some("Mapping doc".to_string()),
        };

        let other_value_mapping = DatastreamIndividualMapping {
            endpoint: Endpoint::try_from("/%{sensor_id}/otherValue").unwrap(),
            mapping_type: MappingType::LongInteger,
            reliability: Reliability::default(),
            retention: Retention::default(),
            #[cfg(feature = "server-fields")]
            database_retention: DatabaseRetention::default(),
            explicit_timestamp: true,
            #[cfg(feature = "doc-fields")]
            description: Some("Mapping description".to_string()),
            #[cfg(feature = "doc-fields")]
            doc: Some("Mapping doc".to_string()),
        };

        let interface_name =
            InterfaceName::try_from("org.astarte-platform.genericsensors.Values").unwrap();
        let version = InterfaceVersion::try_from((1, 0)).unwrap();
        let ownership = Ownership::Device;
        #[cfg(feature = "doc-fields")]
        let description = Some("Interface description".to_owned());
        #[cfg(feature = "doc-fields")]
        let doc = Some("Interface doc".to_owned());

        let mappings = MappingVec::try_from([other_value_mapping, value_mapping].to_vec()).unwrap();

        let datastream_individual = DatastreamIndividual {
            name: interface_name.into_string(),
            version,
            ownership,
            #[cfg(feature = "doc-fields")]
            description,
            #[cfg(feature = "doc-fields")]
            doc,
            mappings,
        };

        let interface = Interface {
            inner: InterfaceTypeAggregation::DatastreamIndividual(datastream_individual),
        };

        let deser_interface = Interface::from_str(INTERFACE_JSON).unwrap();

        assert_eq!(interface, deser_interface);
    }

    #[test]
    fn must_have_one_mapping() {
        let json = r#"{
            "interface_name": "org.astarte-platform.genericproperties.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "properties",
            "ownership": "server",
            "description": "Interface description",
            "doc": "Interface doc",
            "mappings": []
        }"#;

        let interface = Interface::from_str(json);

        let err = interface.unwrap_err();
        assert!(matches!(err, Error::Mapping(MappingError::Empty)));
    }

    #[test]
    fn test_properties() {
        let interface = Interface::from_str(PROPERTIES_JSON).unwrap();

        let exp = InterfaceVersion::try_new(1, 0).unwrap();

        assert_eq!(interface.interface_type(), InterfaceType::Properties);
        assert_eq!(interface.version(), exp);
        assert_eq!(interface.version_major(), 1);
        assert_eq!(interface.version_minor(), 0);

        let InterfaceTypeAggregation::Properties(interface) = interface.inner else {
            panic!()
        };

        let paths: Vec<_> = interface.iter_mappings().collect();

        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].endpoint().to_string(), "/%{sensor_id}/aaaa");
        assert_eq!(paths[1].endpoint().to_string(), "/%{sensor_id}/bbbb");

        let path = MappingPath::try_from("/1/aaaa").unwrap();

        let f = interface.mapping(&path).unwrap();

        assert_eq!(f.mapping_type(), MappingType::LongInteger);
        assert!(f.allow_unset());
    }

    #[test]
    fn test_iter_mappings() {
        let value_mapping = DatastreamIndividualMapping {
            endpoint: Endpoint::try_from("/%{sensor_id}/value").unwrap(),
            mapping_type: MappingType::Double,
            #[cfg(feature = "doc-fields")]
            description: Some("Mapping description".to_string()),
            #[cfg(feature = "doc-fields")]
            doc: Some("Mapping doc".to_string()),
            reliability: Reliability::default(),
            retention: Retention::default(),
            #[cfg(feature = "server-fields")]
            database_retention: DatabaseRetention::default(),
            explicit_timestamp: true,
        };

        let other_value_mapping = DatastreamIndividualMapping {
            endpoint: Endpoint::try_from("/%{sensor_id}/otherValue").unwrap(),
            mapping_type: MappingType::LongInteger,
            #[cfg(feature = "doc-fields")]
            description: Some("Mapping description".to_string()),
            #[cfg(feature = "doc-fields")]
            doc: Some("Mapping doc".to_string()),
            reliability: Reliability::default(),
            retention: Retention::default(),
            #[cfg(feature = "server-fields")]
            database_retention: DatabaseRetention::default(),
            explicit_timestamp: true,
        };

        let interface = Interface::from_str(INTERFACE_JSON).unwrap();
        let interface = interface.as_datastream_individual().unwrap();

        let mut mappings = interface.iter_mappings();

        assert_eq!(mappings.next(), Some(&other_value_mapping));
        assert_eq!(mappings.next(), Some(&value_mapping));
        assert_eq!(mappings.next(), None);
    }

    #[test]
    fn methods_test() {
        let interface = Interface::from_str(INTERFACE_JSON).unwrap();

        assert_eq!(
            interface.interface_name(),
            "org.astarte-platform.genericsensors.Values"
        );
        assert_eq!(interface.version_major(), 1);
        assert_eq!(interface.version_minor(), 0);
        assert_eq!(interface.ownership(), Ownership::Device);
        #[cfg(feature = "doc-fields")]
        assert_eq!(interface.description(), Some("Interface description"));
        assert_eq!(interface.aggregation(), Aggregation::Individual);
        assert_eq!(interface.interface_type(), InterfaceType::Datastream);
        #[cfg(feature = "doc-fields")]
        assert_eq!(interface.doc(), Some("Interface doc"));
    }

    #[test]
    fn serialize_and_deserialize() {
        let interface = Interface::from_str(INTERFACE_JSON).unwrap();
        let serialized = serde_json::to_string(&interface).unwrap();
        let deserialized: Interface = serde_json::from_str(&serialized).unwrap();

        assert_eq!(interface, deserialized);

        let value = serde_json::Value::from_str(&serialized).unwrap();
        let expected = serde_json::Value::from_str(INTERFACE_JSON).unwrap();
        assert_eq!(value, expected);
    }

    #[test]
    fn check_as_prop() {
        let interface = Interface::from_str(PROPERTIES_JSON).unwrap();

        interface.as_properties().expect("interface is a property");

        let interface = Interface::from_str(INTERFACE_JSON).unwrap();

        assert_eq!(interface.as_properties(), None);
    }

    #[cfg(feature = "doc-fields")]
    #[test]
    fn test_with_escaped_descriptions() {
        let json = r#"{
            "interface_name": "org.astarte-platform.genericproperties.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "properties",
            "ownership": "server",
            "description": "Interface description \"escaped\"",
            "doc": "Interface doc \"escaped\"",
            "mappings": [{
                "endpoint": "/double_endpoint",
                "type": "double",
                "doc": "Mapping doc \"escaped\""
            }]
        }"#;

        let interface = Interface::from_str(json).unwrap();
        let interface = interface.as_properties().unwrap();

        assert_eq!(
            interface.description().unwrap(),
            r#"Interface description "escaped""#
        );
        assert_eq!(interface.doc().unwrap(), r#"Interface doc "escaped""#);
        let mapping_doc = interface
            .mapping(&MappingPath::try_from("/double_endpoint").unwrap())
            .unwrap()
            .doc()
            .unwrap();
        assert_eq!(mapping_doc, r#"Mapping doc "escaped""#);
    }

    #[test]
    fn should_convert_into_inner() {
        let interface = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();

        assert!(interface.as_properties().is_some());
        assert!(interface.as_datastream_object().is_none());
        assert!(interface.as_datastream_individual().is_none());
        assert!(interface.is_properties());
        assert!(!interface.is_datastream_object());
        assert!(!interface.is_datastream_object());

        let interface = interface.as_properties().unwrap();
        assert_eq!(interface.mappings_len(), 14);

        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        assert!(interface.as_properties().is_none());
        assert!(interface.as_datastream_object().is_some());
        assert!(interface.as_datastream_individual().is_none());
        assert!(!interface.is_properties());
        assert!(interface.is_datastream_object());
        assert!(!interface.is_datastream_individual());

        let interface = interface.as_datastream_object().unwrap();
        assert_eq!(interface.mappings_len(), 14);

        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();

        assert!(interface.as_properties().is_none());
        assert!(interface.as_datastream_object().is_none());
        assert!(interface.as_datastream_individual().is_some());
        assert!(!interface.is_properties());
        assert!(!interface.is_datastream_object());
        assert!(interface.is_datastream_individual());

        let interface = interface.as_datastream_individual().unwrap();
        assert_eq!(interface.mappings_len(), 14);
    }
}
