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

//! Provides the functionalities to parse and validate an Astarte interface.

pub mod def;
pub mod error;
pub mod mapping;
pub mod reference;
pub(crate) mod validation;

use log::info;
use serde::{Deserialize, Serialize};

#[cfg(feature = "message-hub")]
use astarte_message_hub_proto::InterfaceJson;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

pub(crate) use self::def::{
    Aggregation, InterfaceTypeDef, Mapping, MappingType, Ownership, Reliability,
};
use self::error::InterfaceError;
use self::mapping::vec::Item;
use self::mapping::InterfaceMapping;
use self::reference::{MappingRef, ObjectRef, PropertyRef};
use self::{
    def::{DatabaseRetentionPolicyDef, InterfaceDef, RetentionDef},
    mapping::{
        iter::{IndividualMappingIter, MappingIter, ObjectMappingIter, PropertiesMappingIter},
        path::MappingPath,
        vec::MappingVec,
        BaseMapping, DatastreamIndividualMapping, PropertiesMapping,
    },
    validation::VersionChange,
};

/// Mapping between the endpoint and the path
///
/// The mappings are stored in a BTree so we can implement a custom compare of the endpoint which
/// will make a placeholder equal to any level. The [HashSet](std::collections::HashSet) would not
/// work since we cannot hash the placeholder to the same value as a simple level.
///
/// For example, if we have the following mappings:
///
/// - `/a/b/c`
/// - `/a/%{p}/c`
///
/// They should be considered the same endpoint, but we cannot hash those to the same HashSet key,
/// so we use a [`BTreeMap`] and implement a custom [`Ord`] for the [`MappingPath`]. Which is an enum to
/// compare the parsed endpoint with parameters and the mapping path of a topic received from MQTT.
///
/// A mappings can be accessed by passing the endpoint to the [`Interface::mapping`] method.
pub(crate) type MappingSet<T> = BTreeSet<Item<T>>;

/// Maximum number of mappings an interface can have
///
/// See the [Astarte interface scheme](https://docs.astarte-platform.org/latest/040-interface_schema.html#astarte-interface-schema-mappings)
pub const MAX_INTERFACE_MAPPINGS: usize = 1024;

/// Astarte interface implementation.
///
/// Should be used only through its methods, not instantiated directly.
#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(try_from = "InterfaceDef<std::borrow::Cow<str>>")]
pub struct Interface {
    interface_name: String,
    version_major: i32,
    version_minor: i32,
    ownership: Ownership,
    #[cfg(feature = "interface-doc")]
    description: Option<String>,
    #[cfg(feature = "interface-doc")]
    doc: Option<String>,
    pub(crate) inner: InterfaceType,
}

impl Interface {
    /// Returns the interface name.
    pub fn interface_name(&self) -> &str {
        &self.interface_name
    }

    /// Returns the interface major version.
    pub fn version_major(&self) -> i32 {
        self.version_major
    }

    /// Returns the interface minor version.
    pub fn version_minor(&self) -> i32 {
        self.version_minor
    }

    /// Returns the interface version.
    fn version(&self) -> (i32, i32) {
        (self.version_major, self.version_minor)
    }

    /// Returns the interface type.
    pub fn interface_type(&self) -> InterfaceTypeDef {
        match &self.inner {
            InterfaceType::DatastreamIndividual(_) | InterfaceType::DatastreamObject(_) => {
                InterfaceTypeDef::Datastream
            }
            InterfaceType::Properties(_) => InterfaceTypeDef::Properties,
        }
    }

    /// Returns the interface ownership.
    pub fn ownership(&self) -> Ownership {
        self.ownership
    }

    /// Returns the interface aggregation.
    pub fn aggregation(&self) -> Aggregation {
        match &self.inner {
            InterfaceType::Properties(_) | InterfaceType::DatastreamIndividual(_) => {
                Aggregation::Individual
            }
            InterfaceType::DatastreamObject(_) => Aggregation::Object,
        }
    }

    #[cfg(feature = "interface-doc")]
    /// Returns the interface description
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[cfg(feature = "interface-doc")]
    /// Returns the interface documentation.
    pub fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }

    /// Returns an iterator over the interface's mappings.
    pub fn iter_mappings(&self) -> MappingIter {
        MappingIter::new(&self.inner)
    }

    pub(crate) fn mapping(&self, path: &MappingPath) -> Option<Mapping<&str>> {
        match &self.inner {
            InterfaceType::DatastreamIndividual(individual) => individual.mapping(path),
            InterfaceType::DatastreamObject(object) => object.mapping(path),
            InterfaceType::Properties(properties) => properties.mapping(path),
        }
    }

    /// Returns the number of Mappings in the interface.
    pub fn mappings_len(&self) -> usize {
        match &self.inner {
            InterfaceType::DatastreamIndividual(datastream) => datastream.mappings.len(),
            InterfaceType::DatastreamObject(datastream) => datastream.mappings.len(),
            InterfaceType::Properties(properties) => properties.mappings.len(),
        }
    }

    /// Get a [`MappingRef`] reference of the path if the endpoint is present in the interface.
    pub(crate) fn as_mapping_ref<'a>(
        &'a self,
        path: &'a MappingPath,
    ) -> Option<MappingRef<&Interface>> {
        MappingRef::new(self, path)
    }

    /// Check if the interface is a property.
    pub fn is_property(&self) -> bool {
        matches!(self.inner, InterfaceType::Properties(_))
    }

    /// Returns a [`PropertyRef`] if the interface is a property.
    pub(crate) fn as_prop_ref(&self) -> Option<PropertyRef> {
        self.is_property().then_some(PropertyRef(self))
    }

    /// Check if the interface is an object.
    pub fn is_object(&self) -> bool {
        matches!(self.inner, InterfaceType::DatastreamObject(_))
    }

    /// Returns a [`ObjectRef`] if the interface is an object.
    pub(crate) fn as_object_ref(&self) -> Option<ObjectRef> {
        ObjectRef::new(self)
    }

    /// Validate if an interface is valid
    pub fn validate(&self) -> Result<(), InterfaceError> {
        if self.version() == (0, 0) {
            return Err(InterfaceError::MajorMinor);
        }

        if self.mappings_len() == 0 {
            return Err(InterfaceError::EmptyMappings);
        }

        if self.mappings_len() > MAX_INTERFACE_MAPPINGS {
            return Err(InterfaceError::TooManyMappings(self.mappings_len()));
        }

        Ok(())
    }

    /// Validate if an interface is given the previous version `prev`.
    ///
    /// It will check whether:
    ///
    /// - Both the versions are valid
    /// - The name of the interface is the same
    /// - The new version is a valid successor of the previous version.
    pub fn validate_with(&self, prev: &Self) -> Result<&Self, InterfaceError> {
        // If the interfaces are the same, they are valid
        if self == prev {
            return Ok(self);
        }

        // Check if the wrong interface was passed
        let name = self.interface_name();
        let prev_name = prev.interface_name();
        if name != prev_name {
            return Err(InterfaceError::NameMismatch {
                name: name.to_string(),
                prev_name: prev_name.to_string(),
            });
        }

        // Validate the new interface version
        VersionChange::try_new(self, prev)
            .map_err(InterfaceError::Version)
            .map(|change| {
                info!("Interface {} version changed: {}", name, change);

                self
            })
    }

    /// Returns a typed reference to a property, only if the interface is of type
    /// [`InterfaceTypeDef::Properties`].
    pub fn as_prop(&self) -> Option<PropertyRef> {
        match self.inner {
            InterfaceType::DatastreamIndividual(_) | InterfaceType::DatastreamObject(_) => None,
            InterfaceType::Properties(_) => Some(PropertyRef(self)),
        }
    }

    /// Returns a reference to the specific interface type.
    ///
    /// See the documentation on [`InterfaceType`] for more information.
    pub fn as_inner(&self) -> &InterfaceType {
        &self.inner
    }

    /// Returns the specific interface type.
    ///
    /// See the documentation on [`InterfaceType`] for more information.
    pub fn into_inner(self) -> InterfaceType {
        self.inner
    }

    // TODO: move to message hub (use serialize()), or define try_from
    #[cfg(feature = "message-hub")]
    pub(crate) fn as_proto(&self) -> Result<InterfaceJson, InterfaceError> {
        let iface = serde_json::to_vec(&self).map_err(InterfaceError::Parse)?;
        Ok(InterfaceJson::from(iface))
    }
}

impl Serialize for Interface {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let interface_def = InterfaceDef::from(self);

        interface_def.serialize(serializer)
    }
}

impl FromStr for Interface {
    type Err = InterfaceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Self::Err::from)
    }
}

impl Display for Interface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.interface_name, self.version_major, self.version_minor
        )
    }
}

/// Enum of all the types of interfaces
///
/// This is not a direct representation of only the mapping to permit extensibility of specific
/// properties present only in some aggregations.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum InterfaceType {
    /// Interface with type datastream and aggregations individual.
    DatastreamIndividual(DatastreamIndividual),
    /// Interface with type datastream and aggregations object.
    DatastreamObject(DatastreamObject),
    /// A property interface.
    Properties(Properties),
}

impl InterfaceType {
    /// Return a reference to a [`DatastreamIndividual`].
    pub fn as_datastream_individual(&self) -> Option<&DatastreamIndividual> {
        if let Self::DatastreamIndividual(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Return a reference to a [`DatastreamObject`].
    pub fn as_datastream_object(&self) -> Option<&DatastreamObject> {
        if let Self::DatastreamObject(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Return a reference to a [`Properties`].
    pub fn as_properties(&self) -> Option<&Properties> {
        if let Self::Properties(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the interface type is [`DatastreamIndividual`].
    ///
    /// [`DatastreamIndividual`]: InterfaceType::DatastreamIndividual
    #[must_use]
    pub fn is_datastream_individual(&self) -> bool {
        matches!(self, Self::DatastreamIndividual(..))
    }

    /// Returns `true` if the interface type is [`DatastreamObject`].
    ///
    /// [`DatastreamObject`]: InterfaceType::DatastreamObject
    #[must_use]
    pub fn is_datastream_object(&self) -> bool {
        matches!(self, Self::DatastreamObject(..))
    }

    /// Returns `true` if the interface type is [`Properties`].
    ///
    /// [`Properties`]: InterfaceType::Properties
    #[must_use]
    pub fn is_properties(&self) -> bool {
        matches!(self, Self::Properties(..))
    }
}

/// Interface of type datastream individual.
///
/// For this interface all the mappings have distinct configurations.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatastreamIndividual {
    mappings: MappingVec<DatastreamIndividualMapping>,
}

impl DatastreamIndividual {
    /// Returns an iterator over the interface mappings.
    pub fn iter_mappings(&self) -> IndividualMappingIter {
        IndividualMappingIter::new(&self.mappings)
    }

    pub(crate) fn add_mapping<T>(
        tree: &mut MappingSet<DatastreamIndividualMapping>,
        mapping: &Mapping<T>,
    ) -> Result<(), InterfaceError>
    where
        T: AsRef<str> + Into<String>,
    {
        let individual = Item::new(DatastreamIndividualMapping::try_from(mapping)?);

        if let Some(existing) = tree.get(&individual) {
            return Err(InterfaceError::DuplicateMapping {
                endpoint: existing.endpoint().to_string(),
                duplicate: mapping.endpoint().as_ref().into(),
            });
        }

        tree.insert(individual);

        Ok(())
    }
}

impl MappingAccess for DatastreamIndividual {
    type Mapping = DatastreamIndividualMapping;

    fn get(&self, path: &MappingPath) -> Option<&Self::Mapping> {
        self.mappings.get(path)
    }

    fn len(&self) -> usize {
        self.mappings.len()
    }
}

impl<'a> IntoIterator for &'a DatastreamIndividual {
    type Item = Mapping<&'a str>;

    type IntoIter = IndividualMappingIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mappings()
    }
}

impl<'a, T> TryFrom<&'a InterfaceDef<T>> for DatastreamIndividual
where
    T: AsRef<str> + Into<String>,
{
    type Error = InterfaceError;

    fn try_from(value: &InterfaceDef<T>) -> Result<Self, Self::Error> {
        let mut btree = MappingSet::new();

        for mapping in value.mappings.iter() {
            Self::add_mapping(&mut btree, mapping)?;
        }

        Ok(Self {
            mappings: MappingVec::from(btree),
        })
    }
}

/// Interface of type datastream object.
///
/// For this interface all the mappings have the same prefix and configurations.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatastreamObject {
    reliability: Reliability,
    explicit_timestamp: bool,
    retention: Retention,
    database_retention: DatabaseRetention,
    mappings: MappingVec<BaseMapping>,
}

impl DatastreamObject {
    pub(crate) fn apply<'a>(&self, base_mapping: &'a BaseMapping) -> Mapping<&'a str> {
        let mut mapping = Mapping::from(base_mapping);

        mapping.reliability = self.reliability;
        mapping.explicit_timestamp = self.explicit_timestamp;

        self.retention.apply(&mut mapping);
        self.database_retention.apply(&mut mapping);

        mapping
    }

    /// Returns an iterator over the interface mappings.
    pub fn iter_mappings(&self) -> ObjectMappingIter {
        ObjectMappingIter::new(self)
    }

    /// Check if the mapping is compatible with the interface
    pub(crate) fn is_compatible<T>(&self, mapping: &Mapping<T>) -> bool {
        mapping.reliability() == self.reliability
            && mapping.explicit_timestamp() == self.explicit_timestamp
            && mapping.retention() == self.retention
            && mapping.database_retention() == self.database_retention
    }

    /// Add a mapping to the interface.
    ///
    /// Since the interface is an object, the mapping must be compatible with the interface. It
    /// needs to have the same length and prefix as the other mapping.
    pub(crate) fn add_mapping<T>(
        &self,
        btree: &mut MappingSet<BaseMapping>,
        mapping: &Mapping<T>,
    ) -> Result<(), InterfaceError>
    where
        T: AsRef<str> + Into<String>,
    {
        if !self.is_compatible(mapping) {
            return Err(InterfaceError::InconsistentMapping);
        }

        let mapping = Item::new(BaseMapping::try_from(mapping)?);

        // Check that the mapping has at least two components
        // https://docs.astarte-platform.org/astarte/latest/030-interface.html#endpoints-and-aggregation
        if mapping.endpoint().len() < 2 {
            return Err(InterfaceError::ObjectEndpointTooShort(
                mapping.endpoint().to_string(),
            ));
        }

        // Check if the first element exists
        if let Some(entry) = self.mappings.iter().next() {
            // Check that the mapping has the same endpoint as the other mappings
            if !entry.endpoint().is_same_object(mapping.endpoint()) {
                return Err(InterfaceError::InconsistentEndpoints);
            }
        }

        // Check that the mapping is not already present
        if let Some(existing) = btree.get(&mapping) {
            return Err(InterfaceError::DuplicateMapping {
                endpoint: existing.endpoint().to_string(),
                duplicate: mapping.endpoint().to_string(),
            });
        }

        btree.insert(mapping);

        Ok(())
    }

    pub(crate) fn get_field(&self, base: &MappingPath, field: &str) -> Option<&BaseMapping> {
        self.mappings.get(&(base, field))
    }

    /// Returns the reliability of the object's mappings.
    pub fn reliability(&self) -> Reliability {
        self.reliability
    }

    /// Returns whether the object needs an explicit timestamp to be sent.
    pub fn explicit_timestamp(&self) -> bool {
        self.explicit_timestamp
    }
}

impl MappingAccess for DatastreamObject {
    type Mapping = BaseMapping;

    fn get(&self, path: &MappingPath) -> Option<&Self::Mapping> {
        self.mappings.get(path)
    }

    fn len(&self) -> usize {
        self.mappings.len()
    }
}

impl<'a> IntoIterator for &'a DatastreamObject {
    type Item = Mapping<&'a str>;

    type IntoIter = ObjectMappingIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mappings()
    }
}

impl<T> TryFrom<&InterfaceDef<T>> for DatastreamObject
where
    T: AsRef<str> + Into<String>,
{
    type Error = InterfaceError;

    fn try_from(value: &InterfaceDef<T>) -> Result<Self, Self::Error> {
        let mut mappings_iter = value.mappings.iter();
        let mut btree = MappingSet::new();

        let first = mappings_iter.next().ok_or(InterfaceError::EmptyMappings)?;
        let first_base = BaseMapping::try_from(first)?;

        btree.insert(Item::new(first_base));

        // We create the object from the first mapping and then insert the others, checking if
        // compatible
        let mut object = Self {
            reliability: first.reliability(),
            explicit_timestamp: first.explicit_timestamp(),
            retention: first.retention(),
            database_retention: first.database_retention(),
            mappings: MappingVec::new(),
        };

        for mapping in mappings_iter {
            object.add_mapping(&mut btree, mapping)?;
        }

        object.mappings = MappingVec::from(btree);

        Ok(object)
    }
}

/// Interface of type individual property.
///
/// For this interface all the mappings have their own configuration.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Properties {
    mappings: MappingVec<PropertiesMapping>,
}

impl Properties {
    /// Returns an iterator over the interface mappings.
    pub fn iter_mappings(&self) -> PropertiesMappingIter {
        PropertiesMappingIter::new(&self.mappings)
    }

    pub(crate) fn add_mapping<T>(
        btree: &mut MappingSet<PropertiesMapping>,
        mapping: &Mapping<T>,
    ) -> Result<(), InterfaceError>
    where
        T: AsRef<str> + Into<String>,
    {
        let property = Item::new(PropertiesMapping::try_from(mapping)?);

        if let Some(existing) = btree.get(&property) {
            return Err(InterfaceError::DuplicateMapping {
                endpoint: existing.endpoint().to_string(),
                duplicate: mapping.endpoint().as_ref().into(),
            });
        }

        btree.insert(property);

        Ok(())
    }
}

impl MappingAccess for Properties {
    type Mapping = PropertiesMapping;

    fn get(&self, path: &MappingPath) -> Option<&Self::Mapping> {
        self.mappings.get(path)
    }

    fn len(&self) -> usize {
        self.mappings.len()
    }
}

impl<'a> IntoIterator for &'a Properties {
    type Item = Mapping<&'a str>;

    type IntoIter = PropertiesMappingIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mappings()
    }
}

impl<T> TryFrom<&InterfaceDef<T>> for Properties
where
    T: AsRef<str> + Into<String>,
{
    type Error = InterfaceError;

    fn try_from(value: &InterfaceDef<T>) -> Result<Self, Self::Error> {
        let mut btree = MappingSet::new();

        for mapping in value.mappings.iter() {
            Self::add_mapping(&mut btree, mapping)?;
        }

        Ok(Self {
            mappings: MappingVec::from(btree),
        })
    }
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
    pub(self) fn apply<T>(&self, mapping: &mut Mapping<T>) {
        match self {
            Retention::Discard => {
                mapping.retention = RetentionDef::Discard;
                mapping.expiry = 0;
            }
            Retention::Volatile { expiry } => {
                mapping.retention = RetentionDef::Volatile;
                // This will never error since it will error while deserializing the interface.
                // However astarte can handle bigger integers than i64, so a conservative move is to
                // have an expiry of i64::MAX.
                mapping.expiry = expiry
                    .map(|t| t.as_secs().try_into().unwrap_or(i64::MAX))
                    .unwrap_or(0);
            }
            Retention::Stored { expiry } => {
                mapping.retention = RetentionDef::Stored;
                mapping.expiry = expiry
                    .map(|t| t.as_secs().try_into().unwrap_or(i64::MAX))
                    .unwrap_or(0);
            }
        }
    }
}

impl Default for Retention {
    fn default() -> Self {
        Self::Discard
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
    pub(self) fn apply<T>(&self, mapping: &mut Mapping<T>) {
        match self {
            DatabaseRetention::NoTtl => {
                mapping.database_retention_policy = DatabaseRetentionPolicyDef::NoTtl;
                mapping.database_retention_ttl = None;
            }
            DatabaseRetention::UseTtl { ttl } => {
                mapping.database_retention_policy = DatabaseRetentionPolicyDef::UseTtl;
                mapping.database_retention_ttl = Some(ttl.as_secs().try_into().unwrap_or(i64::MAX));
            }
        }
    }
}

impl Default for DatabaseRetention {
    fn default() -> Self {
        Self::NoTtl
    }
}

/// Access the interface's mappings.
pub trait MappingAccess
where
    for<'a> &'a Self: IntoIterator<Item = Mapping<&'a str>>,
    for<'a> &'a Self::Mapping: Into<Mapping<&'a str>>,
{
    /// Mapping specific for the interface's type.
    type Mapping: InterfaceMapping;

    /// Gets an interface mapping given the path.
    fn get(&self, path: &MappingPath) -> Option<&Self::Mapping>;

    /// Returns the number of mappings in the interface.
    fn len(&self) -> usize;

    /// Returns the mapping definition for the given path.
    fn mapping(&self, path: &MappingPath) -> Option<Mapping<&str>> {
        self.get(path).map(Into::into)
    }

    /// Returns whether the interface has no mappings.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        interface::{
            def::{DatabaseRetentionPolicyDef, RetentionDef},
            mapping::{
                path::MappingPath,
                vec::{Item, MappingVec},
                BaseMapping, DatastreamIndividualMapping,
            },
            Aggregation, DatabaseRetention, DatastreamIndividual, InterfaceType, InterfaceTypeDef,
            Mapping, MappingAccess, MappingSet, MappingType, Ownership, Reliability, Retention,
        },
        test::{E2E_DEVICE_AGGREGATE, E2E_DEVICE_DATASTREAM, E2E_DEVICE_PROPERTY},
        Interface,
    };

    // The mappings are sorted alphabetically by endpoint, so we can confront them
    #[cfg(feature = "interface-doc")]
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

    #[cfg(not(feature = "interface-doc"))]
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
            mapping: BaseMapping {
                endpoint: "/%{sensor_id}/value".try_into().unwrap(),
                mapping_type: MappingType::Double,
                #[cfg(feature = "interface-doc")]
                description: Some("Mapping description".to_string()),
                #[cfg(feature = "interface-doc")]
                doc: Some("Mapping doc".to_string()),
            },
            reliability: Reliability::default(),
            retention: Retention::default(),
            database_retention: DatabaseRetention::default(),
            explicit_timestamp: true,
        };

        let other_value_mapping = DatastreamIndividualMapping {
            mapping: BaseMapping {
                endpoint: "/%{sensor_id}/otherValue".try_into().unwrap(),
                mapping_type: MappingType::LongInteger,
                #[cfg(feature = "interface-doc")]
                description: Some("Mapping description".to_string()),
                #[cfg(feature = "interface-doc")]
                doc: Some("Mapping doc".to_string()),
            },
            reliability: Reliability::default(),
            retention: Retention::default(),
            database_retention: DatabaseRetention::default(),
            explicit_timestamp: true,
        };

        let interface_name = "org.astarte-platform.genericsensors.Values".to_owned();
        let version_major = 1;
        let version_minor = 0;
        let ownership = Ownership::Device;
        #[cfg(feature = "interface-doc")]
        let description = Some("Interface description".to_owned());
        #[cfg(feature = "interface-doc")]
        let doc = Some("Interface doc".to_owned());

        let btree = MappingSet::from_iter(
            [value_mapping, other_value_mapping]
                .into_iter()
                .map(Item::new),
        );

        let datastream_individual = DatastreamIndividual {
            mappings: MappingVec::from(btree),
        };

        let interface = Interface {
            interface_name,
            version_major,
            version_minor,
            ownership,
            #[cfg(feature = "interface-doc")]
            description,
            #[cfg(feature = "interface-doc")]
            doc,
            inner: InterfaceType::DatastreamIndividual(datastream_individual),
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

        assert!(interface.is_err());
        // This is hacky but serde doesn't provide a way to check the error
        let err = format!("{:?}", interface.unwrap_err());
        assert!(err.contains("no mappings"), "Unexpected error: {}", err);
    }

    #[test]
    fn test_properties() {
        let interface = Interface::from_str(PROPERTIES_JSON).unwrap();

        assert!(interface.is_property(), "Properties interface not found");
        assert_eq!(interface.version(), (1, 0));
        assert_eq!(interface.version_major(), 1);
        assert_eq!(interface.version_minor(), 0);

        let paths: Vec<_> = interface.iter_mappings().collect();

        assert_eq!(paths.len(), 2);
        assert_eq!(*paths[0].endpoint(), "/%{sensor_id}/aaaa");
        assert_eq!(*paths[1].endpoint(), "/%{sensor_id}/bbbb");

        let path = MappingPath::try_from("/1/aaaa").unwrap();

        let f = interface.mapping(&path).unwrap();

        assert_eq!(f.mapping_type(), MappingType::LongInteger);
        assert!(f.allow_unset);
    }

    #[test]
    fn test_iter_mappings() {
        let value_mapping = Mapping {
            endpoint: "/%{sensor_id}/value",
            mapping_type: MappingType::Double,
            #[cfg(feature = "interface-doc")]
            description: Some("Mapping description"),
            #[cfg(feature = "interface-doc")]
            doc: Some("Mapping doc"),
            #[cfg(not(feature = "interface-doc"))]
            description: (),
            #[cfg(not(feature = "interface-doc"))]
            doc: (),
            reliability: Reliability::default(),
            retention: RetentionDef::default(),
            database_retention_policy: DatabaseRetentionPolicyDef::default(),
            database_retention_ttl: None,
            allow_unset: false,
            expiry: 0,
            explicit_timestamp: true,
        };

        let other_value_mapping = Mapping {
            endpoint: "/%{sensor_id}/otherValue",
            mapping_type: MappingType::LongInteger,
            #[cfg(feature = "interface-doc")]
            description: Some("Mapping description"),
            #[cfg(feature = "interface-doc")]
            doc: Some("Mapping doc"),
            #[cfg(not(feature = "interface-doc"))]
            description: (),
            #[cfg(not(feature = "interface-doc"))]
            doc: (),
            reliability: Reliability::default(),
            retention: RetentionDef::default(),
            database_retention_policy: DatabaseRetentionPolicyDef::default(),
            database_retention_ttl: None,
            allow_unset: false,
            expiry: 0,
            explicit_timestamp: true,
        };

        let interface = Interface::from_str(INTERFACE_JSON).unwrap();

        let mut mappings_iter = interface.iter_mappings();

        assert_eq!(mappings_iter.len(), 2);
        assert_eq!(mappings_iter.next(), Some(other_value_mapping));
        assert_eq!(mappings_iter.next(), Some(value_mapping));
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
        #[cfg(feature = "interface-doc")]
        assert_eq!(interface.description(), Some("Interface description"));
        assert_eq!(interface.aggregation(), Aggregation::Individual);
        assert_eq!(interface.interface_type(), InterfaceTypeDef::Datastream);
        #[cfg(feature = "interface-doc")]
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

        let prop = interface.as_prop().expect("interface is a property");

        assert!(std::ptr::eq(prop.0, &interface));

        let interface = Interface::from_str(INTERFACE_JSON).unwrap();

        assert_eq!(interface.as_prop(), None);
    }

    #[cfg(feature = "interface-doc")]
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

        assert_eq!(
            interface.description().unwrap(),
            r#"Interface description "escaped""#
        );
        assert_eq!(interface.doc().unwrap(), r#"Interface doc "escaped""#);
        assert_eq!(
            *interface
                .mapping(&crate::interface::mapping::path::tests::mapping(
                    "/double_endpoint"
                ))
                .unwrap()
                .doc()
                .unwrap(),
            r#"Mapping doc "escaped""#
        );
    }

    #[test]
    fn should_convert_into_inner() {
        let interface = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();

        assert!(interface.as_inner().as_properties().is_some());
        assert!(interface.as_inner().as_datastream_object().is_none());
        assert!(interface.as_inner().as_datastream_individual().is_none());
        assert!(interface.as_inner().is_properties());
        assert!(!interface.as_inner().is_datastream_object());
        assert!(!interface.as_inner().is_datastream_object());

        let inner = interface.into_inner();
        let interface = inner.as_properties().unwrap();
        assert_eq!(interface.len(), 14);
        assert!(!interface.is_empty());

        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        assert!(interface.as_inner().as_properties().is_none());
        assert!(interface.as_inner().as_datastream_object().is_some());
        assert!(interface.as_inner().as_datastream_individual().is_none());
        assert!(!interface.as_inner().is_properties());
        assert!(interface.as_inner().is_datastream_object());
        assert!(!interface.as_inner().is_datastream_individual());

        let inner = interface.into_inner();
        let interface = inner.as_datastream_object().unwrap();
        assert_eq!(interface.len(), 14);
        assert!(!interface.is_empty());

        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();

        assert!(interface.as_inner().as_properties().is_none());
        assert!(interface.as_inner().as_datastream_object().is_none());
        assert!(interface.as_inner().as_datastream_individual().is_some());
        assert!(!interface.as_inner().is_properties());
        assert!(!interface.as_inner().is_datastream_object());
        assert!(interface.as_inner().is_datastream_individual());

        let inner = interface.into_inner();
        let interface = inner.as_datastream_individual().unwrap();
        assert_eq!(interface.len(), 14);
        assert!(!interface.is_empty());
    }
}
