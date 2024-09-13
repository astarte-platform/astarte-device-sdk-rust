// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Astarte Interface definition, this module contains the structs for the actual JSON
//! representation definition of the [`Interface`] and mapping.
//!
//! For more information see:
//! [Interface Schema - Astarte](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html)

use std::{fmt::Display, time::Duration};

use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    interface::{DatastreamIndividual, DatastreamObject, Properties},
    Interface,
};

use super::{DatabaseRetention, InterfaceError, InterfaceType, Retention};

/// Utility to skip default value
fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// Utility to check the truthiness of a boolean value.
fn is_false(flag: &bool) -> bool {
    !flag
}

/// Utility to check a integer is equal to 0.
fn is_zero(value: &i64) -> bool {
    *value == 0
}

/// The structure is a direct mapping of the JSON schema, they are then transformed in our
/// internal representation of [Interface](crate::interface::Interface) when de-serializing using
/// [`TryFrom`].
///
/// The fields of the JSON can either be:
///
/// - **Required**: the field is the value it represents, it cannot be omitted.
/// - **Optional with default**: the field is optional, but it is value it represents (not wrapped
///   in [`Option`]). It will not be serialized if the value is the default one.
/// - **Optional**: the field is optional, it is wrapped in [`Option`]. It will not be serialized if
///   the value is [`None`].
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "interface-strict", serde(deny_unknown_fields))]
pub(super) struct InterfaceDef<T> {
    pub(super) interface_name: T,
    pub(super) version_major: i32,
    pub(super) version_minor: i32,
    #[serde(rename = "type")]
    pub(super) interface_type: InterfaceTypeDef,
    pub(super) ownership: Ownership,
    #[serde(default, skip_serializing_if = "is_default")]
    pub(super) aggregation: Aggregation,
    #[cfg(not(feature = "interface-doc"))]
    #[serde(default, skip_serializing, deserialize_with = "doc::deserialize_doc")]
    pub(super) description: (),
    #[cfg(not(feature = "interface-doc"))]
    #[serde(default, skip_serializing, deserialize_with = "doc::deserialize_doc")]
    pub(super) doc: (),
    #[cfg(feature = "interface-doc")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) description: Option<T>,
    #[cfg(feature = "interface-doc")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) doc: Option<T>,
    pub(super) mappings: Vec<Mapping<T>>,
}

/// Mapping of an Interface.
///
/// It includes all the fields available for a mapping, but it it is validated when built with the
/// [`TryFrom`]. It uniforms the different types of mappings like
/// [`DatastreamIndividualMapping`](super::mapping::DatastreamIndividualMapping),
/// [`DatastreamObject`] mappings and [`PropertiesMapping`](super::mapping::PropertiesMapping) in a
/// single struct.
///
/// Since it's a 1:1 representation of the JSON it is used for serialization and deserialization,
/// and then is converted to the internal representation of the mapping with the [`TryFrom`] and
/// [`From`] traits of the [`Interface`]'s' mappings.
//
/// You can find the specification here [Mapping Schema -
/// Astarte](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html#mapping)
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "interface-strict", serde(deny_unknown_fields))]
pub struct Mapping<T> {
    pub(super) endpoint: T,
    #[serde(rename = "type")]
    pub(super) mapping_type: MappingType,
    #[serde(default, skip_serializing_if = "is_default")]
    pub(super) reliability: Reliability,
    #[serde(default, skip_serializing_if = "is_default")]
    pub(super) retention: RetentionDef,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(super) expiry: i64,
    #[serde(default, skip_serializing_if = "is_default")]
    pub(super) database_retention_policy: DatabaseRetentionPolicyDef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) database_retention_ttl: Option<i64>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) allow_unset: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) explicit_timestamp: bool,
    #[cfg(not(feature = "interface-doc"))]
    #[serde(default, skip_serializing, deserialize_with = "doc::deserialize_doc")]
    pub(super) description: (),
    #[cfg(not(feature = "interface-doc"))]
    #[serde(default, skip_serializing, deserialize_with = "doc::deserialize_doc")]
    pub(super) doc: (),
    #[cfg(feature = "interface-doc")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) description: Option<T>,
    #[cfg(feature = "interface-doc")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) doc: Option<T>,
}

impl<T> Mapping<T> {
    pub(super) fn new(endpoint: T, mapping_type: MappingType) -> Self {
        Mapping {
            endpoint,
            mapping_type,
            reliability: Reliability::default(),
            retention: RetentionDef::default(),
            expiry: 0,
            database_retention_policy: DatabaseRetentionPolicyDef::default(),
            database_retention_ttl: None,
            allow_unset: false,
            explicit_timestamp: false,
            description: Default::default(),
            doc: Default::default(),
        }
    }

    #[cfg(feature = "interface-doc")]
    pub(crate) fn with_description(mut self, description: Option<T>) -> Self {
        self.description = description;

        self
    }

    #[cfg(feature = "interface-doc")]
    pub(crate) fn with_doc(mut self, doc: Option<T>) -> Self {
        self.doc = doc;

        self
    }

    /// Path of the mapping.
    ///
    /// It can be parametrized (e.g. `/foo/%{path}/baz`).
    pub fn endpoint(&self) -> &T {
        &self.endpoint
    }

    /// Returns the mapping's type.
    pub fn mapping_type(&self) -> MappingType {
        self.mapping_type
    }

    /// Reliability of the data stream.
    ///
    /// See the [`Reliability`] documentation for more information.
    pub fn reliability(&self) -> Reliability {
        self.reliability
    }

    /// Expiry of the data stream.
    ///
    /// If it's [`None`] the stream will never expire.
    pub fn expiry(&self) -> Option<Duration> {
        match &self.expiry {
            ..=0 => None,
            1.. => Some(Duration::from_secs(self.expiry as u64)),
        }
    }

    /// Expiry of the data stream.
    ///
    /// If it's [`None`] the stream will never expire.
    pub fn expiry_as_i64(&self) -> i64 {
        self.expiry
    }

    /// Retention of the data stream.
    ///
    /// See the [`Retention`] documentation for more information.
    pub fn retention(&self) -> Retention {
        match self.retention {
            RetentionDef::Discard => {
                if self.expiry > 0 {
                    warn!("Discard retention policy with expiry set, ignoring expiry");
                }

                Retention::Discard
            }
            RetentionDef::Volatile => Retention::Volatile {
                expiry: self.expiry(),
            },
            RetentionDef::Stored => Retention::Stored {
                expiry: self.expiry(),
            },
        }
    }

    /// Returns the database retention of the data stream.
    ///
    /// See the [`DatabaseRetention`] for more information.
    pub fn database_retention(&self) -> DatabaseRetention {
        match self.database_retention_policy {
            DatabaseRetentionPolicyDef::NoTtl => {
                if self.database_retention_ttl.is_some() {
                    warn!("no_ttl retention policy with ttl set, ignoring ttl");
                }

                DatabaseRetention::NoTtl
            }
            DatabaseRetentionPolicyDef::UseTtl => {
                if self.database_retention_ttl.is_none() {
                    warn!("use_ttl retention policy without ttl set, using 0 as ttl");
                }

                let ttl = self
                    .database_retention_ttl
                    .and_then(|ttl| {
                        if ttl < 0 {
                            warn!("negative ttl, using 0");
                        }

                        ttl.try_into().ok()
                    })
                    .unwrap_or(0);

                DatabaseRetention::UseTtl {
                    ttl: Duration::from_secs(ttl),
                }
            }
        }
    }

    /// Returns whether the property's mapping can be unset.
    pub fn allow_unset(&self) -> bool {
        self.allow_unset
    }

    /// Returns whether the mapping should be sent with an explicit time stamp.
    pub fn explicit_timestamp(&self) -> bool {
        self.explicit_timestamp
    }

    #[cfg(feature = "interface-doc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "interface-doc")))]
    /// Returns the mapping's description.
    pub fn description(&self) -> Option<&T> {
        self.description.as_ref()
    }

    #[cfg(feature = "interface-doc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "interface-doc")))]
    /// Returns the mapping's documentation.
    pub fn doc(&self) -> Option<&T> {
        self.doc.as_ref()
    }
}

impl<'a> From<&'a Interface> for InterfaceDef<&'a str> {
    fn from(value: &'a Interface) -> Self {
        InterfaceDef {
            interface_name: value.interface_name(),
            version_major: value.version_major(),
            version_minor: value.version_minor(),
            interface_type: value.interface_type(),
            #[cfg(feature = "interface-doc")]
            description: value.description(),
            #[cfg(feature = "interface-doc")]
            doc: value.doc(),
            #[cfg(not(feature = "interface-doc"))]
            description: (),
            #[cfg(not(feature = "interface-doc"))]
            doc: (),
            ownership: value.ownership(),
            aggregation: value.aggregation(),
            mappings: value.iter_mappings().collect(),
        }
    }
}

impl<T> TryFrom<InterfaceDef<T>> for Interface
where
    T: AsRef<str> + Into<String>,
{
    type Error = InterfaceError;

    fn try_from(def: InterfaceDef<T>) -> Result<Self, Self::Error> {
        let inner = match def.interface_type {
            InterfaceTypeDef::Datastream => match def.aggregation {
                Aggregation::Individual => {
                    InterfaceType::DatastreamIndividual(DatastreamIndividual::try_from(&def)?)
                }
                Aggregation::Object => {
                    InterfaceType::DatastreamObject(DatastreamObject::try_from(&def)?)
                }
            },
            InterfaceTypeDef::Properties => InterfaceType::Properties(Properties::try_from(&def)?),
        };

        let interface = Interface {
            interface_name: def.interface_name.into(),
            version_major: def.version_major,
            version_minor: def.version_minor,
            ownership: def.ownership,
            #[cfg(feature = "interface-doc")]
            description: def.description.map(T::into),
            #[cfg(feature = "interface-doc")]
            doc: def.doc.map(T::into),
            inner,
        };

        interface.validate()?;

        Ok(interface)
    }
}

/// Type of an interface.
///
/// See [Interface Schema](https://docs.astarte-platform.org/latest/040-interface_schema.html#reference-astarte-interface-schema)
/// for more information.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceTypeDef {
    /// Stream of non persistent data.
    Datastream,
    /// Stateful value.
    Properties,
}

impl Display for InterfaceTypeDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InterfaceTypeDef::Datastream => write!(f, "datastream"),
            InterfaceTypeDef::Properties => write!(f, "properties"),
        }
    }
}

/// Ownership of an interface.
///
/// See [Interface Schema](https://docs.astarte-platform.org/latest/040-interface_schema.html#reference-astarte-interface-schema)
/// for more information.
#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Ownership {
    /// Data is sent from the device to Astarte.
    Device,
    /// Data is received from Astarte.
    Server,
}

impl Ownership {
    /// Returns `true` if the ownership is [`Device`].
    ///
    /// [`Device`]: Ownership::Device
    #[must_use]
    pub fn is_device(&self) -> bool {
        matches!(self, Self::Device)
    }

    /// Returns `true` if the ownership is [`Server`].
    ///
    /// [`Server`]: Ownership::Server
    #[must_use]
    pub fn is_server(&self) -> bool {
        matches!(self, Self::Server)
    }
}

impl Display for Ownership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ownership::Device => write!(f, "device"),
            Ownership::Server => write!(f, "server"),
        }
    }
}

/// Aggregation of interface's mappings.
///
/// See [Interface Schema](https://docs.astarte-platform.org/latest/040-interface_schema.html#reference-astarte-interface-schema)
/// for more information.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    /// Every mapping changes state or streams data independently.
    #[default]
    Individual,
    /// Send all the data for every mapping as a single object.
    Object,
}

impl Display for Aggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Aggregation::Individual => write!(f, "individual"),
            Aggregation::Object => write!(f, "object"),
        }
    }
}

/// Defines the type of the mapping.
///
/// See the [`AstarteType`](crate::AstarteType) for more information.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum MappingType {
    /// Double mapping.
    Double,
    /// Integer mapping.
    Integer,
    /// Boolean mapping.
    Boolean,
    /// Long integers mapping.
    LongInteger,
    /// String mapping.
    String,
    /// Binary mapping.
    BinaryBlob,
    /// Date time mapping.
    DateTime,
    /// Double array mapping.
    DoubleArray,
    /// Integer array mapping.
    IntegerArray,
    /// Boolean array mapping.
    BooleanArray,
    /// Long integer array mapping.
    LongIntegerArray,
    /// String array mapping.
    StringArray,
    /// Binary array mapping.
    BinaryBlobArray,
    /// Date time array mapping.
    DateTimeArray,
}

impl Display for MappingType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MappingType::Double => write!(f, "double"),
            MappingType::Integer => write!(f, "integer"),
            MappingType::Boolean => write!(f, "boolean"),
            MappingType::LongInteger => write!(f, "longinteger"),
            MappingType::String => write!(f, "string"),
            MappingType::BinaryBlob => write!(f, "binaryblob"),
            MappingType::DateTime => write!(f, "datetime"),
            MappingType::DoubleArray => write!(f, "doublearray"),
            MappingType::IntegerArray => write!(f, "integerarray"),
            MappingType::BooleanArray => write!(f, "booleanarray"),
            MappingType::LongIntegerArray => write!(f, "longintegerarray"),
            MappingType::StringArray => write!(f, "stringarray"),
            MappingType::BinaryBlobArray => write!(f, "binaryblobarray"),
            MappingType::DateTimeArray => write!(f, "datetimearray"),
        }
    }
}

/// Reliability of a data stream.
///
/// Defines whether the sent data should be considered delivered.
///
/// Properties have always a unique reliability.
///
/// See [Reliability](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html#astarte-mapping-schema-reliability)
/// for more information.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Copy, Clone, Default, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Reliability {
    /// If the transport sends the data
    #[default]
    Unreliable,
    /// When we know the data has been received at least once.
    Guaranteed,
    /// When we know the data has been received exactly once.
    Unique,
}

impl Reliability {
    /// Returns `true` if the reliability is [`Unreliable`].
    ///
    /// [`Unreliable`]: Reliability::Unreliable
    #[must_use]
    pub fn is_unreliable(&self) -> bool {
        matches!(self, Self::Unreliable)
    }
}

impl From<Reliability> for QoS {
    fn from(value: Reliability) -> Self {
        match value {
            Reliability::Unreliable => QoS::AtMostOnce,
            Reliability::Guaranteed => QoS::AtLeastOnce,
            Reliability::Unique => QoS::ExactlyOnce,
        }
    }
}

/// Defines the retention of a data stream.
///
/// Describes what to do with the sent data if the transport is incapable of delivering it.
///
/// See [Retention](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html#astarte-mapping-schema-retention)
/// for more information.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Copy, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub(super) enum RetentionDef {
    /// Data is discarded.
    #[default]
    Discard,
    /// Data is kept in a cache in memory.
    Volatile,
    /// Data is kept on disk.
    Stored,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Copy, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub(super) enum DatabaseRetentionPolicyDef {
    #[default]
    NoTtl,
    UseTtl,
}

#[cfg(not(feature = "interface-doc"))]
mod doc {
    use serde::{de::Visitor, Deserializer};
    use tracing::trace;

    pub(super) fn deserialize_doc<'de, D>(de: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_str(DocVisitor)
    }

    struct DocVisitor;

    impl<'de> Visitor<'de> for DocVisitor {
        type Value = ();

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            trace!("visited doc {v}");

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::test::{DEVICE_PROPERTIES, SERVER_PROPERTIES};

    use super::*;

    #[cfg(feature = "interface-strict")]
    #[test]
    fn should_be_strict() {
        let json = r#"{
            "interfaceS_name": "org.astarte-platform.genericproperties.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "properties",
            "ownArship": "server",
            "description": "Interface description \"escaped\"",
            "doc": "Interface doc \"escaped\"",
            "mappings": [{
                "endpoint": "/double_endpoint",
                "type": "double",
                "doc": "Mapping doc \"escaped\""
            }]
        }"#;

        serde_json::from_str::<InterfaceDef<String>>(json)
            .expect_err("should error for misspelled fields");
    }

    #[test]
    fn should_get_expiry() {
        let json = |expiry: i64| {
            format!(
                r#"{{
            "interface_name": "org.astarte-platform.genericproperties.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "properties",
            "ownership": "server",
            "mappings": [{{
                "endpoint": "/double_endpoint",
                "expiry": {expiry},
                "type": "double"
            }}]
        }}"#
            )
        };

        let i = serde_json::from_str::<InterfaceDef<String>>(&json(10)).unwrap();

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.expiry_as_i64(), 10);
        assert_eq!(mapping.expiry(), Some(Duration::from_secs(10)));

        let i = serde_json::from_str::<InterfaceDef<String>>(&json(-42)).unwrap();

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.expiry_as_i64(), -42);
        assert_eq!(mapping.expiry(), None);

        let i = serde_json::from_str::<InterfaceDef<String>>(&json(0)).unwrap();

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.expiry_as_i64(), 0);
        assert_eq!(mapping.expiry(), None);

        let i = serde_json::from_str::<InterfaceDef<String>>(&json(1)).unwrap();

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.expiry_as_i64(), 1);
        assert_eq!(mapping.expiry(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn should_get_retention() {
        let json = |ttl: i64| {
            serde_json::from_str::<InterfaceDef<String>>(&format!(
                r#"{{
            "interface_name": "org.astarte-platform.genericproperties.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "properties",
            "ownership": "server",
            "mappings": [{{
                "endpoint": "/double_endpoint",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": {ttl},
                "type": "double"
            }}]
        }}"#
            ))
            .unwrap()
        };

        let i = json(10);

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.database_retention_ttl, Some(10));
        assert_eq!(
            mapping.database_retention(),
            DatabaseRetention::UseTtl {
                ttl: Duration::from_secs(10)
            }
        );

        let i = json(0);

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.database_retention_ttl, Some(0));
        assert_eq!(
            mapping.database_retention(),
            DatabaseRetention::UseTtl {
                ttl: Duration::from_secs(0)
            }
        );

        let i = json(-32);

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.database_retention_ttl, Some(-32));
        assert_eq!(
            mapping.database_retention(),
            DatabaseRetention::UseTtl {
                ttl: Duration::from_secs(0)
            }
        );
    }

    #[test]
    fn should_check_ownership() {
        let server = Interface::from_str(SERVER_PROPERTIES).unwrap();
        assert!(!server.ownership().is_device());
        assert!(server.ownership().is_server());

        let device = Interface::from_str(DEVICE_PROPERTIES).unwrap();
        assert!(device.ownership().is_device());
        assert!(!device.ownership().is_server());
    }
}
