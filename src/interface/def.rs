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

use std::fmt::Display;

use log::warn;
use rumqttc::QoS;
use serde::{Deserialize, Serialize};

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

/// Utility to check a i32 is equal to 0.
fn is_zero(value: &i32) -> bool {
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
pub(super) struct InterfaceDef<'a> {
    pub(super) interface_name: &'a str,
    pub(super) version_major: i32,
    pub(super) version_minor: i32,
    #[serde(rename = "type")]
    pub(super) interface_type: InterfaceTypeDef,
    pub(super) ownership: Ownership,
    #[serde(default, skip_serializing_if = "is_default")]
    pub(super) aggregation: Aggregation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) description: Option<&'a str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) doc: Option<&'a str>,
    pub(super) mappings: Vec<Mapping<'a>>,
}

/// Represents, the JSON of a mapping. It includes all the fields available for a mapping, but it
/// it is validated when built with the [`TryFrom`]. It uniforms the different types of mappings
/// like [`super::mapping::DatastreamIndividualMapping`], [`DatastreamObject`] mappings and
/// [`super::mapping::PropertiesMapping`] in a single struct.
///
/// Since it's a 1:1 representation of the JSON it is used for serialization and deserialization,
/// and then is converted to the internal representation of the mapping with the [`TryFrom`] and
/// [`From`] traits of the [`InterfaceDef`].
//
/// You can find the specification here
/// [Mapping Schema - Astarte](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html#mapping)
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub(crate) struct Mapping<'a> {
    pub(super) endpoint: &'a str,
    #[serde(rename = "type")]
    pub(super) mapping_type: MappingType,
    #[serde(default, skip_serializing_if = "is_default")]
    pub(super) reliability: Reliability,
    #[serde(default, skip_serializing_if = "is_default")]
    pub(super) retention: RetentionDef,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(super) expiry: i32,
    #[serde(default, skip_serializing_if = "is_default")]
    pub(super) database_retention_policy: DatabaseRetentionPolicyDef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) database_retention_ttl: Option<i32>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) allow_unset: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) explicit_timestamp: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) description: Option<&'a str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) doc: Option<&'a str>,
}

impl<'a> Mapping<'a> {
    pub(super) fn new(endpoint: &'a str, mapping_type: MappingType) -> Self {
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
            description: None,
            doc: None,
        }
    }

    pub(crate) fn with_description(mut self, description: Option<&'a str>) -> Self {
        self.description = description;

        self
    }

    pub(crate) fn with_doc(mut self, doc: Option<&'a str>) -> Self {
        self.doc = doc;

        self
    }

    pub(crate) fn endpoint(&self) -> &str {
        self.endpoint
    }

    pub(crate) fn mapping_type(&self) -> MappingType {
        self.mapping_type
    }

    pub(crate) fn reliability(&self) -> Reliability {
        self.reliability
    }

    pub(crate) fn retention(&self) -> Retention {
        match self.retention {
            RetentionDef::Discard => {
                if self.expiry >= 0 {
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

    pub(crate) fn expiry(&self) -> i32 {
        self.expiry
    }

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

                DatabaseRetention::UseTtl {
                    ttl: self.database_retention_ttl.unwrap_or(0),
                }
            }
        }
    }

    pub fn allow_unset(&self) -> bool {
        self.allow_unset
    }

    pub fn explicit_timestamp(&self) -> bool {
        self.explicit_timestamp
    }

    pub fn description(&self) -> Option<&str> {
        self.description
    }

    pub fn doc(&self) -> Option<&str> {
        self.doc
    }
}

impl<'a> From<&'a Interface> for InterfaceDef<'a> {
    fn from(value: &'a Interface) -> Self {
        InterfaceDef {
            interface_name: value.interface_name(),
            version_major: value.version_major(),
            version_minor: value.version_minor(),
            interface_type: value.interface_type(),
            description: value.description(),
            doc: value.doc(),
            ownership: value.ownership(),
            aggregation: value.aggregation(),
            mappings: value.iter_mappings().collect(),
        }
    }
}

impl<'a> TryFrom<InterfaceDef<'a>> for Interface {
    type Error = InterfaceError;

    fn try_from(def: InterfaceDef<'a>) -> Result<Self, Self::Error> {
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
            interface_name: def.interface_name.to_string(),
            version_major: def.version_major,
            version_minor: def.version_minor,
            ownership: def.ownership,
            description: def.description.map(str::to_string),
            doc: def.doc.map(str::to_string),
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
    Datastream,
    Properties,
}

/// Ownership of an interface.
///
/// See [Interface Schema](https://docs.astarte-platform.org/latest/040-interface_schema.html#reference-astarte-interface-schema)
/// for more information.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Ownership {
    Device,
    Server,
}

/// Aggregation of interface's mappings.
///
/// See [Interface Schema](https://docs.astarte-platform.org/latest/040-interface_schema.html#reference-astarte-interface-schema)
/// for more information.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    #[default]
    Individual,
    Object,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MappingType {
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Copy, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Reliability {
    #[default]
    Unreliable,
    Guaranteed,
    Unique,
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Copy, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub(super) enum RetentionDef {
    #[default]
    Discard,
    Volatile,
    Stored,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Copy, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub(super) enum DatabaseRetentionPolicyDef {
    #[default]
    NoTtl,
    UseTtl,
}
