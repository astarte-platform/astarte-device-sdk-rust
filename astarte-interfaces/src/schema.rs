// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
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

//! Astarte Interface definition, this module contains the structs for the actual JSON
//! representation definition of the [`Interface`](crate::Interface) and mapping.
//!
//! For more information see:
//! [Interface Schema - Astarte](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html)

use std::{fmt::Display, time::Duration};

use cfg_if::cfg_if;
use serde::{Deserialize, Serialize};

use crate::interface::{
    DatabaseRetention as InterfaceDatabaseRetention, Retention as InterfaceRetention,
};

/// Error when validating an [`InterfaceJson`].
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum SchemaError {
    /// The expiry cannot be negative
    #[error("expiry cannot be negative {0}")]
    NegativeExpiry(i64),
    /// The database retention ttl cannot be negative
    #[error("database retention ttl cannot be negative {0}")]
    NegativeDatabaseRetentionTtl(i64),
    /// The database retention ttl must be greater than 60s
    #[error("database retention ttl must be greater than 60s, instead of {0}")]
    DatabaseRetentionTtlTooLow(u64),
    /// Missing database retention ttl with policy use_ttl
    #[error("database retention ttl is missing, but policy is use_ttl")]
    MissingDatabaseRetentionTtl,
    /// Database retention ttl is set to a non zero value, but database_retention_policy is `no_ttl`
    #[cfg(feature = "strict")]
    #[cfg_attr(docsrs, doc(cfg(feature = "strict")))]
    #[error("database_retention_ttl is set to {0}, but database_retention_policy is no_ttl")]
    DatabaseRetentionTtlWithNoTtl(i64),
    /// Expiry is set to a non zero value, but retention is discard
    #[cfg(feature = "strict")]
    #[cfg_attr(docsrs, doc(cfg(feature = "strict")))]
    #[error("expiry is set to {0}, but retention is discard")]
    ExpiryWithDiscard(i64),
}

fn is_none_or_empty<T>(value: &Option<T>) -> bool
where
    T: AsRef<str>,
{
    match value {
        Some(value) => value.as_ref().is_empty(),
        None => true,
    }
}

fn is_none_or_default<T>(value: &Option<T>) -> bool
where
    T: Default + Eq,
{
    match value {
        Some(value) => *value == T::default(),
        None => true,
    }
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
#[cfg_attr(feature = "strict", serde(deny_unknown_fields))]
pub struct InterfaceJson<T>
where
    T: AsRef<str>,
{
    /// The name of the interface.
    ///
    /// This has to be an unique, alphanumeric reverse internet domain
    /// name, shorter than 128 characters.
    pub interface_name: T,
    /// The Major version qualifier for this interface.
    ///
    /// Interfaces with the same id and different `version_major` number are deemed incompatible. It
    /// is then acceptable to redefine any property of the interface when changing the major
    /// version number.
    ///
    /// It must be a positive number.
    pub version_major: i32,
    /// The Minor version qualifier for this interface.
    ///
    /// Interfaces with the same id and major version number and different `version_minor` number are
    /// deemed compatible between each other. When changing the minor number, it is then only
    /// possible to insert further mappings. Any other modification might lead to
    /// incompatibilities and undefined behavior.
    ///
    /// It must be a positive number.
    pub version_minor: i32,
    /// Identifies the type of this Interface.
    ///
    /// Currently two types are supported: datastream and properties. Datastream should be used when
    /// dealing with streams of non-persistent data, where a single path receives updates and
    /// there's no concept of state. Properties, instead, are meant to be an actual state and as
    /// such they have only a change history, and are retained.
    #[serde(rename = "type")]
    pub interface_type: InterfaceType,
    /// Identifies the quality of the interface.
    ///
    /// Interfaces are meant to be unidirectional, and this property defines who's sending or
    /// receiving data. device means the device/gateway is sending data to Astarte, consumer
    /// means the device/gateway is receiving data from Astarte. Bidirectional mode is not
    /// supported, you should instantiate another interface for that.
    pub ownership: Ownership,
    /// Identifies the aggregation of the mappings of the interface.
    ///
    /// Individual means every mapping changes state or streams data independently, whereas an
    /// object aggregation treats the interface as an object, making all the mappings changes
    /// interdependent. Choosing the right aggregation might drastically improve performances.
    #[serde(default, skip_serializing_if = "is_none_or_default")]
    pub aggregation: Option<Aggregation>,
    /// An optional description of the interface.
    #[serde(default, skip_serializing_if = "is_none_or_empty")]
    pub description: Option<T>,
    /// A string containing documentation that will be injected in the generated client code.
    #[serde(default, skip_serializing_if = "is_none_or_empty")]
    pub doc: Option<T>,
    /// Mappings define the endpoint of the interface, where actual data is stored/streamed.
    ///
    /// They are defined as relative URLs (e.g. /my/path) and can be parametrized (e.g.:
    /// /%{myparam}/path). A valid interface must have no mappings clash, which means that every
    /// mapping must resolve to a unique path or collection of paths (including
    /// parametrization). Every mapping acquires type, quality and aggregation of the interface.
    pub mappings: Vec<Mapping<T>>,
}

/// Mapping of an Interface.
///
/// It includes all the fields available for a mapping, but it it is validated when built with the
/// [`TryFrom`]. It uniforms the different types of mappings like
/// [`DatastreamIndividualMapping`](crate::mapping::datastream::individual::DatastreamIndividualMapping),
/// [`DatastreamObjectMapping`](crate::mapping::datastream::object::DatastreamObjectMapping)
/// mappings and [`PropertiesMapping`](super::mapping::properties::PropertiesMapping) in a single
/// struct.
///
/// Since it's a 1:1 representation of the JSON it is used for serialization and deserialization,
/// and then is converted to the internal representation of the mapping with the [`TryFrom`] and
/// [`From`] traits of the [`Interface`](crate::Interface)'s mappings.
//
/// You can find the specification here [Mapping Schema -
/// Astarte](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html#mapping)
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "strict", serde(deny_unknown_fields))]
pub struct Mapping<T>
where
    T: AsRef<str>,
{
    /// Path of the mapping.
    ///
    /// It can be parametrized (e.g. `/foo/%{path}/baz`).
    pub endpoint: T,
    /// Defines the type of the mapping.
    ///
    /// This represent the data that will be published on the mapping.
    #[serde(rename = "type")]
    pub mapping_type: MappingType,
    /// Defines when to consider the data delivered.
    ///
    /// Useful only with datastream. Defines whether the sent data should be considered delivered
    /// when the transport successfully sends the data (unreliable), when we know that the data has
    /// been received at least once (guaranteed) or when we know that the data has been received
    /// exactly once (unique). Unreliable by default. When using reliable data, consider you might
    /// incur in additional resource usage on both the transport and the device's end.
    #[serde(default, skip_serializing_if = "is_none_or_default")]
    pub reliability: Option<Reliability>,
    /// Allow to set a custom timestamp.
    ///
    /// Otherwise a timestamp is added when the message is received. If true explicit timestamp will
    /// also be used for sorting. This feature is only supported on datastreams.
    #[serde(default, skip_serializing_if = "is_none_or_default")]
    pub explicit_timestamp: Option<bool>,
    /// Retention of the data when not deliverable.
    ///
    /// Useful only with datastream. Defines whether the sent data should be discarded if the
    /// transport is temporarily uncapable of delivering it (discard) or should be kept in a cache in
    /// memory (volatile) or on disk (stored), and guaranteed to be delivered in the timeframe
    /// defined by the expiry.
    #[serde(default, skip_serializing_if = "is_none_or_default")]
    pub retention: Option<Retention>,
    /// Expiry for the retain data.
    ///
    /// Useful when retention is stored. Defines after how many seconds a specific data entry should
    /// be kept before giving up and erasing it from the persistent cache. A value <= 0 means the
    /// persistent cache never expires, and is the default.
    #[serde(default, skip_serializing_if = "is_none_or_default")]
    pub expiry: Option<i64>,
    /// Retention policy for the database.
    ///
    /// Useful only with datastream. Defines whether data should expire from the database after a
    /// given interval. Valid values are: `no_ttl` and `use_ttl`.
    #[serde(default, skip_serializing_if = "is_none_or_default")]
    pub database_retention_policy: Option<DatabaseRetentionPolicy>,
    /// Seconds to keep the data in the database.
    ///
    /// Useful when `database_retention_policy` is "`use_ttl`". Defines how many seconds a specific data
    /// entry should be kept before erasing it from the database.
    #[serde(default, skip_serializing_if = "is_none_or_default")]
    pub database_retention_ttl: Option<i64>,
    /// Allows the property to be unset.
    ///
    /// Used only with properties.
    #[serde(default, skip_serializing_if = "is_none_or_default")]
    pub allow_unset: Option<bool>,
    /// An optional description of the mapping.
    #[serde(default, skip_serializing_if = "is_none_or_empty")]
    pub description: Option<T>,
    /// A string containing documentation that will be injected in the generated client code.
    #[serde(default, skip_serializing_if = "is_none_or_empty")]
    pub doc: Option<T>,
}

impl<T> Mapping<T>
where
    T: AsRef<str>,
{
    /// Expiry of the data stream.
    ///
    /// If it's [`None`] the stream will never expire.
    pub fn expiry_as_duration(&self) -> Result<Option<Duration>, SchemaError> {
        self.expiry
            .filter(|expiry| *expiry != 0)
            .map(|expiry| {
                u64::try_from(expiry)
                    .map(Duration::from_secs)
                    .map_err(|_| SchemaError::NegativeExpiry(expiry))
            })
            .transpose()
    }

    /// Retention of the data stream.
    ///
    /// See the [`Retention`] documentation for more information.
    pub fn retention_with_expiry(&self) -> Result<InterfaceRetention, SchemaError> {
        let retention = match self.retention.unwrap_or_default() {
            Retention::Discard => {
                if let Some(expiry) = self.expiry.filter(|exp| *exp > 0) {
                    cfg_if! {
                        if #[cfg(feature = "strict")] {
                            return Err(SchemaError::ExpiryWithDiscard(expiry));
                        } else {
                            tracing::warn!(expiry, "discard retention policy with expiry set, ignoring expiry");
                        }
                    }
                }

                InterfaceRetention::Discard
            }
            Retention::Volatile => InterfaceRetention::Volatile {
                expiry: self.expiry_as_duration()?,
            },
            Retention::Stored => InterfaceRetention::Stored {
                expiry: self.expiry_as_duration()?,
            },
        };

        Ok(retention)
    }

    /// Database retention ttl of the data stream.
    ///
    /// If it's [`None`] the stream will never expire.
    pub fn database_retention_ttl_as_duration(&self) -> Result<Option<Duration>, SchemaError> {
        let Some(ttl) = self.database_retention_ttl else {
            return Ok(None);
        };

        let ttl = u64::try_from(ttl).map_err(|_| SchemaError::NegativeDatabaseRetentionTtl(ttl))?;

        if ttl < 60 {
            return Err(SchemaError::DatabaseRetentionTtlTooLow(ttl));
        }

        Ok(Some(Duration::from_secs(ttl)))
    }

    /// Returns the database retention of the data stream.
    ///
    /// See the [`DatabaseRetention`](InterfaceDatabaseRetention) for more information.
    pub fn database_retention_with_ttl(&self) -> Result<InterfaceDatabaseRetention, SchemaError> {
        match self.database_retention_policy.unwrap_or_default() {
            DatabaseRetentionPolicy::NoTtl => {
                if let Some(ttl) = self.database_retention_ttl.filter(|ttl| *ttl > 0) {
                    cfg_if! {
                        if #[cfg(feature = "strict")] {
                            return Err(SchemaError::DatabaseRetentionTtlWithNoTtl(ttl))
                        } else {
                            tracing::warn!(ttl, "no_ttl retention policy with ttl set, ignoring ttl");
                        }
                    }
                }

                Ok(InterfaceDatabaseRetention::NoTtl)
            }
            DatabaseRetentionPolicy::UseTtl => {
                let ttl = self
                    .database_retention_ttl_as_duration()
                    .and_then(|opt| opt.ok_or(SchemaError::MissingDatabaseRetentionTtl))?;

                Ok(InterfaceDatabaseRetention::UseTtl { ttl })
            }
        }
    }
}

/// Type of an interface.
///
/// See [Interface Schema](https://docs.astarte-platform.org/latest/040-interface_schema.html#reference-astarte-interface-schema)
/// for more information.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceType {
    /// Stream of non persistent data.
    Datastream,
    /// Stateful value.
    Properties,
}

impl Display for InterfaceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InterfaceType::Datastream => write!(f, "datastream"),
            InterfaceType::Properties => write!(f, "properties"),
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

    /// Returns `true` if the reliability is [`Guaranteed`].
    ///
    /// [`Guaranteed`]: Reliability::Guaranteed
    #[must_use]
    pub fn is_guaranteed(&self) -> bool {
        matches!(self, Self::Guaranteed)
    }

    /// Returns `true` if the reliability is [`Unique`].
    ///
    /// [`Unique`]: Reliability::Unique
    #[must_use]
    pub fn is_unique(&self) -> bool {
        matches!(self, Self::Unique)
    }
}

impl Display for Reliability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reliability::Unreliable => write!(f, "unreliable"),
            Reliability::Guaranteed => write!(f, "guaranteed"),
            Reliability::Unique => write!(f, "unique"),
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
pub enum Retention {
    /// Data is discarded.
    #[default]
    Discard,
    /// Data is kept in a cache in memory.
    Volatile,
    /// Data is kept on disk.
    Stored,
}

impl Display for Retention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Retention::Discard => write!(f, "discard"),
            Retention::Volatile => write!(f, "volatile"),
            Retention::Stored => write!(f, "stored"),
        }
    }
}

/// Defines whether data should expire from the database after a given interval.
///
/// See
/// [Database Retention Policy](https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html#astarte-mapping-schema-database_retention_policy)
/// for more information.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Copy, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseRetentionPolicy {
    /// The data will never expiry.
    #[default]
    NoTtl,
    /// The data will expire after the ttl.
    ///
    /// The field [`database_retention_ttl`](Mapping::database_retention_ttl) will be used to
    /// determine how many seconds the data is kept in the database.
    UseTtl,
}

impl Display for DatabaseRetentionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseRetentionPolicy::NoTtl => write!(f, "no_ttl"),
            DatabaseRetentionPolicy::UseTtl => write!(f, "use_ttl"),
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[cfg(feature = "strict")]
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

        serde_json::from_str::<InterfaceJson<String>>(json)
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

        let i = serde_json::from_str::<InterfaceJson<String>>(&json(10)).unwrap();

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.expiry, Some(10));
        assert_eq!(
            mapping.expiry_as_duration().unwrap(),
            Some(Duration::from_secs(10))
        );

        let i: InterfaceJson<String> = serde_json::from_str(&json(-42)).unwrap();

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.expiry, Some(-42));
        assert!(matches!(
            mapping.expiry_as_duration().unwrap_err(),
            SchemaError::NegativeExpiry(-42)
        ));

        let i: InterfaceJson<String> = serde_json::from_str(&json(0)).unwrap();

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.expiry, Some(0));
        assert_eq!(mapping.expiry_as_duration().unwrap(), None);

        let i: InterfaceJson<String> = serde_json::from_str(&json(1)).unwrap();

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.expiry, Some(1));
        assert_eq!(
            mapping.expiry_as_duration().unwrap(),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn should_get_retention() {
        let json = |ttl: i64| {
            serde_json::from_str::<InterfaceJson<String>>(&format!(
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

        let i = json(60);

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.database_retention_ttl, Some(60));
        assert_eq!(
            mapping.database_retention_with_ttl().unwrap(),
            InterfaceDatabaseRetention::UseTtl {
                ttl: Duration::from_secs(60)
            }
        );

        let i = json(0);

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.database_retention_ttl, Some(0));
        assert!(matches!(
            mapping.database_retention_with_ttl().unwrap_err(),
            SchemaError::DatabaseRetentionTtlTooLow(0)
        ));

        let i = json(-32);

        let mapping = i.mappings.first().unwrap();

        assert_eq!(mapping.database_retention_ttl, Some(-32));
        assert!(matches!(
            mapping.database_retention_with_ttl().unwrap_err(),
            SchemaError::NegativeDatabaseRetentionTtl(-32)
        ));
    }

    #[test]
    fn retention_and_expiry() {
        let mut mapping = Mapping {
            endpoint: "/some/path",
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: None,
            retention: Some(Retention::Discard),
            expiry: Some(420),
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: None,
            description: None,
            doc: None,
        };

        cfg_if! {
            if #[cfg(feature = "strict")] {
                let err = mapping.retention_with_expiry().unwrap_err();
                assert!(matches!(err, SchemaError::ExpiryWithDiscard(420)));
            } else {
                let retention = mapping.retention_with_expiry().unwrap();
                assert_eq!(retention, InterfaceRetention::Discard);
           }
        }

        mapping.retention = Some(Retention::Volatile);

        let exp = InterfaceRetention::Volatile {
            expiry: Some(Duration::from_secs(420)),
        };
        assert_eq!(mapping.retention_with_expiry().unwrap(), exp);

        mapping.retention = Some(Retention::Stored);

        let exp = InterfaceRetention::Stored {
            expiry: Some(Duration::from_secs(420)),
        };
        assert_eq!(mapping.retention_with_expiry().unwrap(), exp);

        mapping.expiry = None;

        let exp = InterfaceRetention::Stored { expiry: None };
        assert_eq!(mapping.retention_with_expiry().unwrap(), exp);
    }

    #[test]
    fn database_retention_ttl() {
        let mut mapping = Mapping {
            endpoint: "/some/path",
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: Some(DatabaseRetentionPolicy::NoTtl),
            database_retention_ttl: Some(420),
            allow_unset: None,
            description: None,
            doc: None,
        };

        cfg_if! {
            if #[cfg(feature = "strict")] {
                let err = mapping.database_retention_with_ttl().unwrap_err();
                assert!(matches!(err, SchemaError::DatabaseRetentionTtlWithNoTtl(420)));
            } else {
                let retention = mapping.database_retention_with_ttl().unwrap();
                assert_eq!(retention, InterfaceDatabaseRetention::NoTtl);
           }
        }

        mapping.database_retention_policy = Some(DatabaseRetentionPolicy::UseTtl);

        let exp = InterfaceDatabaseRetention::UseTtl {
            ttl: Duration::from_secs(420),
        };
        assert_eq!(mapping.database_retention_with_ttl().unwrap(), exp);

        mapping.database_retention_ttl = None;

        assert_eq!(mapping.database_retention_ttl_as_duration().unwrap(), None);
    }

    #[test]
    fn interface_type_functions() {
        assert_eq!(InterfaceType::Datastream.to_string(), "datastream");
        assert_eq!(InterfaceType::Properties.to_string(), "properties");
    }

    #[test]
    fn ownership_functions() {
        assert_eq!(Ownership::Device.to_string(), "device");
        assert_eq!(Ownership::Server.to_string(), "server");
        assert!(Ownership::Server.is_server());
        assert!(!Ownership::Device.is_server());
        assert!(Ownership::Device.is_device());
        assert!(!Ownership::Server.is_device());
    }

    #[test]
    fn aggregation_functions() {
        assert_eq!(Aggregation::Individual.to_string(), "individual");
        assert_eq!(Aggregation::Object.to_string(), "object");
    }

    #[test]
    fn mapping_type_functions() {
        assert_eq!(MappingType::Double.to_string(), "double");
        assert_eq!(MappingType::Integer.to_string(), "integer");
        assert_eq!(MappingType::Boolean.to_string(), "boolean");
        assert_eq!(MappingType::LongInteger.to_string(), "longinteger");
        assert_eq!(MappingType::String.to_string(), "string");
        assert_eq!(MappingType::BinaryBlob.to_string(), "binaryblob");
        assert_eq!(MappingType::DateTime.to_string(), "datetime");
        assert_eq!(MappingType::DoubleArray.to_string(), "doublearray");
        assert_eq!(MappingType::IntegerArray.to_string(), "integerarray");
        assert_eq!(MappingType::BooleanArray.to_string(), "booleanarray");
        assert_eq!(
            MappingType::LongIntegerArray.to_string(),
            "longintegerarray"
        );
        assert_eq!(MappingType::StringArray.to_string(), "stringarray");
        assert_eq!(MappingType::BinaryBlobArray.to_string(), "binaryblobarray");
        assert_eq!(MappingType::DateTimeArray.to_string(), "datetimearray");
    }

    #[test]
    fn reliability_functions() {
        assert_eq!(Reliability::Unreliable.to_string(), "unreliable");
        assert_eq!(Reliability::Guaranteed.to_string(), "guaranteed");
        assert_eq!(Reliability::Unique.to_string(), "unique");
        assert!(Reliability::Unreliable.is_unreliable());
        assert!(Reliability::Guaranteed.is_guaranteed());
        assert!(Reliability::Unique.is_unique());
    }

    #[test]
    fn retention_functions() {
        assert_eq!(Retention::Discard.to_string(), "discard");
        assert_eq!(Retention::Volatile.to_string(), "volatile");
        assert_eq!(Retention::Stored.to_string(), "stored");
    }

    #[test]
    fn database_retention_policy_functions() {
        assert_eq!(DatabaseRetentionPolicy::NoTtl.to_string(), "no_ttl");
        assert_eq!(DatabaseRetentionPolicy::UseTtl.to_string(), "use_ttl");
    }
}
