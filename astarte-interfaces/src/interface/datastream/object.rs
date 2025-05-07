// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

//! Datastream with object aggregation
//!
//! Data sent on an object interface is grouped and sent together in a single message.

use std::{borrow::Cow, fmt::Display, str::FromStr};

use cfg_if::cfg_if;
use serde::Serialize;

use crate::{
    error::Error,
    interface::{name::InterfaceName, version::InterfaceVersion, MappingVec, Retention, Schema},
    mapping::{
        datastream::object::DatastreamObjectMapping, path::MappingPath, InterfaceMapping,
        MappingError,
    },
    schema::{Aggregation, InterfaceJson, InterfaceType, Mapping, Ownership, Reliability},
};

/// Error when parsing a [`DatastreamObject`]
#[derive(Debug, thiserror::Error)]
pub enum ObjectError {
    /// Object has a different value for the specified mapping
    #[error("object has a different {ctx} for the mapping {endpoint}")]
    Mapping {
        /// The value that is different
        ctx: &'static str,
        /// Endpoint of the mapping that is different.
        endpoint: String,
    },
    /// Mapping endpoint differs from others
    ///
    /// It needs to have up to the latest level equal to the others endpoints.
    ///
    /// See [the Astarte documentation](https://docs.astarte-platform.org/astarte/latest/030-interface.html#endpoints-and-aggregation)
    #[error("object has an inconsistent endpoint {endpoint}")]
    Endpoint {
        /// Endpoint that is inconsistent.
        endpoint: String,
    },
}

impl ObjectError {
    fn mapping(ctx: &'static str, endpoint: impl AsRef<str>) -> Self {
        Self::Mapping {
            ctx,
            endpoint: endpoint.as_ref().to_string(),
        }
    }
}

/// Interface of type datastream object.
///
/// For this interface all the mappings have the same prefix and configurations.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatastreamObject {
    name: InterfaceName,
    version: InterfaceVersion,
    ownership: Ownership,
    reliability: Reliability,
    explicit_timestamp: bool,
    retention: Retention,
    mappings: MappingVec<DatastreamObjectMapping>,
    #[cfg(feature = "server-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-fields")))]
    database_retention: crate::interface::DatabaseRetention,
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    description: Option<String>,
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    doc: Option<String>,
}

impl DatastreamObject {
    /// Return the reliability for the object.
    #[must_use]
    pub fn reliability(&self) -> Reliability {
        self.reliability
    }

    /// Return true if the object requires an explicit timestamp.
    ///
    /// Otherwise the reception timestamp is used.
    #[must_use]
    pub fn explicit_timestamp(&self) -> bool {
        self.explicit_timestamp
    }

    /// Returns the retention for the object.
    #[must_use]
    pub fn retention(&self) -> Retention {
        self.retention
    }
    /// Returns the database retention for the object.
    #[cfg(feature = "server-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-fields")))]
    #[must_use]
    pub fn database_retention(&self) -> crate::interface::DatabaseRetention {
        self.database_retention
    }

    /// Check if the path if the correct one for this object interface.
    #[must_use]
    pub fn is_object_path(&self, path: &MappingPath<'_>) -> bool {
        let Some(mapping) = self.mappings.iter().next() else {
            unreachable!("objects must have at least one mapping")
        };

        mapping.endpoint().is_object_path(path)
    }

    /// Get a mapping for in the object for the given field.
    ///
    /// The field is the last level of an endpoint.
    #[must_use]
    pub fn mapping(&self, path: &str) -> Option<&DatastreamObjectMapping> {
        self.mappings
            .iter()
            .find(|mapping| mapping.endpoint.eq_object_field(path))
    }
}

impl Schema for DatastreamObject {
    type Mapping = DatastreamObjectMapping;

    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn interface_name(&self) -> &InterfaceName {
        &self.name
    }

    fn version_major(&self) -> i32 {
        self.version.version_major()
    }

    fn version_minor(&self) -> i32 {
        self.version.version_minor()
    }

    fn version(&self) -> InterfaceVersion {
        self.version
    }

    fn interface_type(&self) -> InterfaceType {
        InterfaceType::Datastream
    }

    fn ownership(&self) -> Ownership {
        self.ownership
    }

    fn aggregation(&self) -> Aggregation {
        Aggregation::Object
    }

    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }

    fn iter_mappings(&self) -> impl Iterator<Item = &Self::Mapping> {
        self.mappings.iter()
    }

    fn mappings_len(&self) -> usize {
        self.mappings.len()
    }

    fn iter_interface_mappings(&self) -> impl Iterator<Item = Mapping<Cow<'_, str>>> {
        self.iter_mappings().map(|mapping| {
            cfg_if! {
                if #[cfg(feature = "doc-fields")] {
                    let description = self.description().map(Cow::Borrowed);
                    let doc = self.doc().map(Cow::Borrowed);
                } else {
                    let description = None;
                    let doc = None;
                }
            }

            cfg_if! {
                if #[cfg(feature = "server-fields")] {
                    let database_retention_policy = Some(self.database_retention.into());
                    let database_retention_ttl = self.database_retention.as_ttl_secs();
                } else {
                    let database_retention_policy = None;
                    let database_retention_ttl = None;
                }
            }

            Mapping {
                endpoint: mapping.endpoint().to_string().into(),
                mapping_type: mapping.mapping_type(),
                reliability: Some(self.reliability),
                explicit_timestamp: Some(self.explicit_timestamp),
                retention: Some(self.retention.into()),
                expiry: self.retention.as_expiry_seconds(),
                allow_unset: None,
                database_retention_policy,
                database_retention_ttl,
                description,
                doc,
            }
        })
    }
}

impl Display for DatastreamObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name, self.version)
    }
}

impl<T> TryFrom<InterfaceJson<T>> for DatastreamObject
where
    T: AsRef<str> + Into<String>,
{
    type Error = Error;

    fn try_from(value: InterfaceJson<T>) -> Result<Self, Self::Error> {
        let name = InterfaceName::from_str_ref(value.interface_name)?;
        let version = InterfaceVersion::try_new(value.version_major, value.version_minor)?;

        // Check the compatibility
        let first = value.mappings.first().ok_or(MappingError::Empty)?;

        value
            .mappings
            .iter()
            .skip(1)
            .try_for_each(|mapping| are_mapping_compatible(first, mapping))?;

        // Get the variables
        let reliability = first.reliability.unwrap_or_default();
        let explicit_timestamp = first.explicit_timestamp.unwrap_or_default();
        let retention = first.retention_with_expiry()?;
        #[cfg(feature = "server-fields")]
        let database_retention = first.database_retention_with_ttl()?;

        // Convert the mappings
        let mut iter = value.mappings.into_iter();
        let first = iter
            .next()
            .ok_or(MappingError::Empty)
            .and_then(DatastreamObjectMapping::try_from)?;

        let mut mappings = iter
            .map(|mapping| {
                let mapping = DatastreamObjectMapping::try_from(mapping)?;

                if !mapping.endpoint().is_same_object(first.endpoint()) {
                    return Err(Error::Object(ObjectError::Endpoint {
                        endpoint: mapping.endpoint().to_string(),
                    }));
                }

                Ok(mapping)
            })
            .collect::<Result<Vec<_>, Error>>()?;

        // Push the first
        mappings.push(first);

        let mappings = MappingVec::try_from(mappings)?;

        Ok(Self {
            name: name.into_string(),
            version,
            ownership: value.ownership,
            reliability,
            explicit_timestamp,
            retention,
            #[cfg(feature = "server-fields")]
            database_retention,
            mappings,
            #[cfg(feature = "doc-fields")]
            description: value.description.map(T::into),
            #[cfg(feature = "doc-fields")]
            doc: value.doc.map(T::into),
        })
    }
}

fn are_mapping_compatible<T>(a: &Mapping<T>, b: &Mapping<T>) -> Result<(), ObjectError>
where
    T: AsRef<str>,
{
    if a.reliability != b.reliability {
        return Err(ObjectError::mapping("reliability", &b.endpoint));
    }

    if a.explicit_timestamp != b.explicit_timestamp {
        return Err(ObjectError::mapping("explicit_timestamp", &b.endpoint));
    }

    if a.retention != b.retention {
        return Err(ObjectError::mapping("retention", &b.endpoint));
    }

    if a.expiry != b.expiry {
        return Err(ObjectError::mapping("expiry", &b.endpoint));
    }

    #[cfg(feature = "server-fields")]
    {
        if a.database_retention_policy != b.database_retention_policy {
            return Err(ObjectError::mapping(
                "database_retention_policy",
                &b.endpoint,
            ));
        }

        if a.database_retention_ttl != b.database_retention_ttl {
            return Err(ObjectError::mapping("database_retention_ttl", &b.endpoint));
        }
    }

    Ok(())
}

impl Serialize for DatastreamObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        InterfaceJson::from(self).serialize(serializer)
    }
}

impl FromStr for DatastreamObject {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let interface: InterfaceJson<Cow<str>> = serde_json::from_str(s)?;

        Self::try_from(interface)
    }
}
