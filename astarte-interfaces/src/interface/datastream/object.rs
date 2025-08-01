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
                    let description = mapping.description().map(Cow::Borrowed);
                    let doc = mapping.doc().map(Cow::Borrowed);
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
        if value.interface_type != InterfaceType::Datastream
            || value.aggregation != Some(Aggregation::Object)
        {
            return Err(Error::InterfaceConversion {
                exp_type: InterfaceType::Datastream,
                exp_aggregation: Aggregation::Object,
                got_type: value.interface_type,
                got_aggregation: value.aggregation.unwrap_or_default(),
            });
        }

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

        let mappings = value
            .mappings
            .into_iter()
            .map(|mapping| DatastreamObjectMapping::try_from(mapping).map_err(Error::Mapping))
            .collect::<Result<Vec<_>, Error>>()?;

        let first = mappings.first().ok_or(MappingError::Empty)?;

        mappings.iter().skip(1).try_for_each(|other| {
            if !first.endpoint().is_same_object(other.endpoint()) {
                return Err(Error::Object(ObjectError::Endpoint {
                    endpoint: other.endpoint().to_string(),
                }));
            }
            Ok(())
        })?;

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pretty_assertions::assert_eq;

    use crate::interface::tests::E2E_DEVICE_AGGREGATE;
    use crate::schema::MappingType;
    use crate::Endpoint;

    use super::*;

    #[test]
    fn should_parse_str() {
        let object = DatastreamObject::from_str(
            r#"{
                "interface_name": "com.example.Example",
                "version_major": 0,
                "version_minor": 1,
                "type": "datastream",
                "aggregation": "object",
                "ownership": "server",
                "description": "The description of the\tinterface",
                "doc": "The documentation of the\tinterface",
                "mappings": [{
                    "endpoint": "/prefix/path",
                    "type": "boolean",
                    "reliability": "unique",
                    "explicit_timestamp": true,
                    "retention": "stored",
                    "expiry": 30,
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 420,
                    "description": "The description of the\tmapping",
                    "doc": "The documentation of the\tmapping"
                }]
            }"#,
        )
        .unwrap();

        let exp_mapping = DatastreamObjectMapping {
            endpoint: Endpoint::try_from("/prefix/path").unwrap(),
            mapping_type: MappingType::Boolean,
            #[cfg(feature = "doc-fields")]
            description: Some("The description of the\tmapping".to_string()),
            #[cfg(feature = "doc-fields")]
            doc: Some("The documentation of the\tmapping".to_string()),
        };

        let exp = DatastreamObject {
            name: InterfaceName::try_from("com.example.Example".to_string()).unwrap(),
            version: InterfaceVersion::try_new(0, 1).unwrap(),
            ownership: Ownership::Server,
            reliability: Reliability::Unique,
            explicit_timestamp: true,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            mappings: MappingVec::try_from(vec![exp_mapping.clone()]).unwrap(),
            #[cfg(feature = "server-fields")]
            database_retention: crate::interface::DatabaseRetention::UseTtl {
                ttl: Duration::from_secs(420),
            },
            #[cfg(feature = "doc-fields")]
            description: Some("The description of the\tinterface".to_string()),
            #[cfg(feature = "doc-fields")]
            doc: Some("The documentation of the\tinterface".to_string()),
        };

        assert_eq!(object, exp);

        // Just for coverage
        assert_eq!(object.name(), object.name.as_str());
        assert_eq!(*object.interface_name(), object.name);
        assert_eq!(object.version(), object.version);
        assert_eq!(object.version_major(), object.version.version_major());
        assert_eq!(object.version_minor(), object.version.version_minor());
        assert_eq!(object.ownership(), object.ownership);
        assert_eq!(object.retention(), object.retention);
        assert_eq!(object.reliability(), object.reliability);
        assert_eq!(object.explicit_timestamp(), object.explicit_timestamp);
        assert_eq!(object.interface_type(), InterfaceType::Datastream);
        assert_eq!(object.aggregation(), Aggregation::Object);
        #[cfg(feature = "server-fields")]
        assert_eq!(object.database_retention(), object.database_retention);
        #[cfg(feature = "doc-fields")]
        {
            assert_eq!(object.doc(), object.doc.as_deref());
            assert_eq!(object.description(), object.description.as_deref());
        }

        let path = MappingPath::try_from("/prefix").unwrap();
        assert!(object.is_object_path(&path));

        assert_eq!(*object.mapping("path").unwrap(), exp_mapping);

        let mapping = object.iter_mappings().next().unwrap();
        assert_eq!(*mapping, exp_mapping);

        let exp_interface_mapping = Mapping::<Cow<'_, str>> {
            endpoint: mapping.endpoint.to_string().into(),
            mapping_type: mapping.mapping_type,
            reliability: object.reliability.into(),
            explicit_timestamp: Some(object.explicit_timestamp),
            retention: Some(object.retention.into()),
            expiry: object.retention.as_expiry_seconds(),
            allow_unset: None,
            #[cfg(feature = "doc-fields")]
            description: exp_mapping.description.as_ref().map(Cow::from),
            #[cfg(feature = "doc-fields")]
            doc: exp_mapping.doc.as_ref().map(Cow::from),
            #[cfg(not(feature = "doc-fields"))]
            description: None,
            #[cfg(not(feature = "doc-fields"))]
            doc: None,
            #[cfg(feature = "server-fields")]
            database_retention_policy: Some(object.database_retention.into()),
            #[cfg(feature = "server-fields")]
            database_retention_ttl: object.database_retention.as_ttl_secs(),
            #[cfg(not(feature = "server-fields"))]
            database_retention_policy: None,
            #[cfg(not(feature = "server-fields"))]
            database_retention_ttl: None,
        };
        assert_eq!(
            object.iter_interface_mappings().next().unwrap(),
            exp_interface_mapping
        );

        assert_eq!(object.to_string(), format!("{}:{}", exp.name, exp.version));
    }

    #[test]
    fn should_maintain_mapping_order_serde() {
        let original = DatastreamObject::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized = DatastreamObject::from_str(&serialized).unwrap();

        assert_eq!(deserialized, original);
    }

    #[test]
    fn should_check_same_object_endpoint() {
        let err = DatastreamObject::from_str(
            r#"{
                "interface_name": "com.example.Example",
                "version_major": 0,
                "version_minor": 1,
                "type": "datastream",
                "ownership": "server",
                "aggregation": "object",
                "mappings": [{
                    "endpoint": "/prefix/path",
                    "type": "boolean"
                },{
                    "endpoint": "/wrong/path",
                    "type": "boolean"
                }]
            }"#,
        )
        .unwrap_err();

        assert!(
            matches!(
            &err,
            Error::Object(ObjectError::Endpoint { endpoint } ) if endpoint == "/wrong/path"),
            "{err:?}"
        );
    }
}
