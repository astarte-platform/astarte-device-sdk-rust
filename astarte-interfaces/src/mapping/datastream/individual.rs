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

//! Mapping for Datastream with aggregation individual.
//!
//! In case aggregation is individual, each mapping is treated as an independent value and is
//! managed individually.

use std::borrow::Cow;

use cfg_if::cfg_if;

use crate::{
    interface::Retention,
    mapping::{endpoint::Endpoint, invalid_filed, InterfaceMapping, MappingError},
    schema::{Mapping, MappingType, Reliability},
};

/// Mapping of a [`DatastreamIndividual`](crate::DatastreamIndividual) interface.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatastreamIndividualMapping {
    pub(crate) endpoint: Endpoint<String>,
    pub(crate) mapping_type: MappingType,
    pub(crate) reliability: Reliability,
    pub(crate) retention: Retention,
    pub(crate) explicit_timestamp: bool,
    #[cfg(feature = "server-fields")]
    pub(crate) database_retention: crate::interface::DatabaseRetention,
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    pub(crate) description: Option<String>,
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    pub(crate) doc: Option<String>,
}

impl DatastreamIndividualMapping {
    /// Returns the [`Reliability`] of the mapping.
    #[must_use]
    pub fn reliability(&self) -> Reliability {
        self.reliability
    }

    /// Returns the [`Retention`] of the mapping.
    #[must_use]
    pub fn retention(&self) -> Retention {
        self.retention
    }

    /// Returns the [`DatabaseRetention`](crate::interface::DatabaseRetention) of the mapping.
    #[must_use]
    #[cfg(feature = "server-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-fields")))]
    pub fn database_retention(&self) -> crate::interface::DatabaseRetention {
        self.database_retention
    }

    /// Returns true if the mapping requires an explicit timestamp.
    ///
    /// Otherwise the reception timestamp is used.
    #[must_use]
    pub fn explicit_timestamp(&self) -> bool {
        self.explicit_timestamp
    }
}

impl InterfaceMapping for DatastreamIndividualMapping {
    fn endpoint(&self) -> &Endpoint<String> {
        &self.endpoint
    }

    fn mapping_type(&self) -> MappingType {
        self.mapping_type
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
}

impl<T> TryFrom<Mapping<T>> for DatastreamIndividualMapping
where
    T: AsRef<str> + Into<String>,
{
    type Error = MappingError;

    fn try_from(value: Mapping<T>) -> Result<Self, Self::Error> {
        let endpoint = Endpoint::try_from(value.endpoint.as_ref())?;
        let retention = value.retention_with_expiry()?;
        #[cfg(feature = "server-fields")]
        let database_retention = value.database_retention_with_ttl()?;

        if value.allow_unset.is_some() {
            invalid_filed!(datastream, "allow_unset");
        }

        Ok(Self {
            endpoint,
            reliability: value.reliability.unwrap_or_default(),
            retention,
            explicit_timestamp: value.explicit_timestamp.unwrap_or_default(),
            mapping_type: value.mapping_type,
            #[cfg(feature = "server-fields")]
            database_retention,
            #[cfg(feature = "doc-fields")]
            description: value.description.map(T::into),
            #[cfg(feature = "doc-fields")]
            doc: value.doc.map(T::into),
        })
    }
}

impl<'a> From<&'a DatastreamIndividualMapping> for Mapping<Cow<'a, str>> {
    fn from(value: &'a DatastreamIndividualMapping) -> Self {
        cfg_if! {
            if #[cfg(feature = "doc-fields")] {
                let description = value.description().map(Cow::Borrowed);
                let doc = value.doc().map(Cow::Borrowed);
            } else {
                let description = None;
                let doc = None;
            }
        }

        cfg_if! {
            if #[cfg(feature = "server-fields")] {
                let database_retention_policy = Some(value.database_retention.into());
                let database_retention_ttl = value.database_retention.as_ttl_secs();
            } else {
                let database_retention_policy = None;
                let database_retention_ttl = None;
            }
        }

        Mapping {
            endpoint: value.endpoint.to_string().into(),
            mapping_type: value.mapping_type,
            reliability: value.reliability.into(),
            explicit_timestamp: Some(value.explicit_timestamp),
            retention: Some(value.retention.into()),
            expiry: value.retention.as_expiry_seconds(),
            allow_unset: None,
            database_retention_policy,
            database_retention_ttl,
            description,
            doc,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::schema::{self};

    use super::*;

    #[test]
    fn getters_success() {
        let mapping_type = MappingType::Boolean;
        let reliability = Reliability::Guaranteed;
        let retention_expiry = Duration::from_secs(420);
        let database_ttl = Duration::from_secs(360);
        let description = Some("Individual mapping description");
        let doc = Some("Individual mapping doc");
        let mapping = Mapping {
            endpoint: "/individual/path",
            mapping_type,
            reliability: Some(reliability),
            explicit_timestamp: Some(true),
            retention: Some(schema::Retention::Stored),
            expiry: Some(retention_expiry.as_secs().try_into().unwrap()),
            database_retention_policy: Some(schema::DatabaseRetentionPolicy::UseTtl),
            database_retention_ttl: Some(database_ttl.as_secs().try_into().unwrap()),
            allow_unset: None,
            description,
            doc,
        };

        let individual_mapping = DatastreamIndividualMapping::try_from(mapping).unwrap();

        let exp = Endpoint::try_from("/individual/path").unwrap();
        assert_eq!(*individual_mapping.endpoint(), exp);
        assert_eq!(individual_mapping.mapping_type(), mapping_type);
        assert_eq!(individual_mapping.reliability(), reliability);
        assert!(individual_mapping.explicit_timestamp());
        let exp_retention = Retention::Stored {
            expiry: Some(retention_expiry),
        };
        assert_eq!(individual_mapping.retention(), exp_retention);
        #[cfg(feature = "server-fields")]
        {
            let exp = crate::interface::DatabaseRetention::UseTtl { ttl: database_ttl };
            assert_eq!(individual_mapping.database_retention(), exp);
        }
        #[cfg(feature = "doc-fields")]
        {
            assert_eq!(description, individual_mapping.description());
            assert_eq!(doc, individual_mapping.doc());
        }
    }

    #[cfg(feature = "strict")]
    #[test]
    fn mapping_error_invalid_fields() {
        let mapping = Mapping {
            endpoint: "/individual/path",
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: Some(true),
            description: None,
            doc: None,
        };

        let err = DatastreamIndividualMapping::try_from(mapping).unwrap_err();
        assert!(matches!(
            err,
            MappingError::InvalidField {
                field: "allow_unset",
                interface_type: crate::schema::InterfaceType::Datastream,
            }
        ));
    }
}
