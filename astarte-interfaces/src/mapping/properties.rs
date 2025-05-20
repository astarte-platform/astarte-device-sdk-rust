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

//! Mapping for interfaces of type Property.

use std::borrow::Cow;

use crate::mapping::invalid_filed;
use crate::schema::{Mapping, MappingType};

use super::{endpoint::Endpoint, InterfaceMapping, MappingError};

/// Mapping of a [`Properties`](crate::Properties) interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertiesMapping {
    pub(crate) endpoint: Endpoint<String>,
    pub(crate) mapping_type: MappingType,
    pub(crate) allow_unset: bool,
    #[cfg(feature = "doc-fields")]
    pub(crate) description: Option<String>,
    #[cfg(feature = "doc-fields")]
    pub(crate) doc: Option<String>,
}

impl PropertiesMapping {
    /// Returns true if the property can be unset.
    #[must_use]
    pub fn allow_unset(&self) -> bool {
        self.allow_unset
    }
}

impl InterfaceMapping for PropertiesMapping {
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

impl<T> TryFrom<Mapping<T>> for PropertiesMapping
where
    T: AsRef<str> + Into<String>,
{
    type Error = MappingError;

    fn try_from(value: Mapping<T>) -> Result<Self, Self::Error> {
        let endpoint = Endpoint::try_from(value.endpoint.as_ref())?;

        if value.reliability.is_some() {
            invalid_filed!(properties, "reliability");
        }

        if value.explicit_timestamp.is_some() {
            invalid_filed!(properties, "explicit_timestamp");
        }

        if value.retention.is_some() {
            invalid_filed!(properties, "retention");
        }

        if value.expiry.is_some() {
            invalid_filed!(properties, "expiry");
        }

        if value.database_retention_policy.is_some() {
            invalid_filed!(properties, "database_retention_policy");
        }

        if value.database_retention_ttl.is_some() {
            invalid_filed!(properties, "database_retention_ttl");
        }

        Ok(Self {
            endpoint,
            mapping_type: value.mapping_type,
            allow_unset: value.allow_unset.unwrap_or_default(),
            #[cfg(feature = "doc-fields")]
            description: value.description.map(T::into),
            #[cfg(feature = "doc-fields")]
            doc: value.doc.map(T::into),
        })
    }
}

impl<'a> From<&'a PropertiesMapping> for Mapping<Cow<'a, str>> {
    fn from(value: &'a PropertiesMapping) -> Self {
        Mapping {
            endpoint: value.endpoint().to_string().into(),
            mapping_type: value.mapping_type,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: Some(value.allow_unset),
            #[cfg(feature = "doc-fields")]
            description: value.description().map(Cow::Borrowed),
            #[cfg(feature = "doc-fields")]
            doc: value.doc().map(Cow::Borrowed),
            #[cfg(not(feature = "doc-fields"))]
            description: None,
            #[cfg(not(feature = "doc-fields"))]
            doc: None,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn getters_success() {
        let mapping_type = MappingType::Boolean;
        let description = Some("Property mapping description");
        let doc = Some("Property mapping doc");
        let mapping = Mapping {
            endpoint: "/property/path",
            mapping_type,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: Some(true),
            description,
            doc,
        };

        let prop_mapping = PropertiesMapping::try_from(mapping).unwrap();
        let exp_endpoint = Endpoint::try_from("/property/path").unwrap();
        assert_eq!(*prop_mapping.endpoint(), exp_endpoint);
        assert_eq!(prop_mapping.mapping_type(), mapping_type);
        assert!(prop_mapping.allow_unset());
        #[cfg(feature = "doc-fields")]
        {
            assert_eq!(prop_mapping.description(), description);
            assert_eq!(prop_mapping.doc(), doc);
        }
    }

    #[test]
    fn from_and_into() {
        let description = Some(Cow::Borrowed("Property mapping description"));
        let doc = Some(Cow::Borrowed("Property mapping doc"));
        let mapping = Mapping {
            endpoint: Cow::Borrowed("/property/path"),
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: Some(true),
            description,
            doc,
        };

        let prop_mapping = PropertiesMapping::try_from(mapping.clone()).unwrap();

        let exp = PropertiesMapping {
            endpoint: Endpoint::try_from("/property/path").unwrap(),
            mapping_type: MappingType::Boolean,
            allow_unset: true,
            #[cfg(feature = "doc-fields")]
            description: mapping.description.as_ref().map(|v| v.to_string()),
            #[cfg(feature = "doc-fields")]
            doc: mapping.doc.as_ref().map(|v| v.to_string()),
        };
        assert_eq!(prop_mapping, exp);

        let cov_mapping: Mapping<Cow<str>> = (&prop_mapping).into();

        #[cfg(not(feature = "doc-fields"))]
        let mut mapping = mapping;
        #[cfg(not(feature = "doc-fields"))]
        {
            mapping.description.take();
            mapping.doc.take();
        }

        assert_eq!(cov_mapping, mapping);
    }

    #[cfg(feature = "strict")]
    #[test]
    fn mapping_error_invalid_fields() {
        use crate::schema::{DatabaseRetentionPolicy, InterfaceType, Reliability, Retention};

        let mapping = Mapping {
            endpoint: "/property/path",
            mapping_type: MappingType::Boolean,
            reliability: Some(Reliability::Guaranteed),
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: None,
            description: None,
            doc: None,
        };

        let err = PropertiesMapping::try_from(mapping).unwrap_err();
        assert!(matches!(
            err,
            MappingError::InvalidField {
                field: "reliability",
                interface_type: InterfaceType::Properties
            }
        ));

        let mapping = Mapping {
            endpoint: "/property/path",
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: Some(true),
            retention: None,
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: None,
            description: None,
            doc: None,
        };

        let err = PropertiesMapping::try_from(mapping).unwrap_err();
        assert!(matches!(
            err,
            MappingError::InvalidField {
                field: "explicit_timestamp",
                interface_type: InterfaceType::Properties
            }
        ));

        let mapping = Mapping {
            endpoint: "/property/path",
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: None,
            retention: Some(Retention::Stored),
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: None,
            description: None,
            doc: None,
        };

        let err = PropertiesMapping::try_from(mapping).unwrap_err();
        assert!(matches!(
            err,
            MappingError::InvalidField {
                field: "retention",
                interface_type: InterfaceType::Properties
            }
        ));

        let mapping = Mapping {
            endpoint: "/property/path",
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: Some(420),
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: None,
            description: None,
            doc: None,
        };

        let err = PropertiesMapping::try_from(mapping).unwrap_err();
        assert!(matches!(
            err,
            MappingError::InvalidField {
                field: "expiry",
                interface_type: InterfaceType::Properties
            }
        ));

        let mapping = Mapping {
            endpoint: "/property/path",
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: Some(DatabaseRetentionPolicy::NoTtl),
            database_retention_ttl: None,
            allow_unset: None,
            description: None,
            doc: None,
        };

        let err = PropertiesMapping::try_from(mapping).unwrap_err();
        assert!(matches!(
            err,
            MappingError::InvalidField {
                field: "database_retention_policy",
                interface_type: InterfaceType::Properties
            }
        ));

        let mapping = Mapping {
            endpoint: "/property/path",
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: Some(420),
            allow_unset: None,
            description: None,
            doc: None,
        };

        let err = PropertiesMapping::try_from(mapping).unwrap_err();
        assert!(matches!(
            err,
            MappingError::InvalidField {
                field: "database_retention_ttl",
                interface_type: InterfaceType::Properties
            }
        ));
    }
}
