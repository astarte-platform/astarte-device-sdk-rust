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

/// Mapping of a [`Properties`](super::Properties) interface.
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
