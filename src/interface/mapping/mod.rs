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

//! Mapping of an interface.

use std::{borrow::Borrow, ops::Deref};

use log::warn;

use self::endpoint::Endpoint;

use super::{DatabaseRetention, InterfaceError, Mapping, MappingType, Reliability, Retention};

pub mod endpoint;
pub mod iter;
pub mod path;
pub mod vec;

/// Mapping of a [`DatastreamIndividual`](super::DatastreamIndividual) interface.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatastreamIndividualMapping {
    pub(super) mapping: BaseMapping,
    pub(super) reliability: Reliability,
    pub(super) retention: Retention,
    pub(super) database_retention: DatabaseRetention,
    pub(super) explicit_timestamp: bool,
}

impl InterfaceMapping for DatastreamIndividualMapping {
    fn endpoint(&self) -> &Endpoint<String> {
        self.mapping.endpoint()
    }
}

impl PartialOrd for DatastreamIndividualMapping {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DatastreamIndividualMapping {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.mapping.cmp(&other.mapping)
    }
}

impl<'a> From<&'a DatastreamIndividualMapping> for Mapping<&'a str> {
    fn from(value: &'a DatastreamIndividualMapping) -> Self {
        let mut mapping = Mapping::from(&value.mapping);

        mapping.reliability = value.reliability;
        mapping.explicit_timestamp = value.explicit_timestamp;

        value.retention.apply(&mut mapping);
        value.database_retention.apply(&mut mapping);

        mapping
    }
}

impl<T> TryFrom<&Mapping<T>> for DatastreamIndividualMapping
where
    T: AsRef<str> + Into<String>,
{
    type Error = InterfaceError;

    fn try_from(value: &Mapping<T>) -> Result<Self, Self::Error> {
        let base_mapping = BaseMapping::try_from(value)?;

        if value.allow_unset {
            warn!("datastream cannot have allow_unset, ignoring");
        }

        Ok(Self {
            mapping: base_mapping,
            reliability: value.reliability(),
            retention: value.retention(),
            database_retention: value.database_retention(),
            explicit_timestamp: value.explicit_timestamp(),
        })
    }
}

impl Borrow<Endpoint<String>> for DatastreamIndividualMapping {
    fn borrow(&self) -> &Endpoint<String> {
        &self.mapping.endpoint
    }
}

impl Borrow<BaseMapping> for DatastreamIndividualMapping {
    fn borrow(&self) -> &BaseMapping {
        &self.mapping
    }
}

impl Deref for DatastreamIndividualMapping {
    type Target = BaseMapping;

    fn deref(&self) -> &Self::Target {
        &self.mapping
    }
}

/// Mapping of a [`Properties`](super::Properties) interface.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PropertiesMapping {
    pub(super) mapping: BaseMapping,
    pub(super) allow_unset: bool,
}

impl InterfaceMapping for PropertiesMapping {
    fn endpoint(&self) -> &Endpoint<String> {
        self.mapping.endpoint()
    }
}

impl PartialOrd for PropertiesMapping {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PropertiesMapping {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.mapping.cmp(&other.mapping)
    }
}

impl<'a> From<&'a PropertiesMapping> for Mapping<&'a str> {
    fn from(value: &'a PropertiesMapping) -> Self {
        let mut mapping = Mapping::from(&value.mapping);

        // Properties must have have Reliability Unique
        mapping.reliability = Reliability::Unique;
        mapping.allow_unset = value.allow_unset;

        mapping
    }
}

impl<T> TryFrom<&Mapping<T>> for PropertiesMapping
where
    T: AsRef<str> + Into<String>,
{
    type Error = InterfaceError;

    fn try_from(value: &Mapping<T>) -> Result<Self, Self::Error> {
        let base_mapping = BaseMapping::try_from(value)?;

        if value.explicit_timestamp {
            warn!("property cannot have explicit_timestamp, ignoring");
        }

        Ok(Self {
            mapping: base_mapping,
            allow_unset: value.allow_unset(),
        })
    }
}

impl Borrow<BaseMapping> for PropertiesMapping {
    fn borrow(&self) -> &BaseMapping {
        &self.mapping
    }
}

impl Borrow<Endpoint<String>> for PropertiesMapping {
    fn borrow(&self) -> &Endpoint<String> {
        &self.mapping.endpoint
    }
}

impl Deref for PropertiesMapping {
    type Target = BaseMapping;

    fn deref(&self) -> &Self::Target {
        &self.mapping
    }
}

/// Shared struct for a mapping for all interface types.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BaseMapping {
    pub(super) endpoint: Endpoint<String>,
    pub(super) mapping_type: MappingType,
    #[cfg(feature = "interface-doc")]
    pub(super) description: Option<String>,
    #[cfg(feature = "interface-doc")]
    pub(super) doc: Option<String>,
}

impl BaseMapping {
    pub(crate) fn mapping_type(&self) -> MappingType {
        self.mapping_type
    }

    #[cfg(feature = "interface-doc")]
    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[cfg(feature = "interface-doc")]
    pub(crate) fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }
}

impl InterfaceMapping for BaseMapping {
    fn endpoint(&self) -> &Endpoint<String> {
        &self.endpoint
    }
}

impl Borrow<Endpoint<String>> for BaseMapping {
    fn borrow(&self) -> &Endpoint<String> {
        &self.endpoint
    }
}

impl PartialOrd for BaseMapping {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BaseMapping {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.endpoint.cmp(&other.endpoint)
    }
}

#[cfg(feature = "interface-doc")]
impl<'a> From<&'a BaseMapping> for Mapping<&'a str> {
    fn from(value: &'a BaseMapping) -> Self {
        Self::new(value.endpoint(), value.mapping_type())
            .with_description(value.description())
            .with_doc(value.doc())
    }
}

#[cfg(not(feature = "interface-doc"))]
impl<'a> From<&'a BaseMapping> for Mapping<&'a str> {
    fn from(value: &'a BaseMapping) -> Self {
        Self::new(value.endpoint(), value.mapping_type())
    }
}

impl<T> TryFrom<&Mapping<T>> for BaseMapping
where
    T: AsRef<str> + Into<String>,
{
    type Error = InterfaceError;

    fn try_from(value: &Mapping<T>) -> Result<Self, Self::Error> {
        let endpoint = Endpoint::try_from(value.endpoint().as_ref())?;

        Ok(Self {
            endpoint,
            mapping_type: value.mapping_type(),
            #[cfg(feature = "interface-doc")]
            description: value.description().map(|t| t.as_ref().into()),
            #[cfg(feature = "interface-doc")]
            doc: value.doc().map(|t| t.as_ref().into()),
        })
    }
}

/// Mapping of an interface.
pub trait InterfaceMapping {
    /// Returns a reference to the endpoint of an interface.
    fn endpoint(&self) -> &Endpoint<String>;
}
