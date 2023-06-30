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

use std::{borrow::Borrow, ops::Deref};

use self::endpoint::Endpoint;

use super::{DatabaseRetention, InterfaceError, Mapping, MappingType, Reliability, Retention};

pub mod endpoint;
pub mod iter;
pub mod path;

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct DatastreamIndividualMapping {
    pub(super) mapping: BaseMapping,
    pub(super) reliability: Reliability,
    pub(super) retention: Retention,
    pub(super) database_retention: DatabaseRetention,
    pub(super) explicit_timestamp: bool,
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

impl<'a> From<&'a DatastreamIndividualMapping> for Mapping<'a> {
    fn from(value: &'a DatastreamIndividualMapping) -> Self {
        let mut mapping = Mapping::from(&value.mapping);

        mapping.reliability = value.reliability;
        mapping.explicit_timestamp = value.explicit_timestamp;

        value.retention.apply(&mut mapping);
        value.database_retention.apply(&mut mapping);

        mapping
    }
}

impl TryFrom<&Mapping<'_>> for DatastreamIndividualMapping {
    type Error = InterfaceError;

    fn try_from(value: &Mapping<'_>) -> Result<Self, Self::Error> {
        let base_mapping = BaseMapping::try_from(value)?;

        Ok(Self {
            mapping: base_mapping,
            reliability: value.reliability(),
            retention: value.retention(),
            database_retention: value.database_retention(),
            explicit_timestamp: value.explicit_timestamp(),
        })
    }
}

impl<'a> Borrow<Endpoint<'a>> for DatastreamIndividualMapping {
    fn borrow(&self) -> &Endpoint<'a> {
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct PropertiesMapping {
    pub(super) mapping: BaseMapping,
    pub(super) allow_unset: bool,
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

impl<'a> From<&'a PropertiesMapping> for Mapping<'a> {
    fn from(value: &'a PropertiesMapping) -> Self {
        let mut mapping = Mapping::from(&value.mapping);

        mapping.allow_unset = value.allow_unset;

        mapping
    }
}

impl TryFrom<&Mapping<'_>> for PropertiesMapping {
    type Error = InterfaceError;

    fn try_from(value: &Mapping<'_>) -> Result<Self, Self::Error> {
        let base_mapping = BaseMapping::try_from(value)?;

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

impl<'a> Borrow<Endpoint<'a>> for PropertiesMapping {
    fn borrow(&self) -> &Endpoint<'a> {
        &self.mapping.endpoint
    }
}

impl Deref for PropertiesMapping {
    type Target = BaseMapping;

    fn deref(&self) -> &Self::Target {
        &self.mapping
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct BaseMapping {
    pub(super) endpoint: Endpoint<'static>,
    pub(super) mapping_type: MappingType,
    pub(super) description: Option<String>,
    pub(super) doc: Option<String>,
}

impl BaseMapping {
    pub(crate) fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub(crate) fn mapping_type(&self) -> MappingType {
        self.mapping_type
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub(crate) fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }
}

impl<'a> Borrow<Endpoint<'a>> for BaseMapping {
    fn borrow(&self) -> &Endpoint<'a> {
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

impl<'a> From<&'a BaseMapping> for Mapping<'a> {
    fn from(value: &'a BaseMapping) -> Self {
        Self::new(value.endpoint(), value.mapping_type())
            .with_description(value.description())
            .with_doc(value.doc())
    }
}

impl<'a> TryFrom<&Mapping<'a>> for BaseMapping {
    type Error = InterfaceError;

    fn try_from(value: &Mapping<'a>) -> Result<Self, Self::Error> {
        let endpoint = Endpoint::try_from(value.endpoint())?.into_owned();

        Ok(Self {
            endpoint,
            mapping_type: value.mapping_type(),
            description: value.description().map(ToString::to_string),
            doc: value.doc().map(ToString::to_string),
        })
    }
}
