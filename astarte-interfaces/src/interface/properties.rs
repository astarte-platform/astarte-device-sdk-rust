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

//! Interface for persistent, stateful, synchronized state with no concept of history or timestamp.
//!
//! Properties are useful, for example, when dealing with settings, states or policies/rules.

use std::{borrow::Cow, fmt::Display, str::FromStr};

use serde::Serialize;

use crate::{
    error::Error,
    mapping::{path::MappingPath, properties::PropertiesMapping, MappingError},
    schema::{Aggregation, InterfaceJson, InterfaceType, Mapping, Ownership},
};

use super::{
    name::InterfaceName, version::InterfaceVersion, AggregationIndividual, MappingVec, Schema,
};

/// Interface of type individual property.
///
/// For this interface all the mappings have their own configuration.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Properties {
    name: InterfaceName,
    version: InterfaceVersion,
    ownership: Ownership,
    mappings: MappingVec<PropertiesMapping>,
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    description: Option<String>,
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    doc: Option<String>,
}

impl Schema for Properties {
    type Mapping = PropertiesMapping;

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
        InterfaceType::Properties
    }

    fn ownership(&self) -> Ownership {
        self.ownership
    }

    fn aggregation(&self) -> Aggregation {
        Aggregation::Individual
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
        self.mappings.iter().map(Mapping::from)
    }
}

impl AggregationIndividual for Properties {
    fn mapping(&self, path: &MappingPath) -> Option<&Self::Mapping> {
        self.mappings.get(path)
    }
}

impl Display for Properties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name, self.version)
    }
}

impl<T> TryFrom<InterfaceJson<T>> for Properties
where
    T: AsRef<str> + Into<String>,
{
    type Error = Error;

    fn try_from(value: InterfaceJson<T>) -> Result<Self, Self::Error> {
        if value.interface_type != InterfaceType::Properties
            || value.aggregation.unwrap_or_default() != Aggregation::Individual
        {
            return Err(Error::InterfaceConversion {
                exp_type: InterfaceType::Properties,
                exp_aggregation: Aggregation::Individual,
                got_type: value.interface_type,
                got_aggregation: value.aggregation.unwrap_or_default(),
            });
        }

        let name = InterfaceName::from_str_ref(value.interface_name)?;
        let version = InterfaceVersion::try_new(value.version_major, value.version_minor)?;

        let mappings = value
            .mappings
            .into_iter()
            .map(PropertiesMapping::try_from)
            .collect::<Result<Vec<_>, MappingError>>()
            .and_then(MappingVec::try_from)?;

        Ok(Self {
            name: name.into_string(),
            version,
            ownership: value.ownership,
            mappings,
            #[cfg(feature = "doc-fields")]
            description: value.description.as_ref().map(|v| v.as_ref().to_string()),
            #[cfg(feature = "doc-fields")]
            doc: value.doc.as_ref().map(|v| v.as_ref().to_string()),
        })
    }
}

impl Serialize for Properties {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        InterfaceJson::from(self).serialize(serializer)
    }
}

impl FromStr for Properties {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let interface: InterfaceJson<Cow<str>> = serde_json::from_str(s)?;

        Self::try_from(interface)
    }
}
