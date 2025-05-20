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

//! Datastream with aggregation individual.
//!
//! In case aggregation is individual, each mapping is treated as an independent value and is
//! managed individually.

use std::{borrow::Cow, fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{
    error::Error,
    interface::{
        name::InterfaceName, version::InterfaceVersion, AggregationIndividual, MappingVec, Schema,
    },
    mapping::{
        datastream::individual::DatastreamIndividualMapping, path::MappingPath, MappingError,
    },
    schema::{Aggregation, InterfaceJson, InterfaceType, Mapping, Ownership},
};

/// Interface of type datastream individual.
///
/// For this interface all the mappings have distinct configurations.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
#[serde(try_from = "InterfaceJson<std::borrow::Cow<str>>")]
pub struct DatastreamIndividual {
    pub(crate) name: InterfaceName,
    pub(crate) version: InterfaceVersion,
    pub(crate) ownership: Ownership,
    pub(crate) mappings: MappingVec<DatastreamIndividualMapping>,
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    pub(crate) description: Option<String>,
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    pub(crate) doc: Option<String>,
}

impl Schema for DatastreamIndividual {
    type Mapping = DatastreamIndividualMapping;

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
        self.iter_mappings().map(Mapping::from)
    }
}

impl AggregationIndividual for DatastreamIndividual {
    fn mapping(&self, path: &MappingPath) -> Option<&Self::Mapping> {
        self.mappings.get(path)
    }
}

impl Display for DatastreamIndividual {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name, self.version)
    }
}

impl<T> TryFrom<InterfaceJson<T>> for DatastreamIndividual
where
    T: AsRef<str> + Into<String>,
{
    type Error = Error;

    fn try_from(value: InterfaceJson<T>) -> Result<Self, Self::Error> {
        if value.interface_type != InterfaceType::Datastream
            || value.aggregation.unwrap_or_default() != Aggregation::Individual
        {
            return Err(Error::InterfaceConversion {
                exp_type: InterfaceType::Datastream,
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
            .map(DatastreamIndividualMapping::try_from)
            .collect::<Result<Vec<DatastreamIndividualMapping>, MappingError>>()
            .and_then(MappingVec::try_from)?;

        Ok(Self {
            name: name.into_string(),
            version,
            ownership: value.ownership,
            mappings,
            #[cfg(feature = "doc-fields")]
            description: value.description.map(T::into),
            #[cfg(feature = "doc-fields")]
            doc: value.doc.map(T::into),
        })
    }
}

impl Serialize for DatastreamIndividual {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        InterfaceJson::<Cow<str>>::from(self).serialize(serializer)
    }
}

impl FromStr for DatastreamIndividual {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let interface: InterfaceJson<Cow<str>> = serde_json::from_str(s)?;

        Self::try_from(interface)
    }
}
