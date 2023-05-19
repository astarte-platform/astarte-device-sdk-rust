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

//! Iterators over an interface mappings mappings.

use std::{collections::btree_map::Values, iter::FusedIterator};

use crate::interface::{DatastreamObject, InterfaceIml, Mapping, MappingMap, MappingPath};

use super::{BaseMapping, DatastreamIndividualMapping, PropertiesMapping};

type ValuesIter<'a, T> = Values<'a, MappingPath<'a>, T>;

pub(crate) enum MappingIter<'a> {
    Properties(PropertiesMappingIter<'a>),
    Individual(IndividualMappingIter<'a>),
    Object(ObjectMappingIter<'a>),
}

impl<'a> MappingIter<'a> {
    pub(crate) fn new(interface: &'a InterfaceIml) -> Self {
        match interface {
            InterfaceIml::DatastreamIndividual(individual) => {
                Self::Individual(individual.iter_mappings())
            }
            InterfaceIml::DatastreamObject(object) => Self::Object(object.iter_mappings()),
            InterfaceIml::Properties(properties) => Self::Properties(properties.iter_mappings()),
        }
    }
}

impl<'a> Iterator for MappingIter<'a> {
    type Item = Mapping<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Properties(iter) => iter.next(),
            Self::Individual(iter) => iter.next(),
            Self::Object(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Properties(iter) => iter.size_hint(),
            Self::Individual(iter) => iter.size_hint(),
            Self::Object(iter) => iter.size_hint(),
        }
    }
}

impl DoubleEndedIterator for MappingIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Properties(iter) => iter.next_back(),
            Self::Individual(iter) => iter.next_back(),
            Self::Object(iter) => iter.next_back(),
        }
    }
}

impl ExactSizeIterator for MappingIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Properties(iter) => iter.len(),
            Self::Individual(iter) => iter.len(),
            Self::Object(iter) => iter.len(),
        }
    }
}

impl FusedIterator for MappingIter<'_> {}

#[derive(Debug, Clone)]
pub(crate) struct PropertiesMappingIter<'a> {
    properties: ValuesIter<'a, PropertiesMapping>,
}

impl<'a> PropertiesMappingIter<'a> {
    pub(crate) fn new(properties: &'a MappingMap<PropertiesMapping>) -> Self {
        Self {
            properties: properties.values(),
        }
    }
}

impl<'a> Iterator for PropertiesMappingIter<'a> {
    type Item = Mapping<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.properties.next().map(|mapping| mapping.into())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.properties.size_hint()
    }
}

impl DoubleEndedIterator for PropertiesMappingIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.properties.next_back().map(|mapping| mapping.into())
    }
}

impl ExactSizeIterator for PropertiesMappingIter<'_> {
    fn len(&self) -> usize {
        self.properties.len()
    }
}

impl FusedIterator for PropertiesMappingIter<'_> {}

#[derive(Debug, Clone)]
pub(crate) struct IndividualMappingIter<'a> {
    properties: ValuesIter<'a, DatastreamIndividualMapping>,
}

impl<'a> IndividualMappingIter<'a> {
    pub(crate) fn new(properties: &'a MappingMap<DatastreamIndividualMapping>) -> Self {
        Self {
            properties: properties.values(),
        }
    }
}

impl<'a> Iterator for IndividualMappingIter<'a> {
    type Item = Mapping<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.properties.next().map(|mapping| mapping.into())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.properties.size_hint()
    }
}

impl DoubleEndedIterator for IndividualMappingIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.properties.next_back().map(|mapping| mapping.into())
    }
}

impl ExactSizeIterator for IndividualMappingIter<'_> {
    fn len(&self) -> usize {
        self.properties.len()
    }
}

impl FusedIterator for IndividualMappingIter<'_> {}

#[derive(Debug, Clone)]
pub(crate) struct ObjectMappingIter<'a> {
    interface: &'a DatastreamObject,
    properties: ValuesIter<'a, BaseMapping>,
}

impl<'a> ObjectMappingIter<'a> {
    pub(crate) fn new(interface: &'a DatastreamObject) -> Self {
        Self {
            interface,
            properties: interface.mappings.values(),
        }
    }
}

impl<'a> Iterator for ObjectMappingIter<'a> {
    type Item = Mapping<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.properties
            .next()
            .map(|mapping| self.interface.apply(mapping))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.properties.size_hint()
    }
}

impl<'a> DoubleEndedIterator for ObjectMappingIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.properties
            .next_back()
            .map(move |mapping| self.interface.apply(mapping))
    }
}

impl<'a> ExactSizeIterator for ObjectMappingIter<'a> {
    fn len(&self) -> usize {
        self.properties.len()
    }
}

impl FusedIterator for ObjectMappingIter<'_> {}
