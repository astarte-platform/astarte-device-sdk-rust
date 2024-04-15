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

use std::iter::FusedIterator;

use crate::interface::{DatastreamObject, InterfaceType, Mapping, MappingVec};

use super::{vec::ItemIter, BaseMapping, DatastreamIndividualMapping, PropertiesMapping};

/// Iterator over an [`Interface`](crate::Interface) mappings.
pub struct MappingIter<'a>(InnerIter<'a>);

enum InnerIter<'a> {
    Properties(PropertiesMappingIter<'a>),
    Individual(IndividualMappingIter<'a>),
    Object(ObjectMappingIter<'a>),
}

impl<'a> MappingIter<'a> {
    pub(crate) fn new(interface: &'a InterfaceType) -> Self {
        let iter = match interface {
            InterfaceType::DatastreamIndividual(individual) => {
                InnerIter::Individual(individual.iter_mappings())
            }
            InterfaceType::DatastreamObject(object) => InnerIter::Object(object.iter_mappings()),
            InterfaceType::Properties(properties) => {
                InnerIter::Properties(properties.iter_mappings())
            }
        };

        Self(iter)
    }
}

impl<'a> Iterator for MappingIter<'a> {
    type Item = Mapping<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            InnerIter::Properties(iter) => iter.next(),
            InnerIter::Individual(iter) => iter.next(),
            InnerIter::Object(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.0 {
            InnerIter::Properties(iter) => iter.size_hint(),
            InnerIter::Individual(iter) => iter.size_hint(),
            InnerIter::Object(iter) => iter.size_hint(),
        }
    }
}

impl DoubleEndedIterator for MappingIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            InnerIter::Properties(iter) => iter.next_back(),
            InnerIter::Individual(iter) => iter.next_back(),
            InnerIter::Object(iter) => iter.next_back(),
        }
    }
}

impl ExactSizeIterator for MappingIter<'_> {
    fn len(&self) -> usize {
        match &self.0 {
            InnerIter::Properties(iter) => iter.len(),
            InnerIter::Individual(iter) => iter.len(),
            InnerIter::Object(iter) => iter.len(),
        }
    }
}

impl FusedIterator for MappingIter<'_> {}

/// Iterator over a [`Properties`](crate::interface::Properties) mappings.
#[derive(Debug, Clone)]
pub struct PropertiesMappingIter<'a> {
    properties: ItemIter<'a, PropertiesMapping>,
}

impl<'a> PropertiesMappingIter<'a> {
    pub(crate) fn new(properties: &'a MappingVec<PropertiesMapping>) -> Self {
        Self {
            properties: properties.iter(),
        }
    }
}

impl<'a> Iterator for PropertiesMappingIter<'a> {
    type Item = Mapping<&'a str>;

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

/// Iterator over a [`DatastreamIndividual`](crate::interface::DatastreamIndividual) mappings.
#[derive(Debug, Clone)]
pub struct IndividualMappingIter<'a> {
    properties: ItemIter<'a, DatastreamIndividualMapping>,
}

impl<'a> IndividualMappingIter<'a> {
    pub(crate) fn new(properties: &'a MappingVec<DatastreamIndividualMapping>) -> Self {
        Self {
            properties: properties.iter(),
        }
    }
}

impl<'a> Iterator for IndividualMappingIter<'a> {
    type Item = Mapping<&'a str>;

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

/// Iterator over a [`DatastreamObject`] mappings.
#[derive(Debug, Clone)]
pub struct ObjectMappingIter<'a> {
    interface: &'a DatastreamObject,
    mappings: ItemIter<'a, BaseMapping>,
}

impl<'a> ObjectMappingIter<'a> {
    pub(crate) fn new(interface: &'a DatastreamObject) -> Self {
        Self {
            interface,
            mappings: interface.mappings.iter(),
        }
    }
}

impl<'a> Iterator for ObjectMappingIter<'a> {
    type Item = Mapping<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        self.mappings
            .next()
            .map(|mapping| self.interface.apply(mapping))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.mappings.size_hint()
    }
}

impl<'a> DoubleEndedIterator for ObjectMappingIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.mappings
            .next_back()
            .map(|mapping| self.interface.apply(mapping))
    }
}

impl<'a> ExactSizeIterator for ObjectMappingIter<'a> {
    fn len(&self) -> usize {
        self.mappings.len()
    }
}

impl FusedIterator for ObjectMappingIter<'_> {}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        test::{E2E_DEVICE_AGGREGATE, E2E_DEVICE_DATASTREAM, E2E_DEVICE_PROPERTY},
        Interface,
    };

    #[test]
    fn should_iter() {
        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();

        let t = interface.as_inner();
        let t = t.as_datastream_individual().unwrap();
        let mut iter = t.into_iter();

        assert_eq!(iter.len(), 14);

        let n = iter.next().unwrap();

        assert_eq!(*n.endpoint(), "/binaryblob_endpoint");

        let n = iter.next_back().unwrap();

        assert_eq!(*n.endpoint(), "/stringarray_endpoint");

        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();

        let t = interface.as_inner();
        let t = t.as_datastream_object().unwrap();
        let mut iter = t.into_iter();

        assert_eq!(iter.len(), 14);

        let n = iter.next().unwrap();

        assert_eq!(*n.endpoint(), "/%{sensor_id}/binaryblob_endpoint");

        let n = iter.next_back().unwrap();

        assert_eq!(*n.endpoint(), "/%{sensor_id}/stringarray_endpoint");

        let interface = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();

        let t = interface.as_inner();
        let t = t.as_properties().unwrap();
        let mut iter = t.into_iter();

        assert_eq!(iter.len(), 14);

        let n = iter.next().unwrap();

        assert_eq!(*n.endpoint(), "/%{sensor_id}/binaryblob_endpoint");

        let n = iter.next_back().unwrap();

        assert_eq!(*n.endpoint(), "/%{sensor_id}/stringarray_endpoint");
    }
}
