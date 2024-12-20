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

//! Typed reference to an interface.

use std::{borrow::Borrow, ops::Deref};

use crate::Interface;

use super::{mapping::path::MappingPath, DatastreamObject, Mapping};

/// Struct to hold a reference to an [`Interface`], which is a property.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PropertyRef<'a>(pub(crate) &'a Interface);

impl Borrow<Interface> for PropertyRef<'_> {
    fn borrow(&self) -> &Interface {
        self.0
    }
}

impl Deref for PropertyRef<'_> {
    type Target = Interface;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl AsRef<Interface> for PropertyRef<'_> {
    fn as_ref(&self) -> &Interface {
        self.0
    }
}

/// Reference to an [`Interface`] and a [`DatastreamObject`] that is guaranty to belong to the interface.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ObjectRef<'a> {
    pub(crate) interface: &'a Interface,
    pub(crate) object: &'a DatastreamObject,
}

impl<'a> ObjectRef<'a> {
    /// Create a new reference only if the interface is an object.
    pub(crate) fn new(interface: &'a Interface) -> Option<Self> {
        match &interface.inner {
            crate::interface::InterfaceType::DatastreamIndividual(_)
            | crate::interface::InterfaceType::Properties(_) => None,
            crate::interface::InterfaceType::DatastreamObject(object) => {
                Some(Self { interface, object })
            }
        }
    }
}

impl Deref for ObjectRef<'_> {
    type Target = DatastreamObject;

    fn deref(&self) -> &Self::Target {
        self.object
    }
}

/// Reference to an [`Interface`] and a [`Mapping`] that is guaranty to belong to the interface.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MappingRef<'a, I: 'a> {
    path: &'a MappingPath<'a>,
    mapping: Mapping<&'a str>,
    interface: I,
}

impl<'a> MappingRef<'a, &'a Interface> {
    pub(crate) fn new(interface: &'a Interface, path: &'a MappingPath) -> Option<Self> {
        interface.mapping(path).map(|mapping| Self {
            interface,
            path,
            mapping,
        })
    }

    pub(crate) fn as_prop(&self) -> Option<MappingRef<'a, PropertyRef<'a>>> {
        self.interface.as_prop_ref().map(|interface| MappingRef {
            interface,
            path: self.path,
            mapping: self.mapping,
        })
    }
}

impl<'a> MappingRef<'a, PropertyRef<'a>> {
    pub(crate) fn with_prop(interface: PropertyRef<'a>, path: &'a MappingPath) -> Option<Self> {
        let mapping = interface.0.mapping(path)?;

        Some(Self {
            interface,
            path,
            mapping,
        })
    }
}

impl<I> MappingRef<'_, I> {
    #[inline]
    pub(crate) fn mapping(&self) -> &Mapping<&str> {
        &self.mapping
    }

    #[inline]
    pub(crate) fn interface(&self) -> &I {
        &self.interface
    }

    #[inline]
    pub(crate) fn path(&self) -> &MappingPath {
        self.path
    }
}

impl<'a, I: 'a> Deref for MappingRef<'a, I> {
    type Target = Mapping<&'a str>;

    fn deref(&self) -> &Self::Target {
        &self.mapping
    }
}
