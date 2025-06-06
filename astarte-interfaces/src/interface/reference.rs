// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
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

//! Typed reference to an interface.

use std::ops::Deref;

use super::Interface;
use crate::mapping::path::MappingPath;

/// Reference to an [`Interface`] and a [`Mapping`] that is guaranty to belong to the interface.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MappingRef<'a, I, M> {
    path: &'a MappingPath<'a>,
    interface: &'a I,
    mapping: &'a M,
}

impl<'a, M, I> MappingRef<'a, &'a Interface> {
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
