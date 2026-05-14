// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::iter::{FusedIterator, Peekable};

use crate::AstarteData;
use astarte_interfaces::DatastreamObjectMapping;

/// Iterate over elements in ordered lists.
///
/// Iterator of two ordered lists returning tuples of the element A and B when they are equals,
/// otherwise the smaller one is returned first and the other is none.
pub(super) struct Iter<'a, M, D>
where
    D: Iterator<Item = &'a (String, AstarteData)>,
    M: Iterator<Item = &'a DatastreamObjectMapping>,
{
    data: Peekable<D>,
    mapping: Peekable<M>,
}

impl<'a, M, D> Iter<'a, M, D>
where
    D: Iterator<Item = &'a (String, AstarteData)>,
    M: Iterator<Item = &'a DatastreamObjectMapping>,
{
    pub(super) fn new(data: D, mapping: M) -> Self {
        Self {
            data: data.peekable(),
            mapping: mapping.peekable(),
        }
    }
}

impl<'a, M, D> Iterator for Iter<'a, M, D>
where
    D: Iterator<Item = &'a (String, AstarteData)>,
    M: Iterator<Item = &'a DatastreamObjectMapping>,
{
    type Item = (Option<D::Item>, Option<M::Item>);

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.data.peek();
        let b = self.mapping.peek();

        match (a, b) {
            (None, None) => None,
            (None, Some(_)) => Some((None, (self.mapping.next()))),
            (Some(_), None) => Some((self.data.next(), None)),
            (Some((a, _)), Some(b)) => match b.cmp_object_field(a) {
                // Mapping is less than the data, advance the mapping
                Ordering::Less => Some((None, self.mapping.next())),
                // Mapping and data are equal, advance both
                Ordering::Equal => Some((self.data.next(), self.mapping.next())),
                // Mapping is greater than the data, advance the data
                Ordering::Greater => Some((self.data.next(), None)),
            },
        }
    }
}

impl<'a, M, D> FusedIterator for Iter<'a, M, D>
where
    D: Iterator<Item = &'a (String, AstarteData)>,
    M: Iterator<Item = &'a DatastreamObjectMapping>,
{
}
