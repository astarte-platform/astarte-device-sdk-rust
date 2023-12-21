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

//! Sorted collection of interfaces accessible by endpoint or path.

use std::{
    cmp::Ordering, collections::BTreeSet, fmt::Debug, iter::FusedIterator, ops::Deref,
    slice::Iter as SliceIter,
};

use itertools::Itertools;

use super::{endpoint::Endpoint, InterfaceMapping};

#[derive(Debug, Clone, Default)]
pub(crate) struct MappingVec<T> {
    mappings: Vec<Item<T>>,
}

impl<T> MappingVec<T> {
    /// Create an empty vector.
    pub(crate) fn new() -> Self {
        Self {
            mappings: Vec::new(),
        }
    }

    /// Gets the mapping searching the [`Endpoint`]'s for the one matching the path.
    pub(crate) fn get<U>(&self, path: &U) -> Option<&T>
    where
        T: InterfaceMapping + Debug,
        U: PartialOrd<Endpoint<String>> + Eq + Debug,
    {
        self.mappings
            .binary_search_by(|item| {
                let endpoint = item.endpoint();
                // We invert the ordering for less/greater since the trait is implemented on the
                // path, but we need to actually check the argument
                path.partial_cmp(endpoint)
                    .map_or(Ordering::Less, Ordering::reverse)
            })
            .ok()
            .and_then(|idx| self.mappings.get(idx))
            .map(|item| &item.0)
    }

    /// Returns the number of mappings.
    pub(crate) fn len(&self) -> usize {
        self.mappings.len()
    }

    /// Iterate over the mappings.
    pub(crate) fn iter(&self) -> ItemIter<T> {
        ItemIter {
            items: self.mappings.iter(),
        }
    }
}

impl<T> PartialEq for MappingVec<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.mappings
            .iter()
            .zip_longest(other.mappings.iter())
            .all(|e| match e {
                itertools::EitherOrBoth::Both(a, b) => a.0 == b.0,
                itertools::EitherOrBoth::Left(_) | itertools::EitherOrBoth::Right(_) => false,
            })
    }
}

impl<T> Eq for MappingVec<T> where T: Eq {}

impl<T> From<BTreeSet<Item<T>>> for MappingVec<T> {
    fn from(value: BTreeSet<Item<T>>) -> Self {
        let mappings = value.into_iter().collect();

        Self { mappings }
    }
}

/// Item of the [`MappingVec`] to check that the inner type is [`Ord`] by the [`Endpoint`] returned by
/// [`InterfaceMapping::endpoint`]
#[derive(Debug, Clone, Copy)]
pub(crate) struct Item<T>(T);

impl<T> Item<T> {
    pub(crate) fn new(mapping: T) -> Self {
        Self(mapping)
    }
}

impl<T> Deref for Item<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> PartialEq for Item<T>
where
    T: InterfaceMapping,
{
    fn eq(&self, other: &Self) -> bool {
        self.endpoint() == other.endpoint()
    }
}

impl<T> Eq for Item<T> where T: InterfaceMapping {}

impl<T> PartialOrd for Item<T>
where
    T: InterfaceMapping,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Item<T>
where
    T: InterfaceMapping,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.endpoint().cmp(other.endpoint())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ItemIter<'a, T> {
    items: SliceIter<'a, Item<T>>,
}

impl<'a, T> Iterator for ItemIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next().map(|i| &i.0)
    }
}

impl<'a, T> DoubleEndedIterator for ItemIter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.items.next_back().map(|i| &i.0)
    }
}

impl<'a, T> ExactSizeIterator for ItemIter<'a, T> {
    fn len(&self) -> usize {
        self.items.len()
    }
}

impl<'a, T> FusedIterator for ItemIter<'a, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    impl InterfaceMapping for Endpoint<String> {
        fn endpoint(&self) -> &Endpoint<String> {
            self
        }
    }

    fn enpt(s: impl AsRef<str>) -> Endpoint<String> {
        let endpoint = s.as_ref();

        Endpoint::try_from(endpoint).unwrap()
    }

    #[test]
    fn should_get_mapping() {
        let cases = [enpt("/foo/32"), enpt("/foo/bar"), enpt("/%{param}/param")];
        let btree: BTreeSet<_> = cases.iter().map(|i| Item(i.clone())).collect();
        let mappings = MappingVec::from(btree);

        for case in cases {
            let e = mappings
                .get(&case)
                .unwrap_or_else(|| panic!("failed to get {case}"));

            assert_eq!(*e, case);
        }
    }

    #[test]
    fn should_iter_mappings() {
        // Order is important
        let cases = [enpt("/foo/32"), enpt("/foo/bar"), enpt("/%{param}/param")];
        let btree: BTreeSet<_> = cases.iter().map(|i| Item(i.clone())).collect();
        let mappings = MappingVec::from(btree);

        mappings
            .iter()
            .zip_eq(cases)
            .for_each(|(a, b)| assert_eq!(*a, b))
    }
}
