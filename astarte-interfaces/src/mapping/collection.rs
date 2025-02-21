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

//! Sorted collection of interfaces accessible by endpoint or path.

use std::{fmt::Debug, slice::Iter as SliceIter};

use crate::{interface::MAX_INTERFACE_MAPPINGS, Endpoint};

use super::{path::MappingPath, InterfaceMapping, MappingError};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MappingVec<T>
where
    T: InterfaceMapping,
{
    items: Vec<T>,
}

impl<T> MappingVec<T>
where
    T: InterfaceMapping,
{
    /// Gets the mapping searching the [`Endpoint`]'s for the one matching the path.
    pub fn get(&self, path: &MappingPath<'_>) -> Option<&T>
    where
        T: InterfaceMapping + Debug,
    {
        self.items
            .iter()
            .find(|item| item.endpoint().eq_mapping(path))
    }

    /// Returns the number of mappings.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns the number of mappings.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }
}

impl<'a, T> IntoIterator for &'a MappingVec<T>
where
    T: InterfaceMapping,
{
    type Item = &'a T;

    type IntoIter = SliceIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

impl<T> TryFrom<Vec<T>> for MappingVec<T>
where
    T: InterfaceMapping,
{
    type Error = MappingError;

    fn try_from(value: Vec<T>) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(MappingError::Empty);
        }

        if value.len() > MAX_INTERFACE_MAPPINGS {
            return Err(MappingError::TooMany(value.len()));
        }

        let mut endpoints: Vec<&Endpoint> = value.iter().map(T::endpoint).collect();
        // Sort + check is O(n*log(n)) instead of O(n^2) for checking all items
        endpoints.sort_unstable();
        endpoints.windows(2).try_for_each(|window| {
            let [a, b] = window else {
                // this never happen, for value.len < 2 the all is never called, see `windows`
                unreachable!("windows returned less than 2 elements");
            };

            if a == b {
                return Err(MappingError::Duplicated {
                    endpoint: a.to_string(),
                });
            }

            Ok(())
        })?;

        Ok(Self { items: value })
    }
}

#[cfg(test)]
mod tests {
    use crate::{mapping::endpoint::Endpoint, schema::MappingType, DatastreamObjectMapping};

    use super::*;

    fn make_vecs(cases: &[&str]) -> Vec<DatastreamObjectMapping> {
        let mut endpoints: Vec<DatastreamObjectMapping> = cases
            .iter()
            .map(|&s| {
                let endpoint = Endpoint::try_from(s).unwrap();

                DatastreamObjectMapping {
                    endpoint,
                    mapping_type: MappingType::Boolean,
                    #[cfg(feature = "doc-fields")]
                    description: None,
                    #[cfg(feature = "doc-fields")]
                    doc: None,
                }
            })
            .collect();

        endpoints.sort_by(|a, b| a.endpoint.cmp(&b.endpoint));

        endpoints
    }

    #[test]
    fn should_get_mapping() {
        let cases = ["/foo/a32", "/foo/bar", "/%{param}/param"];

        let endpoints = make_vecs(&cases);

        let mappings = MappingVec::try_from(endpoints.clone()).unwrap();

        for (case, endpoint) in cases.iter().zip(endpoints) {
            let path = MappingPath::try_from(*case).unwrap();
            let e = mappings
                .get(&path)
                .unwrap_or_else(|| panic!("couldn't get mapping {case}"));

            assert_eq!(*e, endpoint);
        }
    }

    #[test]
    fn try_from_single_element() {
        let cases = ["/foo/a32"];

        let endpoints = make_vecs(&cases);

        MappingVec::try_from(endpoints).unwrap();
    }
}
