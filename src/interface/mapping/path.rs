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

//! Path of a mapping in interface. It's the parsed struct path received from the MQTT levels
//! structure of the topic received.

use std::{cmp::Ordering, fmt::Display};

use itertools::{EitherOrBoth, Itertools};

use super::endpoint::Endpoint;

/// Path of a mapping in interface.
///
/// This is used to access the [`Interface`](crate::interface::Interface) so we can compare the parsed [`MappingPath`]
/// with the [`Endpoint`].
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct MappingPath<'a> {
    pub(crate) path: &'a str,
    pub(crate) levels: Vec<&'a str>,
}

impl MappingPath<'_> {
    pub(crate) fn as_str(&self) -> &str {
        self.path
    }
}

impl Display for MappingPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl<'a> TryFrom<&'a str> for MappingPath<'a> {
    type Error = MappingError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        parse_mapping(value)
    }
}

impl PartialEq<&str> for MappingPath<'_> {
    fn eq(&self, &other: &&str) -> bool {
        self.path == other
    }
}

impl PartialEq<str> for MappingPath<'_> {
    fn eq(&self, other: &str) -> bool {
        self.path == other
    }
}

impl<T> PartialEq<Endpoint<T>> for MappingPath<'_>
where
    T: AsRef<str>,
{
    fn eq(&self, other: &Endpoint<T>) -> bool {
        other == self
    }
}

impl<T> PartialOrd<Endpoint<T>> for MappingPath<'_>
where
    T: AsRef<str>,
{
    fn partial_cmp(&self, other: &Endpoint<T>) -> Option<std::cmp::Ordering> {
        Some(other.cmp_levels(&self.levels).reverse())
    }
}

impl<S, T> PartialEq<Endpoint<T>> for (&MappingPath<'_>, S)
where
    S: AsRef<str>,
    T: AsRef<str>,
{
    fn eq(&self, endpoint: &Endpoint<T>) -> bool {
        let (base, path) = self;

        if base.levels.len() != endpoint.len() - 1 {
            return false;
        }

        let levels = base
            .levels
            .iter()
            .copied()
            .chain(std::iter::once(path.as_ref()));

        endpoint
            .levels
            .iter()
            .zip(levels)
            .all(|(e_level, m_level)| e_level.eq_str(m_level))
    }
}

impl<S, T> PartialOrd<Endpoint<T>> for (&MappingPath<'_>, S)
where
    S: AsRef<str>,
    T: AsRef<str>,
{
    fn partial_cmp(&self, endpoint: &Endpoint<T>) -> Option<Ordering> {
        let (base, path) = self;

        let iter = base
            .levels
            .iter()
            .copied()
            .chain(std::iter::once(path.as_ref()))
            .zip_longest(endpoint.levels.iter());

        for i in iter {
            match i {
                EitherOrBoth::Both(a, b) if b == a => {}
                EitherOrBoth::Both(a, b) => return b.partial_cmp(a).map(Ordering::reverse),
                EitherOrBoth::Left(_) => return Some(Ordering::Greater),
                EitherOrBoth::Right(_) => return Some(Ordering::Less),
            }
        }

        Some(Ordering::Equal)
    }
}

/// Error that can happen while parsing the MQTT levels structure of the topic received.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, thiserror::Error)]
pub enum MappingError {
    /// Missing forward slash at the beginning of the path.
    #[error("path missing prefix: {0}")]
    Prefix(String),
    /// The path must contain at least one level.
    #[error("path should have at least one level")]
    Empty,
    /// A path level must contain at least one character, it cannot be `//`.
    #[error("path has an empty level: {0}")]
    EmptyLevel(String),
}

impl MappingError {
    pub(crate) fn path(&self) -> &str {
        match self {
            MappingError::Prefix(path) => path,
            MappingError::Empty => "",
            MappingError::EmptyLevel(path) => path,
        }
    }
}

/// Parses the MQTT levels structure of the topic received.
fn parse_mapping(input: &str) -> Result<MappingPath, MappingError> {
    let path = input
        .strip_prefix('/')
        .ok_or_else(|| MappingError::Prefix(input.to_string()))?;

    // Split and check that none are empty
    let levels: Vec<&str> = path
        .split('/')
        .map(|level| {
            if level.is_empty() {
                return Err(MappingError::EmptyLevel(input.to_string()));
            }

            Ok(level)
        })
        .collect::<Result<_, _>>()?;

    if levels.is_empty() {
        return Err(MappingError::Empty);
    }

    Ok(MappingPath {
        path: input,
        levels,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::interface::mapping::endpoint::Level;

    use super::*;

    /// Helper to create a `MappingPath` from a string literal.
    pub(crate) fn mapping(path: &str) -> MappingPath<'_> {
        MappingPath::try_from(path).expect("failed to create mapping path")
    }

    #[test]
    fn endpoint_equals_to_mapping() {
        let expected = Endpoint {
            path: "/%{sensor_id}/boolean_endpoint".into(),
            levels: vec![
                Level::Parameter("sensor_id".to_string()),
                Level::Simple("boolean_endpoint".to_string()),
            ],
        };

        let path = MappingPath::try_from("/1/boolean_endpoint").unwrap();

        assert_eq!(path, expected);
    }

    #[test]
    fn empty_endpoint() {
        let path = MappingPath::try_from("/").unwrap_err();

        assert_eq!(path, MappingError::EmptyLevel("/".into()));
    }

    #[test]
    fn test_compatible() {
        let endpoint = Endpoint {
            path: "/%{sensor_id}/value".into(),
            levels: vec![
                Level::Parameter("sensor_id".to_string()),
                Level::Simple("value".to_string()),
            ],
        };

        assert_eq!(endpoint, mapping("/foo/value"));
        assert_eq!(endpoint, mapping("/bar/value"));
        assert_ne!(endpoint, mapping("/value"));
        assert_ne!(endpoint, mapping("/foo/bar/value"));
        assert_ne!(endpoint, mapping("/foo/value/bar"));
    }

    #[test]
    fn compare_object_with_endpoint() {
        let endpoint = Endpoint {
            path: "/%{sensor_id}/foo/bar",
            levels: vec![
                Level::Parameter("sensor_id"),
                Level::Simple("foo"),
                Level::Simple("bar"),
            ],
        };

        let cases = [
            ((&mapping("/1/foo"), "bar"), Ordering::Equal),
            ((&mapping("/1/foo"), "a"), Ordering::Less),
            ((&mapping("/1"), "foo"), Ordering::Less),
            ((&mapping("/1/foo"), "some"), Ordering::Greater),
            ((&mapping("/1/foo/bar"), "some"), Ordering::Greater),
        ];

        for (mapping, exp) in cases {
            let res = mapping.partial_cmp(&endpoint);
            assert_eq!(res, Some(exp), "wrong compare for {mapping:?}")
        }
    }
}
