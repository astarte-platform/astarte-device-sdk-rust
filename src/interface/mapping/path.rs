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
//!
//! It will be used to access the [`crate::interface::Mapping`] in an
//! [`crate::interface::Interface`]. Since the mapping is a tree, the path will compared to the
//! [`crate::interface::mapping::endpoint::Endpoint`] of the mapping.

use std::{
    borrow::{Borrow, Cow},
    fmt::Display,
};

use super::endpoint::Endpoint;

/// Path of a mapping in interface.
///
/// This is used to access the [`crate::interface::Interface`] so we can compare the parsed [`MappingPath::Mapping`]
/// with the [`crate::interface::mapping::endpoint::Endpoint`].
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum MappingPath<'a> {
    /// The parsed MQTT levels structure of the topic received.
    Mapping {
        /// Needs to be a [`Cow<'a, str>`], because if a full path is passed, we can avoid allocating
        /// by using the [`Cow::Borrowed`]. While for the object, two different [`&str`] are passed
        /// and we need to allocate a string for [`Cow::Owned`].
        path: Cow<'a, str>,
        levels: Vec<&'a str>,
    },
    /// The [`crate::interface::Mapping`] endpoint of an [`crate::interface::Interface`]'s.
    Endpoint(Endpoint<'a>),
}

impl<'a> MappingPath<'a> {
    pub(crate) fn as_str(&self) -> &str {
        self.borrow()
    }
}

impl<'a> Display for MappingPath<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MappingPath::Mapping { path, .. } => write!(f, "{}", path),
            MappingPath::Endpoint(endpoint) => write!(f, "{}", endpoint),
        }
    }
}

impl<'a> TryFrom<&'a str> for MappingPath<'a> {
    type Error = MappingError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        parse_mapping(value)
    }
}

impl<'a> PartialOrd for MappingPath<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for MappingPath<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (MappingPath::Mapping { levels, .. }, MappingPath::Mapping { levels: o_lvs, .. }) => {
                levels.cmp(o_lvs)
            }
            (MappingPath::Endpoint(endpoint), MappingPath::Endpoint(o_endpoint)) => {
                endpoint.cmp(o_endpoint)
            }
            (MappingPath::Mapping { levels, .. }, MappingPath::Endpoint(endpoint)) => {
                endpoint.cmp_levels(levels).reverse()
            }
            (MappingPath::Endpoint(endpoint), MappingPath::Mapping { levels, .. }) => {
                endpoint.cmp_levels(levels)
            }
        }
    }
}

impl<'a> From<Endpoint<'a>> for MappingPath<'a> {
    fn from(endpoint: Endpoint<'a>) -> Self {
        MappingPath::Endpoint(endpoint)
    }
}

impl<'a> Borrow<str> for MappingPath<'a> {
    fn borrow(&self) -> &str {
        match self {
            MappingPath::Mapping { path, .. } => path,
            MappingPath::Endpoint(endpoint) => endpoint,
        }
    }
}

impl<'a> PartialEq<str> for MappingPath<'a> {
    fn eq(&self, other: &str) -> bool {
        match self {
            MappingPath::Mapping { path, .. } => path == other,
            MappingPath::Endpoint(endpoint) => endpoint == other,
        }
    }
}

impl PartialEq<&str> for MappingPath<'_> {
    fn eq(&self, &other: &&str) -> bool {
        self == other
    }
}

/// Error that can happen while parsing the MQTT levels structure of the topic received.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, thiserror::Error)]
pub enum MappingError {
    #[error("path missing prefix: {0}")]
    Prefix(String),
    #[error("path should have at least one level")]
    Empty,
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

    Ok(MappingPath::Mapping {
        path: Cow::Borrowed(input),
        levels,
    })
}

#[cfg(test)]
mod tests {
    use crate::interface::mapping::endpoint::Level;

    use super::*;

    /// Helper macro to create a `MappingPath` from a string literal.
    #[macro_export]
    macro_rules! mapping {
        ($mapping:expr) => {
            &$crate::MappingPath::try_from($mapping).expect("failed to create mapping path")
        };
    }

    #[test]
    fn endpoint_equals_to_mapping() {
        let expected = MappingPath::Endpoint(Endpoint {
            path: "/%{sensor_id}/boolean_endpoint".into(),
            levels: vec![
                Level::Parameter(Cow::from("sensor_id")),
                Level::Simple(Cow::from("boolean_endpoint")),
            ],
        });

        let path = MappingPath::try_from("/1/boolean_endpoint").unwrap();

        assert!(path.cmp(&expected).is_eq());
    }

    #[test]
    fn empty_endpoint() {
        let path = MappingPath::try_from("/").unwrap_err();

        assert_eq!(path, MappingError::EmptyLevel("/".into()));
    }

    #[test]
    fn test_compatible() {
        let endpoint = MappingPath::Endpoint(Endpoint {
            path: "/%{sensor_id}/value".into(),
            levels: vec![
                Level::Parameter(Cow::from("sensor_id")),
                Level::Simple(Cow::from("value")),
            ],
        });

        assert!(endpoint.cmp(mapping!("/foo/value")).is_eq());
        assert!(endpoint.cmp(mapping!("/bar/value")).is_eq());
        assert!(endpoint.cmp(mapping!("/value")).is_ne());
        assert!(endpoint.cmp(mapping!("/foo/bar/value")).is_ne());
        assert!(endpoint.cmp(mapping!("/foo/value/bar")).is_ne());
    }
}
