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

//! Path of a mapping in interface. It's the parsed struct path received from the MQTT levels
//! structure of the topic received.

use std::fmt::Display;

/// Path of a mapping in interface.
///
/// This is used to access the [`Interface`](crate::interface::Interface) so we can compare the parsed [`MappingPath`]
/// with the [`Endpoint`](crate::Endpoint).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MappingPath<'a> {
    pub(crate) path: &'a str,
    pub(crate) levels: Vec<&'a str>,
}

impl MappingPath<'_> {
    /// Returns the mapping as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.path
    }

    /// Returns the mapping length.
    #[must_use]
    pub fn len(&self) -> usize {
        self.levels.len()
    }

    /// Returns true if the path has no levels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }
}

impl Display for MappingPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl<'a> TryFrom<&'a str> for MappingPath<'a> {
    type Error = MappingPathError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        parse_mapping(value)
    }
}

/// Error that can happen while parsing the MQTT levels structure of the topic received.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, thiserror::Error)]
pub enum MappingPathError {
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

/// Parses the MQTT levels structure of the topic received.
fn parse_mapping(input: &str) -> Result<MappingPath<'_>, MappingPathError> {
    let path = input
        .strip_prefix('/')
        .ok_or_else(|| MappingPathError::Prefix(input.to_string()))?;

    // Split and check that none are empty
    let levels: Vec<&str> = path
        .split('/')
        .map(|level| {
            if level.is_empty() {
                return Err(MappingPathError::EmptyLevel(input.to_string()));
            }

            Ok(level)
        })
        .collect::<Result<_, _>>()?;

    if levels.is_empty() {
        return Err(MappingPathError::Empty);
    }

    Ok(MappingPath {
        path: input,
        levels,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_endpoint() {
        let path = MappingPath::try_from("/").unwrap_err();

        assert_eq!(path, MappingPathError::EmptyLevel("/".into()));
    }

    #[test]
    fn getters_success() {
        let value = "/some/path";
        let path = MappingPath::try_from(value).unwrap();

        assert_eq!(path.as_str(), value);
        assert_eq!(path.len(), 2);
        assert!(!path.is_empty());
        assert_eq!(path.to_string(), value);
    }

    #[test]
    fn parse_mappings_success() {
        let cases = [
            "/foo/value",
            "/bar/value",
            "/value",
            "/foo/bar/valu",
            "/foo/value/ba",
        ];

        for case in cases {
            MappingPath::try_from(case).unwrap_or_else(|err| panic!("failed for {case}: {err}"));
        }
    }

    #[test]
    fn parse_mappings_error() {
        let err = MappingPath::try_from("/").unwrap_err();

        assert!(matches!(err, MappingPathError::EmptyLevel(_)), "{err:?}");
    }
}
