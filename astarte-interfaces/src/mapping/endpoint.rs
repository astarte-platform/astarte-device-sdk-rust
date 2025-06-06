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

//! Endpoint of an interface mapping.

use std::{fmt::Display, slice::Iter as SliceIter, str::FromStr};

use tracing::{error, trace};

use super::path::MappingPath;

/// The maximum length of an endpoint must be 64 levels
pub const ENDPOINT_MAX_LEN: usize = 64;

/// A mapping endpoint.
///
/// - It must be unique within the interface
/// - Parameters should be separated by a slash (`/`)
/// - Parameters are equal to any level and each combination of levels should be unique
/// - Two endpoints are equal if they have the same path
/// - The path must start with a slash (`/`)
/// - The minimum length is 2 character
/// - Each level should start with an ascii letter `[a-zA-Z]`
/// - A level or parameter can only container ascii alpha-numeric character or an underscore
///   `[a-zA-Z0-9_]`
/// - A parameter cannot contain the `/` character
///
/// For more information see [Astarte - Docs](https://docs.astarte-platform.org/astarte/latest/030-interface.html#limitations)
///
/// The endpoints uses Cow to not allocate the string if an error occurs.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct Endpoint<T = String> {
    levels: Vec<Level<T>>,
}

impl<T> Endpoint<T> {
    /// Iter the levels of the endpoint.
    pub fn iter(&self) -> SliceIter<'_, Level<T>> {
        self.levels.iter()
    }

    /// Compare the levels with the one of the endpoint.
    #[must_use]
    pub fn eq_mapping<'a>(&self, mapping: &MappingPath<'a>) -> bool
    where
        T: PartialEq<&'a str> + Eq,
    {
        if self.len() != mapping.len() {
            return false;
        }

        self.iter()
            .zip(mapping.levels.iter())
            .all(|(endpoint_level, path_level)| endpoint_level == path_level)
    }

    // Check if a path is the one of an object endpoint.
    pub(crate) fn eq_object_field<'a>(&self, path: &'a str) -> bool
    where
        T: PartialEq<&'a str> + Eq,
    {
        let last = self.levels.last();
        debug_assert!(
            last.is_some(),
            "an endpoint should always have at least an endpoint"
        );

        last.is_some_and(|endpoint_level| *endpoint_level == path)
    }

    // Check if a path is the one of an object endpoint.
    pub(crate) fn is_object_path<'a>(&self, path: &MappingPath<'a>) -> bool
    where
        T: PartialEq<&'a str> + Eq,
    {
        // Must have the same size -1.
        if self.len().saturating_sub(1) != path.len() {
            return false;
        }

        // This will skip the last one for the endpoint for the check above
        self.iter()
            .zip(path.levels.iter())
            .all(|(endpoint_level, path_level)| match endpoint_level {
                Level::Simple(level) => level == path_level,
                Level::Parameter(_) => true,
            })
    }

    // Returns the number of levels in an endpoint
    pub(crate) fn len(&self) -> usize {
        self.levels.len()
    }

    /// Check that two endpoints are compatible with the same object.
    ///
    // https://docs.astarte-platform.org/astarte/latest/030-interface.html#endpoints-and-aggregation
    pub(crate) fn is_same_object(&self, endpoint: &Self) -> bool
    where
        T: PartialEq + Eq,
    {
        if self.len() != endpoint.len() {
            return false;
        }

        // Iterate over the levels of the two endpoints, except the last one that is the object key.
        self.levels
            .iter()
            .zip(endpoint.levels.iter())
            .rev()
            .skip(1)
            .all(|(level, other_level)| level == other_level)
    }
}

impl<'a> TryFrom<&'a str> for Endpoint<&'a str> {
    type Error = EndpointError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        parse_endpoint(value)
    }
}

impl TryFrom<&str> for Endpoint<String> {
    type Error = EndpointError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Endpoint::<&str>::try_from(value).map(Endpoint::into)
    }
}

impl FromStr for Endpoint<String> {
    type Err = EndpointError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl<T> Display for Endpoint<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for level in &self.levels {
            write!(f, "/{level}")?;
        }

        Ok(())
    }
}

impl From<Endpoint<&str>> for Endpoint<String> {
    fn from(value: Endpoint<&str>) -> Self {
        Self {
            levels: value.levels.into_iter().map(Level::into).collect(),
        }
    }
}

impl<'a, T> PartialEq<MappingPath<'a>> for Endpoint<T>
where
    T: for<'b> PartialEq<&'b str> + Eq,
{
    fn eq(&self, other: &MappingPath<'a>) -> bool {
        self.eq_mapping(other)
    }
}

impl<'a, T> IntoIterator for &'a Endpoint<T> {
    type Item = &'a Level<T>;
    type IntoIter = std::slice::Iter<'a, Level<T>>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Level of an [`Endpoint`].
///
/// # Example
///
/// ```rust
/// # use astarte_interfaces::{Endpoint, mapping::endpoint::Level};
/// let endpoint = Endpoint::try_from("/sensor/%{name}/id").unwrap();
/// let mut iter = endpoint.iter();
///
/// assert_eq!(iter.next(), Some(&Level::Simple("sensor")));
/// assert_eq!(iter.next(), Some(&Level::Parameter("name")));
/// assert_eq!(iter.next(), Some(&Level::Simple("id")));
/// assert_eq!(iter.next(), None);
/// ```
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub enum Level<T> {
    /// Simple level without parameters
    Simple(T),
    /// Parameter level enclosed in `%{bar}`.
    Parameter(T),
}

impl<T> Level<T> {
    /// This prevents mistakes for recursion in the PartialEq traits.
    fn eq_str<'a>(&self, other: &'a str) -> bool
    where
        T: PartialEq<&'a str> + Eq,
    {
        match self {
            Level::Simple(level) => *level == other,
            Level::Parameter(_) => true,
        }
    }
}

impl<T> Display for Level<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Simple(level) => write!(f, "{level}"),
            // We want to print the parameter as `%{parameter}`. So we escape the `{` and `}`.
            Self::Parameter(level) => write!(f, "%{{{level}}}"),
        }
    }
}

impl From<Level<&str>> for Level<String> {
    fn from(value: Level<&str>) -> Self {
        match value {
            Level::Simple(simple) => Level::Simple(simple.into()),
            Level::Parameter(param) => Level::Parameter(param.into()),
        }
    }
}

impl<'a, T> PartialEq<&'a str> for Level<T>
where
    T: PartialEq<&'a str> + Eq,
{
    fn eq(&self, other: &&'a str) -> bool {
        self.eq_str(other)
    }
}

impl<'a, T> PartialEq<Level<T>> for &'a str
where
    T: PartialEq<&'a str> + Eq,
{
    fn eq(&self, other: &Level<T>) -> bool {
        other.eq_str(self)
    }
}

/// Error that can happen when parsing an endpoint.
#[non_exhaustive]
#[derive(thiserror::Error, Debug, Clone)]
pub enum EndpointError {
    /// Missing forward slash at the beginning of the endpoint.
    #[error("endpoint must start with a slash, got instead: {0}")]
    Prefix(String),
    /// Endpoints must contain at least one level.
    ///
    /// The empty endpoint is reserved.
    #[error("endpoint must contain at least a level: {0}")]
    Empty(String),
    /// Endpoints must have at most 64 levels
    #[error("endpoint must contain at most 64 levels, but it has {0}")]
    MaxLen(usize),
    /// Couldn't parse the endpoint's level.
    #[error("endpoint contains invalid level: {input}")]
    Level {
        /// The original endpoint.
        input: String,
        /// Reason for the invalid level.
        #[source]
        error: LevelError,
    },
}

/// Error that can happen when parsing a level.
#[non_exhaustive]
#[derive(thiserror::Error, Debug, Clone)]
pub enum LevelError {
    /// The level must contain at least one character, it cannot be `//`.
    #[error("levels must not be empty")]
    Empty,
    /// Invalid character in the level.
    #[error("levels contain an invalid character {0}")]
    InvalidCharacter(char),
    /// Mixed characters and parameter in level.
    ///
    /// A parameter should incapsulate the whole level (e.g. `/foo%{bar}` is invalid).
    #[error("the parameter should incapsulate the whole level")]
    Parameter,
}

/// Parses an interface endpoint with the following grammar:
///
/// ```text
/// endpoint: '/' level+
/// # We don't allow ending the endpoint with a '/'
/// level: (parameter | simple ) ('/' | EOF)
///
/// # A parameter is an escaped simple level
/// parameter: '%{' simple '}
///
/// simple: simple_part+
/// # Make sure there is no parameter inside by escaping the '{'.
/// # This grammar will not parse a '%' alone at the end of level.
/// simple_part: '%' escape_param | level_char
///
/// # Any UTF-8 character except
/// # - '/' for the level
/// # - '+' and '#' since they are MQTT wild card
/// # - '%' since it is used to escape a parameter
/// level_char: [^/+#%]
/// # Same as level_char, but disallowing the '{'
/// escape_param: [^/+#%{]
/// ```
///
/// Our implementation differs from the grammar in the following ways:
///
/// - We allow ending the level with a '%' since we can peek
///
fn parse_endpoint(input: &str) -> Result<Endpoint<&str>, EndpointError> {
    trace!("parsing endpoint: {}", input);

    let endpoint = input
        .strip_prefix('/')
        .ok_or_else(|| EndpointError::Prefix(input.to_string()))?;

    let levels = endpoint
        .split('/')
        .map(parse_level)
        .collect::<Result<Vec<_>, LevelError>>()
        .map_err(|error| EndpointError::Level {
            input: input.to_string(),
            error,
        })?;

    if levels.is_empty() {
        return Err(EndpointError::Empty(input.to_string()));
    }

    if levels.len() > ENDPOINT_MAX_LEN {
        return Err(EndpointError::MaxLen(levels.len()));
    }

    trace!("levels: {:?}", levels);

    Ok(Endpoint { levels })
}

fn parse_level(input: &str) -> Result<Level<&str>, LevelError> {
    trace!("parsing level: {}", input);

    let level = if let Some(param) = parse_parameter(input)? {
        trace!("level is a parameter: {}", param);

        Level::Parameter(param)
    } else {
        let level = parse_simple(input)?;

        trace!("level is simple: {}", level);

        Level::Simple(level)
    };

    Ok(level)
}

fn parse_simple(input: &str) -> Result<&str, LevelError> {
    let mut chars = input.chars().peekable();

    match chars.next() {
        Some('a'..='z' | 'A'..='Z') => {}
        Some(c) => return Err(LevelError::InvalidCharacter(c)),
        None => return Err(LevelError::Empty),
    }

    while let Some(chr) = chars.next() {
        match chr {
            '%' if Some('{') == chars.peek().copied() => {
                return Err(LevelError::Parameter);
            }
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' => {}
            c => return Err(LevelError::InvalidCharacter(c)),
        }
    }

    Ok(input)
}

fn parse_parameter(input: &str) -> Result<Option<&str>, LevelError> {
    let parameter = input
        .strip_prefix("%{")
        .and_then(|input| input.strip_suffix('}'));

    let name = match parameter {
        Some(param) => {
            let name = parse_simple(param)?;

            Some(name)
        }
        None => None,
    };

    Ok(name)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn endpoint_equals_to_mapping() {
        let endpoint = Endpoint {
            levels: vec![
                Level::Parameter("sensor_id".to_string()),
                Level::Simple("boolean_endpoint".to_string()),
            ],
        };

        let path = MappingPath::try_from("/1/boolean_endpoint").unwrap();

        assert!(endpoint.eq_mapping(&path));
    }

    #[test]
    fn test_parse_parameter() {
        let res = parse_parameter("%{test}");

        assert!(
            res.is_ok(),
            "failed to parse parameter: {}",
            res.unwrap_err()
        );

        let parameter = res.unwrap();

        assert_eq!(parameter, Some("test"));
    }

    #[test]
    fn test_parse_level_parameter() {
        let level = parse_level("%{test}").unwrap();

        assert_eq!(level, Level::Parameter("test"));
    }

    #[test]
    fn test_parse_endpoint() {
        let res = parse_endpoint("/a/%{b}/c");

        assert!(
            res.is_ok(),
            "failed to parse endpoint: {}",
            res.unwrap_err()
        );

        let endpoint = res.unwrap();

        let expected = Endpoint {
            levels: vec![
                Level::Simple("a"),
                Level::Parameter("b"),
                Level::Simple("c"),
            ],
        };

        assert_eq!(endpoint, expected);
    }

    #[test]
    fn test_parse_endpoint_first() {
        let res = parse_endpoint("/%{a}/b/c");

        assert!(
            res.is_ok(),
            "failed to parse endpoint: {}",
            res.unwrap_err()
        );

        let endpoint = res.unwrap();

        let expected = Endpoint {
            levels: vec![
                Level::Parameter("a"),
                Level::Simple("b"),
                Level::Simple("c"),
            ],
        };

        assert_eq!(endpoint, expected);
    }

    #[test]
    fn test_parse_endpoint_multi() {
        let res = parse_endpoint("/a/%{b}/c/%{d}/e");

        assert!(
            res.is_ok(),
            "failed to parse endpoint: {}",
            res.unwrap_err()
        );

        let endpoint = res.unwrap();

        let expected = Endpoint {
            levels: vec![
                Level::Simple("a"),
                Level::Parameter("b"),
                Level::Simple("c"),
                Level::Parameter("d"),
                Level::Simple("e"),
            ],
        };

        assert_eq!(endpoint, expected);
    }

    #[test]
    fn test_parse_endpoint_parameters() {
        let cases = [
            (
                "/%{sensor_id}/boolean_endpoint",
                Endpoint {
                    levels: vec![
                        Level::Parameter("sensor_id"),
                        Level::Simple("boolean_endpoint"),
                    ],
                },
            ),
            (
                "/%{sensor_id}/enable",
                Endpoint {
                    levels: vec![Level::Parameter("sensor_id"), Level::Simple("enable")],
                },
            ),
        ];

        for (endpoint, expected) in cases {
            let res = parse_endpoint(endpoint);

            assert!(
                res.is_ok(),
                "failed to parse endpoint: {}",
                res.unwrap_err()
            );

            let endpoint = res.unwrap();

            assert_eq!(endpoint, expected);
        }
    }

    #[test]
    fn object_equality() {
        let endpoint = Endpoint {
            levels: vec![
                Level::Parameter("sensor_id".to_string()),
                Level::Simple("boolean_endpoint".to_string()),
            ],
        };

        let path = MappingPath::try_from("/1/boolean_endpoint").unwrap();

        assert!(!endpoint.is_object_path(&path));

        let path = MappingPath::try_from("/1").unwrap();
        assert!(endpoint.is_object_path(&path));
    }

    #[test]
    fn object_field() {
        let endpoint = Endpoint {
            levels: vec![
                Level::Parameter("sensor_id".to_string()),
                Level::Simple("boolean_endpoint".to_string()),
            ],
        };

        assert!(endpoint.eq_object_field("boolean_endpoint"));
        assert!(!endpoint.eq_object_field("foo"));
    }

    #[test]
    fn level_eq_str() {
        let param = Level::Parameter("sensor_id".to_string());

        assert_eq!(param, "some");
        assert_eq!(param, "foo");

        let simple = Level::Simple("boolean_endpoint".to_string());

        assert_eq!(simple, "boolean_endpoint");
        assert_ne!(simple, "foo");
    }
}
