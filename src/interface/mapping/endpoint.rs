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

//! Endpoint of an interface mapping.

use std::{
    borrow::Borrow, cmp::Ordering, fmt::Display, ops::Deref, slice::Iter as SliceIter, unreachable,
};

use itertools::{EitherOrBoth, Itertools};
use log::{debug, error, info, trace};

use super::path::MappingPath;

/// A mapping endpoint.
///
/// - It must be unique within the interface
/// - Parameters should be separated by a slash (`/`)
/// - Parameters are equal to any level and each combination of levels should be unique
/// - Two endpoints are equal if they have the same path
/// - The path must start with a slash (`/`)
/// - The minimum length is 2 character
/// - It cannot contain the `+` and `#` characters
/// - A parameter cannot contain the `/` character
///
/// For more information see [Astarte - Docs](https://docs.astarte-platform.org/astarte/latest/030-interface.html#limitations)
///
/// The endpoints uses Cow to not allocate the string if an error occurs.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Endpoint<T> {
    pub(super) path: T,
    pub(super) levels: Vec<Level<T>>,
}

impl<T> Endpoint<T> {
    /// Check that the endpoint is equal to the given mapping
    pub fn eq_mapping<S>(&self, mapping: S) -> bool
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        match MappingPath::try_from(mapping.as_ref()) {
            Ok(mapping) => mapping.eq(self),
            Err(err) => {
                error!("failed to parse mapping {err}");

                false
            }
        }
    }

    /// Iter the levels of the endpoint.
    pub(crate) fn iter(&self) -> SliceIter<Level<T>> {
        self.levels.iter()
    }

    /// Compare the levels with the one of the endpoint.
    pub(crate) fn cmp_levels<U>(&self, levels: &[U]) -> Ordering
    where
        T: AsRef<str>,
        U: AsRef<str>,
    {
        for element in self.iter().zip_longest(levels.iter()) {
            let ordering = match element {
                EitherOrBoth::Left(_) => Ordering::Greater,
                EitherOrBoth::Right(_) => Ordering::Less,
                EitherOrBoth::Both(lvl, o_lvl) => lvl.cmp_str(o_lvl.as_ref()),
            };

            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        Ordering::Equal
    }

    // Returns the number of levels in an endpoint
    pub(crate) fn len(&self) -> usize {
        self.levels.len()
    }

    /// Check that two endpoints are compatible with the same object.
    pub(crate) fn is_same_object(&self, endpoint: &Self) -> bool
    where
        T: PartialEq,
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
            .all(|(level, other_level)| match (level, other_level) {
                (Level::Simple(a), Level::Simple(b)) => a == b,
                (Level::Simple(_), Level::Parameter(_))
                | (Level::Parameter(_), Level::Simple(_)) => false,
                // We do not care about the parameter name, we just need to know that it is a parameter.
                (Level::Parameter(_), Level::Parameter(_)) => true,
            })
    }
}

impl<T> PartialOrd for Endpoint<T>
where
    T: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Endpoint<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.levels.cmp(&other.levels)
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

impl<T> Borrow<str> for Endpoint<T>
where
    T: Borrow<str>,
{
    fn borrow(&self) -> &str {
        self.path.borrow()
    }
}

impl<T> Deref for Endpoint<T>
where
    T: Deref<Target = str>,
{
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl<T> PartialEq<str> for Endpoint<T>
where
    T: PartialEq<str>,
{
    fn eq(&self, other: &str) -> bool {
        self.path.eq(other)
    }
}

impl<T> Display for Endpoint<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl From<Endpoint<&str>> for Endpoint<String> {
    fn from(value: Endpoint<&str>) -> Self {
        Self {
            path: value.path.into(),
            levels: value.levels.into_iter().map(Level::into).collect(),
        }
    }
}

impl<'a, T> PartialEq<MappingPath<'a>> for Endpoint<T>
where
    T: AsRef<str>,
{
    fn eq(&self, other: &MappingPath<'a>) -> bool {
        self.levels == other.levels
    }
}

impl<'a, T> PartialOrd<MappingPath<'a>> for Endpoint<T>
where
    T: AsRef<str>,
{
    fn partial_cmp(&self, other: &MappingPath<'a>) -> Option<Ordering> {
        Some(self.cmp_levels(&other.levels))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum Level<T> {
    Simple(T),
    Parameter(T),
}

impl<T> Level<T> {
    pub(crate) fn cmp_str(&self, other: &str) -> Ordering
    where
        T: AsRef<str>,
    {
        match self {
            Self::Simple(level) => level.as_ref().cmp(other),
            Self::Parameter(_) => Ordering::Equal,
        }
    }

    /// Check if the level is eq to the string
    pub(crate) fn eq_str(&self, other: &str) -> bool
    where
        T: AsRef<str>,
    {
        match self {
            Level::Simple(level) => level.as_ref() == other,
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
            Self::Simple(level) => write!(f, "{}", level),
            // We want to print the parameter as `%{parameter}`. So we escape the `{` and `}`.
            Self::Parameter(level) => write!(f, "%{{{}}}", level),
        }
    }
}

impl<T> PartialOrd for Level<T>
where
    T: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq<str> for Level<T>
where
    T: AsRef<str>,
{
    fn eq(&self, other: &str) -> bool {
        self.eq_str(other)
    }
}

impl<T> PartialOrd<str> for Level<T>
where
    T: AsRef<str>,
{
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        Some(self.cmp_str(other))
    }
}

impl<T> PartialEq<&str> for Level<T>
where
    T: AsRef<str>,
{
    fn eq(&self, other: &&str) -> bool {
        self.eq_str(other)
    }
}

impl<T> PartialOrd<&str> for Level<T>
where
    T: AsRef<str>,
{
    fn partial_cmp(&self, other: &&str) -> Option<Ordering> {
        Some(self.cmp_str(other))
    }
}

impl<T> Ord for Level<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // This could be simplified in an if, but i kept it explicit for clarity.
        match (self, other) {
            (Self::Simple(a), Self::Simple(b)) => a.cmp(b),
            // If any is a parameter, the two levels are equal.
            (Self::Parameter(_), Self::Parameter(_))
            | (Self::Simple(_), Self::Parameter(_))
            | (Self::Parameter(_), Self::Simple(_)) => std::cmp::Ordering::Equal,
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

/// Error that can happen when parsing an endpoint.
#[non_exhaustive]
#[derive(thiserror::Error, Debug, Clone)]
pub enum EndpointError {
    #[error("endpoint must start with a slash, got instead: {0}")]
    Prefix(String),
    #[error("endpoint must contain at least a level: {0}")]
    Empty(String),
    #[error("endpoint contains invalid level: {input}")]
    Level {
        input: String,
        #[source]
        error: LevelError,
    },
}

impl EndpointError {
    pub fn endpoint(&self) -> &str {
        match self {
            EndpointError::Prefix(endpoint) => endpoint,
            EndpointError::Empty(endpoint) => endpoint,
            EndpointError::Level { input, .. } => input,
        }
    }
}

/// Error that can happen when parsing a level.
#[non_exhaustive]
#[derive(thiserror::Error, Debug, Clone)]
pub enum LevelError {
    #[error("levels must not be empty")]
    Empty,
    #[error("levels must not contain MQTT wildcard: {0}")]
    MQTTWildcard(char),
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
    debug!("parsing endpoint: {}", input);

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

    info!("levels: {:?}", levels);

    Ok(Endpoint {
        path: input,
        levels,
    })
}

fn parse_level(input: &str) -> Result<Level<&str>, LevelError> {
    trace!("parsing level: {}", input);

    let level = match parse_parameter(input)? {
        Some(param) => {
            trace!("level is a parameter: {}", param);

            Level::Parameter(param)
        }
        None => {
            let level = parse_simple(input)?;

            trace!("level is simple: {}", level);

            Level::Simple(level)
        }
    };

    Ok(level)
}

fn parse_simple(input: &str) -> Result<&str, LevelError> {
    if input.is_empty() {
        return Err(LevelError::Empty);
    }

    let mut chars = input.chars().peekable();
    while let Some(chr) = chars.next() {
        match chr {
            wildcard @ ('+' | '#') => {
                return Err(LevelError::MQTTWildcard(wildcard));
            }
            '%' if Some('{') == chars.peek().copied() => {
                return Err(LevelError::Parameter);
            }
            '/' => unreachable!("level shouldn't contain '/' since it is used as separator"),
            _ => {}
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
    use super::*;

    impl<T> Endpoint<T> {
        pub(crate) fn eq_strict(&self, other: &Self) -> bool
        where
            T: PartialEq,
        {
            if self.path != other.path {
                return false;
            }

            if self.levels.len() != other.levels.len() {
                return false;
            }

            for (level, other_level) in self.levels.iter().zip(other.levels.iter()) {
                match (level, other_level) {
                    (Level::Simple(a), Level::Simple(b))
                    | (Level::Parameter(a), Level::Parameter(b))
                        if a == b => {}
                    _ => return false,
                }
            }

            true
        }
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
        let res = parse_level("%{test}");

        assert!(
            res.is_ok(),
            "failed to parse level parameter: {}",
            res.unwrap_err()
        );

        let level = res.unwrap();

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
            path: "/a/%{b}/c",
            levels: vec![
                Level::Simple("a"),
                Level::Parameter("b"),
                Level::Simple("c"),
            ],
        };

        assert!(
            endpoint.eq_strict(&expected),
            "endpoint: {:?} != {:?}",
            endpoint,
            expected
        );
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
            path: "/%{a}/b/c",
            levels: vec![
                Level::Parameter("a"),
                Level::Simple("b"),
                Level::Simple("c"),
            ],
        };

        assert!(
            endpoint.eq_strict(&expected),
            "endpoint: {:?} != {:?}",
            endpoint,
            expected
        );
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
            path: "/a/%{b}/c/%{d}/e",
            levels: vec![
                Level::Simple("a"),
                Level::Parameter("b"),
                Level::Simple("c"),
                Level::Parameter("d"),
                Level::Simple("e"),
            ],
        };

        assert!(
            endpoint.eq_strict(&expected),
            "endpoint: {:?} != {:?}",
            endpoint,
            expected
        );
    }

    #[test]
    fn test_parse_endpoint_parameters() {
        let cases = [
            (
                "/%{sensor_id}/boolean_endpoint",
                Endpoint {
                    path: "/%{sensor_id}/boolean_endpoint",
                    levels: vec![
                        Level::Parameter("sensor_id"),
                        Level::Simple("boolean_endpoint"),
                    ],
                },
            ),
            (
                "/%{sensor_id}/enable",
                Endpoint {
                    path: "/%{sensor_id}/enable",
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

            assert!(
                endpoint.eq_strict(&expected),
                "endpoint: {:?} != {:?}",
                endpoint,
                expected
            );
        }
    }
}
