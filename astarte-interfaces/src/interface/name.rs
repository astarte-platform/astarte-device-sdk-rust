// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

//! Name of an interface
//!
//! This has to be an unique, alphanumeric reverse internet domain name, shorter than 128
//! characters.

use std::{borrow::Cow, fmt::Display, sync::OnceLock};

use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};

/// Error when parsing an [`InterfaceName`].
#[derive(Debug, thiserror::Error)]
pub enum InterfaceNameError {
    /// Interface name cannot be empty
    #[error("name cannot be empty")]
    Empty,
    /// Interface name must be at most 128 characters
    #[error("it must be shorter than 128 characters: {0}")]
    TooLong(String),
    /// Interface name must be an alphanumeric reverse domain
    #[error("must be an alphanumeric reverse domain: {0}")]
    Invalid(String),
}

/// Name of an interface
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InterfaceName<T = String> {
    inner: T,
}

impl<T> InterfaceName<T> {
    /// Validate an interface name.
    ///
    /// Implements from for a generic `T` with [`AsRef<T>`].
    pub fn from_str_ref(value: T) -> Result<Self, InterfaceNameError>
    where
        T: AsRef<str>,
    {
        static RE: OnceLock<Regex> = OnceLock::new();

        let value_str = value.as_ref();
        if value_str.is_empty() {
            return Err(InterfaceNameError::Empty);
        }

        if value_str.len() > 128 {
            return Err(InterfaceNameError::TooLong(value_str.to_string()));
        }

        let rgx = RE.get_or_init(|| {
            regex::Regex::new(
                "^([a-zA-Z][a-zA-Z0-9]*\\.([a-zA-Z0-9][a-zA-Z0-9-]*\\.)*)?[a-zA-Z][a-zA-Z0-9]*$",
            )
            .expect("should be a valid regex")
        });

        if !rgx.is_match(value_str) {
            return Err(InterfaceNameError::Invalid(value_str.to_string()));
        }

        Ok(Self { inner: value })
    }

    /// Returns a reference to the Interface name.
    pub fn as_str(&self) -> &str
    where
        T: AsRef<str>,
    {
        self.inner.as_ref()
    }

    /// Converts the Interface name inner type into a string.
    pub fn into_string(self) -> InterfaceName<String>
    where
        T: Into<String>,
    {
        InterfaceName {
            inner: self.inner.into(),
        }
    }
}

impl<'a> TryFrom<&'a str> for InterfaceName<&'a str> {
    type Error = InterfaceNameError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Self::from_str_ref(value)
    }
}

impl TryFrom<String> for InterfaceName<String> {
    type Error = InterfaceNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str_ref(value)
    }
}

impl<'a> TryFrom<Cow<'a, str>> for InterfaceName<Cow<'a, str>> {
    type Error = InterfaceNameError;

    fn try_from(value: Cow<'a, str>) -> Result<Self, Self::Error> {
        Self::from_str_ref(value)
    }
}

impl<T> Serialize for InterfaceName<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for InterfaceName<T>
where
    T: Deserialize<'de> + AsRef<str>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let value = T::deserialize(deserializer)?;

        InterfaceName::from_str_ref(value).map_err(D::Error::custom)
    }
}

impl<T> AsRef<str> for InterfaceName<T>
where
    T: AsRef<str>,
{
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<T> From<InterfaceName<T>> for String
where
    T: Into<String>,
{
    fn from(value: InterfaceName<T>) -> Self {
        value.inner.into()
    }
}

impl<'a> From<&'a InterfaceName> for InterfaceName<Cow<'a, str>> {
    fn from(value: &'a InterfaceName) -> Self {
        InterfaceName {
            inner: value.as_ref().into(),
        }
    }
}

impl Display for InterfaceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
