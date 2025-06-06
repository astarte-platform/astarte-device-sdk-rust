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

//! Version of an Interface
//!
//! An interface has a major and minor version to specify the compatibility and changes.

use std::fmt::Display;

/// Error returned by the [`InterfaceVersion`].
#[derive(Debug, thiserror::Error)]
pub enum VersionError {
    /// The version cannot be negative
    #[error("the version cannot be negative: {major}.{minor}")]
    Negative {
        /// The provided major version
        major: i32,
        /// The provided minor version
        minor: i32,
    },
    /// The version of an interface cannot be 0.0
    #[error("the version of an interface cannot be 0.0")]
    Zero,
}

/// Version of an interface.
///
/// This structs validates to follow the interface versioning rules:
///
/// - The major or minor version must an [`i32`]
/// - The major or minor version must be positive
/// - The minor version must be grater than `0>` if the major is `=0`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InterfaceVersion {
    version_major: i32,
    version_minor: i32,
}

impl InterfaceVersion {
    /// Validate the provided interface version
    pub fn try_new(version_major: i32, version_minor: i32) -> Result<Self, VersionError> {
        if version_major.is_negative() || version_minor.is_negative() {
            return Err(VersionError::Negative {
                major: version_major,
                minor: version_minor,
            });
        }

        if version_major == 0 && version_minor == 0 {
            return Err(VersionError::Zero);
        }

        // Checked negativity above
        Ok(Self {
            version_major,
            version_minor,
        })
    }

    /// Returns the major version of the interface
    #[must_use]
    pub fn version_major(&self) -> i32 {
        self.version_major
    }

    /// Returns the minor version of the interface
    #[must_use]
    pub fn version_minor(&self) -> i32 {
        self.version_minor
    }
}

impl Display for InterfaceVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.version_major, self.version_minor)
    }
}

impl From<&InterfaceVersion> for (i32, i32) {
    fn from(value: &InterfaceVersion) -> Self {
        (value.version_major, value.version_minor)
    }
}

impl From<InterfaceVersion> for (i32, i32) {
    fn from(value: InterfaceVersion) -> Self {
        (value.version_major, value.version_minor)
    }
}

impl TryFrom<(i32, i32)> for InterfaceVersion {
    type Error = VersionError;

    fn try_from((major, minor): (i32, i32)) -> Result<Self, Self::Error> {
        Self::try_new(major, minor)
    }
}

impl Default for InterfaceVersion {
    fn default() -> Self {
        Self {
            version_major: 0,
            version_minor: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn check_into() {
        let ver = InterfaceVersion::try_from((0, 1)).unwrap();
        let ver: (i32, i32) = ver.into();

        assert_eq!(ver, (0, 1));
    }

    #[test]
    fn check_negative() {
        InterfaceVersion::try_from((0, -1)).unwrap_err();
        InterfaceVersion::try_from((-1, 0)).unwrap_err();
        InterfaceVersion::try_from((-1, -1)).unwrap_err();
    }

    #[test]
    fn check_zero() {
        InterfaceVersion::try_from((0, 0)).unwrap_err();
    }

    #[test]
    fn check_default() {
        assert_eq!(
            InterfaceVersion::default(),
            InterfaceVersion {
                version_major: 0,
                version_minor: 1
            }
        );
    }

    #[test]
    fn check_getters() {
        let ver = InterfaceVersion::default();

        assert_eq!(ver.version_major(), 0);
        assert_eq!(ver.version_minor(), 1);
    }

    #[test]
    fn check_display() {
        let ver = InterfaceVersion::try_from((0, 1)).unwrap().to_string();
        assert_eq!(ver, "0.1");
    }
}
