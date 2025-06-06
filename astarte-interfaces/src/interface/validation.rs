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

//! Validate an interface and the version change between two interfaces.

use std::{cmp::Ordering, fmt::Display};

use super::Interface;

/// Error for changing the version of an interface.
#[non_exhaustive]
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum VersionChangeError {
    /// The major version cannot be decreased.
    #[error("the major version decreased: {0}")]
    MajorDecreased(VersionChange),
    /// The minor version cannot be decreased.
    #[error("the minor version decreased: {0}")]
    MinorDecreased(VersionChange),
    /// The interface is different but the version did not change.
    #[error("the version did not change: {0}")]
    SameVersion(VersionChange),
}

/// A change in version of an interface.
///
/// This structure is used to validate that the new version of an interface is a valid successor of
/// the previous version. The version cannot decrease, and they cannot be the same (not really a
/// version change).
///
/// This validates only the version change, not the interface itself. For that, see [`Interface::validate_with`].
#[derive(Debug, Clone, Copy)]
pub struct VersionChange {
    next_major: i32,
    next_minor: i32,
    prev_major: i32,
    prev_minor: i32,
}

impl VersionChange {
    /// Create a new version change from a new and previous interfaces.
    pub fn try_new(next: &Interface, prev: &Interface) -> Result<Self, VersionChangeError> {
        let change = Self {
            next_major: next.version_major(),
            next_minor: next.version_minor(),
            prev_major: prev.version_major(),
            prev_minor: prev.version_minor(),
        };

        change.validate()
    }

    /// Returns the previous version
    #[must_use]
    pub fn previous(&self) -> (i32, i32) {
        (self.prev_major, self.prev_minor)
    }

    /// Returns the next version
    #[must_use]
    pub fn next(&self) -> (i32, i32) {
        (self.next_major, self.next_minor)
    }

    /// Private method for a version change validation.
    ///
    /// Validate if the version change is valid.
    pub fn validate(self) -> Result<Self, VersionChangeError> {
        let major = self.next_major.cmp(&self.prev_major);
        let minor = self.next_minor.cmp(&self.prev_minor);

        match (major, minor) {
            (Ordering::Less, _) => Err(VersionChangeError::MajorDecreased(self)),
            (Ordering::Equal, Ordering::Less) => Err(VersionChangeError::MinorDecreased(self)),
            (Ordering::Equal, Ordering::Equal) => Err(VersionChangeError::SameVersion(self)),
            _ => Ok(self),
        }
    }
}

impl Display for VersionChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{} -> {}.{}",
            self.prev_major, self.prev_minor, self.next_major, self.next_minor
        )
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::Interface;

    #[test]
    fn version_change() {
        let prev = make_interface(1, 2);
        let next = make_interface(2, 2);

        let change = super::VersionChange::try_new(&next, &prev);

        assert!(change.is_ok());

        let change = change.unwrap();

        assert_eq!(change.previous(), (1, 2));
        assert_eq!(change.next(), (2, 2));

        assert_eq!(change.to_string(), "1.2 -> 2.2");
    }

    #[test]
    fn validation_test() {
        // Both major and minor are 0
        let interface_json = r#"
        {
            "interface_name": "org.astarte-platform.genericsensors.Values",
            "version_major": 0,
            "version_minor": 0,
            "type": "datastream",
            "ownership": "device",
            "description": "Interface description",
            "doc": "Interface doc",
            "mappings": [
                {
                    "endpoint": "/%{sensor_id}/value",
                    "type": "double",
                    "explicit_timestamp": true,
                    "description": "Mapping description",
                    "doc": "Mapping doc"
                },
                {
                    "endpoint": "/%{sensor_id}/otherValue",
                    "type": "longinteger",
                    "explicit_timestamp": true,
                    "description": "Mapping description",
                    "doc": "Mapping doc"
                }
            ]
        }"#;

        let deser_interface = Interface::from_str(interface_json);

        assert!(deser_interface.is_err());
    }

    #[test]
    fn validate_same_interface() {
        let prev_interface = Interface::from_str(
            r#"
        {
            "interface_name": "org.astarte-platform.genericsensors.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "datastream",
            "ownership": "device",
            "description": "Interface description",
            "doc": "Interface doc",
            "mappings": [
                {
                    "endpoint": "/%{sensor_id}/value",
                    "type": "double",
                    "explicit_timestamp": true,
                    "description": "Mapping description",
                    "doc": "Mapping doc"
                },
                {
                    "endpoint": "/%{sensor_id}/otherValue",
                    "type": "longinteger",
                    "explicit_timestamp": true,
                    "description": "Mapping description",
                    "doc": "Mapping doc"
                }
            ]
        }"#,
        )
        .unwrap();

        let new_interface = Interface::from_str(
            r#"
        {
            "interface_name": "org.astarte-platform.genericsensors.Values",
            "version_major": 1,
            "version_minor": 0,
            "type": "datastream",
            "ownership": "device",
            "description": "Interface description",
            "doc": "Interface doc",
            "mappings": [
                {
                    "endpoint": "/%{sensor_id}/value",
                    "type": "double",
                    "explicit_timestamp": true,
                    "description": "Mapping description",
                    "doc": "Mapping doc"
                },
                {
                    "endpoint": "/%{sensor_id}/otherValue",
                    "type": "longinteger",
                    "explicit_timestamp": true,
                    "description": "Mapping description",
                    "doc": "Mapping doc"
                }
            ]
        }"#,
        )
        .unwrap();

        assert!(new_interface.validate_with(&prev_interface).is_ok());
    }

    #[test]
    fn validate_version() {
        let interfaces = [
            (make_interface(1, 0), make_interface(1, 1), true),
            (make_interface(2, 1), make_interface(1, 1), false),
            (make_interface(1, 2), make_interface(1, 1), false),
            // Same interface
            (make_interface(1, 1), make_interface(1, 1), true),
        ];

        for (prev, new, expected) in interfaces {
            let res = new.validate_with(&prev);

            assert_eq!(
                res.is_ok(),
                expected,
                "expected to {}: {:?}",
                if expected { "pass" } else { "fail" },
                res
            );
        }
    }

    fn make_interface(major: i32, minor: i32) -> Interface {
        Interface::from_str(&format!(
            r#"{{
            "interface_name": "org.astarte-platform.genericsensors.Values",
            "version_major": {major},
            "version_minor": {minor},
            "type": "datastream",
            "ownership": "device",
            "description": "Interface description",
            "doc": "Interface doc",
            "mappings": [{{
                "endpoint": "/value",
                "type": "double",
                "description": "Mapping description",
                "doc": "Mapping doc"
            }}]
        }}"#
        ))
        .unwrap()
    }
}
