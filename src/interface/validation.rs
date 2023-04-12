// This file is part of Astarte.
// Copyright 2023 SECO Mind Srl
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

use super::{
    error::{Error, ValidationError, VersionChangeError},
    Interface,
};

/// A change in version of an interface.
#[derive(Debug, Clone, Copy)]
pub struct VersionChange {
    pub next_major: i32,
    pub next_minor: i32,
    pub prev_major: i32,
    pub prev_minor: i32,
}

impl VersionChange {
    pub fn new(next: &Interface, prev: &Interface) -> Self {
        Self {
            next_major: next.get_version_minor(),
            next_minor: next.get_version_minor(),
            prev_major: prev.get_version_major(),
            prev_minor: prev.get_version_minor(),
        }
    }

    pub fn validate(&self) -> Result<(), VersionChangeError> {
        let majior = self.next_major.cmp(&self.prev_major);
        let minor = self.next_minor.cmp(&self.prev_minor);

        match (majior, minor) {
            (Ordering::Less, _) => Err(VersionChangeError::MajorDecresed(*self)),
            (Ordering::Equal, Ordering::Less) => Err(VersionChangeError::MinorDecresed(*self)),
            (Ordering::Equal, Ordering::Equal) => Err(VersionChangeError::SameVersion(*self)),
            _ => Ok(()),
        }
    }
}

impl Display for VersionChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "next: {}.{} prev: {}.{}",
            self.next_major, self.next_minor, self.prev_major, self.prev_minor
        )
    }
}

impl Interface {
    /// Validate if an interface is valid
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: add additional validation
        if self.get_version_major() == 0 && self.get_version_minor() == 0 {
            return Err(Error::MajorMinor);
        }
        Ok(())
    }

    /// Validate if an interface is given the previous version `prev`.
    ///
    /// It will check whether:
    ///
    /// - Both the versions are valid
    /// - The name of the interface is the same
    /// - The new version is a valid successor of the previous version.
    pub fn validate_with(&self, prev: &Self) -> Result<&Self, ValidationError> {
        if self == prev {
            return Ok(self);
        }

        self.validate().map_err(ValidationError::InvalidNew)?;

        prev.validate().map_err(ValidationError::InvalidPrev)?;

        let name = self.get_name();
        let prev_name = prev.get_name();
        if name != prev_name {
            return Err(ValidationError::NameMismatch {
                name: name.into(),
                prev_name: prev_name.into(),
            });
        }

        VersionChange::new(self, prev)
            .validate()
            .map_err(ValidationError::Version)
            .map(|()| self)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::{interface::Error, Interface};

    #[test]
    fn validation_test() {
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

        assert!(matches!(deser_interface, Err(Error::MajorMinor)));
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

        assert!(new_interface.validate_with(&prev_interface).is_ok())
    }

    #[test]
    fn validate_version() {
        let make_interface = |major, minor| {
            Interface::from_str(&format!(
                r#"
        {{
            "interface_name": "org.astarte-platform.genericsensors.Values",
            "version_major": {major},
            "version_minor": {minor},
            "type": "datastream",
            "ownership": "device",
            "description": "Interface description",
            "doc": "Interface doc",
            "mappings": []
        }}"#
            ))
            .unwrap()
        };

        let interfaces = [
            (make_interface(1, 0), make_interface(1, 1), true),
            (make_interface(2, 1), make_interface(1, 1), false),
            (make_interface(1, 2), make_interface(1, 1), false),
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
}
