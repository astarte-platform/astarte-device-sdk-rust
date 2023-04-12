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
    pub fn validate_with<'this, 'prev>(
        &'this self,
        prev: &'prev Self,
    ) -> Result<&Self, ValidationError> {
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
