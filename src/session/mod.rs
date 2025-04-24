// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Handles storing the current introspection to mantain
//! a persistent session with the astarte mqtt server.

use std::future::Future;

use itertools::Itertools;

use crate::{error::DynError, interfaces::Interfaces, store::MissingCapability, Interface};

mod sqlite;

/// Interface data associated with the astarte introspection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntrospectionInterface {
    /// Name of the interface.
    pub name: String,
    /// Major version.
    pub version_major: i32,
    /// Minor version.
    pub version_minor: i32,
}

/// Error returned by the retention.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SessionError {
    /// Error in the store introspection method
    #[error("couldn't store introspection")]
    StoreIntrospection(#[source] DynError),
    /// Error in the clear introspection method
    #[error("couldn't clear the introspection")]
    ClearIntrospection(#[source] DynError),
    /// Error in the load introspection method
    #[error("couldn't load the introspection")]
    LoadIntrospection(#[source] DynError),
    /// Error in the remove introspection method
    #[error("couldn't remove the interfaces")]
    RemoveInterfaces(#[source] DynError),
}

impl SessionError {
    fn store_introspection(err: impl Into<DynError>) -> Self {
        Self::StoreIntrospection(err.into())
    }

    fn clear_introspection(err: impl Into<DynError>) -> Self {
        Self::ClearIntrospection(err.into())
    }

    fn load_introspection(err: impl Into<DynError>) -> Self {
        Self::LoadIntrospection(err.into())
    }

    fn remove_interfaces(err: impl Into<DynError>) -> Self {
        Self::RemoveInterfaces(err.into())
    }
}

/// Trait for persistently storing and managing session-related data.
pub trait StoredSession: Clone + Send + Sync {
    /// Adds a slice of `IntrospectionInterface` to the persistent store.
    fn add_interfaces(
        &self,
        interfaces: &[IntrospectionInterface],
    ) -> impl Future<Output = Result<(), SessionError>> + Send;

    /// Clears all `IntrospectionInterface`s from the persistent store.
    fn clear_introspection(&self) -> impl Future<Output = Result<(), SessionError>> + Send;

    /// Loads all stored `IntrospectionInterface`s from the persistent store.
    fn load_introspection(
        &self,
    ) -> impl Future<Output = Result<Vec<IntrospectionInterface>, SessionError>> + Send;

    /// Removes a specific slice of `IntrospectionInterface`s from the persistent store.
    fn remove_interfaces(
        &self,
        interfaces: &[IntrospectionInterface],
    ) -> impl Future<Output = Result<(), SessionError>> + Send;
}

impl StoredSession for MissingCapability {
    async fn add_interfaces(
        &self,
        _interfaces: &[IntrospectionInterface],
    ) -> Result<(), SessionError> {
        unreachable!("the type is un-constructable");
    }

    async fn load_introspection(&self) -> Result<Vec<IntrospectionInterface>, SessionError> {
        unreachable!("the type is un-constructable");
    }

    async fn clear_introspection(&self) -> Result<(), SessionError> {
        unreachable!("the type is un-constructable");
    }

    async fn remove_interfaces(
        &self,
        _interfaces: &[IntrospectionInterface],
    ) -> Result<(), SessionError> {
        unreachable!("the type is un-constructable");
    }
}

impl Interfaces {
    pub(crate) fn matches(&self, stored: &[IntrospectionInterface]) -> bool {
        stored.len() == self.len()
            && stored.iter().all(|stored_i| {
                self.get(&stored_i.name).is_some_and(|i| {
                    i.version_major() == stored_i.version_major
                        && i.version_minor() == stored_i.version_minor
                })
            })
    }
}

impl From<&Interface> for IntrospectionInterface {
    fn from(val: &Interface) -> Self {
        IntrospectionInterface {
            name: val.interface_name().to_string(),
            version_major: val.version_major(),
            version_minor: val.version_minor(),
        }
    }
}

impl From<&Interfaces> for Vec<IntrospectionInterface> {
    fn from(val: &Interfaces) -> Self {
        val.iter().map(|i| i.into()).collect_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use pretty_assertions::assert_eq;

    use crate::Interface;

    use super::IntrospectionInterface;

    #[test]
    fn test_from_interface() {
        let interface = Interface::from_str(crate::test::DEVICE_OBJECT).unwrap();

        let introspection_if_data: IntrospectionInterface = From::from(&interface);

        assert_eq!(interface.interface_name(), introspection_if_data.name);
        assert_eq!(
            interface.version_major(),
            introspection_if_data.version_major
        );
        assert_eq!(
            interface.version_minor(),
            introspection_if_data.version_minor
        );
    }

    #[test]
    fn test_from_interfaces() {
        use crate::session::Interfaces;

        let interfaces = [
            Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(crate::test::DEVICE_OBJECT).unwrap(),
            Interface::from_str(crate::test::SERVER_INDIVIDUAL).unwrap(),
        ];

        let interfaces = Interfaces::from_iter(interfaces);

        let introsopection_interface_vec: Vec<IntrospectionInterface> = From::from(&interfaces);

        assert!(interfaces.matches(&introsopection_interface_vec));
    }
}
