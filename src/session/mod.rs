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

//! Handles the storage of the current introspection to maintain
//! a persistent session with the Astarte MQTT server.

use std::future::Future;

use astarte_interfaces::Interface;
use itertools::Itertools;

use crate::{error::DynError, interfaces::Interfaces};

mod sqlite;

/// Interface data associated with the astarte introspection.
#[derive(Debug, Clone, Copy, Eq, PartialOrd, Ord, Hash)]
pub struct IntrospectionInterface<S = String> {
    /// Name of the interface.
    name: S,
    /// Major version.
    version_major: i32,
    /// Minor version.
    version_minor: i32,
}

impl<S> IntrospectionInterface<S> {
    /// Create a new instance of the struct
    pub fn new(name: S, version_major: i32, version_minor: i32) -> Self {
        Self {
            name,
            version_major,
            version_minor,
        }
    }

    /// Get the name of the interface
    pub fn name(&self) -> &S {
        &self.name
    }
    /// Get the major version of the interface
    pub fn version_major(&self) -> i32 {
        self.version_major
    }
    /// Get the minor version of the interface
    pub fn version_minor(&self) -> i32 {
        self.version_minor
    }
}

impl<T: PartialEq<U>, U> PartialEq<IntrospectionInterface<U>> for IntrospectionInterface<T> {
    fn eq(&self, other: &IntrospectionInterface<U>) -> bool {
        self.name() == other.name()
            && self.version_major() == other.version_major()
            && self.version_minor() == other.version_minor()
    }
}

impl<'a> From<&'a Interface> for IntrospectionInterface<&'a str> {
    fn from(val: &'a Interface) -> Self {
        IntrospectionInterface::new(
            val.interface_name(),
            val.version_major(),
            val.version_minor(),
        )
    }
}

impl From<&Interface> for IntrospectionInterface {
    fn from(val: &Interface) -> Self {
        IntrospectionInterface::new(
            val.interface_name().to_owned(),
            val.version_major(),
            val.version_minor(),
        )
    }
}

impl<'a> From<&'a Interfaces> for Vec<IntrospectionInterface<&'a str>> {
    fn from(val: &'a Interfaces) -> Self {
        val.iter().map(|i| i.into()).collect_vec()
    }
}

impl From<&Interfaces> for Vec<IntrospectionInterface> {
    fn from(val: &Interfaces) -> Self {
        val.iter().map(|i| i.into()).collect_vec()
    }
}

impl From<IntrospectionInterface<&str>> for IntrospectionInterface {
    fn from(value: IntrospectionInterface<&str>) -> Self {
        IntrospectionInterface {
            name: value.name.to_string(),
            version_major: value.version_major,
            version_minor: value.version_minor,
        }
    }
}

/// Error returned by the retention.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SessionError {
    /// Error in the store introspection method
    #[error("couldn't store the introspection")]
    AddInterfaces(#[source] DynError),
    /// Error in the load introspection method
    #[error("couldn't load the introspection")]
    LoadIntrospection(#[source] DynError),
    /// Error in the remove introspection method
    #[error("couldn't remove the interfaces")]
    RemoveInterfaces(#[source] DynError),
}

impl SessionError {
    pub(crate) fn add_interfaces(err: impl Into<DynError>) -> Self {
        Self::AddInterfaces(err.into())
    }

    pub(crate) fn load_introspection(err: impl Into<DynError>) -> Self {
        Self::LoadIntrospection(err.into())
    }

    pub(crate) fn remove_interfaces(err: impl Into<DynError>) -> Self {
        Self::RemoveInterfaces(err.into())
    }
}

/// Trait for persistently storing and managing session-related data.
pub trait StoredSession: Clone + Send + Sync {
    /// Clears all [`IntrospectionInterface`]s from the persistent store.
    /// This method should not return an error or panic since it is called
    /// during the connection of the device.
    fn clear_introspection(&self) -> impl Future<Output = ()> + Send;

    /// Stores the complete introspection in the table.
    /// This method perform the same action as [`StoredSession::add_interfaces`]
    /// but disallows returning an error.
    /// This method should not return an error or panic since it is called
    /// during the connection of the device.
    fn store_introspection(
        &self,
        interfaces: &[IntrospectionInterface],
    ) -> impl Future<Output = ()> + Send;

    /// Adds a slice of `IntrospectionInterface` to the persistent store.
    fn add_interfaces(
        &self,
        interfaces: &[IntrospectionInterface<&str>],
    ) -> impl Future<Output = Result<(), SessionError>> + Send;

    /// Loads all stored `IntrospectionInterface`s from the persistent store.
    fn load_introspection(
        &self,
    ) -> impl Future<Output = Result<Vec<IntrospectionInterface>, SessionError>> + Send;

    /// Removes a specific slice of `IntrospectionInterface`s from the persistent store.
    fn remove_interfaces(
        &self,
        interfaces: &[IntrospectionInterface<&str>],
    ) -> impl Future<Output = Result<(), SessionError>> + Send;
}

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;

    use astarte_interfaces::Interface;
    use pretty_assertions::assert_eq;

    use crate::test::{DEVICE_OBJECT, DEVICE_PROPERTIES, SERVER_INDIVIDUAL};

    use super::IntrospectionInterface;

    impl<'a> From<&'a IntrospectionInterface> for IntrospectionInterface<&'a str> {
        fn from(val: &'a IntrospectionInterface) -> Self {
            IntrospectionInterface::new(val.name(), val.version_major(), val.version_minor())
        }
    }

    impl IntrospectionInterface {
        pub(crate) fn as_ref(&self) -> IntrospectionInterface<&str> {
            self.into()
        }
    }

    #[test]
    fn test_from_interface() {
        let interface = Interface::from_str(DEVICE_OBJECT).unwrap();

        let introspection_if_data_ref: IntrospectionInterface<&str> = From::from(&interface);

        assert_eq!(interface.interface_name(), introspection_if_data_ref.name);
        assert_eq!(
            interface.version_major(),
            introspection_if_data_ref.version_major
        );
        assert_eq!(
            interface.version_minor(),
            introspection_if_data_ref.version_minor
        );

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

        assert_eq!(introspection_if_data_ref, introspection_if_data.as_ref());
    }

    #[test]
    fn test_from_interfaces() {
        use crate::session::Interfaces;

        let interfaces = [
            Interface::from_str(DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(DEVICE_OBJECT).unwrap(),
            Interface::from_str(SERVER_INDIVIDUAL).unwrap(),
        ];

        let interfaces = Interfaces::from_iter(interfaces);

        let introsopection_interface_vec: Vec<IntrospectionInterface<&str>> =
            From::from(&interfaces);

        assert!(interfaces.matches(&introsopection_interface_vec));

        let introsopection_interface_vec: Vec<IntrospectionInterface> = From::from(&interfaces);

        assert!(interfaces.matches(&introsopection_interface_vec));
    }
}
