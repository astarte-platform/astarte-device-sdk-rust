// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
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

//! Handle the introspection for the device

use std::{future::Future, path::Path};

use astarte_interfaces::Interface;

use crate::error::AstarteError;

/// Trait that permits a client to query the interfaces in the device introspection.
pub trait DeviceIntrospection {
    /// Returns a reference to the [`Interface`] with the given name.
    fn get_interface<F, O>(&self, interface_name: &str, f: F) -> impl Future<Output = O> + Send
    where
        F: FnMut(Option<&Interface>) -> O + Send;
}

/// Trait that permits a client to add and remove interfaces dynamically after being connected.
pub trait DynamicIntrospection {
    /// Add a new [`Interface`] to the device introspection.
    ///
    /// Returns a bool to check weather the if the interface was added or was already present.
    fn add_interface(
        &mut self,
        interface: Interface,
    ) -> impl Future<Output = Result<bool, AstarteError>> + Send;

    /// Add one or more [`Interface`] to the device introspection.
    ///
    /// Returns a [`Vec`] with the name of the interfaces that have been added.
    fn extend_interfaces<I>(
        &mut self,
        interfaces: I,
    ) -> impl Future<Output = Result<Vec<String>, AstarteError>> + Send
    where
        I: IntoIterator<Item = Interface> + Send;

    /// Add a new interface from the provided file.
    ///
    /// Returns a bool to check weather the if the interface was added or was already present.
    fn add_interface_from_file<P>(
        &mut self,
        file_path: P,
    ) -> impl Future<Output = Result<bool, AstarteError>> + Send
    where
        P: AsRef<Path> + Send + Sync;

    /// Add a new interface from a string. The string should contain a valid json formatted
    /// interface.
    ///
    /// Returns a bool to check weather the if the interface was added or was already present.
    fn add_interface_from_str(
        &mut self,
        json_str: &str,
    ) -> impl Future<Output = Result<bool, AstarteError>> + Send;

    /// Remove the interface with the name specified as argument.
    ///
    /// Returns a bool to check weather the if the interface was removed or was missing.
    fn remove_interface(
        &mut self,
        interface_name: &str,
    ) -> impl Future<Output = Result<bool, AstarteError>> + Send;

    /// Remove interfaces with names specified as argument.
    ///
    /// Returns a [`Vec`] with the name of the interfaces that have been removed.
    fn remove_interfaces<I>(
        &mut self,
        interfaces_name: I,
    ) -> impl Future<Output = Result<Vec<String>, AstarteError>> + Send
    where
        I: IntoIterator<Item = String> + Send,
        I::IntoIter: Send;
}
