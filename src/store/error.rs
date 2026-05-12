// This file is part of Astarte.
//
// Copyright 2023, 2026 SECO Mind Srl
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

//! Error for the store.

use std::fmt::Display;

/// Error that wraps the type returned by an implementation of the [`super::PropertyStore`] trait.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    /// Could not store a property.
    Store,
    /// Could not update property state.
    UpdateState,
    /// Could not load a property.
    Load,
    /// Could not delete a property.
    Delete,
    /// Could not unset a property.
    Unset,
    /// Could not clear the database.
    Clear,
    /// Could not load all properties.
    LoadAll,
    /// Could not load device properties.
    DeviceProps,
    /// Could not load server properties.
    ServerProps,
    /// Could not load interface properties.
    InterfaceProps,
    /// Could not delete all the interface properties.
    DeleteInterface,
    /// Could not reset properties state
    ResetState,
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Store => write!(f, "could not store property"),
            StoreError::UpdateState => write!(f, "could not update property state"),
            StoreError::Load => write!(f, "could not load property"),
            StoreError::Delete => write!(f, "could not delete property"),
            StoreError::Unset => write!(f, "could not delete property"),
            StoreError::Clear => write!(f, "could not clear database"),
            StoreError::LoadAll => write!(f, "could not load all properties"),
            StoreError::DeviceProps => write!(f, "could not load device properties"),
            StoreError::ServerProps => write!(f, "could not load server properties"),
            StoreError::InterfaceProps => write!(f, "could not load server properties"),
            StoreError::DeleteInterface => {
                write!(f, "could not delete all the interface properties")
            }
            StoreError::ResetState => write!(f, "could not reset properties state"),
        }
    }
}
