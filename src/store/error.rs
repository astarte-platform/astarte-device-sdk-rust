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

//! Error for the store.

/// Dynamic error type of an [`super::PropertyStore`].
type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Error that wraps the type returned by an implementation of the [`super::PropertyStore`] trait.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Could not store a property.
    #[error("could not store property")]
    Store(#[source] DynError),
    /// Could not load a property.
    #[error("could not load property")]
    Load(#[source] DynError),
    /// Could not delete a property.
    #[error("could not delete property")]
    Delete(#[source] DynError),
    /// Could not clear the database.
    #[error("could not clear database")]
    Clear(#[source] DynError),
    /// Could not load all properties.
    #[error("could not load all properties")]
    LoadAll(#[source] DynError),
    /// Could not load device properties.
    #[error("could not load device properties")]
    DeviceProps(#[source] DynError),
    /// Could not load server properties.
    #[error("could not load server properties")]
    ServerProps(#[source] DynError),
    /// Could not load interface properties.
    #[error("could not load server properties")]
    InterfaceProps(#[source] DynError),
}

impl StoreError {
    pub(crate) fn store(err: impl Into<DynError>) -> Self {
        Self::Store(err.into())
    }

    pub(crate) fn load(err: impl Into<DynError>) -> Self {
        Self::Load(err.into())
    }

    pub(crate) fn delete(err: impl Into<DynError>) -> Self {
        Self::Delete(err.into())
    }

    pub(crate) fn clear(err: impl Into<DynError>) -> Self {
        Self::Clear(err.into())
    }

    pub(crate) fn load_all(err: impl Into<DynError>) -> Self {
        Self::LoadAll(err.into())
    }

    pub(crate) fn server_props(err: impl Into<DynError>) -> Self {
        Self::ServerProps(err.into())
    }

    pub(crate) fn device_props(err: impl Into<DynError>) -> Self {
        Self::DeviceProps(err.into())
    }

    pub(crate) fn interface_props(err: impl Into<DynError>) -> Self {
        Self::InterfaceProps(err.into())
    }
}

/// Error that wraps the type returned by an implementation of the [`super::RetentionStore`] trait.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum RetentionStoreError {
    /// Could not clear the store.
    #[error("could not clear store")]
    Clear(#[source] DynError),
    /// Could not get front retention message.
    #[error("could not load retention message")]
    Front(#[source] DynError),
    /// Could not check if store was empty
    #[error("could not check if the store was empty")]
    IsEmpty(#[source] DynError),
    /// Could not load all retention messages.
    #[error("could not load all retention messages")]
    LoadAll(#[source] DynError),
    /// Could not store a retention message.
    #[error("could not store retention message")]
    Store(#[source] DynError),
    /// Could not remove front retention message.
    #[error("could not remove retention message")]
    Remove,
    /// Could not remove front retention message.
    #[error("could not remove front retention message")]
    RemoveFront(#[source] DynError),
}
