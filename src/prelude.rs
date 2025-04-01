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

//! Prelude device module
//! Re-exports common traits frequently used when using the astarte SDK API.

/// Device module that exports trait commonly used when sending or receiving data.
pub mod device {
    pub use crate::client::Client;
    pub use crate::client::ClientDisconnect;
    pub use crate::connection::EventLoop;
    pub use crate::introspection::DynamicIntrospection;
    pub use crate::FromEvent;
}

/// Exports common trait used when accessing stored properties.
pub mod properties {
    pub use crate::properties::PropAccess;
    pub use crate::store::PropertyStore;
}

pub use device::*;
pub use properties::*;

#[cfg(feature = "derive")]
pub use crate::IntoAstarteObject;
