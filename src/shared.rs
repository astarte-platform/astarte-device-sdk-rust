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

//! Shared data held by the [`AstarteDeviceSdk`].

use std::ops::Deref;

use tokio::sync::{Mutex, RwLock};

use crate::interfaces::Interfaces;
use crate::store::wrapper::StoreWrapper;
use crate::{AstarteDeviceSdk, AsyncClient, EventLoop, EventSender};

/// Shared data of the device SDK, this struct is internal of the [`AstarteDeviceSdk`] where is
/// wrapped in an arc to share an immutable reference across tasks.
pub struct SharedDevice<S> {
    pub(crate) realm: String,
    pub(crate) device_id: String,
    pub(crate) client: AsyncClient,
    pub(crate) events_channel: EventSender,
    pub(crate) eventloop: Mutex<EventLoop>,
    pub(crate) interfaces: RwLock<Interfaces>,
    pub(crate) store: StoreWrapper<S>,
}

/// Since the Arch is shared we can only expose a reference of the shared device data.
///
/// Interior mutability is achieved through locking mechanisms like [`Mutex`] and [`RwLock`]
impl<S> Deref for AstarteDeviceSdk<S> {
    type Target = SharedDevice<S>;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}
