// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::RwLock;
use tokio::sync::Semaphore;

use crate::interfaces::Interfaces;
use crate::retention;
use crate::retention::memory::VolatileStore;

/// Shared status between the connection and client.
///
/// It's used to have a single allocation and dereference through a single [`Arc`].
#[derive(Debug)]
pub(crate) struct SharedState {
    // should be locked during every action when modifying the introspection
    // (adding or removing interfaces)
    pub(crate) introspection: Semaphore,
    pub(crate) interfaces: RwLock<Interfaces>,
    pub(crate) volatile_store: VolatileStore,
    pub(crate) retention_ctx: retention::Context,
    pub(crate) status: ConnectionStatus,
}

impl SharedState {
    pub(crate) fn new(interfaces: Interfaces, volatile_store: VolatileStore) -> Self {
        Self {
            introspection: Semaphore::new(1),
            interfaces: RwLock::new(interfaces),
            volatile_store,
            retention_ctx: retention::Context::new(),
            status: ConnectionStatus::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    Connected,
    Disconnected,
    Closed,
}

/// Shared state of the connection
#[derive(Debug)]
pub(crate) struct ConnectionStatus {
    /// Flag if the connection was closed gracefully
    closed: AtomicBool,
    /// Flag if we are connected
    connected: AtomicBool,
}

impl ConnectionStatus {
    pub(crate) fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            // Do not assume we are connected
            // NOTE it's the connection which will set the connected flag to true once the connection gets established
            // this should happen after the [`ConnectionConfig`](crate::builder::ConnectionConfig) `connect` method
            // to ensure the device is already marked as connected after the `build` function is called
            connected: AtomicBool::new(false),
        }
    }

    /// Set the state of the connection.
    pub(crate) fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Release);
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn connection(&self) -> Status {
        if self.closed.load(Ordering::Acquire) {
            Status::Closed
        } else if self.connected.load(Ordering::Acquire) {
            Status::Connected
        } else {
            Status::Disconnected
        }
    }
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        Self::new()
    }
}
