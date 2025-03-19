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
//

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::RwLock;

use crate::interfaces::Interfaces;
use crate::retention;
use crate::retention::memory::VolatileStore;

/// Shared status between the connection and client.
///
/// It's used to have a single allocation and dereference through a single [`Arc`].
#[derive(Debug)]
pub(crate) struct SharedState {
    pub(crate) interfaces: RwLock<Interfaces>,
    pub(crate) volatile_store: VolatileStore,
    pub(crate) retention_ctx: retention::Context,
    pub(crate) status: ConnectionStatus,
}

impl SharedState {
    pub(crate) fn new(interfaces: Interfaces, volatile_store: VolatileStore) -> Self {
        Self {
            interfaces: RwLock::new(interfaces),
            volatile_store,
            retention_ctx: retention::Context::new(),
            status: ConnectionStatus::new(),
        }
    }
}

/// Shared state of the connection
#[derive(Debug)]
pub(crate) struct ConnectionStatus {
    /// Flag if we are connected
    connected: AtomicBool,
    /// Flag if the connection was closed gracefully
    closed: AtomicBool,
}

impl ConnectionStatus {
    pub(crate) fn new() -> Self {
        // Assumes we are connected
        Self {
            connected: AtomicBool::new(true),
            closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub(crate) fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Release);
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        Self::new()
    }
}
