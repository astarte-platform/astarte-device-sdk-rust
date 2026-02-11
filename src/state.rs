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

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use tokio::sync::RwLock;

use crate::builder::Config;
use crate::interfaces::Interfaces;
use crate::retention;
use crate::retention::memory::VolatileStore;

/// Shared status between the connection and client.
///
/// It's used to have a single allocation and dereference through a single [`Arc`].
#[derive(Debug)]
pub struct SharedState {
    pub(crate) config: Config,
    pub(crate) interfaces: RwLock<Interfaces>,
    pub(crate) volatile_store: VolatileStore,
    pub(crate) retention_ctx: retention::Context,
    pub(crate) status: RwLock<ConnStatus>,
    pub(crate) cert_expiry: RwLock<Option<DateTime<Utc>>>,
}

impl SharedState {
    pub(crate) fn new(
        config: Config,
        interfaces: Interfaces,
        volatile_store: VolatileStore,
    ) -> Self {
        Self {
            config,
            interfaces: RwLock::new(interfaces),
            volatile_store,
            retention_ctx: retention::Context::new(),
            status: RwLock::new(ConnStatus::default()),
            cert_expiry: RwLock::new(None),
        }
    }

    pub(crate) fn split(self: Arc<Self>) -> (ClientState, ConnectionState) {
        (
            ClientState::new(Arc::clone(&self)),
            ConnectionState::new(self),
        )
    }

    /// Gets the config for the retention
    pub fn config(&self) -> &Config {
        &self.config
    }
}

/// State of the [`DeviceClient`](crate::DeviceClient)
#[derive(Debug, Clone)]
pub(crate) struct ClientState(Arc<SharedState>);

impl ClientState {
    pub(crate) fn new(shared_state: Arc<SharedState>) -> Self {
        Self(shared_state)
    }

    pub(crate) fn interfaces(&self) -> &RwLock<Interfaces> {
        &self.0.interfaces
    }

    pub(crate) fn volatile_store(&self) -> &VolatileStore {
        &self.0.volatile_store
    }

    pub(crate) fn retention_ctx(&self) -> &retention::Context {
        &self.0.retention_ctx
    }

    pub(crate) async fn connection(&self) -> ConnStatus {
        *self.0.status.read().await
    }

    pub(crate) async fn cert_expiry(&self) -> Option<DateTime<Utc>> {
        *self.0.cert_expiry.read().await
    }
}

/// State of the [`DeviceConnection`](crate::DeviceConnection)
#[derive(Debug, Clone)]
pub(crate) struct ConnectionState(Arc<SharedState>);

impl ConnectionState {
    pub(crate) fn new(shared_state: Arc<SharedState>) -> Self {
        Self(shared_state)
    }

    pub(crate) async fn set_connection(&self, status: ConnStatus) {
        *self.0.status.write().await = status;
    }

    pub(crate) fn interfaces(&self) -> &RwLock<Interfaces> {
        &self.0.interfaces
    }

    pub(crate) fn volatile_store(&self) -> &VolatileStore {
        &self.0.volatile_store
    }

    pub(crate) fn config(&self) -> &Config {
        &self.0.config
    }
}

/// Shared state of the connection
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ConnStatus {
    /// Disconnected from Astarte.
    #[default]
    Disconnected,
    /// Connected to Astarte.
    Connected,
    /// Connection closed with a disconnect.
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;

    impl ConnectionState {
        pub(crate) fn retention_ctx(&self) -> &retention::Context {
            &self.0.retention_ctx
        }
    }

    #[test]
    fn default_connection_state() {
        // Must start disconnected
        assert_eq!(ConnStatus::default(), ConnStatus::Disconnected)
    }
}
