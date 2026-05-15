// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

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
    /// Status of the device, whether it's paired to Astarte
    pub(crate) device_status: AtomicU8,
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
            device_status: AtomicU8::new(DeviceStatus::Unknown.into()),
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

    pub(crate) fn set_device_status(&self, paired: bool) {
        let status = if paired {
            DeviceStatus::Paired
        } else {
            DeviceStatus::Unpaired
        };

        self.device_status.store(status.into(), Ordering::Release);
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

    pub(crate) fn is_device_paired(&self) -> bool {
        let value = DeviceStatus::from(self.0.device_status.load(Ordering::Acquire));

        debug_assert_ne!(
            value,
            DeviceStatus::Unknown,
            "the unknown status should be set only in the builder"
        );

        match value {
            DeviceStatus::Unknown | DeviceStatus::Unpaired => false,
            DeviceStatus::Paired => true,
        }
    }
}

/// State of the [`DeviceConnection`](crate::DeviceConnection)
#[derive(Debug, Clone)]
pub(crate) struct ConnectionState(Arc<SharedState>);

impl ConnectionState {
    pub(crate) fn new(shared_state: Arc<SharedState>) -> Self {
        Self(shared_state)
    }

    pub(crate) async fn get_connection(&self) -> ConnStatus {
        *self.0.status.read().await
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

impl Display for ConnStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnStatus::Disconnected => write!(f, "Disconnected"),
            ConnStatus::Connected => write!(f, "Connected"),
            ConnStatus::Closed => write!(f, "Closed"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub(crate) enum DeviceStatus {
    Unknown = 0,
    Unpaired = 1,
    Paired = 2,
}

impl Display for DeviceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeviceStatus::Unknown => write!(f, "Unknown"),
            DeviceStatus::Unpaired => write!(f, "Unpaired"),
            DeviceStatus::Paired => write!(f, "Paired"),
        }
    }
}

impl From<u8> for DeviceStatus {
    fn from(value: u8) -> Self {
        match value {
            0 | 3.. => DeviceStatus::Unknown,
            1 => DeviceStatus::Unpaired,
            2 => DeviceStatus::Paired,
        }
    }
}

impl From<DeviceStatus> for u8 {
    fn from(value: DeviceStatus) -> Self {
        value as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;
    use rstest::rstest;

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

    #[rstest]
    #[case(0, DeviceStatus::Unknown)]
    #[case(3, DeviceStatus::Unknown)]
    #[case(1, DeviceStatus::Unpaired)]
    #[case(2, DeviceStatus::Paired)]
    fn device_status_from_u8(#[case] value: u8, #[case] exp: DeviceStatus) {
        let res = DeviceStatus::from(value);

        assert_eq!(res, exp);
    }

    #[rstest]
    #[case(DeviceStatus::Unknown, 0)]
    #[case(DeviceStatus::Unpaired, 1)]
    #[case(DeviceStatus::Paired, 2)]
    fn device_status_into_u8(#[case] value: DeviceStatus, #[case] exp: u8) {
        let res = u8::from(value);

        assert_eq!(res, exp);
    }
}
