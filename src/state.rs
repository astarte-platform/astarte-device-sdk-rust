// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use chrono::DateTime;
use chrono::Utc;
use rustls::ClientConfig;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;

use crate::builder::Config;
use crate::interfaces::Interfaces;
use crate::retention;
use crate::retention::TimestampMillis;
use crate::retention::memory::VolatileStore;
use crate::retry::RandomExponentialIter;
use crate::store;

/// Context to create a unique [`Id`].
#[derive(Debug)]
pub struct Context {
    counter: AtomicU32,
}

impl Context {
    /// Create a new context
    pub fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
        }
    }

    fn next_counter(&self) -> u32 {
        // We want the values to be unique, this will wrap around, but it will never wrap on the
        // same ms, the ordering can be relaxed since the only guarantee we need is the operation to
        // be atomic and the counter to yield unique values.
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Returns the next unique id.
    pub fn next(&self) -> retention::Id {
        let timestamp = TimestampMillis::now();

        let counter = self.next_counter();

        retention::Id { timestamp, counter }
    }

    /// Returns the next unique updated_at.
    pub fn next_updated_at(&self) -> store::UpdatedAt {
        let timestamp = Utc::now();

        let counter = self.next_counter();

        store::UpdatedAt::new(timestamp, counter)
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) trait SharedStateExt {
    fn backoff(&self) -> &Mutex<RandomExponentialIter>;
}

/// Shared status between the connection and client.
///
/// It's used to have a single allocation and dereference through a single [`Arc`].
#[derive(Debug)]
pub struct SharedState<S> {
    pub(crate) config: Config,
    pub(crate) property_ctx: Context,
    pub(crate) retention_ctx: Context,
    pub(crate) interfaces: RwLock<Interfaces>,
    pub(crate) status: tokio::sync::watch::Sender<ConnStatus>,
    /// Status of the device, whether it's paired to Astarte
    pub(crate) device_status: AtomicU8,
    pub(crate) cert_expiry: RwLock<Option<DateTime<Utc>>>,
    pub(crate) volatile_store: VolatileStore,
    pub(crate) store: S,
    pub(crate) backoff: Mutex<RandomExponentialIter>,
    pub(crate) tls: rustls::ClientConfig,
}

impl<S> SharedState<S> {
    pub(crate) fn new(
        config: Config,
        interfaces: Interfaces,
        volatile_store: VolatileStore,
        status: tokio::sync::watch::Sender<ConnStatus>,
        store: S,
        backoff: RandomExponentialIter,
        tls: rustls::ClientConfig,
    ) -> Self {
        Self {
            config,
            interfaces: RwLock::new(interfaces),
            volatile_store,
            property_ctx: Context::new(),
            retention_ctx: Context::new(),
            status,
            cert_expiry: RwLock::new(None),
            device_status: AtomicU8::new(DeviceStatus::Unknown.into()),
            store,
            backoff: Mutex::new(backoff),
            tls,
        }
    }

    pub(crate) fn split(self: Arc<Self>) -> (ClientState<S>, ConnectionState<S>) {
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
            DeviceStatus::Registered
        } else {
            DeviceStatus::Unregistered
        };

        self.device_status.store(status.into(), Ordering::Release);
    }
}

/// State of the [`DeviceClient`](crate::DeviceClient)
#[derive(Debug)]
pub(crate) struct ClientState<S>(Arc<SharedState<S>>);

impl<S> ClientState<S> {
    pub(crate) fn new(shared_state: Arc<SharedState<S>>) -> Self {
        Self(shared_state)
    }

    pub(crate) fn interfaces(&self) -> &RwLock<Interfaces> {
        &self.0.interfaces
    }

    pub(crate) fn volatile_store(&self) -> &VolatileStore {
        &self.0.volatile_store
    }

    pub(crate) fn property_ctx(&self) -> &Context {
        &self.0.property_ctx
    }

    pub(crate) fn retention_ctx(&self) -> &Context {
        &self.0.retention_ctx
    }

    pub(crate) fn connection(&self) -> ConnStatus {
        *self.0.status.borrow()
    }

    pub(crate) fn disconnect(&self) {
        // ignore close error
        if let Err(error) = self.0.status.send(ConnStatus::Disconnect) {
            error!(%error, "couldn't send close status")
        }
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
            DeviceStatus::Unknown | DeviceStatus::Unregistered => false,
            DeviceStatus::Registered => true,
        }
    }

    pub(crate) fn store(&self) -> &S {
        &self.0.store
    }

    pub(crate) fn config(&self) -> &Config {
        &self.0.config
    }
}

impl<S> Clone for ClientState<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// State of the [`DeviceConnection`](crate::DeviceConnection)
#[derive(Debug)]
pub struct ConnectionState<S>(Arc<SharedState<S>>);

impl<S> ConnectionState<S> {
    pub(crate) fn new(shared_state: Arc<SharedState<S>>) -> Self {
        Self(shared_state)
    }

    pub(crate) fn property_ctx(&self) -> &Context {
        &self.0.property_ctx
    }

    pub(crate) fn should_exit(&self) -> bool {
        self.0.status.borrow().should_exit()
    }

    pub(crate) fn subscribe_connection(&self) -> tokio::sync::watch::Receiver<ConnStatus> {
        self.0.status.subscribe()
    }

    pub(crate) fn set_connection(&self, new: ConnStatus) {
        self.0.status.send_if_modified(|status| match status {
            ConnStatus::Disconnect | ConnStatus::Closed => {
                debug!("connection disconnected, not updating state");

                false
            }
            _ => {
                *status = new;

                true
            }
        });
    }

    pub(crate) fn close_connection(&self) {
        if let Err(error) = self.0.status.send(ConnStatus::Closed) {
            error!(%error, "couldn't close the connection");
        }
    }

    pub(crate) async fn set_cert_expiry(&self, expiry: Option<DateTime<Utc>>) {
        *self.0.cert_expiry.write().await = expiry;
    }

    pub(crate) fn interfaces(&self) -> &RwLock<Interfaces> {
        &self.0.interfaces
    }

    pub(crate) fn store(&self) -> &S {
        &self.0.store
    }

    pub(crate) fn volatile_store(&self) -> &VolatileStore {
        &self.0.volatile_store
    }

    pub(crate) fn config(&self) -> &Config {
        &self.0.config
    }

    pub(crate) fn tls(&self) -> &ClientConfig {
        &self.0.tls
    }
}

impl<S> SharedStateExt for ConnectionState<S> {
    fn backoff(&self) -> &Mutex<RandomExponentialIter> {
        &self.0.backoff
    }
}

impl<S> Clone for ConnectionState<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Shared state of the connection
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ConnStatus {
    /// Disconnected from Astarte.
    #[default]
    Offline,
    /// Connected to Astarte.
    Connected {
        /// The connection has a session present
        session_present: bool,
    },
    /// Device is online.
    Online,
    /// A client requested a disconnect
    Disconnect,
    /// Connection closed with a disconnect.
    Closed,
}

impl ConnStatus {
    /// Check if we should exit with the current status.
    pub(crate) fn should_exit(&self) -> bool {
        matches!(self, ConnStatus::Disconnect | ConnStatus::Closed)
    }
}

impl Display for ConnStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnStatus::Offline => write!(f, "Offline"),
            ConnStatus::Connected { session_present } => {
                write!(f, "Connected(session_present: {session_present})")
            }
            ConnStatus::Online => write!(f, "Online"),
            ConnStatus::Disconnect => write!(f, "Disconnect"),
            ConnStatus::Closed => write!(f, "Closed"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub(crate) enum DeviceStatus {
    Unknown = 0,
    Unregistered = 1,
    Registered = 2,
}

impl Display for DeviceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeviceStatus::Unknown => write!(f, "Unknown"),
            DeviceStatus::Unregistered => write!(f, "Unregistered"),
            DeviceStatus::Registered => write!(f, "Registered"),
        }
    }
}

impl From<u8> for DeviceStatus {
    fn from(value: u8) -> Self {
        match value {
            0 | 3.. => DeviceStatus::Unknown,
            1 => DeviceStatus::Unregistered,
            2 => DeviceStatus::Registered,
        }
    }
}

impl From<DeviceStatus> for u8 {
    fn from(value: DeviceStatus) -> Self {
        value as u8
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashSet;

    use crate::builder::DEFAULT_VOLATILE_CAPACITY;

    use super::*;

    use pretty_assertions::assert_eq;
    use rstest::rstest;

    impl<S> ConnectionState<S> {
        pub(crate) fn retention_ctx(&self) -> &Context {
            &self.0.retention_ctx
        }
    }

    pub(crate) fn mock_state<S>(
        store: S,
        status_tx: tokio::sync::watch::Sender<ConnStatus>,
        interfaces: Interfaces,
    ) -> SharedState<S> {
        SharedState::new(
            Config::default(),
            interfaces,
            VolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY.get()),
            status_tx,
            store,
            RandomExponentialIter::default(),
            astarte_device_tls::config().unwrap(),
        )
    }

    #[test]
    fn default_connection_state() {
        // Must start disconnected
        assert_eq!(ConnStatus::default(), ConnStatus::Offline)
    }

    #[rstest]
    #[case(0, DeviceStatus::Unknown)]
    #[case(3, DeviceStatus::Unknown)]
    #[case(1, DeviceStatus::Unregistered)]
    #[case(2, DeviceStatus::Registered)]
    fn device_status_from_u8(#[case] value: u8, #[case] exp: DeviceStatus) {
        let res = DeviceStatus::from(value);

        assert_eq!(res, exp);
    }

    #[rstest]
    #[case(DeviceStatus::Unknown, 0)]
    #[case(DeviceStatus::Unregistered, 1)]
    #[case(DeviceStatus::Registered, 2)]
    fn device_status_into_u8(#[case] value: DeviceStatus, #[case] exp: u8) {
        let res = u8::from(value);

        assert_eq!(res, exp);
    }

    #[test]
    fn id_should_be_unique() {
        const NUM: usize = 5;
        const CAP: usize = 1000;
        let ctx = Arc::new(Context::new());

        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<retention::Id>>(NUM);

        let handles = (0..NUM)
            .map(|_| {
                std::thread::spawn({
                    let ctx = Arc::clone(&ctx);
                    let tx = tx.clone();

                    move || {
                        let mut out = Vec::with_capacity(CAP);
                        let mut prev = ctx.next();
                        for _i in 0..CAP {
                            let new = ctx.next();

                            assert!(new > prev);

                            out.push(prev);
                            prev = new;
                        }

                        tx.send(out).expect("channel closed");
                    }
                })
            })
            .collect::<Vec<_>>();

        drop(tx);

        let mut recvd = HashSet::new();
        while let Ok(out) = rx.recv() {
            for i in out {
                assert!(recvd.insert(i));
            }
        }

        for handle in handles {
            handle.join().expect("worker thread panicked");
        }
    }
}
