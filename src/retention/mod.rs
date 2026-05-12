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

//! Handles stored retention for the connections.

use std::{
    borrow::Cow,
    collections::HashSet,
    fmt::Display,
    future::Future,
    num::{NonZeroUsize, TryFromIntError},
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astarte_device_error::Error;
use astarte_interfaces::{interface::Retention, schema::Reliability};
use futures::{StreamExt, TryStreamExt};
use tracing::warn;

use crate::{
    error::Report,
    interfaces::Interfaces,
    retention::memory::VolatileStore,
    store::StoreCapabilities,
    validate::{ValidatedIndividual, ValidatedObject},
};

pub(crate) mod memory;
pub(crate) mod sqlite;

/// Error returned by the retention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetentionError {
    /// Couldn't store the publish information.
    Store,
    /// Couldn't mark the publish as received.
    Received,
    /// Couldn't update the sent flag for the  publish.
    UpdateSent,
    /// Couldn't reset all the publishes.
    Reset,
    /// Couldn't get unset publishes information.
    Unsent,
    /// Couldn't delete the publishes with interface.
    DeleteInterface,
    /// Couldn't delete the publishes for interfaces.
    DeleteInterfaceMany,
    /// Couldn't fetch all the publishes' interfaces.
    FetchInterfaces,
    /// Couldn't set the maximum capacity of items.
    SetCapacity,
    /// Couldn't acquire the store connection
    Connection,
}

impl Display for RetentionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetentionError::Store => write!(f, "couldn't store publish"),
            RetentionError::Received => write!(f, "couldn't mark publish as received"),
            RetentionError::UpdateSent => write!(f, "couldn't update status to sent"),
            RetentionError::Reset => write!(f, "couldn't reset all publishes"),
            RetentionError::Unsent => write!(f, "couldn't get unset publishes"),
            RetentionError::DeleteInterface => write!(f, "couldn't delete interface"),
            RetentionError::DeleteInterfaceMany => write!(f, "couldn't delete multiple interfaces"),
            RetentionError::FetchInterfaces => write!(f, "couldn't fetch interfaces"),
            RetentionError::SetCapacity => write!(f, "couldn't set capacity"),
            RetentionError::Connection => write!(f, "store operation error"),
        }
    }
}

impl RetentionError {
    pub(crate) fn store(ctx: &'static str, info: &PublishInfo<'_>) -> Error<Self> {
        Error::with(RetentionError::Store, ctx).set_ctx(format!(
            "for {}{}:{}",
            info.interface, info.path, info.version_major
        ))
    }

    pub(crate) fn update_sent(id: Id, flag: bool) -> Error<Self> {
        Error::new(RetentionError::Store).set_ctx(format!("for id {id} and sent flag {flag}"))
    }
}

/// Publish information to be stored.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PublishInfo<'a> {
    pub(crate) interface: Cow<'a, str>,
    pub(crate) path: Cow<'a, str>,
    pub(crate) version_major: i32,
    pub(crate) reliability: Reliability,
    pub(crate) expiry: Option<Duration>,
    pub(crate) sent: bool,
    pub(crate) value: Cow<'a, [u8]>,
}

impl<'a> PublishInfo<'a> {
    pub(crate) fn from_ref(
        interface: &'a str,
        path: &'a str,
        version_major: i32,
        reliability: Reliability,
        retention: Retention,
        sent: bool,
        value: &'a [u8],
    ) -> Self {
        debug_assert!(retention.is_stored());

        Self {
            interface: Cow::Borrowed(interface),
            path: Cow::Borrowed(path),
            version_major,
            reliability,
            expiry: retention.as_expiry().copied(),
            sent,
            value: Cow::Borrowed(value),
        }
    }

    pub(crate) fn from_individual(
        sent: bool,
        individual: &'a ValidatedIndividual,
        value: &'a [u8],
    ) -> Self {
        Self::from_ref(
            &individual.interface,
            &individual.path,
            individual.version_major,
            individual.reliability,
            individual.retention,
            sent,
            value,
        )
    }

    fn from_obj(sent: bool, obj: &'a ValidatedObject, value: &'a [u8]) -> Self {
        Self::from_ref(
            &obj.interface,
            &obj.path,
            obj.version_major,
            obj.reliability,
            obj.retention,
            sent,
            value,
        )
    }

    /// Returns an owned version of the PublishInfo
    fn into_owned(self) -> PublishInfo<'static> {
        PublishInfo {
            interface: self.interface.into_owned().into(),
            path: self.path.into_owned().into(),
            version_major: self.version_major,
            reliability: self.reliability,
            expiry: self.expiry,
            sent: self.sent,
            value: self.value.into_owned().into(),
        }
    }
}

/// Trait to store application packet for a connection.
///
/// A store wants to implement this retention to implement the interfaces with retention stored for
/// a connection.
pub trait StoredRetention: Clone + Send + Sync {
    /// Store a publish.
    fn store_publish(
        &self,
        id: &Id,
        publish: PublishInfo<'_>,
    ) -> impl Future<Output = Result<(), Error<RetentionError>>> + Send;

    /// It will mark the stored publish as sent or unset given the flag.
    fn update_sent_flag(
        &self,
        id: &Id,
        sent: bool,
    ) -> impl Future<Output = Result<(), Error<RetentionError>>> + Send;

    /// It will mark the stored publish as received.
    fn mark_received(
        &self,
        id: &Id,
    ) -> impl Future<Output = Result<(), Error<RetentionError>>> + Send;

    /// Deletes all the stored publishes for the interface.
    fn delete_interface(
        &self,
        interface: &str,
    ) -> impl Future<Output = Result<(), Error<RetentionError>>> + Send;

    /// Resend all the publishes that were not sent.
    ///
    /// It will fetch at most `limit` elements and store them in the [`Vec`], returning the actual
    /// number of stored elements.
    ///
    /// This function is designed to be called multiple times, so the vector is passed from the
    /// caller to be re-used and not reallocated.
    fn unsent_publishes(
        &self,
        limit: usize,
        buf: &mut Vec<(Id, PublishInfo<'static>)>,
    ) -> impl Future<Output = Result<usize, Error<RetentionError>>> + Send;

    /// Marks all publishes as unset and cleans up expired publishes.
    fn reset_all_publishes(&self)
    -> impl Future<Output = Result<(), Error<RetentionError>>> + Send;

    /// Retrieves all the interfaces with data stored in the retention.
    fn fetch_all_interfaces(
        &self,
    ) -> impl Future<Output = Result<HashSet<StoredInterface>, Error<RetentionError>>> + Send;

    /// Set the store size limit.
    fn set_max_retention_items(
        &self,
        size: NonZeroUsize,
    ) -> impl Future<Output = Result<(), Error<RetentionError>>> + Send;
}

/// Interface and major version of a [`PublishInfo`] stored in the retention.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoredInterface {
    /// Name of the interface.
    pub name: String,
    /// Major version.
    pub version_major: i32,
}

impl Display for StoredInterface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name, self.version_major)
    }
}

/// Utility trait that can be used to simplify the boilerplate for the connections.
pub(crate) trait StoredRetentionExt: StoredRetention {
    async fn store_publish_individual(
        &self,
        id: &Id,
        individual: &ValidatedIndividual,
        value: &[u8],
        sent: bool,
    ) -> Result<(), Error<RetentionError>> {
        // Always store as not sent, so we can mark it afterwards
        let publish = PublishInfo::from_individual(sent, individual, value);

        self.store_publish(id, publish).await
    }

    async fn store_publish_object(
        &self,
        id: &Id,
        obj: &ValidatedObject,
        value: &[u8],
        sent: bool,
    ) -> Result<(), Error<RetentionError>> {
        // Always store as not sent, so we can mark it afterwards
        let publish = PublishInfo::from_obj(sent, obj, value);

        self.store_publish(id, publish).await
    }

    /// Removes the outdated interfaces from the introspection
    async fn cleanup_introspection(
        &self,
        interfaces: &Interfaces,
    ) -> Result<(), Error<RetentionError>> {
        let iter = self
            .fetch_all_interfaces()
            .await?
            .into_iter()
            .filter_map(|stored| {
                let Some(interface) = interfaces.get(&stored.name) else {
                    // Stored interface is not in the introspection
                    return Some(stored.name);
                };

                if interface.version_major() != stored.version_major {
                    // Different major version
                    return Some(stored.name);
                }

                None
            });

        futures::stream::iter(iter)
            .then(|interface| async move { self.delete_interface(&interface).await })
            .try_collect::<()>()
            .await?;

        Ok(())
    }
}

impl<T: StoredRetention> StoredRetentionExt for T {}

/// Retention Id to be passed to the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RetentionId {
    Volatile(Id),
    Stored(Id),
}

impl Display for RetentionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetentionId::Volatile(id) => write!(f, "Volatile {id}"),
            RetentionId::Stored(id) => write!(f, "Stored {id}"),
        }
    }
}

/// Id of a publish
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use]
pub struct Id {
    timestamp: TimestampMillis,
    counter: u32,
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp, self.counter)
    }
}

fn duration_from_epoch() -> Duration {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(t) => t,
        Err(err) => {
            warn!(error = %Report::new(&err), "untrusted system clock, time returned before unix epoch");

            // Let's use the positive duration, this will wrap around in weird ways if its,
            // close to the unix epoch
            err.duration()
        }
    }
}

/// Wrapper to a u128 to easily convert it into bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampMillis(u128);

impl TimestampMillis {
    /// Create a new timestamp from the specified milliseconds.
    ///
    /// This should be a [`u128`], but it's not supported from the rust std library and should be
    /// converted manually.
    pub fn from_millis(millis: u128) -> Self {
        Self(millis)
    }

    /// Get the current system time.
    pub fn now() -> Self {
        let timestamp = duration_from_epoch();

        Self(timestamp.as_millis())
    }
    /// Standardize the conversion of the timestamp to bytes.
    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_be_bytes()
    }
}

impl Display for TimestampMillis {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        <u128 as Display>::fmt(&self.0, f)
    }
}

impl TryFrom<TimestampMillis> for Duration {
    type Error = TryFromIntError;

    fn try_from(value: TimestampMillis) -> Result<Self, Self::Error> {
        value.0.try_into().map(Duration::from_millis)
    }
}

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

    /// Returns the next unique id.
    pub fn next(&self) -> Id {
        let timestamp = TimestampMillis::now();

        // We want the values to be unique, this will wrap around, but it will never wrap on the
        // same ms, the ordering can be relaxed since the only guarantee we need is for the counter
        // to yield unique values.
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);

        Id { timestamp, counter }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) async fn stored_mark_unsent<S>(store: &S, id: &Id)
where
    S: StoreCapabilities,
{
    let Some(retention) = store.get_retention() else {
        return;
    };

    let update_result = retention.update_sent_flag(id, false).await;

    if let Err(e) = update_result {
        warn!(error=%Report::new(e),
            "error in the store implementation while marking a record as not sent");
    }
}

pub(crate) async fn volatile_mark_unsent(volatile: &VolatileStore, id: &Id) {
    volatile.mark_sent(id, false).await;
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn id_should_be_unique() {
        const NUM: usize = 5;
        const CAP: usize = 1000;
        let ctx = Arc::new(Context::new());

        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<Id>>(NUM);

        for _ in 0..NUM {
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
            });
        }

        drop(tx);

        let mut recvd = HashSet::new();
        while let Ok(out) = rx.recv() {
            for i in out {
                assert!(recvd.insert(i));
            }
        }
    }
}
