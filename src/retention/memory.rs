// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
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

//! In memory store for the [`volatile`](crate::interface::Retention::Volatile) interfaces.
//!
//! It's a configurable size FIFO cache for the volatile packets.

use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::sync::Mutex;
use tracing::error;

use crate::{
    interface::Retention,
    validate::{ValidatedIndividual, ValidatedObject},
};

use super::Id;

/// Shared struct for the volatile retention.
///
/// The methods will only require a `&self` and handle the locking internally to prevent problems
/// in the critical sections.
#[derive(Debug, Clone)]
pub(crate) struct SharedVolatileStore {
    store: Arc<Mutex<VolatileStore>>,
}

impl SharedVolatileStore {
    pub(crate) fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(VolatileStore::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            store: Arc::new(Mutex::new(VolatileStore::with_capacity(capacity))),
        }
    }

    pub(crate) async fn push<T>(&self, id: Id, value: T)
    where
        T: TryInto<ItemValue, Error = VolatileItemError>,
    {
        self.store.lock().await.push(id, value);
    }

    pub(crate) async fn push_unsent<T>(&self, id: Id, value: T)
    where
        T: TryInto<ItemValue, Error = VolatileItemError>,
    {
        self.store.lock().await.push_unsent(id, value);
    }

    pub(crate) async fn mark_sent(&self, id: &Id, sent: bool) -> Option<bool> {
        self.store.lock().await.mark_sent(id, sent)
    }

    pub(crate) async fn mark_received(&self, id: &Id) -> Option<ItemValue> {
        self.store.lock().await.mark_received(id)
    }

    pub(crate) async fn get_unsent(&self, buf: &mut Vec<(Id, ItemValue)>, limit: usize) -> usize {
        self.store.lock().await.get_unsent(buf, limit)
    }

    pub(crate) async fn reset_sent(&self) {
        self.store.lock().await.reset_sent()
    }

    /// This method will swap the capacity.
    pub(crate) async fn set_capacity(&self, capacity: usize) {
        self.store.lock().await.set_capacity(capacity);
    }
}

#[derive(Debug)]
struct VolatileStore {
    store: VecDeque<VolatileItem>,
}

impl VolatileStore {
    fn new() -> Self {
        Self {
            store: VecDeque::new(),
        }
    }

    #[cfg(test)]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            store: VecDeque::with_capacity(capacity),
        }
    }

    fn push<T>(&mut self, id: Id, value: T)
    where
        T: TryInto<ItemValue, Error = VolatileItemError>,
    {
        self.push_item(id, value, true);
    }

    fn push_unsent<T>(&mut self, id: Id, value: T)
    where
        T: TryInto<ItemValue, Error = VolatileItemError>,
    {
        self.push_item(id, value, false);
    }

    fn push_item<T>(&mut self, id: Id, value: T, sent: bool)
    where
        T: TryInto<ItemValue, Error = VolatileItemError>,
    {
        if self.is_full() {
            // remote the expired only if its full, it will be done while iterating
            self.remove_expired();

            // If still full, remove the oldest one
            if self.is_full() {
                self.store.pop_front();
            }
        }

        let res = value.try_into();
        debug_assert!(res.is_ok(), "BUG: value should have retention volatile");

        let item = match res {
            Ok(item) => item,
            Err(err) => {
                error!("{err}");

                return;
            }
        };

        self.store.push_back(VolatileItem::new(id, item, sent));
    }

    fn mark_sent(&mut self, id: &Id, sent: bool) -> Option<bool> {
        self.store
            .iter_mut()
            .find(|item| item.id == *id)
            .map(|item| std::mem::replace(&mut item.sent, sent))
    }

    fn mark_received(&mut self, id: &Id) -> Option<ItemValue> {
        let idx = self
            .store
            .iter()
            .enumerate()
            .find_map(|(idx, item)| (item.id == *id).then_some(idx))?;

        self.store.remove(idx).map(|item| item.value)
    }

    fn reset_sent(&mut self) {
        self.store.iter_mut().for_each(|item| {
            item.sent = false;
        })
    }

    fn get_unsent(&mut self, unsent: &mut Vec<(Id, ItemValue)>, limit: usize) -> usize {
        self.remove_expired();

        let before = unsent.len();

        let unsent_iter = self
            .store
            .iter()
            .filter(|item| !item.sent)
            .map(|item| (item.id, item.value.clone()))
            .take(limit);

        unsent.extend(unsent_iter);

        unsent.len() - before
    }

    fn remove_expired(&mut self) {
        let now = SystemTime::now();

        self.store.retain(|item| !item.is_expired(now));
    }

    fn is_full(&mut self) -> bool {
        self.store.len() == self.store.capacity()
    }

    fn set_capacity(&mut self, capacity: usize) {
        let current = self.store.capacity();

        if capacity < current {
            let diff = current.saturating_sub(capacity);
            // Remove first elements
            self.store.drain(..diff);

            self.store.shrink_to_fit();
        } else {
            // Number of elements that needed to be reserved
            let additional = capacity.saturating_sub(self.store.len());

            self.store.reserve_exact(additional);
        }
    }
}

impl Default for VolatileStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct VolatileItem {
    id: Id,
    store_time: SystemTime,
    sent: bool,
    value: ItemValue,
}

impl VolatileItem {
    fn new(id: Id, value: ItemValue, sent: bool) -> Self {
        Self {
            id,
            sent,
            store_time: SystemTime::now(),
            value,
        }
    }

    fn is_expired(&self, now: SystemTime) -> bool {
        let Some(expiry) = self.value.expiry() else {
            return false;
        };

        (self.store_time + expiry) < now
    }
}

/// Failed to store publish information for interface without volatile retention.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
#[error("interface {interface} doesn't have retention volatile, but has {retention:?} instead")]
pub(crate) struct VolatileItemError {
    interface: String,
    retention: Retention,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ItemValue {
    Individual(ValidatedIndividual),
    Object(ValidatedObject),
}

impl ItemValue {
    fn expiry(&self) -> Option<Duration> {
        match self {
            ItemValue::Individual(i) => i.retention.expiry(),
            ItemValue::Object(o) => o.retention.expiry(),
        }
    }
}

impl TryFrom<ValidatedIndividual> for ItemValue {
    type Error = VolatileItemError;

    fn try_from(value: ValidatedIndividual) -> Result<Self, Self::Error> {
        if !value.retention.is_volatile() {
            return Err(VolatileItemError {
                interface: value.interface,
                retention: value.retention,
            });
        }

        Ok(Self::Individual(value))
    }
}

impl TryFrom<ValidatedObject> for ItemValue {
    type Error = VolatileItemError;

    fn try_from(value: ValidatedObject) -> Result<Self, Self::Error> {
        if !value.retention.is_volatile() {
            return Err(VolatileItemError {
                interface: value.interface,
                retention: value.retention,
            });
        }

        Ok(Self::Object(value))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        interface::{Reliability, Retention},
        retention::Context,
        AstarteType,
    };

    use super::*;

    #[test]
    fn should_be_full() {
        let info = ValidatedIndividual {
            interface: "interface".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let mut store = VolatileStore::with_capacity(1);

        let ctx = Context::new();

        store.push(ctx.next(), info);

        assert!(store.is_full());
    }

    #[test]
    fn should_remove_last() {
        let info1 = ValidatedIndividual {
            interface: "interface1".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let info2 = ValidatedIndividual {
            interface: "interface2".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let mut store = VolatileStore::with_capacity(1);
        let ctx = Context::new();

        store.push(ctx.next(), info1);

        assert!(store.is_full());

        store.push(ctx.next(), info2.clone());

        assert_eq!(store.store[0].value, ItemValue::Individual(info2));
    }

    #[test]
    fn should_remove_expired() {
        let info1 = ValidatedIndividual {
            interface: "interface1".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info2 = ValidatedIndividual {
            interface: "interface2".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info3 = ValidatedIndividual {
            interface: "interface3".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let mut store = VolatileStore::with_capacity(2);

        let ctx = Context::new();

        store.push(ctx.next(), info1);
        store.push(ctx.next(), info2);

        store.store[0].store_time -= Duration::from_secs(1);

        assert!(store.is_full());

        store.push(ctx.next(), info3.clone());

        assert_eq!(store.store[0].value, ItemValue::Individual(info3));
    }

    #[test]
    fn should_clone_non_expired() {
        let info1 = ValidatedIndividual {
            interface: "interface1".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info2 = ValidatedIndividual {
            interface: "interface2".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info3 = ValidatedIndividual {
            interface: "interface3".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let mut store = VolatileStore::with_capacity(3);

        let ctx = Context::new();

        store.push(ctx.next(), info1);
        store.push(ctx.next(), info2);
        let id_info_3 = ctx.next();
        store.push(id_info_3, info3.clone());

        store.store[0].store_time -= Duration::from_secs(1);
        store.store[1].store_time -= Duration::from_secs(1);

        store.reset_sent();

        let mut buf = Vec::with_capacity(1);
        assert_eq!(store.get_unsent(&mut buf, 1), 1);
        let (id, item) = buf.pop().unwrap();
        assert_eq!((id, item), (id_info_3, ItemValue::Individual(info3)));

        // the expired element get evicted
        assert_eq!(store.store.len(), 1);
    }

    #[test]
    fn check_retention_volatile() {
        let info = ValidatedIndividual {
            interface: "interface1".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Discard,
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let res = ItemValue::try_from(info);

        assert!(res.is_err());
    }

    #[test]
    fn should_mark_sent() {
        let info1 = ValidatedIndividual {
            interface: "interface1".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info2 = ValidatedIndividual {
            interface: "interface2".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info3 = ValidatedIndividual {
            interface: "interface3".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let mut store = VolatileStore::with_capacity(3);

        let ctx = Context::new();

        store.push(ctx.next(), info1);
        let id = ctx.next();
        store.push(id, info2);
        store.push(ctx.next(), info3.clone());

        // assert that the message is marked as sent during creation
        assert!(store.mark_sent(&id, true).unwrap());

        assert!(store.store[1].sent)
    }

    #[test]
    fn should_mark_received() {
        let info1 = ValidatedIndividual {
            interface: "interface1".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info2 = ValidatedIndividual {
            interface: "interface2".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info3 = ValidatedIndividual {
            interface: "interface3".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let mut store = VolatileStore::with_capacity(3);

        let ctx = Context::new();

        store.push(ctx.next(), info1);
        let id = ctx.next();
        store.push(id, info2);
        store.push(ctx.next(), info3.clone());

        assert!(store.mark_received(&id).is_some());

        assert_eq!(store.store.len(), 2);
        assert_eq!(store.store[1].value, ItemValue::Individual(info3));
    }

    #[test]
    fn should_mark_all_unsent() {
        let info1 = ValidatedIndividual {
            interface: "interface1".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info2 = ValidatedIndividual {
            interface: "interface2".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_nanos(1)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info3 = ValidatedIndividual {
            interface: "interface3".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let mut store = VolatileStore::with_capacity(3);

        let ctx = Context::new();

        let id = ctx.next();
        store.push(id, info1);
        store.mark_sent(&id, true);
        let id = ctx.next();
        store.push(id, info2);
        store.mark_sent(&id, true);
        let id = ctx.next();
        store.push(id, info3.clone());
        store.mark_sent(&id, true);

        store.reset_sent();

        assert!(!store.store[0].sent);
        assert!(!store.store[1].sent);
        assert!(!store.store[2].sent);
    }

    #[test]
    fn should_return_unsent_only() {
        let info = ValidatedIndividual {
            interface: "interface1".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_secs(3599)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };
        let info_unsent_check = ValidatedIndividual {
            interface: "interface_check".to_string(),
            path: "path".to_string(),
            version_major: 1,
            reliability: Reliability::Unique,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_secs(3600)),
            },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let mut store = VolatileStore::with_capacity(50);

        let ctx = Context::new();

        const ELEMENTS: usize = 50;

        for i in 0..ELEMENTS {
            let id = ctx.next();

            // 4 elements marked as not sent
            if (i + 1) % 25 == 0 {
                store.push(id, info_unsent_check.clone());
                store.mark_sent(&id, false);
                let id = ctx.next();
                store.push(id, info_unsent_check.clone());
                store.mark_sent(&id, false);
            } else {
                store.push(id, info.clone());
                store.mark_sent(&id, true);
            }
        }

        let check_is_unsent_element = |item: &ItemValue| {
            matches!(item, ItemValue::Individual(ref individual)
                if individual.interface == "interface_check" && individual.version_major == 1)
        };

        let mut buf = Vec::with_capacity(4);

        store.get_unsent(&mut buf, 50);

        assert_eq!(buf.len(), 4);
        assert!(check_is_unsent_element(&buf[0].1));
        assert!(check_is_unsent_element(&buf[1].1));
        assert!(check_is_unsent_element(&buf[2].1));
        assert!(check_is_unsent_element(&buf[3].1));
    }
}
