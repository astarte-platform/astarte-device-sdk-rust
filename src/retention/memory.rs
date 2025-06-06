// This file is part of Astarte.
//
// Copyright 2024 - 2025 SECO Mind Srl
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

//! In memory store for the [`volatile`](crate::interface::Retention::Volatile) interfaces.
//!
//! It's a configurable size FIFO cache for the volatile packets.

use std::{
    collections::VecDeque,
    time::{Duration, SystemTime},
};

use astarte_interfaces::interface::Retention;
use tokio::sync::Mutex;
use tracing::{error, trace};

use crate::{
    builder::DEFAULT_VOLATILE_CAPACITY,
    validate::{ValidatedIndividual, ValidatedObject},
};

use super::Id;

/// Struct for the volatile retention.
///
/// The methods will only require a `&self` and handle the locking internally to prevent problems
/// in the critical sections.
#[derive(Debug, Default)]
pub(crate) struct VolatileStore {
    store: Mutex<State>,
}

impl VolatileStore {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            store: Mutex::new(State::with_capacity(capacity)),
        }
    }

    pub(crate) async fn push<T>(&self, id: Id, value: T)
    where
        T: TryInto<ItemValue, Error = VolatileItemError>,
    {
        self.store.lock().await.push(id, value);
    }

    pub(crate) async fn mark_sent(&self, id: &Id, sent: bool) -> Option<bool> {
        self.store.lock().await.mark_sent(id, sent)
    }

    pub(crate) async fn mark_received(&self, id: &Id) -> Option<ItemValue> {
        self.store.lock().await.mark_received(id)
    }

    pub(crate) async fn pop_next(&self) -> Option<ItemValue> {
        self.store.lock().await.pop_next()
    }

    pub(crate) async fn delete_interface(&self, interface_name: &str) -> usize {
        self.store.lock().await.delete_interface(interface_name)
    }

    /// This method will swap the capacity.
    #[cfg(feature = "message-hub")]
    pub(crate) async fn set_capacity(&self, capacity: usize) {
        self.store.lock().await.set_capacity(capacity);
    }
}

#[derive(Debug)]
struct State {
    store: VecDeque<VolatileItem>,
}

impl State {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            store: VecDeque::with_capacity(capacity),
        }
    }

    fn push<T>(&mut self, id: Id, value: T)
    where
        T: TryInto<ItemValue, Error = VolatileItemError>,
    {
        if self.is_full() {
            if self.store.capacity() == 0 {
                // we shouldn't store anything
                return;
            }

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

        self.store.push_back(VolatileItem::new(id, item));
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

    fn pop_next(&mut self) -> Option<ItemValue> {
        let now = SystemTime::now();

        std::iter::from_fn(|| self.store.pop_front())
            .find(|item| !item.is_expired(now))
            .map(|item| item.value)
    }

    fn remove_expired(&mut self) {
        let now = SystemTime::now();

        self.store.retain(|item| !item.is_expired(now));
    }

    fn is_full(&mut self) -> bool {
        self.store.len() == self.store.capacity()
    }

    /// A capacity of 0 will make every push into this store a noop.
    #[cfg(feature = "message-hub")]
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

    fn delete_interface(&mut self, interface_name: &str) -> usize {
        let now = SystemTime::now();

        let mut count = 0;

        self.store.retain(|v| {
            let expired_or_interface = v.is_expired(now) || v.is_interface(interface_name);

            if expired_or_interface {
                count += 1;
            }

            !expired_or_interface
        });

        trace!(count, "interface removed");

        count
    }
}

impl Default for State {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_VOLATILE_CAPACITY)
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
    fn new(id: Id, value: ItemValue) -> Self {
        Self {
            id,
            sent: false,
            store_time: SystemTime::now(),
            value,
        }
    }

    fn is_expired(&self, now: SystemTime) -> bool {
        let Some(expiry) = self.value.expiry() else {
            return false;
        };

        let expired = (self.store_time + expiry) < now;

        if expired {
            trace!(%self.id, "expired");
        }

        expired
    }

    fn is_interface(&self, interface_name: &str) -> bool {
        let name = match &self.value {
            ItemValue::Individual(validated_individual) => &validated_individual.interface,
            ItemValue::Object(validated_object) => &validated_object.interface,
        };

        name == interface_name
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
            ItemValue::Individual(i) => i.retention.as_expiry().copied(),
            ItemValue::Object(o) => o.retention.as_expiry().copied(),
        }
    }

    /// Allow the stored retention, if the [`StoreCapabilities`](crate::store::StoreCapabilities) doesn't support the retention.
    fn is_retenton_stored_or_volatile(retention: Retention) -> bool {
        matches!(
            retention,
            Retention::Stored { .. } | Retention::Volatile { .. }
        )
    }
}

impl TryFrom<ValidatedIndividual> for ItemValue {
    type Error = VolatileItemError;

    fn try_from(value: ValidatedIndividual) -> Result<Self, Self::Error> {
        if !Self::is_retenton_stored_or_volatile(value.retention) {
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
        if !Self::is_retenton_stored_or_volatile(value.retention) {
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

    use astarte_interfaces::interface::Retention;
    use astarte_interfaces::schema::Reliability;
    use pretty_assertions::assert_eq;

    use crate::{aggregate::AstarteObject, retention::Context, AstarteType};

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

        let mut store = State::with_capacity(1);

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

        let mut store = State::with_capacity(1);
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

        let mut store = State::with_capacity(2);

        let ctx = Context::new();

        store.push(ctx.next(), info1);
        store.push(ctx.next(), info2);

        store.store[0].store_time -= Duration::from_secs(1);

        assert!(store.is_full());

        store.push(ctx.next(), info3.clone());

        assert_eq!(store.store[0].value, ItemValue::Individual(info3));
    }

    #[test]
    fn should_pop_non_expired() {
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

        let mut store = State::with_capacity(3);

        let ctx = Context::new();

        store.push(ctx.next(), info1);
        store.push(ctx.next(), info2);
        store.push(ctx.next(), info3.clone());

        store.store[0].store_time -= Duration::from_secs(1);
        store.store[1].store_time -= Duration::from_secs(1);

        assert_eq!(store.pop_next(), Some(ItemValue::Individual(info3)));
        assert!(store.store.is_empty());
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

        let info = ValidatedObject {
            interface: "interface1".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Discard,
            data: AstarteObject::new(),
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

        let mut store = State::with_capacity(3);

        let ctx = Context::new();

        store.push(ctx.next(), info1);
        let id = ctx.next();
        store.push(id, info2);
        store.push(ctx.next(), info3.clone());

        assert!(!store.mark_sent(&id, true).unwrap());

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

        let mut store = State::with_capacity(3);

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
    fn capacity_0_volatile_store_should_not_store() {
        let mut store = State::with_capacity(0);
        let ctx = Context::new();

        let info = ValidatedIndividual {
            interface: "interface3".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        store.push(ctx.next(), info.clone());

        assert_eq!(None, store.pop_next());
    }

    #[test]
    fn should_accept_stored_retention_items() {
        let mut store = State::with_capacity(1);
        let ctx = Context::new();

        let info = ValidatedIndividual {
            interface: "interface3".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Stored { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        store.push(ctx.next(), info.clone());

        assert_eq!(store.pop_next().unwrap(), ItemValue::Individual(info));

        let info = ValidatedObject {
            interface: "interface3".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Stored { expiry: None },
            data: AstarteObject::new(),
            timestamp: None,
        };

        store.push(ctx.next(), info.clone());

        assert_eq!(store.pop_next().unwrap(), ItemValue::Object(info));
    }

    #[test]
    fn should_delete_interfaces() {
        let mut store = State::default();
        let ctx = Context::new();

        let interface = "interface_individual";
        let individual = ValidatedIndividual {
            interface: interface.to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Stored { expiry: None },
            data: AstarteType::Integer(42),
            timestamp: None,
        };

        let object = ValidatedObject {
            interface: "interface_object".to_string(),
            path: "path".to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Stored { expiry: None },
            data: AstarteObject::new(),
            timestamp: None,
        };

        store.push(ctx.next(), individual.clone());
        store.push(ctx.next(), individual.clone());
        store.push(ctx.next(), object.clone());

        assert_eq!(store.delete_interface(interface), 2);

        assert_eq!(store.pop_next().unwrap(), ItemValue::Object(object));
    }
}
