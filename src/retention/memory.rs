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
    time::{Duration, SystemTime},
};

use tracing::error;

use crate::{
    builder::DEFAULT_VOLATILE_CAPACITY,
    interface::Retention,
    validate::{ValidatedIndividual, ValidatedObject},
};

pub(crate) struct VolatileRetention {
    store: VecDeque<VolitileItem>,
}

impl VolatileRetention {
    pub(crate) fn new() -> Self {
        Self::with_capacity(DEFAULT_VOLATILE_CAPACITY)
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            store: VecDeque::with_capacity(capacity),
        }
    }

    pub(crate) fn push<T>(&mut self, value: T)
    where
        T: TryInto<ItemValue, Error = VolitileItemError>,
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
        debug_assert!(res.is_ok(), "BUG: value should hanve retention volatile");

        let item = match res {
            Ok(item) => item,
            Err(err) => {
                error!("{err}");

                return;
            }
        };

        self.store.push_back(VolitileItem::new(item));
    }

    pub(crate) fn pop_next(&mut self) -> Option<ItemValue> {
        let now = SystemTime::now();

        std::iter::from_fn(|| self.store.pop_front())
            .find(|item| !item.is_expiered(now))
            .map(|item| item.value)
    }

    fn remove_expired(&mut self) {
        let now = SystemTime::now();

        self.store.retain(|item| !item.is_expiered(now));
    }

    fn is_full(&mut self) -> bool {
        self.store.len() == self.store.capacity()
    }
}

impl Default for VolatileRetention {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct VolitileItem {
    store_time: SystemTime,
    value: ItemValue,
}

impl VolitileItem {
    fn new(value: ItemValue) -> Self {
        Self {
            store_time: SystemTime::now(),
            value,
        }
    }

    fn is_expiered(&self, now: SystemTime) -> bool {
        let Some(expiry) = self.value.expiry() else {
            return false;
        };

        (self.store_time + expiry) < now
    }
}

/// Failed to store publish information for interface without volatile retention.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
#[error("interface {interface} has't retention volatile, but has {retention:?} instead")]
pub(crate) struct VolitileItemError {
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
    type Error = VolitileItemError;

    fn try_from(value: ValidatedIndividual) -> Result<Self, Self::Error> {
        if !value.retention.is_volatile() {
            return Err(VolitileItemError {
                interface: value.interface,
                retention: value.retention,
            });
        }

        Ok(Self::Individual(value))
    }
}

impl TryFrom<ValidatedObject> for ItemValue {
    type Error = VolitileItemError;

    fn try_from(value: ValidatedObject) -> Result<Self, Self::Error> {
        if !value.retention.is_volatile() {
            return Err(VolitileItemError {
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

        let mut store = VolatileRetention::with_capacity(1);

        store.push(info);

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

        let mut store = VolatileRetention::with_capacity(1);

        store.push(info1);

        assert!(store.is_full());

        store.push(info2.clone());

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

        let mut store = VolatileRetention::with_capacity(2);

        store.push(info1);
        store.push(info2);

        store.store[0].store_time -= Duration::from_secs(1);

        assert!(store.is_full());

        store.push(info3.clone());

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

        let mut store = VolatileRetention::with_capacity(3);

        store.push(info1);
        store.push(info2);
        store.push(info3.clone());

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
    }
}
