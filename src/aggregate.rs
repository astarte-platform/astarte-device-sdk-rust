// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
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

//! Value to send or receive from Astarte.

use serde::ser::SerializeMap;
use serde::Serialize;

use crate::types::AstarteType;

/// Map of name and [`AstarteType`].
///
/// This is used to send data on an interface with object aggregation.
///
/// # Example
///
/// Parse this data structure into a `HashMap` compatible with transmission of Astarte objects.
///
/// ```
/// use astarte_device_sdk::aggregate::AstarteObject;
/// use astarte_device_sdk::types::AstarteType;
///
/// let sensor = AstarteType::String("light".to_string());
/// let id = AstarteType::Integer(42i32);
///
/// let mut object = AstarteObject::new();
/// object.insert("name".to_string(), sensor.clone());
/// object.insert("id".to_string(),  id.clone());
///
/// assert_eq!(object.get("name"), Some(&sensor));
/// assert_eq!(object.get("id"), Some(&id));
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Default)]
pub struct AstarteObject {
    pub(crate) inner: Vec<(String, AstarteType)>,
}

impl AstarteObject {
    /// Returns an empty object.
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Returns an empty object with the given capacity pre-allocated
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
        }
    }

    /// Add the value with the given name to the object.
    ///
    /// The keys of the object are unique. It overrides and returns the previous value if it's already set.
    pub fn insert(&mut self, key: String, value: AstarteType) -> Option<AstarteType> {
        match self.inner.iter_mut().find(|(item, _)| *item == key) {
            Some((_, old)) => Some(std::mem::replace(old, value)),
            None => {
                self.inner.push((key, value));

                None
            }
        }
    }

    /// Returns a reference to the value with the given name, if present.
    pub fn get(&self, name: &str) -> Option<&AstarteType> {
        self.inner
            .iter()
            .find_map(|(item, value)| (*item == name).then_some(value))
    }

    /// Remove the value with the given name, if present.
    pub fn remove(&mut self, name: &str) -> Option<AstarteType> {
        let position = self.inner.iter().position(|(item, _)| *item == name)?;

        let (_name, value) = self.inner.swap_remove(position);

        Some(value)
    }

    /// Returns the number of mapping elements.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the object has no mapping data.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Iterates the name and values of the object.
    pub fn iter(&self) -> impl Iterator<Item = &(String, AstarteType)> {
        self.inner.iter()
    }

    /// Iterates the name and values of the object.
    pub fn into_key_values(self) -> impl Iterator<Item = (String, AstarteType)> {
        self.inner.into_iter()
    }
}

impl FromIterator<(String, AstarteType)> for AstarteObject {
    fn from_iter<T: IntoIterator<Item = (String, AstarteType)>>(iter: T) -> Self {
        Self {
            inner: Vec::from_iter(iter),
        }
    }
}

impl Serialize for AstarteObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s_map = serializer.serialize_map(Some(self.len()))?;
        for (key, value) in self.iter() {
            s_map.serialize_entry(key, value)?;
        }
        s_map.end()
    }
}

/// Data for an [`Astarte data event`](crate::DeviceEvent).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Individual data, can be both from a datastream or property.
    Individual(AstarteType),
    /// Object data, also called aggregate. Can only be from a datastream.
    Object(AstarteObject),
    /// Unset of a property
    Unset,
}

impl Value {
    /// Returns `true` if the aggregation is [`Individual`].
    ///
    /// [`Individual`]: Value::Individual
    #[must_use]
    pub fn is_individual(&self) -> bool {
        matches!(self, Self::Individual(..))
    }

    /// Get a reference to the [`AstarteType`] if the aggregate is
    /// [`Individual`](Value::Individual).
    pub fn as_individual(&self) -> Option<&AstarteType> {
        if let Self::Individual(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Take out of the enum an [`AstarteType`] if the aggregate is
    /// [`Individual`](Value::Individual).
    pub fn take_individual(self) -> Option<AstarteType> {
        if let Self::Individual(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the aggregation is [`Object`].
    ///
    /// [`Object`]: Value::Object
    #[must_use]
    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object(..))
    }

    /// Get a reference to the [`AstarteObject`] if the aggregate is [`Object`](Value::Object).
    pub fn as_object(&self) -> Option<&AstarteObject> {
        if let Self::Object(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Take out of the enum an [`HashMap`] if the aggregate is [`Object`](Value::Object).
    pub fn take_object(self) -> Option<AstarteObject> {
        if let Self::Object(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the aggregation is [`Unset`].
    ///
    /// [`Unset`]: Value::Unset
    #[must_use]
    pub fn is_unset(&self) -> bool {
        matches!(self, Self::Unset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_increase_coverage() {
        let individual = AstarteType::Integer(42);
        let val = Value::Individual(AstarteType::Integer(42));
        assert!(val.is_individual());
        assert_eq!(val.as_individual(), Some(&individual));
        assert_eq!(val.as_object(), None);

        let val = Value::Object(AstarteObject::new());
        assert!(val.is_object());
        assert_eq!(val.as_individual(), None);
        assert_eq!(val.as_object(), Some(&AstarteObject::new()));

        assert!(Value::Unset.is_unset());
    }
}
