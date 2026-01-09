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

use serde::Serialize;
use serde::ser::SerializeMap;

use crate::types::AstarteData;

/// Map of name and [`AstarteData`].
///
/// This is used to send data on an interface with object aggregation.
///
/// # Example
///
/// Create the data structure `AstarteObject` used to transmit of Astarte objects.
///
/// ```
/// use astarte_device_sdk::aggregate::AstarteObject;
/// use astarte_device_sdk::types::AstarteData;
///
/// let sensor = AstarteData::String("light".to_string());
/// let id = AstarteData::Integer(42i32);
///
/// let mut object = AstarteObject::new();
/// object.insert("name".to_string(), sensor.clone());
/// object.insert("id".to_string(),  id.clone());
///
/// assert_eq!(object.get("name"), Some(&sensor));
/// assert_eq!(object.get("id"), Some(&id));
/// ```
#[derive(Debug, Clone, Default)]
pub struct AstarteObject {
    pub(crate) inner: Vec<(String, AstarteData)>,
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
    pub fn insert(&mut self, key: String, value: AstarteData) -> Option<AstarteData> {
        match self.inner.iter_mut().find(|(item, _)| *item == key) {
            Some((_, old)) => Some(std::mem::replace(old, value)),
            None => {
                self.inner.push((key, value));

                None
            }
        }
    }

    /// Returns a reference to the value with the given name, if present.
    pub fn get(&self, name: &str) -> Option<&AstarteData> {
        self.inner
            .iter()
            .find_map(|(item, value)| (*item == name).then_some(value))
    }

    /// Remove the value with the given name, if present.
    pub fn remove(&mut self, name: &str) -> Option<AstarteData> {
        let position = self.inner.iter().position(|(item, _)| *item == name)?;

        let (_name, value) = self.inner.swap_remove(position);

        Some(value)
    }

    /// Returns the number of mapping data in the object.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the object has no mapping data.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Iterates the name and values of the object.
    pub fn iter(&self) -> impl Iterator<Item = &(String, AstarteData)> {
        self.inner.iter()
    }

    /// Iterates the name and values of the object.
    pub fn into_key_values(self) -> impl Iterator<Item = (String, AstarteData)> {
        self.inner.into_iter()
    }
}

impl FromIterator<(String, AstarteData)> for AstarteObject {
    fn from_iter<T: IntoIterator<Item = (String, AstarteData)>>(iter: T) -> Self {
        Self {
            inner: Vec::from_iter(iter),
        }
    }
}

// Ignore the order of elements for equality
impl PartialEq for AstarteObject {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        self.iter()
            .all(|(key, value)| other.get(key).is_some_and(|other| other == value))
    }
}

/// Serialize the [`AstarteObject`] as a map.
impl Serialize for AstarteObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(self.len()))?;
        for (name, value) in self.iter() {
            s.serialize_entry(name, value)?;
        }
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn create_with_capacity() {
        let exp = 10;
        let object = AstarteObject::with_capacity(exp);
        assert_eq!(object.inner.capacity(), exp);
    }

    #[test]
    fn add_value_to_obj_and_replace() {
        let mut object = AstarteObject::new();
        let exp = AstarteData::from("foo");
        object.insert("foo".to_string(), exp.clone());
        assert_eq!(object.get("foo"), Some(&exp));

        let exp = AstarteData::from("other");
        object.insert("foo".to_string(), exp.clone());
        assert_eq!(object.get("foo"), Some(&exp));
    }

    #[test]
    fn iter_object_values() {
        let values = [
            ("foo", AstarteData::from("foo")),
            ("bar", AstarteData::from("bar")),
            ("some", AstarteData::from("some")),
        ]
        .map(|(n, v)| (n.to_string(), v));

        let object = AstarteObject::from_iter(values.clone());

        assert!(!object.is_empty());
        assert_eq!(object.len(), values.len());

        for (exp, val) in object.iter().zip(&values) {
            assert_eq!(exp, val)
        }

        for (exp, val) in object.into_key_values().zip(values) {
            assert_eq!(exp, val)
        }
    }

    #[test]
    fn astarte_object_custom_serialize_map() {
        let values = [
            ("foo", AstarteData::from("foo")),
            ("bar", AstarteData::from("bar")),
            ("some", AstarteData::from("some")),
        ]
        .map(|(n, v)| (n.to_string(), v));

        let object = AstarteObject::from_iter(values.clone());

        let json = serde_json::to_string(&object).unwrap();

        let de: serde_json::Value = serde_json::from_str(&json).unwrap();
        let map = de.as_object().unwrap();
        assert_eq!(map.len(), object.len());
        let foo = map.get("foo").and_then(serde_json::Value::as_str).unwrap();
        assert_eq!(foo, "foo");
        let bar = map.get("bar").and_then(serde_json::Value::as_str).unwrap();
        assert_eq!(bar, "bar");
        let some = map.get("some").and_then(serde_json::Value::as_str).unwrap();
        assert_eq!(some, "some");
    }

    #[test]
    fn astarte_object_custom_partial_eq() {
        let values = [
            ("foo", AstarteData::from("foo")),
            ("bar", AstarteData::from("bar")),
            ("some", AstarteData::from("some")),
        ]
        .map(|(n, v)| (n.to_string(), v));

        let other: Vec<(String, AstarteData)> = values.iter().rev().cloned().collect();

        let value = AstarteObject::from_iter(values);
        let other = AstarteObject::from_iter(other);

        assert_eq!(value, other);
    }
}
