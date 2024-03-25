// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
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

use std::collections::HashMap;

use crate::{error::Error, types::AstarteType};

/// A **trait** required by all data to be sent using
/// [send_object()][crate::Client::send_object] and
/// [send_object_with_timestamp()][crate::Client::send_object_with_timestamp].
/// It ensures correct parsing of the data.
///
/// The returned hash map should have as values the data to transmit for each
/// object endpoint and as keys the endpoints themselves.
///
/// The Astarte Device SDK provides a procedural macro that can be used to automatically
/// generate `AstarteAggregate` implementations for Structs.
/// To use the procedural macro enable the feature `derive`.
pub trait AstarteAggregate {
    /// Parse this data structure into a `HashMap` compatible with transmission of Astarte objects.
    /// ```
    /// use std::collections::HashMap;
    /// use std::convert::TryInto;
    ///
    /// use astarte_device_sdk::{types::AstarteType, error::Error, AstarteAggregate};
    ///
    /// struct Person {
    ///     name: String,
    ///     age: i32,
    ///     phones: Vec<String>,
    /// }
    ///
    /// // This is what #[derive(AstarteAggregate)] would generate.
    /// impl AstarteAggregate for Person {
    ///     fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error>
    ///     {
    ///         let mut r = HashMap::new();
    ///         r.insert("name".to_string(), self.name.try_into()?);
    ///         r.insert("age".to_string(), self.age.try_into()?);
    ///         r.insert("phones".to_string(), self.phones.try_into()?);
    ///         Ok(r)
    ///     }
    /// }
    /// ```
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error>;
}

impl AstarteAggregate for HashMap<String, AstarteType> {
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error> {
        Ok(self)
    }
}

/// Data for an [`Astarte data event`](crate::AstarteDeviceDataEvent).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Individual data, can be both from a datastream or property.
    Individual(AstarteType),
    /// Object data, also called aggregate. Can only be from a datastream.
    Object(HashMap<String, AstarteType>),
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

    /// Returns `true` if the aggregation is [`Object`].
    ///
    /// [`Object`]: Value::Object
    #[must_use]
    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object(..))
    }

    /// Get a reference to the [`HashMap`] if the aggregate is [`Object`](Value::Object).
    pub fn as_object(&self) -> Option<&HashMap<String, AstarteType>> {
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

        let val = Value::Object(HashMap::new());
        assert!(val.is_object());
        assert_eq!(val.as_individual(), None);
        assert_eq!(val.as_object(), Some(&HashMap::new()));

        assert!(Value::Unset.is_unset());
    }
}
