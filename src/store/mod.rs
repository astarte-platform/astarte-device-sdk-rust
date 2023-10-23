// This file is part of Astarte.
//
// Copyright 2021 SECO Mind Srl
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

//! Provides functionality for instantiating an Astarte sqlite database.

use chrono::Utc;
use std::{error::Error as StdError, fmt::Debug};

use async_trait::async_trait;

use crate::interface::Reliability;
use crate::{interface::Ownership, types::AstarteType};

pub use self::sqlite::SqliteStore;

pub mod error;
pub mod memory;
pub mod sqlite;
pub mod wrapper;

/// Trait providing compatibility with Astarte devices to databases.
///
/// Any database implementing this trait can be used as permanent storage for the properties
/// of an Astarte device.
///
/// This SDK provides an implementation of a sqlite database for which this trait has already
/// been implemented, see [`crate::store::sqlite::SqliteStore`].
#[async_trait]
pub trait PropertyStore: Debug + Send + Sync + 'static
where
    // NOTE: the bounds are required to be compatible with the tokio tasks, with an additional Sync
    //       bound to further restrict the error type.
    Self::Err: StdError + Send + Sync + 'static,
{
    type Err;

    /// Stores a property within the database.
    async fn store_prop(&self, prop: StoredProp<&str, &AstarteType>) -> Result<(), Self::Err>;
    /// Load a property from the database.
    ///
    /// The property store should delete the property from the database if the major version of the
    /// interface does not match the one provided.
    async fn load_prop(
        &self,
        interface: &str,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, Self::Err>;
    /// Delete a property from the database.
    async fn delete_prop(&self, interface: &str, path: &str) -> Result<(), Self::Err>;
    /// Removes all saved properties from the database.
    async fn clear_props(&self) -> Result<(), Self::Err>;
    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err>;
    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err>;
    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err>;
    /// Retrieves all the property values of a specific interface in the database.
    async fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, Self::Err>;
}

/// Data structure used to return stored properties by a database implementing the AstarteDatabase
/// trait.
#[derive(Debug, Clone, Copy, PartialOrd)]
pub struct StoredProp<S = String, V = AstarteType> {
    pub interface: S,
    pub path: S,
    pub value: V,
    pub interface_major: i32,
    pub ownership: Ownership,
}

impl StoredProp {
    pub fn as_ref(&self) -> StoredProp<&str, &AstarteType> {
        self.into()
    }
}

impl<'a> From<&'a StoredProp> for StoredProp<&'a str, &'a AstarteType> {
    fn from(value: &'a StoredProp) -> Self {
        Self {
            interface: &value.interface,
            path: &value.path,
            value: &value.value,
            interface_major: value.interface_major,
            ownership: value.ownership,
        }
    }
}

impl<T, U, V, W> PartialEq<StoredProp<T, V>> for StoredProp<U, W>
where
    U: PartialEq<T>,
    W: PartialEq<V>,
{
    fn eq(&self, other: &StoredProp<T, V>) -> bool {
        self.interface == other.interface
            && self.path == other.path
            && self.value == other.value
            && self.interface_major == other.interface_major
            && self.ownership == other.ownership
    }
}

#[derive(Debug, Clone)]
pub struct RetentionMessage {
    pub(self) id: i64,
    absolute_expiry: Option<i64>,
    pub(crate) payload: Vec<u8>,
    pub(crate) reliability: Reliability,
    pub(crate) topic: String,
}

pub struct RetentionMessageBuilder {
    id: i64,
    expiry: Option<i64>,
    payload: Vec<u8>,
    reliability: Reliability,
    topic: String,
}

impl RetentionMessageBuilder {
    pub(crate) fn new(payload: Vec<u8>, reliability: Reliability, topic: String) -> Self {
        RetentionMessageBuilder {
            id: 0,
            expiry: None,
            payload,
            reliability,
            topic,
        }
    }

    pub(crate) fn expiry(&mut self, expiry: i32) -> &mut Self {
        self.expiry = if expiry <= 0 {
            None
        } else {
            let now = Utc::now().timestamp();
            Some(now + expiry as i64)
        };

        self
    }

    pub(crate) fn id(&mut self, id: i64) -> &mut Self {
        self.id = id;

        self
    }

    pub(crate) fn build(&mut self) -> RetentionMessage {
        RetentionMessage {
            id: self.id,
            absolute_expiry: self.expiry.or(None),
            payload: self.payload.clone(),
            reliability: self.reliability,
            topic: self.topic.clone(),
        }
    }
}

impl RetentionMessage {
    pub(crate) fn is_expired(&self) -> bool {
        match self.absolute_expiry {
            None => false,
            Some(expiry) => Utc::now().timestamp() > expiry,
        }
    }
}

/// Trait providing compatibility with Astarte devices to databases.
///
/// Any database implementing this trait can be used as permanent storage for the retention message
/// of an Astarte device.
#[async_trait]
pub trait RetentionStore: Debug + Send + Sync + 'static
where
    // NOTE: the bounds are required to be compatible with the tokio tasks, with an additional Sync
    //       bound to further restrict the error type.
    Self::Err: StdError + Send + Sync + 'static,
{
    type Err;

    /// Removes all saved RetentionMessage from the store.
    async fn clear_retention_messages(&self) -> Result<(), Self::Err>;

    /// Provides the first RetentionMessage, or None if the store is empty.
    async fn front_retention_message(&self) -> Result<Option<RetentionMessage>, Self::Err>;

    /// Returns `true` if the store is empty.
    async fn is_empty_retention_message(&self) -> Result<bool, Self::Err>;

    /// Returns all stored RetentionMessage.
    async fn load_all_retention_messages(&self) -> Result<Vec<RetentionMessage>, Self::Err>;

    /// Stores a message within the store.
    async fn persist_retention_message(
        &self,
        retention_message: RetentionMessage,
    ) -> Result<(), Self::Err>;

    /// Removes the first RetentionMessage.
    async fn remove_front_retention_message(&self) -> Result<(), Self::Err>;

    /// Removes the RetentionMessage.
    async fn remove_retention_message(
        &self,
        retention_message: RetentionMessage,
    ) -> Result<(), Self::Err>;
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use crate::store::{memory::MemoryStore, wrapper::StoreWrapper};

    use super::*;

    pub(crate) async fn test_property_store<S>(store: S)
    where
        S: PropertyStore,
    {
        let ty = AstarteType::Integer(23);

        store.clear_props().await.unwrap();

        // non existing
        assert_eq!(store.load_prop("com.test", "/test", 1).await.unwrap(), None);

        let prop = StoredProp {
            interface: "com.test",
            path: "/test",
            value: &ty,
            interface_major: 1,
            ownership: Ownership::Device,
        };

        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );

        //major version mismatch
        assert_eq!(store.load_prop("com.test", "/test", 2).await.unwrap(), None);

        // after mismatch the path should be deleted
        assert_eq!(store.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // delete
        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );
        store.delete_prop("com.test", "/test").await.unwrap();
        assert_eq!(store.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // unset
        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );
        let unset = StoredProp {
            interface: "com.test",
            path: "/test",
            value: &AstarteType::Unset,
            interface_major: 1,
            ownership: Ownership::Device,
        };
        store.store_prop(unset).await.unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            AstarteType::Unset
        );

        // clear
        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store
                .load_prop("com.test", "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );
        store.clear_props().await.unwrap();
        assert_eq!(store.load_prop("com.test", "/test", 1).await.unwrap(), None);

        // load all props
        let device = StoredProp {
            interface: "com.test1".into(),
            path: "/test1".into(),
            value: ty.clone(),
            interface_major: 1,
            ownership: Ownership::Device,
        };
        let server = StoredProp {
            interface: "com.test2".into(),
            path: "/test2".into(),
            value: ty.clone(),
            interface_major: 1,
            ownership: Ownership::Server,
        };

        store.store_prop(device.as_ref()).await.unwrap();
        store.store_prop(server.as_ref()).await.unwrap();

        let expected = [device.clone(), server.clone()];

        let mut props = store.load_all_props().await.unwrap();

        props.sort_unstable_by(|a, b| a.interface.cmp(&b.interface));

        assert_eq!(props, expected);

        let dev_props = store.device_props().await.unwrap();
        assert_eq!(dev_props, [device.clone()]);

        let serv_props = store.server_props().await.unwrap();
        assert_eq!(serv_props, [server.clone()]);

        // props from interface
        let props = store.interface_props("com.test1").await.unwrap();
        assert_eq!(props, vec![device]);
        let props = store.interface_props("com.test2").await.unwrap();
        assert_eq!(props, vec![server]);

        // test all types
        let all_types = [
            AstarteType::Double(4.5),
            AstarteType::Integer(-4),
            AstarteType::Boolean(true),
            AstarteType::LongInteger(45543543534_i64),
            AstarteType::String("hello".into()),
            AstarteType::BinaryBlob(b"hello".to_vec()),
            AstarteType::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteType::DoubleArray(vec![1.2, 3.4, 5.6, 7.8]),
            AstarteType::IntegerArray(vec![1, 3, 5, 7]),
            AstarteType::BooleanArray(vec![true, false, true, true]),
            AstarteType::LongIntegerArray(vec![45543543534_i64, 45543543535_i64, 45543543536_i64]),
            AstarteType::StringArray(vec!["hello".to_owned(), "world".to_owned()]),
            AstarteType::BinaryBlobArray(vec![b"hello".to_vec(), b"world".to_vec()]),
            AstarteType::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]),
        ];

        for ty in all_types {
            let path = format!("/test/{}", ty.display_type());

            let prop = StoredProp {
                interface: "com.test",
                path: &path,
                value: &ty,
                interface_major: 1,
                ownership: Ownership::Server,
            };

            store.store_prop(prop).await.unwrap();

            let res = store.load_prop("com.test", &path, 1).await.unwrap();

            assert_eq!(res, Some(ty));
        }
    }

    /// Test that the error is Send + Sync + 'static to be send across task boundaries.
    #[tokio::test]
    async fn erro_should_compatible_with_tokio() {
        let mem = StoreWrapper::new(MemoryStore::new()).await;

        let exp = AstarteType::Integer(1);
        let prop = StoredProp {
            interface: "com.test",
            path: "/test",
            value: &exp,
            interface_major: 1,
            ownership: Ownership::Device,
        };
        mem.store_prop(prop).await.unwrap();

        let res = tokio::spawn(async move { mem.load_prop("com.test", "/test", 1).await })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(res, Some(exp));
    }

    pub(crate) async fn test_retention_store<S>(store: S)
    where
        S: RetentionStore,
    {
        let ty: Vec<u8> = vec![23];

        store.clear_retention_messages().await.unwrap();

        // non existing
        assert!(store.is_empty_retention_message().await.unwrap());

        let retention_message = RetentionMessage {
            id: 0,
            topic: "/device/interface/com.test".to_string(),
            payload: ty.clone(),
            reliability: Reliability::Guaranteed,
            absolute_expiry: None,
        };

        store
            .persist_retention_message(retention_message.clone())
            .await
            .unwrap();
        assert!(!store.is_empty_retention_message().await.unwrap());
        assert_eq!(
            store
                .front_retention_message()
                .await
                .unwrap()
                .unwrap()
                .payload,
            ty
        );

        // delete
        store.remove_front_retention_message().await.unwrap();
        assert!(store.is_empty_retention_message().await.unwrap());

        // clear
        store
            .persist_retention_message(retention_message)
            .await
            .unwrap();
        assert_eq!(
            store
                .front_retention_message()
                .await
                .unwrap()
                .unwrap()
                .payload,
            ty
        );
        store.clear_retention_messages().await.unwrap();
        assert!(store.is_empty_retention_message().await.unwrap());
    }
}
