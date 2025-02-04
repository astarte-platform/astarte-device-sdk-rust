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

use std::{error::Error as StdError, fmt::Debug, future::Future};

pub use self::sqlite::SqliteStore;
use crate::{
    interface::{
        reference::{MappingRef, PropertyRef},
        Ownership,
    },
    retention::StoredRetention,
    types::AstarteType,
    Interface,
};

pub mod error;
pub mod memory;
pub mod sqlite;
pub mod wrapper;

/// Inform what capabilities are implemented for a store.
///
/// This is a crutch until specialization is implemented in the std library, while still being
/// generic and accept external store implementations.
pub trait StoreCapabilities {
    /// Type used for the [`StoredRetention`].
    ///
    /// This should be self, it's used as an associated type to not introduce dynamic dispatch.
    type Retention: StoredRetention;

    /// Returns the retention if the store supports it.
    fn get_retention(&self) -> Option<&Self::Retention>;
}

/// Data passed to the store that identifies an interface
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterfaceInfo<S = String> {
    pub(crate) name: S,
    pub(crate) ownership: Ownership,
}

impl<S> InterfaceInfo<S> {
    fn new(name: S, ownership: Ownership) -> Self {
        Self { name, ownership }
    }

    pub(crate) fn owned_name(self) -> InterfaceInfo
    where
        S: ToString,
    {
        InterfaceInfo {
            name: self.name.to_string(),
            ownership: self.ownership,
        }
    }
}

/// Converts an interface object reference to the store needed input
impl<'a> From<&'a Interface> for InterfaceInfo<&'a str> {
    fn from(interface: &'a Interface) -> Self {
        Self::new(interface.interface_name(), interface.ownership())
    }
}

/// Converts a property ref object reference to the store needed input
impl<'a> From<&'a PropertyRef<'a>> for InterfaceInfo<&'a str> {
    fn from(prop_ref: &'a PropertyRef) -> Self {
        Self::new(prop_ref.0.interface_name(), prop_ref.0.ownership())
    }
}

/// Converts a stored prop reference to the store needed input
impl<'a, S, V> From<&'a StoredProp<S, V>> for InterfaceInfo<&'a str>
where
    S: AsRef<str>,
{
    fn from(stored_prop: &'a StoredProp<S, V>) -> Self {
        Self::new(stored_prop.interface.as_ref(), stored_prop.ownership)
    }
}

/// Trait providing compatibility with Astarte devices to databases.
///
/// Any database implementing this trait can be used as permanent storage for the properties
/// of an Astarte device.
///
/// This SDK provides an implementation of a sqlite database for which this trait has already
/// been implemented, see [`crate::store::sqlite::SqliteStore`].
pub trait PropertyStore: Clone + Debug + Send + Sync + 'static
where
    // NOTE: the bounds are required to be compatible with the tokio tasks, with an additional Sync
    //       bound to further restrict the error type.
    Self::Err: StdError + Send + Sync + 'static,
{
    /// Reason for a failed operation.
    type Err;

    /// Stores a property within the database.
    fn store_prop(
        &self,
        prop: StoredProp<&str, &AstarteType>,
    ) -> impl Future<Output = Result<(), Self::Err>> + Send;
    /// Load a property from the database.
    ///
    /// The property store should delete the property from the database if the major version of the
    /// interface does not match the one provided.
    fn load_prop<I>(
        &self,
        interface: &InterfaceInfo<I>,
        path: &str,
        interface_major: i32,
    ) -> impl Future<Output = Result<Option<AstarteType>, Self::Err>> + Send
    where
        I: AsRef<str> + Send + Sync;
    /// Unset a property from the database.
    fn unset_prop<I>(
        &self,
        interface: &InterfaceInfo<I>,
        path: &str,
    ) -> impl Future<Output = Result<(), Self::Err>> + Send
    where
        I: AsRef<str> + Send + Sync;
    /// Delete a property from the database.
    fn delete_prop<I>(
        &self,
        interface: &InterfaceInfo<I>,
        path: &str,
    ) -> impl Future<Output = Result<(), Self::Err>> + Send
    where
        I: AsRef<str> + Send + Sync;
    /// Removes all saved properties from the database.
    fn clear(&self) -> impl Future<Output = Result<(), Self::Err>> + Send;
    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    fn load_all_props(&self) -> impl Future<Output = Result<Vec<StoredProp>, Self::Err>> + Send;
    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    fn device_props(&self) -> impl Future<Output = Result<Vec<StoredProp>, Self::Err>> + Send;
    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    fn server_props(&self) -> impl Future<Output = Result<Vec<StoredProp>, Self::Err>> + Send;
    /// Retrieves all the property values of a specific interface in the database.
    fn interface_props<I>(
        &self,
        interface: &InterfaceInfo<I>,
    ) -> impl Future<Output = Result<Vec<StoredProp>, Self::Err>> + Send
    where
        I: AsRef<str> + Send + Sync;
    /// Deletes all the properties of the interface from the database.
    fn delete_interface<I>(
        &self,
        interface: &InterfaceInfo<I>,
    ) -> impl Future<Output = Result<(), Self::Err>> + Send
    where
        I: AsRef<str> + Send + Sync;
    /// Retrieves all the device properties, including the one that were unset but not deleted.
    fn device_props_with_unset(
        &self,
    ) -> impl Future<Output = Result<Vec<OptStoredProp>, Self::Err>> + Send;
}

/// Data structure used to return stored properties by a database implementing the [`PropertyStore`]
/// trait.
#[derive(Debug, Clone, Copy, PartialOrd)]
pub struct StoredProp<S = String, V = AstarteType> {
    /// Interface name of the property.
    pub interface: S,
    /// Path of the property's mapping.
    pub path: S,
    /// Value of the property.
    pub value: V,
    /// Major version of the interface.
    ///
    /// This is important to check if a stored property is compatible with the current interface
    /// version.
    pub interface_major: i32,
    /// Ownership of the property.
    ///
    /// If it's [`Ownership::Device`] the property was sent from the device to Astarte. Instead, if
    /// it's [`Ownership::Server`] it was received from Astarte.
    pub ownership: Ownership,
}

/// A property that may be unset.
///
/// This is returned by getting all the properties (`load_all_props`) that have not been deleted
/// yet, since they where not sent to Astarte.
pub type OptStoredProp = StoredProp<String, Option<AstarteType>>;

impl StoredProp {
    /// Coverts the stored property into a reference to its values.
    pub fn as_ref(&self) -> StoredProp<&str, &AstarteType> {
        self.into()
    }
}

impl<'a> StoredProp<&'a str, &'a AstarteType> {
    /// Create a new with the given [`Interface`], path and value.
    pub(crate) fn from_mapping(
        mapping: &'a MappingRef<'a, PropertyRef>,
        value: &'a AstarteType,
    ) -> Self {
        Self {
            interface: mapping.interface().interface_name(),
            path: mapping.path().as_str(),
            value,
            interface_major: mapping.interface().version_major(),
            ownership: mapping.interface().ownership(),
        }
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

impl<S2, S1, T2, T1> PartialEq<StoredProp<S2, T2>> for StoredProp<S1, T1>
where
    S1: PartialEq<S2>,
    T1: PartialEq<T2>,
{
    fn eq(&self, other: &StoredProp<S2, T2>) -> bool {
        self.interface == other.interface
            && self.path == other.path
            && self.value == other.value
            && self.interface_major == other.interface_major
            && self.ownership == other.ownership
    }
}

#[cfg(test)]
mod tests {
    use crate::store::{memory::MemoryStore, wrapper::StoreWrapper};

    use chrono::{TimeZone, Utc};

    use super::*;

    pub(crate) async fn test_property_store<S>(store: S)
    where
        S: PropertyStore,
    {
        let ty = AstarteType::Integer(23);
        let prop = StoredProp {
            interface: "com.test",
            path: "/test",
            value: &ty,
            interface_major: 1,
            ownership: Ownership::Device,
        };
        let store_data = (&prop).into();

        store.clear().await.unwrap();

        // non existing
        assert_eq!(
            store.load_prop(&store_data, "/test", 1).await.unwrap(),
            None
        );

        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store
                .load_prop(&store_data, "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );

        //major version mismatch
        assert_eq!(
            store.load_prop(&store_data, "/test", 2).await.unwrap(),
            None
        );

        // after mismatch the path should be deleted
        assert_eq!(
            store.load_prop(&store_data, "/test", 1).await.unwrap(),
            None
        );

        // unset
        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store
                .load_prop(&store_data, "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );
        store.unset_prop(&store_data, "/test").await.unwrap();
        assert_eq!(
            store.load_prop(&store_data, "/test", 1).await.unwrap(),
            None
        );
        // with unset
        assert!(store.device_props().await.unwrap().is_empty());
        assert!(store.load_all_props().await.unwrap().is_empty());
        assert!(store.server_props().await.unwrap().is_empty());
        assert_eq!(
            &[StoredProp {
                interface: "com.test",
                path: "/test",
                value: None,
                interface_major: 1,
                ownership: Ownership::Device,
            }],
            store.device_props_with_unset().await.unwrap().as_slice()
        );

        // delete
        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store
                .load_prop(&store_data, "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );
        store.delete_prop(&store_data, "/test").await.unwrap();
        assert_eq!(
            store.load_prop(&store_data, "/test", 1).await.unwrap(),
            None
        );

        // clear
        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store
                .load_prop(&store_data, "/test", 1)
                .await
                .unwrap()
                .unwrap(),
            ty
        );
        store.clear().await.unwrap();
        assert_eq!(
            store.load_prop(&store_data, "/test", 1).await.unwrap(),
            None
        );

        // load all props
        let device = StoredProp {
            interface: "com.test1".into(),
            path: "/test1".into(),
            value: ty.clone(),
            interface_major: 1,
            ownership: Ownership::Device,
        };
        let device_interface_data = (Into::<InterfaceInfo<&'_ str>>::into(&device)).owned_name();
        let server = StoredProp {
            interface: "com.test2".into(),
            path: "/test2".into(),
            value: ty.clone(),
            interface_major: 1,
            ownership: Ownership::Server,
        };
        let server_interface_data = (&server).into();

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
        let props = store.interface_props(&device_interface_data).await.unwrap();
        assert_eq!(props, vec![device]);
        let props = store.interface_props(&server_interface_data).await.unwrap();
        assert_eq!(props, vec![server]);

        // delete interface properties
        store
            .delete_interface(&device_interface_data)
            .await
            .unwrap();
        let prop = store.interface_props(&device_interface_data).await.unwrap();

        assert!(prop.is_empty());

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
            let prop_interface_data = (&prop).into();

            store.store_prop(prop).await.unwrap();

            let res = store
                .load_prop(&prop_interface_data, &path, 1)
                .await
                .unwrap();

            assert_eq!(res, Some(ty));
        }
    }

    /// Test that the error is Send + Sync + 'static to be send across task boundaries.
    #[tokio::test]
    async fn error_should_compatible_with_tokio() {
        let mem = StoreWrapper::new(MemoryStore::new());

        let exp = AstarteType::Integer(1);
        let prop = StoredProp {
            interface: "com.test",
            path: "/test",
            value: &exp,
            interface_major: 1,
            ownership: Ownership::Device,
        };
        let prop_interface_data = Into::<InterfaceInfo<&'_ str>>::into(&prop).owned_name();
        mem.store_prop(prop).await.unwrap();

        let res =
            tokio::spawn(async move { mem.load_prop(&prop_interface_data, "/test", 1).await })
                .await
                .unwrap()
                .unwrap();

        assert_eq!(res, Some(exp));
    }
}
