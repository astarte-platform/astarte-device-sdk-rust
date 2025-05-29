// This file is part of Astarte.
//
// Copyright 2021 - 2025 SECO Mind Srl
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

use std::{collections::HashSet, error::Error as StdError, fmt::Debug, future::Future};

use astarte_interfaces::schema::Ownership;
use astarte_interfaces::{Properties, Schema};

pub use self::sqlite::SqliteStore;
use crate::interfaces::MappingRef;
use crate::retention::{Id, PublishInfo, RetentionError, StoredInterface};
use crate::session::{IntrospectionInterface, SessionError, StoredSession};
use crate::{retention::StoredRetention, types::AstarteType};

pub mod error;
pub mod memory;
#[cfg(test)]
pub(crate) mod mock;
pub mod sqlite;
pub mod wrapper;

/// Inform what capabilities are implemented for a store.
///
/// It requires the store to implement [`PropertyStore`] since it's a required features.
///
/// This is a crutch until specialization is implemented in the std library, while still being
/// generic and accept external store implementations.
pub trait StoreCapabilities: PropertyStore {
    /// Type used for the [`StoredRetention`].
    ///
    /// This should be self, it's used as an associated type to not introduce dynamic dispatch.
    type Retention: StoredRetention;
    /// Type used for the [`StoredSession`].
    ///
    /// This should be self, it's used as an associated type to not introduce dynamic dispatch.
    type Session: StoredSession;

    /// Returns the retention if the store supports it.
    fn get_retention(&self) -> Option<&Self::Retention>;

    /// Returns the introspection store if supported.
    fn get_session(&self) -> Option<&Self::Session>;
}

/// Un-constructable type for a default capability.
///
/// This should be the never type [`!`] in the future.
/// Useful for types which do not have a capability but must implement [`StoreCapabilities`]
#[derive(Clone, Copy)]
pub enum MissingCapability {}

#[cfg_attr(__coverage, coverage(off))]
impl StoredRetention for MissingCapability {
    async fn store_publish(
        &self,
        _id: &Id,
        _publish: PublishInfo<'_>,
    ) -> Result<(), RetentionError> {
        unreachable!("the type is Un-constructable");
    }

    async fn update_sent_flag(&self, _id: &Id, _sent: bool) -> Result<(), RetentionError> {
        unreachable!("the type is Un-constructable");
    }

    async fn mark_received(&self, _packet: &Id) -> Result<(), RetentionError> {
        unreachable!("the type is Un-constructable");
    }

    async fn delete_publish(&self, _id: &Id) -> Result<(), RetentionError> {
        unreachable!("the type is Un-constructable");
    }

    async fn delete_interface(&self, _interface: &str) -> Result<(), RetentionError> {
        unreachable!("the type is Un-constructable");
    }

    async fn unsent_publishes(
        &self,
        _limit: usize,
        _buf: &mut Vec<(Id, PublishInfo<'static>)>,
    ) -> Result<usize, RetentionError> {
        unreachable!("the type is Un-constructable");
    }

    async fn reset_all_publishes(&self) -> Result<(), RetentionError> {
        unreachable!("the type is Un-constructable");
    }

    async fn fetch_all_interfaces(&self) -> Result<HashSet<StoredInterface>, RetentionError> {
        unreachable!("the type is Un-constructable");
    }
}

#[cfg_attr(__coverage, coverage(off))]
impl StoredSession for MissingCapability {
    async fn add_interfaces(
        &self,
        _interfaces: &[IntrospectionInterface<&str>],
    ) -> Result<(), SessionError> {
        unreachable!("the type is un-constructable");
    }

    async fn load_introspection(&self) -> Result<Vec<IntrospectionInterface>, SessionError> {
        unreachable!("the type is un-constructable");
    }

    async fn store_introspection(&self, _interfaces: &[IntrospectionInterface]) {
        unreachable!("the type is un-constructable");
    }

    async fn clear_introspection(&self) {
        unreachable!("the type is un-constructable");
    }

    async fn remove_interfaces(
        &self,
        _interfaces: &[IntrospectionInterface<&str>],
    ) -> Result<(), SessionError> {
        unreachable!("the type is un-constructable");
    }
}

/// Data passed to the store that identifies a property
// NOTE: this is needed to get the property mapping from a stored property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertyMapping<'a> {
    /// Interface name for the mapping.
    interface_name: &'a str,
    /// Interface major version.
    version_major: i32,
    /// Ownership of the property.
    ownership: Ownership,
    /// Path of the property.
    path: &'a str,
}

impl PropertyMapping<'_> {
    /// Returns the name of the property interface.
    pub fn interface_name(&self) -> &str {
        self.interface_name
    }

    /// Returns the major version of the property interface.
    pub fn version_major(&self) -> i32 {
        self.version_major
    }

    /// Returns the [`Ownership`] of the property interface.
    pub fn ownership(&self) -> Ownership {
        self.ownership
    }

    /// Returns the path of the property data.
    pub fn path(&self) -> &str {
        self.path
    }
}
impl<'a> From<&'a MappingRef<'a, Properties>> for PropertyMapping<'a> {
    fn from(value: &'a MappingRef<'a, Properties>) -> Self {
        let interface = value.interface();

        Self {
            interface_name: interface.interface_name().as_str(),
            version_major: interface.version_major(),
            ownership: interface.ownership(),
            path: value.path().as_str(),
        }
    }
}

impl<'a, S, V> From<&'a StoredProp<S, V>> for PropertyMapping<'a>
where
    S: AsRef<str>,
{
    fn from(value: &'a StoredProp<S, V>) -> Self {
        Self {
            interface_name: value.interface.as_ref(),
            version_major: value.interface_major,
            ownership: value.ownership,
            path: value.path.as_ref(),
        }
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
    fn load_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> impl Future<Output = Result<Option<AstarteType>, Self::Err>> + Send;
    /// Unset a property from the database.
    fn unset_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> impl Future<Output = Result<(), Self::Err>> + Send;
    /// Delete a property from the database.
    fn delete_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> impl Future<Output = Result<(), Self::Err>> + Send;
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
    fn interface_props(
        &self,
        interface: &Properties,
    ) -> impl Future<Output = Result<Vec<StoredProp>, Self::Err>> + Send;
    /// Deletes all the properties of the interface from the database.
    fn delete_interface(
        &self,
        interface: &Properties,
    ) -> impl Future<Output = Result<(), Self::Err>> + Send;
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
        mapping: &'a MappingRef<'a, Properties>,
        value: &'a AstarteType,
    ) -> Self {
        Self {
            interface: mapping.interface().interface_name().as_str(),
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
    use std::str::FromStr;

    use crate::store::{memory::MemoryStore, wrapper::StoreWrapper};
    use crate::test::{
        E2E_DEVICE_PROPERTY, E2E_DEVICE_PROPERTY_NAME, E2E_SERVER_PROPERTY,
        E2E_SERVER_PROPERTY_NAME,
    };

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
        let property_mapping = PropertyMapping::from(&prop);

        store.clear().await.unwrap();

        // non existing
        assert_eq!(store.load_prop(&property_mapping).await.unwrap(), None);

        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store.load_prop(&property_mapping).await.unwrap().unwrap(),
            ty
        );

        let mut property_mapping_next = property_mapping;
        property_mapping_next.version_major = 2;

        //major version mismatch
        assert_eq!(store.load_prop(&property_mapping_next).await.unwrap(), None);

        // after mismatch the path should be deleted
        assert_eq!(store.load_prop(&property_mapping).await.unwrap(), None);

        // unset
        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store.load_prop(&property_mapping).await.unwrap().unwrap(),
            ty
        );
        store.unset_prop(&property_mapping).await.unwrap();
        assert_eq!(store.load_prop(&property_mapping).await.unwrap(), None);
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
            store.load_prop(&property_mapping).await.unwrap().unwrap(),
            ty
        );
        store.delete_prop(&property_mapping).await.unwrap();
        assert_eq!(store.load_prop(&property_mapping).await.unwrap(), None);

        // clear
        store.store_prop(prop).await.unwrap();
        assert_eq!(
            store.load_prop(&property_mapping).await.unwrap().unwrap(),
            ty
        );
        store.clear().await.unwrap();
        assert_eq!(store.load_prop(&property_mapping).await.unwrap(), None);

        // load all props
        let device_prop_interface = Properties::from_str(E2E_DEVICE_PROPERTY).unwrap();
        let device_prop = StoredProp {
            interface: E2E_DEVICE_PROPERTY_NAME.to_string(),
            path: "/sensor2/integer_endpoint".to_string(),
            value: ty.clone(),
            interface_major: 1,
            ownership: Ownership::Device,
        };
        let server_prop_interface = Properties::from_str(E2E_SERVER_PROPERTY).unwrap();
        let server_prop = StoredProp {
            interface: E2E_SERVER_PROPERTY_NAME.to_string(),
            path: "/sensor2/integer_endpoint".to_string(),
            value: ty.clone(),
            interface_major: 1,
            ownership: Ownership::Server,
        };

        store.store_prop(device_prop.as_ref()).await.unwrap();
        store.store_prop(server_prop.as_ref()).await.unwrap();

        let expected = [device_prop.clone(), server_prop.clone()];

        let mut props = store.load_all_props().await.unwrap();

        props.sort_unstable_by(|a, b| a.interface.cmp(&b.interface));

        assert_eq!(props, expected);

        let dev_props = store.device_props().await.unwrap();
        assert_eq!(dev_props, [device_prop.clone()]);

        let serv_props = store.server_props().await.unwrap();
        assert_eq!(serv_props, [server_prop.clone()]);

        // props from interface
        let props = store.interface_props(&device_prop_interface).await.unwrap();
        assert_eq!(props, vec![device_prop.clone()]);
        let props = store.interface_props(&server_prop_interface).await.unwrap();
        assert_eq!(props, vec![server_prop.clone()]);

        // delete interface properties
        store
            .delete_interface(&device_prop_interface)
            .await
            .unwrap();
        let prop = store.interface_props(&device_prop_interface).await.unwrap();
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
            let prop_mapping = PropertyMapping::from(&prop);

            store.store_prop(prop).await.unwrap();

            let res = store.load_prop(&prop_mapping).await.unwrap();

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
        mem.store_prop(prop).await.unwrap();

        let exp2 = exp.clone();
        let res = tokio::spawn(async move {
            let prop = StoredProp {
                interface: "com.test",
                path: "/test",
                value: &exp2,
                interface_major: 1,
                ownership: Ownership::Device,
            };
            let prop_interface_data = PropertyMapping::from(&prop);
            mem.load_prop(&prop_interface_data).await
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(res, Some(exp));
    }
}
