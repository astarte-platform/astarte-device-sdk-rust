// This file is part of Astarte.
//
// Copyright 2021-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Provides functionality for instantiating an Astarte sqlite database.

use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::num::{NonZero, NonZeroUsize};

use astarte_device_error::Error;
use astarte_interfaces::schema::Ownership;
use astarte_interfaces::{Properties, Schema};
use chrono::DateTime;
use chrono::Utc;

use self::error::StoreError;
pub use self::sqlite::SqliteStore;
use crate::interfaces::MappingRef;
use crate::retention::StoredRetention;
use crate::retention::{Id, PublishInfo, RetentionError, StoredInterface};
use crate::session::{IntrospectionInterface, SessionError, StoredSession};
use crate::types::AstarteData;

pub mod error;
pub mod memory;
#[cfg(test)]
pub(crate) mod mock;
pub mod sqlite;

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

/// Trait providing compatibility with Astarte devices to databases.
///
/// Any database implementing this trait can be used as permanent storage for the properties
/// of an Astarte device.
///
/// This SDK provides an implementation of a sqlite database for which this trait has already
/// been implemented, see [`crate::store::sqlite::SqliteStore`].
pub trait PropertyStore: Send + Sync + 'static {
    /// Stores a property within the database.
    ///
    /// The property should not be updated if the value is the same.
    fn store_prop(
        &self,
        prop: Prop,
    ) -> impl Future<Output = Result<PropMetadata, Error<StoreError>>> + Send;

    /// Unset a property from the database.
    fn unset_prop(
        &self,
        property: &PropertyMapping<'_>,
        updated_at: UpdatedAt,
    ) -> impl Future<Output = Result<PropMetadata, Error<StoreError>>> + Send;

    /// Load a property from the database.
    ///
    /// The property store should delete the property from the database if the major version of the
    /// interface does not match the one provided.
    fn load_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> impl Future<Output = Result<Option<StoredProp>, Error<StoreError>>> + Send;

    /// Update state flag of a property only if the value matches the expected one
    fn update_state(
        &self,
        interface_name: &str,
        path: &str,
        state: PropertyState,
        epoch: u8,
    ) -> impl Future<Output = Result<bool, Error<StoreError>>> + Send;

    /// Delete a property from the database.
    fn delete_server_prop(
        &self,
        interface_name: &str,
        path: &str,
    ) -> impl Future<Output = Result<bool, Error<StoreError>>> + Send;

    /// Delete a property from the database.
    ///
    /// It will delete the properties only when the epoch is matching to prevent ABA problems when
    /// unsetting.
    fn delete_device_prop(
        &self,
        interface_name: &str,
        path: &str,
        epoch: u8,
    ) -> impl Future<Output = Result<bool, Error<StoreError>>> + Send;

    /// Removes all saved properties from the database.
    fn clear(&self) -> impl Future<Output = Result<(), Error<StoreError>>> + Send;

    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    fn load_all_props(
        &self,
        limit: NonZero<usize>,
        last_updated_at: Option<UpdatedAt>,
    ) -> impl Future<Output = Result<Vec<StoredProp>, Error<StoreError>>> + Send;

    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    fn device_props(
        &self,
        limit: NonZero<usize>,
        last_updated_at: Option<UpdatedAt>,
    ) -> impl Future<Output = Result<Vec<StoredProp>, Error<StoreError>>> + Send;

    /// Retrieves all property values in the database, together with their interface name, path
    /// and major version.
    fn server_props(
        &self,
        limit: NonZero<usize>,
        last_updated_at: Option<UpdatedAt>,
    ) -> impl Future<Output = Result<Vec<StoredProp>, Error<StoreError>>> + Send;

    /// Retrieves all the property values of a specific interface in the database.
    fn interface_props(
        &self,
        interface: &Properties,
        limit: NonZero<usize>,
        last_updated_at: Option<UpdatedAt>,
    ) -> impl Future<Output = Result<Vec<StoredProp>, Error<StoreError>>> + Send;

    /// Deletes all the properties of the interface from the database.
    fn delete_interface(
        &self,
        interface: &Properties,
    ) -> impl Future<Output = Result<(), Error<StoreError>>> + Send;

    /// Retrieves all the device properties, including the one that were unset but not deleted.
    fn device_props_with_unset(
        &self,
        state: PropertyState,
        limit: NonZero<usize>,
        last_updated_at: Option<UpdatedAt>,
    ) -> impl Future<Output = Result<Vec<OptStoredProp>, Error<StoreError>>> + Send;

    /// Resets the state of properties
    ///
    /// Deletes all the unset and reset epoch and state.
    fn reset_session(&self) -> impl Future<Output = Result<(), Error<StoreError>>> + Send;
}

/// Timestamp with counter for the updated at
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UpdatedAt {
    timestamp: DateTime<Utc>,
    counter: u32,
}

impl UpdatedAt {
    /// Creates a new updated at
    pub fn new(timestamp: DateTime<Utc>, counter: u32) -> Self {
        Self { timestamp, counter }
    }

    /// Gets the timestamp
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Gets the counter
    pub fn counter(&self) -> u32 {
        self.counter
    }
}

impl Display for UpdatedAt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}]", self.timestamp, self.counter)
    }
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
    ) -> Result<(), Error<RetentionError>> {
        unreachable!("the type is Un-constructable");
    }

    async fn update_sent_flag(&self, _id: &Id, _sent: bool) -> Result<(), Error<RetentionError>> {
        unreachable!("the type is Un-constructable");
    }

    async fn mark_received(&self, _packet: &Id) -> Result<(), Error<RetentionError>> {
        unreachable!("the type is Un-constructable");
    }

    async fn delete_interface(&self, _interface: &str) -> Result<(), Error<RetentionError>> {
        unreachable!("the type is Un-constructable");
    }

    async fn unsent_publishes(
        &self,
        _limit: usize,
        _buf: &mut Vec<(Id, PublishInfo<'static>)>,
    ) -> Result<usize, Error<RetentionError>> {
        unreachable!("the type is Un-constructable");
    }

    async fn reset_all_publishes(&self) -> Result<(), Error<RetentionError>> {
        unreachable!("the type is Un-constructable");
    }

    async fn fetch_all_interfaces(
        &self,
    ) -> Result<HashSet<StoredInterface>, Error<RetentionError>> {
        unreachable!("the type is Un-constructable");
    }

    async fn set_max_retention_items(
        &self,
        _size: NonZeroUsize,
    ) -> Result<(), Error<RetentionError>> {
        unreachable!("the type is Un-constructable");
    }
}

#[cfg_attr(__coverage, coverage(off))]
impl StoredSession for MissingCapability {
    async fn add_interfaces(
        &self,
        _interfaces: &[IntrospectionInterface<&str>],
    ) -> Result<(), Error<SessionError>> {
        unreachable!("the type is un-constructable");
    }

    async fn load_introspection(&self) -> Result<Vec<IntrospectionInterface>, Error<SessionError>> {
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
    ) -> Result<(), Error<SessionError>> {
        unreachable!("the type is un-constructable");
    }
}

/// Data passed to the store that identifies a property
// NOTE: this is needed to get the property mapping from a stored property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertyMapping<'a> {
    /// Interface name for the mapping.
    pub(crate) interface_name: &'a str,
    /// Interface major version.
    pub(crate) version_major: i32,
    /// Ownership of the property.
    pub(crate) ownership: Ownership,
    /// Path of the property.
    pub(crate) path: &'a str,
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

/// Metadata returned after storing a property
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct PropMetadata {
    /// Present when the property was updated
    pub(crate) epoch: Option<u8>,
}

impl PropMetadata {
    /// Present when the property was updated
    pub fn epoch(&self) -> Option<u8> {
        self.epoch
    }

    /// Sets the epoch
    pub fn set_epoch(&mut self, epoch: Option<u8>) {
        self.epoch = epoch;
    }
}

/// Optionally unset [`StoredProp`].
pub type OptStoredProp = StoredProp<Option<AstarteData>>;

/// A property returned by the stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredProp<V = AstarteData> {
    /// Interface name of the property.
    pub interface: String,
    /// Path of the property's mapping.
    pub path: String,
    /// Value of the property.
    pub value: V,
    /// Major version of the interface.
    ///
    /// This is important to check if a stored property is compatible with the current interface
    /// version.
    interface_major: i32,
    /// Ownership of the property.
    ///
    /// If it's [`Ownership::Device`] the property was sent from the device to Astarte. Instead, if
    /// it's [`Ownership::Server`] it was received from Astarte.
    ownership: Ownership,
    /// Revision of the property.
    ///
    /// Returned when the property is stored or unset.
    epoch: u8,
    /// Time stamp of the last time the property was updated.
    updated_at: UpdatedAt,
}

impl<V> StoredProp<V> {
    /// Create a stored server property
    pub fn from_server(
        interface: String,
        path: String,
        value: V,
        interface_major: i32,
        ownership: Ownership,
        updated_at: UpdatedAt,
    ) -> Self {
        Self {
            interface,
            path,
            value,
            interface_major,
            ownership,
            epoch: 0,
            updated_at,
        }
    }

    /// Create a stored device property
    pub fn from_device(
        interface: String,
        path: String,
        value: V,
        interface_major: i32,
        ownership: Ownership,
        epoch: u8,
        updated_at: UpdatedAt,
    ) -> Self {
        Self {
            interface,
            path,
            value,
            interface_major,
            ownership,
            epoch,
            updated_at,
        }
    }

    /// Revision of the property.
    ///
    /// Returned when the property is stored or unset.
    pub fn epoch(&self) -> u8 {
        self.epoch
    }

    /// Time stamp of the last time the property was updated.
    pub fn updated_at(&self) -> UpdatedAt {
        self.updated_at
    }

    /// Major version of the interface.
    ///
    /// This is important to check if a stored property is compatible with the current interface
    /// version.
    pub fn interface_major(&self) -> i32 {
        self.interface_major
    }

    /// Ownership of the property.
    ///
    /// If it's [`Ownership::Device`] the property was sent from the device to Astarte. Instead, if
    /// it's [`Ownership::Server`] it was received from Astarte.
    pub fn ownership(&self) -> Ownership {
        self.ownership
    }
}

/// Specifies the state of the property stored in the database.
/// When a property reaches the [`PropertyState::Completed`] state the property was
/// sent using the client transport.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub enum PropertyState {
    /// Property changed but not sent
    #[default]
    Changed,
    /// Property updated
    Completed,
}

/// Data structure used to store properties
///
/// Used by a database implementing the [`PropertyStore`] trait.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Prop {
    /// Interface name of the property.
    pub interface: String,
    /// Path of the property's mapping.
    pub path: String,
    /// Value of the property.
    pub value: AstarteData,
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
    /// Timestamp of when the property was updated
    pub updated_at: UpdatedAt,
}

impl Prop {
    /// Create a new with the given [`Interface`], path and value.
    pub(crate) fn from_mapping(
        mapping: &MappingRef<'_, Properties>,
        value: AstarteData,
        updated_at: UpdatedAt,
    ) -> Self {
        Self {
            interface: mapping.interface().interface_name().to_string(),
            path: mapping.path().to_string(),
            value,
            interface_major: mapping.interface().version_major(),
            ownership: mapping.interface().ownership(),
            updated_at,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;

    use crate::state::Context;
    use crate::test::{
        E2E_DEVICE_PROPERTY, E2E_DEVICE_PROPERTY_NAME, E2E_SERVER_PROPERTY,
        E2E_SERVER_PROPERTY_NAME,
    };

    use astarte_interfaces::Properties;
    use chrono::{TimeZone, Utc};
    use pretty_assertions::assert_eq;

    use super::*;

    const LIMIT: NonZero<usize> = NonZero::new(50).unwrap();
    const ONE: NonZero<usize> = NonZero::new(1).unwrap();

    pub(crate) fn prop_to_stored_prop(prop: &Prop, epoch: Option<u8>) -> StoredProp {
        StoredProp {
            interface: prop.interface.clone(),
            path: prop.path.clone(),
            value: prop.value.clone(),
            interface_major: prop.interface_major,
            ownership: prop.ownership,
            epoch: epoch.unwrap_or(0),
            updated_at: prop.updated_at,
        }
    }

    pub(crate) async fn test_property_store<S>(store: S)
    where
        S: PropertyStore,
    {
        let ctx = Context::new();

        let ty = AstarteData::Integer(23);

        let device_interface = E2E_DEVICE_PROPERTY_NAME;
        let device_path = "/123/integer_endpoint";
        let device_prop = Prop {
            interface: device_interface.to_string(),
            path: device_path.to_string(),
            value: ty.clone(),
            interface_major: 0,
            ownership: Ownership::Device,
            updated_at: ctx.next_updated_at(),
        };
        let device_mapping = PropertyMapping {
            interface_name: &device_prop.interface,
            version_major: device_prop.interface_major,
            ownership: device_prop.ownership,
            path: &device_prop.path,
        };
        let device_properties = Properties::from_str(E2E_DEVICE_PROPERTY).unwrap();

        let server_interface = E2E_SERVER_PROPERTY_NAME;
        let server_path = "/456/integer_endpoint";
        let server_prop = Prop {
            interface: server_interface.to_string(),
            path: server_path.to_string(),
            value: ty.clone(),
            interface_major: 0,
            ownership: Ownership::Server,
            updated_at: ctx.next_updated_at(),
        };
        let server_mapping = PropertyMapping {
            interface_name: &server_prop.interface,
            version_major: server_prop.interface_major,
            ownership: server_prop.ownership,
            path: &server_prop.path,
        };
        let server_properties = Properties::from_str(E2E_SERVER_PROPERTY).unwrap();

        // First clear the db
        store.clear().await.unwrap();

        // non existing
        check_load_non_existing(&store, device_mapping).await;

        check_store_and_load(&store, &device_prop, device_mapping).await;

        check_update_state(&store, &device_prop, device_mapping).await;

        check_load_different_version(&store, &device_prop, device_mapping).await;

        check_unset_and_load(&store, &ctx, &device_prop, device_mapping).await;

        check_fetch_multiple_props(
            &store,
            &ctx,
            &device_prop,
            device_mapping,
            &server_prop,
            server_mapping,
        )
        .await;

        check_with_interface(
            &store,
            &device_prop,
            &device_properties,
            &server_prop,
            &server_properties,
        )
        .await;

        check_delete_prop(
            &store,
            &device_prop,
            device_mapping,
            &device_properties,
            &server_prop,
            server_mapping,
            &server_properties,
        )
        .await;

        check_load_and_reset_state(&store, &device_prop).await;

        // test all types
        check_for_all_types(&store, ctx).await;
    }

    async fn check_load_non_existing<S>(store: &S, device_property_mapping: PropertyMapping<'_>)
    where
        S: PropertyStore,
    {
        let res = store.load_prop(&device_property_mapping).await.unwrap();

        assert_eq!(res, None);
    }

    async fn check_store_and_load<S>(
        store: &S,
        device_prop: &Prop,
        device_property_mapping: PropertyMapping<'_>,
    ) where
        S: PropertyStore,
    {
        let meta = store.store_prop(device_prop.clone()).await.unwrap();
        assert_eq!(meta, PropMetadata { epoch: Some(0) });

        // Check the stored prop
        let res = store
            .load_prop(&device_property_mapping)
            .await
            .unwrap()
            .unwrap();

        let exp = prop_to_stored_prop(device_prop, meta.epoch());

        assert_eq!(res, exp);

        // Same prop, no store
        let meta = store.store_prop(device_prop.clone()).await.unwrap();

        assert_eq!(meta, PropMetadata { epoch: None });

        let res = store
            .load_prop(&device_property_mapping)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(res, exp);
    }

    async fn check_update_state<S>(
        store: &S,
        device_prop: &Prop,
        device_mapping: PropertyMapping<'_>,
    ) where
        S: PropertyStore,
    {
        store.clear().await.unwrap();

        let meta = store.store_prop(device_prop.clone()).await.unwrap();
        assert_eq!(meta, PropMetadata { epoch: Some(0) });

        let res = store
            .device_props_with_unset(PropertyState::Changed, LIMIT, None)
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        let res = &res[0];
        assert_eq!(res.interface, device_prop.interface);
        assert_eq!(res.path, device_prop.path);

        let updated = store
            .update_state(
                device_mapping.interface_name,
                device_mapping.path,
                PropertyState::Completed,
                res.epoch(),
            )
            .await
            .unwrap();

        assert!(updated);

        let res = store
            .device_props_with_unset(PropertyState::Completed, LIMIT, None)
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        let res = &res[0];
        assert_eq!(res.interface, device_prop.interface);
        assert_eq!(res.path, device_prop.path);

        let updated = store
            .update_state(
                device_mapping.interface_name,
                device_mapping.path,
                PropertyState::Changed,
                res.epoch(),
            )
            .await
            .unwrap();

        assert!(updated);

        let res = store
            .device_props_with_unset(PropertyState::Completed, LIMIT, None)
            .await
            .unwrap();
        assert!(res.is_empty());
    }

    async fn check_load_different_version<S>(
        store: &S,
        device_prop: &Prop,
        device_property_mapping: PropertyMapping<'_>,
    ) where
        S: PropertyStore,
    {
        store.clear().await.unwrap();

        let meta = store.store_prop(device_prop.clone()).await.unwrap();
        assert_eq!(meta, PropMetadata { epoch: Some(0) });

        let mut property_mapping_next = device_property_mapping;
        property_mapping_next.version_major = 2;

        //major version mismatch
        let next_vers = store.load_prop(&property_mapping_next).await.unwrap();
        assert_eq!(next_vers, None);

        // after mismatch the path should be deleted
        let prev_vers = store.load_prop(&device_property_mapping).await.unwrap();
        assert_eq!(prev_vers, None);
    }

    async fn check_unset_and_load<S>(
        store: &S,
        ctx: &Context,
        device_prop: &Prop,
        device_property_mapping: PropertyMapping<'_>,
    ) where
        S: PropertyStore,
    {
        store.clear().await.unwrap();

        let meta = store.store_prop(device_prop.clone()).await.unwrap();
        assert_eq!(meta, PropMetadata { epoch: Some(0) });

        let unset_updated_at = ctx.next_updated_at();
        let meta_unset = store
            .unset_prop(&device_property_mapping, unset_updated_at)
            .await
            .unwrap();
        assert_eq!(meta_unset, PropMetadata { epoch: Some(1) });

        let unset_prop = store.load_prop(&device_property_mapping).await.unwrap();
        assert_eq!(unset_prop, None);

        // no unset if already unset
        let meta_already_unset = store
            .unset_prop(&device_property_mapping, ctx.next_updated_at())
            .await
            .unwrap();
        assert_eq!(meta_already_unset, PropMetadata { epoch: None });

        let res = store
            .device_props_with_unset(PropertyState::Changed, LIMIT, None)
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        let res = &res[0];
        assert_eq!(res.interface, device_prop.interface);
        assert_eq!(res.path, device_prop.path);
        assert_eq!(res.value, None);
    }

    async fn check_fetch_multiple_props<S>(
        store: &S,
        ctx: &Context,
        device_prop: &Prop,
        device_mapping: PropertyMapping<'_>,
        server_prop: &Prop,
        server_mapping: PropertyMapping<'_>,
    ) where
        S: PropertyStore,
    {
        store.clear().await.unwrap();

        let meta_device = store.store_prop(device_prop.clone()).await.unwrap();
        assert_eq!(meta_device, PropMetadata { epoch: Some(0) });
        let meta_server = store.store_prop(server_prop.clone()).await.unwrap();
        assert_eq!(meta_server, PropMetadata { epoch: Some(0) });

        let exp_device = prop_to_stored_prop(device_prop, meta_device.epoch());
        let exp_server = prop_to_stored_prop(server_prop, meta_server.epoch());

        let res = store.device_props(LIMIT, None).await.unwrap();
        assert_eq!(res, std::slice::from_ref(&exp_device));

        let res = store.server_props(LIMIT, None).await.unwrap();
        assert_eq!(res, std::slice::from_ref(&exp_server));

        let mut res = store.load_all_props(LIMIT, None).await.unwrap();
        res.sort_unstable_by_key(|p| p.updated_at);
        assert_eq!(res, [exp_device.clone(), exp_server.clone()]);

        // With limit
        let mut res = Vec::new();

        let props_fir = store.load_all_props(ONE, None).await.unwrap();
        assert_eq!(props_fir.len(), 1);
        let prop_fir = &props_fir[0];
        res.push(prop_fir.clone());
        let last_update = Some(prop_fir.updated_at);

        let props_sec = store.load_all_props(ONE, last_update).await.unwrap();
        assert_eq!(props_sec.len(), 1);
        let prop_sec = &props_sec[0];
        res.push(prop_sec.clone());
        assert_eq!(res, [exp_device.clone(), exp_server.clone()]);

        // Unset
        let dev_updated_at = ctx.next_updated_at();
        let meta = store
            .unset_prop(&device_mapping, dev_updated_at)
            .await
            .unwrap();
        assert_eq!(meta, PropMetadata { epoch: Some(1) });
        let ser_updated_at = ctx.next_updated_at();
        let meta = store
            .unset_prop(&server_mapping, ser_updated_at)
            .await
            .unwrap();
        assert_eq!(meta, PropMetadata { epoch: Some(1) });

        assert!(store.device_props(LIMIT, None).await.unwrap().is_empty());
        assert!(store.load_all_props(LIMIT, None).await.unwrap().is_empty());
        assert!(store.server_props(LIMIT, None).await.unwrap().is_empty());

        let res = store
            .device_props_with_unset(PropertyState::Changed, LIMIT, None)
            .await
            .unwrap();
        assert_eq!(
            res,
            [StoredProp {
                interface: exp_device.interface,
                path: exp_device.path,
                value: None,
                interface_major: device_prop.interface_major,
                ownership: Ownership::Device,
                updated_at: dev_updated_at,
                epoch: 1
            }],
        );
    }

    async fn check_with_interface<S>(
        store: &S,
        device_prop: &Prop,
        device_properties: &Properties,
        server_prop: &Prop,
        server_properties: &Properties,
    ) where
        S: PropertyStore,
    {
        store.clear().await.unwrap();

        let meta_device = store.store_prop(device_prop.clone()).await.unwrap();
        assert_eq!(meta_device, PropMetadata { epoch: Some(0) });
        let meta_server = store.store_prop(server_prop.clone()).await.unwrap();
        assert_eq!(meta_server, PropMetadata { epoch: Some(0) });

        let exp_device = prop_to_stored_prop(device_prop, meta_device.epoch());
        let exp_server = prop_to_stored_prop(server_prop, meta_server.epoch());

        // props from interface
        let device_props = store
            .interface_props(device_properties, LIMIT, None)
            .await
            .unwrap();
        assert_eq!(device_props, std::slice::from_ref(&exp_device));
        let server_props = store
            .interface_props(server_properties, LIMIT, None)
            .await
            .unwrap();
        assert_eq!(server_props, std::slice::from_ref(&exp_server));
    }

    async fn check_load_and_reset_state<S>(store: &S, device_prop: &Prop)
    where
        S: PropertyStore,
    {
        store.clear().await.unwrap();

        let meta_device = store.store_prop(device_prop.clone()).await.unwrap();
        assert_eq!(meta_device, PropMetadata { epoch: Some(0) });

        // update the state of the device property to completed
        let updated = store
            .update_state(
                &device_prop.interface,
                &device_prop.path,
                PropertyState::Completed,
                meta_device.epoch().unwrap(),
            )
            .await
            .unwrap();
        assert!(updated);

        // check that no properties are in the changed state
        let res = store
            .device_props_with_unset(PropertyState::Changed, LIMIT, None)
            .await
            .unwrap();
        assert!(res.is_empty());

        // reset the state of the properties to bring changes back
        store.reset_session().await.unwrap();

        // ensure state is now changed
        let res = store
            .device_props_with_unset(PropertyState::Changed, LIMIT, None)
            .await
            .unwrap();
        let exp = StoredProp {
            interface: device_prop.interface.clone(),
            path: device_prop.path.clone(),
            value: Some(device_prop.value.clone()),
            interface_major: device_prop.interface_major,
            ownership: device_prop.ownership,
            epoch: meta_device.epoch().unwrap(),
            updated_at: device_prop.updated_at,
        };
        assert_eq!(res, [exp]);
    }

    async fn check_delete_prop<S>(
        store: &S,
        device_prop: &Prop,
        device_mapping: PropertyMapping<'_>,
        device_properties: &Properties,
        server_prop: &Prop,
        server_mapping: PropertyMapping<'_>,
        server_properties: &Properties,
    ) where
        S: PropertyStore,
    {
        store.clear().await.unwrap();

        let meta_device = store.store_prop(device_prop.clone()).await.unwrap();
        assert_eq!(meta_device, PropMetadata { epoch: Some(0) });
        let meta_server = store.store_prop(server_prop.clone()).await.unwrap();
        assert_eq!(meta_server, PropMetadata { epoch: Some(0) });

        // dont delete epoch
        let deleted = store
            .delete_device_prop(
                &device_prop.interface,
                &device_prop.path,
                meta_device.epoch().unwrap() + 1,
            )
            .await
            .unwrap();
        assert!(!deleted);
        let is_stored = store.load_prop(&device_mapping).await.unwrap().is_some();
        assert!(is_stored);
        // delete
        let deleted = store
            .delete_device_prop(
                &device_prop.interface,
                &device_prop.path,
                meta_device.epoch().unwrap(),
            )
            .await
            .unwrap();
        assert!(deleted);
        let is_deleted = store.load_prop(&device_mapping).await.unwrap().is_none();
        assert!(is_deleted);

        // should now be empty
        let props = store
            .device_props_with_unset(PropertyState::Changed, LIMIT, None)
            .await
            .unwrap();
        assert!(props.is_empty());

        // delete server prop
        let deleted = store
            .delete_server_prop(&server_prop.interface, &server_prop.path)
            .await
            .unwrap();
        assert!(deleted);
        assert_eq!(store.load_prop(&server_mapping).await.unwrap(), None);

        let meta_device = store.store_prop(device_prop.clone()).await.unwrap();
        assert_eq!(meta_device, PropMetadata { epoch: Some(0) });
        let meta_server = store.store_prop(server_prop.clone()).await.unwrap();
        assert_eq!(meta_server, PropMetadata { epoch: Some(0) });

        // delete interface properties
        store.delete_interface(device_properties).await.unwrap();
        let prop = store
            .interface_props(device_properties, LIMIT, None)
            .await
            .unwrap();
        assert!(prop.is_empty());
        store.delete_interface(server_properties).await.unwrap();
        let prop = store
            .interface_props(server_properties, LIMIT, None)
            .await
            .unwrap();
        assert!(prop.is_empty());
    }

    async fn check_for_all_types<S>(store: &S, ctx: Context)
    where
        S: PropertyStore,
    {
        let all_types = [
            AstarteData::Double(4.5.try_into().unwrap()),
            AstarteData::Integer(-4),
            AstarteData::Boolean(true),
            AstarteData::LongInteger(45543543534_i64),
            AstarteData::String("hello".into()),
            AstarteData::BinaryBlob(b"hello".to_vec()),
            AstarteData::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
            AstarteData::DoubleArray([1.2, 3.4, 5.6, 7.8].map(|v| v.try_into().unwrap()).to_vec()),
            AstarteData::IntegerArray(vec![1, 3, 5, 7]),
            AstarteData::BooleanArray(vec![true, false, true, true]),
            AstarteData::LongIntegerArray(vec![45543543534_i64, 45543543535_i64, 45543543536_i64]),
            AstarteData::StringArray(vec!["hello".to_owned(), "world".to_owned()]),
            AstarteData::BinaryBlobArray(vec![b"hello".to_vec(), b"world".to_vec()]),
            AstarteData::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ]),
        ];

        for ty in all_types {
            let path = format!("/test/{}", ty.display_type());

            let prop = Prop {
                interface: "com.test".to_string(),
                path: path.clone(),
                value: ty.clone(),
                interface_major: 1,
                ownership: Ownership::Server,
                updated_at: ctx.next_updated_at(),
            };
            let prop_mapping = PropertyMapping {
                interface_name: &prop.interface,
                version_major: prop.interface_major,
                ownership: prop.ownership,
                path: &prop.path,
            };

            let meta = store.store_prop(prop.clone()).await.unwrap();

            let res = store.load_prop(&prop_mapping).await.unwrap();

            let exp = prop_to_stored_prop(&prop, meta.epoch());
            assert_eq!(res, Some(exp));
        }
    }
}
