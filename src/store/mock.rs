// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

use std::collections::HashSet;
use std::num::NonZero;

use astarte_device_error::Error;
use astarte_interfaces::Properties;
use mockall::mock;

use crate::retention::{Id, PublishInfo, RetentionError, StoredInterface};
use crate::session::{IntrospectionInterface, SessionError, StoredSession};
use crate::store::PropertyState;

use super::error::StoreError;
use super::{
    OptStoredProp, Prop, PropMetadata, PropertyMapping, PropertyStore, StoreCapabilities,
    StoredProp, StoredRetention, UpdatedAt,
};

/// trait that should be mocked to control which capabilities
/// the store is allowed to return
pub(crate) trait MockedStoreCapabilities {
    // add an expectation if the retention store capability should be returned
    fn return_retention(&self) -> bool;
    // add an expectation if the session store capability should be returned
    fn return_session(&self) -> bool;
}

mock! {
    pub(crate) Store {
        async fn store_publish_call<'a>(
            &self,
            id: &Id,
            publish: PublishInfo<'a>,
        ) -> Result<(), Error<RetentionError>>;

        async fn update_sent_flag_call(
            &self,
            id: &Id,
            sent: bool,
        ) -> Result<(), Error<RetentionError>>;

        pub(crate) async fn mark_received_call(
            &self,
            id: &Id,
        ) -> Result<(), Error<RetentionError>>;

        pub(crate) async fn delete_interface_call(
            &self,
            interface: &str,
        ) -> Result<(), Error<RetentionError>>;

        pub(crate) async fn unsent_publishes_call(
            &self,
            limit: usize,
            buf: &mut Vec<(Id, crate::retention::PublishInfo<'static>)>,
        ) -> Result<usize, Error<RetentionError>>;

        pub(crate) async fn reset_all_publishes_call(&self) -> Result<(), Error<RetentionError>>;

        pub(crate) async fn fetch_all_interfaces_call(
            &self,
        ) -> Result<std::collections::HashSet<StoredInterface>, Error<RetentionError>>;

        pub(crate) async fn set_max_retention_items_call(
            &self,
            size: std::num::NonZeroUsize,
        ) -> Result<(), Error<RetentionError>>;
    }

    impl Clone for Store {
        fn clone(&self) -> Self;
    }

    impl std::fmt::Debug for Store {
        fn fmt<'a>(&self, f: &mut std::fmt::Formatter<'a>) -> std::fmt::Result;
    }

    impl PropertyStore for Store {
        async fn store_prop(
            &self,
            prop: Prop,
        ) -> Result<PropMetadata, Error<StoreError>>;

        async fn update_state(
            &self,
            interface_name: &str,
            path: &str,
            state: PropertyState,
            epoch: u8,
        ) -> Result<bool, Error<StoreError>>;

        async fn load_prop<'a>(
            &self,
            property: &PropertyMapping<'a>,
        ) -> Result<Option<StoredProp>, Error<StoreError>>;

        async fn unset_prop<'a>(
            &self,
            property: &PropertyMapping<'a>,
            updated_at: UpdatedAt,
        ) -> Result<PropMetadata, Error<StoreError>>;

        async fn delete_server_prop(
            &self,
            interface_name: &str,
            path: &str,
        ) -> Result<bool, Error<StoreError>>;

        async fn delete_device_prop(
            &self,
            interface_name: &str,
            path: &str,
            epoch: u8,
        ) -> Result<bool, Error<StoreError>>;

        async fn clear(&self) -> Result<(), Error<StoreError>>;

        async fn load_all_props(
            &self,
            limit: NonZero<usize>,
            last_updated_at: Option<UpdatedAt>,
        ) -> Result<Vec<StoredProp>, Error<StoreError>>;

        async fn device_props(
            &self,
            limit: NonZero<usize>,
            last_updated_at: Option<UpdatedAt>,
        ) -> Result<Vec<StoredProp>, Error<StoreError>>;

        async fn server_props(
            &self,
            limit: NonZero<usize>,
            last_updated_at: Option<UpdatedAt>,
        ) -> Result<Vec<StoredProp>, Error<StoreError>>;

        async fn interface_props(
            &self,
            interface: &Properties,
            limit: NonZero<usize>,
            last_updated_at: Option<UpdatedAt>,
        ) -> Result<Vec<StoredProp>, Error<StoreError>>;

        async fn delete_interface(
            &self,
            interface: &Properties,
        ) -> Result<(), Error<StoreError>>;

        async fn device_props_with_unset(
            &self,
            state: PropertyState,
            limit: NonZero<usize>,
            last_updated_at: Option<UpdatedAt>,
        ) -> Result<Vec<OptStoredProp>, Error<StoreError>>;

        async fn reset_session(&self) -> Result<(), Error<StoreError>>;
    }


    impl MockedStoreCapabilities for Store {
        fn return_retention(&self) -> bool;
        fn return_session(&self) -> bool;
    }

    impl StoredSession for Store {
        async fn add_interfaces<'a>(
            &self,
            interfaces: &[IntrospectionInterface<&'a str>],
        ) -> Result<(), Error<SessionError>>;

        async fn clear_introspection(&self);

        async fn store_introspection(
            &self,
            interfaces: &[IntrospectionInterface],
        );

        async fn load_introspection(
            &self,
        ) -> Result<Vec<IntrospectionInterface>, Error<SessionError>>;

        async fn remove_interfaces<'a>(
            &self,
            interfaces: &[IntrospectionInterface<&'a str>],
        ) -> Result<(), Error<SessionError>>;
    }
}

// Un-mockable delete_interface method, because implemented by 2 traits. So we forward the call to a
// renamed impl method.
impl StoredRetention for MockStore {
    async fn store_publish<'a>(
        &self,
        id: &Id,
        publish: PublishInfo<'a>,
    ) -> Result<(), Error<RetentionError>> {
        self.store_publish_call(id, publish).await
    }

    async fn update_sent_flag(&self, id: &Id, sent: bool) -> Result<(), Error<RetentionError>> {
        self.update_sent_flag_call(id, sent).await
    }

    async fn mark_received(&self, id: &Id) -> Result<(), Error<RetentionError>> {
        self.mark_received_call(id).await
    }

    async fn delete_interface(&self, interface: &str) -> Result<(), Error<RetentionError>> {
        self.delete_interface_call(interface).await
    }

    async fn unsent_publishes(
        &self,
        limit: usize,
        buf: &mut Vec<(Id, crate::retention::PublishInfo<'static>)>,
    ) -> Result<usize, Error<RetentionError>> {
        self.unsent_publishes_call(limit, buf).await
    }

    async fn reset_all_publishes(&self) -> Result<(), Error<RetentionError>> {
        self.reset_all_publishes_call().await
    }

    async fn fetch_all_interfaces(
        &self,
    ) -> Result<HashSet<StoredInterface>, Error<RetentionError>> {
        self.fetch_all_interfaces_call().await
    }

    async fn set_max_retention_items(
        &self,
        size: std::num::NonZeroUsize,
    ) -> Result<(), Error<RetentionError>> {
        self.set_max_retention_items_call(size).await
    }
}

impl StoreCapabilities for MockStore {
    type Retention = Self;
    type Session = Self;

    fn get_retention(&self) -> Option<&Self::Retention> {
        self.return_retention().then_some(self)
    }

    fn get_session(&self) -> Option<&Self::Session> {
        self.return_session().then_some(self)
    }
}
