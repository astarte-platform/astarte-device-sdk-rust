// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

use mockall::mock;

use crate::{
    session::{IntrospectionInterface, SessionError, StoredSession},
    AstarteType,
};

use super::{
    error::StoreError, MissingCapability, PropertyInterface, PropertyMapping, PropertyStore,
    StoreCapabilities, StoredProp,
};

/// trait that should be mocked to control which capabilities
/// the store is allowed to return
pub(crate) trait MockedStoreCapabilities {
    // add an expectation if the retention store capability should be returned
    // TODO to be used when a mock for the retention gets implemented
    #[allow(dead_code)]
    fn return_retention(&self) -> bool;
    // add an expectation if the session store capability should be returned
    fn return_session(&self) -> bool;
}

mock! {
    pub(crate) Store {}

    impl Clone for Store {
        fn clone(&self) -> Self;
    }

    impl std::fmt::Debug for Store {
        fn fmt<'a>(&self, f: &mut std::fmt::Formatter<'a>) -> std::fmt::Result;
    }

    impl PropertyStore for Store {
        type Err = StoreError;

        async fn store_prop<'a, 'b>(
            &self,
            prop: StoredProp<&'a str, &'b AstarteType>,
        ) -> Result<(), StoreError>;

        async fn load_prop<'a>(
            &self,
            property: &PropertyMapping<'a>,
            interface_major: i32,
        ) -> Result<Option<AstarteType>, StoreError>;

        async fn unset_prop<'a>(
            &self,
            property: &PropertyMapping<'a>,
        ) -> Result<(), StoreError>;

        async fn delete_prop<'a>(
            &self,
            property: &PropertyMapping<'a>,
        ) -> Result<(), StoreError>;

        async fn clear(&self) -> Result<(), StoreError>;

        async fn load_all_props(&self) -> Result<Vec<StoredProp>, StoreError>;

        async fn device_props(&self) -> Result<Vec<StoredProp>, StoreError>;

        async fn server_props(&self) -> Result<Vec<StoredProp>, StoreError>;

        async fn interface_props<'a>(
            &self,
            interface: &PropertyInterface<'a>,
        ) -> Result<Vec<StoredProp>, StoreError>;

        async fn delete_interface<'a>(
            &self,
            interface: &PropertyInterface<'a>,
        ) -> Result<(), StoreError>;

        async fn device_props_with_unset(
            &self,
        ) -> Result<Vec<super::OptStoredProp>, StoreError>;
    }

    impl MockedStoreCapabilities for Store {
        fn return_retention(&self) -> bool;
        fn return_session(&self) -> bool;
    }

    impl StoredSession for Store {
        async fn add_interfaces<'a>(
            &self,
            interfaces: &[IntrospectionInterface<&'a str>],
        ) -> Result<(), SessionError>;

        async fn clear_introspection(&self);

        async fn store_introspection(
            &self,
            interfaces: &[IntrospectionInterface],
        );

        async fn load_introspection(
            &self,
        ) -> Result<Vec<IntrospectionInterface>, SessionError>;

        async fn remove_interfaces<'a>(
            &self,
            interfaces: &[IntrospectionInterface<&'a str>],
        ) -> Result<(), SessionError>;
    }
}

impl StoreCapabilities for MockStore {
    type Retention = MissingCapability;
    type Session = Self;

    fn get_retention(&self) -> Option<&Self::Retention> {
        // TODO enable once a mock for store capabilities gets implemented
        //self.return_session().then_some(self)
        None
    }

    fn get_session(&self) -> Option<&Self::Session> {
        self.return_session().then_some(self)
    }
}
