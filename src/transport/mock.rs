// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

//! Mock for the transport to use in the tests.

use std::any::Any;
use std::collections::HashMap;

use astarte_interfaces::{
    DatastreamIndividual, DatastreamObject, Interface, MappingPath, Properties,
};
use mockall::mock;

use super::{Connection, Disconnect, Publish, Receive, ReceivedEvent, Register};
use crate::aggregate::AstarteObject;
use crate::builder::{BuildConfig, ConnectionConfig, DeviceTransport};
use crate::error::AstarteError;
use crate::interfaces::MappingRef;
use crate::interfaces::{self, Interfaces};
use crate::store::StoreCapabilities;
use crate::{AstarteData, Timestamp};

type GenericPayload = Box<dyn Any + Send + Sync>;

mock! {
    pub(crate) Config<S: StoreCapabilities> {}

    impl<S: StoreCapabilities> ConnectionConfig<S> for Config<S> {
        type Store = S;
        type Conn = MockCon<S>;

        async fn connect(self, config: BuildConfig<S>) -> Result<DeviceTransport<MockCon<S>>, AstarteError>;
    }
}

mock! {
    pub(crate) Con<S> {}

    impl<S: StoreCapabilities> Connection for Con<S> {
        type Sender = MockSender;
        type Store = S;

        fn is_paired(&self) -> impl Future<Output = Result<bool, std::io::Error>> + Send {
        }
    }

    impl<S: StoreCapabilities> Receive for Con<S> {
        type Payload = GenericPayload;

        async fn next_event(
            &mut self,
        ) -> Result<Option<ReceivedEvent<GenericPayload>>, AstarteError>;

        fn reconnect(&mut self, interfaces: &Interfaces) -> impl Future<Output = Result<crate::transport::AttemptStatus<GenericPayload>, AstarteError>> + Send {
        }

        fn deserialize_property<'a>(
            &self,
            mapping: &MappingRef<'a, Properties> ,
            payload: GenericPayload,
        ) -> Result<Option<AstarteData>, AstarteError>;

        fn deserialize_individual<'a>(
            &self,
            mapping: &MappingRef<'a, DatastreamIndividual> ,
            payload: GenericPayload,
        ) -> Result<(AstarteData, Option<Timestamp>), AstarteError>;

        fn deserialize_object<'a>(
            &self,
            object: &DatastreamObject,
            path: &MappingPath<'a> ,
            payload: GenericPayload,
        ) -> Result<(AstarteObject, Option<Timestamp>), AstarteError>;
    }
}

mock! {
    pub(crate) Sender {}

    impl Clone for Sender {
        fn clone(&self) -> Self;
    }

    impl Publish for Sender {
        async fn send_individual(
            &mut self,
            data: crate::validate::ValidatedIndividual,
        ) -> Result<(), AstarteError>;

        async fn send_object(
            &mut self,
            data: crate::validate::ValidatedObject,
        ) -> Result<(), AstarteError>;

        async fn send_individual_stored(
            &mut self,
            id: crate::retention::RetentionId,
            data: crate::validate::ValidatedIndividual,
        ) -> Result<(), AstarteError>;

        async fn send_object_stored(
            &mut self,
            id: crate::retention::RetentionId,
            data: crate::validate::ValidatedObject,
        ) -> Result<(), AstarteError>;

        async fn resend_stored<'a>(
            &mut self,
            id: crate::retention::RetentionId,
            data: crate::retention::PublishInfo<'a> ,
        ) -> Result<(), AstarteError>;

        async fn resend_stored_property(
            &mut self,
            property_data: crate::store::OptStoredProp,
        ) -> Result<(), AstarteError>;

        async fn send_property(
            &mut self,
            data: crate::validate::ValidatedProperty,
        ) -> Result<(), AstarteError>;

        async fn unset(
            &mut self,
            data: crate::validate::ValidatedUnset,
        ) -> Result<(), AstarteError>;

        fn serialize_individual(&self, data: &crate::validate::ValidatedIndividual) -> Result<Vec<u8>, AstarteError>;

        fn serialize_object(&self, data: &crate::validate::ValidatedObject) -> Result<Vec<u8>, AstarteError>;
    }

    impl Register for Sender {
        async fn add_interface(
            &mut self,
            interfaces: &Interfaces,
            added_interface: &interfaces::Validated,
        ) -> Result<(), AstarteError>;

        async fn extend_interfaces(
            &mut self,
            interfaces: &Interfaces,
            added_interface: &interfaces::ValidatedCollection,
        ) -> Result<(), AstarteError>;

        async fn remove_interface(
            &mut self,
            interfaces: &Interfaces,
            removed_interface: &Interface,
        ) -> Result<(), AstarteError>;

        async fn remove_interfaces<'a,'b>(
            &mut self,
            interfaces: &Interfaces,
            removed_interfaces: &HashMap<&'a str, &'b Interface>,
        ) -> Result<(), AstarteError>;
    }

    impl Disconnect for Sender {
        async fn disconnect(&mut self) -> Result<(), AstarteError>;
    }
}
