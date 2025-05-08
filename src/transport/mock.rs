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

//! Mock for the transport to use in the tests.

use std::any::Any;
use std::collections::HashMap;

use mockall::mock;

use super::{
    Connection, Disconnect, Publish, Receive, ReceivedEvent, Reconnect, Register, TransportError,
};
use crate::aggregate::AstarteObject;
use crate::builder::{BuildConfig, ConnectionConfig, DeviceTransport};
use crate::interface::mapping::path::MappingPath;
use crate::interface::reference::{MappingRef, ObjectRef};
use crate::interfaces::{self, Interfaces};
use crate::store::StoreCapabilities;
use crate::{AstarteType, Error, Interface, Timestamp};

type GenericPayload = Box<dyn Any + Send + Sync>;

mock! {
    pub(crate) Config<S: StoreCapabilities> {}

    impl<S: StoreCapabilities> ConnectionConfig<S> for Config<S> {
        type Store = S;
        type Conn = MockCon<S>;
        type Err = Error;

        async fn connect(self, config: BuildConfig<S>) -> Result<DeviceTransport<MockCon<S>>, Error>;
    }
}

mock! {
    pub(crate) Con<S> {}

    impl<S: StoreCapabilities> Connection for Con<S> {
        type Sender = MockSender;
        type Store = S;
    }

    impl<S: StoreCapabilities> Receive  for Con<S> {
        type Payload = GenericPayload;

        async fn next_event(
            &mut self,
        ) -> Result<Option<ReceivedEvent<GenericPayload>>, TransportError>;

        fn deserialize_property<'a>(
            &self,
            mapping: &MappingRef<'a, &'a Interface> ,
            payload: GenericPayload,
        ) -> Result<Option<AstarteType>, TransportError>;

        fn deserialize_individual<'a>(
            &self,
            mapping: &MappingRef<'a, &'a Interface> ,
            payload: GenericPayload,
        ) -> Result<(AstarteType, Option<Timestamp>), TransportError>;

        fn deserialize_object<'a>(
            &self,
            object: &ObjectRef<'a>,
            path: &MappingPath<'a> ,
            payload: GenericPayload,
        ) -> Result<(AstarteObject, Option<Timestamp>), TransportError>;
    }

    impl<S: StoreCapabilities> Reconnect  for Con<S> {
        async fn reconnect(&mut self, interfaces: &Interfaces) -> Result<bool, Error> {
        }
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
        ) -> Result<(),crate::Error>;

        async fn send_object(
            &mut self,
            data: crate::validate::ValidatedObject,
        ) -> Result<(),crate::Error>;

        async fn send_individual_stored(
            &mut self,
            id: crate::retention::RetentionId,
            data: crate::validate::ValidatedIndividual,
        ) -> Result<(),crate::Error>;

        async fn send_object_stored(
            &mut self,
            id: crate::retention::RetentionId,
            data: crate::validate::ValidatedObject,
        ) -> Result<(),crate::Error>;

        async fn resend_stored<'a>(
            &mut self,
            id: crate::retention::RetentionId,
            data: crate::retention::PublishInfo<'a> ,
        ) -> Result<(),crate::Error>;

        async fn send_property(
            &mut self,
            data: crate::validate::ValidatedProperty,
        ) -> Result<(),crate::Error>;

        async fn unset(
            &mut self,
            data: crate::validate::ValidatedUnset,
        ) -> Result<(),crate::Error>;

        fn serialize_individual(&self, data: &crate::validate::ValidatedIndividual) -> Result<Vec<u8>, crate::Error>;

        fn serialize_object(&self, data: &crate::validate::ValidatedObject) -> Result<Vec<u8>, crate::Error>;
    }

    impl Register for Sender {
        async fn add_interface(
            &mut self,
            interfaces: &Interfaces,
            added_interface: &interfaces::Validated,
        ) -> Result<(), crate::Error>;

        async fn extend_interfaces(
            &mut self,
            interfaces: &Interfaces,
            added_interface: &interfaces::ValidatedCollection,
        ) -> Result<(), crate::Error>;

        async fn remove_interface(
            &mut self,
            interfaces: &Interfaces,
            removed_interface: &Interface,
        ) -> Result<(), crate::Error>;

        async fn remove_interfaces<'a,'b>(
            &mut self,
            interfaces: &Interfaces,
            removed_interfaces: &HashMap<&'a str, &'b Interface>,
        ) -> Result<(), crate::Error>;
    }

    impl Disconnect for Sender {
        async fn disconnect(&mut self) -> Result<(), crate::Error>;
    }
}
