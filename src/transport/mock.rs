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

//! Mock for the transport to use in the tests.

use std::ops::ControlFlow;

use astarte_interfaces::Interface;
use mockall::mock;

use super::{Decode, Encode, Introspection, Sender, Transport};
use crate::builder::{BuildConfig, ConnectionConfig};
use crate::connection::incoming::ctx::ConnectionCtx;
use crate::error::AstarteError;
use crate::interfaces::{self, Interfaces};
use crate::state::SharedState;
use crate::store::StoreCapabilities;
use crate::validate::individual::ValidatedIndividual;
use crate::validate::object::ValidatedObject;
use crate::validate::properties::{ValidatedProperty, ValidatedUnset};

mock! {
    pub(crate) Config {}

    impl ConnectionConfig for Config {
        type Store<S> = S
        where
            S: Send + Sync + 'static;
        type Connection = MockCon;
        type Client = MockSender;
        type Encoder = MockEncoder;


        async fn configure<S>(
            &mut self,
            config: SharedState<S>,
        ) ->  Result<BuildConfig<S, MockEncoder>, AstarteError>
        where
            S: StoreCapabilities;

        async fn is_registered<S>(
            &mut self,
            state: &crate::state::SharedState<S> ,
        ) -> Result<bool, AstarteError>
        where
            S: StoreCapabilities;


        async fn register<S>(
            &mut self,
            state: &crate::state::ConnectionState<S> ,
        ) -> Result<ControlFlow<(MockSender, MockCon)>, AstarteError>
        where
            S: StoreCapabilities;
    }
}

mock! {
  pub(crate) Con {
    pub(crate) async fn connect_call<S>(
        &mut self,
        state: &crate::state::ConnectionState<S>,
        interfaces: &Interfaces,
        first: bool,
    ) -> Result<ControlFlow<bool>, AstarteError>
    where
        S: StoreCapabilities;

    pub(crate) async fn poll_call(&mut self) -> Result<(), AstarteError>;
  }
}

// NOTE: The poll method needs to be proxied since it has a generic lifetime and type, and mockall
// doesn't support it
impl Transport for MockCon {
    async fn connect<S>(
        &mut self,
        state: &crate::state::ConnectionState<S>,
        interfaces: &Interfaces,
        first: bool,
    ) -> Result<ControlFlow<bool>, AstarteError>
    where
        S: StoreCapabilities,
    {
        self.connect_call(state, interfaces, first).await
    }

    async fn poll<'a, S>(&mut self, _ctx: &ConnectionCtx<'a, S>) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.poll_call().await
    }
}

mock! {
    pub(crate) Sender {
        pub(crate) async fn handshake_call<S>(
            &mut self,
            ctx: &crate::state::ConnectionState<S>,
            interfaces: &Interfaces,
            session_present: bool,
        ) -> Result<ControlFlow<()>, AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn disconnect_call(&mut self) -> Result<(), AstarteError>;

        pub(crate) async fn send_individual_call(
            &mut self,
            data: ValidatedIndividual,
        ) -> Result<(), AstarteError>;

        pub(crate) async fn send_object_call(
            &mut self,
            data: ValidatedObject,
        ) -> Result<(), AstarteError>;

        pub(crate) async fn send_individual_stored_call<S>(
            &mut self,
            ctx: &crate::state::ConnectionState<S>,
            id: crate::retention::RetentionId,
            data: ValidatedIndividual,
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn send_object_stored_call<S>(
            &mut self,
            state: &crate::state::ConnectionState<S>,
            id: crate::retention::RetentionId,
            data: ValidatedObject,
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn resend_stored_call<S>(
            &mut self,
            state: &crate::state::ConnectionState<S>,
            id: crate::retention::RetentionId,
            data: crate::retention::PublishInfo<'static>,
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn resend_stored_property_call<S>(
            &mut self,
            state: &crate::state::ConnectionState<S>,
            property_data: crate::store::OptStoredProp,
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn send_property_call<S>(
            &mut self,
            state: &crate::state::ConnectionState<S>,
            data: ValidatedProperty,
            epoch: u8,
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn unset_call<S>(
            &mut self,
            state: &crate::state::ConnectionState<S>,
            data: ValidatedUnset,
            epoch: u8,
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn add_interface_call<S>(
            &mut self,
            ctx: &crate::state::ConnectionState<S>,
            interfaces: &Interfaces,
            added_interface: &Interface,
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn extend_interfaces_call<S>(
            &mut self,
            ctx: &crate::state::ConnectionState<S>,
            interfaces: &Interfaces,
            added_interface: &interfaces::ValidatedCollection,
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn remove_interface_call<S>(
            &mut self,
            ctx: &crate::state::ConnectionState<S>,
            interfaces: &Interfaces,
            interface: &crate::transport::RemovedInterface,
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;

        pub(crate) async fn remove_interfaces_call<S>(
            &mut self,
            ctx: &crate::state::ConnectionState<S>,
            interfaces: &Interfaces,
            removed_interfaces: &[crate::transport::RemovedInterface],
        ) -> Result<(), AstarteError>
        where
            S: StoreCapabilities;
    }
}

impl Sender for MockSender {
    async fn handshake<S>(
        &mut self,
        ctx: &crate::state::ConnectionState<S>,
        interfaces: &Interfaces,
        session_present: bool,
    ) -> Result<ControlFlow<()>, AstarteError>
    where
        S: StoreCapabilities,
    {
        self.handshake_call(ctx, interfaces, session_present).await
    }

    async fn disconnect(&mut self) -> Result<(), AstarteError> {
        self.disconnect_call().await
    }

    async fn send_individual(&mut self, data: ValidatedIndividual) -> Result<(), AstarteError> {
        self.send_individual_call(data).await
    }

    async fn send_object(&mut self, data: ValidatedObject) -> Result<(), AstarteError> {
        self.send_object_call(data).await
    }

    async fn send_individual_stored<S>(
        &mut self,
        ctx: &crate::state::ConnectionState<S>,
        id: crate::retention::RetentionId,
        data: ValidatedIndividual,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.send_individual_stored_call(ctx, id, data).await
    }

    async fn send_object_stored<S>(
        &mut self,
        state: &crate::state::ConnectionState<S>,
        id: crate::retention::RetentionId,
        data: ValidatedObject,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.send_object_stored_call(state, id, data).await
    }

    async fn resend_stored<'a, S>(
        &mut self,
        state: &crate::state::ConnectionState<S>,
        id: crate::retention::RetentionId,
        data: crate::retention::PublishInfo<'a>,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.resend_stored_call(state, id, data.into_owned()).await
    }

    async fn resend_stored_property<S>(
        &mut self,
        state: &crate::state::ConnectionState<S>,
        property_data: crate::store::OptStoredProp,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.resend_stored_property_call(state, property_data).await
    }

    async fn send_property<S>(
        &mut self,
        state: &crate::state::ConnectionState<S>,
        data: ValidatedProperty,
        epoch: u8,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.send_property_call(state, data, epoch).await
    }

    async fn unset<S>(
        &mut self,
        state: &crate::state::ConnectionState<S>,
        data: ValidatedUnset,
        epoch: u8,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.unset_call(state, data, epoch).await
    }
}

impl Introspection for MockSender {
    async fn add_interface<S>(
        &mut self,
        ctx: &crate::state::ConnectionState<S>,
        interfaces: &Interfaces,
        added_interface: &Interface,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.add_interface_call(ctx, interfaces, added_interface)
            .await
    }

    async fn extend_interfaces<S>(
        &mut self,
        ctx: &crate::state::ConnectionState<S>,
        interfaces: &Interfaces,
        added_interface: &interfaces::ValidatedCollection,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.extend_interfaces_call(ctx, interfaces, added_interface)
            .await
    }

    async fn remove_interface<S>(
        &mut self,
        ctx: &crate::state::ConnectionState<S>,
        interfaces: &Interfaces,
        interface: &crate::transport::RemovedInterface,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.remove_interface_call(ctx, interfaces, interface).await
    }

    async fn remove_interfaces<S>(
        &mut self,
        ctx: &crate::state::ConnectionState<S>,
        interfaces: &Interfaces,
        removed_interfaces: &[crate::transport::RemovedInterface],
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        self.remove_interfaces_call(ctx, interfaces, removed_interfaces)
            .await
    }
}

mock! {
    pub(crate) Encoder {}

    impl Clone for Encoder {
        fn clone(&self) -> Self;
    }

    impl Encode for Encoder {
        fn serialize_individual(&self, data: &ValidatedIndividual) -> Result<Vec<u8>, AstarteError>;

        fn serialize_object(&self, data: &ValidatedObject) -> Result<Vec<u8>, AstarteError>;
    }
}

mock! {
    pub(crate) Decoder {}

    impl Decode for Decoder {
        fn deserialize_property<'a>(
            self,
            mapping: &interfaces::MappingRef<'a, astarte_interfaces::Properties> ,
        ) -> Result<Option<crate::AstarteData>, AstarteError>;

        fn deserialize_individual<'a>(
            self,
            mapping: &interfaces::MappingRef<'a, astarte_interfaces::DatastreamIndividual> ,
        ) -> Result<(crate::AstarteData, Option<crate::Timestamp>), AstarteError>;

        fn deserialize_object<'a>(
            self,
            object: &astarte_interfaces::DatastreamObject,
            path: &astarte_interfaces::MappingPath<'a> ,
        ) -> Result<(crate::aggregate::AstarteObject, Option<crate::Timestamp>), AstarteError>;
    }
}
