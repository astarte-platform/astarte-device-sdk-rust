// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
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

//! # Astarte Connection Traits
//!
//! This module defines traits and structures for handling communication and interaction
//! with the Astarte.
//!
//! The module includes traits for publishing and receiving Astarte data over a connection,
//! as well as registering and managing interfaces on a device.

use std::ops::ControlFlow;

use astarte_interfaces::schema::Ownership;
use astarte_interfaces::{
    DatastreamIndividual, DatastreamObject, Interface, MappingPath, Properties,
};

use crate::Timestamp;
use crate::aggregate::AstarteObject;
use crate::connection::incoming::ctx::ConnectionCtx;
use crate::error::AstarteError;
use crate::interfaces::{self, Interfaces, MappingRef};
use crate::retention::{PublishInfo, RetentionId};
use crate::state::ConnectionState;
use crate::store::{OptStoredProp, StoreCapabilities};
use crate::types::AstarteData;
use crate::validate::individual::ValidatedIndividual;
use crate::validate::object::ValidatedObject;
use crate::validate::properties::{ValidatedProperty, ValidatedUnset};

#[cfg(feature = "message-hub")]
#[cfg_attr(astarte_device_sdk_docsrs, doc(cfg(feature = "message-hub")))]
pub mod grpc;
pub mod mqtt;

#[cfg(test)]
pub(crate) mod mock;

pub(crate) trait Transport: Send + Sync {
    /// Function called by [`DeviceConnection`](crate::connection::DeviceConnection) when the
    /// connecting to the cloud.
    ///
    /// It tries to reconnect once, if it succeed it will return the session state. True when the
    /// session is present, false otherwise. It will signal a retry with [`ControlFlow::Continue`].
    fn connect<S>(
        &mut self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
        first: bool,
    ) -> impl Future<Output = Result<ControlFlow<bool>, AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// This function returns the next event from the connection
    /// and waits for it as necessary. It's important to note that not
    /// every received incoming event must get returned from this method.
    /// Implementations could decide to process internally some types of
    /// incoming messages.
    fn poll<'a, S>(
        &mut self,
        ctx: &ConnectionCtx<'a, S>,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;
}

/// Payload received from a transport
pub(crate) trait Decode: Send + Sync {
    /// Deserializes a received payload to an property.
    fn deserialize_property(
        self,
        mapping: &MappingRef<'_, Properties>,
    ) -> Result<Option<AstarteData>, AstarteError>;

    /// Deserializes a received payload to an individual astarte value
    fn deserialize_individual(
        self,
        mapping: &MappingRef<'_, DatastreamIndividual>,
    ) -> Result<(AstarteData, Option<Timestamp>), AstarteError>;

    /// Deserializes a received payload to an aggregate object
    fn deserialize_object(
        self,
        object: &DatastreamObject,
        path: &MappingPath<'_>,
    ) -> Result<(AstarteObject, Option<Timestamp>), AstarteError>;
}

// Implement the publication for a connection.
///
/// A connection should manage only the cleanup of the stored publishes.
///
/// It's generic over the id provided by the store for the retention.
pub(crate) trait Sender: Send + Sync {
    /// After connecting send handshake messages
    ///
    /// This needs to retry errors internally.
    fn handshake<S>(
        &mut self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
        session_present: bool,
    ) -> impl Future<Output = Result<ControlFlow<()>, AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// Gracefully disconnect from the transport
    fn disconnect(&mut self) -> impl Future<Output = Result<(), AstarteError>> + Send;

    /// Sends validated individual values over this connection
    fn send_individual(
        &mut self,
        data: ValidatedIndividual,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send;

    /// Sends validated objects values over this connection
    fn send_object(
        &mut self,
        data: ValidatedObject,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send;

    /// Sends validated individual values with stored retention over this connection.
    ///
    /// The id is to identify the packet to confirm it was received by the server.
    fn send_individual_stored<S>(
        &mut self,
        ctx: &ConnectionState<S>,
        id: RetentionId,
        data: ValidatedIndividual,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// Sends validated objects values with stored retention over this connection
    ///
    /// The id is to identify the packet to confirm it was received by the server.
    fn send_object_stored<S>(
        &mut self,
        state: &ConnectionState<S>,
        id: RetentionId,
        data: ValidatedObject,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// Resend previously stored publish.
    fn resend_stored<'a, S>(
        &mut self,
        state: &ConnectionState<S>,
        id: RetentionId,
        data: PublishInfo<'a>,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// Resend previously stored property.
    fn resend_stored_property<S>(
        &mut self,
        state: &ConnectionState<S>,
        property_data: OptStoredProp,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// Sends validated property values over this connection
    fn send_property<S>(
        &mut self,
        state: &ConnectionState<S>,
        data: ValidatedProperty,
        epoch: u8,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// Unset a property value over this connection.
    fn unset<S>(
        &mut self,
        state: &ConnectionState<S>,
        data: ValidatedUnset,
        epoch: u8,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;
}

/// Payload sent from a client
pub(crate) trait Encode: Clone + Send + Sync {
    /// Serializes an individual astarte value.
    fn serialize_individual(&self, data: &ValidatedIndividual) -> Result<Vec<u8>, AstarteError>;

    /// Serializes an aggregate object.
    fn serialize_object(&self, data: &ValidatedObject) -> Result<Vec<u8>, AstarteError>;
}

pub(crate) trait Introspection {
    /// Called when an interface gets added to the device interface list.
    /// This method should convey to the server that a new interface got added.
    fn add_interface<S>(
        &mut self,
        ctx: &ConnectionState<S>,
        interfaces: &Interfaces,
        added_interface: &Interface,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// Called when multiple interfaces are added.
    ///
    /// This method should convey to the server that one or more interfaces have been added.
    ///
    /// The added interfaces are still not present in the [`Interfaces`]
    fn extend_interfaces<S>(
        &mut self,
        ctx: &ConnectionState<S>,
        interfaces: &Interfaces,
        added_interface: &interfaces::ValidatedCollection,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// Called when an interface gets removed from the device interface list.
    /// It relays to the server the removal of the interface.
    fn remove_interface<S>(
        &mut self,
        ctx: &ConnectionState<S>,
        interfaces: &Interfaces,
        interface: &RemovedInterface,
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;

    /// Called when multiple interfaces get removed from the device interface list.
    /// It relays to the server the removal of the interface.
    fn remove_interfaces<S>(
        &mut self,
        ctx: &ConnectionState<S>,
        interfaces: &Interfaces,
        removed_interfaces: &[RemovedInterface],
    ) -> impl Future<Output = Result<(), AstarteError>> + Send
    where
        S: StoreCapabilities;
}

/// Removed interface.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RemovedInterface {
    pub(crate) interface_name: String,
    pub(crate) version_major: i32,
    pub(crate) version_minor: i32,
    pub(crate) ownership: Ownership,
}

impl RemovedInterface {
    pub(crate) fn interface_name(&self) -> &str {
        &self.interface_name
    }

    pub(crate) fn version_major(&self) -> i32 {
        self.version_major
    }

    pub(crate) fn version_minor(&self) -> i32 {
        self.version_minor
    }

    pub(crate) fn ownership(&self) -> Ownership {
        self.ownership
    }
}

impl<'a> From<&'a Interface> for RemovedInterface {
    fn from(value: &'a Interface) -> Self {
        Self {
            interface_name: value.interface_name().to_string(),
            ownership: value.ownership(),
            version_major: value.version_major(),
            version_minor: value.version_minor(),
        }
    }
}
