// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
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

//! # Astarte Connection Traits
//!
//! This module defines traits and structures for handling communication and interaction
//! with the Astarte.
//!
//! The module includes traits for publishing and receiving Astarte data over a connection,
//! as well as registering and managing interfaces on a device.

use std::{collections::HashMap, future::Future};

use astarte_interfaces::{
    DatastreamIndividual, DatastreamObject, Interface, MappingPath, Properties,
};

use crate::{
    Timestamp,
    aggregate::AstarteObject,
    client::RecvError,
    interfaces::{self, Interfaces, MappingRef},
    retention::{PublishInfo, RetentionId},
    store::{OptStoredProp, StoreCapabilities},
    types::AstarteData,
    validate::{ValidatedIndividual, ValidatedObject, ValidatedProperty, ValidatedUnset},
};

#[cfg(feature = "message-hub")]
#[cfg_attr(astarte_device_sdk_docsrs, doc(cfg(feature = "message-hub")))]
pub mod grpc;
pub mod mqtt;

#[cfg(test)]
pub(crate) mod mock;

#[derive(thiserror::Error, Debug)]
pub(crate) enum TransportError {
    /// Error that will be sent to the client
    #[error("error that will be sent to the client, {0:?}")]
    Recv(#[from] RecvError),
    /// Error from the underline transport
    #[error("error from the underline transport")]
    Transport(#[source] crate::Error),
}

/// Holds generic event data such as interface name and path
/// The payload must be deserialized after verification with the
/// specific [`Connection::deserialize_individual`] or [`Connection::serialize_individual`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReceivedEvent<P> {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) payload: P,
}

/// Trait to link a Sender to a Connection.
pub trait Connection: Send + Sync {
    /// Sender for the connection.
    ///
    /// This is the client part of the connection to send data.
    type Sender: Send + Sync + Clone;
    /// Storage configured by the connection.
    ///
    /// This reduces the number of generics for connection, since a single client type is associated
    /// with a connection.
    type Store: StoreCapabilities;
}

/// Implement the publication for a connection.
///
/// A connection should manage only the cleanup of the stored publishes.
///
/// It's generic over the id provided by the store for the retention.
pub(crate) trait Publish {
    /// Sends validated individual values over this connection
    fn send_individual(
        &mut self,
        data: ValidatedIndividual,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Sends validated objects values over this connection
    fn send_object(
        &mut self,
        data: ValidatedObject,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Sends validated individual values with stored retention over this connection.
    ///
    /// The id is to identify the packet to confirm it was received by the server.
    fn send_individual_stored(
        &mut self,
        id: RetentionId,
        data: ValidatedIndividual,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Sends validated objects values with stored retention over this connection
    ///
    /// The id is to identify the packet to confirm it was received by the server.
    fn send_object_stored(
        &mut self,
        id: RetentionId,
        data: ValidatedObject,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Resend previously stored publish.
    fn resend_stored(
        &mut self,
        id: RetentionId,
        data: PublishInfo<'_>,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Resend previously stored property.
    fn resend_stored_property(
        &mut self,
        property_data: OptStoredProp,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Sends validated property values over this connection
    fn send_property(
        &mut self,
        data: ValidatedProperty,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Unset a property value over this connection.
    fn unset(
        &mut self,
        data: ValidatedUnset,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Serializes an individual astarte value.
    fn serialize_individual(&self, data: &ValidatedIndividual) -> Result<Vec<u8>, crate::Error>;

    /// Serializes an aggregate object.
    fn serialize_object(&self, data: &ValidatedObject) -> Result<Vec<u8>, crate::Error>;
}

pub(crate) trait Receive {
    type Payload: Send + Sync + 'static;

    /// This function returns the next event from the connection
    /// and waits for it as necessary. It's important to note that not
    /// every received incoming event must get returned from this method.
    /// Implementations could decide to process internally some types of
    /// incoming messages.
    ///
    /// This function returns [`None`] to signal a disconnection from Astarte.
    fn next_event(
        &mut self,
    ) -> impl Future<Output = Result<Option<ReceivedEvent<Self::Payload>>, TransportError>> + Send;

    /// Deserializes a received payload to an property.
    fn deserialize_property(
        &self,
        mapping: &MappingRef<'_, Properties>,
        payload: Self::Payload,
    ) -> Result<Option<AstarteData>, TransportError>;

    /// Deserializes a received payload to an individual astarte value
    fn deserialize_individual(
        &self,
        mapping: &MappingRef<'_, DatastreamIndividual>,
        payload: Self::Payload,
    ) -> Result<(AstarteData, Option<Timestamp>), TransportError>;

    /// Deserializes a received payload to an aggregate object
    fn deserialize_object(
        &self,
        object: &DatastreamObject,
        path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(AstarteObject, Option<Timestamp>), TransportError>;
}

/// Reconnect the device to Astarte.
pub(crate) trait Reconnect {
    /// Function called by [`DeviceConnection`](crate::connection::DeviceConnection) when the
    /// [`Receive::next_event`] returns [`None`].
    ///
    /// It tries to reconnect once, if it succeed it will return true, otherwise it will return
    /// false.
    fn reconnect(
        &mut self,
        interfaces: &Interfaces,
    ) -> impl Future<Output = Result<bool, crate::Error>> + Send;
}

pub(crate) trait Register {
    /// Called when an interface gets added to the device interface list.
    /// This method should convey to the server that a new interface got added.
    fn add_interface(
        &mut self,
        interfaces: &Interfaces,
        added_interface: &interfaces::Validated,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Called when multiple interfaces are added.
    ///
    /// This method should convey to the server that one or more interfaces have been added.
    ///
    /// The added interfaces are still not present in the [`Interfaces`]
    fn extend_interfaces(
        &mut self,
        interfaces: &Interfaces,
        added_interface: &interfaces::ValidatedCollection,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Called when an interface gets removed from the device interface list.
    /// It relays to the server the removal of the interface.
    fn remove_interface(
        &mut self,
        interfaces: &Interfaces,
        removed_interface: &Interface,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Called when multiple interfaces get removed from the device interface list.
    /// It relays to the server the removal of the interface.
    fn remove_interfaces(
        &mut self,
        interfaces: &Interfaces,
        removed_interfaces: &HashMap<&str, &Interface>,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;
}

/// Gracefully close the connection.
pub(crate) trait Disconnect {
    /// Gracefully disconnect from the transport
    fn disconnect(&mut self) -> impl Future<Output = Result<(), crate::Error>> + Send;
}
