/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

//! # Astarte Connection Traits
//!
//! This module defines traits and structures for handling communication and interaction
//! with the Astarte.
//!
//! The module includes traits for publishing and receiving Astarte data over a connection,
//! as well as registering and managing interfaces on a device.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
    },
    interfaces::{self, Interfaces},
    shared::SharedDevice,
    store::PropertyStore,
    types::AstarteType,
    validate::{ValidatedIndividual, ValidatedObject},
    Interface, Timestamp,
};

#[cfg(feature = "message-hub")]
pub mod grpc;
pub mod mqtt;

/// Holds generic event data such as interface name and path
/// The payload must be deserialized after verification with the
/// specific [`Connection::deserialize_individual`] or [`Connection::serialize_individual`]
#[derive(Debug, Clone)]
pub(crate) struct ReceivedEvent<P> {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) payload: P,
}

#[async_trait]
pub(crate) trait Publish {
    /// Sends validated individual values over this connection
    async fn send_individual(&self, data: ValidatedIndividual<'_>) -> Result<(), crate::Error>;

    /// Sends validated objects values over this connection
    async fn send_object(&self, data: ValidatedObject<'_>) -> Result<(), crate::Error>;
}

#[async_trait]
pub(crate) trait Receive {
    type Payload: Send + Sync + 'static;

    /// This function returns the next event from the connection
    /// and waits for it as necessary. It's important to note that not
    /// every received incoming event must get returned from this method.
    /// Implementations could decide to process internally some types of
    /// incoming messages.
    async fn next_event<S>(
        &self,
        device: &SharedDevice<S>,
    ) -> Result<ReceivedEvent<Self::Payload>, crate::Error>
    where
        S: PropertyStore;

    /// Deserializes a received payload to an individual astarte value
    fn deserialize_individual(
        &self,
        mapping: MappingRef<'_, &Interface>,
        payload: Self::Payload,
    ) -> Result<(AstarteType, Option<Timestamp>), crate::Error>;

    /// Deserializes a received payload to an aggregate object
    fn deserialize_object(
        &self,
        object: ObjectRef,
        path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), crate::Error>;
}

#[async_trait]
pub(crate) trait Register {
    /// Called when an interface gets added to the device interface list.
    /// This method should convey to the server that a new interface got added.
    async fn add_interface(
        &self,
        interfaces: &Interfaces,
        added_interface: &interfaces::Validated,
    ) -> Result<(), crate::Error>;

    /// Called when an interface gets removed from the device interface list.
    /// It relays to the server the removal of the interface.
    async fn remove_interface(
        &self,
        interfaces: &Interfaces,
        removed_interface: &Interface,
    ) -> Result<(), crate::Error>;

    /// Called when multiple interfaces are added.
    ///
    /// This method should convey to the server that one or more interfaces have been added.
    ///
    /// The added interfaces are still not present in the [`Interfaces`]
    async fn extend_interfaces(
        &self,
        interfaces: &Interfaces,
        added_interface: &interfaces::ValidatedCollection,
    ) -> Result<(), crate::Error>;
}

#[async_trait]
pub trait Disconnect {
    /// User callable api to gracefully disconnect from the transport
    async fn disconnect(self) -> Result<(), crate::Error>;
}

#[cfg(test)]
mod test {
    use tokio::sync::RwLock;

    use crate::{
        interface::{mapping::path::MappingPath, reference::MappingRef},
        interfaces::Interfaces,
        shared::SharedDevice,
        store::memory::MemoryStore,
        types::{AstarteType, TypeError},
        validate::{ValidatedIndividual, ValidatedObject},
        AstarteAggregate, EventSender, Interface,
    };

    pub(crate) fn mock_shared_device(
        interfaces: Interfaces,
        tx: EventSender,
    ) -> SharedDevice<MemoryStore> {
        SharedDevice {
            interfaces: RwLock::new(interfaces),
            store: crate::store::wrapper::StoreWrapper::new(MemoryStore::new()),
            tx,
        }
    }

    pub(crate) fn mock_validate_object<'a, D>(
        interface: &'a Interface,
        path: &'a MappingPath<'a>,
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<ValidatedObject<'a>, crate::Error>
    where
        D: AstarteAggregate + Send,
    {
        let object = interface
            .as_object_ref()
            .ok_or_else(|| crate::Error::Aggregation {
                exp: crate::interface::Aggregation::Object,
                got: interface.aggregation(),
            })?;

        let aggregate = data.astarte_aggregate()?;

        ValidatedObject::validate(object, path, aggregate, timestamp).map_err(|uve| uve.into())
    }

    pub(crate) fn mock_validate_individual<'a, D>(
        mapping_ref: MappingRef<'a, &'a Interface>,
        path: &'a MappingPath<'a>,
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<ValidatedIndividual<'a>, crate::Error>
    where
        D: TryInto<AstarteType> + Send,
    {
        let individual = data.try_into().map_err(|_| TypeError::Conversion)?;

        ValidatedIndividual::validate(mapping_ref, path, individual, timestamp)
            .map_err(|uve| uve.into())
    }
}
