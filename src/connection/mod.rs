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
use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
    },
    shared::SharedDevice,
    store::PropertyStore,
    types::AstarteType,
    Interface, Timestamp,
};

pub mod mqtt;

/// Holds generic event data such as interface name and path
/// The payload must be deserialized after verification with the
/// specific [`Connection::deserialize_individual`] or [`Connection::serialize_individual`]
pub(crate) struct ReceivedEvent<P> {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) payload: P,
}

#[async_trait]
pub(crate) trait Connection<S>: Clone {
    type Payload: Send + Sync + 'static;

    /// Method called imediately after the Connection gets created
    async fn connect(&self, device: &SharedDevice<S>) -> Result<(), crate::Error>
    where
        S: PropertyStore;

    /// This function returns the next event from the connection
    /// and waits for it as necessary. It's important to note that not
    /// every received incoming event must get returned from this method.
    /// Implementations could decide to process internally some types of
    /// incoming messages.
    async fn next_event(
        &self,
        device: &SharedDevice<S>,
    ) -> Result<ReceivedEvent<Self::Payload>, crate::Error>
    where
        S: PropertyStore;

    /// Deserializes a received payload to an individual astarte value
    fn deserialize_individual(
        &self,
        mappig: MappingRef<'_, &Interface>,
        payload: &Self::Payload,
    ) -> Result<(AstarteType, Option<Timestamp>), crate::Error>;

    /// Deserializes a received payload to an aggregate object
    fn deserialize_object(
        &self,
        object: ObjectRef,
        path: &MappingPath<'_>,
        payload: &Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), crate::Error>;

    /// Sends individual [`AstarteType`] values over this connection
    async fn send_individual(
        &self,
        mapping: MappingRef<'_, &Interface>,
        path: &MappingPath<'_>,
        data: &AstarteType,
        timestamp: Option<Timestamp>,
    ) -> Result<(), crate::Error>
    where
        S: 'static;

    /// Sends objects [`HashMap`] values over this connection
    async fn send_object(
        &self,
        object: ObjectRef<'_>,
        path: &MappingPath<'_>,
        data: &HashMap<String, AstarteType>,
        timestamp: Option<Timestamp>,
    ) -> Result<(), crate::Error>
    where
        S: 'static;
}

#[async_trait]
pub(crate) trait Registry {
    /// Method to start receiving events related to the passed interface
    async fn subscribe(&self, interface: &str) -> Result<(), crate::Error>;

    /// Method that stops the connection from receiving events related to the passed interface
    async fn unsubscribe(&self, interface: &str) -> Result<(), crate::Error>;

    /// Method that sends the introspection [`String`] over the connection
    async fn send_introspection(&self, introspection: String) -> Result<(), crate::Error>;
}
