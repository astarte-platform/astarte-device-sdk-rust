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

//! Handles the sending of individual datastream.

use tracing::{debug, trace, warn};

use crate::client::ValidatedIndividual;
use crate::interface::mapping::path::MappingPath;
use crate::interface::Retention;
use crate::state::SharedState;
use crate::store::StoreCapabilities;
use crate::transport::Connection;
use crate::{AstarteType, Error};

use super::{DeviceClient, Publish, RetentionId, StoreWrapper, StoredRetentionExt, Timestamp};

impl<C> DeviceClient<C>
where
    C: Connection,
{
    pub(crate) async fn send_datastream_individual(
        &mut self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: AstarteType,
        timestamp: Option<Timestamp>,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let interfaces = self.state.interfaces.read().await;
        let mapping = interfaces.interface_mapping(interface_name, path)?;

        let validated = ValidatedIndividual::validate(mapping, data, timestamp)?;

        debug!("sending individual type {}", validated.data.display_type());

        if !self.state.status.is_connected() {
            trace!("publish individual while connection is offline");

            return Self::offline_send_individual(
                &self.state,
                &self.store,
                &mut self.sender,
                validated,
            )
            .await;
        }

        match mapping.retention() {
            Retention::Volatile { .. } => {
                Self::send_volatile_individual(&self.state, &mut self.sender, validated).await
            }
            Retention::Stored { .. } => {
                Self::send_stored_individual(&self.state, &self.store, &mut self.sender, validated)
                    .await
            }
            Retention::Discard => self.sender.send_individual(validated).await,
        }
    }

    async fn offline_send_individual(
        state: &SharedState,
        store: &StoreWrapper<C::Store>,
        sender: &mut C::Sender,
        data: ValidatedIndividual,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        match data.retention {
            Retention::Discard => {
                debug!("drop publish with retention discard since disconnected");
            }
            Retention::Volatile { .. } => {
                let id = state.retention_ctx.next();

                state.volatile_store.push(id, data).await;
            }
            Retention::Stored { .. } => {
                let id = state.retention_ctx.next();
                if let Some(retention) = store.get_retention() {
                    let value = sender.serialize_individual(&data)?;

                    retention
                        .store_publish_individual(&id, &data, &value)
                        .await?;
                } else {
                    warn!("storing interface with retention stored in volatile since the store doesn't support retention");

                    state.volatile_store.push(id, data).await;
                }
            }
        }

        Ok(())
    }

    async fn send_volatile_individual(
        state: &SharedState,
        sender: &mut C::Sender,
        data: ValidatedIndividual,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let id = state.retention_ctx.next();

        state.volatile_store.push(id, data.clone()).await;

        sender
            .send_individual_stored(RetentionId::Volatile(id), data)
            .await
    }

    async fn send_stored_individual(
        state: &SharedState,
        store: &StoreWrapper<C::Store>,
        sender: &mut C::Sender,
        data: ValidatedIndividual,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let Some(retention) = store.get_retention() else {
            warn!("not storing interface with retention stored since the store doesn't support retention");

            return sender.send_individual(data).await;
        };

        let value = sender.serialize_individual(&data)?;

        let id = state.retention_ctx.next();

        retention
            .store_publish_individual(&id, &data, &value)
            .await?;

        sender
            .send_individual_stored(RetentionId::Stored(id), data)
            .await
    }
}
