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

use tracing::{debug, trace};

use crate::builder::DEFAULT_CHANNEL_SIZE;
use crate::retention::memory::{ItemValue, VolatileStore};
use crate::retention::{RetentionId, StoredRetention, StoredRetentionExt};
use crate::state::ConnectionStatus;
use crate::store::wrapper::StoreWrapper;
use crate::store::StoreCapabilities;
use crate::transport::{Connection, Publish};
use crate::Error;

use super::DeviceConnection;

impl<C> DeviceConnection<C>
where
    C: Connection,
{
    /// This function is called once at the start to send all the stored packet.
    pub(super) async fn init_stored_retention(&mut self) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let Some(retention) = self.store.get_retention() else {
            return Ok(());
        };

        let interfaces = self.state.interfaces.read().await;

        retention.cleanup_introspection(&interfaces).await?;

        retention.reset_all_publishes().await?;

        Self::resend_stored_publishes(&mut self.store, &mut self.sender, &self.state.status)
            .await?;

        Ok(())
    }

    pub(super) async fn resend_volatile_publishes(
        volatile_store: &VolatileStore,
        sender: &mut C::Sender,
        status: &ConnectionStatus,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        while let Some(item) = volatile_store.pop_next().await {
            match item {
                ItemValue::Individual(individual) => {
                    sender.send_individual(individual).await?;
                }
                ItemValue::Object(object) => {
                    sender.send_object(object).await?;
                }
            }

            // Let's check if we are still connected after the await
            if !status.is_connected() {
                debug!("disconnected");

                break;
            }
        }

        Ok(())
    }

    pub(super) async fn resend_stored_publishes(
        store: &mut StoreWrapper<C::Store>,
        sender: &mut C::Sender,
        status: &ConnectionStatus,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let Some(retention) = store.get_retention() else {
            return Ok(());
        };

        let mut buf = Vec::new();

        debug!("start sending store publishes");
        loop {
            let count = retention
                .unsent_publishes(DEFAULT_CHANNEL_SIZE, &mut buf)
                .await?;

            trace!("loaded {count} stored publishes");

            for (id, info) in buf.drain(..) {
                sender.resend_stored(RetentionId::Stored(id), info).await?;

                // Let's check if we are still connected after the await
                if !status.is_connected() {
                    debug!("disconnected");

                    break;
                }
            }

            if count == 0 || count < DEFAULT_CHANNEL_SIZE {
                trace!("all stored publishes sent");

                break;
            }

            buf.clear();
        }

        Ok(())
    }
}
