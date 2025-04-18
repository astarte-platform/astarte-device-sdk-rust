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

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, error, trace};

use crate::builder::DEFAULT_CHANNEL_SIZE;
use crate::error::Report;
use crate::retention::memory::ItemValue;
use crate::retention::{RetentionId, StoredRetention, StoredRetentionExt};
use crate::state::SharedState;
use crate::store::wrapper::StoreWrapper;
use crate::store::StoreCapabilities;
use crate::transport::{Connection, Publish, Receive, Reconnect};
use crate::Error;

use super::DeviceConnection;

impl<C> DeviceConnection<C>
where
    C: Connection,
{
    /// This function is called once at the start to send all the stored packet.
    pub(super) async fn init_stored_retention(&mut self) -> Result<(), Error>
    where
        C::Sender: Publish + 'static,
    {
        let Some(retention) = self.store.get_retention() else {
            return Ok(());
        };

        {
            let interfaces = self.state.interfaces.read().await;

            retention.cleanup_introspection(&interfaces).await?;
        }

        retention.reset_all_publishes().await?;

        self.resend_retention(false).await;

        Ok(())
    }

    /// Reconnect the connection and resends all retention publishes
    pub(crate) async fn reconnect_and_resend(&mut self) -> Result<(), Error>
    where
        C: Reconnect + Receive,
        C::Sender: Publish + 'static,
    {
        self.cancel_prev_resend().await;

        self.reconnect().await?;

        self.resend_retention(true).await;

        Ok(())
    }

    /// Send all the publishes from another task to not block the event loop.
    async fn resend_retention(&mut self, volatile: bool)
    where
        C::Sender: Publish + 'static,
    {
        let state = Arc::clone(&self.state);
        let mut sender = self.sender.clone();
        let mut store = self.store.clone();

        // The queue of unpublish packets could grow indefinitely, but the should all be sent at the
        // end.
        //
        // NOTE: This is only needed because the MQTT library uses a managed channel to send the to
        //       the event loop while polling, and it's not possible to send the data directly
        //       without also receiving. This is a big limitation of the current library.
        self.resend = Some(tokio::task::spawn(async move {
            let _interfaces = state.interfaces.read().await;

            // We exit on errors so the connection task should exit with the error.
            if volatile {
                if let Err(err) = Self::resend_volatile_publishes(&mut sender, &state).await {
                    error!(error = %Report::new(&err), "error sending volatile retention");
                }
            }

            if let Err(err) = Self::resend_stored_publishes(&mut store, &mut sender).await {
                error!(error = %Report::new(&err), "error sending stored retention");
            }
        }));
    }

    /// Check if there is a previous task for the resend of stored publishes and cancels it.
    async fn cancel_prev_resend(&mut self) {
        if let Some(resend) = self.resend.take() {
            debug!("cancel previous resend");

            resend.abort();

            match resend.await {
                Ok(()) => {
                    trace!("task already exited")
                }
                Err(err) if err.is_cancelled() => {
                    debug!("resend task was cancelled");
                }
                Err(err) => {
                    error!(error = %Report::new(err), "resend task paniched");
                }
            }
        }
    }

    async fn resend_volatile_publishes(
        sender: &mut C::Sender,
        state: &SharedState,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        while let Some(item) = state.volatile_store.pop_next().await {
            match item {
                ItemValue::Individual(individual) => {
                    sender.send_individual(individual).await?;
                }
                ItemValue::Object(object) => {
                    sender.send_object(object).await?;
                }
            }
        }

        Ok(())
    }

    async fn resend_stored_publishes(
        store: &mut StoreWrapper<C::Store>,
        sender: &mut C::Sender,
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
            }

            if count == 0 || count < DEFAULT_CHANNEL_SIZE {
                trace!("all stored publishes sent");

                break;
            }

            buf.clear();
        }

        Ok(())
    }

    async fn reconnect(&mut self) -> Result<(), Error>
    where
        C: Reconnect,
    {
        let interfaces = self.state.interfaces.read().await;
        debug!("reconnecting");

        // Wait for the first reconnection to prevent an immediate retry when the reconnection is
        // actually successful immediately (not entering the while loop). An example is not
        // installing the interfaces on Astarte.
        let timeout = self.backoff.next();

        debug!("waiting {timeout} seconds before retrying");

        tokio::time::sleep(Duration::from_secs(timeout)).await;

        while !self.connection.reconnect(&interfaces).await? {
            let timeout = self.backoff.next();

            debug!("waiting {timeout} seconds before retrying");

            tokio::time::sleep(Duration::from_secs(timeout)).await;
        }

        // Now we are reconnected
        self.state.status.set_connected(true);

        Ok(())
    }
}
