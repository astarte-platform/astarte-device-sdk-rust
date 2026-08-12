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

use std::num::NonZero;
use std::ops::ControlFlow;

use astarte_device_error::ResultExt;
use tracing::{debug, info, instrument, trace};

use crate::error::{AstarteError, ErrorKind};
use crate::interfaces::Interfaces;
use crate::retention::memory::ItemValue;
use crate::retention::{RetentionId, StoredRetention};
use crate::retry::RetryAction;
use crate::state::{ConnStatus, ConnectionState};
use crate::store::{PropertyState, StoreCapabilities, UpdatedAt};
use crate::transport::Sender;

pub(crate) struct ResendCtx<'a, C, S> {
    pub(crate) sender: &'a mut C,
    pub(crate) state: &'a ConnectionState<S>,
    pub(crate) interfaces: &'a Interfaces,
}

impl<'a, C, S> ResendCtx<'a, C, S>
where
    C: Sender,
{
    /// Send all the publishes from another task to not block the event loop.
    ///
    /// This needs to be somewhat cancel safe, since on disconnect will by cancelled externally
    #[instrument(skip(self))]
    pub(crate) async fn resend(&mut self) -> Result<ControlFlow<()>, AstarteError>
    where
        S: StoreCapabilities,
    {
        const ONE: NonZero<usize> = NonZero::new(1).unwrap();
        const THIRDS: NonZero<usize> = NonZero::new(3).unwrap();

        trace!("starting resend task");

        // NOTE this should use div_ceil on NonZero but requires MSRV 1.92. This will never be 0
        //      anyway, so the unwrap_or is not needed
        let limit = NonZero::new(
            self.state
                .config()
                .channel_size
                .get()
                .div_ceil(THIRDS.get()),
        )
        .unwrap_or(ONE);

        let mut total_sent = usize::MAX;

        let mut prop_last_updated_at = None;

        // TODO: lock the clients before checking all data has been sent
        while total_sent != 0 {
            total_sent = 0;

            let volatile_sent = self.resend_volatile_publishes(limit).await?;
            total_sent = total_sent.saturating_add(volatile_sent);

            let stored_sent = self.resend_stored_publishes(limit).await?;
            total_sent = total_sent.saturating_add(stored_sent);

            let prop_sent = self
                .send_device_properties(self.interfaces, limit, &mut prop_last_updated_at)
                .await?;
            total_sent = total_sent.saturating_add(prop_sent);

            debug!(total_sent)
        }

        self.state.set_connection(ConnStatus::Online);

        info!("all packet sent");

        Ok(ControlFlow::Break(()))
    }

    /// Sends the device owned properties even the null values.
    /// This ignores the purge properties that should be sent by the connection implementation.
    /// Since the purge properties we sent earlier new properties could have gotten unset.
    async fn send_device_properties(
        &mut self,
        interfaces: &Interfaces,
        limit: NonZero<usize>,
        last_updated_at: &mut Option<UpdatedAt>,
    ) -> Result<usize, AstarteError>
    where
        S: StoreCapabilities,
    {
        let device_properties = self
            .state
            .store()
            .device_props_with_unset(PropertyState::Changed, limit, *last_updated_at)
            .await
            .map_kind(ErrorKind::Store)?;

        let count = device_properties.len();

        debug!(count, limit, "fetched properties");

        for prop in device_properties {
            *last_updated_at = Some(prop.updated_at());

            if !interfaces.has_property(&prop.interface, prop.interface_major()) {
                debug!(
                    interface = prop.interface,
                    path = prop.path,
                    "skipping property not in introspection",
                );

                continue;
            }

            debug!(
                interface = prop.interface,
                path = prop.path,
                "sending device-owned property",
            );

            // Don't wait for the ack since it's not fundamental for the connection
            self.sender.resend_stored_property(self.state, prop).await?;
        }

        Ok(count)
    }

    async fn resend_volatile_publishes(
        &mut self,
        limit: NonZero<usize>,
    ) -> Result<usize, AstarteError>
    where
        S: StoreCapabilities,
    {
        let mut buf = Vec::new();

        let count = self
            .state
            .volatile_store()
            .get_unsent(&mut buf, limit.get())
            .await;

        trace!("loaded {count} volatile publishes");

        for (id, value) in buf {
            match value {
                ItemValue::Individual(individual) => {
                    self.sender
                        .send_individual_stored(self.state, RetentionId::Volatile(id), individual)
                        .await?;
                }
                ItemValue::Object(object) => {
                    self.sender
                        .send_object_stored(self.state, RetentionId::Volatile(id), object)
                        .await?;
                }
            };
        }

        Ok(count)
    }

    async fn resend_stored_publishes(
        &mut self,
        limit: NonZero<usize>,
    ) -> Result<usize, AstarteError>
    where
        S: StoreCapabilities,
    {
        let Some(retention) = self.state.store().get_retention() else {
            return Ok(0);
        };

        let mut buf = Vec::new();

        debug!("start sending store publishes");

        let count = retention
            .unsent_publishes(limit.get(), &mut buf)
            .await
            .map_kind(ErrorKind::Retention)?;

        trace!("loaded {count} stored publishes");

        for (id, info) in buf.drain(..) {
            self.sender
                .resend_stored(self.state, RetentionId::Stored(id), info)
                .await?;
        }

        Ok(count)
    }
}

impl<'a, C, S> RetryAction for ResendCtx<'a, C, S>
where
    C: Sender,
    S: StoreCapabilities,
{
    type Out = ();
    type Err = AstarteError;

    async fn make(&mut self) -> Result<ControlFlow<Self::Out>, Self::Err> {
        self.resend().await
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::outgoing::tests::mock_sender_task;

    use crate::state::ConnStatus;
    use crate::store::memory::MemoryStore;

    use super::*;

    #[tokio::test]
    async fn resend_success_no_data() {
        let (mut this, _client_tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let interfaces = this.state.interfaces().read().await;

        let mut ctx = ResendCtx {
            sender: &mut this.sender,
            state: &this.state,
            interfaces: &interfaces,
        };

        let res = ctx.resend().await.unwrap();

        assert_eq!(res, ControlFlow::Break(()));
    }
}
