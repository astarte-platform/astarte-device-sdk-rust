// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

        // Wait before trying to reconnect the first time, this will prevent cases where the
        // connection will loop that will keep throwing errors.
        //
        // The back-off will start with a wait time of 0s and increase exponentially until it
        // reaches a max. We also keep track of when the last disconnection happened, to reset the
        // wait time only if the connection has been stable for a certain duration of time.
        //
        // An example of an error loop is when the MQTT connection Interfaces (the device
        // introspection) contains interfaces not installed on Astarte:
        //
        // - Device: publishes on that interface
        // - Astarte: disconnects us since we don't have that interface in the introspection
        // - Device: reconnects and succeeds on the first try (the while loop bellow)
        // - Device: publishes on the same interface and gets disconnected again.
        //
        // If we didn't keep track of the last disconnection, the error loop above would continue to
        // happen without timeouts, wasting device bandwidth and resources.
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use astarte_interfaces::MappingPath;
    use mockall::{predicate, Sequence};
    use tempfile::TempDir;

    use crate::connection::tests::{mock_connection, mock_connection_with_store};
    use crate::retention::{PublishInfo, RetentionId, StoredRetentionExt};
    use crate::store::{SqliteStore, StoreCapabilities};
    use crate::test::{STORED_DEVICE_DATASTREAM, STORED_DEVICE_DATASTREAM_NAME};
    use crate::transport::mock::MockSender;
    use crate::validate::ValidatedIndividual;
    use crate::AstarteData;

    #[tokio::test]
    async fn reconnect_success_no_data() {
        let (mut connection, _rx) = mock_connection(&[]);

        let mut seq = Sequence::new();

        connection
            .connection
            .expect_reconnect()
            .with(predicate::always())
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Ok(true));

        connection
            .sender
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockSender::new);

        connection.reconnect_and_resend().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_init_stored_retention_simple() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::connect(tmp.path()).await.unwrap();

        let (mut connection, _rx) = mock_connection_with_store(&[STORED_DEVICE_DATASTREAM], store);

        let retention_id = connection.state.retention_ctx.next();
        let path = "/endpoint1";
        let value = AstarteData::LongInteger(42);
        let bytes = [4, 2];

        let individual = {
            let mapping_path = MappingPath::try_from(path).unwrap();
            let interfaces = connection.state.interfaces.read().await;
            let mapping = interfaces
                .get_individual(STORED_DEVICE_DATASTREAM_NAME, &mapping_path)
                .unwrap();

            ValidatedIndividual::validate(mapping, value.clone(), None).unwrap()
        };

        connection
            .store
            .get_retention()
            .unwrap()
            .store_publish_individual(&retention_id, &individual, &bytes)
            .await
            .unwrap();

        connection
            .store
            .get_retention()
            .unwrap()
            .mark_as_sent(&retention_id)
            .await
            .unwrap();

        let mut seq = Sequence::new();

        connection
            .sender
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut sender = MockSender::new();
                let mut seq = Sequence::new();

                let individual = individual.clone();

                sender
                    .expect_resend_stored()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(move |id, info| {
                        let publish_info = PublishInfo::from_individual(false, &individual, &bytes);

                        *id == RetentionId::Stored(retention_id) && publish_info == *info
                    })
                    .returning(|_, _| Ok(()));

                sender
            });

        connection.init_stored_retention().await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), connection.resend.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}
