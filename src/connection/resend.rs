// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

use std::num::NonZero;
use std::ops::ControlFlow;
use std::time::Duration;

use astarte_interfaces::schema::Ownership;
use tracing::{debug, error, info, trace};

use crate::Error;
use crate::error::Report;
use crate::retention::memory::ItemValue;
use crate::retention::{
    RetentionId, StoredRetention, StoredRetentionExt, stored_mark_unsent, volatile_mark_unsent,
};
use crate::state::{ConnStatus, ConnectionState};
use crate::store::wrapper::StoreWrapper;
use crate::store::{PropertyMapping, PropertyState, PropertyStore, StoreCapabilities};
use crate::transport::{AttemptStatus, Connection, Publish, Receive, Reconnect};

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
            let interfaces = self.state.interfaces().read().await;

            retention.cleanup_introspection(&interfaces).await?;
        }

        self.resend_retention(false).await;

        Ok(())
    }

    /// Reconnect the connection and resends all retention publishes
    pub(crate) async fn reconnect_and_resend(&mut self) -> Result<ControlFlow<()>, Error>
    where
        C: Reconnect + Receive,
        C::Sender: Publish + 'static,
    {
        self.cancel_prev_resend().await;

        if self.reconnect().await?.is_break() {
            return Ok(ControlFlow::Break(()));
        }

        self.resend_retention(true).await;

        Ok(ControlFlow::Continue(()))
    }

    /// Send all the publishes from another task to not block the event loop.
    async fn resend_retention(&mut self, volatile: bool)
    where
        C::Sender: Publish + 'static,
    {
        let state = self.state.clone();
        let mut sender = self.sender.clone();
        let mut store = self.store.clone();
        let limit = self.state.config().channel_size;

        // The queue of unpublish packets could grow indefinitely, but the should all be sent at the
        // end.
        //
        // NOTE: This is only needed because the MQTT library uses a managed channel to send the to
        //       the event loop while polling, and it's not possible to send the data directly
        //       without also receiving. This is a big limitation of the current library.
        self.resend = Some(tokio::task::spawn(async move {
            let _interfaces = state.interfaces().read().await;

            let mut remaining_data = true;

            while remaining_data {
                remaining_data = false;

                if volatile {
                    match Self::resend_volatile_publishes(&mut sender, &state, limit).await {
                        Ok(sent) => remaining_data |= sent >= limit.get(),
                        Err(err) => {
                            error!(error = %Report::new(&err), "error sending volatile retention");
                            // in case of errors while sending we still exit the loop
                            break;
                        }
                    }
                }

                match Self::resend_stored_publishes(&mut store, &mut sender, limit).await {
                    Ok(sent) => remaining_data |= sent >= limit.get(),
                    Err(err) => {
                        error!(error = %Report::new(&err), "error sending stored retention");
                        // in case of errors while sending we still exit the loop
                        break;
                    }
                }

                match Self::send_device_properties(&mut store, &mut sender, limit.get()).await {
                    Ok(sent) => remaining_data |= sent >= limit.get(),
                    Err(err) => {
                        error!(error = %Report::new(&err), "error sending device properties");

                        break;
                    }
                }
            }

            // after everything got resent we are connected
            state.set_connection(ConnStatus::Connected).await;
        }));
    }

    /// Sends the device owned properties even the null values.
    /// This ignores the purge properties that should be sent by the connection implementation.
    /// Since the purge properties we sent earlier new properties could have gotten unset.
    async fn send_device_properties(
        store: &mut StoreWrapper<C::Store>,
        sender: &mut C::Sender,
        limit: usize,
    ) -> Result<usize, Error>
    where
        C::Sender: Publish,
    {
        let device_properties = store
            .device_props_with_unset(PropertyState::Changed, limit, 0)
            .await?;
        let count = device_properties.len();

        debug!("fetched {count} properties (limit: {limit})");

        for prop in device_properties {
            debug!(
                "sending device-owned property = {}{}",
                prop.interface, prop.path
            );

            // Don't wait for the ack since it's not fundamental for the connection
            sender.resend_stored_property(prop.clone()).await?;

            let property_mapping = PropertyMapping::from(&prop);

            if prop.value.is_some() {
                let updated = store
                    .update_state(
                        &property_mapping,
                        PropertyState::Completed,
                        prop.value.clone(),
                    )
                    .await?;

                debug!(?updated, "updated state");
            } else {
                let updated = store.delete_expected_prop(&property_mapping, None).await?;

                debug!(?updated, "deleted expected prop");
            }
        }

        Ok(count)
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
        state: &ConnectionState,
        limit: NonZero<usize>,
    ) -> Result<usize, Error>
    where
        C::Sender: Publish,
    {
        let mut buf = Vec::new();

        let count = state
            .volatile_store()
            .get_unsent(&mut buf, limit.get())
            .await;

        trace!("loaded {count} volatile publishes");

        for (id, value) in buf.drain(..) {
            // mark as sent before so that no resend is tried while in flight
            state.volatile_store().mark_sent(&id, true).await;

            let result = match value {
                ItemValue::Individual(individual) => {
                    sender
                        .send_individual_stored(RetentionId::Volatile(id), individual)
                        .await
                }
                ItemValue::Object(object) => {
                    sender
                        .send_object_stored(RetentionId::Volatile(id), object)
                        .await
                }
            };

            if let Err(e) = result {
                error!(error=%Report::new(&e), "error while sending volatile marking unsent");
                volatile_mark_unsent(state.volatile_store(), &id).await;
                return Err(e);
            }
        }

        Ok(count)
    }

    async fn resend_stored_publishes(
        store: &mut StoreWrapper<C::Store>,
        sender: &mut C::Sender,
        limit: NonZero<usize>,
    ) -> Result<usize, Error>
    where
        C::Sender: Publish,
    {
        let Some(retention) = store.get_retention() else {
            return Ok(0);
        };

        let mut buf = Vec::new();

        debug!("start sending store publishes");

        let count = retention.unsent_publishes(limit.get(), &mut buf).await?;

        trace!("loaded {count} stored publishes");

        for (id, info) in buf.drain(..) {
            // mark as sent before so that no resend is tried while in flight
            retention.update_sent_flag(&id, true).await?;
            let result = sender.resend_stored(RetentionId::Stored(id), info).await;

            if let Err(e) = result {
                error!(error=%Report::new(&e), "error while sending stored marking unsent");
                stored_mark_unsent(store, &id).await;
                return Err(e);
            }
        }

        Ok(count)
    }

    async fn reconnect(&mut self) -> Result<ControlFlow<()>, Error>
    where
        C: Reconnect,
    {
        let interfaces = self.state.interfaces().read().await;

        info!("reconnecting");

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

        if self.wait_timeout(timeout).await.is_break() {
            return Ok(ControlFlow::Break(()));
        }

        let session;

        loop {
            let attempt_status = self.connection.reconnect(&interfaces).await?;

            match attempt_status {
                AttemptStatus::Connected { session_present } => {
                    session = session_present;
                    break;
                }
                AttemptStatus::Disconnected => {
                    let timeout = self.backoff.next();

                    if self.wait_timeout(timeout).await.is_break() {
                        return Ok(ControlFlow::Break(()));
                    }
                }
            }
        }

        // if we are connected but the session is not present we have to cleanup the retention data
        if !session {
            // when the session is not present we reset the sent flags for stored messages
            info!("the session is not present after reconnection we will resend the packets");

            if let Some(retention) = self.store.get_retention() {
                retention.reset_all_publishes().await?;
            }

            self.state.volatile_store().reset_sent().await;

            self.store.reset_state(Ownership::Device).await?;
        }

        Ok(ControlFlow::Continue(()))
    }

    async fn wait_timeout(&self, timeout: Duration) -> ControlFlow<()> {
        debug!(seconds = timeout.as_secs(), "waiting before retrying");

        let is_disconnected =
            Self::run_until_disconnect(&self.disconnect, tokio::time::sleep(timeout))
                .await
                .is_none();

        if is_disconnected {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;
    use std::time::Duration;

    use astarte_interfaces::MappingPath;
    use futures::FutureExt;
    use mockall::{Sequence, predicate};
    use tempfile::TempDir;

    use crate::AstarteData;
    use crate::connection::tests::{mock_connection, mock_connection_with_store};
    use crate::retention::{PublishInfo, RetentionId, StoredRetention, StoredRetentionExt};
    use crate::state::ConnStatus;
    use crate::store::{SqliteStore, StoreCapabilities};
    use crate::test::{STORED_DEVICE_DATASTREAM, STORED_DEVICE_DATASTREAM_NAME};
    use crate::transport::mock::MockSender;
    use crate::validate::ValidatedIndividual;

    #[tokio::test]
    async fn reconnect_success_no_data() {
        let mut connection = mock_connection(&[], ConnStatus::Connected);

        let mut seq = Sequence::new();

        connection
            .connection
            .expect_reconnect()
            .with(predicate::always())
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                futures::future::ok(crate::transport::AttemptStatus::Connected {
                    session_present: true,
                })
                .boxed()
            });

        connection
            .sender
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockSender::new);

        let control = connection.reconnect_and_resend().await.unwrap();

        assert_eq!(control, ControlFlow::Continue(()));
    }

    #[tokio::test]
    async fn sqlite_init_stored_retention_simple() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(tmp.path())
            .await
            .unwrap();

        let mut connection =
            mock_connection_with_store(&[STORED_DEVICE_DATASTREAM], ConnStatus::Connected, store);

        let retention_id = connection.state.retention_ctx().next();
        let path = "/endpoint1";
        let value = AstarteData::LongInteger(42);
        let bytes = [4, 2];

        let individual = {
            let mapping_path = MappingPath::try_from(path).unwrap();
            let interfaces = connection.state.interfaces().read().await;
            let mapping = interfaces
                .get_individual(STORED_DEVICE_DATASTREAM_NAME, &mapping_path)
                .unwrap();

            ValidatedIndividual::validate(mapping, value.clone(), None).unwrap()
        };

        connection
            .store
            .get_retention()
            .unwrap()
            .store_publish_individual(&retention_id, &individual, &bytes, true)
            .await
            .unwrap();

        connection
            .store
            .get_retention()
            .unwrap()
            .update_sent_flag(&retention_id, true)
            .await
            .unwrap();

        // NOTE the reset_all_publishes is called BEFORE the init retention
        connection
            .store
            .get_retention()
            .unwrap()
            .reset_all_publishes()
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
