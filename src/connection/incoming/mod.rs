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

use std::ops::ControlFlow;

use tracing::{debug, info, instrument};

use crate::DeviceEvent;
use crate::error::AstarteError;
use crate::retry::{RetryAction, RetryFuture};
use crate::state::{ConnStatus, ConnectionState};
use crate::store::StoreCapabilities;
use crate::transport::Transport;

use self::ctx::ConnectionCtx;

pub(crate) mod ctx;

pub(crate) struct ReceiverTask<C, S> {
    pub(crate) connection: C,
    pub(crate) state: ConnectionState<S>,
    pub(crate) status_rx: tokio::sync::watch::Receiver<ConnStatus>,
    pub(crate) events: async_channel::Sender<DeviceEvent>,
    /// First connection flags.
    ///
    /// Used to optimize the connection when we just called the registration.
    pub(crate) first: bool,
}

impl<C, S> ReceiverTask<C, S> {
    fn should_exit(&mut self) -> bool {
        self.status_rx.borrow_and_update().should_exit()
    }

    #[instrument(skip(self))]
    pub(crate) async fn receiver(&mut self) -> Result<(), AstarteError>
    where
        C: Transport,
        S: StoreCapabilities,
    {
        'conn: while !self.should_exit() {
            debug!("receiver connecting");

            let mut backoff = RetryFuture {
                rx: &mut self.status_rx,
                state: &self.state,
            };

            let mut connect = ConnectAction {
                first: &mut self.first,
                state: &self.state,
                connection: &mut self.connection,
            };

            let Some(session_present) = backoff.retry(&mut connect).await? else {
                break 'conn;
            };

            self.state
                .set_connection(ConnStatus::Connected { session_present });

            let ctx = ConnectionCtx {
                state: &self.state,
                events: &self.events,
            };

            match backoff.or_closed(self.connection.poll(&ctx)).await {
                Some(Ok(())) => {
                    debug!("receiver disconnected");
                }
                Some(Err(err)) => {
                    self.state.close_connection();

                    return Err(err);
                }
                None => {
                    break;
                }
            };

            // We do not cancel the poll and let it disconnect by itself
            if self.should_exit() {
                break;
            }

            self.state.set_connection(ConnStatus::Offline);

            // Wait before retrying
            let mut retry = RetryFuture {
                rx: &mut self.status_rx,
                state: &self.state,
            };

            if retry.backoff().await.is_none() {
                self.state.close_connection();

                break;
            }
        }

        self.state.close_connection();

        info!("disconnected receiver exiting");

        Ok(())
    }
}

struct ConnectAction<'a, C, S> {
    first: &'a mut bool,
    state: &'a ConnectionState<S>,
    connection: &'a mut C,
}

impl<'a, C, S> RetryAction for ConnectAction<'a, C, S>
where
    C: Transport,
    S: StoreCapabilities,
{
    type Out = bool;

    type Err = AstarteError;

    async fn make(&mut self) -> Result<ControlFlow<Self::Out>, Self::Err> {
        let first = std::mem::replace(self.first, false);

        let interfaces = self.state.interfaces().read().await;

        self.connection
            .connect(self.state, &interfaces, first)
            .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ops::ControlFlow;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;

    use astarte_interfaces::Interface;

    use crate::builder::DEFAULT_CHANNEL_SIZE;
    use crate::interfaces::Interfaces;
    use crate::state::ConnStatus;
    use crate::state::tests::mock_state;
    use mockall::{Sequence, predicate};

    use crate::store::memory::MemoryStore;
    use crate::transport::mock::MockCon;

    use super::*;

    pub(crate) fn mock_receiver_task<S>(
        store: S,
        interfaces: &[&str],
        initial_status: ConnStatus,
    ) -> (
        ReceiverTask<MockCon, S>,
        async_channel::Receiver<DeviceEvent>,
    ) {
        let (status_tx, status_rx) = tokio::sync::watch::channel(initial_status);
        let (events_tx, events_rx) = async_channel::bounded(DEFAULT_CHANNEL_SIZE.get());

        let interfaces =
            Interfaces::from_iter(interfaces.iter().map(|i| Interface::from_str(i).unwrap()));
        let state = ConnectionState::new(Arc::new(mock_state(store, status_tx, interfaces)));

        (
            ReceiverTask {
                status_rx,
                state,
                connection: MockCon::new(),
                events: events_tx,
                first: true,
            },
            events_rx,
        )
    }

    #[tokio::test]
    async fn poll_and_disconnect() {
        let (mut this, _events) = mock_receiver_task(MemoryStore::new(), &[], ConnStatus::Offline);

        let state = this.state.clone();

        let mut seq = Sequence::new();

        this.connection
            .expect_connect_call::<MemoryStore>()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::always(),
                predicate::always(),
                predicate::eq(true),
            )
            .returning(|_, _, _| Ok(ControlFlow::Break(true)));

        this.connection
            .expect_poll_call()
            .once()
            .in_sequence(&mut seq)
            .returning({
                let state = state.clone();
                move || {
                    state.set_connection(ConnStatus::Closed);
                    Ok(())
                }
            });

        tokio::time::timeout(Duration::from_secs(2), this.receiver())
            .await
            .unwrap()
            .unwrap();
    }
}
