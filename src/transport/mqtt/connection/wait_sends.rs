// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
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

use std::ops::ControlFlow;

use rumqttc::{ConnectionError, Event, Publish};
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, info};

use crate::error::Report;
use crate::transport::mqtt::connection::handle_event;

use super::ConnError;
use super::context::{Connection, Ctx};

pub(crate) type TaskHandle = AbortOnDropHandle<Result<(), ConnError>>;

/// Waits for all the packets sent in the [`Init`] to be ACK-ed by Astarte.
#[derive(Debug)]
pub(super) struct WaitTask {
    pub(super) connection: Connection,
    pub(super) handle: TaskHandle,
    pub(super) session_present: bool,
}

impl WaitTask {
    /// Waits for the initialization task to complete.
    ///
    /// We check that the task is finished, or we pull the connection. This ensures that all the
    /// packets in are sent correctly and the handle can advance to completion. Eventual published
    /// packets are returned.
    pub(super) async fn wait_connection<S>(
        &mut self,
        ctx: &mut Ctx<'_, S>,
    ) -> Result<ControlFlow<bool, Publish>, ConnError> {
        loop {
            let publish = tokio::select! {
                // Join handle is cancel safe
                res = &mut self.handle => {
                    debug!("task joined");

                    return self.handle_join(ctx, res);
                }
                // I hope this is cancel safe
                res = self.connection.eventloop_mut().poll() => {
                    Self::handle_poll(res)?
                }
            };

            if let Some(publish) = publish {
                return Ok(ControlFlow::Continue(publish));
            }
        }
    }

    fn handle_join<S>(
        &mut self,
        ctx: &mut Ctx<'_, S>,
        res: Result<Result<(), ConnError>, JoinError>,
    ) -> Result<ControlFlow<bool, Publish>, ConnError> {
        // Don't move the handle to await the task
        match res {
            Ok(Ok(())) => {
                info!("device connected");

                // if the device successfully connects we set the sync status to true
                ctx.session_synced = true;
                self.connection.set_clean_session(false);

                Ok(ControlFlow::Break(self.session_present))
            }
            Ok(Err(err)) => {
                error!(error = %Report::new(&err), "init task failed");

                Err(err)
            }
            Err(err) => {
                error!(error = %Report::new(&err), "failed to join init task");

                // We should never panic, but return an error instead. This is probably a test/mock
                // expectation failing.
                debug_assert!(!err.is_panic(), "task panicked while waiting for acks");

                Err(ConnError::JoinError)
            }
        }
    }

    fn handle_poll(res: Result<Event, ConnectionError>) -> Result<Option<Publish>, ConnError> {
        debug!("next event polled");

        match res {
            Ok(event) => handle_event(event),
            Err(err) => Err(ConnError::Connection {
                ctx: "waiting for send task",
                source: err,
            }),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::{Arc, OnceLock};

    use futures::FutureExt;
    use mockall::Sequence;
    use sync_wrapper::SyncWrapper;

    use crate::interfaces::Interfaces;
    use crate::state::SharedState;
    use crate::store::mock::MockStore;
    use crate::store::wrapper::StoreWrapper;
    use crate::transport::mqtt::ClientSender;
    use crate::transport::mqtt::client::{AsyncClient, EventLoop};
    use crate::transport::mqtt::config::transport::TransportProvider;
    use crate::transport::mqtt::test::mock_state;

    use super::*;

    pub(crate) fn mock_wait_task(
        client: AsyncClient,
        eventloop: EventLoop,
        session_present: bool,
    ) -> WaitTask {
        WaitTask {
            connection: Connection {
                client,
                eventloop: SyncWrapper::new(eventloop),
            },
            handle: AbortOnDropHandle::new(tokio::spawn(futures::future::ok(()))),
            session_present,
        }
    }

    /// Used to create and store the vars of the ctx.
    struct ConnectionCtx {
        pub(crate) sender: Arc<OnceLock<ClientSender>>,
        pub(crate) state: SharedState,
        pub(crate) provider: TransportProvider,
        pub(crate) store: StoreWrapper<MockStore>,
        pub(crate) interfaces: Interfaces,
        /// Whether the stored introspection matches the current one
        pub(crate) session_synced: bool,
    }

    impl ConnectionCtx {
        async fn mock() -> Self {
            Self {
                sender: Arc::default(),
                state: mock_state(&[]),
                provider: TransportProvider::configure(None, false).await.unwrap(),
                store: StoreWrapper::new(MockStore::default()),
                interfaces: Interfaces::default(),
                session_synced: true,
            }
        }

        fn ctx(&self) -> Ctx<'_, MockStore> {
            Ctx {
                sender: &self.sender,
                state: &self.state,
                provider: &self.provider,
                store: &self.store,
                interfaces: &self.interfaces,
                session_synced: self.session_synced,
            }
        }
    }

    #[tokio::test]
    async fn wait_connection() {
        let client = AsyncClient::default();
        let mut eventloop = EventLoop::default();

        let mut seq = Sequence::new();
        eventloop
            .expect_poll()
            .once()
            .in_sequence(&mut seq)
            .with()
            .returning(|| futures::future::pending().boxed());

        let mut wait = mock_wait_task(client, eventloop, true);

        let mock = ConnectionCtx::mock().await;
        let mut ctx = mock.ctx();

        let res = wait.wait_connection(&mut ctx).await.unwrap();

        assert_eq!(res, ControlFlow::Break(true));
    }
}
