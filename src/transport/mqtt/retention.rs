// This file is part of Astarte.
//
// Copyright 2024, 2026 SECO Mind Srl
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

//! Stored interface retention.
//!
//! When available it will use the SQLite database to store the interface retention to disk, so that
//! the data is guarantied to be delivered in the time-frame specified by the expiry even after
//! shutdowns or reboots.
//!
//! When an interface major version is updated the retention cache must be invalidated. Since the
//! payload will be publish on the new introspection.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use astarte_device_error::Error;
use rumqttc::{AckOfPub, Token};
use tokio::task::JoinHandle;
use tokio_util::either::Either;
use tracing::{debug, error, info, instrument, trace};

use crate::retention::{RetentionId, StoredRetention};
use crate::state::{ConnStatus, ConnectionState};
use crate::store::StoreCapabilities;

use super::error::MqttError;

type Item = (RetentionId, Token<AckOfPub>);
pub(crate) type RetSender = tokio::sync::mpsc::Sender<Item>;
pub(crate) type RetReceiver = tokio::sync::mpsc::Receiver<Item>;

pub(crate) struct RetentionTask<S> {
    state: ConnectionState<S>,
    packets: HashMap<RetentionId, Token<AckOfPub>>,
    rx: RetReceiver,
    status_rx: tokio::sync::watch::Receiver<ConnStatus>,
}

impl<S> RetentionTask<S> {
    pub(crate) fn spawn(
        state: ConnectionState<S>,
        rx: RetReceiver,
    ) -> JoinHandle<Result<(), Error<MqttError>>>
    where
        S: StoreCapabilities,
    {
        let status_rx = state.subscribe_connection();

        let mut this = Self {
            state,
            packets: HashMap::new(),
            rx,
            status_rx,
        };

        tokio::spawn(async move { this.handle_events().await })
    }

    pub(crate) fn queue(&mut self, id: RetentionId, token: Token<AckOfPub>) {
        let old = self.packets.insert(id, token);

        debug_assert!(
            old.is_none_or(|mut p| p.check().is_err()),
            "duplicated packet {id}"
        );
    }

    pub(crate) async fn handle_events(&mut self) -> Result<(), Error<MqttError>>
    where
        S: StoreCapabilities,
    {
        while let Some(item) = self.next_item().await {
            match item {
                Either::Left((id, token)) => {
                    self.queue(id, token);
                }
                Either::Right(id) => {
                    Self::mark_packet_received(&self.state, id).await;
                }
            }
        }

        info!("retention task exiting");

        self.on_exit().await;

        Ok(())
    }

    async fn next_item(&mut self) -> Option<Either<Item, RetentionId>>
    where
        S: StoreCapabilities,
    {
        let fut = NextFuture(&mut self.packets);

        tokio::select! {
            recv = self.rx.recv() => {
                let recv = recv?;

                Some(Either::Left(recv))
            }
            id = fut => {
                Some(Either::Right(id))
            }
            // Only on close, no disconnect
            _ = self.status_rx.wait_for(|c| *c == ConnStatus::Closed) => {
                debug!("connection closed");

                None
            }
        }
    }

    async fn on_exit(&mut self)
    where
        S: StoreCapabilities,
    {
        for (id, mut token) in self.packets.drain() {
            if token.check().is_ok() {
                Self::mark_packet_received(&self.state, id).await;
            }
        }
    }

    /// Marks the packets as received for the retention.
    #[instrument(skip_all, fields(%id))]
    async fn mark_packet_received(state: &ConnectionState<S>, id: RetentionId)
    where
        S: StoreCapabilities,
    {
        trace!("received packet");

        match &id {
            RetentionId::Volatile(id) => {
                state.volatile_store().mark_received(id).await;
            }
            RetentionId::Stored(id) => {
                if let Some(retention) = state.store().get_retention() {
                    let res = retention.mark_received(id).await;

                    if let Err(error) = res {
                        error!(%error, "couln't mark packet as received");
                    }
                }
            }
        }

        debug!("marked as received");
    }
}

pub(crate) struct NextFuture<'a>(&'a mut HashMap<RetentionId, Token<AckOfPub>>);

impl Future for NextFuture<'_> {
    type Output = RetentionId;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut is_error = false;

        let item = self
            .0
            .extract_if(|_id, f| match Pin::new(f).poll(cx) {
                Poll::Ready(Ok(_)) => true,
                Poll::Ready(Err(error)) => {
                    error!(%error, "couldn't wait for Ack");

                    is_error = true;

                    true
                }
                Poll::Pending => false,
            })
            .next();

        match item {
            Some((id, _)) => {
                if is_error {
                    // NOTE Since the wake has been consumed. Wake the waker task again
                    cx.waker().wake_by_ref();

                    // Ignore the packet, since the state will be reset in the reconnection when resending
                    Poll::Pending
                } else {
                    Poll::Ready(id)
                }
            }
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rumqttc::Resolver;

    use crate::builder::DEFAULT_CHANNEL_SIZE;
    use crate::interfaces::Interfaces;
    use crate::state::tests::mock_state;
    use crate::state::{ConnStatus, Context};
    use crate::store::mock::MockStore;

    use super::*;

    #[tokio::test]
    async fn should_queue_and_get_next() {
        let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE.get());
        let (status_tx, status_rx) = tokio::sync::watch::channel(ConnStatus::Online);

        let state = ConnectionState::new(Arc::new(mock_state(
            MockStore::new(),
            status_tx,
            Interfaces::new(),
        )));

        let mut retention = RetentionTask {
            state,
            packets: HashMap::new(),
            rx,
            status_rx,
        };

        let ctx = Context::new();

        let i1 = ctx.next();
        let (_t1, n1) = Resolver::new();

        let i2 = ctx.next();
        let (t2, n2) = Resolver::new();

        let i3 = ctx.next();
        let (_t3, n3) = Resolver::new();

        retention.queue(RetentionId::Stored(i1), n1);
        retention.queue(RetentionId::Stored(i2), n2);

        tx.try_send((RetentionId::Stored(i3), n3)).unwrap();

        assert!(matches!(
            retention.next_item().await,
            Some(Either::Left((RetentionId::Stored(id), _))) if id == i3
        ));

        t2.resolve(AckOfPub::None);
        assert!(matches!(
            retention.next_item().await,
            Some(Either::Right(RetentionId::Stored(id))) if id == i2
        ));
    }
}
