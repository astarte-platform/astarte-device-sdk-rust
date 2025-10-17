// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
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

use std::{
    collections::HashMap,
    future::{Future, IntoFuture},
    pin::Pin,
    task::Poll,
};

use rumqttc::{AckOfPub, Token, TokenError};
use tracing::{debug, trace, warn};

use crate::retention::RetentionId;

pub(crate) type RetSender = flume::Sender<(RetentionId, Token<AckOfPub>)>;
pub(crate) type RetReceiver = flume::Receiver<(RetentionId, Token<AckOfPub>)>;

pub(crate) struct MqttRetention {
    packets: HashMap<RetentionId, Token<AckOfPub>>,
    rx: RetReceiver,
}

impl MqttRetention {
    pub(crate) fn new(rx: RetReceiver) -> Self {
        Self {
            packets: HashMap::new(),
            rx,
        }
    }

    /// The retention client is disconnected and all packets have been handled
    pub(crate) fn is_empty(&self) -> bool {
        self.rx.is_empty() && self.rx.is_disconnected() && self.packets.is_empty()
    }

    /// Discards retention packets and returns the id of received packets
    pub(crate) fn discard(&mut self) -> Vec<RetentionId> {
        debug!("discarding retention packets");

        self.packets
            .drain()
            .chain(self.rx.drain())
            .filter_map(|(id, mut token)| token.check().map(|_| id).ok())
            .collect()
    }

    pub(crate) fn queue(&mut self) -> usize {
        if self.rx.is_empty() {
            trace!("rx empty, queued 0 packets");

            return 0;
        }

        let mut count: usize = 0;
        // get all the already present publishes
        for (id, notice) in self.rx.drain() {
            let prev = self.packets.insert(id, notice);

            debug_assert!(prev.is_none(), "The IDs should be unique");

            count = count.saturating_add(1);
        }

        debug_assert!(count > 0, "the rx shouldn't be empty");
        trace!("queued {count} packets");

        count
    }

    fn next_received(&mut self) -> Option<Result<RetentionId, TokenError>> {
        let (id, res) = self
            .packets
            .iter_mut()
            .find_map(|(id, v)| match v.check() {
                Ok(_) => Some((*id, Ok(*id))),
                Err(TokenError::Waiting) => None,
                Err(TokenError::Disconnected) => Some((*id, Err(TokenError::Disconnected))),
            })?;

        self.packets.remove(&id);

        trace!("remove packet {id}");

        Some(res)
    }
}

impl<'a> IntoFuture for &'a mut MqttRetention {
    type Output = Result<RetentionId, TokenError>;

    type IntoFuture = MqttRetentionFuture<'a>;

    fn into_future(self) -> Self::IntoFuture {
        MqttRetentionFuture(self)
    }
}

impl Iterator for MqttRetention {
    type Item = Result<RetentionId, TokenError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_received()
    }
}

pub(crate) struct MqttRetentionFuture<'a>(&'a mut MqttRetention);

impl std::future::Future for MqttRetentionFuture<'_> {
    type Output = Result<RetentionId, TokenError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self.get_mut().0;

        this.queue();

        let first = this.packets.iter_mut().find_map(|(id, token)| {
            let poll = <Token<AckOfPub> as Future>::poll(Pin::new(token), cx);

            match poll {
                Poll::Pending => None,
                Poll::Ready(Ok(_)) => Some((*id, Ok(*id))),
                Poll::Ready(Err(TokenError::Waiting)) => {
                    warn!(%id, "future returned Ready(Waiting), this should not happen and it could lead to errors on the next poll");

                    // NOTE: we could return None here, but after some consideration it's safer to
                    //       error and drop the token instead of risking a panic if we poll the
                    //       Future again
                    Some((*id, Err(TokenError::Disconnected)))
                }
                Poll::Ready(Err(TokenError::Disconnected)) => {
                    Some((*id, Err(TokenError::Disconnected)))
                }
            }
        });

        match first {
            Some((id, res)) => {
                let prev = this.packets.remove(&id);

                debug_assert!(prev.is_some(), "No token could be removed");

                Poll::Ready(res)
            }
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use rumqttc::Resolver;

    use crate::retention::Context;

    use super::*;

    #[test]
    fn should_queue_and_get_next() {
        let (tx, rx) = flume::unbounded();

        let mut retention = MqttRetention::new(rx);

        let ctx = Context::new();

        let i1 = ctx.next();
        let (t1, n1) = Resolver::new();

        let i2 = ctx.next();
        let (t2, n2) = Resolver::new();

        let i3 = ctx.next();
        let (_t3, n3) = Resolver::new();

        tx.send((RetentionId::Stored(i1), n1)).unwrap();
        tx.send((RetentionId::Stored(i2), n2)).unwrap();
        tx.send((RetentionId::Stored(i3), n3)).unwrap();

        assert_eq!(retention.queue(), 3);

        let n = retention.next();
        assert!(n.is_none());

        t2.resolve(AckOfPub::None);

        let n = retention.next().unwrap().unwrap();
        assert_eq!(n, RetentionId::Stored(i2));

        drop(t1);
        let res = retention.next().unwrap();
        assert!(res.is_err(), "expected error but got {:?}", res.unwrap());
    }
}
