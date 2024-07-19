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

use std::collections::HashMap;

use flume::TryRecvError;
use rumqttc::{NoticeError, NoticeFuture};
use tracing::trace;

use crate::{retention::Id, Error};

pub(crate) type RetSender = flume::Sender<(Id, NoticeFuture)>;
pub(crate) type RetReceiver = flume::Receiver<(Id, NoticeFuture)>;

pub(crate) struct MqttRetention {
    packets: HashMap<Id, NoticeFuture>,
    rx: RetReceiver,
}

impl MqttRetention {
    pub(crate) fn new(rx: RetReceiver) -> Self {
        Self {
            packets: HashMap::new(),
            rx,
        }
    }

    pub(crate) fn queue(&mut self) -> Result<usize, Error> {
        if self.rx.is_empty() {
            return Ok(0);
        }

        let mut count: usize = 0;
        loop {
            // get all the already present publishes
            match self.rx.try_recv() {
                Ok((id, notice)) => {
                    self.insert(id, notice);

                    count = count.saturating_add(1);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Err(Error::Disconnected),
            }
        }

        Ok(count)
    }

    fn insert(&mut self, id: Id, notice: NoticeFuture) {
        let prev = self.packets.insert(id, notice);

        debug_assert!(prev.is_none(), "The IDs should be unique");
    }

    fn next_received(&mut self) -> Option<Result<Id, NoticeError>> {
        let (id, res) = self
            .packets
            .iter_mut()
            .find_map(|(id, v)| v.try_wait().map(|res| (*id, res)))?;

        self.packets.remove(&id);

        trace!("remove packet {id}");

        Some(res.map(|()| id))
    }
}

impl Iterator for MqttRetention {
    type Item = Result<Id, NoticeError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_received()
    }
}

#[cfg(test)]
mod tests {
    use rumqttc::NoticeTx;

    use crate::retention::Context;

    use super::*;

    #[test]
    fn should_queue_and_get_next() {
        let (tx, rx) = flume::unbounded();

        let mut retention = MqttRetention::new(rx);

        let ctx = Context::new();

        let i1 = ctx.next();
        let (t1, n1) = NoticeTx::new();

        let i2 = ctx.next();
        let (t2, n2) = NoticeTx::new();

        let i3 = ctx.next();
        let (_t3, n3) = NoticeTx::new();

        tx.send((i1, n1)).unwrap();
        tx.send((i2, n2)).unwrap();
        tx.send((i3, n3)).unwrap();

        assert_eq!(retention.queue().unwrap(), 3);

        let n = retention.next();
        assert!(n.is_none());

        t2.success();

        let n = retention.next().unwrap().unwrap();
        assert_eq!(n, i2);

        t1.error(NoticeError::Recv);
        let res = retention.next().unwrap();
        assert!(res.is_err(), "expected error but got {:?}", res.unwrap());
    }
}
