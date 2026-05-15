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

use rumqttc::Publish;

use super::context::Connection;
use super::{ConnError, handle_event};

#[derive(Debug)]
pub(super) struct Connected {
    pub(super) connection: Connection,
}
impl Connected {
    pub(super) async fn next_publish(&mut self) -> Result<Publish, ConnError> {
        loop {
            let publish = self
                .connection
                .eventloop_mut()
                .poll()
                .await
                .map_err(|err| ConnError::Connection {
                    ctx: "next publish",
                    source: err,
                })
                .and_then(handle_event)?;

            if let Some(publish) = publish {
                return Ok(publish);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use futures::FutureExt;
    use mockall::Sequence;
    use rstest::rstest;
    use sync_wrapper::SyncWrapper;

    use crate::transport::mqtt::client::{AsyncClient, EventLoop};
    use crate::transport::mqtt::connection::tests::publish_pkt;

    use super::*;

    pub(crate) fn mock_connected(client: AsyncClient, eventloop: EventLoop) -> Connected {
        Connected {
            connection: Connection {
                client,
                eventloop: SyncWrapper::new(eventloop),
            },
        }
    }

    #[rstest]
    #[tokio::test]
    async fn should_poll(publish_pkt: Publish) {
        let client = AsyncClient::default();
        let mut eventloop = EventLoop::default();

        let mut seq = Sequence::new();

        eventloop
            .expect_poll()
            .once()
            .in_sequence(&mut seq)
            .with()
            .returning(|| {
                futures::future::ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::PingReq)).boxed()
            });

        eventloop
            .expect_poll()
            .once()
            .in_sequence(&mut seq)
            .with()
            .returning({
                let publish = publish_pkt.clone();
                move || {
                    futures::future::ok(rumqttc::Event::Incoming(rumqttc::Incoming::Publish(
                        publish.clone(),
                    )))
                    .boxed()
                }
            });

        let mut connected = mock_connected(client, eventloop);

        let res = connected.next_publish().await.unwrap();

        assert_eq!(res, publish_pkt)
    }
}
