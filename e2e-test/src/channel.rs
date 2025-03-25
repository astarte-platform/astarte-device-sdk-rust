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

use chrono::{DateTime, Utc};
use eyre::{ensure, Context, OptionExt};
use futures::StreamExt;
use phoenix_chan::Message;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tracing::{error, trace};

#[derive(Debug)]
pub enum Reply {
    PhxReply(Box<Message<PhxReply>>),
    NewEvent(Box<Message<NewEvent>>),
}

impl Reply {
    pub fn as_phx_reply(&self) -> Option<&Message<PhxReply>> {
        if let Self::PhxReply(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_new_event(&self) -> Option<&Message<NewEvent>> {
        if let Self::NewEvent(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn try_into_phx_reply(self) -> Result<Message<PhxReply>, Self> {
        if let Self::PhxReply(v) = self {
            Ok(*v)
        } else {
            Err(self)
        }
    }

    pub fn try_into_new_event(self) -> Result<Message<NewEvent>, Self> {
        if let Self::NewEvent(v) = self {
            Ok(*v)
        } else {
            Err(self)
        }
    }
}

#[derive(Debug)]
pub(crate) struct Channel {
    room: String,
    device_id: String,
    client: Arc<phoenix_chan::Client>,
    joined: bool,
    rx: flume::Receiver<Reply>,
}

impl Channel {
    pub(crate) async fn connect(
        appengine_url: &Url,
        realm: &str,
        token: &str,
        device_id: &str,
        tasks: &mut JoinSet<eyre::Result<()>>,
        cancel: broadcast::Receiver<()>,
    ) -> eyre::Result<Self> {
        let uri = format!(
            "{}/v1/socket/websocket?vsn=2.0.0&realm={}&token={}",
            appengine_url, realm, token
        )
        .replace("http", "ws")
        .parse()?;

        let client = phoenix_chan::Client::builder(uri)?.connect().await?;
        let client = Arc::new(client);

        let room = format!("rooms:{}:e2e_test_{}", realm, device_id);

        let rx = spawn_channel_recv(&client, tasks, cancel);

        Ok(Self {
            room,
            device_id: device_id.to_string(),
            client,
            joined: false,
            rx,
        })
    }

    async fn wait_for(&mut self, id: usize) -> eyre::Result<()> {
        let reply = tokio::time::timeout(
            Duration::from_secs(2),
            self.rx
                .stream()
                .filter_map(|msg| {
                    let phx_reply = msg
                        .try_into_phx_reply()
                        .ok()
                        .filter(|phx_reply| phx_reply.message_reference == Some(id.to_string()));

                    futures::future::ready(phx_reply)
                })
                .next(),
        )
        .await
        .wrap_err_with(|| format!("waiting for {id}"))?
        .ok_or_eyre("channel disconnected")?;

        ensure!(reply.payload.is_ok(), "channel error {:?}", reply);

        Ok(())
    }

    pub(crate) async fn join(&mut self) -> eyre::Result<()> {
        let id = self.client.join(&self.room).await?;

        self.wait_for(id).await?;

        self.joined = true;

        Ok(())
    }

    pub(crate) async fn watch(&mut self, trigger: TransitiveTrigger<'_>) -> eyre::Result<()> {
        let id = self.client.send(&self.room, "watch", trigger).await?;

        self.wait_for(id).await?;

        Ok(())
    }

    pub(crate) async fn next_event(&mut self) -> eyre::Result<NewEvent> {
        let new_event = tokio::time::timeout(
            Duration::from_secs(2),
            self.rx
                .stream()
                .filter_map(|msg| {
                    let new_event = msg.try_into_new_event().ok();

                    futures::future::ready(new_event)
                })
                .next(),
        )
        .await
        .wrap_err("waiting for new_event")?
        .ok_or_eyre("channel disconnected")?;

        Ok(new_event.payload)
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        if !self.joined {
            return;
        }

        let client = Arc::clone(&self.client);

        let room = std::mem::take(&mut self.room);

        tokio::spawn(async move {
            if let Err(err) = client.leave(&room).await {
                error!(
                    room,
                    error = format!("{:#}", eyre::Report::new(err)),
                    "failed to leave room"
                )
            }
        });
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct TransitiveTrigger<'a> {
    pub(crate) name: &'a str,
    pub(crate) device_id: &'a str,
    pub(crate) simple_trigger: SimpleTrigger<'a>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum SimpleTrigger<'a> {
    DeviceTrigger {
        on: DeviceTriggerCondition,
        device_id: &'a str,
    },
    DataTrigger {
        on: DataTriggerCondition,
        device_id: &'a str,
        interface_name: &'a str,
        match_path: &'a str,
        value_match_operator: &'a str,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceTriggerCondition {
    DeviceConnected,
    DeviceDisconnected,
    DeviceError,
    DeviceEmptyCacheReceived,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataTriggerCondition {
    IncomingData,
    ValueStored,
}

#[derive(Debug, Deserialize)]
pub struct PhxReply {
    pub status: PhxStatus,
    pub response: serde_json::Value,
}

impl PhxReply {
    pub(crate) fn is_ok(&self) -> bool {
        matches!(self.status, PhxStatus::Ok)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhxStatus {
    Ok,
    Error,
}

#[derive(Debug, Deserialize)]
pub struct NewEvent {
    pub device_id: String,
    pub timestamp: DateTime<Utc>,
    pub event: Event,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    IncomingData {
        interface: String,
        path: String,
        value: serde_json::Value,
    },
}

fn spawn_channel_recv(
    client: &Arc<phoenix_chan::Client>,
    tasks: &mut JoinSet<eyre::Result<()>>,
    mut cancel: broadcast::Receiver<()>,
) -> flume::Receiver<Reply> {
    let client = Arc::clone(client);

    let (tx, rx) = flume::bounded::<Reply>(20);

    tasks.spawn(async move {
        loop {
            let message = tokio::select! {
                res = cancel.recv() => {
                    res.wrap_err("channel receiver error")?;

                    return Ok(());
                }
                res = client.recv::<serde_json::Value>() => {
                    res.wrap_err("channel receiver error")?
                }
            };

            match message.event_name.as_str() {
                "phx_reply" => {
                    let message = message.deserialize_payload::<PhxReply>()?;

                    tx.send(Reply::PhxReply(Box::new(message)))?;
                }
                "new_event" => {
                    let message = message.deserialize_payload::<NewEvent>()?;

                    tx.send(Reply::NewEvent(Box::new(message)))?;
                }
                _ => {
                    trace!(?message, "ignoring received event")
                }
            }
        }
    });

    rx
}

pub(crate) async fn register_triggers(channel: &mut Channel) -> eyre::Result<()> {
    channel.join().await?;
    let device_id = &channel.device_id.clone();

    channel
        .watch(TransitiveTrigger {
            name: &format!("connectiontrigger-{device_id}"),
            device_id,
            simple_trigger: SimpleTrigger::DeviceTrigger {
                on: DeviceTriggerCondition::DeviceConnected,
                device_id,
            },
        })
        .await?;

    channel
        .watch(TransitiveTrigger {
            name: &format!("disconnectiontrigger-{device_id}"),
            device_id,
            simple_trigger: SimpleTrigger::DeviceTrigger {
                on: DeviceTriggerCondition::DeviceDisconnected,
                device_id,
            },
        })
        .await?;

    channel
        .watch(TransitiveTrigger {
            name: &format!("errortrigger-{device_id}"),
            device_id,
            simple_trigger: SimpleTrigger::DeviceTrigger {
                on: DeviceTriggerCondition::DeviceError,
                device_id,
            },
        })
        .await?;

    channel
        .watch(TransitiveTrigger {
            name: &format!("datatrigger-{device_id}"),
            device_id,
            simple_trigger: SimpleTrigger::DataTrigger {
                on: DataTriggerCondition::IncomingData,
                device_id,
                interface_name: "*",
                match_path: "/*",
                value_match_operator: "*",
            },
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_connection_trigger() {
        let device_id = "FdUczQZzSoyFnKDl-srB0w";
        let trigger = TransitiveTrigger {
            name: &format!("connectiontrigger-{device_id}"),
            device_id,
            simple_trigger: SimpleTrigger::DeviceTrigger {
                on: DeviceTriggerCondition::DeviceConnected,
                device_id,
            },
        };

        let value = serde_json::to_string(&trigger).unwrap();

        let exp = r#"{"name":"connectiontrigger-FdUczQZzSoyFnKDl-srB0w","device_id":"FdUczQZzSoyFnKDl-srB0w","simple_trigger":{"type":"device_trigger","on":"device_connected","device_id":"FdUczQZzSoyFnKDl-srB0w"}}"#;
        assert_eq!(value, exp);
    }
}
