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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use eyre::{Context, bail, ensure, eyre};
use phoenix_chan::Message;
use phoenix_chan::tungstenite::http::Uri;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tracing::{debug, error, info, instrument, trace};
use uuid::Uuid;

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
    joined: AtomicBool,
    rx: async_channel::Receiver<Reply>,
}

impl Channel {
    pub(crate) async fn connect(
        mut appengine_ws: Url,
        realm: &str,
        token: &str,
        device_id: &str,
        tasks: &mut JoinSet<eyre::Result<()>>,
        cancel: broadcast::Receiver<()>,
    ) -> eyre::Result<Self> {
        appengine_ws
            .query_pairs_mut()
            .append_pair("vsn", "2.0.0")
            .append_pair("realm", realm)
            .append_pair("token", token);

        let uri = Uri::try_from(appengine_ws.to_string())?;

        let tls: rustls::ClientConfig = crate::tls::client_config()?;
        let tls = Arc::new(tls);
        let client = phoenix_chan::Client::builder(uri)?
            .tls_config(tls)
            .connect()
            .await?;
        let client = Arc::new(client);

        let uuid = Uuid::now_v7();

        let room = format!("rooms:{realm}:e2e_test_{uuid}_{device_id}");

        let rx = spawn_channel_recv(&client, tasks, cancel);

        let this = Self {
            room,
            client,
            joined: AtomicBool::new(false),
            rx,
            device_id: device_id.to_string(),
        };

        this.join().await?;

        Ok(this)
    }

    #[instrument(skip(self))]
    async fn wait_for(&self, id: usize) -> eyre::Result<()> {
        trace!("waiting for response");

        loop {
            let reply = tokio::time::timeout(Duration::from_secs(2), self.rx.recv())
                .await
                .wrap_err_with(|| format!("waiting for {id}"))?
                .wrap_err("channel closed")?;

            trace!(?reply, "received a new message");

            let phx_reply = reply
                .try_into_phx_reply()
                .ok()
                .filter(|phx_reply| phx_reply.message_reference == Some(id.to_string()));

            let Some(msg) = phx_reply else {
                trace!("skipping");

                continue;
            };

            trace!(?msg, "reply received");

            ensure!(msg.payload.is_ok(), "channel error {:?}", msg);

            break;
        }

        Ok(())
    }

    #[instrument(skip(self))]
    pub(crate) async fn join(&self) -> eyre::Result<()> {
        let id = self.client.join(&self.room).await?;

        self.wait_for(id).await?;

        self.joined
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|joined| eyre!("cmp exchange failed joined={joined}"))?;

        Ok(())
    }

    #[instrument(skip(self, trigger), fields(trigger_name = trigger.name))]
    pub(crate) async fn watch(&self, trigger: TransitiveTrigger<'_>) -> eyre::Result<()> {
        let id = self.client.send(&self.room, "watch", trigger).await?;

        self.wait_for(id).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub(crate) async fn next_data_event(&self) -> eyre::Result<IncomingData> {
        for i in 0.. {
            debug!(i, "waiting for next device data event");

            let reply = tokio::time::timeout(Duration::from_secs(2), self.rx.recv())
                .await
                .wrap_err("waiting for new_event")?
                .wrap_err("error receiving from channel")?;

            let Ok(new_event) = reply.try_into_new_event() else {
                continue;
            };

            let Ok(data) = new_event.payload.event.try_into_incoming_data() else {
                continue;
            };

            return Ok(data);
        }

        bail!("never received data event")
    }

    #[instrument(skip(self))]
    pub(crate) async fn next_device_disconnected(&mut self) -> eyre::Result<()> {
        for i in 0.. {
            debug!(i, "waiting for next device disconnected");

            let reply = tokio::time::timeout(Duration::from_secs(2), self.rx.recv())
                .await
                .wrap_err("waiting for new_event")?
                .wrap_err("error receiving from channel")?;

            let Ok(new_event) = reply.try_into_new_event() else {
                continue;
            };

            let Ok(()) = new_event.payload.event.try_into_device_disconnected() else {
                continue;
            };

            info!("Device disconnected from Astarte");

            return Ok(());
        }

        bail!("never received disconnect")
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        if !self.joined.load(Ordering::Relaxed) {
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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TriggerTarget<'a> {
    #[default]
    AllDevices,
    DeviceId(&'a str),
    GroupName(&'a str),
}

impl<'a> TriggerTarget<'a> {
    /// Returns `true` if the trigger target is [`AllDevices`].
    ///
    /// [`AllDevices`]: TriggerTarget::AllDevices
    #[must_use]
    pub(crate) fn is_all_devices(&self) -> bool {
        matches!(self, Self::AllDevices)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TransitiveTrigger<'a> {
    pub(crate) name: &'a str,
    #[serde(
        default,
        flatten,
        skip_serializing_if = "TriggerTarget::is_all_devices"
    )]
    pub(crate) target: TriggerTarget<'a>,
    pub(crate) simple_trigger: SimpleTrigger<'a>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceTriggerCondition {
    DeviceConnected,
    DeviceDisconnected,
    DeviceError,
    DeviceEmptyCacheReceived,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
    DeviceConnected(DeviceConnected),
    DeviceDisconnected,
    DeviceError(DeviceError),
    IncomingData(IncomingData),
}

impl Event {
    pub fn try_into_incoming_data(self) -> Result<IncomingData, Self> {
        if let Self::IncomingData(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    pub fn try_into_device_error(self) -> Result<DeviceError, Self> {
        if let Self::DeviceError(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    pub fn try_into_device_connected(self) -> Result<DeviceConnected, Self> {
        if let Self::DeviceConnected(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    pub fn try_into_device_disconnected(self) -> Result<(), Self> {
        if let Self::DeviceDisconnected = self {
            Ok(())
        } else {
            Err(self)
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub struct DeviceConnected {
    pub device_ip_address: String,
}

#[derive(Debug, Deserialize)]
pub struct DeviceError {
    pub error_name: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct IncomingData {
    pub interface: String,
    pub path: String,
    pub value: serde_json::Value,
}

fn spawn_channel_recv(
    client: &Arc<phoenix_chan::Client>,
    tasks: &mut JoinSet<eyre::Result<()>>,
    cancel: broadcast::Receiver<()>,
) -> async_channel::Receiver<Reply> {
    let client = Arc::clone(client);

    let (tx, rx) = async_channel::bounded::<Reply>(20);

    tasks.spawn(recv_phx_events(client, tx, cancel));

    rx
}

#[instrument(skip_all)]
async fn recv_phx_events(
    client: Arc<phoenix_chan::Client>,
    tx: async_channel::Sender<Reply>,
    mut cancel: broadcast::Receiver<()>,
) -> eyre::Result<()> {
    loop {
        let message = tokio::select! {
            res = cancel.recv() => {
                res.wrap_err("channel receiver error")?;

                trace!("cancel received");

                return Ok(());
            }
            res = client.recv::<serde_json::Value>() => {
                trace!("received");

                res.wrap_err("channel receiver error").inspect_err(|err| error!(%err, "recv error"))?
            }
        };

        trace!(?message, "message received");

        match message.event_name.as_str() {
            "phx_reply" => {
                let message = message.deserialize_payload::<PhxReply>()?;

                tx.send(Reply::PhxReply(Box::new(message))).await?;
            }
            "new_event" => {
                let message = message.deserialize_payload::<NewEvent>()?;

                tx.send(Reply::NewEvent(Box::new(message))).await?;
            }
            _ => {
                trace!("ignoring received event")
            }
        }
    }
}

pub(crate) async fn register_triggers(channel: &Channel) -> eyre::Result<()> {
    let device_id = channel.device_id.as_str();

    channel
        .watch(TransitiveTrigger {
            name: &format!("connectiontrigger-{device_id}"),
            target: TriggerTarget::DeviceId(device_id),
            simple_trigger: SimpleTrigger::DeviceTrigger {
                on: DeviceTriggerCondition::DeviceConnected,
                device_id,
            },
        })
        .await?;

    channel
        .watch(TransitiveTrigger {
            name: &format!("disconnectiontrigger-{device_id}"),
            target: TriggerTarget::DeviceId(device_id),
            simple_trigger: SimpleTrigger::DeviceTrigger {
                on: DeviceTriggerCondition::DeviceDisconnected,
                device_id,
            },
        })
        .await?;

    channel
        .watch(TransitiveTrigger {
            name: &format!("errortrigger-{device_id}"),
            target: TriggerTarget::DeviceId(device_id),
            simple_trigger: SimpleTrigger::DeviceTrigger {
                on: DeviceTriggerCondition::DeviceError,
                device_id,
            },
        })
        .await?;

    channel
        .watch(TransitiveTrigger {
            name: &format!("datatrigger-{device_id}"),
            target: TriggerTarget::DeviceId(device_id),
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
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn serialize_connection_trigger() {
        let device_id = "FdUczQZzSoyFnKDl-srB0w";
        let trigger = TransitiveTrigger {
            name: &format!("connectiontrigger-{device_id}"),
            target: TriggerTarget::DeviceId(device_id),
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
