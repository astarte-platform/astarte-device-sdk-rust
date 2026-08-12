// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
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

//! State of the MQTT connection
//!
//! ### Errors
//!
//! - If we cannot read or write to the Store
//! - If we the pairing API returns a non retryable error
//!
//! #### In-flight Messages
//!
//! Messages that were already in-flight should still be completed even if the introspection changes during their transmission.
//!
//! During disconnections, any messages should be enqueued and processed once the client reconnects.
//!
//! #### Handling Incoming Messages
//!
//! Incoming messages must be processed as they arrive. If an error occurs during this process, it should be propagated up to the client with appropriate error-handling mechanisms.

use std::fmt::Debug;
use std::ops::ControlFlow;

use astarte_device_error::{Error, ResultExt, WrapError};
use astarte_interfaces::{DatastreamIndividual, DatastreamObject, MappingPath, Properties};
use bytes::Bytes;
use rumqttc::{Event, Packet};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, trace};

use crate::aggregate::AstarteObject;
use crate::connection::incoming::ctx::ConnectionCtx;
use crate::error::{AstarteError, ErrorKind};
use crate::interfaces::{Interfaces, MappingRef};
use crate::logging::security::{SecurityEvent, notify_security_event, notify_tls_error};
use crate::state::ConnectionState;
use crate::store::StoreCapabilities;
use crate::transport::{Decode, Transport};
use crate::{AstarteData, Timestamp, properties};

use super::deps::EventLoop;
use super::error::MqttError;
use super::pairing::client::ClientArgs;
use super::pairing::mk_connection::MakeConnection;
use super::topic::ParsedTopic;
use super::{Mqtt, payload};

fn is_tls_error(error: &rumqttc::ConnectionError) -> Option<&rustls::Error> {
    std::error::Error::source(error).and_then(|s| match s.downcast_ref() {
        Some(rumqttc::ConnectionError::Tls(rumqttc::TlsError::TLS(tls))) => Some(tls),
        _ => None,
    })
}

/// Handles the MQTT connection between a device and Astarte.
///
///  It manages the interaction with the MQTT broker, handling connections, subscriptions, and
///  message publishing following the Astarte protocol.
#[derive(Debug)]
pub struct MqttConnection {
    // TODO: this could be a P of pairing, to use the crypto of fdo for the credentials secret
    pub(crate) config: Mqtt,
    pub(crate) connection: Connection,
}

impl MqttConnection {
    #[instrument(skip_all, fields(topic = publish.topic))]
    async fn handle_publish<S>(
        &self,
        ctx: &ConnectionCtx<'_, S>,
        publish: rumqttc::Publish,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        // If we receive a topic we cannot parse nor handle, to be more robust we ignore it since is
        // not actionable and doesn't change the state of the connection since it's not specified
        // in the Astarte protocol.
        let publish_topic =
            match ParsedTopic::try_parse(self.config.client_id.as_ref(), &publish.topic) {
                Ok(topic) => topic,
                Err(error) => {
                    error!(%error, "couldn't parse topic");

                    return Ok(());
                }
            };

        match publish_topic {
            ParsedTopic::PurgeProperties => {
                debug!("Purging properties");

                self.purge_server_properties(ctx, &publish.payload).await?;

                Ok(())
            }
            ParsedTopic::InterfacePath { interface, path } => {
                ctx.handle_event(interface, path, MqttDecoder::new(publish.payload))
                    .await
            }
        }
    }

    /// This function deletes all the stored server owned properties after receiving a publish on
    /// `/control/consumer/properties`
    async fn purge_server_properties<S>(
        &self,
        ctx: &ConnectionCtx<'_, S>,
        bdata: &[u8],
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        let paths = properties::extract_set_properties(bdata)
            .map_kind(|k| ErrorKind::Mqtt(MqttError::PurgeProp(k)))?;

        let mut last_updated_at = None;

        loop {
            let stored_props = ctx
                .state
                .store()
                .server_props(ctx.state.config().channel_size, last_updated_at)
                .await
                .map_kind(ErrorKind::Store)?;

            if stored_props.is_empty() {
                break;
            }

            for prop in &stored_props {
                last_updated_at = Some(prop.updated_at());

                // TODO: extract should split interface and path
                if paths.contains(&format!("{}{}", prop.interface, prop.path)) {
                    continue;
                }

                ctx.state
                    .store()
                    .delete_server_prop(&prop.interface, &prop.path)
                    .await
                    .map_kind(ErrorKind::Store)?;
            }
        }

        Ok(())
    }
}

impl Transport for MqttConnection {
    async fn connect<S>(
        &mut self,
        state: &ConnectionState<S>,
        _interfaces: &Interfaces,
        first: bool,
    ) -> Result<ControlFlow<bool>, AstarteError>
    where
        S: StoreCapabilities,
    {
        // Do not check on first connection, we just registered
        if !first {
            let cred = self
                .config
                .credentials(state)
                .await
                .map_kind(|k| ErrorKind::Mqtt(MqttError::PairingApi(k)))?;

            let mut mk_conn = MakeConnection {
                keepalive: self.config.keepalive,
                state,
                args: ClientArgs {
                    client_id: self.config.client_id.as_ref(),
                    pairing_url: &self.config.pairing_url,
                    token: &cred,
                },
            };

            // TODO: should we retry only for certain error codes
            if let Err(error) = mk_conn.connect(&mut self.connection).await {
                error!(%error, "couldn't connect to Astarte");

                return Ok(ControlFlow::Continue(()));
            }
        }

        while let ControlFlow::Continue(event) = self.connection.poll().await? {
            match event {
                Event::Outgoing(packet) => {
                    trace!(?packet, "outgoing packet");
                }
                Event::Incoming(Packet::ConnAck(connack)) => {
                    let session_present = connack.session_present;

                    info!(session_present, "connected");

                    return Ok(ControlFlow::Break(session_present));
                }
                Event::Incoming(packet) => {
                    error!(
                        ?packet,
                        "unexpected packet receive while waiting for CONNACK"
                    );

                    notify_security_event(SecurityEvent::UnexpectedMessageReceived);

                    return Err(Error::with(
                        ErrorKind::Mqtt(MqttError::Connection),
                        "unexpected packet while waiting for CONNACK",
                    ));
                }
            }
        }

        Ok(ControlFlow::Continue(()))
    }

    async fn poll<S>(&mut self, ctx: &ConnectionCtx<'_, S>) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        while let ControlFlow::Continue(event) = self.connection.poll().await? {
            let incoming = match event {
                Event::Incoming(incoming) => {
                    trace!("incoming packet {incoming:?}");

                    incoming
                }
                Event::Outgoing(outgoing) => {
                    trace!(?outgoing, "outgoing packet");

                    continue;
                }
            };

            match incoming {
                Packet::Publish(publish) => {
                    debug!(topic = publish.topic, "incoming publish",);

                    self.handle_publish(ctx, publish).await?;
                }
                Packet::Disconnect => {
                    if ctx.state.should_exit() {
                        debug!("server sent a disconnect packet");
                    } else {
                        error!("server sent a disconnect packet");
                    }

                    return Ok(());
                }
                Packet::SubAck(sub_ack) => {
                    let is_error = sub_ack
                        .return_codes
                        .contains(&rumqttc::SubscribeReasonCode::Failure);

                    if is_error {
                        error!(pkid = sub_ack.pkid, "subscribe call failed");
                    }
                }
                Packet::ConnAck(connack) => {
                    error!(connack=?connack, "connack received after the initial connection");

                    notify_security_event(SecurityEvent::UnexpectedMessageReceived);

                    return Err(Error::with(
                        ErrorKind::Mqtt(MqttError::Connection),
                        "unexpected CONNACK packet received",
                    ));
                }
                _ => {}
            }
        }

        trace!("poll returned");

        Ok(())
    }
}

pub(crate) struct MqttDecoder {
    data: Bytes,
}
impl MqttDecoder {
    fn new(data: Bytes) -> Self {
        Self { data }
    }
}

impl Decode for MqttDecoder {
    fn deserialize_property(
        self,
        mapping: &MappingRef<'_, Properties>,
    ) -> Result<Option<AstarteData>, AstarteError> {
        payload::deserialize_property(mapping, &self.data)
            .map_kind(|k| ErrorKind::Mqtt(MqttError::Payload(k)))
    }

    fn deserialize_individual(
        self,
        mapping: &MappingRef<'_, DatastreamIndividual>,
    ) -> Result<(AstarteData, Option<Timestamp>), AstarteError> {
        payload::deserialize_individual(mapping, &self.data)
            .map_kind(|k| ErrorKind::Mqtt(MqttError::Payload(k)))
    }

    fn deserialize_object(
        self,
        object: &DatastreamObject,
        path: &MappingPath<'_>,
    ) -> Result<(AstarteObject, Option<Timestamp>), AstarteError> {
        payload::deserialize_object(object, path, &self.data)
            .map_kind(|k| ErrorKind::Mqtt(MqttError::Payload(k)))
    }
}

pub(crate) struct Connection {
    pub(crate) eventloop: sync_wrapper::SyncWrapper<EventLoop>,
    pub(crate) retention: JoinHandle<Result<(), Error<MqttError>>>,
    pub(crate) retention_joined: bool,
}

impl Connection {
    /// Set the transport for the rumqttc
    #[cfg(not(test))]
    pub(crate) fn set_transport(&mut self, transport: rumqttc::Transport) {
        self.eventloop
            .get_mut()
            .mqtt_options
            .set_transport(transport);
    }

    #[cfg(test)]
    pub(crate) fn set_transport(&mut self, _transport: rumqttc::Transport) {}

    pub(crate) async fn poll(&mut self) -> Result<ControlFlow<(), rumqttc::Event>, AstarteError> {
        if self.retention_joined {
            return Err(Error::with(
                ErrorKind::Mqtt(MqttError::Task),
                "retention task exited",
            ));
        }

        tokio::select! {
            res = self.eventloop.get_mut().poll() => {
                match res {
                    Ok(event) => {
                        Ok(ControlFlow::Continue(event))
                    }
                    Err(error) => {
                        if let Some(err) = is_tls_error(&error) {
                            notify_tls_error(err);
                        }

                        error!(%error, "couldn't poll the connection");

                        Ok(ControlFlow::Break(()))
                    }
                }
            }
            // Error if the retention task exited
            res = &mut self.retention => {
                self.retention_joined = true;

                res.wrap_err_msg(MqttError::Task, "retention task error").flatten().map_kind(ErrorKind::Mqtt)?;

                // Error if the retention task exited before the connection
                Err(Error::with(ErrorKind::Mqtt(MqttError::Task), "retention task exited"))
            }
        }
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            eventloop: _,
            retention,
            retention_joined,
        } = self;

        f.debug_struct("Connection")
            .field("retention", retention)
            .field("retention_joined", retention_joined)
            .finish_non_exhaustive()
    }
}
