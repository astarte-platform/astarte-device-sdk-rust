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
use rumqttc::{Event, Packet, Publish};
use tracing::{debug, error, trace};

use crate::logging::security::{SecurityEvent, notify_security_event, notify_tls_error};
use crate::store::StoreCapabilities;

use self::context::ConnCtx;
use self::disconnected::Disconnected;
use self::handshake::Handshake;
use self::state::State;
use self::wait_connack::Connack;
use self::wait_sends::TaskHandle;

use super::error::MqttError;
use crate::pairing::Pairing;

mod connected;
pub(crate) mod context;
mod disconnected;
mod handshake;
mod state;
mod wait_connack;
mod wait_sends;

fn is_tls_error(error: &Error<MqttError>) -> Option<&rustls::Error> {
    std::error::Error::source(error).and_then(|s| match s.downcast_ref() {
        Some(rumqttc::ConnectionError::Tls(rumqttc::TlsError::TLS(tls))) => Some(tls),
        _ => None,
    })
}

#[derive(Debug)]
pub(crate) struct MqttState<P> {
    /// The device is disconnected from Astarte, it will need to recreate the connection.
    pub(crate) pairing: P,
    state: State,
}

impl<P> MqttState<P> {
    pub(crate) fn new(pairing: P) -> Self {
        Self {
            pairing,
            state: State::Disconnected(Disconnected { connection: None }),
        }
    }

    pub(crate) async fn next_publish(&mut self) -> Option<Publish> {
        match &mut self.state {
            State::Connected(connected) => match connected.next_publish().await {
                Ok(publish) => Some(publish),
                Err(error) => {
                    if let Some(err) = is_tls_error(&error) {
                        notify_tls_error(err);
                    }

                    self.state.set_disconnected();

                    error!(%error, "couldn't poll next publish");

                    None
                }
            },
            state => {
                error!(?state, "cloudn't poll since status is not connected");

                None
            }
        }
    }

    /// Reconnects
    pub(crate) async fn reconnect<S>(
        &mut self,
        ctx: &mut ConnCtx<'_, S>,
    ) -> Result<ControlFlow<bool, Publish>, Error<MqttError>>
    where
        P: Pairing,
        S: StoreCapabilities,
    {
        debug_assert!(!matches!(self.state, State::Transition));

        match &mut self.state {
            State::Connected(_) => Ok(ControlFlow::Break(true)),
            State::Disconnected(disconnected) => {
                let (session_present, join_handle) =
                    Self::handle_reconnect(disconnected, &mut self.pairing, ctx).await?;

                let Some(join_handle) = join_handle else {
                    self.state.set_connected()?;

                    return Ok(ControlFlow::Break(session_present));
                };

                let wait_task = self.state.set_wait_task(session_present, join_handle)?;
                let control_flow = wait_task.wait_connection(ctx).await;

                self.handle_task(control_flow)
            }
            State::WaitAcks(wait_task) => {
                let control_flow = wait_task.wait_connection(ctx).await;

                self.handle_task(control_flow)
            }
            State::Transition => {
                self.state.set_disconnected();

                Err(Error::with(
                    MqttError::Connection,
                    "broken connection state",
                ))
            }
        }
    }

    async fn handle_reconnect<S>(
        disconnected: &mut Disconnected,
        pairing: &mut P,
        ctx: &mut ConnCtx<'_, S>,
    ) -> Result<(bool, Option<TaskHandle>), Error<MqttError>>
    where
        P: Pairing,
        S: StoreCapabilities,
    {
        let cfg = pairing.config(ctx).await.wrap_err_with(|error| {
            error!(%error, "couldn't get pairing config");

            Error::new(MqttError::DevicePairing)
        })?;

        // FIXME: Not sure if this is the best place to do it
        ctx.state.set_device_status(true);

        let connection = disconnected
            .connect(ctx, &cfg)
            .await
            .map_kind(MqttError::PairingApi)?;

        // NOTE: the next packet will always be a CONNACK, see Disconnected for more information
        let mut connack = Connack { connection };
        let session_present = connack.wait(ctx).await?;

        let mut handshake = Handshake {
            connection: connack.connection,
            session_present,
        };

        let join_handle = handshake.start(ctx, cfg.client_id.as_ref()).await;

        Ok((session_present, join_handle))
    }

    fn handle_task(
        &mut self,
        control_flow: Result<ControlFlow<bool, Publish>, Error<MqttError>>,
    ) -> Result<ControlFlow<bool, Publish>, Error<MqttError>> {
        match control_flow {
            Ok(p @ ControlFlow::Continue(_)) => Ok(p),
            Ok(p @ ControlFlow::Break(_)) => {
                self.state.set_connected()?;

                Ok(p)
            }
            Err(err) => {
                self.state.set_disconnected();

                Err(err)
            }
        }
    }
}

fn handle_event(event: Event) -> Result<Option<Publish>, Error<MqttError>> {
    trace!("handling event");

    let incoming = match event {
        Event::Incoming(incoming) => {
            trace!("incoming packet {incoming:?}");

            incoming
        }
        Event::Outgoing(outgoing) => {
            trace!("outgoing packet {outgoing:?}");

            return Ok(None);
        }
    };

    match incoming {
        Packet::ConnAck(connack) => {
            error!(connack=?connack, "connack received after the initial connection, broker bug");

            notify_security_event(SecurityEvent::UnexpectedMessageReceived);

            Err(Error::with(
                MqttError::Connection,
                "unexpected CONNACK packet received",
            ))
        }
        Packet::Publish(publish) => {
            debug!("incoming publish on {}", publish.topic);

            Ok(Some(publish))
        }
        Packet::Disconnect => {
            debug!("server sent a disconnect packet");

            Err(Error::with(
                MqttError::Disconnect,
                "DISCONNECT packet received",
            ))
        }
        _ => {
            trace!("incoming packet");

            Ok(None)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {

    use rstest::{fixture, rstest};
    use rumqttc::{ConnAck, Outgoing, Pkid};

    use crate::pairing::api::PairingApi;
    use crate::transport::mqtt::MqttConfig;
    use crate::transport::mqtt::client::{AsyncClient, EventLoop};

    use super::connected::tests::mock_connected;
    use super::*;

    pub(crate) fn mock_mqtt_state_connected(
        client: AsyncClient,
        eventloop: EventLoop,
        mqtt_config: MqttConfig,
    ) -> MqttState<PairingApi> {
        MqttState {
            pairing: PairingApi::new(mqtt_config),
            state: State::Connected(mock_connected(client, eventloop)),
        }
    }

    #[fixture]
    pub(crate) fn publish_pkt() -> Publish {
        Publish {
            dup: false,
            qos: rumqttc::QoS::AtMostOnce,
            retain: false,
            topic: "foo/bar".to_string(),
            pkid: Pkid::default(),
            payload: vec![42].into(),
        }
    }

    #[rstest]
    #[case(Event::Incoming(Packet::Publish(publish_pkt())), Some(publish_pkt()))]
    #[case(Event::Outgoing(Outgoing::PingReq), None)]
    #[case(Event::Outgoing(Outgoing::Publish(1)), None)]
    #[case(Event::Incoming(Packet::PingResp), None)]
    fn should_handle_event(#[case] event: Event, #[case] exp: Option<Publish>) {
        let res = handle_event(event).unwrap();

        assert_eq!(res, exp);
    }

    #[rstest]
    #[case(Event::Incoming(Packet::Disconnect))]
    #[case(Event::Incoming(Packet::ConnAck(ConnAck{session_present:false, code: rumqttc::ConnectReturnCode::Success})))]
    fn should_handle_event_errsos(#[case] event: Event) {
        let _ = handle_event(event).unwrap_err();
    }
}
