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

use std::time::Instant;

use astarte_device_error::Error;
use rumqttc::{Event, Packet};
use tracing::{debug, error, trace};

use crate::error::Report;
use crate::logging::security::{SecurityEvent, notify_security_event};
use crate::transport::mqtt::error::MqttError;

use super::context::{ConnCtx, Connection};

#[derive(Debug)]
pub(super) struct Connack<'a> {
    pub(super) connection: &'a mut Connection,
}

impl<'a> Connack<'a> {
    pub(super) async fn wait<S>(
        &mut self,
        ctx: &mut ConnCtx<'_, S>,
    ) -> Result<bool, Error<MqttError>> {
        let instant = Instant::now();
        // NOTE: this is to remove an infinite loop and prevent a miss-behaves broker to never send
        //       the CONNACK. We just need to check the elapsed time, since we are guarantied to
        //       wake up periodically for outgoing PINGREQ.
        let timeout = ctx.state.config.connection_timeout.saturating_mul(2);

        while instant.elapsed() < timeout {
            let event = match self.connection.eventloop_mut().poll().await {
                Ok(event) => {
                    trace!("event received");

                    event
                }
                Err(err) => {
                    error!(error = %Report::new(&err),"error received from mqtt connection");

                    return Err(
                        Error::with(MqttError::Connection, "waiting for CONNACK").set_source(err)
                    );
                }
            };

            match event {
                Event::Incoming(Packet::ConnAck(connack)) => {
                    debug!("connack received");

                    return Ok(connack.session_present);
                }
                Event::Incoming(Packet::Disconnect) => {
                    error!("disconnect received");

                    return Err(Error::new(MqttError::Disconnect));
                }
                Event::Incoming(incoming) => {
                    error!(incoming = ?incoming,"unexpected packet received while waiting for connack");

                    notify_security_event(SecurityEvent::UnexpectedMessageReceived);

                    return Err(Error::with(
                        MqttError::Connection,
                        "expected CONNACK packet",
                    ));
                }
                Event::Outgoing(outgoing) => {
                    debug!("outgoing packet while waiting for connack {outgoing:?}");
                }
            }
        }

        error!("timeout reached while waiting for CONNACK");

        Err(Error::with(
            MqttError::Connection,
            "timeout waiting for CONNACK packet",
        ))
    }
}
