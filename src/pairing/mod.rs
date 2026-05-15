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

//! Module to pair a new device to the transport

use std::time::Duration;

use rumqttc::{MqttOptions, NetworkOptions, Transport};
use url::Url;

use crate::transport::mqtt::components::ClientId;
use crate::transport::mqtt::connection::context::Ctx;

use self::api::PairingError;

pub mod api;
#[cfg(feature = "fdo")]
pub mod fdo;

/// Trait to register a device to Astarte.
pub trait Pairing: Send + Sync {
    /// Error returned while pairing
    type Error: std::error::Error;

    /// Configures or registers the device.
    fn config<S>(
        &mut self,
        ctx: &mut Ctx<'_, S>,
    ) -> impl Future<Output = Result<PairingConfig, Self::Error>> + Send
    where
        S: Send + Sync;
}

/// Device configuration after pairing.
#[derive(Debug)]
pub struct PairingConfig {
    pub(crate) client_id: ClientId,
    pub(crate) secret: String,
    pub(crate) pairing_url: Url,
    pub(crate) keepalive: Duration,
}

impl PairingConfig {
    /// Builds the options to connect to the broker
    pub(crate) fn build_mqtt_opts(
        &self,
        transport: Transport,
        broker_url: &Url,
        timeout: Duration,
    ) -> Result<(MqttOptions, NetworkOptions), PairingError> {
        let host = broker_url
            .host_str()
            .ok_or_else(|| PairingError::Config("missing host in url".to_string()))?;
        let port = broker_url
            .port()
            .ok_or_else(|| PairingError::Config("missing port in url".to_string()))?;

        let mut mqtt_opts = MqttOptions::new(self.client_id.to_string(), host, port);

        let keep_alive = self.keepalive.as_secs();
        let conn_timeout = timeout.as_secs();
        if keep_alive >= conn_timeout {
            return Err(PairingError::Config(format!(
                "Keep alive ({keep_alive}s) should be less than the connection timeout ({conn_timeout}s)"
            )));
        }

        let mut net_opts = NetworkOptions::new();
        net_opts.set_connection_timeout(conn_timeout);

        mqtt_opts.set_keep_alive(self.keepalive);

        mqtt_opts.set_transport(transport);

        // Set the clean_session since this is the first connection.
        mqtt_opts.set_clean_session(true);

        Ok((mqtt_opts, net_opts))
    }
}
