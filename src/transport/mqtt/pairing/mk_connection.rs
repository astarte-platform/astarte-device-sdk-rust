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

use std::time::Duration;

use astarte_device_error::Error;
use rumqttc::{MqttOptions, NetworkOptions, Transport};
use tracing::{debug, error, trace};
use url::Url;

use crate::error::Report;
use crate::logging::security::{SecurityEvent, notify_security_event};
use crate::state::ConnectionState;
use crate::transport::mqtt::config::tls::ClientAuth;
use crate::transport::mqtt::config::transport::TransportProvider;
use crate::transport::mqtt::connection::Connection;
use crate::transport::mqtt::deps::{AsyncClient, EventLoop};
use crate::transport::mqtt::pairing::client::{ApiClient, ClientArgs};

use super::PairingApiError;

/// Make the MQTT connection.
///
/// It will call the pairing API to register the device or validate an existing certificate.
#[derive(Debug)]
pub(crate) struct MakeConnection<'a, S> {
    pub(crate) keepalive: Duration,
    pub(crate) state: &'a ConnectionState<S>,
    pub(crate) args: ClientArgs<'a>,
}

impl<'a, S> MakeConnection<'a, S> {
    pub(crate) async fn create(
        &mut self,
    ) -> Result<(AsyncClient, EventLoop), Error<PairingApiError>> {
        let api = ApiClient::create(self.args, self.state.config(), self.state.tls().clone())?;

        let store_dir = self.state.config().writable_dir.as_deref();
        let client_auth =
            TransportProvider::retrieve_credentials(&api, self.args.client_id, store_dir)
                .await
                .inspect_err(|err| {
                    error!(error = %Report::new(err),"couldn't pair device");
                })?;

        let client_auth = match client_auth {
            Some(auth) => {
                trace!("auth present and valid");

                auth
            }
            None => {
                trace!("missing or invalid auth, creating");

                TransportProvider::create_credentials(self.state, &api, self.args.client_id).await?
            }
        };

        let expiry = client_auth.validity_not_after();

        // NOTE: this will be updated even if returned None, since otherwise we would keep the
        //       previous expiry
        self.state.set_cert_expiry(expiry).await;

        let broker_url = api.get_broker_url().await?;

        if broker_url.scheme() == "mqtt" {
            notify_security_event(SecurityEvent::AlarmUnsecureCommunication);
        }

        self.create_connection(&broker_url, client_auth).await
    }

    pub(crate) async fn connect(
        &mut self,
        connection: &mut Connection,
    ) -> Result<(), Error<PairingApiError>> {
        let api = ApiClient::create(self.args, self.state.config(), self.state.tls().clone())?;

        let store_dir = self.state.config().writable_dir.as_deref();
        let client_auth =
            TransportProvider::retrieve_credentials(&api, self.args.client_id, store_dir)
                .await
                .inspect_err(|err| {
                    error!(error = %Report::new(err),"couldn't pair device");
                })?;

        let expiry = match client_auth {
            Some(client_auth) => {
                debug!("auth present and valid");

                client_auth.validity_not_after()
            }
            None => {
                debug!("connection present, but auth invalid");

                let client_auth =
                    TransportProvider::create_credentials(self.state, &api, self.args.client_id)
                        .await?;

                let expiry = client_auth.validity_not_after();

                let transport = TransportProvider::config_transport(self.state, client_auth)?;

                connection.set_transport(transport);

                expiry
            }
        };

        // NOTE: this will be updated even if returned None, since otherwise we would keep the
        //       previous expiry
        self.state.set_cert_expiry(expiry).await;

        Ok(())
    }

    async fn create_connection(
        &mut self,
        broker_url: &Url,
        client_auth: ClientAuth,
    ) -> Result<(AsyncClient, EventLoop), Error<PairingApiError>> {
        let config = self.state.config();

        let transport = TransportProvider::config_transport(self.state, client_auth)?;

        let (mqtt_opts, net_opts) =
            self.build_mqtt_opts(transport, broker_url, config.connection_timeout)?;

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, config.channel_size.get());

        eventloop.set_network_options(net_opts);

        Ok((client, eventloop))
    }

    /// Builds the options to connect to the broker
    pub(crate) fn build_mqtt_opts(
        &self,
        transport: Transport,
        broker_url: &Url,
        timeout: Duration,
    ) -> Result<(MqttOptions, NetworkOptions), Error<PairingApiError>> {
        let host = broker_url.host_str().ok_or_else(|| {
            Error::with(PairingApiError::InvalidArgument, "missing host in url")
                .set_ctx(broker_url.to_string())
        })?;
        let port = broker_url.port().ok_or_else(|| {
            Error::with(PairingApiError::InvalidArgument, "missing port in url")
                .set_ctx(broker_url.to_string())
        })?;

        let mut mqtt_opts = MqttOptions::new(self.args.client_id.to_string(), host, port);

        let keep_alive = self.keepalive.as_secs();
        let conn_timeout = timeout.as_secs();
        if keep_alive >= conn_timeout {
            return Err(Error::with(
                PairingApiError::InvalidArgument,
                "keep alive should be lessa than the connection timeout",
            )
            .set_ctx(format!(
                "got keep alive ({keep_alive}s) and connection timeout {conn_timeout}s)"
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
