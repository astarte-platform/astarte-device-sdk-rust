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

use tracing::{debug, error};

use crate::error::Report;
use crate::pairing::PairingConfig;
use crate::pairing::api::client::{ApiClient, ClientArgs};
use crate::transport::mqtt::{ClientSender, PairingError};

use super::context::{Connection, Ctx};

/// Disconnected state.
///
/// We return to this state after any errors from the connection. Its is safe since
/// `rumqttc::Eventloop` will always call `self.clear()` after an error is polled, setting
/// `self.network` to `None`. So next poll we will always receive a ConnAck packet.
#[derive(Debug)]
pub(super) struct Disconnected {
    pub(super) connection: Option<Connection>,
}

impl Disconnected {
    pub(super) async fn connect<S>(
        &mut self,
        ctx: &mut Ctx<'_, S>,
        cfg: &PairingConfig,
    ) -> Result<&mut Connection, PairingError> {
        let args = ClientArgs {
            realm: &cfg.client_id.realm,
            device_id: &cfg.client_id.device_id,
            pairing_url: &cfg.pairing_url,
            token: &cfg.secret,
        };

        let api = ApiClient::from_transport(&ctx.state.config, ctx.provider, args)?;

        let client_auth = ctx
            .provider
            .retrieve_credentials(&api, cfg.client_id.as_ref())
            .await
            .inspect_err(|err| {
                error!(error = %Report::new(err),"couldn't pair device");
            })?;

        let (connection, expiry) = match (&mut self.connection, client_auth) {
            (Some(connection), Some(client_auth)) => {
                debug!("connection and auth present and valid");

                (connection, client_auth.validity_not_after())
            }
            (Some(connection), None) => {
                debug!("connection present, but auth invalid");

                let client_auth = ctx
                    .provider
                    .create_credentials(&api, cfg.client_id.as_ref())
                    .await?;

                let expiry = client_auth.validity_not_after();

                let transport = ctx.provider.config_transport(client_auth)?;

                connection.set_transport(transport);

                (connection, expiry)
            }
            (conn @ None, Some(client_auth)) => {
                debug!("connection missing, but auth present");

                let expiry = client_auth.validity_not_after();

                let transport = ctx.provider.config_transport(client_auth)?;

                (
                    Self::create_connection(conn, ctx, cfg, &api, transport).await?,
                    expiry,
                )
            }
            (conn @ None, None) => {
                debug!("creating new connection");

                let client_auth = ctx
                    .provider
                    .create_credentials(&api, cfg.client_id.as_ref())
                    .await?;

                let expiry = client_auth.validity_not_after();

                let transport = ctx.provider.config_transport(client_auth)?;

                (
                    Self::create_connection(conn, ctx, cfg, &api, transport).await?,
                    expiry,
                )
            }
        };

        // NOTE: this will be updated even if returned None, since otherwise we would keep the
        //       previous expiry
        *ctx.state.cert_expiry.write().await = expiry;

        Ok(connection)
    }

    async fn create_connection<'a, S>(
        connection: &'a mut Option<Connection>,
        ctx: &Ctx<'_, S>,
        cfg: &PairingConfig,
        api: &ApiClient<'_>,
        transport: rumqttc::Transport,
    ) -> Result<&'a mut Connection, PairingError> {
        let broker_url = api.get_broker_url().await?;

        let (mqtt_opts, net_opts) =
            cfg.build_mqtt_opts(transport, &broker_url, ctx.state.config.connection_timeout)?;

        let new = Connection::new(&ctx.state.config, mqtt_opts, net_opts);

        let client_sender = ClientSender {
            id: cfg.client_id.clone(),
            client: new.client.clone(),
        };

        if ctx.sender.set(client_sender).is_err() {
            error!("couldn't set the client sender, already set");
        }

        Ok(connection.insert(new))
    }
}
