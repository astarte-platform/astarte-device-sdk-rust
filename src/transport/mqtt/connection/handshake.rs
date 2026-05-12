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

use astarte_device_error::{Error, ResultExt, WrapError};
use rumqttc::QoS;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, trace};

use crate::properties::encode_set_properties;
use crate::session::StoredSession;
use crate::store::{PropertyStore, StoreCapabilities};
use crate::transport::mqtt::client::AsyncClient;
use crate::transport::mqtt::components::ClientId;
use crate::transport::mqtt::error::MqttError;
use crate::transport::mqtt::{AsyncClientExt, SessionData};

use super::context::{ConnCtx, Connection};
use super::wait_sends::TaskHandle;

#[derive(Debug)]
pub(super) struct Handshake<'a> {
    pub(super) session_present: bool,
    pub(super) connection: &'a mut Connection,
}

impl<'a> Handshake<'a> {
    pub(super) async fn start<S>(
        &mut self,
        ctx: &mut ConnCtx<'_, S>,
        client_id: ClientId<&'_ str>,
    ) -> Option<TaskHandle>
    where
        S: PropertyStore + StoreCapabilities,
    {
        if self.session_present && ctx.session_synced {
            debug!("session already synchronized");

            return None;
        }

        debug!(
            session_present = self.session_present,
            session_sync = ctx.session_synced,
            "perform again handshake to synchronize the device",
        );

        // NOTE set session synced to false since we are not synchronized
        //      also clear the stored introspection so it can be updated
        //      set to true after a successful handshake in WaitAcks.
        ctx.session_synced = false;

        let session_data = SessionData::from_props(ctx.interfaces, ctx.store).await;

        if let Some(session) = ctx.store.get_session() {
            trace!("Clearing stored introspection before the full handshake");

            session.clear_introspection().await;
        }

        Some(Self::full_handshake(
            self.connection.client.clone(),
            client_id.into(),
            ctx.store.clone(),
            session_data,
        ))
    }

    fn full_handshake<S>(
        client: AsyncClient,
        client_id: ClientId,
        store: S,
        session_data: SessionData,
    ) -> TaskHandle
    where
        S: PropertyStore + StoreCapabilities,
    {
        let handle = tokio::spawn(async move {
            let client_id = client_id.as_ref();
            Self::subscribe_server_interfaces(&client, client_id, &session_data.server_interfaces)
                .await?;

            client
                .send_introspection(client_id, session_data.interfaces)
                .await?;

            if let Some(session) = store.get_session() {
                trace!("Introspection sent successfully, storing");

                session
                    .store_introspection(&session_data.interfaces_stored)
                    .await;
            }

            Self::send_empty_cache(&client, client_id).await?;

            Self::purge_device_properties(&client, client_id, &session_data.device_properties)
                .await?;

            Ok(())
        });

        AbortOnDropHandle::new(handle)
    }

    /// Subscribes to the passed list of interfaces
    async fn subscribe_server_interfaces(
        client: &AsyncClient,
        client_id: ClientId<&str>,
        server_interfaces: &[String],
    ) -> Result<(), Error<MqttError>> {
        debug!("subscribing server properties");

        client
            .subscribe(
                format!("{client_id}/control/consumer/properties"),
                QoS::ExactlyOnce,
            )
            .await
            .wrap_err_msg(MqttError::Subscribe, "subscribe consumer properties")?;

        debug!(
            "subscribing on {} server interfaces",
            server_interfaces.len()
        );

        client
            .subscribe_interfaces(client_id, server_interfaces)
            .await
            .wrap_err_msg(MqttError::Subscribe, "subscribe server interface")?;

        Ok(())
    }

    /// Sends the empty cache command as per the astarte protocol definition
    async fn send_empty_cache(
        client: &AsyncClient,
        client_id: ClientId<&str>,
    ) -> Result<(), Error<MqttError>> {
        debug!("sending emptyCache");

        client
            .publish(
                format!("{client_id}/control/emptyCache"),
                QoS::ExactlyOnce,
                false,
                "1",
            )
            .await
            .wrap_err_msg(MqttError::Publish, "empty cache")?;

        Ok(())
    }

    /// Sends the passed device owned properties
    async fn purge_device_properties(
        client: &AsyncClient,
        client_id: ClientId<&str>,
        device_properties: &[String],
    ) -> Result<(), Error<MqttError>> {
        let payload = encode_set_properties(device_properties).map_kind(MqttError::PurgeProp)?;

        client
            .publish(
                format!("{client_id}/control/producer/properties"),
                QoS::ExactlyOnce,
                false,
                payload,
            )
            .await
            .wrap_err_msg(MqttError::Publish, "purge device properties")?;

        Ok(())
    }
}
