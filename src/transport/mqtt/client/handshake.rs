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

use std::collections::HashSet;
use std::num::NonZero;
use std::ops::ControlFlow;

use astarte_device_error::{Error, ResultExt, WrapError};
use astarte_interfaces::schema::Ownership;
use rumqttc::QoS;
use tracing::{debug, error};

use crate::error::{AstarteError, ErrorKind};
use crate::interfaces::Interfaces;
use crate::properties::encode_set_properties;
use crate::state::ConnectionState;
use crate::store::{PropertyStore, StoreCapabilities};
use crate::transport::mqtt::error::MqttError;

use super::MqttClient;

impl MqttClient {
    pub(super) async fn handshake_impl<S>(
        &mut self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
        session_present: bool,
    ) -> Result<ControlFlow<()>, AstarteError>
    where
        S: StoreCapabilities,
    {
        if session_present & self.session_synced {
            debug!("session already synchronized");

            return Ok(ControlFlow::Break(()));
        }

        debug!(
            session_present = session_present,
            session_sync = self.session_synced,
            "perform again handshake to synchronize the device",
        );

        self.full_handshake(state, interfaces)
            .await
            .map_kind(ErrorKind::Mqtt)?;

        debug!("handshake sent");

        Ok(ControlFlow::Break(()))
    }

    async fn full_handshake<S>(
        &mut self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
    ) -> Result<(), Error<MqttError>>
    where
        S: StoreCapabilities,
    {
        self.subscribe_server_interfaces(interfaces).await?;

        self.send_introspection(interfaces.get_introspection_string())
            .await?;

        self.send_empty_cache().await?;

        self.purge_device_properties(state, interfaces).await?;

        Ok(())
    }

    /// Subscribes to the passed list of interfaces
    async fn subscribe_server_interfaces(
        &self,
        interfaces: &Interfaces,
    ) -> Result<(), Error<MqttError>> {
        debug!("subscribing server properties");

        self.sender
            .subscribe(
                format!("{}/control/consumer/properties", self.id),
                QoS::ExactlyOnce,
            )
            .await
            .wrap_err_msg(MqttError::Subscribe, "subscribe consumer properties")?;

        for interface in interfaces
            .iter()
            .filter(|i| i.ownership() == Ownership::Server)
        {
            let if_name = interface.interface_name();

            debug!(interface = if_name, "subscribing on interface");

            self.subscribe(if_name).await.wrap_err_with(|_| {
                Error::with(MqttError::Subscribe, "server interface").set_ctx(if_name.to_string())
            })?;
        }

        Ok(())
    }

    /// Sends the empty cache command as per the astarte protocol definition
    async fn send_empty_cache(&self) -> Result<(), Error<MqttError>> {
        debug!("sending emptyCache");

        self.sender
            .publish(
                format!("{}/control/emptyCache", self.id),
                QoS::ExactlyOnce,
                false,
                "1",
            )
            .await
            .wrap_err_msg(MqttError::Publish, "empty cache")?;

        Ok(())
    }

    /// Sends the passed device owned properties
    async fn purge_device_properties<S>(
        &self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
    ) -> Result<(), Error<MqttError>>
    where
        S: StoreCapabilities,
    {
        debug!("sending purge device properties");

        let device_properties = Self::load_set_device_properties(
            interfaces,
            state.store(),
            state.config().channel_size,
        )
        .await;
        let payload = encode_set_properties(&device_properties).map_kind(MqttError::PurgeProp)?;

        self.sender
            .publish(
                format!("{}/control/producer/properties", self.id),
                QoS::ExactlyOnce,
                false,
                payload,
            )
            .await
            .wrap_err_msg(MqttError::Publish, "purge device properties")?;

        Ok(())
    }

    async fn load_set_device_properties<S>(
        interfaces: &Interfaces,
        store: &S,
        limit: NonZero<usize>,
    ) -> HashSet<String>
    where
        S: PropertyStore,
    {
        let mut last_updated_at = None;

        let mut set_props = HashSet::new();

        loop {
            let props = match store.device_props(limit, last_updated_at).await {
                Ok(props) => props,
                Err(error) => {
                    error!(%error, "error while loading device properties from the store");

                    // Return the incomplete list since we will republish all the set properties
                    return set_props;
                }
            };

            debug!(loaded_properties = props.len());

            if props.is_empty() {
                return set_props;
            }

            for prop in props {
                last_updated_at = Some(prop.updated_at());

                if interfaces.has_property(&prop.interface, prop.interface_major()) {
                    set_props.insert(format!("{}{}", prop.interface, prop.path));
                }
            }
        }
    }
}
