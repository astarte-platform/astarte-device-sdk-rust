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

use std::fmt::Debug;
use std::ops::ControlFlow;

use astarte_device_error::{Error, ResultExt, WrapError};
use astarte_interfaces::Interface;
use astarte_interfaces::schema::Reliability;
use rumqttc::{AckOfPub, QoS, Token};
use tracing::{debug, info, instrument};

use super::components::ClientId;
use super::deps::AsyncClient;
use super::{Introspection, Sender, ValidatedProperty, payload};
use crate::error::{AstarteError, ErrorKind};
use crate::interfaces::{self, DeviceIntrospection, Interfaces};
use crate::retention::{PublishInfo, RetentionId, StoredRetention};
use crate::session::{IntrospectionInterface, StoredSession};
use crate::state::ConnectionState;
use crate::store::{OptStoredProp, PropertyState, StoreCapabilities};
use crate::transport::{Encode, RemovedInterface};
use crate::validate::individual::ValidatedIndividual;
use crate::validate::object::ValidatedObject;
use crate::validate::properties::ValidatedUnset;

use super::{components::to_qos, error::MqttError, retention::RetSender};

pub(crate) mod handshake;

/// Struct representing an MQTT connection handler for an Astarte device.
///
/// It manages the interaction with the MQTT broker, handling connections, subscriptions, and
/// message publishing following the Astarte protocol.
#[derive(Debug, Clone)]
pub struct MqttClient {
    pub(crate) id: ClientId,
    pub(crate) sender: AsyncClient,
    pub(crate) retention: RetSender,
    pub(crate) session_synced: bool,
}

impl MqttClient {
    /// Send a binary payload over this mqtt connection.
    async fn send(
        &self,
        interface: &str,
        path: &str,
        reliability: QoS,
        payload: Vec<u8>,
    ) -> Result<Token<AckOfPub>, Error<MqttError>> {
        self.sender
            .publish(
                format!("{}/{interface}{path}", self.id),
                reliability,
                false,
                payload,
            )
            .await
            .wrap_err_msg(MqttError::Publish, "while sending")
    }

    async fn subscribe(&self, interface_name: &str) -> Result<(), Error<MqttError>> {
        let topic = self.id.make_interface_wildcard(interface_name);

        self.sender
            .subscribe(topic, QoS::ExactlyOnce)
            .await
            .wrap_err(MqttError::Subscribe)?;

        Ok(())
    }

    async fn unsubscribe(&self, interface_name: &str) -> Result<(), Error<MqttError>> {
        let topic = self.id.make_interface_wildcard(interface_name);

        self.sender
            .unsubscribe(topic)
            .await
            .wrap_err(MqttError::Unsubscribe)?;

        Ok(())
    }

    pub(crate) async fn send_introspection(
        &self,
        introspection: String,
    ) -> Result<(), Error<MqttError>> {
        debug!(introspection, "sending introspection:");

        let path = self.id.to_string();

        self.sender
            .publish(path, QoS::ExactlyOnce, false, introspection)
            .await
            .wrap_err_msg(MqttError::Publish, "sending introspection")?;

        Ok(())
    }

    async fn mark_received<S>(
        &self,
        state: &ConnectionState<S>,
        id: &RetentionId,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        match id {
            RetentionId::Volatile(id) => {
                state.volatile_store().mark_received(id).await;
            }
            RetentionId::Stored(id) => {
                if let Some(retention) = state.store().get_retention() {
                    retention
                        .mark_received(id)
                        .await
                        .map_kind(ErrorKind::Retention)?;
                }
            }
        }

        Ok(())
    }

    async fn mark_sent<S>(
        &self,
        state: &ConnectionState<S>,
        id: RetentionId,
        reliability: Reliability,
        notice: Token<AckOfPub>,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        match reliability {
            // Since it's Unreliable we will never know the broker received it
            Reliability::Unreliable => {
                self.mark_received(state, &id).await?;
            }
            Reliability::Guaranteed | Reliability::Unique => {
                self.retention
                    .send((id, notice))
                    .await
                    .wrap_err_msg(ErrorKind::Disconnected, "while sending to retention")?;

                match id {
                    RetentionId::Volatile(id) => {
                        state.volatile_store().mark_sent(&id, true).await;
                    }
                    RetentionId::Stored(id) => {
                        if let Some(retention) = state.store().get_retention() {
                            retention
                                .update_sent_flag(&id, true)
                                .await
                                .map_kind(ErrorKind::Retention)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Sender for MqttClient {
    #[instrument(skip(self, state, interfaces))]
    async fn handshake<S>(
        &mut self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
        session_present: bool,
    ) -> Result<ControlFlow<()>, AstarteError>
    where
        S: StoreCapabilities,
    {
        self.handshake_impl(state, interfaces, session_present)
            .await
    }

    async fn disconnect(&mut self) -> Result<(), AstarteError> {
        self.sender
            .disconnect()
            .await
            .wrap_err(ErrorKind::Mqtt(MqttError::Disconnect))?
            .await
            .wrap_err(ErrorKind::Mqtt(MqttError::Disconnect))?;

        info!("disconnect packet sent");

        Ok(())
    }

    async fn send_individual(
        &mut self,
        validated: ValidatedIndividual,
    ) -> Result<(), AstarteError> {
        let buf = payload::serialize_individual(&validated.data, validated.timestamp)
            .map_kind(|k| ErrorKind::Mqtt(MqttError::Payload(k)))?;

        self.send(
            &validated.interface,
            &validated.path,
            to_qos(validated.reliability),
            buf,
        )
        .await
        .map_kind(ErrorKind::Mqtt)?;

        Ok(())
    }

    async fn send_property<S>(
        &mut self,
        state: &ConnectionState<S>,
        validated: ValidatedProperty,
        epoch: u8,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        let buf = payload::serialize_individual(&validated.data, None)
            .map_kind(|k| ErrorKind::Mqtt(MqttError::Payload(k)))?;

        self.send(&validated.interface, &validated.path, QoS::ExactlyOnce, buf)
            .await
            .map_kind(ErrorKind::Mqtt)?;

        state
            .store()
            .update_state(
                &validated.interface,
                &validated.path,
                PropertyState::Completed,
                epoch,
            )
            .await
            .map_kind(ErrorKind::Store)?;

        Ok(())
    }

    async fn send_object(&mut self, validated: ValidatedObject) -> Result<(), AstarteError> {
        let buf = payload::serialize_object(&validated.data, validated.timestamp)
            .map_kind(|k| ErrorKind::Mqtt(MqttError::Payload(k)))?;

        self.send(
            &validated.interface,
            &validated.path,
            to_qos(validated.reliability),
            buf,
        )
        .await
        .map_kind(ErrorKind::Mqtt)?;

        Ok(())
    }

    async fn send_individual_stored<S>(
        &mut self,
        state: &ConnectionState<S>,
        id: RetentionId,
        validated: ValidatedIndividual,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        debug_assert!(
            !validated.retention.is_discard(),
            "send stored called for retention discard"
        );

        let buf = payload::serialize_individual(&validated.data, validated.timestamp)
            .map_kind(|k| ErrorKind::Mqtt(MqttError::Payload(k)))?;

        let notice = self
            .send(
                &validated.interface,
                &validated.path,
                to_qos(validated.reliability),
                buf,
            )
            .await
            .map_kind(ErrorKind::Mqtt)?;

        self.mark_sent(state, id, validated.reliability, notice)
            .await?;

        Ok(())
    }

    async fn send_object_stored<S>(
        &mut self,
        state: &ConnectionState<S>,
        id: RetentionId,
        validated: ValidatedObject,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        debug_assert!(
            !validated.retention.is_discard(),
            "send stored called for retention discard"
        );

        let buf = payload::serialize_object(&validated.data, validated.timestamp)
            .map_kind(|k| ErrorKind::Mqtt(MqttError::Payload(k)))?;

        let notice = self
            .send(
                &validated.interface,
                &validated.path,
                to_qos(validated.reliability),
                buf,
            )
            .await
            .map_kind(ErrorKind::Mqtt)?;

        self.mark_sent(state, id, validated.reliability, notice)
            .await?;

        Ok(())
    }

    async fn resend_stored<S>(
        &mut self,
        state: &ConnectionState<S>,
        id: RetentionId,
        data: PublishInfo<'_>,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        debug_assert!(
            state.store().get_retention().is_some(),
            "resend stored called without store that supports retention"
        );

        let notice = self
            .send(
                &data.interface,
                &data.path,
                to_qos(data.reliability),
                data.value.into(),
            )
            .await
            .map_kind(ErrorKind::Mqtt)?;

        self.mark_sent(state, id, data.reliability, notice).await?;

        Ok(())
    }

    /// Resend previously stored property.
    async fn resend_stored_property<S>(
        &mut self,
        state: &ConnectionState<S>,
        property_data: OptStoredProp,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        let buf = property_data
            .value
            .as_ref()
            .map(|d| payload::serialize_individual(d, None))
            .unwrap_or(Ok(Vec::new()))
            .map_kind(|k| ErrorKind::Mqtt(MqttError::Payload(k)))?;

        self.send(
            &property_data.interface,
            &property_data.path,
            QoS::ExactlyOnce,
            buf,
        )
        .await
        .map_kind(ErrorKind::Mqtt)?;

        state
            .store()
            .update_state(
                &property_data.interface,
                &property_data.path,
                PropertyState::Completed,
                property_data.epoch(),
            )
            .await
            .map_kind(ErrorKind::Store)?;

        Ok(())
    }

    async fn unset<S>(
        &mut self,
        state: &ConnectionState<S>,
        validated: ValidatedUnset,
        epoch: u8,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        // We send an empty vector as payload to unset the property, https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#payload-format
        self.send(
            &validated.interface,
            &validated.path,
            QoS::ExactlyOnce,
            Vec::new(),
        )
        .await
        .map_kind(ErrorKind::Mqtt)?;

        state
            .store()
            .update_state(
                &validated.interface,
                &validated.path,
                PropertyState::Completed,
                epoch,
            )
            .await
            .map_kind(ErrorKind::Store)?;

        Ok(())
    }
}

impl Introspection for MqttClient {
    async fn add_interface<S>(
        &mut self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
        added: &Interface,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        if added.ownership().is_server() {
            self.subscribe(added.interface_name())
                .await
                .map_kind(ErrorKind::Mqtt)?
        }

        let introspection = DeviceIntrospection::new(interfaces.iter_with_added(added)).to_string();

        self.send_introspection(introspection)
            .await
            .wrap_err_msg(ErrorKind::Mqtt(MqttError::Publish), "send introspection")?;

        if let Some(session) = state.store().get_session() {
            let interface: IntrospectionInterface<&str> = added.into();
            session
                .add_interfaces(&[interface])
                .await
                .map_kind(ErrorKind::Session)?;
        }

        Ok(())
    }

    async fn remove_interface<S>(
        &mut self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
        removed: &RemovedInterface,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        let iter = interfaces.iter_without_removed(removed);
        let introspection = DeviceIntrospection::new(iter).to_string();

        self.send_introspection(introspection).await.wrap_err_msg(
            ErrorKind::Mqtt(MqttError::Publish),
            "while sending introspection",
        )?;

        if removed.ownership().is_server() {
            self.unsubscribe(removed.interface_name())
                .await
                .map_kind(ErrorKind::Mqtt)?;
        }

        if let Some(session) = state.store().get_session() {
            let interface = IntrospectionInterface::<&str>::from(removed);

            session
                .remove_interfaces(&[interface])
                .await
                .map_kind(ErrorKind::Session)?;
        }

        Ok(())
    }

    /// Called when multiple interfaces are added.
    ///
    /// This method should convey to the server that one or more interfaces have been added.
    async fn extend_interfaces<S>(
        &mut self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
        added: &interfaces::ValidatedCollection,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        let server_interfaces = added
            .values()
            .filter_map(|i| {
                if i.ownership().is_server() {
                    Some(i.interface_name())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // TODO: subscribe to many interfaces, needs max payload
        for interface in server_interfaces {
            self.subscribe(interface).await.map_kind(ErrorKind::Mqtt)?;
        }

        let introspection =
            DeviceIntrospection::new(interfaces.iter_with_added_many(added)).to_string();

        self.send_introspection(introspection)
            .await
            .map_kind(ErrorKind::Mqtt)?;

        if let Some(session) = state.store().get_session() {
            let added: Vec<IntrospectionInterface<&str>> =
                added.iter_interfaces().map(|i| i.into()).collect();

            session
                .add_interfaces(&added)
                .await
                .map_kind(ErrorKind::Session)?;
        }

        Ok(())
    }

    async fn remove_interfaces<S>(
        &mut self,
        state: &ConnectionState<S>,
        interfaces: &Interfaces,
        removed: &[RemovedInterface],
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        let interfaces = interfaces.iter_without_removed_many(removed);
        let introspection = DeviceIntrospection::new(interfaces).to_string();

        self.send_introspection(introspection)
            .await
            .map_kind(ErrorKind::Mqtt)?;

        for iface in removed {
            if iface.ownership().is_server() {
                self.unsubscribe(iface.interface_name())
                    .await
                    .map_kind(ErrorKind::Mqtt)?;
            }
        }

        if let Some(session) = state.store().get_session() {
            let removed: Vec<IntrospectionInterface<&str>> =
                removed.iter().map(|i| i.into()).collect();

            session
                .remove_interfaces(&removed)
                .await
                .map_kind(ErrorKind::Session)?;
        }

        Ok(())
    }
}

/// Encoder for mqtt messages
#[derive(Debug, Clone, Copy)]
pub struct MqttEncoder {}

impl Encode for MqttEncoder {
    fn serialize_individual(
        &self,
        validated: &ValidatedIndividual,
    ) -> Result<Vec<u8>, AstarteError> {
        payload::serialize_individual(&validated.data, validated.timestamp)
            .map_kind(|err| ErrorKind::Mqtt(MqttError::Payload(err)))
    }

    fn serialize_object(&self, validated: &ValidatedObject) -> Result<Vec<u8>, AstarteError> {
        payload::serialize_object(&validated.data, validated.timestamp)
            .map_kind(|err| ErrorKind::Mqtt(MqttError::Payload(err)))
    }
}
