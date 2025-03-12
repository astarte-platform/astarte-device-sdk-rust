// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Separates the state and logic of the MQTT connection logic from sending or receiving events.
//!
//! ### Architecture Overview
//!
//! This documentation outlines the principles and procedures for establishing and maintaining
//! connections, handling introspection changes, and managing incoming messages.
//!
//! ####  Never Error
//!
//! The system must never error. Instead, it should attempt to re-connect to Astarte while allowing
//! clients to enqueue messages seamlessly. This ensures continuity and robust message handling.
//!
//! #### Block Only for Introspection Changes
//!
//! Blocking should only occur when introspection is modified (i.e. when an interface is added or
//! removed). This blocking must resolve before handling new messages.
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

use std::{
    collections::VecDeque,
    fmt::{Debug, Display},
    time::Duration,
};

use rumqttc::{
    mqttbytes, ClientError, ConnectionError, Event, Packet, Publish, QoS, StateError, TokenError,
    Transport,
};
use sync_wrapper::SyncWrapper;
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, error, info, trace, warn};

use crate::{
    error::Report,
    interfaces::Interfaces,
    properties::{encode_set_properties, PropertiesError},
    retry::ExponentialIter,
    store::{error::StoreError, wrapper::StoreWrapper, OptStoredProp, PropertyStore},
    transport::mqtt::{pairing::ApiClient, payload::Payload, AsyncClientExt},
};

use super::{
    client::{AsyncClient, EventLoop},
    config::transport::TransportProvider,
    ClientId, PairingError, PayloadError, SessionData,
};

/// Errors while initializing the MQTT connection.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
enum InitError {
    /// Couldn't send the message.
    #[error("couldn't send messages for {ctx}")]
    Client {
        #[source]
        backtrace: ClientError,
        ctx: &'static str,
    },
    /// Couldn't wait for the message acknowledgment.
    #[error("couldn't wait the message acknowledgment for {ctx}")]
    Ack {
        #[source]
        backtrace: TokenError,
        ctx: &'static str,
    },
    /// Couldn't serialize the device property payload.
    #[error("coudln't serialize the device property payload")]
    Payload(#[from] PayloadError),
    /// Couldn't delete the unset property
    #[error("coudln't delete the unset property")]
    Unset(#[source] StoreError),
    /// Couldn't send purge device properties
    #[error("couldn't send purge properties")]
    PurgeProperties(#[source] PropertiesError),
}

/// Error while polling the connection
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum PollError {
    /// Couldn't reconnect to Astarte
    #[error("couldn't reconnect to Astarte")]
    Pairing(#[from] PairingError),
    /// Couldn't complete a store operation
    #[error("store operation failed")]
    Store(#[from] StoreError),
}

impl InitError {
    const fn client(ctx: &'static str) -> impl Fn(ClientError) -> InitError {
        move |backtrace: ClientError| InitError::Client { backtrace, ctx }
    }

    const fn ack(ctx: &'static str) -> impl Fn(TokenError) -> InitError {
        move |backtrace: TokenError| InitError::Ack { backtrace, ctx }
    }
}

/// MQTT connection to Astarte that can be pulled to receive packets.
#[derive(Debug)]
pub(crate) struct MqttConnection {
    connection: Connection,
    /// Queue for the packet published while re/connecting.
    buff: VecDeque<Publish>,
    state: State,
}

impl MqttConnection {
    pub(crate) fn new(
        client: AsyncClient,
        eventloop: EventLoop,
        provider: TransportProvider,
        state: impl Into<State>,
    ) -> Self {
        let connection = Connection {
            client,
            eventloop: SyncWrapper::new(eventloop),
            provider,
        };

        Self {
            connection,
            buff: VecDeque::new(),
            state: state.into(),
        }
    }

    /// Iterate for the next publish event from the connection.
    ///
    /// Return [`None`] when disconnected.
    pub(crate) async fn next_publish(&mut self) -> Option<Publish> {
        // Check if there are backed up publishes.
        if let Some(publish) = self.buff.pop_front() {
            return Some(publish);
        }

        // Here we only get the connected state so we don't need to pass all the arguments, like the
        // interfaces or the store.
        let State::Connected(connected) = &mut self.state else {
            return None;
        };

        loop {
            match connected.poll(&mut self.connection).await {
                Next::Same => {}
                Next::Publish(publish) => return Some(publish),
                Next::State(next) => {
                    debug_assert!(!next.is_connected());

                    self.state = next;

                    return None;
                }
            }
        }
    }

    /// Wait for the connection to be established to the Astarte.
    ///
    /// This function assumes that the connection is valid, but will handle disconnection and
    /// reconnection automatically.
    pub(crate) async fn wait_connack<S>(
        client: AsyncClient,
        eventloop: EventLoop,
        provider: TransportProvider,
        client_id: ClientId<&str>,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> Result<Self, PollError>
    where
        S: PropertyStore,
    {
        let mut mqtt_connection = Self::new(client, eventloop, provider, Connecting);

        mqtt_connection
            .connect(client_id, interfaces, store)
            .await?;

        Ok(mqtt_connection)
    }

    /// Connect to astarte, wait till the state is [`Connected`].
    pub(crate) async fn connect<S>(
        &mut self,
        client_id: ClientId<&str>,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> Result<(), PollError>
    where
        S: PropertyStore,
    {
        let mut exp_back = ExponentialIter::default();

        // Wait till we are in the Init state, so we do not need to handle the incoming publishes,
        // but all the initialization packets are being queued.
        while !self.state.is_connected() {
            let opt_publish = self
                .state
                .poll(&mut self.connection, client_id, interfaces, store)
                .await?;

            if let Some(publish) = opt_publish {
                debug!("received a publish");

                disable_clean_session(self.connection.eventloop_mut());

                self.buff.push_back(publish);
            }

            // Check if an error occurred
            if self.state.is_disconnected() {
                let timeout = exp_back.next();

                debug!("waiting {timeout} seconds before retrying");

                tokio::time::sleep(Duration::from_secs(timeout)).await;
            }
        }

        disable_clean_session(self.connection.eventloop_mut());

        Ok(())
    }
}

#[cfg(not(test))]
fn disable_clean_session(eventloop: &mut EventLoop) {
    eventloop.mqtt_options.set_clean_session(false);
}

#[cfg(test)]
fn disable_clean_session(_eventloop: &mut EventLoop) {}

/// Struct to hold the connection and client to be passed to the state.
///
/// We pass a mutable reference to the connection from outside the state so we can freely move from
/// one state to the other, returning a new one. This without having to move the connection from
/// behind the mutable reference.
#[derive(Debug)]
struct Connection {
    client: AsyncClient,
    // NOTE: this should be replaces by Exclusive<EventLoop> when the feature `exclusive_wrapper`
    //       is stabilized or the EventLoop becomes Sync
    //       https://doc.rust-lang.org/std/sync/struct.Exclusive.html
    eventloop: SyncWrapper<EventLoop>,
    provider: TransportProvider,
}

impl Connection {
    fn eventloop_mut(&mut self) -> &mut EventLoop {
        self.eventloop.get_mut()
    }
}

/// This cannot be a type state machine, because any additional data cannot be moved out of the enum
/// when polling. The only data that can be easily changed is the current state into the next one.
#[derive(Debug)]
pub(crate) enum State {
    /// The device is disconnected from Astarte, it will need to recreate the connection.
    Disconnected(Disconnected),
    /// The CONNECT packet has been sent, we need to wait for the ConACK packet.
    Connecting(Connecting),
    /// A connection has been established with the broker. We need to publish and subscribe to the
    /// information need for the communication with Astarte.
    ///
    /// See the documentation for [Connect and Disconnect](https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#connection-and-disconnection).
    Handshake(Handshake),
    /// The publish and subscribe packets have been sent, we need to wait for all of them to be
    /// ACKed.
    WaitAcks(WaitAcks),
    /// Connected with Astarte.
    Connected(Connected),
}

impl State {
    async fn poll<S>(
        &mut self,
        conn: &mut Connection,
        client_id: ClientId<&str>,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> Result<Option<Publish>, PollError>
    where
        S: PropertyStore,
    {
        trace!("state {}", self);

        let next = match self {
            State::Disconnected(disconnected) => disconnected.reconnect(conn, client_id).await?,
            State::Connecting(connecting) => connecting.wait_connack(conn).await,
            State::Handshake(handshake) => {
                let session_data = SessionData::try_from_props(interfaces, store).await?;

                handshake.start(conn, client_id, store, session_data)
            }
            State::WaitAcks(init) => init.wait_connection(conn).await?,
            State::Connected(connected) => connected.poll(conn).await,
        };

        let res = match next {
            Next::Same => None,
            Next::Publish(publish) => Some(publish),
            Next::State(state) => {
                *self = state;

                None
            }
        };

        Ok(res)
    }

    /// Returns `true` if the state is [`Connected`].
    ///
    /// [`Connected`]: State::Connected
    #[must_use]
    fn is_connected(&self) -> bool {
        matches!(self, Self::Connected(..))
    }

    /// Returns `true` if the state is [`Disconnected`].
    ///
    /// [`Disconnected`]: State::Disconnected
    #[must_use]
    pub(crate) fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(..))
    }
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            State::Disconnected(_) => "Disconnected",
            State::Connecting(_) => "Connecting",
            State::Handshake(_) => "Handshake",
            State::WaitAcks(_) => "WaitAcks",
            State::Connected(_) => "Connected",
        };

        write!(f, "{state}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Disconnected;

impl Disconnected {
    /// Recreate the MQTT transport (TLS) to the broker.
    ///
    /// It recreates the credentials and reconnect to the broker, using the same
    /// session. If it fails, it returns an error so that the whole connection process can
    /// be retried.
    async fn reconnect(
        &mut self,
        conn: &mut Connection,
        client_id: ClientId<&str>,
    ) -> Result<Next, PairingError> {
        let api = ApiClient::from_transport(&conn.provider, client_id.realm, client_id.device_id)?;

        let transport = match conn.provider.recreate_transport(&api).await {
            Ok(transport) => transport,
            Err(err) => {
                error!(error = %Report::new(err),"couldn't pair device");

                return Ok(Next::Same);
            }
        };

        debug!("created a new transport, reconnecting");

        let eventloop = conn.eventloop_mut();
        eventloop.clean();
        Self::set_transport(eventloop, transport);

        Ok(Next::state(Connecting))
    }

    #[cfg(not(test))]
    fn set_transport(eventloop: &mut EventLoop, transport: Transport) {
        eventloop.mqtt_options.set_transport(transport);
    }

    #[cfg(test)]
    fn set_transport(_eventloop: &mut EventLoop, _transport: Transport) {}
}

impl From<Disconnected> for State {
    fn from(value: Disconnected) -> Self {
        State::Disconnected(value)
    }
}

/// Waits for an incoming MQTT Connack packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Connecting;

impl Connecting {
    async fn wait_connack(&mut self, conn: &mut Connection) -> Next {
        let event = match conn.eventloop_mut().poll().await {
            Ok(event) => event,
            Err(err) => return Next::handle_error(err),
        };

        match event {
            Event::Incoming(Packet::ConnAck(connack)) => {
                trace!("connack received");

                Next::state(Handshake {
                    session_present: connack.session_present,
                })
            }
            Event::Incoming(incoming) => {
                error!(incoming = ?incoming,"unexpected packet received while waiting for connack");

                Next::state(Disconnected)
            }
            Event::Outgoing(outgoing) => {
                warn!("unexpected outgoing packet while waiting for connack {outgoing:?}");

                // We stay in connack since we shouldn't have any outgoing packet in this state, but
                // the specification doesn't disallow the client to send packet while waiting for
                // the ConnAck
                Next::Same
            }
        }
    }
}

impl From<Connecting> for State {
    fn from(value: Connecting) -> Self {
        State::Connecting(value)
    }
}

/// State that gets called when a [`rumqttc::ConnAck`] is received.
/// Following the astarte protocol it performs the following tasks:
///  - Subscribes to the server owned interfaces in the interface list
///  - Sends the introspection
///  - Sends the emptycache command
///  - Sends the device owned properties stored locally
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Handshake {
    session_present: bool,
}

impl Handshake {
    fn start<S>(
        self,
        conn: &mut Connection,
        client_id: ClientId<&str>,
        store: &StoreWrapper<S>,
        session_data: SessionData,
    ) -> Next
    where
        S: PropertyStore,
    {
        let client = conn.client.clone();

        let client_id_cl: ClientId = client_id.into();

        let store = store.clone();
        let handle: JoinHandle<Result<(), InitError>> = tokio::spawn(async move {
            let client_id = client_id_cl.as_ref();
            Self::subscribe_server_interfaces(&client, client_id, &session_data.server_interfaces)
                .await?;
            client
                .send_introspection(client_id, session_data.interfaces)
                .await
                .map_err(InitError::client("send introspection"))?
                .await
                .map_err(InitError::ack("subscribe server interface"))?;

            debug!("session present {}", self.session_present);
            if !self.session_present {
                Self::send_empty_cache(&client, client_id).await?;

                Self::purge_device_properties(&client, client_id, &session_data.device_properties)
                    .await?;

                Self::send_device_properties(
                    &client,
                    client_id,
                    &store,
                    &session_data.device_properties,
                )
                .await?;
            }

            Ok(())
        });

        Next::state(WaitAcks { handle })
    }

    /// Subscribes to the passed list of interfaces
    async fn subscribe_server_interfaces(
        client: &AsyncClient,
        client_id: ClientId<&str>,
        server_interfaces: &[String],
    ) -> Result<(), InitError> {
        debug!("subscribing server properties");

        client
            .subscribe(
                format!("{client_id}/control/consumer/properties"),
                QoS::ExactlyOnce,
            )
            .await
            .map_err(InitError::client("subscribe consumer properties"))?
            .await
            .map_err(InitError::ack("subscribe consumer properties"))?;

        debug!(
            "subscribing on {} server interfaces",
            server_interfaces.len()
        );

        let notice = client
            .subscribe_interfaces(client_id, server_interfaces)
            .await
            .map_err(InitError::client("subscribe server interface"))?;

        if let Some(notice) = notice {
            notice
                .await
                .map_err(InitError::ack("subscribe server interface"))?;
        }

        Ok(())
    }

    /// Sends the empty cache command as per the astarte protocol definition
    async fn send_empty_cache(
        client: &AsyncClient,
        client_id: ClientId<&str>,
    ) -> Result<(), InitError> {
        debug!("sending emptyCache");

        client
            .publish(
                format!("{client_id}/control/emptyCache"),
                QoS::ExactlyOnce,
                false,
                "1",
            )
            .await
            .map_err(InitError::client("empty cache"))?
            .await
            .map_err(InitError::ack("empty cache"))?;

        Ok(())
    }

    /// Sends the passed device owned properties
    async fn purge_device_properties(
        client: &AsyncClient,
        client_id: ClientId<&str>,
        device_properties: &[OptStoredProp],
    ) -> Result<(), InitError> {
        let iter = device_properties
            .iter()
            .filter(|val| val.value.is_some())
            .map(|val| format!("{}{}", val.interface, val.path));

        let payload = encode_set_properties(iter).map_err(InitError::PurgeProperties)?;

        client
            .publish(
                format!("{client_id}/control/producer/properties"),
                QoS::ExactlyOnce,
                false,
                payload,
            )
            .await
            .map_err(InitError::client("purge device properties"))?
            .await
            .map_err(InitError::ack("purge device properties"))?;

        Ok(())
    }

    /// Sends the passed device owned properties
    async fn send_device_properties<S>(
        client: &AsyncClient,
        client_id: ClientId<&str>,
        store: &StoreWrapper<S>,
        device_properties: &[OptStoredProp],
    ) -> Result<(), InitError>
    where
        S: PropertyStore,
    {
        for prop in device_properties {
            let topic = format!("{}/{}{}", client_id, prop.interface, prop.path);

            debug!(
                "sending device-owned property = {}{}",
                prop.interface, prop.path
            );

            let payload = match &prop.value {
                Some(value) => Payload::new(value).to_vec()?,
                // Unset the property
                None => Vec::new(),
            };

            // Don't wait for the ack since it's not fundamental for the connection
            client
                .publish(topic, rumqttc::QoS::ExactlyOnce, false, payload)
                .await
                .map_err(InitError::client("device property"))?
                .await
                .map_err(InitError::ack("device properties"))?;

            if prop.value.is_none() {
                trace!("clearing unset property {}/{}", prop.interface, prop.path);

                store
                    .delete_prop(&prop.into())
                    .await
                    .map_err(InitError::Unset)?;
            }
        }

        Ok(())
    }
}

impl From<Handshake> for State {
    fn from(value: Handshake) -> Self {
        State::Handshake(value)
    }
}

/// Waits for all the packets sent in the [`Init`] to be ACK-ed by Astarte.
#[derive(Debug)]
pub(crate) struct WaitAcks {
    handle: JoinHandle<Result<(), InitError>>,
}

impl WaitAcks {
    /// Waits for the initialization task to complete.
    ///
    /// We check that the task is finished, or we pull the connection. This ensures that all the
    /// packets in are sent correctly and the handle can advance to completion. Eventual published
    /// packets are returned.
    async fn wait_connection(&mut self, conn: &mut Connection) -> Result<Next, StoreError> {
        tokio::select! {
            // Join handle is cancel safe
            res = &mut self.handle => {
                debug!("task joined");

                Self::handle_join(res)
            }
            // I hope this is cancel safe
            res = conn.eventloop_mut().poll() => {
                debug!("next event polled");

                let next = match res {
                    Ok(event) => Next::handle_event(event),
                    Err(err) => Next::handle_error(err),
                };

                Ok(next)
            }
        }
    }

    fn handle_join(res: Result<Result<(), InitError>, JoinError>) -> Result<Next, StoreError> {
        // Don't move the handle to await the task
        match res {
            Ok(Ok(())) => {
                info!("device connected");
                Ok(Next::state(Connected))
            }
            Ok(Err(InitError::Unset(err))) => {
                error!(error = %Report::new(&err), "init task failed");

                Err(err)
            }
            Ok(Err(err)) => {
                error!(error = %Report::new(err), "init task failed");

                Ok(Next::state(Disconnected))
            }
            Err(err) => {
                error!(error = %Report::new(&err), "failed to join init task");

                // We should never panic, but return an error instead. This is probably a test/mock
                // expectation failing.
                debug_assert!(!err.is_panic(), "task panicked while waiting for acks");

                Ok(Next::state(Disconnected))
            }
        }
    }
}

/// Abort the task when there a change of state (e.g. [`Disconnected`]) while the handle is not
/// finished.
impl Drop for WaitAcks {
    fn drop(&mut self) {
        if !self.handle.is_finished() {
            self.handle.abort();
        }
    }
}

impl From<WaitAcks> for State {
    fn from(value: WaitAcks) -> Self {
        State::WaitAcks(value)
    }
}

/// Established connection to Astarte
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Connected;

impl Connected {
    async fn poll(&mut self, conn: &mut Connection) -> Next {
        match conn.eventloop_mut().poll().await {
            Ok(event) => Next::handle_event(event),
            Err(err) => Next::handle_error(err),
        }
    }
}

impl From<Connected> for State {
    fn from(value: Connected) -> Self {
        State::Connected(value)
    }
}

/// Enum to better represent the semantic of the next value, is a tuple of (Option<State>, Option<Publish>).
#[derive(Debug)]
#[must_use]
enum Next {
    /// Keep the same state
    Same,
    /// A packet was published
    Publish(Publish),
    /// Change to the next state.
    State(State),
}

impl Next {
    /// Handles a connection error.
    fn handle_error(err: ConnectionError) -> Self {
        error!(error = %Report::new(&err),"error received from mqtt connection");

        match err {
            ConnectionError::NetworkTimeout
            | ConnectionError::Io(_)
            | ConnectionError::FlushTimeout
            | ConnectionError::MqttState(StateError::Deserialization(mqttbytes::Error::Io(_))) => {
                trace!("disconnected, wait for connack");

                Next::state(Connecting)
            }
            ConnectionError::NotConnAck(_) => {
                trace!("wait for connack");

                Next::state(Connecting)
            }
            ConnectionError::MqttState(StateError::ConnectionAborted)
            | ConnectionError::RequestsDone => {
                info!("MQTT connection closed");

                Next::state(Connecting)
            }
            ConnectionError::Tls(_) | ConnectionError::ConnectionRefused(_) => {
                trace!("recreate the connection");

                Next::state(Disconnected)
            }
            ConnectionError::MqttState(_) => {
                trace!("no state change");

                Next::Same
            }
        }
    }

    fn handle_event(event: Event) -> Next {
        trace!("handling event");

        let incoming = match event {
            Event::Incoming(incoming) => {
                trace!("incoming packet {incoming:?}");

                incoming
            }
            Event::Outgoing(outgoing) => {
                trace!("outgoing packet {outgoing:?}");

                return Next::Same;
            }
        };

        match incoming {
            rumqttc::Packet::ConnAck(connack) => {
                debug!("connack received, initializing connection");

                Next::state(Handshake {
                    session_present: connack.session_present,
                })
            }
            rumqttc::Packet::Publish(publish) => {
                debug!("incoming publish on {}", publish.topic);

                Next::Publish(publish)
            }
            Packet::Disconnect => {
                debug!("server sent a disconnect packet");

                Next::state(Disconnected)
            }
            _ => {
                trace!("incoming packet");

                Next::Same
            }
        }
    }

    fn state<T>(value: T) -> Self
    where
        T: Into<State>,
    {
        Self::State(value.into())
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use mockall::predicate;
    use rumqttc::{AckOfPub, SubAck};

    use crate::{
        store::{memory::MemoryStore, StoredProp},
        test::{DEVICE_PROPERTIES, INDIVIDUAL_SERVER_DATASTREAM, OBJECT_DEVICE_DATASTREAM},
        transport::mqtt::test::notify_success,
        AstarteType, Interface,
    };

    use super::*;

    #[tokio::test]
    async fn test_connect_client_response() {
        let mut eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        let mut seq = mockall::Sequence::new();

        // Connak response for loop in connect method
        eventl
            .expect_poll()
            .once()
            .in_sequence(&mut seq)
            .returning(|| {
                Box::pin(async {
                    tokio::task::yield_now().await;

                    Ok(Event::Incoming(rumqttc::Packet::ConnAck(
                        rumqttc::ConnAck {
                            session_present: false,
                            code: rumqttc::ConnectReturnCode::Success,
                        },
                    )))
                })
            });

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(|| {
                let mut client = AsyncClient::default();

                let mut seq = mockall::Sequence::new();

                client
                    .expect_subscribe::<String>()
                    .once()
                    .in_sequence(&mut seq)
                    .with(
                        predicate::eq("realm/device_id/control/consumer/properties".to_string()),
                        predicate::always(),
                    )
                    .returning(|_topic, _qos| notify_success(SubAck::new(0,Vec::new())));

                client
                    .expect_subscribe()
                    .once()
                    .in_sequence(&mut seq)
                    .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()), predicate::always())
                    .returning(|_: String, _| notify_success(SubAck::new(0, Vec::new())));

                // Client id
                client
                    .expect_publish::<String, String>()
                    .once()
                    .in_sequence(&mut seq)
                    .with(
                        predicate::eq("realm/device_id".to_string()),
                        predicate::always(),
                        predicate::always(),
                        predicate::always(),
                    )
                    .returning(|_, _, _, _| notify_success(AckOfPub::None));

                // empty cache
                client
                    .expect_publish::<String, &str>()
                    .once()
                    .in_sequence(&mut seq)
                    .with(
                        predicate::eq("realm/device_id/control/emptyCache".to_string()),
                        predicate::always(),
                        predicate::always(),
                        predicate::eq("1"),
                    )
                    .returning(|_, _, _, _| notify_success(AckOfPub::None));


                // purge device properties
                client
                    .expect_publish::<String, Vec<u8>>()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(|topic, qos, _, _| {
                        topic == "realm/device_id/control/producer/properties"
                            && *qos == QoS::ExactlyOnce
                    })
                    .returning(|_, _, _, _| notify_success(AckOfPub::None));

                // device property publish
                client
                    .expect_publish::<String, Vec<u8>>()
                    .once()
                    .in_sequence(&mut seq)
                    .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/sensor1/name".to_string()), predicate::always(), predicate::always(), predicate::always())
                    .returning(|_, _, _, _| notify_success(AckOfPub::None));

                client
            });

        // Catch other call to poll
        eventl
            .expect_poll()
            .once()
            .in_sequence(&mut seq)
            .returning(|| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    Ok(Event::Incoming(rumqttc::Packet::PingReq))
                })
            });

        let interfaces = [
            Interface::from_str(DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(OBJECT_DEVICE_DATASTREAM).unwrap(),
            Interface::from_str(INDIVIDUAL_SERVER_DATASTREAM).unwrap(),
        ];

        let interfaces = Interfaces::from_iter(interfaces);
        let store = StoreWrapper::new(MemoryStore::new());

        let interface = Interface::from_str(DEVICE_PROPERTIES).unwrap();

        let prop = StoredProp {
            interface: interface.interface_name(),
            path: "/sensor1/name",
            value: &AstarteType::String("temperature".to_string()),
            interface_major: 0,
            ownership: interface.ownership(),
        };

        store
            .store_prop(prop)
            .await
            .expect("Error while storing test property");

        let connection = tokio::time::timeout(
            Duration::from_secs(3),
            MqttConnection::wait_connack(
                client,
                eventl,
                TransportProvider::configure(
                    "http://api.astarte.localhost/pairing".parse().unwrap(),
                    "secret".to_string(),
                    None,
                    true,
                )
                .await
                .expect("failed to configure transport provider"),
                ClientId {
                    realm: "realm",
                    device_id: "device_id",
                },
                &interfaces,
                &store,
            ),
        )
        .await
        .expect("taimeout reached")
        .expect("failed to connect");

        assert!(connection.state.is_connected());
    }
}
