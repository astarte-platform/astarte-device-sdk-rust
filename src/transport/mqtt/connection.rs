// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
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
    fmt::Display,
    sync::{Arc, OnceLock},
};

use rumqttc::{
    ClientError, ConnectionError, Event, Packet, Publish, QoS, StateError, Transport, mqttbytes,
};
use sync_wrapper::SyncWrapper;
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, error, info, trace, warn};

use crate::{
    builder::Config,
    error::Report,
    interfaces::Interfaces,
    logging::security::{SecurityEvent, notify_security_event, notify_tls_error},
    properties::{PropertiesError, encode_set_properties},
    session::StoredSession,
    state::SharedState,
    store::{PropertyStore, StoreCapabilities, wrapper::StoreWrapper},
    transport::mqtt::{AsyncClientExt, config::MqttTransportOptions, pairing::ApiClient},
};

use super::{
    ClientId, MqttConfig, PairingError, PayloadError, SessionData,
    client::{AsyncClient, EventLoop},
    config::transport::TransportProvider,
    error::MqttError,
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
    /// Couldn't serialize the device property payload.
    #[error("coudln't serialize the device property payload")]
    Payload(#[from] PayloadError),
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
}

impl InitError {
    const fn client(ctx: &'static str) -> impl Fn(ClientError) -> InitError {
        move |backtrace: ClientError| InitError::Client { backtrace, ctx }
    }
}

#[derive(Debug)]
pub(crate) struct MqttConnectionEstablished {
    connection: Connection,
    state: State,
}

impl MqttConnectionEstablished {
    async fn reconnect<S>(
        &mut self,
        client_id: ClientId<&str>,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
        shared_state: &SharedState,
        buff: &mut VecDeque<Publish>,
    ) -> Result<bool, MqttError>
    where
        S: StoreCapabilities,
    {
        if self.state.is_connected() {
            debug!("already connected");

            return Ok(true);
        }

        loop {
            let opt_publish = self
                .state
                .poll(
                    &mut self.connection,
                    client_id,
                    interfaces,
                    store,
                    shared_state,
                )
                .await?;

            if let Some(publish) = opt_publish {
                debug!("publish received");

                buff.push_back(publish);
            }

            match &self.state {
                State::Connected(_) => {
                    debug!("reconnected");

                    break Ok(true);
                }
                State::Disconnected(_) | State::Connecting(_) => {
                    debug!("error occurred");

                    break Ok(false);
                }
                state => {
                    trace!(?state, "polling next event");
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NeedsTransport {
    mqtt_config: MqttConfig,
    client_sender: Arc<OnceLock<AsyncClient>>,
}

impl NeedsTransport {
    async fn create_transport(&mut self, config: &Config) -> Result<Option<Connection>, MqttError> {
        let opt = match self.mqtt_config.try_create_transport(config).await {
            Ok(opt) => opt,
            Err(MqttError::Pairing(PairingError::RequestNoNetwork(e))) => {
                info!(error=%Report::new(e), "can't create the transport as network is still unreachable");

                return Ok(None);
            }
            Err(e) => {
                return Err(e);
            }
        };

        let MqttTransportOptions {
            mqtt_opts,
            net_opts: _net_opts,
            provider,
        } = opt;

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, config.channel_size.get());

        // NOTE we were never connected so we need to set the clean session to ensure no acks stored by the server will be returned
        Connection::set_clean_session(&mut eventloop, true);

        let connection = Connection {
            client: client.clone(),
            eventloop: SyncWrapper::new(eventloop),
            provider,
            session_synced: false,
        };

        if self.client_sender.set(client).is_err() {
            error!("coudln't set the client sender, already set");
        }

        Ok(Some(connection))
    }
}

/// State of the MQTT link, it will be changed one time only when it goes from
/// being Absent to Established, this is why we allow large_enum_variant
/// It should be Established for most of the application's lifetime.
#[derive(Debug)]
pub(crate) enum MqttLinkState {
    Absent(NeedsTransport),
    Established(Box<MqttConnectionEstablished>),
}

impl MqttLinkState {
    fn get_mut(&mut self) -> Option<&mut MqttConnectionEstablished> {
        match self {
            MqttLinkState::Absent(_) => None,
            MqttLinkState::Established(mqtt_connection_established) => {
                Some(mqtt_connection_established)
            }
        }
    }

    async fn ensure_established(
        &mut self,
        config: &Config,
    ) -> Result<Option<&mut MqttConnectionEstablished>, MqttError> {
        if let MqttLinkState::Absent(needs_transport) = self {
            let Some(connection) = needs_transport.create_transport(config).await? else {
                return Ok(None);
            };

            *self = MqttLinkState::Established(Box::new(MqttConnectionEstablished {
                connection,
                state: Connecting.into(),
            }));
        }

        Ok(self.get_mut())
    }
}

/// MQTT connection to Astarte that can be pulled to receive packets.
#[derive(Debug)]
pub(crate) struct MqttConnection {
    link: MqttLinkState,
    /// Queue for the packet published while re/connecting.
    buff: VecDeque<Publish>,
}

impl MqttConnection {
    pub(crate) fn without_transport(
        mqtt_config: MqttConfig,
        client_sender: Arc<OnceLock<AsyncClient>>,
    ) -> Self {
        Self {
            link: MqttLinkState::Absent(NeedsTransport {
                mqtt_config,
                client_sender,
            }),
            buff: VecDeque::new(),
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

        let connection = self.link.get_mut()?;
        // Here we only get the connected state so we don't need to pass all the arguments, like the
        // interfaces or the store.
        let State::Connected(connected) = &mut connection.state else {
            return None;
        };

        loop {
            match connected.poll(&mut connection.connection).await {
                Next::Same => {}
                Next::Publish(publish) => return Some(publish),
                Next::State(next) => {
                    debug_assert!(!next.is_connected());

                    connection.state = next;

                    return None;
                }
            }
        }
    }

    /// Reconnect to astarte, wait till the state is [`Connected`].
    pub(crate) async fn reconnect<S>(
        &mut self,
        client_id: ClientId<&str>,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
        shared_state: &SharedState,
    ) -> Result<bool, MqttError>
    where
        S: PropertyStore + StoreCapabilities,
    {
        let Some(connection) = self.link.ensure_established(&shared_state.config).await? else {
            return Ok(false);
        };

        connection
            .reconnect(client_id, interfaces, store, shared_state, &mut self.buff)
            .await
    }

    /// Returns true only if the state is connected and the session present is true
    pub(crate) fn is_session_present(&self) -> bool {
        let MqttLinkState::Established(established) = &self.link else {
            return false;
        };

        established.state.is_session_present()
    }
}

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
    /// Whether the stored introspection matches the current one
    session_synced: bool,
}

impl Connection {
    fn eventloop_mut(&mut self) -> &mut EventLoop {
        self.eventloop.get_mut()
    }

    pub(crate) fn set_session_synced(&mut self, sync: bool) {
        self.session_synced = sync;
    }

    #[cfg(not(test))]
    fn set_clean_session(eventloop: &mut EventLoop, clean: bool) {
        eventloop.mqtt_options.set_clean_session(clean);
    }

    #[cfg(test)]
    fn set_clean_session(_eventloop: &mut EventLoop, _clean: bool) {}
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
        shared_state: &SharedState,
    ) -> Result<Option<Publish>, PollError>
    where
        S: PropertyStore + StoreCapabilities,
    {
        trace!("state {}", self);

        let next = match self {
            State::Disconnected(disconnected) => {
                disconnected
                    .reconnect(conn, client_id, shared_state)
                    .await?
            }
            State::Connecting(connecting) => connecting.wait_connack(conn).await,
            State::Handshake(handshake) => {
                let session_data = SessionData::from_props(interfaces, store).await;

                handshake.start(conn, client_id, store, session_data).await
            }
            State::WaitAcks(init) => init.wait_connection(conn).await,
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

    /// Returns `true` if the state is [`Connected`] with and the connack had a session present flag.
    fn is_session_present(&self) -> bool {
        matches!(
            self,
            Self::Connected(Connected {
                session_present: true
            })
        )
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
        shared_state: &SharedState,
    ) -> Result<Next, PairingError> {
        let api = ApiClient::from_transport(
            &shared_state.config,
            &conn.provider,
            client_id.realm,
            client_id.device_id,
        )?;

        let transport = match conn.provider.validate_transport(&api).await {
            Ok(transport) => transport,
            Err(err) => {
                error!(error = %Report::new(err),"couldn't pair device");

                return Ok(Next::Same);
            }
        };

        *shared_state.cert_expiry.write().await = conn.provider.fetch_cert_expiry(client_id).await;

        debug!("created a new transport, reconnecting");

        Self::set_transport(conn.eventloop_mut(), transport);

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
            Ok(event) => {
                trace!("event received");

                event
            }
            Err(err) => return Next::handle_error(err),
        };

        match event {
            Event::Incoming(Packet::ConnAck(connack)) => {
                debug!("connack received");

                // NOTE permanently set the clean session to false since the mqtt library will keep inflight messages in memory
                Connection::set_clean_session(conn.eventloop_mut(), false);

                Next::state(Handshake {
                    session_present: connack.session_present,
                })
            }
            Event::Incoming(incoming) => {
                error!(incoming = ?incoming,"unexpected packet received while waiting for connack");

                notify_security_event(SecurityEvent::UnexpectedMessageReceived);

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
    async fn start<S>(
        self,
        conn: &mut Connection,
        client_id: ClientId<&str>,
        store: &StoreWrapper<S>,
        session_data: SessionData,
    ) -> Next
    where
        S: PropertyStore + StoreCapabilities,
    {
        let client = conn.client.clone();
        let client_id: ClientId = client_id.into();
        let store = store.clone();

        let handle = if self.session_present && conn.session_synced {
            debug!(
                session_sync = conn.session_synced,
                "introspection already synchronized"
            );

            None
        } else {
            debug!(
                session_present = self.session_present,
                session_sync = conn.session_synced,
                "perform again handshake to synchronize the device",
            );

            // NOTE set session synced to false since we are not synchronized
            // also clear the stored introspection so it can be updated
            // set to true after a successful handshake in [`WaitAcks`]
            conn.set_session_synced(false);
            if let Some(session) = store.get_session() {
                trace!("Clearing stored introspection before the full handshake");
                session.clear_introspection().await;
            }

            Some(Self::full_handshake(client, client_id, store, session_data))
        };

        Next::state(WaitAcks {
            handle,
            session_present: self.session_present,
        })
    }

    fn full_handshake<S>(
        client: AsyncClient,
        client_id: ClientId,
        store: StoreWrapper<S>,
        session_data: SessionData,
    ) -> JoinHandle<Result<(), InitError>>
    where
        S: PropertyStore + StoreCapabilities,
    {
        tokio::spawn(async move {
            let client_id = client_id.as_ref();
            Self::subscribe_server_interfaces(&client, client_id, &session_data.server_interfaces)
                .await?;

            client
                .send_introspection(client_id, session_data.interfaces)
                .await
                .map_err(InitError::client("send introspection"))?;

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
        })
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
            .map_err(InitError::client("subscribe consumer properties"))?;

        debug!(
            "subscribing on {} server interfaces",
            server_interfaces.len()
        );

        client
            .subscribe_interfaces(client_id, server_interfaces)
            .await
            .map_err(InitError::client("subscribe server interface"))?;

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
            .map_err(InitError::client("empty cache"))?;

        Ok(())
    }

    /// Sends the passed device owned properties
    async fn purge_device_properties(
        client: &AsyncClient,
        client_id: ClientId<&str>,
        device_properties: &[String],
    ) -> Result<(), InitError> {
        let payload =
            encode_set_properties(device_properties).map_err(InitError::PurgeProperties)?;

        client
            .publish(
                format!("{client_id}/control/producer/properties"),
                QoS::ExactlyOnce,
                false,
                payload,
            )
            .await
            .map_err(InitError::client("purge device properties"))?;

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
    handle: Option<JoinHandle<Result<(), InitError>>>,
    session_present: bool,
}

impl WaitAcks {
    /// Waits for the initialization task to complete.
    ///
    /// We check that the task is finished, or we pull the connection. This ensures that all the
    /// packets in are sent correctly and the handle can advance to completion. Eventual published
    /// packets are returned.
    async fn wait_connection(&mut self, conn: &mut Connection) -> Next {
        if let Some(handle) = self.handle.as_mut() {
            tokio::select! {
                // Join handle is cancel safe
                res = handle => {
                    debug!("task joined");

                    Self::handle_join(res, conn, self.session_present)
                }
                // I hope this is cancel safe
                res = conn.eventloop_mut().poll() => {
                    Self::handle_poll(res)
                }
            }
        } else {
            Next::state(Connected::new(self.session_present))
        }
    }

    fn handle_join(
        res: Result<Result<(), InitError>, JoinError>,
        conn: &mut Connection,
        session_present: bool,
    ) -> Next {
        // Don't move the handle to await the task
        match res {
            Ok(Ok(())) => {
                info!("device connected");
                // if the device successfully connects we set the sync status to true
                conn.set_session_synced(true);
                Next::state(Connected::new(session_present))
            }
            Ok(Err(err)) => {
                error!(error = %Report::new(err), "init task failed");

                Next::state(Disconnected)
            }
            Err(err) => {
                error!(error = %Report::new(&err), "failed to join init task");

                // We should never panic, but return an error instead. This is probably a test/mock
                // expectation failing.
                debug_assert!(!err.is_panic(), "task panicked while waiting for acks");

                Next::state(Disconnected)
            }
        }
    }

    fn handle_poll(res: Result<Event, ConnectionError>) -> Next {
        debug!("next event polled");

        match res {
            Ok(Event::Incoming(rumqttc::Packet::ConnAck(connack))) => {
                debug!("connack received, initializing connection");

                Next::state(Handshake {
                    session_present: connack.session_present,
                })
            }
            Ok(event) => Next::handle_event(event),
            Err(err) => Next::handle_error(err),
        }
    }
}

/// Abort the task when there a change of state (e.g. [`Disconnected`]) while the handle is not
/// finished.
impl Drop for WaitAcks {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.as_ref()
            && !handle.is_finished()
        {
            handle.abort();
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
pub(crate) struct Connected {
    session_present: bool,
}

impl Connected {
    pub(crate) fn new(session_present: bool) -> Self {
        Self { session_present }
    }

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
            ConnectionError::Tls(tls_err) => {
                trace!("tls error recreate the connection");

                notify_tls_error(&tls_err);

                Next::state(Disconnected)
            }
            ConnectionError::ConnectionRefused(_) => {
                trace!("recreate the connection");

                Next::state(Disconnected)
            }
            ConnectionError::MqttState(StateError::Unsolicited(pkid)) => {
                warn!("rumqtt reports unsolicited ack to pkid: {}", pkid);
                Next::state(Connecting)
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
            // FIXME according to the MQTT spec the connack is only sent after the connack
            // here we are also handling connack packets received after the first one
            // this situation should probably be treated as a critical error
            rumqttc::Packet::ConnAck(connack) => {
                error!(connack=?connack, "connack received after the initial connection, broker bug");

                Next::Same
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

    use astarte_interfaces::Interface;
    use futures::FutureExt;
    use mockall::{Sequence, predicate};
    use rumqttc::{AckOfPub, SubAck};

    use crate::{
        AstarteData,
        builder::Config,
        retention::memory::VolatileStore,
        store::{OptStoredProp, mock::MockStore},
        test::{DEVICE_OBJECT, DEVICE_PROPERTIES, DEVICE_PROPERTIES_NAME, SERVER_INDIVIDUAL},
        transport::mqtt::test::notify_success,
    };

    use super::*;

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
                session_synced: false,
            };

            Self {
                link: MqttLinkState::Established(Box::new(MqttConnectionEstablished {
                    connection,
                    state: state.into(),
                })),
                buff: VecDeque::new(),
            }
        }
    }

    fn mock_receive_connack(
        seq: &mut Sequence,
        eventl: &mut EventLoop,
        session_present: bool,
        code: rumqttc::ConnectReturnCode,
    ) {
        eventl
            .expect_poll()
            .once()
            .in_sequence(seq)
            .returning(move || {
                futures::future::ok(Event::Incoming(rumqttc::Packet::ConnAck(
                    rumqttc::ConnAck {
                        // NOTE the session present flag is true and the introspection matches
                        session_present,
                        code,
                    },
                )))
                .boxed()
            });
    }

    fn mock_handshake_with_session(
        seq: &mut Sequence,
        store: &mut MockStore,
        client: &mut AsyncClient,
        props: Vec<OptStoredProp>,
    ) {
        store
            .expect_device_props_with_unset()
            .once()
            .in_sequence(seq)
            .returning({
                let props = props.clone();

                move |_, _| Ok(props.clone())
            });

        client
            .expect_clone()
            .once()
            .in_sequence(seq)
            .returning(AsyncClient::default);

        store
            .expect_clone()
            .once()
            .in_sequence(seq)
            .returning(move || {
                let mut store = MockStore::new();
                let mut seq = Sequence::new();

                for p in props.iter().filter(|p| p.value.is_none()).cloned() {
                    store
                        .expect_unset_prop()
                        .once()
                        .in_sequence(&mut seq)
                        .withf(move |m| m.interface_name() == p.interface)
                        .returning(|_| Ok(()));
                }

                store
            });
    }

    fn mock_full_handshake(
        seq: &mut Sequence,
        store: &mut MockStore,
        client: &mut AsyncClient,
        client_id: ClientId,
        server_interfaces: &[Interface],
        props: Vec<OptStoredProp>,
    ) {
        store
            .expect_device_props_with_unset()
            .once()
            .in_sequence(seq)
            .returning({
                let props = props.clone();

                move |_, _| Ok(props.clone())
            });

        client.expect_clone().once().in_sequence(seq).returning({
            let server_interfaces = server_interfaces.to_vec();

            move || {
                let mut client = AsyncClient::default();
                let mut seq = Sequence::new();

                client
                    .expect_subscribe::<String>()
                    .once()
                    .in_sequence(&mut seq)
                    .with(
                        predicate::eq(format!("{client_id}/control/consumer/properties")),
                        predicate::eq(QoS::ExactlyOnce),
                    )
                    .returning(|_, _| notify_success(SubAck::new(0, vec![])));

                for i in server_interfaces.clone() {
                    client
                        .expect_subscribe::<String>()
                        .once()
                        .in_sequence(&mut seq)
                        .with(
                            predicate::eq(format!("{client_id}/{}/#", i.interface_name())),
                            predicate::eq(QoS::ExactlyOnce),
                        )
                        .returning(|_, _| notify_success(SubAck::new(0, vec![])));
                }

                // Introspection
                client
                    .expect_publish::<String, String>()
                    .once()
                    .in_sequence(&mut seq)
                    .with(
                        predicate::eq(client_id.to_string()),
                        predicate::eq(QoS::ExactlyOnce),
                        predicate::eq(false),
                        predicate::always(),
                    )
                    .returning(|_, _, _, _| notify_success(AckOfPub::None));

                // Introspection
                client
                    .expect_publish::<String, &str>()
                    .once()
                    .in_sequence(&mut seq)
                    .with(
                        predicate::eq(format!("{client_id}/control/emptyCache")),
                        predicate::eq(QoS::ExactlyOnce),
                        predicate::eq(false),
                        predicate::eq("1"),
                    )
                    .returning(|_, _, _, _| notify_success(AckOfPub::None));

                client
                    .expect_publish::<String, Vec<u8>>()
                    .once()
                    .in_sequence(&mut seq)
                    .with(
                        predicate::eq(format!("{client_id}/control/producer/properties")),
                        predicate::eq(QoS::ExactlyOnce),
                        predicate::eq(false),
                        predicate::always(),
                    )
                    .returning(|_, _, _, _| notify_success(AckOfPub::None));

                client
            }
        });

        store
            .expect_clone()
            .once()
            .in_sequence(seq)
            .returning(move || {
                let mut store = MockStore::new();
                let mut seq = Sequence::new();

                store
                    .expect_return_session()
                    .once()
                    .in_sequence(&mut seq)
                    .with()
                    .returning(|| true);

                store
                    .expect_clear_introspection()
                    .once()
                    .in_sequence(&mut seq)
                    .with()
                    .return_const(());

                store
                    .expect_return_session()
                    .once()
                    .in_sequence(&mut seq)
                    .with()
                    .returning(|| true);

                store
                    .expect_store_introspection()
                    .once()
                    .in_sequence(&mut seq)
                    .with(predicate::always())
                    .returning(|_| ());

                store
            });
    }

    fn mock_wait_acks(seq: &mut Sequence, eventl: &mut EventLoop) {
        eventl.expect_poll().in_sequence(seq).once().returning(|| {
            trace!("pending the poll");

            futures::future::pending().boxed()
        });
    }

    #[tokio::test]
    async fn test_connect_client_response() {
        let client_id = ClientId {
            realm: "realm",
            device_id: "device_id",
        };
        let server_interface = Interface::from_str(SERVER_INDIVIDUAL).unwrap();
        let interface = Interface::from_str(DEVICE_PROPERTIES).unwrap();

        let mut eventl = EventLoop::new();
        let mut store = MockStore::new();
        let mut client = AsyncClient::default();
        let mut seq = Sequence::new();
        let prop = OptStoredProp {
            interface: DEVICE_PROPERTIES_NAME.to_string(),
            path: "/sensor1/name".to_string(),
            value: Some(AstarteData::String("temperature".to_string())),
            interface_major: 0,
            ownership: interface.ownership(),
        };

        // Connecting
        mock_receive_connack(
            &mut seq,
            &mut eventl,
            false,
            rumqttc::ConnectReturnCode::Success,
        );
        // Handshake
        mock_full_handshake(
            &mut seq,
            &mut store,
            &mut client,
            client_id.into(),
            std::slice::from_ref(&server_interface),
            vec![prop],
        );
        // WaitAcks
        mock_wait_acks(&mut seq, &mut eventl);

        let provider = TransportProvider::configure(
            "http://api.astarte.localhost/pairing".parse().unwrap(),
            "secret".to_string(),
            None,
            true,
        )
        .await
        .expect("failed to configure transport provider");

        let mut connection = MqttConnectionEstablished {
            connection: Connection {
                client,
                eventloop: SyncWrapper::new(eventl),
                provider,
                session_synced: false,
            },
            state: State::Connecting(Connecting),
        };

        let store = StoreWrapper::new(store);
        let shared_state = SharedState::new(
            Config::default(),
            Interfaces::from_iter([server_interface, interface]),
            VolatileStore::with_capacity(1),
        );

        let connected = tokio::time::timeout(
            Duration::from_secs(2),
            connection.reconnect(
                client_id,
                &*shared_state.interfaces.read().await,
                &store,
                &shared_state,
                &mut VecDeque::new(),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(connected);
    }

    #[tokio::test]
    async fn test_connect_store_fast_handshake() {
        let client_id = ClientId {
            realm: "realm",
            device_id: "device_id",
        };

        let interfaces = [
            Interface::from_str(DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(DEVICE_OBJECT).unwrap(),
            Interface::from_str(SERVER_INDIVIDUAL).unwrap(),
        ];

        let mut eventl = EventLoop::new();
        let mut store = MockStore::new();
        let mut client = AsyncClient::default();
        let mut seq = Sequence::new();

        // Connecting
        mock_receive_connack(
            &mut seq,
            &mut eventl,
            true,
            rumqttc::ConnectReturnCode::Success,
        );
        // Handshake
        mock_handshake_with_session(&mut seq, &mut store, &mut client, Vec::new());
        // if the session is present we avoid polling here
        // mock_wait_acks(&mut seq, &mut eventl);

        let interfaces = Interfaces::from_iter(interfaces);
        let shared_state = SharedState::new(
            Config::default(),
            interfaces.clone(),
            VolatileStore::with_capacity(1),
        );
        let store = StoreWrapper::new(store);
        let provider = TransportProvider::configure(
            "http://api.astarte.localhost/pairing".parse().unwrap(),
            "secret".to_string(),
            None,
            true,
        )
        .await
        .expect("failed to configure transport provider");

        let mut connection = MqttConnectionEstablished {
            connection: Connection {
                client,
                eventloop: SyncWrapper::new(eventl),
                provider,
                // Session ok
                session_synced: true,
            },
            state: State::Connecting(Connecting),
        };

        let connected = connection
            .reconnect(
                client_id,
                &interfaces,
                &store,
                &shared_state,
                &mut VecDeque::new(),
            )
            .await
            .unwrap();

        assert!(connected);
    }
}
