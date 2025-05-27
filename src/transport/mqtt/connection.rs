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

use std::{collections::VecDeque, fmt::Display, time::Duration};

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
    session::StoredSession,
    store::{wrapper::StoreWrapper, OptStoredProp, PropertyStore, StoreCapabilities},
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
            session_synced: false,
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
        S: PropertyStore + StoreCapabilities,
    {
        let session_sync = Self::is_introspection_stored(interfaces, store).await;
        debug!(session_sync = session_sync, "Creating new mqtt connection");
        let mut mqtt_connection = Self::new(client, eventloop, provider, Connecting);
        mqtt_connection.connection.set_session_synced(session_sync);

        let mut exp_back = ExponentialIter::default();

        while !mqtt_connection
            .reconnect(client_id, interfaces, store)
            .await?
        {
            let timeout = exp_back.next();

            debug!("waiting {timeout} seconds before retrying");

            tokio::time::sleep(Duration::from_secs(timeout)).await;
        }

        Ok(mqtt_connection)
    }

    #[must_use]
    pub(crate) async fn is_introspection_stored<S>(
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> bool
    where
        S: StoreCapabilities,
    {
        let Some(introspection) = store.get_session() else {
            return false;
        };

        let stored = match introspection.load_introspection().await {
            Ok(s) => s,
            Err(err) => {
                error!(
                    error = %Report::new(err),
                    "couldn't retrieve the introspection from the store",
                );
                return false;
            }
        };

        !interfaces.is_empty() && interfaces.matches(&stored)
    }

    /// Reconnect to astarte, wait till the state is [`Connected`].
    pub(crate) async fn reconnect<S>(
        &mut self,
        client_id: ClientId<&str>,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> Result<bool, PollError>
    where
        S: PropertyStore + StoreCapabilities,
    {
        if self.state.is_connected() {
            debug!("already connected");

            return Ok(true);
        }

        loop {
            let opt_publish = self
                .state
                .poll(&mut self.connection, client_id, interfaces, store)
                .await?;

            if let Some(publish) = opt_publish {
                debug!("publish received");

                self.buff.push_back(publish);
            }

            match &self.state {
                State::Connected(_) => {
                    debug!("reconnected");

                    break Ok(true);
                }
                State::Disconnected(_) => {
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

        // We can not access a field of the mock so we have to add the cfg
        #[cfg(not(test))]
        {
            self.eventloop
                .get_mut()
                .mqtt_options
                .set_clean_session(!sync);
        }
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
        S: PropertyStore + StoreCapabilities,
    {
        trace!("state {}", self);

        let next = match self {
            State::Disconnected(disconnected) => disconnected.reconnect(conn, client_id).await?,
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

            Self::prop_handshake(client, client_id, store, session_data)
        } else {
            debug!(
                session_present = self.session_present,
                session_sync = conn.session_synced,
                "perform again handshake to synchronize the device",
            );

            // NOTE set session synced to false since we are not synchronized
            // also clear the stored introspection so it can be updated
            // set to true after a succesful handshake in [`WaitAcks`]
            conn.set_session_synced(false);
            if let Some(session) = store.get_session() {
                trace!("Clearing stored introspection before the full handshake");
                session.clear_introspection().await;
            }

            Self::full_handshake(client, client_id, store, session_data)
        };

        Next::state(WaitAcks { handle })
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
                .map_err(InitError::client("send introspection"))?
                .await
                .map_err(InitError::ack("introspection ack error"))?;

            if let Some(session) = store.get_session() {
                trace!("Introspection sent successfully, storing");

                session
                    .store_introspection(&session_data.interfaces_stored)
                    .await;
            }

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

            Ok(())
        })
    }

    fn prop_handshake<S>(
        client: AsyncClient,
        client_id: ClientId,
        store: StoreWrapper<S>,
        session_data: SessionData,
    ) -> JoinHandle<Result<(), InitError>>
    where
        S: PropertyStore,
    {
        // NOTE send device properties even if synchronized because they could
        // have been updated while disconnected
        tokio::spawn(async move {
            let client_id = client_id.as_ref();

            Self::send_device_properties(
                &client,
                client_id,
                &store,
                &session_data.device_properties,
            )
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

                let _logged_error = store.delete_prop(&prop.into()).await.inspect_err(|e| {
                    error!(error = %Report::new(e),
                        "couldn't delete unset property, proceding anyways to ensure connection")
                });
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
    async fn wait_connection(&mut self, conn: &mut Connection) -> Next {
        tokio::select! {
            // Join handle is cancel safe
            res = &mut self.handle => {
                debug!("task joined");

                Self::handle_join(res, conn)
            }
            // I hope this is cancel safe
            res = conn.eventloop_mut().poll() => {
                debug!("next event polled");

                match res {
                    Ok(event) => Next::handle_event(event),
                    Err(err) => Next::handle_error(err),
                }
            }
        }
    }

    fn handle_join(res: Result<Result<(), InitError>, JoinError>, conn: &mut Connection) -> Next {
        // Don't move the handle to await the task
        match res {
            Ok(Ok(())) => {
                info!("device connected");
                // if the device successfully connects we set the sync status to true
                conn.set_session_synced(true);
                Next::state(Connected)
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

    use mockall::{predicate, Sequence};
    use rumqttc::{AckOfPub, SubAck};

    use crate::{
        session::{IntrospectionInterface, SessionError},
        store::{memory::MemoryStore, mock::MockStore, StoredProp},
        test::{DEVICE_OBJECT, DEVICE_PROPERTIES, SERVER_INDIVIDUAL},
        transport::mqtt::test::notify_success,
        AstarteType, Interface,
    };

    use super::*;

    trait ConnectionBehavior: Clone + Send + Sync {
        fn subscribe_interfaces(&self, seq: &mut Sequence, client: &mut AsyncClient);

        fn send_introspection(&self, seq: &mut Sequence, client: &mut AsyncClient);

        fn send_empty_cache(&self, seq: &mut Sequence, client: &mut AsyncClient);

        fn send_purge_properties(&self, seq: &mut Sequence, client: &mut AsyncClient);

        fn send_properties(&self, seq: &mut Sequence, client: &mut AsyncClient);
    }

    #[derive(Clone)]
    struct ConnectSuccess {
        full_handshake: bool,
        client_id: ClientId,
        server_interfaces: Vec<String>,
        device_properties: Vec<StoredProp>,
    }

    impl ConnectionBehavior for ConnectSuccess {
        fn subscribe_interfaces(&self, seq: &mut Sequence, client: &mut AsyncClient) {
            if !self.full_handshake {
                return;
            }

            client
                .expect_subscribe::<String>()
                .once()
                .in_sequence(seq)
                .with(
                    predicate::eq(format!("{}/control/consumer/properties", self.client_id)),
                    predicate::always(),
                )
                .returning(|_topic, _qos| notify_success(SubAck::new(0, Vec::new())));

            for interf in &self.server_interfaces {
                client
                    .expect_subscribe()
                    .once()
                    .in_sequence(seq)
                    .with(
                        predicate::eq(format!("{}/{}/#", self.client_id, interf)),
                        predicate::always(),
                    )
                    .returning(|_: String, _| notify_success(SubAck::new(0, Vec::new())));
            }
        }

        fn send_introspection(&self, seq: &mut Sequence, client: &mut AsyncClient) {
            if !self.full_handshake {
                return;
            }

            client
                .expect_publish::<String, String>()
                .once()
                .in_sequence(seq)
                .with(
                    predicate::eq(self.client_id.to_string()),
                    predicate::always(),
                    predicate::always(),
                    predicate::always(),
                )
                .returning(|_, _, _, _| notify_success(AckOfPub::None));
        }

        fn send_empty_cache(&self, seq: &mut Sequence, client: &mut AsyncClient) {
            if !self.full_handshake {
                return;
            }

            client
                .expect_publish::<String, &str>()
                .once()
                .in_sequence(seq)
                .with(
                    predicate::eq(format!("{}/control/emptyCache", self.client_id)),
                    predicate::always(),
                    predicate::always(),
                    predicate::eq("1"),
                )
                .returning(|_, _, _, _| notify_success(AckOfPub::None));
        }

        fn send_purge_properties(&self, seq: &mut Sequence, client: &mut AsyncClient) {
            if !self.full_handshake {
                return;
            }

            client
                .expect_publish::<String, Vec<u8>>()
                .once()
                .in_sequence(seq)
                .with(
                    predicate::eq(format!("{}/control/producer/properties", self.client_id)),
                    predicate::eq(QoS::ExactlyOnce),
                    predicate::always(),
                    predicate::always(),
                )
                .returning(|_, _, _, _| notify_success(AckOfPub::None));
        }

        fn send_properties(&self, _seq: &mut Sequence, client: &mut AsyncClient) {
            for prop in &self.device_properties {
                client
                    .expect_publish::<String, Vec<u8>>()
                    .once()
                    .with(
                        predicate::eq(format!(
                            "{}/{}{}",
                            self.client_id, prop.interface, prop.path
                        )),
                        predicate::always(),
                        predicate::always(),
                        predicate::always(),
                    )
                    .returning(|_, _, _, _| notify_success(AckOfPub::None));
            }
        }
    }

    fn mock_client<CB>(seq: &mut Sequence, behavior: CB) -> AsyncClient
    where
        CB: ConnectionBehavior + 'static,
    {
        let mut client = AsyncClient::default();

        client
            .expect_clone()
            .once()
            .in_sequence(seq)
            .returning(move || {
                let mut client = AsyncClient::default();

                let mut seq = mockall::Sequence::new();

                behavior.subscribe_interfaces(&mut seq, &mut client);

                behavior.send_introspection(&mut seq, &mut client);

                behavior.send_empty_cache(&mut seq, &mut client);

                behavior.send_purge_properties(&mut seq, &mut client);

                behavior.send_properties(&mut seq, &mut client);

                client
            });

        client
    }

    #[tokio::test]
    async fn test_connect_client_response() {
        let mut seq = mockall::Sequence::new();
        let client_id = ClientId {
            realm: "realm",
            device_id: "device_id",
        };
        let server_interface = Interface::from_str(SERVER_INDIVIDUAL).unwrap();
        let interface = Interface::from_str(DEVICE_PROPERTIES).unwrap();
        let prop = StoredProp {
            interface: interface.interface_name().to_owned(),
            path: "/sensor1/name".to_owned(),
            value: AstarteType::String("temperature".to_string()),
            interface_major: 0,
            ownership: interface.ownership(),
        };

        let behavior = ConnectSuccess {
            full_handshake: true,
            client_id: client_id.into(),
            server_interfaces: vec![server_interface.interface_name().to_owned()],
            device_properties: vec![prop.clone()],
        };

        let mut eventl = EventLoop::new();
        eventl
            .expect_poll()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                Box::pin(async move {
                    tokio::task::yield_now().await;

                    Ok(Event::Incoming(rumqttc::Packet::ConnAck(
                        rumqttc::ConnAck {
                            // a session present doesn't matter if the introspection does not match
                            session_present: true,
                            code: rumqttc::ConnectReturnCode::Success,
                        },
                    )))
                })
            });
        let client = mock_client(&mut seq, behavior);
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
            Interface::from_str(DEVICE_OBJECT).unwrap(),
            Interface::from_str(SERVER_INDIVIDUAL).unwrap(),
        ];

        let interfaces = Interfaces::from_iter(interfaces);
        let store = StoreWrapper::new(MemoryStore::new());

        store
            .store_prop(prop.as_ref())
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
                client_id,
                &interfaces,
                &store,
            ),
        )
        .await
        .expect("timeout reached")
        .expect("failed to connect");

        assert!(connection.state.is_connected());
    }

    #[tokio::test]
    async fn test_connect_store_load_error() {
        let mut seq = mockall::Sequence::new();
        let client_id = ClientId {
            realm: "realm",
            device_id: "device_id",
        };
        let server_interface = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let mut mock_store = MockStore::new();
        // enable session capability
        mock_store.expect_return_session().return_const(true);

        // NOTE an error while loading the stored introspection can happen
        // the error will be logged and the connection will continue with a full
        // handshake
        mock_store
            .expect_load_introspection()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Err(SessionError::load_introspection("test load error")));

        let behavior = ConnectSuccess {
            full_handshake: true,
            client_id: client_id.into(),
            server_interfaces: vec![server_interface.interface_name().to_owned()],
            device_properties: vec![],
        };

        let interfaces = [
            Interface::from_str(DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(DEVICE_OBJECT).unwrap(),
            Interface::from_str(SERVER_INDIVIDUAL).unwrap(),
        ];
        let interfaces = Interfaces::from_iter(interfaces);

        let mut eventl = EventLoop::new();
        eventl
            .expect_poll()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                Box::pin(async move {
                    tokio::task::yield_now().await;

                    Ok(Event::Incoming(rumqttc::Packet::ConnAck(
                        rumqttc::ConnAck {
                            session_present: false,
                            code: rumqttc::ConnectReturnCode::Success,
                        },
                    )))
                })
            });
        // properties will be fetched to be sent during a full handshake
        mock_store
            .expect_device_props_with_unset()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Ok(Vec::new()));
        // a clone is performed to pass the store over to the handshake task
        mock_store.expect_clone().once().returning({
            let interfaces = interfaces.clone();
            move || {
                let mut mock_store = MockStore::new();
                // enable session capability
                mock_store.expect_return_session().return_const(true);
                let mut seq = Sequence::new();

                // when performing a full handshake we clear the introspection
                // to ensure a consistent state or a full handshake
                mock_store
                    .expect_clear_introspection()
                    .once()
                    .in_sequence(&mut seq)
                    .return_const(());

                mock_store
                    .expect_store_introspection()
                    .once()
                    .in_sequence(&mut seq)
                    .with(predicate::function({
                        let expected = interfaces.clone();
                        move |actual| expected.matches(actual)
                    }))
                    .return_const(());

                mock_store
            }
        });
        let client = mock_client(&mut seq, behavior);
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

        let mock_store = StoreWrapper::new(mock_store);

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
                client_id,
                &interfaces,
                &mock_store,
            ),
        )
        .await
        .expect("timeout reached")
        .expect("failed to connect");

        assert!(connection.state.is_connected());
    }

    #[tokio::test]
    async fn test_connect_store_fast_handshake() {
        let mut seq = mockall::Sequence::new();
        let client_id = ClientId {
            realm: "realm",
            device_id: "device_id",
        };
        let server_interface = Interface::from_str(SERVER_INDIVIDUAL).unwrap();

        let mut mock_store = MockStore::new();
        // enable session capability
        mock_store.expect_return_session().return_const(true);

        let interfaces = [
            Interface::from_str(DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(DEVICE_OBJECT).unwrap(),
            Interface::from_str(SERVER_INDIVIDUAL).unwrap(),
        ];

        // NOTE by returning the same interfaces as registered in the device introspeciton
        // only a faster and simpler handshake will be performed
        mock_store
            .expect_load_introspection()
            .once()
            .in_sequence(&mut seq)
            .returning({
                let interfaces = interfaces.clone();
                move || {
                    let interfaces: Vec<IntrospectionInterface> =
                        interfaces.iter().map(|i| i.into()).collect();

                    Ok(interfaces)
                }
            });

        let behavior = ConnectSuccess {
            full_handshake: false,
            client_id: client_id.into(),
            server_interfaces: vec![server_interface.interface_name().to_owned()],
            device_properties: vec![],
        };

        let mut eventl = EventLoop::new();
        eventl
            .expect_poll()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                Box::pin(async move {
                    tokio::task::yield_now().await;

                    Ok(Event::Incoming(rumqttc::Packet::ConnAck(
                        rumqttc::ConnAck {
                            // NOTE the session present flag is true and the introspection matches
                            session_present: true,
                            code: rumqttc::ConnectReturnCode::Success,
                        },
                    )))
                })
            });
        let client = mock_client(&mut seq, behavior);
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

        // a clone is performed to pass the store over to the handshake task
        mock_store.expect_clone().once().returning(MockStore::new);
        // properties will be fetched to be sent after connection
        mock_store
            .expect_device_props_with_unset()
            .once()
            .returning(|| Ok(Vec::new()));
        // after a fast handshake we do not store or clear the old introspection
        // since it is up to date

        // session storage
        let interfaces = Interfaces::from_iter(interfaces);

        let mock_store = StoreWrapper::new(mock_store);

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
                client_id,
                &interfaces,
                &mock_store,
            ),
        )
        .await
        .expect("timeout reached")
        .expect("failed to connect");

        assert!(connection.state.is_connected());
    }
}
