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
    fmt::{Debug, Display},
    pin::pin,
};

use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use rumqttc::{
    ClientError, ConnectionError, Event, NoticeError, NoticeFuture, Packet, Publish, QoS,
    SubscribeFilter, Transport,
};
use sync_wrapper::SyncWrapper;
use tokio::{
    select,
    task::{JoinError, JoinHandle},
};

use crate::{
    interfaces::Interfaces,
    store::{error::StoreError, wrapper::StoreWrapper, PropertyStore, StoredProp},
    transport::mqtt::{pairing::ApiClient, payload::Payload},
};

use super::{
    client::{AsyncClient, EventLoop},
    config::transport::TransportProvider,
    ClientId, PayloadError, SessionData,
};

pub struct MqttState {
    connection: Connection,
    state: State,
}

impl MqttState {
    pub(crate) async fn wait_connack<S>(
        client: AsyncClient,
        eventloop: EventLoop,
        provider: TransportProvider,
        client_id: ClientId<'_>,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> Result<Self, StoreError>
    where
        S: PropertyStore,
    {
        let mut connection = Connection {
            client,
            eventloop: SyncWrapper::new(eventloop),
            provider,
        };

        let mut state = State::WaitConnack(WaitConnack);

        // We wait till the state of the connection is not init, since the messages to the broker
        // should have been queued firs and we have yet to receive any publish.
        while !state.is_init() {
            let publish = state
                .poll(client_id, &mut connection, interfaces, store)
                .await?;

            if let Some(publish) = publish {
                debug!("unexpected publish {publish:?}");

                unreachable!("BUG: publish received in non init state");
            }
        }

        Ok(Self { state, connection })
    }

    pub(crate) async fn next_publish<S>(
        &mut self,
        client_id: ClientId<'_>,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> Result<Publish, StoreError>
    where
        S: PropertyStore,
    {
        // TODO: implement a timeout
        loop {
            let publish = self
                .state
                .poll(client_id, &mut self.connection, interfaces, store)
                .await?;

            if let Some(publish) = publish {
                return Ok(publish);
            }
        }
    }

    pub(crate) fn client(&self) -> &AsyncClient {
        &self.connection.client
    }

    /// Sends the introspection [`String`].
    pub(crate) async fn send_introspection(
        &self,
        client_id: ClientId<'_>,
        introspection: String,
    ) -> Result<(), ClientError> {
        StartInit::send_introspection(self.client(), client_id, introspection)
            .await
            .map(drop)
    }

    /// Subscribe to many interfaces
    pub(crate) async fn subscribe_many(
        &self,
        client_id: ClientId<'_>,
        interfaces_names: &[&str],
    ) -> Result<(), ClientError> {
        Connection::subscribe_many(self.client(), client_id, interfaces_names)
            .await
            .map(drop)
    }
}

struct Connection {
    client: AsyncClient,
    // NOTE: this should be replaces by Exclusive<EventLoop> when the feature `exclusive_wrapper`
    //       is stabilized or the EventLoop becomes Sync
    //       https://doc.rust-lang.org/std/sync/struct.Exclusive.html
    eventloop: SyncWrapper<EventLoop>,
    provider: TransportProvider,
}

impl Connection {
    fn eventloop(&mut self) -> &mut EventLoop {
        self.eventloop.get_mut()
    }

    /// Subscribe to many interfaces
    async fn subscribe_many<S>(
        client: &AsyncClient,
        client_id: ClientId<'_>,
        interfaces_names: &[S],
    ) -> Result<Option<NoticeFuture>, ClientError>
    where
        S: Display + Debug,
    {
        // should not subscribe if there are no interfaces
        if interfaces_names.is_empty() {
            debug!("empty subscribe many");

            return Ok(None);
        } else if interfaces_names.len() == 1 {
            trace!("subscribing on single interface");

            let name = &interfaces_names[0];

            return client
                .subscribe(format!("{client_id}/{name}/#"), rumqttc::QoS::ExactlyOnce)
                .await
                .map(Some);
        }

        trace!("subscribing on {interfaces_names:?}");

        let topics = interfaces_names
            .iter()
            .map(|name| SubscribeFilter {
                path: format!("{client_id}/{name}/#"),
                qos: rumqttc::QoS::ExactlyOnce,
            })
            .collect_vec();

        debug!("topics {topics:?}");

        client.subscribe_many(topics).await.map(Some)
    }
}

#[derive(Debug)]
enum State {
    Disconnected(Disconnected),
    WaitConnack(WaitConnack),
    StartInit(StartInit),
    Init(Init),
    Connected(Connected),
}

impl State {
    fn from_error<T>(state: T, err: ConnectionError) -> State
    where
        T: Into<State>,
    {
        error!("error received from mqtt connection: {err}");

        match err {
            ConnectionError::MqttState(_)
            | ConnectionError::NetworkTimeout
            | ConnectionError::FlushTimeout
            | ConnectionError::Io(_)
            | ConnectionError::RequestsDone => {
                trace!("no state change");

                state.into()
            }
            ConnectionError::NotConnAck(_) => State::WaitConnack(WaitConnack),
            ConnectionError::Tls(_) | ConnectionError::ConnectionRefused(_) => {
                State::Disconnected(Disconnected)
            }
        }
    }

    /// Sets the default state and returns the current state.
    fn take(&mut self) -> State {
        std::mem::take(self)
    }

    /// Returns `true` if the state is [`Init`].
    ///
    /// [`Init`]: State::Init
    #[must_use]
    fn is_init(&self) -> bool {
        matches!(self, Self::Init(..))
    }

    async fn poll<S>(
        &mut self,
        client_id: ClientId<'_>,
        connection: &mut Connection,
        interfaces: &Interfaces,
        store: &StoreWrapper<S>,
    ) -> Result<Option<Publish>, StoreError>
    where
        S: PropertyStore,
    {
        trace!("state {}", self);

        let (state, packet) = match self.take() {
            State::Disconnected(disconnected) => {
                let state = disconnected.poll(connection, client_id).await;

                (state, None)
            }
            State::WaitConnack(wait_connack) => {
                let state = wait_connack.poll(connection).await;

                (state, None)
            }
            State::StartInit(start_init) => {
                let session_data = SessionData::try_from_props(interfaces, store).await?;

                let state = start_init.poll(&connection, client_id, session_data);

                (state, None)
            }
            State::Init(init) => match init.poll(connection).await {
                Next::State(state) => (state, None),
                Next::Publish(publish, state) => (state, Some(publish)),
            },
            State::Connected(connected) => match connected.poll(connection).await {
                Next::State(state) => (state, None),
                Next::Publish(publish, state) => (state, Some(publish)),
            },
        };

        *self = state;

        Ok(packet)
    }
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            State::Disconnected(_) => "Disconnected",
            State::WaitConnack(_) => "WaitConnack",
            State::StartInit(_) => "StartInit",
            State::Init(_) => "Init",
            State::Connected(_) => "Connected",
        };

        write!(f, "{state}")
    }
}

impl Default for State {
    fn default() -> Self {
        Self::Disconnected(Disconnected)
    }
}

/// Next state or next publish after polling the connection.
#[derive(Debug)]
enum Next {
    State(State),
    Publish(Publish, State),
}

impl Next {
    fn from_event<T>(state: T, event: Event) -> Next
    where
        T: Into<State>,
    {
        let incoming = match event {
            Event::Incoming(incoming) => {
                trace!("incoming packet {incoming:?}");

                incoming
            }
            Event::Outgoing(outgoing) => {
                trace!("outgoing packet {outgoing:?}");

                return Next::State(state.into());
            }
        };

        match incoming {
            rumqttc::Packet::ConnAck(connack) => {
                debug!("connack received, initializing connection");

                Next::State(State::StartInit(StartInit {
                    session_present: connack.session_present,
                }))
            }
            rumqttc::Packet::Publish(publish) => {
                trace!("incoming publish");

                Next::Publish(publish, state.into())
            }
            Packet::Disconnect => {
                debug!("server sent a disconnect packet");

                Next::State(State::Disconnected(Disconnected))
            }
            _ => {
                trace!("incoming packet");

                Next::State(state.into())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Disconnected;

impl Disconnected {
    /// Recreate the MQTT transport (TLS) to the broker.
    ///
    /// It recreate the credentials and reconnect to the broker, using the same
    /// session. If it fails, it returns an error so that the whole connection process can
    /// be retried.
    async fn poll(self, connection: &mut Connection, client_id: ClientId<'_>) -> State {
        let api =
            ApiClient::from_transport(&connection.provider, client_id.realm, client_id.device_id);

        let transport = match connection.provider.recreate_transport(&api).await {
            Ok(transport) => transport,
            Err(err) => {
                error!("couldn't pair device: {err}");

                return State::Disconnected(self);
            }
        };

        debug!("created a new transport, reconnecting");

        connection.eventloop().clean();
        Self::set_transport(connection.eventloop(), transport);

        State::WaitConnack(WaitConnack)
    }

    #[cfg(not(test))]
    fn set_transport(eventloop: &mut EventLoop, transport: Transport) {
        eventloop.mqtt_options.set_transport(transport);
    }

    #[cfg(test)]
    fn set_transport(_eventloop: &mut EventLoop, _transport: Transport) {}
}

/// Waits for an incoming MQTT Connack packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitConnack;

impl WaitConnack {
    async fn poll(self, connection: &mut Connection) -> State {
        let event = match connection.eventloop().poll().await {
            Ok(event) => event,
            Err(err) => return State::from_error(self, err),
        };

        match event {
            Event::Incoming(Packet::ConnAck(connack)) => {
                trace!("connack received");

                State::StartInit(StartInit {
                    session_present: connack.session_present,
                })
            }
            Event::Incoming(incoming) => {
                error!("unexpected packet received while waiting for connack {incoming:?}");

                State::Disconnected(Disconnected)
            }
            Event::Outgoing(outgoing) => {
                warn!("unexpected outgoing packet while waiting for connack {outgoing:?}");

                // We stay in connack since we shouldn't have any outgoing packet in this state, but
                // the specification doesn't disallow the client to send packet while waiting for
                // the ConnAck
                State::WaitConnack(self)
            }
        }
    }
}

impl From<WaitConnack> for State {
    fn from(value: WaitConnack) -> Self {
        Self::WaitConnack(value)
    }
}

/// State that gets called when a [`rumqttc::ConnAck`] is received.
/// Following the astarte protocol it performs the following tasks:
///  - Subscribes to the server owned interfaces in the interface list
///  - Sends the introspection
///  - Sends the emptycache command
///  - Sends the device owned properties stored locally
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StartInit {
    session_present: bool,
}

impl StartInit {
    fn poll(
        self,
        connection: &Connection,
        client_id: ClientId<'_>,
        session_data: SessionData,
    ) -> State {
        let client = connection.client.clone();

        let realm = client_id.realm.to_string();
        let device_id = client_id.device_id.to_string();

        let handle: JoinHandle<Result<(), InitError>> = tokio::spawn(async move {
            let client_id = ClientId {
                realm: &realm,
                device_id: &device_id,
            };

            Self::subscribe_server_interfaces(&client, client_id, &session_data.server_interfaces)
                .await?;
            Self::send_introspection(&client, client_id, session_data.interfaces)
                .await
                .map_err(InitError::client("send introspection"))?
                .wait_async()
                .await
                .map_err(InitError::notice("subscribe server interface"))?;

            debug!("session present {}", self.session_present);
            if !self.session_present {
                Self::send_empty_cache(&client, client_id).await?;
                Self::send_device_properties(&client, client_id, &session_data.device_properties)
                    .await?;
            }

            Ok(())
        });

        State::Init(Init { handle })
    }

    /// Subscribes to the passed list of interfaces
    async fn subscribe_server_interfaces(
        client: &AsyncClient,
        client_id: ClientId<'_>,
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
            .wait_async()
            .await
            .map_err(InitError::notice("subscribe consumer properties"))?;

        debug!(
            "subscribing on {} server interfaces",
            server_interfaces.len()
        );

        let notice = Connection::subscribe_many(client, client_id, server_interfaces)
            .await
            .map_err(InitError::client("subscribe server interface"))?;

        if let Some(notice) = notice {
            notice
                .wait_async()
                .await
                .map_err(InitError::notice("subscribe server interface"))?;
        }

        Ok(())
    }

    /// Sends the introspection [`String`].
    async fn send_introspection(
        client: &AsyncClient,
        client_id: ClientId<'_>,
        introspection: String,
    ) -> Result<NoticeFuture, ClientError> {
        debug!("sending introspection: {introspection}");

        let path = client_id.to_string();

        client
            .publish(path, QoS::ExactlyOnce, false, introspection)
            .await
    }

    /// Sends the empty cache command as per the astarte protocol definition
    async fn send_empty_cache(
        client: &AsyncClient,
        client_id: ClientId<'_>,
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
            .wait_async()
            .await
            .map_err(InitError::notice("empty cache"))
    }

    /// Sends the passed device owned properties
    async fn send_device_properties(
        client: &AsyncClient,
        client_id: ClientId<'_>,
        device_properties: &[StoredProp],
    ) -> Result<(), InitError> {
        for prop in device_properties {
            let topic = format!("{}/{}{}", client_id, prop.interface, prop.path);

            debug!(
                "sending device-owned property = {}{}",
                prop.interface, prop.path
            );

            let payload = Payload::new(&prop.value).to_vec()?;

            // Don't wait for the ack since it's not fundamental for the connection
            client
                .publish(topic, rumqttc::QoS::ExactlyOnce, false, payload)
                .await
                .map_err(InitError::client("device property"))?;
        }

        Ok(())
    }
}

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
    Notice {
        #[source]
        backtrace: NoticeError,
        ctx: &'static str,
    },
    /// Couldn't serialize the device property payload.
    #[error("coudln't serialize the device property payload")]
    Payload(#[from] PayloadError),
}

impl InitError {
    const fn client(ctx: &'static str) -> impl Fn(ClientError) -> InitError {
        move |backtrace: ClientError| InitError::Client { backtrace, ctx }
    }

    const fn notice(ctx: &'static str) -> impl Fn(NoticeError) -> InitError {
        move |backtrace: NoticeError| InitError::Notice { backtrace, ctx }
    }
}

/// Following the astarte protocol it performs the following tasks:
///  - Subscribes to the server owned interfaces in the interface list
///  - Sends the introspection
///  - Sends the emptycache command
///  - Sends the device owned properties stored locally
#[derive(Debug)]
struct Init {
    handle: JoinHandle<Result<(), InitError>>,
}

impl Init {
    async fn poll(mut self, connection: &mut Connection) -> Next {
        dbg!("cacca");

        let mut a = pin!(&mut self.handle);

        let next = select! {
            res = &mut a => {
                dbg!("join");
                Next::State(Self::handle_join(res))
            },
            event = connection.eventloop().poll() => {
                dbg!("evnt");
                Self::handle_event(self, event)
            }
        };

        dbg!(&next);

        next
    }

    fn handle_join(res: Result<Result<(), InitError>, JoinError>) -> State {
        match res {
            Ok(Ok(())) => {
                info!("device connected");
                State::Connected(Connected)
            }
            Ok(Err(err)) => {
                error!("init task failed: {err}");

                State::Disconnected(Disconnected)
            }
            Err(err) => {
                error!("failed to join init task: {err}");

                State::Disconnected(Disconnected)
            }
        }
    }

    fn handle_event(self, event: Result<Event, ConnectionError>) -> Next {
        match event {
            Ok(event) => {
                trace!("event received {event:?}");

                Next::from_event(self, event)
            }
            Err(err) => Next::State(State::from_error(self, err)),
        }
    }
}

impl From<Init> for State {
    fn from(value: Init) -> Self {
        State::Init(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Connected;

impl Connected {
    async fn poll(self, connection: &mut Connection) -> Next {
        let event = match connection.eventloop().poll().await {
            Ok(event) => event,
            Err(err) => return Next::State(State::from_error(self, err)),
        };

        Next::from_event(self, event)
    }
}

impl From<Connected> for State {
    fn from(value: Connected) -> Self {
        Self::Connected(value)
    }
}
