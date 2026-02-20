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

//! Connection to Astarte, for handling events and reconnection on error.

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use async_channel::SendError;
use chrono::Utc;
use futures::future::Either;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::Timestamp;
use crate::error::Report;
use crate::retry::RandomExponentialIter;
use crate::state::{ConnStatus, ConnectionState};
use crate::transport::TransportError;
use crate::{
    Error,
    client::RecvError,
    event::DeviceEvent,
    store::wrapper::StoreWrapper,
    transport::{Connection, Publish, Receive, Reconnect},
};

mod incoming;
mod resend;

/// Handles the messages from the device and astarte.
pub trait EventLoop {
    /// Poll updates from the connection implementation, can be placed in a loop to receive data.
    ///
    /// This is a blocking function. It should be placed on a dedicated thread/task or as the main
    /// thread.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{
    ///     store::memory::MemoryStore, builder::DeviceBuilder,
    ///     transport::mqtt::MqttConfig, types::AstarteData, prelude::*,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mqtt_config = MqttConfig::with_credential_secret("realm_id", "device_id", "credential_secret", "pairing_url");
    ///
    ///     let (client, mut connection) = DeviceBuilder::new()
    ///         .store(MemoryStore::new())
    ///         .connection(mqtt_config)
    ///         .build().await.unwrap();
    ///
    ///     tokio::spawn(async move {
    ///         loop {
    ///             let event = client.recv().await;
    ///             assert!(event.is_ok());
    ///         }
    ///     });
    ///
    ///     connection.handle_events().await;
    /// }
    /// ```
    fn handle_events(self) -> impl Future<Output = Result<(), crate::Error>> + Send;
}

/// Astarte device implementation.
#[derive(Debug)]
pub struct DeviceConnection<C>
where
    C: Connection,
{
    events: async_channel::Sender<Result<DeviceEvent, RecvError>>,
    disconnect: async_channel::Receiver<()>,
    store: StoreWrapper<C::Store>,
    connection: C,
    sender: C::Sender,
    state: ConnectionState,
    resend: Option<JoinHandle<()>>,
    backoff: RandomExponentialIter,
}

impl<C> DeviceConnection<C>
where
    C: Connection,
{
    pub(crate) fn new(
        events: async_channel::Sender<Result<DeviceEvent, RecvError>>,
        disconnect: async_channel::Receiver<()>,
        store: StoreWrapper<C::Store>,
        state: ConnectionState,
        connection: C,
        sender: C::Sender,
        backoff: RandomExponentialIter,
    ) -> Self {
        Self {
            events,
            store,
            state,
            connection,
            sender,
            resend: None,
            backoff,
            disconnect,
        }
    }

    /// Validate a timestamp based on the mapping explicit_timestamp value.
    ///
    // The order of incoming message is guaranteed so, even if we generate the reception
    // timestamp late, we still (should) have a consistent order of timestamp between messages
    fn validate_timestamp(
        interface_name: &str,
        path: &str,
        explicit_timestamp: bool,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Timestamp, RecvError> {
        match (timestamp, explicit_timestamp) {
            (None, false) => Ok(Utc::now()),
            (Some(timestamp), true) => Ok(timestamp),
            (Some(_), false) => {
                warn!("received timestamp on interface without `explicit_timestamp`, ignoring");

                Ok(Utc::now())
            }
            (None, true) => {
                error!("missing timestamp on interface with `explicit_timestamp`");

                if cfg!(debug_assertions) {
                    Err(RecvError::MissingTimestamp {
                        interface_name: interface_name.to_string(),
                        path: path.to_string(),
                    })
                } else {
                    Ok(Utc::now())
                }
            }
        }
    }

    /// Keeps polling connection events
    #[instrument(skip(self))]
    pub(super) async fn poll(&mut self) -> Result<ConnStatus, TransportError>
    where
        C: Receive + Reconnect,
        C::Sender: Publish + 'static,
    {
        trace!("polling connection");
        let Some(event) = self.connection.next_event().await? else {
            info!("disconnected");

            self.state.set_connection(ConnStatus::Disconnected).await;

            // This will check if the connection was closed
            return Ok(ConnStatus::Disconnected);
        };

        trace!("event received");

        let event = self
            .handle_event(&event.interface, &event.path, event.payload)
            .await
            .map(|data| DeviceEvent {
                interface: event.interface,
                path: event.path,
                data,
            })?;

        self.send_to_clients(Ok(event)).await.map_err(|err| {
            debug!(error = %Report::new(err), "disconnected");

            TransportError::Transport(Error::Disconnected)
        })?;

        Ok(ConnStatus::Connected)
    }

    async fn send_to_clients(
        &self,
        event: Result<DeviceEvent, RecvError>,
    ) -> Result<(), SendError<Result<DeviceEvent, RecvError>>> {
        let send = pin!(self.events.send(event));
        let timeout = pin!(tokio::time::sleep(Duration::from_secs(10)));

        match futures::future::select(send, timeout).await {
            Either::Left((send_res, _)) => send_res.inspect(|()| trace!("event sent to device")),
            Either::Right(((), send)) => {
                warn!("slow to send Astarte events to client, maybe no one is consuming them");

                send.await
            }
        }
    }

    async fn run_until_disconnect<F>(
        disconnect: &async_channel::Receiver<()>,
        f: F,
    ) -> Option<F::Output>
    where
        F: Future,
    {
        if disconnect.is_empty() && disconnect.is_closed() {
            return Some(f.await);
        }

        let disconnect = pin!(disconnect.recv());
        let f = pin!(f);

        match futures::future::select(disconnect, f).await {
            Either::Left((Ok(()), _f)) => {
                debug!("disconnect received");

                None
            }
            Either::Left((Err(error), f)) => {
                error!(%error, "disconnect closed");

                Some(f.await)
            }
            Either::Right((f_out, _disconnect)) => Some(f_out),
        }
    }
}

impl<C> Drop for DeviceConnection<C>
where
    C: Connection,
{
    fn drop(&mut self) {
        let state = self.state.clone();

        tokio::task::spawn(async move {
            state.set_connection(ConnStatus::Closed).await;
        });
    }
}

impl<C> EventLoop for DeviceConnection<C>
where
    C: Connection + Reconnect + Receive + 'static,
    C::Sender: Publish + 'static,
{
    async fn handle_events(mut self) -> Result<(), crate::Error> {
        self.init_stored_retention().await?;

        loop {
            match self.poll().await {
                Ok(ConnStatus::Connected) => {}
                Ok(ConnStatus::Disconnected) => {
                    if self.reconnect_and_resend().await?.is_break() {
                        break;
                    }
                }
                Ok(ConnStatus::Closed) => {
                    break;
                }
                Err(TransportError::Transport(err)) => {
                    return Err(err);
                }
                // send the error to the client
                Err(TransportError::Recv(recv_err)) => {
                    self.events.send(Err(recv_err)).await.map_err(|send_err| {
                        if let Err(err) = send_err.into_inner() {
                            error!(error = %Report::new(err), "failed to send receive error");
                        }

                        Error::Disconnected
                    })?;
                }
            }
        }

        info!("connection closed successfully");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{ControlFlow, Deref, DerefMut};
    use std::str::FromStr;
    use std::sync::Arc;

    use astarte_interfaces::{Interface, Schema};
    use futures::FutureExt;
    use mockall::Sequence;
    use pretty_assertions::assert_eq;

    use crate::AstarteData;
    use crate::builder::{DEFAULT_CHANNEL_SIZE, DEFAULT_VOLATILE_CAPACITY};
    use crate::interfaces::Interfaces;
    use crate::retention::memory::VolatileStore;
    use crate::state::SharedState;
    use crate::store::StoreCapabilities;
    use crate::store::memory::MemoryStore;
    use crate::test::{E2E_SERVER_DATASTREAM, E2E_SERVER_DATASTREAM_NAME};
    use crate::transport::ReceivedEvent;
    use crate::transport::mock::{MockCon, MockSender};

    use super::*;

    pub(crate) struct TestConnection<S>
    where
        S: StoreCapabilities,
    {
        pub(crate) inner: DeviceConnection<MockCon<S>>,
        pub(crate) events: async_channel::Receiver<Result<DeviceEvent, RecvError>>,
        pub(crate) disconnect: async_channel::Sender<()>,
    }

    impl<S> Deref for TestConnection<S>
    where
        S: StoreCapabilities,
    {
        type Target = DeviceConnection<MockCon<S>>;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl<S> DerefMut for TestConnection<S>
    where
        S: StoreCapabilities,
    {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.inner
        }
    }

    pub(crate) fn mock_connection(
        interfaces: &[&str],
        initial_status: ConnStatus,
    ) -> TestConnection<MemoryStore> {
        mock_connection_with_store(interfaces, initial_status, MemoryStore::new())
    }

    pub(crate) fn mock_connection_with_store<S>(
        interfaces: &[&str],
        initial_status: ConnStatus,
        store: S,
    ) -> TestConnection<S>
    where
        S: StoreCapabilities,
    {
        let interfaces = interfaces.iter().map(|i| Interface::from_str(i).unwrap());
        let interfaces = Interfaces::from_iter(interfaces);

        let connection = MockCon::new();
        let sender = MockSender::new();
        let (events_tx, events_rx) = async_channel::bounded(DEFAULT_CHANNEL_SIZE);
        let (disconnect_tx, disconnect_rx) = async_channel::bounded(1);
        let mut state = SharedState::new(
            interfaces,
            VolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY),
        );

        *state.status.get_mut() = initial_status;

        let connection = DeviceConnection::new(
            events_tx,
            disconnect_rx,
            StoreWrapper::new(store),
            ConnectionState::new(Arc::new(state)),
            connection,
            sender,
            RandomExponentialIter::default(),
        );

        TestConnection {
            inner: connection,
            events: events_rx,
            disconnect: disconnect_tx,
        }
    }

    #[tokio::test]
    async fn poll_disconnected() {
        let mut connection = mock_connection(&[], ConnStatus::Connected);

        let mut seq = Sequence::new();

        connection
            .connection
            .expect_next_event()
            .once()
            .in_sequence(&mut seq)
            .with()
            .returning(|| Ok(None));

        let status = connection.poll().await.unwrap();

        assert_eq!(status, ConnStatus::Disconnected);
    }

    #[tokio::test]
    async fn reconnect_cancelled_for_disconnect() {
        let mut connection = mock_connection(&[], ConnStatus::Disconnected);

        let mut seq = Sequence::new();
        connection
            .connection
            .expect_reconnect()
            .times(0..)
            .in_sequence(&mut seq)
            .returning(|_| Box::pin(futures::future::pending()));

        let disconnect = connection.disconnect;
        let handle = tokio::spawn(async move { connection.inner.reconnect_and_resend().await });

        disconnect.try_send(()).unwrap();

        let status = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(status, ControlFlow::Break(()));
    }

    #[tokio::test]
    async fn poll_individual() {
        let mut connection = mock_connection(&[E2E_SERVER_DATASTREAM], ConnStatus::Disconnected);

        let endpoint = "/boolean_endpoint";
        let value = true;

        let mut seq = Sequence::new();

        connection
            .connection
            .expect_reconnect()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| futures::future::ok(true).boxed());

        connection
            .sender
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(MockSender::new);

        connection
            .connection
            .expect_next_event()
            .once()
            .in_sequence(&mut seq)
            .with()
            .returning(move || {
                Ok(Some(ReceivedEvent {
                    interface: E2E_SERVER_DATASTREAM_NAME.to_string(),
                    path: endpoint.to_string(),
                    payload: Box::new(value),
                }))
            });

        connection
            .connection
            .expect_deserialize_individual()
            .once()
            .in_sequence(&mut seq)
            .withf(move |mapping, payload| {
                mapping.interface().name() == E2E_SERVER_DATASTREAM_NAME
                    && mapping.path().as_str() == endpoint
                    && *payload.downcast_ref::<bool>().unwrap() == value
            })
            .returning(|_, payload| {
                let value = payload
                    .downcast_ref::<bool>()
                    .map(|val| AstarteData::Boolean(*val))
                    .unwrap();

                Ok((value, None))
            });

        // first ensure the status is connected (starts off with a disconnected status)
        let controlflow = connection.reconnect_and_resend().await.unwrap();

        assert_eq!(controlflow, ControlFlow::Continue(()));

        let status = connection.poll().await.unwrap();

        assert_eq!(status, ConnStatus::Connected);

        let event = connection.events.try_recv().unwrap().unwrap();

        // Cannot eq the timestamp
        assert_eq!(event.interface, E2E_SERVER_DATASTREAM_NAME);
        assert_eq!(event.path, endpoint);
        let data = event.data.try_into_individual().unwrap();
        assert_eq!(data.0, AstarteData::Boolean(value));
    }
}
