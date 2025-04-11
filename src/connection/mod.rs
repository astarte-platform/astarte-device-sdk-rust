// This file is part of Astarte.
//
// Copyright 2024 - 2025 SECO Mind Srl
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
use std::sync::Arc;

use chrono::Utc;
use tracing::{debug, info, warn};

use crate::state::SharedState;
use crate::transport::TransportError;
use crate::Timestamp;
use crate::{
    client::RecvError,
    event::DeviceEvent,
    store::wrapper::StoreWrapper,
    transport::{Connection, Disconnect, Publish, Receive, Reconnect, Register},
    Error,
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
    ///     transport::mqtt::MqttConfig, types::AstarteType, prelude::*,
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
    tx: flume::Sender<Result<DeviceEvent, RecvError>>,
    store: StoreWrapper<C::Store>,
    connection: C,
    sender: C::Sender,
    state: Arc<SharedState>,
}

impl<C> DeviceConnection<C>
where
    C: Connection,
{
    pub(crate) fn new(
        tx: flume::Sender<Result<DeviceEvent, RecvError>>,
        store: StoreWrapper<C::Store>,
        state: Arc<SharedState>,
        connection: C,
        sender: C::Sender,
    ) -> Self {
        Self {
            tx,
            store,
            state,
            connection,
            sender,
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
            (None, true) => Err(RecvError::MissingTimestamp {
                interface_name: interface_name.to_string(),
                path: path.to_string(),
            }),
        }
    }
}

impl<C> EventLoop for DeviceConnection<C>
where
    C: Connection + Reconnect + Receive + Send + Sync + 'static,
    C::Sender: Send + Register + Publish + Disconnect + 'static,
{
    async fn handle_events(mut self) -> Result<(), crate::Error> {
        self.init_stored_retention().await?;

        loop {
            let opt = match self.connection.next_event().await {
                Ok(opt) => opt,
                Err(TransportError::Transport(err)) => {
                    return Err(err);
                }
                // send the error to the client
                Err(TransportError::Recv(recv_err)) => {
                    self.tx
                        .send_async(Err(recv_err))
                        .await
                        .map_err(|_| Error::Disconnected)?;

                    continue;
                }
            };

            let Some(event_data) = opt else {
                if self.state.status.is_closed() {
                    debug!("connection closed");

                    break;
                }

                debug!("reconnecting");

                self.state.status.set_connected(false);

                let interfaces = self.state.interfaces.read().await;

                self.connection.reconnect(&interfaces).await?;

                Self::resend_volatile_publishes(
                    &self.state.volatile_store,
                    &mut self.sender,
                    &self.state.status,
                )
                .await?;
                Self::resend_stored_publishes(
                    &mut self.store,
                    &mut self.sender,
                    &self.state.status,
                )
                .await?;

                self.state.status.set_connected(true);

                continue;
            };

            self.handle_connection_event(event_data).await?
        }

        info!("connection closed successfully");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::builder::{DEFAULT_CHANNEL_SIZE, DEFAULT_VOLATILE_CAPACITY};
    use crate::interfaces::Interfaces;
    use crate::retention::memory::VolatileStore;
    use crate::store::memory::MemoryStore;
    use crate::store::StoreCapabilities;
    use crate::transport::mock::{MockCon, MockSender};
    use crate::Interface;

    use super::*;

    #[expect(dead_code)]
    pub(crate) fn mock_connection(
        interfaces: &[&str],
    ) -> (
        DeviceConnection<MockCon<MemoryStore>>,
        flume::Receiver<Result<DeviceEvent, RecvError>>,
    ) {
        mock_connection_with_store(interfaces, MemoryStore::new())
    }

    pub(crate) fn mock_connection_with_store<S>(
        interfaces: &[&str],
        store: S,
    ) -> (
        DeviceConnection<MockCon<S>>,
        flume::Receiver<Result<DeviceEvent, RecvError>>,
    )
    where
        S: StoreCapabilities,
    {
        let interfaces = interfaces.iter().map(|i| Interface::from_str(i).unwrap());
        let interfaces = Interfaces::from_iter(interfaces);

        let connection = MockCon::new();
        let sender = MockSender::new();
        let (tx, rx) = flume::bounded(DEFAULT_CHANNEL_SIZE);
        let state = SharedState::new(
            interfaces,
            VolatileStore::with_capacity(DEFAULT_VOLATILE_CAPACITY),
        );

        let connection = DeviceConnection::new(
            tx,
            StoreWrapper::new(store),
            Arc::new(state),
            connection,
            sender,
        );

        (connection, rx)
    }
}
