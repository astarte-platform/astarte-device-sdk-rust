// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
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

//! Connection to Astarte, for handling events and reconnection on error.

use std::future::Future;
use std::num::NonZero;
use std::ops::ControlFlow;

use astarte_device_error::{ResultExt, WrapError};
use tokio::task::JoinSet;
use tracing::{debug, info, instrument, trace};

use crate::builder::ConnectionConfig;
use crate::error::{AstarteError, ErrorKind};
use crate::event::DeviceEvent;
use crate::retention::{StoredRetention, StoredRetentionExt};
use crate::retry::{RetryAction, RetryFuture};
use crate::state::{ConnStatus, ConnectionState};
use crate::store::StoreCapabilities;
use crate::transport::Introspection;
use crate::transport::Sender;
use crate::transport::Transport;
use crate::validate::Validated;

use self::incoming::ReceiverTask;
use self::outgoing::SenderTask;

pub(crate) mod incoming;
pub(crate) mod outgoing;

/// Handles the messages from the device and astarte.
pub trait Connection {
    /// Poll updates from the connection implementation, can be placed in a loop to receive data.
    ///
    /// This is a blocking function. It should be placed on a dedicated thread/task or as the main
    /// thread.
    ///
    /// ```no_run
    /// use astarte_device_sdk::store::memory::MemoryStore;
    /// use astarte_device_sdk::builder::DeviceBuilder;
    /// use astarte_device_sdk::transport::mqtt::{MqttConfig, Credential, MqttArgs};
    /// use astarte_device_sdk::types::AstarteData;
    /// use astarte_device_sdk::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let args = MqttArgs {
    ///         realm: "realm_id".to_string(),
    ///         device_id: "device_id".to_string(),
    ///         credential: Credential::secret("credential_secret"),
    ///         pairing_url: "http://api.astarte.localhost/pairing".parse().expect("a valid URL")
    ///     };
    ///     let mqtt_config = MqttConfig::new(args);
    ///
    ///     let (client, mut connection) = DeviceBuilder::new()
    ///         .store(MemoryStore::new())
    ///         .connection(mqtt_config)
    ///         .build().await.unwrap();
    ///
    ///     tokio::spawn(async move {
    ///         loop {
    ///             let event = client.recv().await;
    ///             assert!(event.is_some());
    ///         }
    ///     });
    ///
    ///     connection.handle_events().await;
    /// }
    /// ```
    fn handle_events(self) -> impl Future<Output = Result<(), AstarteError>> + Send;
}

/// Astarte device implementation.
// TODO: we cannot implement drop on device connection since we move it's fields
#[derive(Debug)]
pub struct DeviceConnection<C, S> {
    connection: C,
    state: ConnectionState<S>,
    events: async_channel::Sender<DeviceEvent>,
    client_rx: tokio::sync::mpsc::Receiver<Validated>,
    status_rx: tokio::sync::watch::Receiver<ConnStatus>,
}

impl<C, S> DeviceConnection<C, S> {
    pub(crate) fn new(
        connection: C,
        state: ConnectionState<S>,
        events: async_channel::Sender<DeviceEvent>,
        client_rx: tokio::sync::mpsc::Receiver<Validated>,
        status_rx: tokio::sync::watch::Receiver<ConnStatus>,
    ) -> Self {
        Self {
            events,
            state,
            connection,
            client_rx,
            status_rx,
        }
    }

    /// This function is called once at the start to send all the stored packet.
    #[instrument(skip(self))]
    pub(crate) async fn init_store(
        &self,
        stored_retention: NonZero<usize>,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        trace!("initialize stored retention and properties");

        let interfaces = self.state.interfaces().read().await;

        // set max retention items in the store
        if let Some(retention) = self.state.store().get_retention() {
            {
                debug!("cleaning up the retention introspection");
                retention
                    .cleanup_introspection(&interfaces)
                    .await
                    .map_kind(ErrorKind::Retention)?;
            }

            retention
                .set_max_retention_items(stored_retention)
                .await
                .map_kind(ErrorKind::Retention)?;

            debug!("resetting all datastream sent flags");
            retention
                .reset_all_publishes()
                .await
                .map_kind(ErrorKind::Retention)?;
        }

        trace!("resetting all properties state");

        self.state
            .store()
            .reset_session()
            .await
            .map_kind(ErrorKind::Store)?;

        Ok(())
    }
}

impl<C, S> Connection for DeviceConnection<C, S>
where
    C: ConnectionConfig,
    C::Connection: Transport,
    C::Client: Sender + Introspection,
    S: StoreCapabilities,
{
    #[instrument(skip(self))]
    async fn handle_events(mut self) -> Result<(), AstarteError> {
        trace!("starting connection");

        let mut backoff = RetryFuture {
            rx: &mut self.status_rx,
            state: &self.state,
        };

        // register the device
        let mut action = RegisterAction {
            connection: &mut self.connection,
            state: &self.state,
        };
        let Some((sender, connection)) = backoff.retry(&mut action).await? else {
            info!("connection closed while pairing");

            return Ok(());
        };

        let mut tasks = JoinSet::new();

        let mut receiver = ReceiverTask {
            connection,
            state: self.state.clone(),
            status_rx: self.status_rx.clone(),
            events: self.events,
            first: true,
        };

        let mut sender = SenderTask {
            client_rx: self.client_rx,
            status_rx: self.status_rx,
            sender,
            state: self.state,
        };

        // spawn receive task
        tasks.spawn(async move { receiver.receiver().await });

        // spawn send task
        tasks.spawn(async move { sender.sender().await });

        // join tasks
        while let Some(res) = tasks.join_next().await {
            res.wrap_err_msg(ErrorKind::Disconnected, "while joining task")
                .flatten()?;
        }

        info!("connection closed successfully");

        Ok(())
    }
}

// TODO: check if the register is cancel safe
struct RegisterAction<'a, C, S> {
    connection: &'a mut C,
    state: &'a ConnectionState<S>,
}

impl<'a, C, S> RetryAction for RegisterAction<'a, C, S>
where
    C: ConnectionConfig,
    S: StoreCapabilities,
{
    type Out = (C::Client, C::Connection);

    type Err = AstarteError;

    async fn make(&mut self) -> Result<ControlFlow<Self::Out>, Self::Err> {
        self.connection.register(self.state).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::NonZero;
    use std::ops::{Deref, DerefMut};
    use std::str::FromStr;
    use std::sync::Arc;

    use astarte_interfaces::Interface;
    use mockall::{Sequence, predicate};

    use crate::builder::DEFAULT_CHANNEL_SIZE;
    use crate::interfaces::Interfaces;
    use crate::retention::StoredInterface;
    use crate::state::tests::mock_state;
    use crate::store::StoreCapabilities;
    use crate::store::mock::MockStore;
    use crate::transport::mock::MockCon;

    use super::*;

    pub(crate) struct TestConnection<S>
    where
        S: StoreCapabilities,
    {
        pub(crate) inner: DeviceConnection<MockCon, S>,
        pub(crate) _events: async_channel::Receiver<DeviceEvent>,
        pub(crate) _client: tokio::sync::mpsc::Sender<Validated>,
    }

    impl<S> Deref for TestConnection<S>
    where
        S: StoreCapabilities,
    {
        type Target = DeviceConnection<MockCon, S>;

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
        let (events_tx, events_rx) = async_channel::bounded(DEFAULT_CHANNEL_SIZE.get());
        let (client_tx, client_rx) = tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE.get());
        let (status_tx, status_rx) = tokio::sync::watch::channel(initial_status);
        let state = mock_state(store, status_tx, interfaces);

        let connection = DeviceConnection::new(
            connection,
            ConnectionState::new(Arc::new(state)),
            events_tx,
            client_rx,
            status_rx,
        );

        TestConnection {
            inner: connection,
            _events: events_rx,
            _client: client_tx,
        }
    }

    #[tokio::test]
    async fn init_store_mock_store() {
        let retention_size = NonZero::new(1).unwrap();

        let retention_intf = "com.example";

        let mut store = MockStore::new();
        let mut seq = Sequence::new();

        store
            .expect_return_retention()
            .once()
            .in_sequence(&mut seq)
            .returning(|| true);
        store
            .expect_fetch_all_interfaces_call()
            .once()
            .in_sequence(&mut seq)
            .returning(|| {
                Ok(HashSet::from_iter([StoredInterface {
                    name: retention_intf.to_string(),
                    version_major: 1,
                }]))
            });

        store
            .expect_delete_interface_call()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(retention_intf))
            .returning(|_| Ok(()));

        store
            .expect_set_max_retention_items_call()
            .once()
            .with(predicate::eq(retention_size))
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));

        store
            .expect_reset_all_publishes_call()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Ok(()));

        store
            .expect_reset_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Ok(()));

        let connection = mock_connection_with_store(&[], ConnStatus::Offline, store);

        connection.init_store(retention_size).await.unwrap();
    }

    #[tokio::test]
    async fn init_store_mock_store_no_retention() {
        let mut seq = Sequence::new();

        let mut store = MockStore::new();
        store
            .expect_return_retention()
            .once()
            .in_sequence(&mut seq)
            .returning(|| false);
        store
            .expect_reset_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Ok(()));

        let connection = mock_connection_with_store(&[], ConnStatus::Offline, store);

        connection
            .init_store(NonZero::new(1).unwrap())
            .await
            .unwrap();
    }
}
