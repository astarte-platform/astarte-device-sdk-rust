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

use std::ops::ControlFlow;

use astarte_device_error::ResultExt;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::error::{AstarteError, ErrorKind};
use crate::interfaces::Interfaces;
use crate::retention::{RetentionId, StoredRetention};
use crate::retry::{RetryAction, RetryFuture};
use crate::state::{ConnStatus, ConnectionState};
use crate::store::StoreCapabilities;
use crate::transport::{Introspection, Sender};
use crate::validate::Validated;
use crate::validate::individual::ValidatedIndividual;
use crate::validate::object::ValidatedObject;

use self::resend::ResendCtx;

pub(crate) mod resend;

pub(crate) struct SenderTask<C, S> {
    pub(crate) client_rx: tokio::sync::mpsc::Receiver<Validated>,
    pub(crate) status_rx: tokio::sync::watch::Receiver<ConnStatus>,
    pub(crate) sender: C,
    pub(crate) state: ConnectionState<S>,
}

impl<C, S> SenderTask<C, S>
where
    C: Sender + Introspection,
{
    #[instrument(skip_all)]
    async fn recv(&mut self) -> Option<Validated> {
        tokio::select! {
            event = self.client_rx.recv() => {
                event
            }
            res = self.status_rx.wait_for(|status| *status != ConnStatus::Online) => {
                match res {
                    Ok(status) => {
                        trace!(status = %*status, "no longer connected");

                        None
                    },
                    Err(error) => {
                        trace!(%error, "channel closed");

                        None
                    },
                }
            }
        }
    }

    async fn wait_for_connection(&mut self) -> Option<bool> {
        let res = self
            .status_rx
            .wait_for(|status| {
                matches!(
                    status,
                    ConnStatus::Connected { .. } | ConnStatus::Disconnect | ConnStatus::Closed
                )
            })
            .await;

        match res.as_deref() {
            Ok(ConnStatus::Connected { session_present }) => Some(*session_present),
            Ok(ConnStatus::Disconnect | ConnStatus::Closed) | Err(_) => None,
            // NOTE: matched in the wait_for
            Ok(ConnStatus::Offline | ConnStatus::Online) => unreachable!(),
        }
    }

    /// Called when there's a new session
    async fn reset_storage(&self) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        if let Some(retention) = self.state.store().get_retention() {
            retention
                .reset_all_publishes()
                .await
                .map_kind(ErrorKind::Retention)?;
        }

        self.state.volatile_store().reset_sent().await;

        self.state
            .store()
            .reset_session()
            .await
            .map_kind(ErrorKind::Store)?;

        Ok(())
    }

    #[instrument(skip_all)]
    pub(crate) async fn sender(&mut self) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        // Wait for the connection to establish
        while let Some(session_present) = self.wait_for_connection().await {
            debug!("sender connected, starting handshake");

            // if we are connected but the session is not present we have to cleanup the retention data
            if !session_present {
                // when the session is not present we reset the sent flags for stored messages
                info!("session not present, resetting session");

                self.reset_storage().await?;
            }

            let mut backoff = RetryFuture {
                rx: &mut self.status_rx,
                state: &self.state,
            };

            // Perform connection handshake, lock the interfaces for the whole handshake
            {
                let interfaces = self.state.interfaces().read().await;

                let mut handshake = HandshakeAction {
                    sender: &mut self.sender,
                    state: &self.state,
                    interfaces: &interfaces,
                    session_present,
                };

                if backoff.retry(&mut handshake).await?.is_none() {
                    break;
                }

                // Resent device data
                let mut ctx = ResendCtx {
                    sender: &mut self.sender,
                    state: &self.state,
                    interfaces: &interfaces,
                };

                if backoff.retry(&mut ctx).await?.is_none() {
                    break;
                }
            }

            // Poll until the connection changes
            while let Some(recv) = self.recv().await {
                // TODO: check if this would deadlock on channel full
                self.handle_received(recv).await;
            }

            debug!("sender offline");
        }

        let timeout = self.state.config().send_timeout;
        let mut retry = RetryFuture {
            rx: &mut self.status_rx,
            state: &self.state,
        };

        let res = retry
            .or_closed(tokio::time::timeout(timeout, self.sender.disconnect()))
            .await;

        match res {
            Some(Ok(res)) => {
                res?;

                info!("disconnect sent to sever")
            }
            Some(Err(_)) => {
                warn!("disconnect timeout reached, exiting");
            }
            None => {
                trace!("closed")
            }
        }

        self.state.close_connection();

        info!("sender exiting");

        Ok(())
    }

    async fn handle_received(&mut self, recv: Validated)
    where
        S: StoreCapabilities,
    {
        trace!("sender received event");

        // Handle retention error
        match recv {
            Validated::Individual { retention, data } => {
                self.send_individual(retention, data).await;
            }
            Validated::Object { retention, data } => {
                self.send_object(retention, data).await;
            }
            Validated::Property { epoch, data } => {
                if let Err(error) = self.sender.send_property(&self.state, data, epoch).await {
                    error!(%error, "couldn't send property");
                }
            }
            Validated::Unset { epoch, data } => {
                if let Err(error) = self.sender.unset(&self.state, data, epoch).await {
                    error!(%error, "couldn't unset property");
                }
            }
            Validated::AddInterface(to_add) => {
                let interfaces = self.state.interfaces().read().await;

                if let Err(error) = self
                    .sender
                    .add_interface(&self.state, &interfaces, &to_add)
                    .await
                {
                    error!(%error, "couldn't add interface");
                }
            }
            Validated::ExtendInterfaces(to_add) => {
                let interfaces = self.state.interfaces().read().await;

                if let Err(error) = self
                    .sender
                    .extend_interfaces(&self.state, &interfaces, &to_add)
                    .await
                {
                    error!(%error, "couldn't extend interfaces");
                }
            }
            Validated::RemoveInterface(to_remove) => {
                let interfaces = self.state.interfaces().read().await;

                if let Err(error) = self
                    .sender
                    .remove_interface(&self.state, &interfaces, &to_remove)
                    .await
                {
                    error!(%error, "couldn't remove interface");
                }
            }
            Validated::RemoveInterfaceMany(to_remove) => {
                let interfaces = self.state.interfaces().read().await;

                if let Err(error) = self
                    .sender
                    .remove_interfaces(&self.state, &interfaces, &to_remove)
                    .await
                {
                    error!(%error, "couldn't remove interfaces");
                }
            }
        }
    }

    async fn send_individual(&mut self, retention: Option<RetentionId>, data: ValidatedIndividual)
    where
        S: StoreCapabilities,
    {
        match retention {
            Some(ret_id) => {
                if let Err(error) = self
                    .sender
                    .send_individual_stored(&self.state, ret_id, data)
                    .await
                {
                    error!(%error, "couldn't send individual datastream");
                }
            }
            None => {
                if let Err(error) = self.sender.send_individual(data).await {
                    error!(%error, "couldn't send individual datastream");
                }
            }
        }
    }

    async fn send_object(&mut self, retention: Option<RetentionId>, data: ValidatedObject)
    where
        S: StoreCapabilities,
    {
        match retention {
            Some(ret_id) => {
                if let Err(error) = self
                    .sender
                    .send_object_stored(&self.state, ret_id, data)
                    .await
                {
                    error!(%error, "couldn't send object datastream");
                }
            }
            None => {
                if let Err(error) = self.sender.send_object(data).await {
                    error!(%error, "couldn't send object datastream");
                }
            }
        }
    }
}

struct HandshakeAction<'a, C, S> {
    sender: &'a mut C,
    state: &'a ConnectionState<S>,
    interfaces: &'a Interfaces,
    session_present: bool,
}

impl<'a, C, S> RetryAction for HandshakeAction<'a, C, S>
where
    C: Sender,
    S: StoreCapabilities,
{
    type Out = ();

    type Err = AstarteError;

    async fn make(&mut self) -> Result<ControlFlow<Self::Out>, Self::Err> {
        self.sender
            .handshake(self.state, self.interfaces, self.session_present)
            .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use astarte_interfaces::Interface;
    use astarte_interfaces::interface::Retention;
    use astarte_interfaces::schema::Reliability;
    use chrono::Utc;
    use mockall::predicate;

    use crate::AstarteData;
    use crate::aggregate::AstarteObject;
    use crate::builder::DEFAULT_CHANNEL_SIZE;
    use crate::interfaces::Interfaces;
    use crate::interfaces::tests::{mock_validated_collection, mock_validated_interface};
    use crate::retention::RetentionId;
    use crate::state::tests::mock_state;
    use crate::store::memory::MemoryStore;
    use crate::test::{
        E2E_DEVICE_AGGREGATE, E2E_DEVICE_DATASTREAM, E2E_DEVICE_DATASTREAM_NAME,
        E2E_DEVICE_PROPERTY, E2E_DEVICE_PROPERTY_NAME,
    };
    use crate::transport::RemovedInterface;
    use crate::transport::mock::MockSender;
    use crate::validate::individual::ValidatedIndividual;
    use crate::validate::object::ValidatedObject;
    use crate::validate::properties::{ValidatedProperty, ValidatedUnset};

    use super::*;

    pub(crate) fn mock_sender_task<S>(
        store: S,
        interfaces: &[&str],
        initial_status: ConnStatus,
    ) -> (
        SenderTask<MockSender, S>,
        tokio::sync::mpsc::Sender<Validated>,
    ) {
        let (status_tx, status_rx) = tokio::sync::watch::channel(initial_status);
        let (client_tx, client_rx) = tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE.get());

        let interfaces =
            Interfaces::from_iter(interfaces.iter().map(|i| Interface::from_str(i).unwrap()));
        let state =
            ConnectionState::new(Arc::new(mock_state(store, status_tx.clone(), interfaces)));

        (
            SenderTask {
                client_rx,
                status_rx,
                sender: MockSender::new(),
                state,
            },
            client_tx,
        )
    }

    #[tokio::test]
    async fn test_handle_received_individual_no_retention() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let data = ValidatedIndividual {
            interface: E2E_DEVICE_DATASTREAM_NAME.to_string(),
            path: "/double_endpoint".to_string(),
            version_major: 0,
            reliability: Reliability::Unreliable,
            retention: Retention::Discard,
            timestamp: Some(Utc::now()),
            data: AstarteData::try_from(42.5).unwrap(),
        };

        task.sender
            .expect_send_individual_call()
            .once()
            .with(predicate::eq(data.clone()))
            .returning(|_| Ok(()));

        task.handle_received(Validated::Individual {
            retention: None,
            data,
        })
        .await;
    }

    #[tokio::test]
    async fn test_handle_received_individual_volatile_retention() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let data = ValidatedIndividual {
            interface: E2E_DEVICE_DATASTREAM_NAME.to_string(),
            path: "/double_endpoint".to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Volatile { expiry: None },
            timestamp: None,
            data: AstarteData::try_from(42.5).unwrap(),
        };

        let ret_id = RetentionId::Volatile(task.state.retention_ctx().next());

        task.sender
            .expect_send_individual_stored_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::eq(ret_id),
                predicate::eq(data.clone()),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::Individual {
            retention: Some(ret_id),
            data,
        })
        .await;
    }

    #[tokio::test]
    async fn test_handle_received_individual_stored_retention() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let data = ValidatedIndividual {
            interface: E2E_DEVICE_DATASTREAM_NAME.to_string(),
            path: "/double_endpoint".to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Stored { expiry: None },
            timestamp: None,
            data: AstarteData::try_from(42.5).unwrap(),
        };

        let ret_id = RetentionId::Stored(task.state.retention_ctx().next());

        task.sender
            .expect_send_individual_stored_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::eq(ret_id),
                predicate::eq(data.clone()),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::Individual {
            retention: Some(ret_id),
            data,
        })
        .await;
    }

    #[tokio::test]
    async fn test_handle_received_object_no_retention() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let data = ValidatedObject {
            interface: "org.astarte-platform.rust.e2etest.DeviceAggregate".to_string(),
            path: "/sensor_1".to_string(),
            version_major: 0,
            reliability: Reliability::Unreliable,
            retention: Retention::Discard,
            data: AstarteObject::from_iter([(
                "endpoint1".to_string(),
                AstarteData::try_from(1.0).unwrap(),
            )]),
            timestamp: Some(Utc::now()),
        };

        task.sender
            .expect_send_object_call()
            .once()
            .with(predicate::eq(data.clone()))
            .returning(|_| Ok(()));

        task.handle_received(Validated::Object {
            retention: None,
            data,
        })
        .await;
    }

    #[tokio::test]
    async fn test_handle_received_object_volatile_retention() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let data = ValidatedObject {
            interface: "org.astarte-platform.rust.e2etest.DeviceAggregate".to_string(),
            path: "/sensor_1".to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Volatile { expiry: None },
            data: AstarteObject::from_iter([(
                "endpoint1".to_string(),
                AstarteData::try_from(1.0).unwrap(),
            )]),
            timestamp: None,
        };

        let ret_id = RetentionId::Volatile(task.state.retention_ctx().next());

        task.sender
            .expect_send_object_stored_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::eq(ret_id),
                predicate::eq(data.clone()),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::Object {
            retention: Some(ret_id),
            data,
        })
        .await;
    }

    #[tokio::test]
    async fn test_handle_received_object_stored_retention() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let data = ValidatedObject {
            interface: "org.astarte-platform.rust.e2etest.DeviceAggregate".to_string(),
            path: "/sensor_1".to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Stored { expiry: None },
            data: AstarteObject::from_iter([(
                "endpoint1".to_string(),
                AstarteData::try_from(1.0).unwrap(),
            )]),
            timestamp: None,
        };

        let ret_id = RetentionId::Stored(task.state.retention_ctx().next());

        task.sender
            .expect_send_object_stored_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::eq(ret_id),
                predicate::eq(data.clone()),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::Object {
            retention: Some(ret_id),
            data,
        })
        .await;
    }

    #[tokio::test]
    async fn test_handle_received_property() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let data = ValidatedProperty {
            interface: E2E_DEVICE_PROPERTY_NAME.to_string(),
            path: "/sensor_1/enable".to_string(),
            version_major: 0,
            data: AstarteData::Boolean(true),
        };

        let epoch = 5;

        task.sender
            .expect_send_property_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::eq(data.clone()),
                predicate::eq(epoch),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::Property { epoch, data })
            .await;
    }

    #[tokio::test]
    async fn test_handle_received_unset() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let data = ValidatedUnset {
            interface: E2E_DEVICE_PROPERTY_NAME.to_string(),
            path: "/sensor_1/enable".to_string(),
        };

        let epoch = 3;

        task.sender
            .expect_unset_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::eq(data.clone()),
                predicate::eq(epoch),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::Unset { epoch, data }).await;
    }

    #[tokio::test]
    async fn test_handle_received_add_interface() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let interface = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();

        task.sender
            .expect_add_interface_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::always(),
                predicate::eq(interface.clone()),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::AddInterface(interface))
            .await;
    }

    #[tokio::test]
    async fn test_handle_received_extend_interfaces() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let interface = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();
        let collection = mock_validated_collection(&[mock_validated_interface(interface, false)]);

        task.sender
            .expect_extend_interfaces_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::always(),
                predicate::eq(collection.clone()),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::ExtendInterfaces(collection))
            .await;
    }

    #[tokio::test]
    async fn test_handle_received_remove_interface() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let interface = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();
        let removed = RemovedInterface::from(&interface);

        task.sender
            .expect_remove_interface_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::always(),
                predicate::eq(removed.clone()),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::RemoveInterface(removed))
            .await;
    }

    #[tokio::test]
    async fn test_handle_received_remove_interface_many() {
        let (mut task, _tx) = mock_sender_task(MemoryStore::new(), &[], ConnStatus::Online);

        let interface = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();
        let removed = vec![RemovedInterface::from(&interface)];

        task.sender
            .expect_remove_interfaces_call::<MemoryStore>()
            .once()
            .with(
                predicate::always(),
                predicate::always(),
                predicate::eq(removed.clone()),
            )
            .returning(|_, _, _| Ok(()));

        task.handle_received(Validated::RemoveInterfaceMany(removed))
            .await;
    }
}
