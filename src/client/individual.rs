// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

//! Handles the sending of individual datastream.

use astarte_interfaces::interface::Retention;
use astarte_interfaces::MappingPath;
use tracing::{debug, trace, warn};

use crate::client::ValidatedIndividual;
use crate::state::{SharedState, Status};
use crate::store::StoreCapabilities;
use crate::transport::Connection;
use crate::{AstarteType, Error};

use super::{DeviceClient, Publish, RetentionId, StoreWrapper, StoredRetentionExt, Timestamp};

impl<C> DeviceClient<C>
where
    C: Connection,
{
    pub(crate) async fn send_datastream_individual(
        &mut self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: AstarteType,
        timestamp: Option<Timestamp>,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let interfaces = self.state.interfaces.read().await;
        let mapping = interfaces.get_individual(interface_name, path)?;

        let validated = ValidatedIndividual::validate(mapping, data, timestamp)?;

        debug!("sending individual type {}", validated.data.display_type());

        match self.state.status.connection() {
            Status::Connected => {
                trace!("publish individual while connection is online");
            }
            Status::Disconnected => {
                trace!("publish individual while connection is offline");

                return Self::offline_send_individual(
                    &self.state,
                    &self.store,
                    &mut self.sender,
                    validated,
                )
                .await;
            }
            Status::Closed => {
                return Err(Error::Disconnected);
            }
        }

        match mapping.mapping().retention() {
            Retention::Volatile { .. } => {
                Self::send_volatile_individual(&self.state, &mut self.sender, validated).await
            }
            Retention::Stored { .. } => {
                Self::send_stored_individual(&self.state, &self.store, &mut self.sender, validated)
                    .await
            }
            Retention::Discard => self.sender.send_individual(validated).await,
        }
    }

    async fn offline_send_individual(
        state: &SharedState,
        store: &StoreWrapper<C::Store>,
        sender: &mut C::Sender,
        data: ValidatedIndividual,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        match data.retention {
            Retention::Discard => {
                debug!("drop publish with retention discard since disconnected");
            }
            Retention::Volatile { .. } => {
                let id = state.retention_ctx.next();

                state.volatile_store.push(id, data).await;
            }
            Retention::Stored { .. } => {
                let id = state.retention_ctx.next();
                if let Some(retention) = store.get_retention() {
                    let value = sender.serialize_individual(&data)?;

                    retention
                        .store_publish_individual(&id, &data, &value)
                        .await?;
                } else {
                    warn!("storing interface with retention stored in volatile since the store doesn't support retention");

                    state.volatile_store.push(id, data).await;
                }
            }
        }

        Ok(())
    }

    async fn send_volatile_individual(
        state: &SharedState,
        sender: &mut C::Sender,
        data: ValidatedIndividual,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let id = state.retention_ctx.next();

        state.volatile_store.push(id, data.clone()).await;

        sender
            .send_individual_stored(RetentionId::Volatile(id), data)
            .await
    }

    async fn send_stored_individual(
        state: &SharedState,
        store: &StoreWrapper<C::Store>,
        sender: &mut C::Sender,
        data: ValidatedIndividual,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let id = state.retention_ctx.next();

        match store.get_retention() {
            Some(retention) => {
                let value = sender.serialize_individual(&data)?;

                retention
                    .store_publish_individual(&id, &data, &value)
                    .await?;

                sender
                    .send_individual_stored(RetentionId::Stored(id), data)
                    .await?;
            }
            None => {
                warn!("storing interface with retention stored in volatile since the store doesn't support retention");

                state.volatile_store.push(id, data.clone()).await;

                sender
                    .send_individual_stored(RetentionId::Volatile(id), data)
                    .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use astarte_interfaces::schema::Reliability;
    use chrono::Utc;
    use mockall::{predicate, Sequence};
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::*;

    use crate::client::tests::{mock_client, mock_client_with_store};
    use crate::retention::memory::ItemValue;
    use crate::retention::{PublishInfo, StoredRetention};
    use crate::store::SqliteStore;
    use crate::test::{
        E2E_DEVICE_DATASTREAM, E2E_DEVICE_DATASTREAM_NAME, STORED_DEVICE_DATASTREAM,
        STORED_DEVICE_DATASTREAM_NAME, VOLATILE_DEVICE_DATASTREAM, VOLATILE_DEVICE_DATASTREAM_NAME,
    };
    use crate::Client;

    #[tokio::test]
    async fn send_datastream_individual_connected_discard() {
        let (mut client, _tx) = mock_client(&[E2E_DEVICE_DATASTREAM]);

        client.state.status.set_connected(true);

        let path = "/integer_endpoint";
        let value = 42;
        let timestamp = Utc::now();

        let mut seq = Sequence::new();

        client
            .sender
            .expect_send_individual()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(ValidatedIndividual {
                interface: E2E_DEVICE_DATASTREAM_NAME.to_string(),
                path: path.to_string(),
                version_major: 0,
                reliability: Reliability::Unreliable,
                retention: Retention::Discard,
                data: AstarteType::Integer(value),
                timestamp: Some(timestamp),
            }))
            .returning(|_| Ok(()));

        client
            .send_individual_with_timestamp(
                E2E_DEVICE_DATASTREAM_NAME,
                path,
                value.into(),
                timestamp,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn send_datastream_individual_connected_volatile() {
        let (mut client, _tx) = mock_client(&[VOLATILE_DEVICE_DATASTREAM]);

        client.state.status.set_connected(true);

        let path = "/endpoint1";
        let value = 42i64;

        let mut seq = Sequence::new();

        let expected = ValidatedIndividual {
            interface: VOLATILE_DEVICE_DATASTREAM_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::LongInteger(value),
            timestamp: None,
        };
        client
            .sender
            .expect_send_individual_stored()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::function(|r| matches!(r, RetentionId::Volatile(_))),
                predicate::eq(expected.clone()),
            )
            .returning(|_, _| Ok(()));

        client
            .send_individual(VOLATILE_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        let item = client.state.volatile_store.pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Individual(expected));
    }

    #[tokio::test]
    async fn send_datastream_individual_connected_stored_no_retention_cap() {
        let (mut client, _tx) = mock_client(&[STORED_DEVICE_DATASTREAM]);

        client.state.status.set_connected(true);

        let path = "/endpoint2";
        let value = true;

        let mut seq = Sequence::new();

        let expected = ValidatedIndividual {
            interface: STORED_DEVICE_DATASTREAM_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            data: AstarteType::Boolean(value),
            timestamp: None,
        };
        client
            .sender
            .expect_send_individual_stored()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::function(|r| matches!(r, RetentionId::Volatile(_))),
                predicate::eq(expected.clone()),
            )
            .returning(|_, _| Ok(()));

        client
            .send_individual(STORED_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        let item = client.state.volatile_store.pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Individual(expected));
    }

    #[tokio::test]
    async fn send_datastream_individual_connected_stored_sqlite() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::connect(tmp.path()).await.unwrap();
        let (mut client, _tx) = mock_client_with_store(&[STORED_DEVICE_DATASTREAM], store);

        client.state.status.set_connected(true);

        let path = "/endpoint2";
        let value = true;
        let exp = ValidatedIndividual {
            interface: STORED_DEVICE_DATASTREAM_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            data: AstarteType::Boolean(value),
            timestamp: None,
        };
        const EXP_SER: &[u8] = &[1, 2, 3, 4];

        let mut seq = Sequence::new();

        client
            .sender
            .expect_serialize_individual()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(exp.clone()))
            .returning(|_| Ok(EXP_SER.to_vec()));

        client
            .sender
            .expect_send_individual_stored()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::function(|r| matches!(r, RetentionId::Stored(_))),
                predicate::eq(exp.clone()),
            )
            .returning(|_, _| Ok(()));

        client
            .send_individual(STORED_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        let mut stored = Vec::new();
        let read = client
            .store
            .store
            .unsent_publishes(2, &mut stored)
            .await
            .unwrap();
        assert_eq!(read, 1);
        assert_eq!(stored.len(), 1);
        assert_eq!(
            stored.pop().unwrap().1,
            PublishInfo {
                interface: STORED_DEVICE_DATASTREAM_NAME.into(),
                path: path.into(),
                version_major: 0,
                reliability: Reliability::Unique,
                expiry: Some(Duration::from_secs(30)),
                sent: false,
                value: EXP_SER.into()
            }
        );
    }

    #[tokio::test]
    async fn send_datastream_individual_offline_discard() {
        let (mut client, _tx) = mock_client(&[E2E_DEVICE_DATASTREAM]);

        client.state.status.set_connected(false);

        let path = "/integer_endpoint";
        let value = 42;
        let timestamp = Utc::now();

        // No expects on sender since discard
        client
            .send_individual_with_timestamp(
                E2E_DEVICE_DATASTREAM_NAME,
                path,
                value.into(),
                timestamp,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn send_datastream_individual_offline_volatile() {
        let (mut client, _tx) = mock_client(&[VOLATILE_DEVICE_DATASTREAM]);

        client.state.status.set_connected(false);

        let path = "/endpoint1";
        let value = 42i64;

        let expected = ValidatedIndividual {
            interface: VOLATILE_DEVICE_DATASTREAM_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Volatile { expiry: None },
            data: AstarteType::LongInteger(value),
            timestamp: None,
        };

        client
            .send_individual(VOLATILE_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        let item = client.state.volatile_store.pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Individual(expected));
    }

    #[tokio::test]
    async fn send_datastream_individual_offline_stored_no_retention_cap() {
        let (mut client, _tx) = mock_client(&[STORED_DEVICE_DATASTREAM]);

        client.state.status.set_connected(false);

        let path = "/endpoint2";
        let value = true;

        let expected = ValidatedIndividual {
            interface: STORED_DEVICE_DATASTREAM_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            data: AstarteType::Boolean(value),
            timestamp: None,
        };

        // Send
        client
            .send_individual(STORED_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        let item = client.state.volatile_store.pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Individual(expected));
    }

    #[tokio::test]
    async fn send_datastream_individual_offline_stored_sqlite() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::connect(tmp.path()).await.unwrap();
        let (mut client, _tx) = mock_client_with_store(&[STORED_DEVICE_DATASTREAM], store);

        client.state.status.set_connected(false);

        let path = "/endpoint2";
        let value = true;
        let exp = ValidatedIndividual {
            interface: STORED_DEVICE_DATASTREAM_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Unique,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            data: AstarteType::Boolean(value),
            timestamp: None,
        };
        const EXP_SER: &[u8] = &[1, 2, 3, 4];

        let mut seq = Sequence::new();

        client
            .sender
            .expect_serialize_individual()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(exp.clone()))
            .returning(|_| Ok(EXP_SER.to_vec()));

        // Send
        client
            .send_individual(STORED_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        let mut stored = Vec::new();
        let read = client
            .store
            .store
            .unsent_publishes(2, &mut stored)
            .await
            .unwrap();
        assert_eq!(read, 1);
        assert_eq!(stored.len(), 1);
        assert_eq!(
            stored.pop().unwrap().1,
            PublishInfo {
                interface: STORED_DEVICE_DATASTREAM_NAME.into(),
                path: path.into(),
                version_major: 0,
                reliability: Reliability::Unique,
                expiry: Some(Duration::from_secs(30)),
                sent: false,
                value: EXP_SER.into()
            }
        );
    }
}
