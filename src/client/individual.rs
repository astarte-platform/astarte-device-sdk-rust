// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

//! Handles the sending of individual datastream.

use astarte_device_error::ResultExt;
use astarte_interfaces::MappingPath;
use tracing::{debug, instrument};

use crate::builder::ConnectionConfig;
use crate::error::{AstarteError, ErrorKind};
use crate::store::StoreCapabilities;
use crate::transport::Encode;
use crate::validate::individual::ValidatedIndividual;
use crate::{AstarteData, Timestamp};

use super::DeviceClient;

impl<C, S> DeviceClient<C, S>
where
    C: ConnectionConfig,
{
    #[instrument(skip_all, fields(interface = interface_name, path = %path, mapping = data.display_type()))]
    pub(crate) async fn send_datastream_individual(
        &self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: AstarteData,
        timestamp: Option<Timestamp>,
    ) -> Result<(), AstarteError>
    where
        C::Encoder: Encode,
        S: StoreCapabilities,
    {
        let interfaces = self.state.interfaces().read().await;
        let mapping = interfaces
            .get_individual(interface_name, path)
            .map_kind(ErrorKind::Interface)?;

        let validated = ValidatedIndividual::validate(mapping, data, timestamp)
            .map_kind(ErrorKind::Interface)?;

        debug!(
            mapping_type = validated.data.display_type(),
            "sending individual"
        );

        self.send(validated).await
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::time::Duration;

    use astarte_interfaces::interface::Retention;
    use astarte_interfaces::schema::Reliability;
    use chrono::Utc;
    use mockall::{Sequence, predicate};
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::*;

    use crate::Client;
    use crate::client::tests::{mock_client, mock_client_with_store};
    use crate::error::ErrorKind;
    use crate::retention::memory::ItemValue;
    use crate::retention::{PublishInfo, RetentionId, StoredRetention};
    use crate::state::ConnStatus;
    use crate::store::SqliteStore;
    use crate::test::{
        E2E_DEVICE_DATASTREAM, E2E_DEVICE_DATASTREAM_NAME, STORED_DEVICE_DATASTREAM,
        STORED_DEVICE_DATASTREAM_NAME, VOLATILE_DEVICE_DATASTREAM, VOLATILE_DEVICE_DATASTREAM_NAME,
    };
    use crate::validate::Validated;

    #[tokio::test]
    async fn send_datastream_individual_connected_discard() {
        let mut client = mock_client(&[E2E_DEVICE_DATASTREAM], ConnStatus::Online);

        let path = "/integer_endpoint";
        let value = 42;
        let timestamp = Utc::now();

        client
            .send_individual_with_timestamp(
                E2E_DEVICE_DATASTREAM_NAME,
                path,
                value.into(),
                timestamp,
            )
            .await
            .unwrap();

        let recv = client.client_rx.try_recv().unwrap();
        assert_eq!(
            recv,
            Validated::Individual {
                retention: None,
                data: ValidatedIndividual {
                    interface: E2E_DEVICE_DATASTREAM_NAME.to_string(),
                    path: path.to_string(),
                    version_major: 0,
                    reliability: Reliability::Unreliable,
                    retention: Retention::Discard,
                    data: AstarteData::Integer(value),
                    timestamp: Some(timestamp),
                }
            }
        );
    }

    #[tokio::test]
    async fn send_datastream_individual_connected_volatile() {
        let mut client = mock_client(&[VOLATILE_DEVICE_DATASTREAM], ConnStatus::Online);

        let path = "/endpoint1";
        let value = 42i64;

        let expected = ValidatedIndividual {
            interface: VOLATILE_DEVICE_DATASTREAM_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Volatile { expiry: None },
            data: AstarteData::LongInteger(value),
            timestamp: None,
        };

        client
            .send_individual(VOLATILE_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        let Validated::Individual { retention, data } = client.client_rx.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(data, expected);
        assert!(retention.is_some_and(|r| matches!(r, RetentionId::Volatile(..))));

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Individual(expected));
    }

    #[tokio::test]
    async fn send_datastream_individual_connected_stored_no_retention_cap() {
        let mut client = mock_client(&[STORED_DEVICE_DATASTREAM], ConnStatus::Online);

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
            data: AstarteData::Boolean(value),
            timestamp: None,
        };

        client
            .send_individual(STORED_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        let Validated::Individual { retention, data } = client.client_rx.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(data, expected);
        assert!(retention.is_some_and(|r| matches!(r, RetentionId::Volatile(..))));

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Individual(expected));
    }

    #[tokio::test]
    async fn send_datastream_individual_connected_stored_sqlite() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(tmp.path())
            .await
            .unwrap();

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
            data: AstarteData::Boolean(value),
            timestamp: None,
        };
        const EXP_SER: &[u8] = &[1, 2, 3, 4];

        let mut client =
            mock_client_with_store(&[STORED_DEVICE_DATASTREAM], ConnStatus::Online, store);
        let mut seq = Sequence::new();

        client
            .encoder
            .expect_serialize_individual()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(exp.clone()))
            .returning(|_| Ok(EXP_SER.to_vec()));

        client
            .send_individual(STORED_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        let Validated::Individual { retention, data } = client.client_rx.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(data, exp);
        assert!(retention.is_some_and(|r| matches!(r, RetentionId::Stored(..))));

        let mut stored = Vec::new();
        let read = client
            .state
            .store()
            .unsent_publishes(2, &mut stored)
            .await
            .unwrap();
        assert_eq!(read, 1);
        assert_eq!(stored.len(), 1);
        stored.clear();
    }

    #[tokio::test]
    async fn send_datastream_individual_offline_discard() {
        let client = mock_client(&[E2E_DEVICE_DATASTREAM], ConnStatus::Offline);

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

        assert!(client.client_rx.is_empty());
    }

    #[tokio::test]
    async fn send_datastream_individual_offline_volatile() {
        let client = mock_client(&[VOLATILE_DEVICE_DATASTREAM], ConnStatus::Offline);

        let path = "/endpoint1";
        let value = 42i64;

        let expected = ValidatedIndividual {
            interface: VOLATILE_DEVICE_DATASTREAM_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Volatile { expiry: None },
            data: AstarteData::LongInteger(value),
            timestamp: None,
        };

        client
            .send_individual(VOLATILE_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        assert!(client.client_rx.is_empty());

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Individual(expected));
    }

    #[tokio::test]
    async fn send_datastream_individual_offline_stored_no_retention_cap() {
        let client = mock_client(&[STORED_DEVICE_DATASTREAM], ConnStatus::Offline);

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
            data: AstarteData::Boolean(value),
            timestamp: None,
        };

        // Send
        client
            .send_individual(STORED_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap();

        assert!(client.client_rx.is_empty());

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Individual(expected));
    }

    #[tokio::test]
    async fn send_datastream_individual_offline_stored_sqlite() {
        let tmp = TempDir::new().unwrap();

        let store = SqliteStore::options()
            .with_writable_dir(tmp.path())
            .await
            .unwrap();
        let mut client =
            mock_client_with_store(&[STORED_DEVICE_DATASTREAM], ConnStatus::Offline, store);

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
            data: AstarteData::Boolean(value),
            timestamp: None,
        };
        const EXP_SER: &[u8] = &[1, 2, 3, 4];

        let mut seq = Sequence::new();

        client
            .encoder
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

        assert!(client.client_rx.is_empty());

        let mut stored = Vec::new();
        let read = client
            .state
            .store()
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
    async fn send_datastream_individual_closed_stored_sqlite() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(tmp.path())
            .await
            .unwrap();
        let mut client =
            mock_client_with_store(&[STORED_DEVICE_DATASTREAM], ConnStatus::Closed, store);

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
            data: AstarteData::Boolean(value),
            timestamp: None,
        };
        const EXP_SER: &[u8] = &[1, 2, 3, 4];

        let mut seq = Sequence::new();

        client
            .encoder
            .expect_serialize_individual()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(exp))
            .returning(|_| Ok(EXP_SER.to_vec()));

        // Send
        let err = client
            .send_individual(STORED_DEVICE_DATASTREAM_NAME, path, value.into())
            .await
            .unwrap_err();
        assert_eq!(*err.kind(), ErrorKind::Disconnected);

        assert!(client.client_rx.is_empty());

        let mut stored = Vec::new();
        let read = client
            .state
            .store()
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
