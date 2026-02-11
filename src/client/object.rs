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

//! Handles the sending of object datastream.

use astarte_interfaces::MappingPath;
use tracing::info;

use crate::Error;
use crate::client::ValidatedObject;
use crate::{aggregate::AstarteObject, transport::Connection};

use super::{DeviceClient, Publish};

impl<C> DeviceClient<C>
where
    C: Connection,
{
    pub(crate) async fn send_datastream_object(
        &mut self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: AstarteObject,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let interfaces = self.state.interfaces().read().await;
        let interface = interfaces.get_object(interface_name, path)?;

        let validated = ValidatedObject::validate(interface, path, data, timestamp)?;

        info!(interface = interface_name, path = %path, "sending object",);

        Self::send(&self.state, &self.store, &mut self.sender, validated).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use astarte_interfaces::interface::Retention;
    use astarte_interfaces::schema::Reliability;
    use chrono::Utc;
    use mockall::{Sequence, predicate};
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::*;

    use crate::client::tests::{mock_client, mock_client_with_store};
    use crate::interfaces::tests::DEVICE_OBJECT;
    use crate::retention::memory::ItemValue;
    use crate::retention::{PublishInfo, RetentionId, StoredRetention};
    use crate::state::ConnStatus;
    use crate::store::SqliteStore;
    use crate::test::{
        E2E_DEVICE_DATASTREAM, E2E_DEVICE_DATASTREAM_NAME, STORED_DEVICE_OBJECT,
        STORED_DEVICE_OBJECT_NAME, VOLATILE_DEVICE_OBJECT, VOLATILE_DEVICE_OBJECT_NAME,
    };
    use crate::{AstarteData, Client};

    #[tokio::test]
    async fn send_datastream_object_connected_discard() {
        let mut client = mock_client(&[DEVICE_OBJECT], ConnStatus::Connected);

        let interface = "test.device.object";
        let path = "/sensor_1";
        let timestamp = Utc::now();

        let obj = AstarteObject::from_iter(
            [
                ("double_endpoint", AstarteData::try_from(42.0).unwrap()),
                ("integer_endpoint", AstarteData::Integer(42)),
                ("boolean_endpoint", AstarteData::Boolean(false)),
                (
                    "booleanarray_endpoint",
                    AstarteData::BooleanArray(vec![true, false]),
                ),
            ]
            .map(|(n, v)| (n.to_string(), v)),
        );

        let mut seq = Sequence::new();
        client
            .sender
            .expect_send_object()
            .with(predicate::eq(ValidatedObject {
                interface: interface.to_string(),
                path: path.to_string(),
                version_major: 0,
                reliability: Reliability::Unreliable,
                retention: Retention::Discard,
                data: obj.clone(),
                timestamp: Some(timestamp),
            }))
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));

        // Test the sent
        client
            .send_object_with_timestamp(interface, path, obj, timestamp)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn send_datastream_object_connected_volatile() {
        let mut client = mock_client(&[VOLATILE_DEVICE_OBJECT], ConnStatus::Connected);

        let path = "/endpoint";
        let value = AstarteObject::from_iter(
            [
                ("longinteger", AstarteData::LongInteger(42)),
                ("boolean", AstarteData::Boolean(true)),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );

        let mut seq = Sequence::new();

        let expected = ValidatedObject {
            interface: VOLATILE_DEVICE_OBJECT_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_secs(30)),
            },
            data: value.clone(),
            timestamp: None,
        };
        client
            .sender
            .expect_send_object_stored()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::function(|r| matches!(r, RetentionId::Volatile(_))),
                predicate::eq(expected.clone()),
            )
            .returning(|_, _| Ok(()));

        client
            .send_object(VOLATILE_DEVICE_OBJECT_NAME, path, value)
            .await
            .unwrap();

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Object(expected));
    }

    #[tokio::test]
    async fn send_datastream_object_connected_stored_no_retention_cap() {
        let mut client = mock_client(&[STORED_DEVICE_OBJECT], ConnStatus::Connected);

        let path = "/endpoint";
        let value = AstarteObject::from_iter(
            [
                ("longinteger", AstarteData::LongInteger(42)),
                ("boolean", AstarteData::Boolean(true)),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );

        let mut seq = Sequence::new();

        let expected = ValidatedObject {
            interface: STORED_DEVICE_OBJECT_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            data: value.clone(),
            timestamp: None,
        };
        client
            .sender
            .expect_send_object_stored()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::function(|r| matches!(r, RetentionId::Volatile(_))),
                predicate::eq(expected.clone()),
            )
            .returning(|_, _| Ok(()));

        client
            .send_object(STORED_DEVICE_OBJECT_NAME, path, value)
            .await
            .unwrap();

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Object(expected));
    }

    #[tokio::test]
    async fn send_datastream_object_connected_stored_sqlite() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::connect(tmp.path()).await.unwrap();
        let mut client =
            mock_client_with_store(&[STORED_DEVICE_OBJECT], ConnStatus::Connected, store);

        let path = "/endpoint";
        let value = AstarteObject::from_iter(
            [
                ("longinteger", AstarteData::LongInteger(42)),
                ("boolean", AstarteData::Boolean(true)),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );
        let exp = ValidatedObject {
            interface: STORED_DEVICE_OBJECT_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            data: value.clone(),
            timestamp: None,
        };
        const EXP_SER: &[u8] = &[1, 2, 3, 4];

        let mut seq = Sequence::new();

        client
            .sender
            .expect_serialize_object()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(exp.clone()))
            .returning(|_| Ok(EXP_SER.to_vec()));

        client
            .sender
            .expect_send_object_stored()
            .once()
            .in_sequence(&mut seq)
            .with(
                predicate::function(|r| matches!(r, RetentionId::Stored(_))),
                predicate::eq(exp.clone()),
            )
            .returning(|_, _| Ok(()));

        client
            .send_object(STORED_DEVICE_OBJECT_NAME, path, value)
            .await
            .unwrap();

        let mut stored = Vec::new();
        let read = client
            .store
            .store
            .unsent_publishes(2, &mut stored)
            .await
            .unwrap();
        assert_eq!(read, 0);
        assert_eq!(stored.len(), 0);
        stored.clear();

        // reset sent
        client.store.store.reset_all_publishes().await.unwrap();

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
                interface: STORED_DEVICE_OBJECT_NAME.into(),
                path: path.into(),
                version_major: 0,
                reliability: Reliability::Guaranteed,
                expiry: Some(Duration::from_secs(30)),
                sent: false,
                value: EXP_SER.into()
            }
        );
    }

    #[tokio::test]
    async fn send_datastream_object_offline_discard() {
        let mut client = mock_client(&[DEVICE_OBJECT], ConnStatus::Disconnected);

        let interface = "test.device.object";
        let path = "/sensor_1";
        let timestamp = Utc::now();

        let obj = AstarteObject::from_iter(
            [
                ("double_endpoint", AstarteData::try_from(42.0).unwrap()),
                ("integer_endpoint", AstarteData::Integer(42)),
                ("boolean_endpoint", AstarteData::Boolean(false)),
                (
                    "booleanarray_endpoint",
                    AstarteData::BooleanArray(vec![true, false]),
                ),
            ]
            .map(|(n, v)| (n.to_string(), v)),
        );

        // Test the sent
        client
            .send_object_with_timestamp(interface, path, obj, timestamp)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn send_datastream_object_offline_volatile() {
        let mut client = mock_client(&[VOLATILE_DEVICE_OBJECT], ConnStatus::Disconnected);

        let path = "/endpoint";
        let value = AstarteObject::from_iter(
            [
                ("longinteger", AstarteData::LongInteger(42)),
                ("boolean", AstarteData::Boolean(true)),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );

        let expected = ValidatedObject {
            interface: VOLATILE_DEVICE_OBJECT_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Volatile {
                expiry: Some(Duration::from_secs(30)),
            },
            data: value.clone(),
            timestamp: None,
        };

        client
            .send_object(VOLATILE_DEVICE_OBJECT_NAME, path, value)
            .await
            .unwrap();

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Object(expected));
    }

    #[tokio::test]
    async fn send_datastream_object_offline_stored_no_retention_cap() {
        let mut client = mock_client(&[STORED_DEVICE_OBJECT], ConnStatus::Disconnected);

        let path = "/endpoint";
        let value = AstarteObject::from_iter(
            [
                ("longinteger", AstarteData::LongInteger(42)),
                ("boolean", AstarteData::Boolean(true)),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );

        let expected = ValidatedObject {
            interface: STORED_DEVICE_OBJECT_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            data: value.clone(),
            timestamp: None,
        };

        // Send
        client
            .send_object(STORED_DEVICE_OBJECT_NAME, path, value)
            .await
            .unwrap();

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Object(expected));
    }

    #[tokio::test]
    async fn send_datastream_object_offline_stored_sqlite() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::connect(tmp.path()).await.unwrap();
        let mut client =
            mock_client_with_store(&[STORED_DEVICE_OBJECT], ConnStatus::Disconnected, store);

        let path = "/endpoint";
        let value = AstarteObject::from_iter(
            [
                ("longinteger", AstarteData::LongInteger(42)),
                ("boolean", AstarteData::Boolean(true)),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );

        let exp = ValidatedObject {
            interface: STORED_DEVICE_OBJECT_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            data: value.clone(),
            timestamp: None,
        };
        const EXP_SER: &[u8] = &[1, 2, 3, 4];

        let mut seq = Sequence::new();

        client
            .sender
            .expect_serialize_object()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(exp.clone()))
            .returning(|_| Ok(EXP_SER.to_vec()));

        // Send
        client
            .send_object(STORED_DEVICE_OBJECT_NAME, path, value)
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
                interface: STORED_DEVICE_OBJECT_NAME.into(),
                path: path.into(),
                version_major: 0,
                reliability: Reliability::Guaranteed,
                expiry: Some(Duration::from_secs(30)),
                sent: false,
                value: EXP_SER.into()
            }
        );
    }

    #[tokio::test]
    async fn send_datastream_object_closed_stored_sqlite() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::connect(tmp.path()).await.unwrap();
        let mut client = mock_client_with_store(&[STORED_DEVICE_OBJECT], ConnStatus::Closed, store);

        let path = "/endpoint";
        let value = AstarteObject::from_iter(
            [
                ("longinteger", AstarteData::LongInteger(42)),
                ("boolean", AstarteData::Boolean(true)),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );

        let exp = ValidatedObject {
            interface: STORED_DEVICE_OBJECT_NAME.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Guaranteed,
            retention: Retention::Stored {
                expiry: Some(Duration::from_secs(30)),
            },
            data: value.clone(),
            timestamp: None,
        };
        const EXP_SER: &[u8] = &[1, 2, 3, 4];

        let mut seq = Sequence::new();
        client
            .sender
            .expect_serialize_object()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(exp))
            .returning(|_| Ok(EXP_SER.to_vec()));

        // Send
        let err = client
            .send_object(STORED_DEVICE_OBJECT_NAME, path, value)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Disconnected));

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
                interface: STORED_DEVICE_OBJECT_NAME.into(),
                path: path.into(),
                version_major: 0,
                reliability: Reliability::Guaranteed,
                expiry: Some(Duration::from_secs(30)),
                sent: false,
                value: EXP_SER.into()
            }
        );
    }

    #[tokio::test]
    async fn send_datastream_object_interface_not_found() {
        let mut client = mock_client(&[], ConnStatus::Connected);

        let interface = "test.device.object";
        let path = "/sensor_1";
        let timestamp = Utc::now();

        let obj = AstarteObject::from_iter(
            [
                ("double_endpoint", AstarteData::try_from(42.0).unwrap()),
                ("integer_endpoint", AstarteData::Integer(42)),
                ("boolean_endpoint", AstarteData::Boolean(false)),
                (
                    "booleanarray_endpoint",
                    AstarteData::BooleanArray(vec![true, false]),
                ),
            ]
            .map(|(n, v)| (n.to_string(), v)),
        );

        // Test the sent
        let err = client
            .send_object_with_timestamp(interface, path, obj, timestamp)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::InterfaceNotFound {
                name,
            } if name == interface
        ));
    }

    #[tokio::test]
    async fn send_datastream_object_wrong_aggregation() {
        let mut client = mock_client(&[E2E_DEVICE_DATASTREAM], ConnStatus::Connected);

        let path = "/sensor_1";
        let timestamp = Utc::now();

        let obj = AstarteObject::from_iter(
            [
                ("double_endpoint", AstarteData::try_from(42.0).unwrap()),
                ("integer_endpoint", AstarteData::Integer(42)),
                ("boolean_endpoint", AstarteData::Boolean(false)),
                (
                    "booleanarray_endpoint",
                    AstarteData::BooleanArray(vec![true, false]),
                ),
            ]
            .map(|(n, v)| (n.to_string(), v)),
        );

        // Test the sent
        let err = client
            .send_object_with_timestamp(E2E_DEVICE_DATASTREAM_NAME, path, obj, timestamp)
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Aggregation(..)));
    }
}
