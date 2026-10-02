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

//! Handles the sending of object datastream.

use astarte_device_error::ResultExt;
use astarte_interfaces::MappingPath;
use tracing::{info, instrument};

use crate::aggregate::AstarteObject;
use crate::builder::ConnectionConfig;
use crate::error::{AstarteError, ErrorKind};
use crate::store::StoreCapabilities;
use crate::transport::Encode;
use crate::validate::object::ValidatedObject;

use super::DeviceClient;

impl<C, S> DeviceClient<C, S>
where
    C: ConnectionConfig,
{
    #[instrument(skip_all, fields(interface = interface_name, path = %path))]
    pub(crate) async fn send_datastream_object(
        &self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: AstarteObject,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
        C::Encoder: Encode,
    {
        let interfaces = self.state.interfaces().read().await;
        let interface = interfaces
            .get_object(interface_name, path)
            .map_kind(ErrorKind::Interface)?;

        let validated = ValidatedObject::validate(interface, path, data, timestamp)
            .map_kind(ErrorKind::Interface)?;

        info!(interface = interface_name, path = %path, "sending object",);

        self.send(validated).await
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
    use crate::error::InterfaceError;
    use crate::interfaces::tests::DEVICE_OBJECT;
    use crate::retention::memory::ItemValue;
    use crate::retention::{PublishInfo, RetentionId, StoredRetention};
    use crate::state::ConnStatus;
    use crate::store::SqliteStore;
    use crate::test::{
        E2E_DEVICE_DATASTREAM, E2E_DEVICE_DATASTREAM_NAME, STORED_DEVICE_OBJECT,
        STORED_DEVICE_OBJECT_NAME, VOLATILE_DEVICE_OBJECT, VOLATILE_DEVICE_OBJECT_NAME,
    };
    use crate::validate::Validated;
    use crate::{AstarteData, Client};

    #[tokio::test]
    async fn send_datastream_object_connected_discard() {
        let mut client = mock_client(&[DEVICE_OBJECT], ConnStatus::Online);

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
            .send_object_with_timestamp(interface, path, obj.clone(), timestamp)
            .await
            .unwrap();

        let exp = ValidatedObject {
            interface: interface.to_string(),
            path: path.to_string(),
            version_major: 0,
            reliability: Reliability::Unreliable,
            retention: Retention::Discard,
            data: obj,
            timestamp: Some(timestamp),
        };
        let res = client.client_rx.try_recv().unwrap();

        assert_eq!(
            res,
            Validated::Object {
                retention: None,
                data: exp
            }
        );
    }

    #[tokio::test]
    async fn send_datastream_object_connected_volatile() {
        let mut client = mock_client(&[VOLATILE_DEVICE_OBJECT], ConnStatus::Online);

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

        assert_eq!(item, ItemValue::Object(expected.clone()));

        let Validated::Object { retention, data } = client.client_rx.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(data, expected);
        assert!(matches!(retention, Some(RetentionId::Volatile(_))))
    }

    #[tokio::test]
    async fn send_datastream_object_connected_stored_no_retention_cap() {
        let mut client = mock_client(&[STORED_DEVICE_OBJECT], ConnStatus::Online);

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

        client
            .send_object(STORED_DEVICE_OBJECT_NAME, path, value)
            .await
            .unwrap();

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Object(expected.clone()));

        let Validated::Object { retention, data } = client.client_rx.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(data, expected);
        assert!(matches!(retention, Some(RetentionId::Volatile(_))))
    }

    #[tokio::test]
    async fn send_datastream_object_connected_stored_sqlite() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(tmp.path())
            .await
            .unwrap();
        let mut client = mock_client_with_store(&[STORED_DEVICE_OBJECT], ConnStatus::Online, store);

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
            .encoder
            .expect_serialize_object()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(exp.clone()))
            .returning(|_| Ok(EXP_SER.to_vec()));

        client
            .send_object(STORED_DEVICE_OBJECT_NAME, path, value)
            .await
            .unwrap();

        let Validated::Object { retention, data } = client.client_rx.try_recv().unwrap() else {
            panic!()
        };
        assert!(matches!(retention, Some(RetentionId::Stored(_))));
        assert_eq!(data, exp);

        let mut stored = Vec::new();
        let read = client
            .state
            .store()
            .unsent_publishes(2, &mut stored)
            .await
            .unwrap();
        assert_eq!(read, 1);
        assert_eq!(stored.len(), 1);
    }

    #[tokio::test]
    async fn send_datastream_object_offline_discard() {
        let client = mock_client(&[DEVICE_OBJECT], ConnStatus::Offline);

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
        assert!(client.client_rx.is_empty());
    }

    #[tokio::test]
    async fn send_datastream_object_offline_volatile() {
        let client = mock_client(&[VOLATILE_DEVICE_OBJECT], ConnStatus::Offline);

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
        assert!(client.client_rx.is_empty());

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Object(expected));
    }

    #[tokio::test]
    async fn send_datastream_object_offline_stored_no_retention_cap() {
        let client = mock_client(&[STORED_DEVICE_OBJECT], ConnStatus::Offline);

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
        assert!(client.client_rx.is_empty());

        let item = client.state.volatile_store().pop_next().await.unwrap();

        assert_eq!(item, ItemValue::Object(expected));
    }

    #[tokio::test]
    async fn send_datastream_object_offline_stored_sqlite() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::options()
            .with_writable_dir(tmp.path())
            .await
            .unwrap();
        let mut client =
            mock_client_with_store(&[STORED_DEVICE_OBJECT], ConnStatus::Offline, store);

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
            .encoder
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
        let store = SqliteStore::options()
            .with_writable_dir(tmp.path())
            .await
            .unwrap();
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
            .encoder
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
        assert_eq!(*err.kind(), ErrorKind::Disconnected);

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
        let client = mock_client(&[], ConnStatus::Online);

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

        assert_eq!(
            *err.kind(),
            ErrorKind::Interface(InterfaceError::InterfaceNotFound)
        );
    }

    #[tokio::test]
    async fn send_datastream_object_wrong_aggregation() {
        let client = mock_client(&[E2E_DEVICE_DATASTREAM], ConnStatus::Online);

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

        assert_eq!(
            *err.kind(),
            ErrorKind::Interface(InterfaceError::Aggregation)
        );
    }
}
