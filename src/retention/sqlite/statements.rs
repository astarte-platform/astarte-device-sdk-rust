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

use std::{borrow::Cow, collections::HashSet, time::Duration};

use rusqlite::{Connection, OptionalExtension, Transaction};
use tracing::{debug, trace, warn};

use crate::{
    retention::{Id, PublishInfo, StoredInterface},
    store::sqlite::{
        statements::{include_query, ReadConnection, WriteConnection},
        wrap_sync_call, SqliteError,
    },
};

use super::{RetentionMapping, RetentionPublish, TimestampSecs};

impl WriteConnection {
    pub(super) fn store(
        &mut self,
        mapping: &RetentionMapping<'_>,
        publish: &RetentionPublish<'_>,
    ) -> Result<(), SqliteError> {
        let exists = read_mapping(self, &mapping.interface, &mapping.path)?.is_some_and(|stored| {
            if stored != *mapping {
                warn!("mappings differ, replacing");

                false
            } else {
                trace!("mapping already exists");

                true
            }
        });
        let transaction = self.transaction().map_err(SqliteError::Transaction)?;

        wrap_sync_call(|| {
            if !exists {
                Self::store_mapping(&transaction, mapping)?;
                trace!("mapping stored");
            }

            Self::store_publish(&transaction, publish)?;
            trace!("publish stored");

            transaction.commit()?;

            Ok(())
        })
    }

    pub(super) fn store_mapping(
        transaction: &Transaction<'_>,
        mapping: &RetentionMapping<'_>,
    ) -> Result<(), SqliteError> {
        let mut statement = transaction
            .prepare_cached(include_query!("queries/retention/write/store_mapping.sql"))
            .map_err(SqliteError::Prepare)?;

        let expiry_sec = mapping.expiry_to_sql();

        statement
            .execute((
                &mapping.interface,
                &mapping.path,
                mapping.version_major,
                mapping.reliability,
                expiry_sec,
            ))
            .map_err(SqliteError::Query)?;

        Ok(())
    }

    pub(super) fn store_publish(
        transaction: &Transaction<'_>,
        publish: &RetentionPublish<'_>,
    ) -> Result<(), SqliteError> {
        let mut statement = transaction
            .prepare_cached(include_query!("queries/retention/write/store_publish.sql"))
            .map_err(SqliteError::Prepare)?;

        let be_bytes = publish.id.timestamp.to_bytes();
        let timestamp = be_bytes.as_slice();
        let counter = publish.id.counter;

        let expiry = publish.expiry_time.map(|e| e.to_bytes());
        let expiry = expiry.as_ref().map(|e| e.as_slice());

        statement
            .execute((
                timestamp,
                counter,
                &publish.interface,
                &publish.path,
                expiry,
                publish.sent,
                &publish.payload,
            ))
            .map_err(SqliteError::Query)?;

        Ok(())
    }

    pub(super) fn update_publish_sent_flag(&self, id: &Id, sent: bool) -> Result<(), SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/retention/write/update_sent.sql"))
                .map_err(SqliteError::Prepare)?;

            let timestamp = id.timestamp.to_bytes();
            let timestamp = timestamp.as_slice();

            let changed = statement.execute((sent, timestamp, id.counter))?;

            debug_assert_eq!(changed, 1);

            Ok(())
        })
    }

    pub(super) fn delete_publish_by_id(&self, id: &Id) -> Result<(), SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/retention/write/delete_publish.sql"))
                .map_err(SqliteError::Prepare)?;

            let timestamp = id.timestamp.to_bytes();
            let timestamp = timestamp.as_slice();

            let changed = statement.execute((timestamp, id.counter))?;

            debug_assert!(changed <= 1);

            if changed == 0 {
                debug!(?id, "no retention row deleted");
            }

            Ok(())
        })
    }

    pub(super) fn delete_interface(&mut self, interface: &str) -> Result<(), SqliteError> {
        let transaction = self.transaction().map_err(SqliteError::Transaction)?;

        wrap_sync_call(move || {
            Self::delete_interface_transaction(&transaction, interface)?;

            transaction.commit().map_err(SqliteError::Transaction)?;

            Ok(())
        })
    }

    pub(super) fn delete_interface_many<I, S>(&mut self, interfaces: I) -> Result<(), SqliteError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let transaction = self.transaction().map_err(SqliteError::Transaction)?;

        wrap_sync_call(move || {
            for intf in interfaces {
                Self::delete_interface_transaction(&transaction, intf.as_ref())?;
            }

            transaction.commit().map_err(SqliteError::Transaction)?;

            Ok(())
        })
    }

    fn delete_interface_transaction(
        transaction: &Transaction,
        interface: &str,
    ) -> Result<(), SqliteError> {
        // Delete publishes
        let mut statement = transaction
            .prepare_cached(include_query!(
                "queries/retention/write/delete_publish_by_interface.sql"
            ))
            .map_err(SqliteError::Prepare)?;

        statement.execute([interface]).map_err(SqliteError::Query)?;

        // Delete mappings
        let mut statement = transaction
            .prepare_cached(include_query!(
                "queries/retention/write/delete_mapping_by_interface.sql"
            ))
            .map_err(SqliteError::Prepare)?;

        statement.execute([interface]).map_err(SqliteError::Query)?;

        Ok(())
    }

    pub(super) fn delete_expired(&self, now: &TimestampSecs) -> Result<(), SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/retention/write/delete_expired.sql"))
                .map_err(SqliteError::Prepare)?;

            let timestamp = now.to_bytes();
            let timestamp = timestamp.as_slice();

            let deleted = statement.execute([timestamp])?;

            debug!(deleted, "deleted expired records");

            Ok(())
        })
    }

    pub(super) fn reset_all_sent(&self) -> Result<(), SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/retention/write/reset_all_sent.sql"))
                .map_err(SqliteError::Prepare)?;

            statement.execute([])?;

            Ok(())
        })
    }
}

impl ReadConnection {
    pub(super) fn all_interfaces(&self) -> Result<HashSet<StoredInterface>, SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!("queries/retention/read/all_interfaces.sql"))
                .map_err(SqliteError::Prepare)?;

            let interfaces = statement
                .query_map([], |row| {
                    Ok(StoredInterface {
                        name: row.get(0)?,
                        version_major: row.get(1)?,
                    })
                })
                .map_err(SqliteError::Query)?
                .collect::<Result<HashSet<StoredInterface>, rusqlite::Error>>()
                .map_err(SqliteError::Query)?;

            Ok(interfaces)
        })
    }

    pub(super) fn unset_publishes(
        &self,
        buf: &mut Vec<(Id, PublishInfo<'static>)>,
        now: &TimestampSecs,
        limit: usize,
    ) -> Result<usize, SqliteError> {
        wrap_sync_call(|| {
            let mut statement = self
                .prepare_cached(include_query!(
                    "queries/retention/read/unsent_publishes.sql"
                ))
                .map_err(SqliteError::Prepare)?;

            let now = now.to_bytes();
            let now = now.as_slice();

            // Cap to max
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);

            let (count, _) = statement
                .query_map((now, limit), |row| {
                    let id = Id {
                        timestamp: row.get(0)?,
                        counter: row.get(1)?,
                    };

                    Ok((
                        id,
                        PublishInfo {
                            interface: Cow::Owned(row.get(2)?),
                            path: Cow::Owned(row.get(3)?),
                            sent: row.get(4)?,
                            value: Cow::Owned(row.get(5)?),
                            reliability: row.get(6)?,
                            version_major: row.get(7)?,
                            expiry: expiry_from_sql(row.get(8)?),
                        },
                    ))
                })
                .map_err(SqliteError::Query)?
                .try_fold((0usize, buf), |(count, buf), res| {
                    let res = res?;

                    buf.push(res);

                    Ok((count.saturating_add(1), buf))
                })
                .map_err(SqliteError::Query)?;

            Ok(count)
        })
    }
}

fn read_mapping(
    connection: &Connection,
    interface: &str,
    path: &str,
) -> Result<Option<RetentionMapping<'static>>, SqliteError> {
    let mut statement = connection
        .prepare_cached(include_query!("queries/retention/read/mapping.sql"))
        .map_err(SqliteError::Prepare)?;

    statement
        .query_row([interface, path], |row| {
            let expiry: Option<i64> = row.get(4)?;
            let expiry = expiry.and_then(|exp| {
                // If the conversion fails, let's keep the packet forever.
                exp.try_into().ok().map(Duration::from_secs)
            });

            Ok(RetentionMapping {
                interface: Cow::Owned(row.get(0)?),
                path: Cow::Owned(row.get(1)?),
                version_major: row.get(2)?,
                reliability: row.get(3)?,
                expiry,
            })
        })
        .optional()
        .map_err(SqliteError::Query)
}

fn expiry_from_sql(expiry: Option<i64>) -> Option<Duration> {
    expiry.and_then(|exp| {
        // If the conversion fails, since the u64 was to big for the i64, we will keep the
        // packet forever.
        exp.try_into().ok().map(Duration::from_secs)
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use itertools::Itertools;

    use crate::{
        interface::Reliability,
        retention::{Context, StoredRetention, TimestampMillis},
        store::SqliteStore,
    };

    use super::*;

    impl ReadConnection {
        fn publish(&self, id: &Id) -> Result<Option<RetentionPublish<'static>>, SqliteError> {
            let timestamp = id.timestamp.to_bytes();
            let timestamp = timestamp.as_slice();

            let mut statement = self
                .prepare_cached(include_query!("queries/retention/read/publish.sql"))
                .map_err(SqliteError::Prepare)?;

            statement
                .query_row((timestamp, id.counter), |row| {
                    let id = Id {
                        timestamp: row.get(0)?,
                        counter: row.get(1)?,
                    };

                    Ok(RetentionPublish {
                        id,
                        interface: Cow::Owned(row.get(2)?),
                        path: Cow::Owned(row.get(3)?),
                        expiry_time: row.get(4)?,
                        sent: row.get(5)?,
                        payload: Cow::Owned(row.get(6)?),
                    })
                })
                .optional()
                .map_err(SqliteError::Query)
        }
    }

    async fn store_mapping(store: &SqliteStore, mapping: &RetentionMapping<'_>) {
        let mut writer = store.writer.lock().await;
        let t = writer.transaction().unwrap();
        WriteConnection::store_mapping(&t, mapping).unwrap();
        t.commit().unwrap();
    }

    async fn store_publish(store: &SqliteStore, publish: &RetentionPublish<'_>) {
        let mut writer = store.writer.lock().await;
        let t = writer.transaction().unwrap();
        WriteConnection::store_publish(&t, publish).unwrap();
        t.commit().unwrap();
    }

    pub(crate) fn fetch_publish(store: &SqliteStore, id: &Id) -> Option<RetentionPublish<'static>> {
        store
            .with_reader(|reader| reader.publish(id))
            .expect("failed to fetch publish")
    }

    pub(crate) fn fetch_mapping(
        store: &SqliteStore,
        interface: &str,
        path: &str,
    ) -> Option<RetentionMapping<'static>> {
        store
            .with_reader(|reader| read_mapping(reader, interface, path))
            .unwrap()
    }

    #[tokio::test]
    async fn should_store_and_check_mapping() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let mapping = RetentionMapping {
            interface: "com.Foo".into(),
            path: "/bar".into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        store_mapping(&store, &mapping).await;

        let res = fetch_mapping(&store, &mapping.interface, &mapping.path).unwrap();

        assert_eq!(res, mapping);
    }

    #[tokio::test]
    async fn should_replace_mapping() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let mut mapping = RetentionMapping {
            interface: "com.Foo".into(),
            path: "/bar".into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        store_mapping(&store, &mapping).await;

        let res = store
            .with_reader(|reader| read_mapping(reader, &mapping.interface, &mapping.path))
            .unwrap()
            .unwrap();

        assert_eq!(res, mapping);

        mapping.version_major = 2;

        store_mapping(&store, &mapping).await;

        let res = store
            .with_reader(|reader| read_mapping(reader, &mapping.interface, &mapping.path))
            .unwrap()
            .unwrap();

        assert_eq!(res, mapping);
    }

    #[tokio::test]
    async fn expiry_too_big() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let mut mapping = RetentionMapping {
            interface: "com.Foo".into(),
            path: "/bar".into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: Some(Duration::from_secs(u64::MAX)),
        };

        store_mapping(&store, &mapping).await;

        let res = store
            .with_reader(|reader| read_mapping(reader, &mapping.interface, &mapping.path))
            .unwrap()
            .unwrap();

        mapping.expiry = None;

        assert_eq!(res, mapping);
    }

    #[tokio::test]
    async fn should_store_and_check_publish() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: Some(Duration::from_secs(2)),
        };
        store_mapping(&store, &mapping).await;

        let mut expiry_time = TimestampSecs::now();
        expiry_time.0 += 100;

        let publish = RetentionPublish {
            id: Id {
                timestamp: TimestampMillis(1),
                counter: 2,
            },
            interface: interface.into(),
            path: path.into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry_time: Some(expiry_time),
        };

        store_publish(&store, &publish).await;

        let res = fetch_publish(&store, &publish.id).unwrap();

        assert_eq!(res, publish);
    }

    #[tokio::test]
    async fn should_store_and_replace_mapping() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mut mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };
        store_mapping(&store, &mapping).await;

        let res = store
            .with_reader(|reader| read_mapping(reader, &mapping.interface, &mapping.path))
            .unwrap()
            .unwrap();

        assert_eq!(res, mapping);

        mapping.version_major = 42;

        let publish = RetentionPublish {
            id: Id {
                timestamp: TimestampMillis(1),
                counter: 2,
            },
            interface: interface.into(),
            path: path.into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry_time: None,
        };

        store.writer.lock().await.store(&mapping, &publish).unwrap();

        let res = fetch_publish(&store, &publish.id).unwrap();

        assert_eq!(res, publish);

        let res = store
            .with_reader(|reader| read_mapping(reader, &mapping.interface, &mapping.path))
            .unwrap()
            .unwrap();

        assert_eq!(res, mapping);
    }

    #[tokio::test]
    async fn should_update_sent_flag() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        store_mapping(&store, &mapping).await;

        let mut publish = RetentionPublish {
            id: Id {
                timestamp: TimestampMillis(1),
                counter: 2,
            },
            interface: interface.into(),
            path: path.into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry_time: None,
        };
        store_publish(&store, &publish).await;

        store.update_sent_flag(&publish.id, true).await.unwrap();

        let res = fetch_publish(&store, &publish.id).unwrap();

        publish.sent = true;

        assert_eq!(res, publish);

        store.update_sent_flag(&publish.id, false).await.unwrap();

        let res = fetch_publish(&store, &publish.id).unwrap();

        publish.sent = false;

        assert_eq!(res, publish);
    }

    #[tokio::test]
    async fn should_remove_sent_packet() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };
        store_mapping(&store, &mapping).await;

        let id = Id {
            timestamp: TimestampMillis(1),
            counter: 1,
        };
        let exp = RetentionPublish {
            id,
            interface: interface.into(),
            path: path.into(),
            expiry_time: None,
            sent: false,
            payload: [].as_slice().into(),
        };

        store_publish(&store, &exp).await;

        store.mark_received(&id).await.unwrap();

        let publish = fetch_publish(&store, &exp.id);

        assert_eq!(publish, None);
    }

    #[tokio::test]
    async fn should_delete_expired_and_do_nothing_on_delete() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: Some(Duration::from_secs(100)),
        };

        store_mapping(&store, &mapping).await;

        let id = Id {
            timestamp: TimestampMillis::now(),
            counter: 1,
        };

        let info = PublishInfo {
            interface: mapping.interface,
            path: mapping.path,
            version_major: mapping.version_major,
            reliability: mapping.reliability,
            expiry: mapping.expiry,
            sent: true,
            value: [].as_slice().into(),
        };
        let exp = RetentionPublish::from_info(id, &info).unwrap();

        store_publish(&store, &exp).await;

        // delete expired by adding 101 seconds to be sure the mock record gets deleted
        store
            .writer
            .lock()
            .await
            .delete_expired(&TimestampSecs(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 101,
            ))
            .unwrap();

        // check that we can still delete the record
        store.writer.lock().await.delete_publish_by_id(&id).unwrap();
    }

    #[tokio::test]
    async fn should_delete_interface() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };
        store_mapping(&store, &mapping).await;

        let id = Id {
            timestamp: TimestampMillis(1),
            counter: 1,
        };
        let exp = RetentionPublish {
            id,
            interface: interface.into(),
            path: path.into(),
            expiry_time: None,
            sent: false,
            payload: [].as_slice().into(),
        };

        store_publish(&store, &exp).await;

        store.delete_interface(interface).await.unwrap();

        let publish = fetch_publish(&store, &id);

        assert_eq!(publish, None);

        let mapping = store
            .with_reader(|reader| read_mapping(reader, &mapping.interface, &mapping.path))
            .unwrap();

        assert_eq!(mapping, None);
    }

    #[tokio::test]
    async fn should_delete_interface_many() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };
        store_mapping(&store, &mapping).await;

        let id = Id {
            timestamp: TimestampMillis(1),
            counter: 1,
        };
        let exp = RetentionPublish {
            id,
            interface: interface.into(),
            path: path.into(),
            expiry_time: None,
            sent: false,
            payload: [].as_slice().into(),
        };

        store_publish(&store, &exp).await;

        store.delete_interface_many(&[interface]).await.unwrap();

        let publish = fetch_publish(&store, &id);

        assert_eq!(publish, None);

        let mapping = store
            .with_reader(|reader| read_mapping(reader, interface, path))
            .unwrap();

        assert_eq!(mapping, None);
    }

    #[tokio::test]
    async fn should_get_all_packets() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        store_mapping(&store, &mapping).await;

        let publishes = [
            RetentionPublish {
                id: Id {
                    timestamp: TimestampMillis(1),
                    counter: 2,
                },
                interface: interface.into(),
                path: path.into(),
                expiry_time: None,
                sent: false,
                payload: [].as_slice().into(),
            },
            RetentionPublish {
                id: Id {
                    timestamp: TimestampMillis(2),
                    counter: 0,
                },
                interface: interface.into(),
                path: path.into(),
                expiry_time: None,
                sent: false,
                payload: [].as_slice().into(),
            },
            RetentionPublish {
                id: Id {
                    timestamp: TimestampMillis(2),
                    counter: 1,
                },
                interface: interface.into(),
                path: path.into(),
                expiry_time: None,
                sent: false,
                payload: [].as_slice().into(),
            },
        ];

        for publish in &publishes {
            store_publish(&store, publish).await;
        }

        let expected = publishes
            .into_iter()
            .map(|p| {
                (
                    p.id,
                    PublishInfo {
                        interface: p.interface,
                        path: p.path,
                        reliability: mapping.reliability,
                        version_major: mapping.version_major,
                        value: p.payload,
                        sent: p.sent,
                        expiry: mapping.expiry,
                    },
                )
            })
            .collect_vec();

        let mut res = Vec::new();
        let count = store.unsent_publishes(100, &mut res).await.unwrap();

        assert_eq!(count, 3);

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn should_store_and_delete_publish() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };
        store_mapping(&store, &mapping).await;

        let publish = RetentionPublish {
            id: Id {
                timestamp: TimestampMillis(1),
                counter: 2,
            },
            interface: interface.into(),
            path: path.into(),
            expiry_time: None,
            sent: false,
            payload: [].as_slice().into(),
        };
        store_publish(&store, &publish).await;

        let res = fetch_publish(&store, &publish.id).unwrap();

        assert_eq!(res, publish);

        store.mark_received(&publish.id).await.unwrap();

        let res = fetch_publish(&store, &publish.id);

        assert!(res.is_none());
    }

    #[tokio::test]
    async fn should_resend_all_without_expired() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: Some(Duration::from_secs(3600)),
        };

        store_mapping(&store, &mapping).await;

        let ctx = Context::new();

        let info = PublishInfo::from_ref(
            interface,
            path,
            mapping.version_major,
            mapping.reliability,
            crate::interface::Retention::Stored {
                expiry: mapping.expiry,
            },
            false,
            &[],
        );

        // Only the first is still valid.
        let packets = [
            RetentionPublish::from_info(ctx.next(), &info).unwrap(),
            RetentionPublish::from_info(
                Id {
                    timestamp: TimestampMillis(2),
                    counter: 0,
                },
                &info,
            )
            .unwrap(),
            RetentionPublish::from_info(
                Id {
                    timestamp: TimestampMillis(2),
                    counter: 1,
                },
                &info,
            )
            .unwrap(),
        ];

        for publish in &packets {
            store_publish(&store, publish).await;
        }

        let expected = packets
            .into_iter()
            // Only the first
            .take(1)
            .map(|p| {
                (
                    p.id,
                    PublishInfo {
                        interface: p.interface,
                        path: p.path,
                        reliability: mapping.reliability,
                        version_major: mapping.version_major,
                        expiry: mapping.expiry,
                        sent: p.sent,
                        value: p.payload,
                    },
                )
            })
            .collect_vec();

        let mut res = Vec::new();
        let count = store.unsent_publishes(100, &mut res).await.unwrap();

        assert_eq!(count, 1);

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn should_get_unsent_publishes() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        store_mapping(&store, &mapping).await;

        let ctx = Context::new();

        let mut info = PublishInfo::from_ref(
            interface,
            path,
            mapping.version_major,
            mapping.reliability,
            crate::interface::Retention::Stored {
                expiry: mapping.expiry,
            },
            false,
            &[],
        );

        // Only the first is still valid.
        let info_cl = info.clone();
        let mut packets = vec![RetentionPublish::from_info(ctx.next(), &info_cl).unwrap()];

        info.sent = true;

        packets.push(
            RetentionPublish::from_info(
                Id {
                    timestamp: TimestampMillis(2),
                    counter: 0,
                },
                &info,
            )
            .unwrap(),
        );

        packets.push(
            RetentionPublish::from_info(
                Id {
                    timestamp: TimestampMillis(2),
                    counter: 1,
                },
                &info,
            )
            .unwrap(),
        );

        for publish in &packets {
            store_publish(&store, publish).await;
        }

        let expected = packets
            .into_iter()
            // Only the first
            .take(1)
            .map(|p| {
                (
                    p.id,
                    PublishInfo {
                        interface: p.interface,
                        path: p.path,
                        reliability: mapping.reliability,
                        version_major: mapping.version_major,
                        expiry: mapping.expiry,
                        sent: p.sent,
                        value: p.payload,
                    },
                )
            })
            .collect_vec();

        let mut res = Vec::new();
        let count = store.unsent_publishes(100, &mut res).await.unwrap();

        assert_eq!(count, 1);

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn should_get_interfaces() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let mapping1 = RetentionMapping {
            interface: "com.Foo".into(),
            path: "/path".into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        let mapping2 = RetentionMapping {
            interface: "com.Bar".into(),
            path: "path".into(),
            version_major: 2,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        store_mapping(&store, &mapping1).await;
        store_mapping(&store, &mapping2).await;

        let res = store.with_reader(|reader| reader.all_interfaces()).unwrap();

        let mut expected = HashSet::new();
        expected.insert(StoredInterface {
            name: "com.Foo".to_string(),
            version_major: 1,
        });
        expected.insert(StoredInterface {
            name: "com.Bar".to_string(),
            version_major: 2,
        });

        assert_eq!(expected, res);
    }
}
