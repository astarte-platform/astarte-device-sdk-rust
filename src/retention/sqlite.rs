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

//! Retention implemented using an SQLite database.

use std::{array::TryFromSliceError, borrow::Cow, time::Duration};

use async_trait::async_trait;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use sqlx::{query_file, Sqlite, Transaction};
use tracing::error;

use crate::{interface::Reliability, store::SqliteStore};

use super::{Id, PublishInfo, RetentionError, StoredRetention, TimestampMillis};

/// Error returned by the retention.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqliteRetentionError {
    /// Couldn't get all the packets.
    #[error("couldn't get all the packets")]
    AllPackets(#[source] sqlx::Error),
    /// Couldn't convert timestamp bytes
    #[error("couldn't convert timestamp bytes")]
    Timestamp(#[source] TryFromSliceError),
    /// Reliability must not be at most once (0)
    #[error("invalid reliability ({0})")]
    Reliability(u8),
    /// Couldn't store the mapping
    #[error("couldn't store mapping {path}")]
    StoreMapping {
        /// The source of the error
        #[source]
        backtrace: sqlx::Error,
        /// Topic of the mapping
        path: String,
    },
    /// Couldn't store the publish
    #[error("couldn't store publish for {path}")]
    StorePublish {
        /// The source of the error
        #[source]
        backtrace: sqlx::Error,
        /// Topic of the mapping
        path: String,
    },
    /// Couldn't mark publish as received.
    #[error("coudln't mark publish with id {id} as received")]
    Delete {
        /// The source of the error
        #[source]
        backtrace: sqlx::Error,
        /// Id of the publish
        id: Id,
    },
    /// Wrong number of row deleted.
    #[error("error while deleting received publish {id}, rows affected {rows}")]
    DeletedRows {
        /// Id of the packet
        id: Id,
        /// Rows modified
        rows: u64,
    },
    /// Couldn't acquire transaction
    #[error("couldn't {ctx} transaction")]
    Transaction {
        ctx: &'static str,
        #[source]
        backtrace: sqlx::Error,
    },
    /// Couldn't split the interface in name and path
    #[error("couldn't split interface and path for {id} in {path}")]
    Path { path: String, id: Id },
    /// Couldn't delete expired publishes
    #[error("couldn't delete expired publishes")]
    Expired(#[source] sqlx::Error),
    /// Couldn't fetch mapping
    #[error("couldn't fetch mapping {path}")]
    #[cfg(test)]
    Mapping {
        /// The source of the error
        #[source]
        backtrace: sqlx::Error,
        /// Path of the packet
        path: String,
    },
    /// Couldn't fetch publish
    #[error("couldn't fetch publish {id}")]
    #[cfg(test)]
    Publish {
        /// The source of the error
        #[source]
        backtrace: sqlx::Error,
        /// Id of the publish
        id: Id,
    },
}

impl SqliteRetentionError {
    const fn begin(backtrace: sqlx::Error) -> Self {
        Self::Transaction {
            ctx: "begin",
            backtrace,
        }
    }

    const fn commit(backtrace: sqlx::Error) -> Self {
        Self::Transaction {
            ctx: "commit",
            backtrace,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RetentionMapping<'a> {
    path: Cow<'a, str>,
    version_major: i32,
    reliability: Reliability,
    expiry: Option<Duration>,
}

impl<'a> From<&'a PublishInfo<'a>> for RetentionMapping<'a> {
    fn from(value: &'a PublishInfo<'a>) -> Self {
        Self {
            path: format!("{}{}", value.interface, value.path).into(),
            version_major: value.version_major,
            reliability: value.reliability,
            expiry: value.expiry,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RetentionPublish<'a> {
    id: Id,
    path: Cow<'a, str>,
    payload: Cow<'a, [u8]>,
    expiry: Option<TimestampMillis>,
}

impl<'a> RetentionPublish<'a> {
    fn from_info(id: Id, info: &'a PublishInfo<'a>) -> Self {
        let expiry = info.expiry.map(|exp| {
            let t = id.timestamp.saturating_add(exp.as_millis());
            TimestampMillis(t)
        });

        Self {
            id,
            path: format!("{}{}", info.interface, info.path).into(),
            payload: Cow::Borrowed(&info.value),
            expiry,
        }
    }
}

/// Gets the [`Reliability`] from a stored [`u8`].
fn reliability_from_row(qos: u8) -> Option<Reliability> {
    match qos {
        1 => Some(Reliability::Guaranteed),
        2 => Some(Reliability::Unique),
        _ => None,
    }
}

fn split_interface_and_path(
    id: &Id,
    mut full: String,
) -> Result<(String, String), SqliteRetentionError> {
    let Some(idx) = full.find('/') else {
        return Err(SqliteRetentionError::Path {
            path: full,
            id: *id,
        });
    };

    let path = full.split_off(idx);

    Ok((full, path))
}

// Implementation of utilities used in the retention
impl SqliteStore {
    fn fetch_publishes(
        &self,
    ) -> impl Stream<Item = Result<(Id, PublishInfo<'static>), SqliteRetentionError>> + '_ {
        query_file!("queries/retention/all_publishes.sql")
            .fetch(&self.db_conn)
            .map_err(SqliteRetentionError::AllPackets)
            .and_then(|row| async move {
                let id = Id::from_row(&row.t_millis, row.counter)
                    .map_err(SqliteRetentionError::Timestamp)?;

                let reliability = reliability_from_row(row.reliability)
                    .ok_or(SqliteRetentionError::Reliability(row.reliability))?;

                let (interface, path) = split_interface_and_path(&id, row.path)?;

                let expiry = row.expiry_sec.and_then(|exp| {
                    // If the conversion fails, let's keep the packet forever.
                    exp.try_into().ok().map(Duration::from_secs)
                });

                Ok((
                    id,
                    PublishInfo {
                        interface: interface.into(),
                        path: path.into(),
                        version_major: row.major_version,
                        reliability,
                        expiry,
                        value: row.payload.into(),
                    },
                ))
            })
    }

    async fn store_mapping(
        &self,
        mapping: &RetentionMapping<'_>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), SqliteRetentionError> {
        let retention: u8 = match mapping.reliability {
            Reliability::Unreliable => return Err(SqliteRetentionError::Reliability(0)),
            Reliability::Guaranteed => 1,
            Reliability::Unique => 2,
        };
        let exp: Option<i64> = mapping.expiry.and_then(|exp| {
            // If the conversion fails, since the u64 was to big for the i64, we will keep the
            // packet forever.
            exp.as_secs().try_into().ok()
        });

        query_file!(
            "queries/retention/store_mapping.sql",
            mapping.path,
            mapping.version_major,
            retention,
            exp
        )
        .execute(&mut **transaction)
        .await
        .map_err(|err| SqliteRetentionError::StoreMapping {
            backtrace: err,
            path: mapping.path.to_string(),
        })?;

        Ok(())
    }

    async fn store_publish(
        &self,
        publish: &RetentionPublish<'_>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), SqliteRetentionError> {
        let be_bytes = publish.id.timestamp.to_bytes();
        let timestamp = be_bytes.as_slice();
        let counter = publish.id.counter;

        let payload: &[u8] = &publish.payload;

        let expiry = publish.expiry.map(|e| e.to_bytes());
        let expiry = expiry.as_ref().map(|e| e.as_slice());

        query_file!(
            "queries/retention/store_publish.sql",
            timestamp,
            counter,
            publish.path,
            expiry,
            payload,
        )
        .execute(&mut **transaction)
        .await
        .map_err(|err| SqliteRetentionError::StorePublish {
            backtrace: err,
            path: publish.path.to_string(),
        })?;

        Ok(())
    }

    #[cfg(test)]
    async fn mapping(
        &self,
        path: &str,
    ) -> Result<Option<RetentionMapping<'static>>, SqliteRetentionError> {
        let Some(row) = query_file!("queries/retention/mapping.sql", path)
            .fetch_optional(&self.db_conn)
            .await
            .map_err(|err| SqliteRetentionError::Mapping {
                backtrace: err,
                path: path.to_string(),
            })?
        else {
            return Ok(None);
        };

        let reliability =
            reliability_from_row(row.qos).ok_or(SqliteRetentionError::Reliability(row.qos))?;

        let expiry = row.expiry_sec.and_then(|exp| {
            // If the conversion fails, let's keep the packet forever.
            exp.try_into().ok().map(Duration::from_secs)
        });

        Ok(Some(RetentionMapping {
            path: row.path.into(),
            version_major: row.major_version,
            reliability,
            expiry,
        }))
    }

    #[cfg(test)]
    async fn publish(
        &self,
        id: &Id,
    ) -> Result<Option<RetentionPublish<'static>>, SqliteRetentionError> {
        let timestamp = id.timestamp.to_bytes();
        let timestamp = timestamp.as_slice();

        let Some(row) = query_file!("queries/retention/publish.sql", timestamp, id.counter)
            .fetch_optional(&self.db_conn)
            .await
            .map_err(|err| SqliteRetentionError::Publish {
                backtrace: err,
                id: *id,
            })?
        else {
            return Ok(None);
        };

        let id =
            Id::from_row(&row.t_millis, row.counter).map_err(SqliteRetentionError::Timestamp)?;

        let expiry = row
            .expiry_t_millis
            .map(|v| {
                TimestampMillis::try_from(v.as_slice()).map_err(SqliteRetentionError::Timestamp)
            })
            .transpose()?;

        Ok(Some(RetentionPublish {
            id,
            path: row.path.into(),
            payload: row.payload.into(),
            expiry,
        }))
    }

    async fn delete_publish_by_id(&self, id: &Id) -> Result<(), SqliteRetentionError> {
        let timestamp = id.timestamp.to_bytes();
        let timestamp = timestamp.as_slice();

        let res = query_file!(
            "queries/retention/delete_publish.sql",
            timestamp,
            id.counter
        )
        .execute(&self.db_conn)
        .await
        .map_err(|err| SqliteRetentionError::Delete {
            backtrace: err,
            id: *id,
        })?;

        let rows = res.rows_affected();
        if rows != 1 {
            return Err(SqliteRetentionError::DeletedRows { id: *id, rows });
        }

        Ok(())
    }

    async fn store(
        &self,
        mapping: &RetentionMapping<'_>,
        publish: &RetentionPublish<'_>,
    ) -> Result<(), SqliteRetentionError> {
        let mut transaction = self
            .db_conn
            .begin()
            .await
            .map_err(SqliteRetentionError::begin)?;

        self.store_mapping(mapping, &mut transaction).await?;
        self.store_publish(publish, &mut transaction).await?;

        transaction
            .commit()
            .await
            .map_err(SqliteRetentionError::commit)
    }

    async fn delete_expired(&self) -> Result<(), SqliteRetentionError> {
        let now = TimestampMillis::now().to_bytes();
        let now = now.as_slice();

        query_file!("queries/retention/delete_expired.sql", now)
            .execute(&self.db_conn)
            .await
            .map_err(SqliteRetentionError::Expired)?;

        Ok(())
    }
}

#[async_trait]
impl StoredRetention for SqliteStore {
    /// Store a publish.
    async fn store_publish(&self, publish: PublishInfo<'_>) -> Result<Id, RetentionError> {
        let id = self.retention_ctx.next();

        let ret_map = RetentionMapping::from(&publish);
        let ret_pub = RetentionPublish::from_info(id, &publish);

        self.store(&ret_map, &ret_pub)
            .await
            .map_err(|err| RetentionError::store(&publish, err))?;

        Ok(id)
    }

    /// It will mark the stored publish as received.
    async fn mark_received(&self, id: &Id) -> Result<(), RetentionError> {
        self.delete_publish_by_id(id)
            .await
            .map_err(|err| RetentionError::received(*id, err))
    }

    /// Resend all the packets.
    async fn all_publishes(
        &self,
    ) -> Result<BoxStream<Result<(Id, PublishInfo<'static>), RetentionError>>, RetentionError> {
        self.delete_expired().await.map_err(RetentionError::all)?;

        Ok(self.fetch_publishes().map_err(RetentionError::all).boxed())
    }

    async fn delete_publish(&self, id: &Id) -> Result<(), RetentionError> {
        self.delete_publish_by_id(id)
            .await
            .map_err(|err| RetentionError::delete(*id, err))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::retention::Context;

    use super::*;

    #[tokio::test]
    async fn should_store_and_check_mapping() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let mapping = RetentionMapping {
            path: "realm/device_id/com.Foo/bar".into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let res = store.mapping(&mapping.path).await.unwrap().unwrap();

        assert_eq!(res, mapping);
    }

    #[tokio::test]
    async fn should_replace_mapping() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let mut mapping = RetentionMapping {
            path: "realm/device_id/com.Foo/bar".into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let res = store.mapping(&mapping.path).await.unwrap().unwrap();

        assert_eq!(res, mapping);

        mapping.version_major = 2;

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let res = store.mapping(&mapping.path).await.unwrap().unwrap();

        assert_eq!(res, mapping);
    }

    #[tokio::test]
    async fn expiry_too_big() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let mut mapping = RetentionMapping {
            path: "realm/device_id/com.Foo/bar".into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: Some(Duration::from_secs(u64::MAX)),
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let res = store.mapping(&mapping.path).await.unwrap().unwrap();

        mapping.expiry = None;

        assert_eq!(res, mapping);
    }

    #[test]
    fn should_get_id_from_row() {
        let exp_t = TimestampMillis(42u128);
        let exp_c = 9;
        let t = exp_t.to_be_bytes();

        let id = Id::from_row(t.as_slice(), exp_c).unwrap();

        assert_eq!(id.timestamp, exp_t);
        assert_eq!(id.counter, exp_c);
    }

    #[tokio::test]
    async fn should_store_and_check_publish() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let topic = "realm/device_id/com.Foo/bar";

        let mapping = RetentionMapping {
            path: topic.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let packet = RetentionPublish {
            id: Id {
                timestamp: TimestampMillis(1),
                counter: 2,
            },
            path: topic.into(),
            payload: [].as_slice().into(),
            expiry: None,
        };
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_publish(&packet, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let res = store.publish(&packet.id).await.unwrap().unwrap();

        assert_eq!(res, packet);
    }

    #[tokio::test]
    async fn should_remove_sent_packet() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let topic = "realm/device_id/com.Foo/bar";

        let mapping = RetentionMapping {
            path: topic.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let id = Id {
            timestamp: TimestampMillis(1),
            counter: 1,
        };
        let exp = RetentionPublish {
            id,
            path: topic.into(),
            payload: [].as_slice().into(),
            expiry: None,
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_publish(&exp, &mut t).await.unwrap();
        t.commit().await.unwrap();

        store.delete_publish_by_id(&id).await.unwrap();

        let packet = store.publish(&exp.id).await.unwrap();

        assert_eq!(packet, None);
    }

    #[tokio::test]
    async fn error_remove_missing_sent_packet() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let id = Id {
            timestamp: TimestampMillis(1),
            counter: 1,
        };
        store.delete_publish_by_id(&id).await.unwrap_err();
    }

    #[tokio::test]
    async fn should_get_all_packets() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let topic = "realm/device_id/com.Foo/bar";

        let mapping = RetentionMapping {
            path: topic.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let packets = [
            RetentionPublish {
                id: Id {
                    timestamp: TimestampMillis(1),
                    counter: 2,
                },
                path: topic.into(),
                payload: [].as_slice().into(),
                expiry: None,
            },
            RetentionPublish {
                id: Id {
                    timestamp: TimestampMillis(2),
                    counter: 0,
                },
                path: topic.into(),
                payload: [].as_slice().into(),
                expiry: None,
            },
            RetentionPublish {
                id: Id {
                    timestamp: TimestampMillis(2),
                    counter: 1,
                },
                path: topic.into(),
                payload: [].as_slice().into(),
                expiry: None,
            },
        ];

        let mut t = store.db_conn.begin().await.unwrap();
        for packet in &packets {
            store.store_publish(packet, &mut t).await.unwrap();
        }
        t.commit().await.unwrap();

        let expected = packets
            .into_iter()
            .map(|p| {
                let (intf, path) = split_interface_and_path(&p.id, p.path.into()).unwrap();

                (
                    p.id,
                    PublishInfo {
                        path: path.into(),
                        reliability: mapping.reliability,
                        interface: intf.into(),
                        version_major: mapping.version_major,
                        expiry: mapping.expiry,
                        value: p.payload,
                    },
                )
            })
            .collect_vec();

        let res = store
            .fetch_publishes()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn should_resend_all() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let topic = "realm/device_id/com.Foo/bar";

        let mapping = RetentionMapping {
            path: topic.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let packets = [
            RetentionPublish {
                id: Id {
                    timestamp: TimestampMillis(1),
                    counter: 2,
                },
                path: topic.into(),
                payload: [].as_slice().into(),
                expiry: None,
            },
            RetentionPublish {
                id: Id {
                    timestamp: TimestampMillis(2),
                    counter: 0,
                },
                path: topic.into(),
                payload: [].as_slice().into(),
                expiry: None,
            },
            RetentionPublish {
                id: Id {
                    timestamp: TimestampMillis(2),
                    counter: 1,
                },
                path: topic.into(),
                payload: [].as_slice().into(),
                expiry: None,
            },
        ];

        let mut t = store.db_conn.begin().await.unwrap();
        for packet in &packets {
            store.store_publish(packet, &mut t).await.unwrap();
        }
        t.commit().await.unwrap();

        let expected = packets
            .into_iter()
            .map(|p| {
                let (intf, path) = split_interface_and_path(&p.id, p.path.into()).unwrap();

                (
                    p.id,
                    PublishInfo {
                        path: path.into(),
                        reliability: mapping.reliability,
                        interface: intf.into(),
                        version_major: mapping.version_major,
                        expiry: mapping.expiry,
                        value: p.payload,
                    },
                )
            })
            .collect_vec();

        let res = store
            .fetch_publishes()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn should_store_and_delete_publish() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let topic = "realm/device_id/com.Foo/bar";

        let mapping = RetentionMapping {
            path: topic.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let packet = RetentionPublish {
            id: Id {
                timestamp: TimestampMillis(1),
                counter: 2,
            },
            path: topic.into(),
            payload: [].as_slice().into(),
            expiry: None,
        };
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_publish(&packet, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let res = store.publish(&packet.id).await.unwrap().unwrap();

        assert_eq!(res, packet);

        store.delete_publish_by_id(&packet.id).await.unwrap();

        let res = store.publish(&packet.id).await.unwrap();

        assert!(res.is_none());
    }

    #[test]
    fn should_split_path() {
        let (interface, path) = ("foo", "/bar/other");

        let (i, p) = split_interface_and_path(
            &Id {
                timestamp: TimestampMillis(0),
                counter: 0,
            },
            format!("{interface}{path}"),
        )
        .unwrap();

        assert_eq!(i, interface);
        assert_eq!(p, path);
    }

    #[tokio::test]
    async fn should_resend_all_without_expired() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let topic = "realm/device_id/com.Foo/bar";

        let mapping = RetentionMapping {
            path: topic.into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: Some(Duration::from_secs(3600)),
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let ctx = Context::new();

        let info = PublishInfo::from_ref(
            "realm/device_id/com.Foo",
            "/bar",
            mapping.version_major,
            mapping.reliability,
            crate::interface::Retention::Stored {
                expiry: mapping.expiry,
            },
            &[],
        );

        // Only the first is still valid.
        let packets = [
            RetentionPublish::from_info(ctx.next(), &info),
            RetentionPublish::from_info(
                Id {
                    timestamp: TimestampMillis(2),
                    counter: 0,
                },
                &info,
            ),
            RetentionPublish::from_info(
                Id {
                    timestamp: TimestampMillis(2),
                    counter: 1,
                },
                &info,
            ),
        ];

        let mut t = store.db_conn.begin().await.unwrap();
        for packet in &packets {
            store.store_publish(packet, &mut t).await.unwrap();
        }
        t.commit().await.unwrap();

        let expected = packets
            .into_iter()
            // Only the first
            .take(1)
            .map(|p| {
                let (intf, path) = split_interface_and_path(&p.id, p.path.into()).unwrap();

                (
                    p.id,
                    PublishInfo {
                        path: path.into(),
                        reliability: mapping.reliability,
                        interface: intf.into(),
                        version_major: mapping.version_major,
                        expiry: mapping.expiry,
                        value: p.payload,
                    },
                )
            })
            .collect_vec();

        store.delete_expired().await.unwrap();

        let res = store
            .fetch_publishes()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(res, expected);
    }
}
