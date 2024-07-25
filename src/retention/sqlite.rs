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

use std::{array::TryFromSliceError, borrow::Cow, collections::HashSet, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use sqlx::{query_file, Sqlite, Transaction};
use tracing::{error, warn};

use crate::{interface::Reliability, store::SqliteStore};

use super::{Id, PublishInfo, RetentionError, StoredInterface, StoredRetention, TimestampMillis};

/// Error returned by the retention.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqliteRetentionError {
    /// Couldn't reset all the sent publishes.
    #[error("couldn't reset all the sent packets")]
    ResetSent(#[source] sqlx::Error),
    /// Couldn't get unsent publishes.
    #[error("couldn't get all the packets")]
    UnsetPublishes(#[source] sqlx::Error),
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
    /// Couldn't delete publish by id.
    #[error("coudln't delete publish with id {id}")]
    DeleteId {
        /// The source of the error
        #[source]
        backtrace: sqlx::Error,
        /// Id of the publish
        id: Id,
    },
    /// Couldn't delete publish by id.
    #[error("coudln't delete publish with interface {interface}")]
    DeleteInterface {
        /// The source of the error
        #[source]
        backtrace: sqlx::Error,
        /// Interface to delete.
        interface: String,
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
    /// Couldn't delete expired publishes
    #[error("couldn't delete expired publishes")]
    Expired(#[source] sqlx::Error),
    /// Couldn't fetch mapping
    #[error("couldn't fetch mapping {path}")]
    Mapping {
        /// The source of the error
        #[source]
        backtrace: sqlx::Error,
        /// Path of the packet
        path: String,
    },
    /// Couldn't fetch interfaces
    #[error("couldn't fetch interfaces")]
    AllInterfaces(#[source] sqlx::Error),
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
    interface: Cow<'a, str>,
    path: Cow<'a, str>,
    version_major: i32,
    reliability: Reliability,
    expiry: Option<Duration>,
}

impl<'a> From<&'a PublishInfo<'a>> for RetentionMapping<'a> {
    fn from(value: &'a PublishInfo<'a>) -> Self {
        Self {
            interface: Cow::Borrowed(&value.interface),
            path: Cow::Borrowed(&value.path),
            version_major: value.version_major,
            reliability: value.reliability,
            expiry: value.expiry,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RetentionPublish<'a> {
    id: Id,
    interface: Cow<'a, str>,
    path: Cow<'a, str>,
    expiry: Option<TimestampMillis>,
    sent: bool,
    payload: Cow<'a, [u8]>,
}

impl<'a> RetentionPublish<'a> {
    fn from_info(id: Id, info: &'a PublishInfo<'a>) -> Self {
        let expiry = info.expiry.map(|exp| {
            let t = id.timestamp.saturating_add(exp.as_millis());
            TimestampMillis(t)
        });

        Self {
            id,
            interface: Cow::Borrowed(&info.interface),
            path: Cow::Borrowed(&info.path),
            expiry,
            sent: info.sent,
            payload: Cow::Borrowed(&info.value),
        }
    }
}

/// Gets the [`Reliability`] from a stored [`u8`].
fn reliability_from_row(qos: u8) -> Option<Reliability> {
    match qos {
        0 => Some(Reliability::Unreliable),
        1 => Some(Reliability::Guaranteed),
        2 => Some(Reliability::Unique),
        _ => None,
    }
}

// Implementation of utilities used in the retention
impl SqliteStore {
    async fn fetch_mapping_interfaces(
        &self,
    ) -> Result<HashSet<StoredInterface>, SqliteRetentionError> {
        query_file!("queries/retention/all_interfaces.sql")
            .fetch(&self.db_conn)
            .map_err(SqliteRetentionError::AllInterfaces)
            .map_ok(|e| StoredInterface {
                name: e.interface,
                version_major: e.major_version,
            })
            .try_collect::<HashSet<StoredInterface>>()
            .await
    }

    async fn fetch_unsent(
        &self,
        now: &TimestampMillis,
        limit: usize,
        buf: &mut Vec<(Id, PublishInfo<'static>)>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<usize, SqliteRetentionError> {
        let now = now.to_bytes();
        let now = now.as_slice();

        // Cap to max
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut count: usize = 0;

        query_file!("queries/retention/unsent_publishes.sql", now, limit)
            .fetch(&mut **transaction)
            .map(|res| {
                res.map_err(SqliteRetentionError::UnsetPublishes)
                    .and_then(|row| {
                        let id = Id::from_row(&row.t_millis, row.counter)
                            .map_err(SqliteRetentionError::Timestamp)?;

                        let reliability = reliability_from_row(row.reliability)
                            .ok_or(SqliteRetentionError::Reliability(row.reliability))?;

                        let expiry = row.expiry_sec.and_then(|exp| {
                            // If the conversion fails, let's keep the packet forever.
                            exp.try_into().ok().map(Duration::from_secs)
                        });

                        Ok((
                            id,
                            PublishInfo {
                                interface: row.interface.into(),
                                path: row.path.into(),
                                version_major: row.major_version,
                                reliability,
                                expiry,
                                sent: row.sent,
                                value: row.payload.into(),
                            },
                        ))
                    })
            })
            .try_for_each(|val| {
                buf.push(val);

                count = count.saturating_add(1);

                futures::future::ok(())
            })
            .await?;

        Ok(count)
    }

    async fn store_mapping(
        &self,
        mapping: &RetentionMapping<'_>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), SqliteRetentionError> {
        if let Some(stored) = self
            .mapping(&mapping.interface, &mapping.path, transaction)
            .await?
        {
            if stored == *mapping {
                return Ok(());
            }

            warn!("mappings differ, replacing");
        }

        let retention: u8 = match mapping.reliability {
            Reliability::Unreliable => 0,
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
            mapping.interface,
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
            publish.interface,
            publish.path,
            expiry,
            publish.sent,
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

    async fn mapping(
        &self,
        interface: &str,
        path: &str,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Option<RetentionMapping<'static>>, SqliteRetentionError> {
        let Some(row) = query_file!("queries/retention/mapping.sql", interface, path)
            .fetch_optional(&mut **transaction)
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
            interface: row.interface.into(),
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
            interface: row.interface.into(),
            path: row.path.into(),
            expiry,
            sent: row.sent,
            payload: row.payload.into(),
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
        .map_err(|err| SqliteRetentionError::DeleteId {
            backtrace: err,
            id: *id,
        })?;

        let rows = res.rows_affected();
        if rows != 1 {
            return Err(SqliteRetentionError::DeletedRows { id: *id, rows });
        }

        Ok(())
    }

    async fn delete_publish_and_mappings(
        &self,
        interface: &str,
    ) -> Result<(), SqliteRetentionError> {
        let mut t = self
            .db_conn
            .begin()
            .await
            .map_err(SqliteRetentionError::begin)?;

        let t_ref = &mut t;

        query_file!(
            "queries/retention/delete_publish_by_interface.sql",
            interface,
        )
        .execute(&mut **t_ref)
        .await
        .map_err(|err| SqliteRetentionError::DeleteInterface {
            interface: interface.to_string(),
            backtrace: err,
        })?;

        query_file!(
            "queries/retention/delete_mapping_by_interface.sql",
            interface,
        )
        .execute(&mut **t_ref)
        .await
        .map_err(|err| SqliteRetentionError::DeleteInterface {
            interface: interface.to_string(),
            backtrace: err,
        })?;

        t.commit().await.map_err(SqliteRetentionError::commit)?;

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

    async fn delete_expired(
        &self,
        now: &TimestampMillis,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), SqliteRetentionError> {
        let now = now.to_bytes();
        let now = now.as_slice();

        query_file!("queries/retention/delete_expired.sql", now)
            .execute(&mut **transaction)
            .await
            .map_err(SqliteRetentionError::Expired)?;

        Ok(())
    }

    async fn update_publish_sent_flag(
        &self,
        id: &Id,
        sent: bool,
    ) -> Result<(), SqliteRetentionError> {
        let timestamp = id.timestamp.to_bytes();
        let timestamp = timestamp.as_slice();

        query_file!(
            "queries/retention/update_sent.sql",
            sent,
            timestamp,
            id.counter
        )
        .execute(&self.db_conn)
        .await
        .map_err(SqliteRetentionError::Expired)?;

        Ok(())
    }

    async fn reset_all_sent(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), SqliteRetentionError> {
        query_file!("queries/retention/reset_all_sent.sql")
            .execute(&mut **transaction)
            .await
            .map_err(SqliteRetentionError::ResetSent)?;

        Ok(())
    }
}

#[async_trait]
impl StoredRetention for SqliteStore {
    async fn store_publish(&self, publish: PublishInfo<'_>) -> Result<Id, RetentionError> {
        let id = self.retention_ctx.next();

        let ret_map = RetentionMapping::from(&publish);
        let ret_pub = RetentionPublish::from_info(id, &publish);

        self.store(&ret_map, &ret_pub)
            .await
            .map_err(|err| RetentionError::store(&publish, err))?;

        Ok(id)
    }

    async fn update_sent_flag(&self, id: &Id, sent: bool) -> Result<(), RetentionError> {
        self.update_publish_sent_flag(id, sent)
            .await
            .map_err(|err| RetentionError::update_sent(*id, sent, err))
    }

    async fn mark_received(&self, id: &Id) -> Result<(), RetentionError> {
        self.delete_publish_by_id(id)
            .await
            .map_err(|err| RetentionError::received(*id, err))
    }

    async fn delete_publish(&self, id: &Id) -> Result<(), RetentionError> {
        self.delete_publish_by_id(id)
            .await
            .map_err(|err| RetentionError::delete_publish(*id, err))
    }

    async fn delete_interface(&self, interface: &str) -> Result<(), RetentionError> {
        self.delete_publish_and_mappings(interface)
            .await
            .map_err(|err| RetentionError::delete_interface(interface.to_string(), err))
    }

    async fn unsent_publishes(
        &self,
        limit: usize,
        buf: &mut Vec<(Id, PublishInfo<'static>)>,
    ) -> Result<usize, RetentionError> {
        let mut t = self
            .db_conn
            .begin()
            .await
            .map_err(|err| RetentionError::unsent(SqliteRetentionError::begin(err)))?;

        let now = TimestampMillis::now();

        self.delete_expired(&now, &mut t)
            .await
            .map_err(RetentionError::unsent)?;

        let count = self
            .fetch_unsent(&now, limit, buf, &mut t)
            .await
            .map_err(RetentionError::unsent)?;

        t.commit()
            .await
            .map_err(|err| RetentionError::unsent(SqliteRetentionError::commit(err)))?;

        Ok(count)
    }

    async fn reset_all_publishes(&self) -> Result<(), RetentionError> {
        let mut t = self
            .db_conn
            .begin()
            .await
            .map_err(|err| RetentionError::reset(SqliteRetentionError::begin(err)))?;

        self.delete_expired(&TimestampMillis::now(), &mut t)
            .await
            .map_err(RetentionError::reset)?;

        self.reset_all_sent(&mut t)
            .await
            .map_err(RetentionError::reset)?;

        t.commit()
            .await
            .map_err(|err| RetentionError::unsent(SqliteRetentionError::commit(err)))?;

        Ok(())
    }

    async fn fetch_all_interfaces(&self) -> Result<HashSet<StoredInterface>, RetentionError> {
        self.fetch_mapping_interfaces()
            .await
            .map_err(RetentionError::fetch_interfaces)
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
            interface: "com.Foo".into(),
            path: "/bar".into(),
            version_major: 1,
            reliability: Reliability::Guaranteed,
            expiry: None,
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();

        let res = store
            .mapping(&mapping.interface, &mapping.path, &mut t)
            .await
            .unwrap()
            .unwrap();
        t.commit().await.unwrap();

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

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();

        let res = store
            .mapping(&mapping.interface, &mapping.path, &mut t)
            .await
            .unwrap()
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(res, mapping);

        mapping.version_major = 2;

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();

        let res = store
            .mapping(&mapping.interface, &mapping.path, &mut t)
            .await
            .unwrap()
            .unwrap();
        t.commit().await.unwrap();

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

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();

        let res = store
            .mapping(&mapping.interface, &mapping.path, &mut t)
            .await
            .unwrap()
            .unwrap();
        t.commit().await.unwrap();

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

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
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
            interface: interface.into(),
            path: path.into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry: None,
        };
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_publish(&packet, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let res = store.publish(&packet.id).await.unwrap().unwrap();

        assert_eq!(res, packet);
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
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let mut packet = RetentionPublish {
            id: Id {
                timestamp: TimestampMillis(1),
                counter: 2,
            },
            interface: interface.into(),
            path: path.into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry: None,
        };
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_publish(&packet, &mut t).await.unwrap();
        t.commit().await.unwrap();

        store.update_sent_flag(&packet.id, true).await.unwrap();

        let res = store.publish(&packet.id).await.unwrap().unwrap();

        packet.sent = true;

        assert_eq!(res, packet);

        store.update_sent_flag(&packet.id, false).await.unwrap();

        let res = store.publish(&packet.id).await.unwrap().unwrap();

        packet.sent = false;

        assert_eq!(res, packet);
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
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let id = Id {
            timestamp: TimestampMillis(1),
            counter: 1,
        };
        let exp = RetentionPublish {
            id,
            interface: interface.into(),
            path: path.into(),
            expiry: None,
            sent: false,
            payload: [].as_slice().into(),
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_publish(&exp, &mut t).await.unwrap();
        t.commit().await.unwrap();

        store.delete_publish_by_id(&id).await.unwrap();

        let packet = store.publish(&exp.id).await.unwrap();

        assert_eq!(packet, None);
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
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let id = Id {
            timestamp: TimestampMillis(1),
            counter: 1,
        };
        let exp = RetentionPublish {
            id,
            interface: interface.into(),
            path: path.into(),
            expiry: None,
            sent: false,
            payload: [].as_slice().into(),
        };

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_publish(&exp, &mut t).await.unwrap();
        t.commit().await.unwrap();

        store.delete_interface(interface).await.unwrap();

        let packet = store.publish(&exp.id).await.unwrap();

        assert_eq!(packet, None);

        let mut t = store.db_conn.begin().await.unwrap();
        let mapping = store
            .mapping(&mapping.interface, &mapping.path, &mut t)
            .await
            .unwrap();

        assert_eq!(mapping, None);
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

        let interface = "com.Foo";
        let path = "/bar";

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
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
                interface: interface.into(),
                path: path.into(),
                expiry: None,
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
                expiry: None,
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
                expiry: None,
                sent: false,
                payload: [].as_slice().into(),
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

        let mut t = store.db_conn.begin().await.unwrap();

        let mut res = Vec::new();
        let count = store
            .fetch_unsent(&TimestampMillis::now(), 100, &mut res, &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

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
        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let packet = RetentionPublish {
            id: Id {
                timestamp: TimestampMillis(1),
                counter: 2,
            },
            interface: interface.into(),
            path: path.into(),
            expiry: None,
            sent: false,
            payload: [].as_slice().into(),
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

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

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

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping, &mut t).await.unwrap();
        t.commit().await.unwrap();

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
        let mut packets = vec![RetentionPublish::from_info(ctx.next(), &info_cl)];

        info.sent = true;

        packets.push(RetentionPublish::from_info(
            Id {
                timestamp: TimestampMillis(2),
                counter: 0,
            },
            &info,
        ));

        packets.push(RetentionPublish::from_info(
            Id {
                timestamp: TimestampMillis(2),
                counter: 1,
            },
            &info,
        ));

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

        let mut t = store.db_conn.begin().await.unwrap();
        store.store_mapping(&mapping1, &mut t).await.unwrap();
        store.store_mapping(&mapping2, &mut t).await.unwrap();
        t.commit().await.unwrap();

        let res = store.fetch_mapping_interfaces().await.unwrap();

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
