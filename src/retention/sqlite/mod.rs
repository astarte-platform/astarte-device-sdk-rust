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

//! Retention implemented using an SQLite database.

use std::{borrow::Cow, collections::HashSet, num::TryFromIntError, time::Duration};

use astarte_interfaces::schema::Reliability;
use rusqlite::{
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
    ToSql,
};
use tracing::{debug, error, instrument, trace};

use crate::{
    error::Report,
    store::{
        sqlite::{statements::WriteConnection, SqliteError},
        SqliteStore,
    },
};

use super::{
    duration_from_epoch, Id, PublishInfo, RetentionError, StoredInterface, StoredRetention,
    TimestampMillis,
};

mod statements;

impl FromSql for TimestampMillis {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let blob = value.as_blob()?;
        let bytes = blob.try_into().map_err(|_| FromSqlError::InvalidBlobSize {
            expected_size: 16,
            blob_size: blob.len(),
        })?;
        let timestamp = u128::from_be_bytes(bytes);

        Ok(Self(timestamp))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RetentionMapping<'a> {
    pub(crate) interface: Cow<'a, str>,
    pub(crate) path: Cow<'a, str>,
    pub(crate) version_major: i32,
    pub(crate) reliability: RetentionReliability,
    pub(crate) expiry: Option<Duration>,
}

impl RetentionMapping<'_> {
    pub(crate) fn expiry_to_sql(&self) -> Option<i64> {
        self.expiry.and_then(|exp| {
            // If the conversion fails, since the u64 was to big for the i64, we will keep the
            // packet forever.
            exp.as_secs().try_into().ok()
        })
    }
}

impl<'a> From<&'a PublishInfo<'a>> for RetentionMapping<'a> {
    fn from(value: &'a PublishInfo<'a>) -> Self {
        Self {
            interface: Cow::Borrowed(&value.interface),
            path: Cow::Borrowed(&value.path),
            version_major: value.version_major,
            reliability: RetentionReliability(value.reliability),
            expiry: value.expiry,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RetentionPublish<'a> {
    /// Unique id of the publish.
    id: Id,
    /// Composes with the path the relation with the [`RetentionMapping`].
    interface: Cow<'a, str>,
    /// Composes with the interface the relation with the [`RetentionMapping`].
    path: Cow<'a, str>,
    /// Optional expiration time calculated from the store time from the [`Id::timestamp`] and
    /// [`RetentionMapping::expiry`].
    expiry_time: Option<TimestampSecs>,
    /// Flag to check if the publish was sent.
    sent: bool,
    /// The serialized payload of the publish
    payload: Cow<'a, [u8]>,
}

impl<'a> RetentionPublish<'a> {
    fn from_info(id: Id, info: &'a PublishInfo<'a>) -> Result<Self, TryFromIntError> {
        let expiry_time = info
            .expiry
            .map(|expiry| TimestampSecs::from_id(&id, expiry))
            .transpose()?;

        Ok(Self {
            id,
            interface: Cow::Borrowed(&info.interface),
            path: Cow::Borrowed(&info.path),
            expiry_time,
            sent: info.sent,
            payload: Cow::Borrowed(&info.value),
        })
    }
}

/// New type to impl traits on [`Reliability`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RetentionReliability(Reliability);

impl From<RetentionReliability> for Reliability {
    fn from(value: RetentionReliability) -> Self {
        value.0
    }
}

impl From<Reliability> for RetentionReliability {
    fn from(value: Reliability) -> Self {
        RetentionReliability(value)
    }
}

impl ToSql for RetentionReliability {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let value: u8 = match self.0 {
            Reliability::Unreliable => 0,
            Reliability::Guaranteed => 1,
            Reliability::Unique => 2,
        };

        Ok(ToSqlOutput::from(value))
    }
}

impl FromSql for RetentionReliability {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(RetentionReliability(Reliability::Unreliable)),
            1 => Ok(RetentionReliability(Reliability::Guaranteed)),
            2 => Ok(RetentionReliability(Reliability::Unique)),
            err => Err(FromSqlError::OutOfRange(err)),
        }
    }
}

/// Expiration time of a [`RetentionPublish`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TimestampSecs(u64);

impl TimestampSecs {
    /// Get the current timestamp in seconds.
    fn now() -> Self {
        Self(duration_from_epoch().as_secs())
    }

    /// Standardize the conversion of the timestamp to bytes.
    fn from_id(id: &Id, expiry: Duration) -> Result<Self, TryFromIntError> {
        Duration::try_from(id.timestamp)
            .map(|timestamp| Self(timestamp.saturating_add(expiry).as_secs()))
    }

    /// Standardize the conversion of the timestamp to bytes.
    pub fn to_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl FromSql for TimestampSecs {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let blob = value.as_blob()?;

        let bytes = blob.try_into().map_err(|_| FromSqlError::InvalidBlobSize {
            expected_size: 8,
            blob_size: blob.len(),
        })?;

        let value = u64::from_be_bytes(bytes);

        Ok(Self(value))
    }
}

impl StoredRetention for SqliteStore {
    async fn store_publish(&self, id: &Id, info: PublishInfo<'_>) -> Result<(), RetentionError> {
        let mapping = RetentionMapping::from(&info);
        let publish = RetentionPublish::from_info(*id, &info)
            .map_err(|err| RetentionError::store(&info, err))?;

        self.writer
            .lock()
            .await
            .store(&mapping, &publish)
            .map_err(|err| RetentionError::store(&info, err))?;

        Ok(())
    }

    async fn update_sent_flag(&self, id: &Id, sent: bool) -> Result<(), RetentionError> {
        self.writer
            .lock()
            .await
            .update_publish_sent_flag(id, sent)
            .map_err(|err| RetentionError::update_sent(*id, sent, err))?;

        Ok(())
    }

    async fn mark_received(&self, id: &Id) -> Result<(), RetentionError> {
        self.writer
            .lock()
            .await
            .delete_publish_by_id(id)
            .map_err(|err| RetentionError::received(*id, err))?;

        Ok(())
    }

    async fn delete_publish(&self, id: &Id) -> Result<(), RetentionError> {
        self.writer
            .lock()
            .await
            .delete_publish_by_id(id)
            .map_err(|err| RetentionError::delete_publish(*id, err))?;

        Ok(())
    }

    async fn delete_interface(&self, interface: &str) -> Result<(), RetentionError> {
        self.writer
            .lock()
            .await
            .delete_interface(interface)
            .map_err(|err| RetentionError::delete_interface(interface.to_string(), err))?;

        Ok(())
    }

    async fn unsent_publishes(
        &self,
        limit: usize,
        buf: &mut Vec<(Id, PublishInfo<'static>)>,
    ) -> Result<usize, RetentionError> {
        let now = TimestampSecs::now();

        // Prefer the two different queries, so we can free the writer
        {
            self.writer
                .lock()
                .await
                .delete_expired(&now)
                .map_err(RetentionError::unsent)?;
        }

        let count = self
            .with_reader(|reader| reader.unset_publishes(buf, &now, limit))
            .map_err(RetentionError::unsent)?;

        Ok(count)
    }

    async fn reset_all_publishes(&self) -> Result<(), RetentionError> {
        let now = TimestampSecs::now();

        let writer = self.writer.lock().await;

        writer
            .delete_expired(&now)
            .map_err(RetentionError::unsent)?;

        writer.reset_all_sent().map_err(RetentionError::reset)?;

        Ok(())
    }

    async fn fetch_all_interfaces(&self) -> Result<HashSet<StoredInterface>, RetentionError> {
        self.with_reader(|reader| reader.all_interfaces())
            .map_err(RetentionError::fetch_interfaces)
    }

    async fn set_max_retention_items(
        &self,
        size: std::num::NonZeroUsize,
    ) -> Result<(), RetentionError> {
        let mut connection = self.writer.lock().await;

        connection.retention_capacity = size;

        let removed = connection
            .free_retention_items(0)
            .map_err(|err| RetentionError::set_capacity(size, err))?;

        if removed > 0 {
            if let Err(err) = connection.execute("VACUUM", []) {
                error!(error = %Report::new(err), "failed to vacuum the database");
            }
        }

        Ok(())
    }
}

impl WriteConnection {
    /// Empty space when the database is full to allow storing newer elements
    ///
    /// In sequence
    /// - first we check the occupied space is greater than the capacity
    /// - if so, remove the expired elements from the store
    /// - if the available space is still unsufficient, remove the oldest elements
    ///
    /// Return the number of removed elements.
    #[instrument(skip(self))]
    pub(crate) fn free_retention_items(&mut self, to_store: usize) -> Result<usize, SqliteError> {
        let max_items = self.retention_capacity.get();

        debug!("remove elements from strore if it's full");

        let count_stored = self.count_stored()?;

        trace!(count_stored, "initial count");

        let stored = count_stored.saturating_add(to_store);

        if stored <= max_items {
            return Ok(0);
        }

        trace!("store is full, evicting expired retention items");
        let expired = self.delete_expired(&TimestampSecs::now())?;

        trace!(expired, "removed expired items");

        let stored = stored.saturating_sub(expired);

        if stored <= max_items {
            return Ok(expired);
        }

        let to_remove = stored.saturating_sub(max_items);

        trace!(
            to_remove,
            "store is still full, evicting the oldest elements"
        );

        let removed = self.remove_oldest(to_remove)?;

        debug!(removed, "removed elements");

        Ok(removed.saturating_add(expired))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use astarte_interfaces::interface::Retention;
    use statements::tests::{fetch_mapping, fetch_publish};

    use crate::retention::Context;

    use super::*;

    pub(crate) fn publish_with_expiry(
        path: &'static str,
        expiry: Option<Duration>,
    ) -> PublishInfo<'static> {
        PublishInfo::from_ref(
            "com.Foo",
            path,
            1,
            Reliability::Unique,
            Retention::Stored { expiry },
            false,
            &[],
        )
    }

    #[tokio::test]
    async fn should_store_publish() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let publish_info = publish_with_expiry(path, None);

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Unique.into(),
            expiry: None,
        };

        let id = Context::new().next();

        let publish = RetentionPublish {
            id,
            interface: interface.into(),
            path: path.into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry_time: None,
        };

        store.store_publish(&id, publish_info).await.unwrap();

        let res = fetch_publish(&store, &id).unwrap();

        assert_eq!(res, publish);

        let res = fetch_mapping(&store, interface, path).unwrap();

        assert_eq!(res, mapping)
    }

    #[tokio::test]
    async fn should_remove_expired_and_store_publish() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        // suppose we can only store 2 publishes and we try storing 3 publishes: the new one should replace the expired one
        let capacity = NonZeroUsize::new(2).unwrap();
        store.set_max_retention_items(capacity).await.unwrap();

        let interface = "com.Foo";

        let ctx = Context::new();

        let id1 = ctx.next();
        store
            .store_publish(&id1, publish_with_expiry("/path1", None))
            .await
            .unwrap();

        // create a publish already expired, so it will be removed when a new publish is stored.
        // the publish must be expiref from at least 1sec since the expiration check is performed over seconds (not ms nor ns)
        let Id { timestamp, counter } = ctx.next();
        let timestamp = TimestampMillis(timestamp.0.saturating_sub(1000));
        let id2 = Id { timestamp, counter };

        store
            .store_publish(&id2, publish_with_expiry("/path2", Some(Duration::ZERO)))
            .await
            .unwrap();

        // this will cause publish1 to be removed since the store is full
        let id3 = ctx.next();
        store
            .store_publish(&id3, publish_with_expiry("/path3", None))
            .await
            .unwrap();

        let res = fetch_publish(&store, &id1).unwrap();

        let publish1 = RetentionPublish {
            id: id1,
            interface: interface.into(),
            path: "/path1".into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry_time: None,
        };

        assert_eq!(res, publish1);

        let res = fetch_publish(&store, &id2);
        assert!(res.is_none());

        let res = fetch_publish(&store, &id3).unwrap();

        let publish3 = RetentionPublish {
            id: id3,
            interface: interface.into(),
            path: "/path3".into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry_time: None,
        };

        assert_eq!(res, publish3);
    }

    #[tokio::test]
    async fn should_free_space_and_store_publish() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        // create a store which can only store 2 publishes.
        // try storing 3 publishes: a new one should replace the oldest one.
        let capacity = NonZeroUsize::new(2).unwrap();
        store.set_max_retention_items(capacity).await.unwrap();

        let interface = "com.Foo";

        let ctx = Context::new();

        // first publish to be inserted. This will be removed
        let id1 = ctx.next();
        store
            .store_publish(&id1, publish_with_expiry("/path1", None))
            .await
            .unwrap();

        let id2 = ctx.next();
        store
            .store_publish(&id2, publish_with_expiry("/path2", None))
            .await
            .unwrap();

        // this will cause publish1 to be removed since the store is full
        let id3 = ctx.next();
        store
            .store_publish(&id3, publish_with_expiry("/path3", None))
            .await
            .unwrap();

        let res = fetch_publish(&store, &id1);
        assert!(res.is_none());

        let res = fetch_publish(&store, &id2).unwrap();

        let publish2 = RetentionPublish {
            id: id2,
            interface: interface.into(),
            path: "/path2".into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry_time: None,
        };

        assert_eq!(res, publish2);

        let res = fetch_publish(&store, &id3).unwrap();

        let publish3 = RetentionPublish {
            id: id3,
            interface: interface.into(),
            path: "/path3".into(),
            payload: [].as_slice().into(),
            sent: false,
            expiry_time: None,
        };

        assert_eq!(res, publish3);
    }

    #[tokio::test]
    async fn should_mark_received() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let interface = "com.Foo";
        let path = "/bar";

        let publish_info = publish_with_expiry(path, None);

        let mapping = RetentionMapping {
            interface: interface.into(),
            path: path.into(),
            version_major: 1,
            reliability: Reliability::Unique.into(),
            expiry: None,
        };

        let id = Context::new().next();

        store.store_publish(&id, publish_info).await.unwrap();

        store.mark_received(&id).await.unwrap();

        let res = fetch_publish(&store, &id);

        assert_eq!(res, None);

        let res = fetch_mapping(&store, interface, path).unwrap();

        assert_eq!(res, mapping)
    }

    #[tokio::test]
    async fn should_fetch_all_interfaces() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let publish_info = publish_with_expiry("/bar", None);

        let id = Context::new().next();

        store.store_publish(&id, publish_info).await.unwrap();

        let res = store.fetch_all_interfaces().await.unwrap();

        let exp = StoredInterface {
            name: "com.Foo".into(),
            version_major: 1,
        };

        assert_eq!(res.len(), 1);
        assert!(res.contains(&exp));
    }

    #[tokio::test]
    async fn should_mark_sent_and_reset() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        let publish_info = publish_with_expiry("/bar", None);

        let id = Context::new().next();

        store.store_publish(&id, publish_info).await.unwrap();

        store.update_sent_flag(&id, true).await.unwrap();

        let res = fetch_publish(&store, &id).unwrap();
        assert!(res.sent);

        store.reset_all_publishes().await.unwrap();

        let res = fetch_publish(&store, &id).unwrap();
        assert!(!res.sent);
    }
}
