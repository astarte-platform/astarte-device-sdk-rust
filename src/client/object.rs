// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

use tracing::{debug, trace, warn};

use crate::aggregate::AstarteObject;
use crate::client::ValidatedObject;
use crate::error::AggregationError;
use crate::interface::mapping::path::MappingPath;
use crate::interface::{Aggregation, Retention};
use crate::state::SharedState;
use crate::store::StoreCapabilities;
use crate::transport::Connection;
use crate::Error;

use super::{DeviceClient, Publish, RetentionId, StoreWrapper, StoredRetentionExt};

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
        let interfaces = self.state.interfaces.read().await;
        let interface = interfaces
            .get(interface_name)
            .ok_or_else(|| Error::InterfaceNotFound {
                name: interface_name.to_string(),
            })?;

        let object = interface.as_object_ref().ok_or_else(|| {
            Error::Aggregation(AggregationError::new(
                interface_name,
                path.as_str(),
                Aggregation::Object,
                interface.aggregation(),
            ))
        })?;

        let validated = ValidatedObject::validate(object, path, data, timestamp)?;

        debug!("sending object {}{}", interface_name, path);

        if !self.state.status.is_connected() {
            trace!("publish object while connection is offline");

            return Self::offline_send_object(
                &self.state,
                &self.store,
                &mut self.sender,
                validated,
            )
            .await;
        }

        match validated.retention {
            Retention::Volatile { .. } => {
                Self::send_volatile_object(&self.state, &mut self.sender, validated).await
            }
            Retention::Stored { .. } => {
                Self::send_stored_object(&self.state, &self.store, &mut self.sender, validated)
                    .await
            }
            Retention::Discard => self.sender.send_object(validated).await,
        }
    }

    async fn offline_send_object(
        state: &SharedState,
        store: &StoreWrapper<C::Store>,
        sender: &mut C::Sender,
        data: ValidatedObject,
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
                    let value = sender.serialize_object(&data)?;

                    retention.store_publish_object(&id, &data, &value).await?;
                } else {
                    warn!("storing interface with retention stored in volatile since the store doesn't support retention");

                    state.volatile_store.push(id, data).await;
                }
            }
        }

        Ok(())
    }

    async fn send_volatile_object(
        state: &SharedState,
        sender: &mut C::Sender,
        data: ValidatedObject,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let id = state.retention_ctx.next();

        state.volatile_store.push(id, data.clone()).await;

        sender
            .send_object_stored(RetentionId::Volatile(id), data)
            .await
    }

    async fn send_stored_object(
        state: &SharedState,
        store: &StoreWrapper<C::Store>,
        sender: &mut C::Sender,
        data: ValidatedObject,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let Some(retention) = store.get_retention() else {
            warn!("not storing interface with retention stored since the store doesn't support retention");

            return sender.send_object(data).await;
        };

        let value = sender.serialize_object(&data)?;

        let id = state.retention_ctx.next();

        retention.store_publish_object(&id, &data, &value).await?;

        sender
            .send_object_stored(RetentionId::Stored(id), data)
            .await
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use mockall::{predicate, Sequence};

    use crate::client::tests::mock_client;
    use crate::interface::Reliability;
    use crate::interfaces::tests::DEVICE_OBJECT;
    use crate::{AstarteType, Client};

    use super::*;

    #[tokio::test]
    async fn should_send_object() {
        let (mut client, _tx) = mock_client(&[DEVICE_OBJECT]);

        let interface = "test.device.object";
        let path = "/sensor_1";
        let timestamp = Utc::now();

        let obj = AstarteObject::from_iter(
            [
                ("double_endpoint", AstarteType::Double(42.0)),
                ("integer_endpoint", AstarteType::Integer(42)),
                ("boolean_endpoint", AstarteType::Boolean(false)),
                (
                    "booleanarray_endpoint",
                    AstarteType::BooleanArray(vec![true, false]),
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

        client.state.status.set_connected(true);

        // Test the sent
        client
            .send_object_with_timestamp(interface, path, obj, timestamp)
            .await
            .unwrap();
    }
}
