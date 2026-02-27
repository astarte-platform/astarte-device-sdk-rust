// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

//! Handles the sending of properties

use astarte_interfaces::schema::Ownership;
use astarte_interfaces::{InterfaceMapping, MappingPath, Properties, Schema};
use tracing::{debug, error, trace};

use crate::interfaces::MappingRef;
use crate::state::ConnStatus;
use crate::store::{PropertyMapping, PropertyState, PropertyStore, StoredProp};
use crate::transport::Connection;
use crate::validate::{ValidatedProperty, ValidatedUnset};
use crate::{AstarteData, Error};

use super::{DeviceClient, Publish};

impl<C> DeviceClient<C>
where
    C: Connection,
{
    pub(crate) async fn send_property(
        &mut self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: AstarteData,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let interfaces = self.state.interfaces().read().await;
        let mapping = interfaces.get_property(interface_name, path)?;

        let validated = ValidatedProperty::validate(mapping, data)?;

        trace!("sending individual type {}", validated.data.display_type());

        if self.is_prop_stored(&mapping, &validated).await? {
            debug!("property was already sent, no need to send it again");
            return Ok(());
        }

        let prop = StoredProp {
            interface: validated.interface.as_str(),
            path: validated.path.as_str(),
            value: &validated.data,
            interface_major: mapping.interface().version_major(),
            ownership: Ownership::Device,
        };

        self.store.store_prop(prop).await?;

        debug!(
            "property sent {interface_name}{path}:{}",
            mapping.interface().version_major()
        );

        match self.state.connection().await {
            ConnStatus::Connected => {
                let expected = Some(validated.data.clone());

                self.sender.send_property(validated).await?;

                let updated = self
                    .store
                    .update_state(
                        &PropertyMapping::from(&mapping),
                        PropertyState::Completed,
                        expected,
                    )
                    .await?;

                trace!(
                    ?updated,
                    "property sent {interface_name}{path}:{}",
                    mapping.interface().version_major()
                );
            }
            ConnStatus::Disconnected => {
                trace!("property not sent since offline")
            }
            ConnStatus::Closed => {
                return Err(Error::Disconnected);
            }
        }

        Ok(())
    }

    /// Checks whether a passed interface is a property and if it is already stored with the same value.
    /// Useful to prevent sending a property twice with the same value.
    async fn is_prop_stored(
        &self,
        mapping: &MappingRef<'_, Properties>,
        new: &ValidatedProperty,
    ) -> Result<bool, Error> {
        // Check if this property is already in db
        let stored = self.try_load_prop(mapping).await?;

        Ok(stored.is_some_and(|val| val == new.data))
    }

    /// Get a property or deletes it if a version or type miss-match happens.
    pub(crate) async fn try_load_prop(
        &self,
        mapping: &MappingRef<'_, Properties>,
    ) -> Result<Option<AstarteData>, Error> {
        let property_mapping = PropertyMapping::from(mapping);
        let mapping = mapping.mapping();

        let value = self.store.load_prop(&property_mapping).await?;

        let value = match value {
            Some(value) if !value.eq_mapping_type(mapping.mapping_type()) => {
                error!(
                    ?value,
                    "stored property type mismatch, expected {}",
                    mapping.mapping_type(),
                );
                self.store.delete_prop(&property_mapping).await?;

                None
            }

            Some(value) => Some(value),
            None => None,
        };

        Ok(value)
    }

    pub(crate) async fn send_unset(
        &mut self,
        interface_name: &str,
        path: &MappingPath<'_>,
    ) -> Result<(), Error>
    where
        C::Sender: Publish,
    {
        let interfaces = self.state.interfaces().read().await;
        let mapping = interfaces.get_property(interface_name, path)?;

        let validated = ValidatedUnset::validate(mapping)?;

        debug!("unsetting property {interface_name}{path}");

        let property_mapping = PropertyMapping::from(&mapping);
        self.store.unset_prop(&property_mapping).await?;

        match self.state.connection().await {
            ConnStatus::Connected => {
                self.sender.unset(validated.clone()).await?;

                let updated = self
                    .store
                    .delete_expected_prop(&PropertyMapping::from(&mapping), None)
                    .await?;

                debug!(?updated, "delete unset property");
            }
            ConnStatus::Disconnected => {
                trace!("not deleting property from store, since disconnected");
            }
            ConnStatus::Closed => {
                return Err(Error::Disconnected);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use mockall::{Sequence, predicate};

    use crate::client::tests::mock_client;
    use crate::store::{PropertyMapping, PropertyStore, StoredProp};
    use crate::test::{E2E_DEVICE_PROPERTY, E2E_DEVICE_PROPERTY_NAME};
    use crate::validate::{ValidatedProperty, ValidatedUnset};
    use crate::{AstarteData, Client};

    #[tokio::test]
    async fn send_property_connected() {
        let mut client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Connected);

        let path = "/sensor_1/longinteger_endpoint";
        let value = AstarteData::LongInteger(42);

        let mut seq = Sequence::new();

        client
            .sender
            .expect_send_property()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(ValidatedProperty {
                interface: E2E_DEVICE_PROPERTY_NAME.to_string(),
                path: path.to_string(),
                version_major: 0,
                data: value.clone(),
            }))
            .returning(|_| Ok(()));

        // Send
        client
            .set_property(E2E_DEVICE_PROPERTY_NAME, path, value.clone())
            .await
            .unwrap();

        let interfaces = client.state.interfaces().read().await;
        let path = MappingPath::try_from(path).unwrap();
        let mapping = interfaces
            .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap();

        let prop = client
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prop, value);
    }

    #[tokio::test]
    async fn send_property_offline() {
        let mut client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Disconnected);

        let path = "/sensor_1/longinteger_endpoint";
        let value = AstarteData::LongInteger(42);

        // Send
        client
            .set_property(E2E_DEVICE_PROPERTY_NAME, path, value.clone())
            .await
            .unwrap();

        let interfaces = client.state.interfaces().read().await;
        let path = MappingPath::try_from(path).unwrap();
        let mapping = interfaces
            .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap();

        let prop = client
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(prop, value);
    }

    #[tokio::test]
    async fn send_property_connected_already_stored() {
        let mut client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Connected);

        let path = "/sensor_1/longinteger_endpoint";
        let value = AstarteData::LongInteger(42);

        // No expect, but store the prop
        client
            .store
            .store_prop(StoredProp {
                interface: E2E_DEVICE_PROPERTY_NAME,
                path,
                value: &value,
                interface_major: 0,
                ownership: Ownership::Device,
            })
            .await
            .unwrap();

        // Send
        client
            .set_property(E2E_DEVICE_PROPERTY_NAME, path, value.clone())
            .await
            .unwrap();

        let interfaces = client.state.interfaces().read().await;
        let path = MappingPath::try_from(path).unwrap();
        let mapping = interfaces
            .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap();

        let prop = client
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(prop, value);
    }

    #[tokio::test]
    async fn unset_property_connected_already_stored() {
        let mut client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Connected);

        let path = "/sensor_1/longinteger_endpoint";

        let mut seq = Sequence::new();

        client
            .sender
            .expect_unset()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(ValidatedUnset {
                interface: E2E_DEVICE_PROPERTY_NAME.to_string(),
                path: path.to_string(),
            }))
            .returning(|_| Ok(()));

        // Send
        client
            .unset_property(E2E_DEVICE_PROPERTY_NAME, path)
            .await
            .unwrap();

        let interfaces = client.state.interfaces().read().await;
        let path = MappingPath::try_from(path).unwrap();
        let mapping = interfaces
            .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap();

        let prop = client
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap();

        assert_eq!(prop, None);
    }

    #[tokio::test]
    async fn send_property_connected_already_stored_wrong_type() {
        let mut client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Connected);

        let path = "/sensor_1/longinteger_endpoint";
        let value = AstarteData::LongInteger(42);

        client
            .store
            .store_prop(StoredProp {
                interface: E2E_DEVICE_PROPERTY_NAME,
                path,
                // Wrong type
                value: &AstarteData::Boolean(false),
                interface_major: 0,
                ownership: Ownership::Device,
            })
            .await
            .unwrap();

        let mut seq = Sequence::new();

        client
            .sender
            .expect_send_property()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(ValidatedProperty {
                interface: E2E_DEVICE_PROPERTY_NAME.to_string(),
                path: path.to_string(),
                version_major: 0,
                data: value.clone(),
            }))
            .returning(|_| Ok(()));

        // Send
        client
            .set_property(E2E_DEVICE_PROPERTY_NAME, path, value.clone())
            .await
            .unwrap();

        let interfaces = client.state.interfaces().read().await;
        let path = MappingPath::try_from(path).unwrap();
        let mapping = interfaces
            .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap();

        let prop = client
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prop, value);
    }

    #[tokio::test]
    async fn unset_property_offline_already_stored() {
        let mut client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Disconnected);

        let path = "/sensor_1/longinteger_endpoint";

        // Send
        client
            .unset_property(E2E_DEVICE_PROPERTY_NAME, path)
            .await
            .unwrap();

        let interfaces = client.state.interfaces().read().await;
        let path = MappingPath::try_from(path).unwrap();
        let mapping = interfaces
            .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap();

        let prop = client
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap();

        assert_eq!(prop, None);
    }
}
