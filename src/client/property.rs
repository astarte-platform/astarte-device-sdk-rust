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

//! Handles the sending of properties

use astarte_device_error::{Error, ResultExt};
use astarte_interfaces::schema::Ownership;
use astarte_interfaces::{MappingPath, Schema};
use tracing::{debug, instrument, trace};

use crate::AstarteData;
use crate::builder::ConnectionConfig;
use crate::error::{AstarteError, ErrorKind};
use crate::state::ConnStatus;
use crate::store::{Prop, PropertyMapping, StoreCapabilities};
use crate::validate::Validated;
use crate::validate::properties::{ValidatedProperty, ValidatedUnset};

use super::DeviceClient;

impl<C, S> DeviceClient<C, S>
where
    C: ConnectionConfig,
{
    #[instrument(skip_all, fields(interface = interface_name, path = %path, mapping = data.display_type()))]
    pub(crate) async fn send_property(
        &self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: AstarteData,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        let interfaces = self.state.interfaces().read().await;
        let mapping = interfaces
            .get_property(interface_name, path)
            .map_kind(ErrorKind::Interface)?;

        let validated =
            ValidatedProperty::validate(mapping, data).map_kind(ErrorKind::Interface)?;

        trace!("sending individual type {}", validated.data.display_type());

        let prop = Prop {
            interface: validated.interface.clone(),
            path: validated.path.clone(),
            value: validated.data.clone(),
            interface_major: mapping.interface().version_major(),
            ownership: Ownership::Device,
            updated_at: self.state.property_ctx().next_updated_at(),
        };

        let meta = self
            .state
            .store()
            .store_prop(prop)
            .await
            .map_kind(ErrorKind::Store)?;

        let Some(epoch) = meta.epoch() else {
            debug!("property was already sent, no need to send it again");

            return Ok(());
        };

        debug!("property updated");

        match self.state.connection() {
            ConnStatus::Online => {
                self.send_timeout(Validated::Property {
                    epoch,
                    data: validated,
                })
                .await?;

                debug!("property sent");
            }
            ConnStatus::Offline | ConnStatus::Connected { .. } => {
                trace!("property not sent since offline")
            }
            ConnStatus::Disconnect | ConnStatus::Closed => {
                return Err(Error::with(
                    ErrorKind::Disconnected,
                    "while sending property",
                ));
            }
        }

        Ok(())
    }

    #[instrument(skip_all, fields(interface = interface_name, path = %path))]
    pub(crate) async fn send_unset(
        &self,
        interface_name: &str,
        path: &MappingPath<'_>,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
    {
        let interfaces = self.state.interfaces().read().await;
        let mapping = interfaces
            .get_property(interface_name, path)
            .map_kind(ErrorKind::Interface)?;

        let validated = ValidatedUnset::validate(mapping).map_kind(ErrorKind::Interface)?;

        debug!("unsetting property");

        let property_mapping = PropertyMapping::from(&mapping);

        let meta = self
            .state
            .store()
            .unset_prop(
                &property_mapping,
                self.state.property_ctx().next_updated_at(),
            )
            .await
            .map_kind(ErrorKind::Store)?;

        let Some(epoch) = meta.epoch() else {
            debug!("property was already unset, no need to send it again");

            return Ok(());
        };

        match self.state.connection() {
            ConnStatus::Online => {
                self.send_timeout(Validated::Unset {
                    epoch,
                    data: validated,
                })
                .await?;

                debug!("unset sent");
            }
            ConnStatus::Offline | ConnStatus::Connected { .. } => {
                trace!("not deleting property from store, since disconnected");
            }
            ConnStatus::Disconnect | ConnStatus::Closed => {
                return Err(Error::with(
                    ErrorKind::Disconnected,
                    "while unsetting property",
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::client::tests::mock_client;
    use crate::store::{Prop, PropertyMapping, PropertyStore};
    use crate::test::{E2E_DEVICE_PROPERTY, E2E_DEVICE_PROPERTY_NAME};
    use crate::{AstarteData, Client};

    #[tokio::test]
    async fn send_property_connected() {
        let mut client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Online);

        let path = "/sensor_1/longinteger_endpoint";
        let value = AstarteData::LongInteger(42);

        // Send
        client
            .set_property(E2E_DEVICE_PROPERTY_NAME, path, value.clone())
            .await
            .unwrap();

        let prop = {
            let interfaces = client.state.interfaces().read().await;
            let path = MappingPath::try_from(path).unwrap();
            let mapping = interfaces
                .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
                .unwrap();

            client
                .state
                .store()
                .load_prop(&PropertyMapping::from(&mapping))
                .await
                .unwrap()
                .unwrap()
        };
        assert_eq!(prop.value, value);

        let res = client.client_rx.try_recv().unwrap();
        assert_eq!(
            res,
            Validated::Property {
                epoch: prop.epoch(),
                data: ValidatedProperty {
                    interface: E2E_DEVICE_PROPERTY_NAME.to_string(),
                    path: path.to_string(),
                    version_major: 0,
                    data: value.clone(),
                }
            }
        );
    }

    #[tokio::test]
    async fn send_property_offline() {
        let client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Offline);

        let path = "/sensor_1/longinteger_endpoint";
        let value = AstarteData::LongInteger(42);

        // Send
        client
            .set_property(E2E_DEVICE_PROPERTY_NAME, path, value.clone())
            .await
            .unwrap();

        assert!(client.client_rx.is_empty());

        let interfaces = client.state.interfaces().read().await;
        let path = MappingPath::try_from(path).unwrap();
        let mapping = interfaces
            .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap();

        let prop = client
            .state
            .store()
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(prop.value, value);
    }

    #[tokio::test]
    async fn send_property_connected_already_stored() {
        let client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Online);

        let path = "/sensor_1/longinteger_endpoint";
        let value = AstarteData::LongInteger(42);

        // No expect, but store the prop
        client
            .state
            .store()
            .store_prop(Prop {
                interface: E2E_DEVICE_PROPERTY_NAME.to_string(),
                path: path.to_string(),
                value: value.clone(),
                interface_major: 0,
                ownership: Ownership::Device,
                updated_at: client.state.property_ctx().next_updated_at(),
            })
            .await
            .unwrap();

        // Send
        client
            .set_property(E2E_DEVICE_PROPERTY_NAME, path, value.clone())
            .await
            .unwrap();

        assert!(client.client_rx.is_empty());

        let interfaces = client.state.interfaces().read().await;
        let path = MappingPath::try_from(path).unwrap();
        let mapping = interfaces
            .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap();

        let prop = client
            .state
            .store()
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(prop.value, value);
    }

    #[tokio::test]
    async fn unset_property_connected_already_stored() {
        let client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Online);

        let path = "/sensor_1/longinteger_endpoint";

        // Send
        client
            .unset_property(E2E_DEVICE_PROPERTY_NAME, path)
            .await
            .unwrap();

        let prop = {
            let interfaces = client.state.interfaces().read().await;
            let path = MappingPath::try_from(path).unwrap();
            let mapping = interfaces
                .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
                .unwrap();

            client
                .state
                .store()
                .load_prop(&PropertyMapping::from(&mapping))
                .await
                .unwrap()
        };

        assert_eq!(prop, None);
    }

    #[tokio::test]
    async fn unset_property_offline_already_stored() {
        let client = mock_client(&[E2E_DEVICE_PROPERTY], ConnStatus::Offline);

        let path = "/sensor_1/longinteger_endpoint";

        // Send
        client
            .unset_property(E2E_DEVICE_PROPERTY_NAME, path)
            .await
            .unwrap();

        assert!(client.client_rx.is_empty());

        let prop = {
            let interfaces = client.state.interfaces().read().await;
            let path = MappingPath::try_from(path).unwrap();
            let mapping = interfaces
                .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
                .unwrap();

            client
                .state
                .store()
                .load_prop(&PropertyMapping::from(&mapping))
                .await
                .unwrap()
        };

        assert_eq!(prop, None);
    }
}
