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

//! # Astarte GRPC Store Module
//!
//! This module provides an implementation of the PropertyStore.
//! It defines the `GrpcStore` struct, which handles device properties differently than server
//! properties, to allow retrieving all device properties directly from the message hub server.

use std::sync::Arc;

use astarte_device_error::Error;
use astarte_device_error::ResultExt;
use astarte_device_error::WrapError;
use astarte_interfaces::Properties;
use astarte_interfaces::Schema;
use astarte_interfaces::schema::Ownership;
use astarte_message_hub_proto::PropertyFilter;
use futures::TryFutureExt;
use tokio::sync::Mutex;
use tracing::error;

use crate::error::Report;
use crate::store::PropertyState;
use crate::store::error::StoreError;
use crate::{
    AstarteData,
    store::{OptStoredProp, PropertyMapping, PropertyStore, StoreCapabilities, StoredProp},
};

use super::MsgHubClient;
use super::convert;
use super::error::GrpcError;

// Store implementation designed specifically for the grpc connection
/// Used mainly to request device owned properties to the message hub instead of looking them up in the local storage
#[derive(Debug, Clone)]
pub struct GrpcStore<S> {
    client: Arc<Mutex<MsgHubClient>>,
    inner: S,
}

impl<S> GrpcStore<S> {
    pub(crate) fn new(client: MsgHubClient, store: S) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
            inner: store,
        }
    }

    async fn grpc_server_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> Result<Option<AstarteData>, Error<GrpcError>> {
        self.client
            .lock()
            .await
            .get_property(astarte_message_hub_proto::PropertyIdentifier {
                interface_name: property.interface_name().to_string(),
                path: property.path().to_owned(),
            })
            .await
            .map_err(|status| {
                Error::with(GrpcError::Status, "getting server property").set_message(status)
            })
            .and_then(|resp| {
                resp.into_inner()
                    .data
                    .map(|data| AstarteData::try_from(data).map_kind(GrpcError::Conversion))
                    .transpose()
            })
    }

    async fn grpc_server_properties(&self) -> Result<Vec<StoredProp>, Error<GrpcError>> {
        self.client
            .lock()
            .await
            .get_all_properties(PropertyFilter {
                ownership: Some(astarte_message_hub_proto::Ownership::Server.into()),
            })
            .await
            .map_err(|status| {
                Error::with(GrpcError::Status, "getting server properties").set_message(status)
            })
            .and_then(|resp| {
                let resp = resp.into_inner();

                convert::map_set_stored_properties(resp).map_kind(GrpcError::Conversion)
            })
    }

    async fn grpc_interface_properties(
        &self,
        interface: &Properties,
    ) -> Result<Vec<StoredProp>, Error<GrpcError>> {
        self.client
            .lock()
            .await
            .get_properties(astarte_message_hub_proto::InterfaceName {
                name: interface.interface_name().to_string(),
            })
            .await
            .map_err(|status| {
                Error::with(GrpcError::Status, "getting interface properties").set_message(status)
            })
            .and_then(|resp| {
                let resp = resp.into_inner();

                convert::map_set_stored_properties(resp).map_kind(GrpcError::Conversion)
            })
    }
}

impl<S> StoreCapabilities for GrpcStore<S>
where
    S: StoreCapabilities,
{
    type Retention = S::Retention;
    type Session = S::Session;

    fn get_retention(&self) -> Option<&Self::Retention> {
        self.inner.get_retention()
    }

    fn get_session(&self) -> Option<&Self::Session> {
        self.inner.get_session()
    }
}

/// We implement the PropertyStore to override the behavior when retrieving or storing
/// owned properties. Currently we store all properties locally but only return device owned properties.
/// Server owned properties are retrieved from the message hub server if online otherwise the last locally
/// stored value is returned.
impl<S> PropertyStore for GrpcStore<S>
where
    S: PropertyStore,
{
    async fn store_prop(
        &self,
        prop: StoredProp<&str, &AstarteData>,
    ) -> Result<(), Error<StoreError>> {
        self.inner.store_prop(prop).await
    }

    async fn update_state(
        &self,
        property: &PropertyMapping<'_>,
        state: PropertyState,
        expected: Option<AstarteData>,
    ) -> Result<bool, Error<StoreError>> {
        self.inner.update_state(property, state, expected).await
    }

    async fn load_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> Result<Option<AstarteData>, Error<StoreError>> {
        if property.ownership() == Ownership::Server {
            let server_prop = self.grpc_server_prop(property).await;

            match server_prop {
                Ok(p) => {
                    return Ok(p);
                }
                Err(e) => {
                    // NOTE if an error occurs we'll try to load a server property from the local store
                    error!(error = %Report::new(e), "error while requesting server property, returning stored server property");
                }
            }
        }

        self.inner.load_prop(property).await
    }

    async fn unset_prop(&self, prop: &PropertyMapping<'_>) -> Result<(), Error<StoreError>> {
        self.inner.unset_prop(prop).await
    }

    async fn delete_prop(&self, prop: &PropertyMapping<'_>) -> Result<(), Error<StoreError>> {
        self.inner.delete_prop(prop).await
    }

    async fn delete_expected_prop(
        &self,
        property: &PropertyMapping<'_>,
        expected: Option<AstarteData>,
    ) -> Result<bool, Error<StoreError>> {
        self.inner.delete_expected_prop(property, expected).await
    }

    async fn clear(&self) -> Result<(), Error<StoreError>> {
        self.inner.clear().await
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Error<StoreError>> {
        let mut device_props = self.inner.device_props().await?;

        let server_props = self.server_props().await.wrap_err(StoreError::LoadAll)?;

        device_props.extend(server_props);

        Ok(device_props)
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Error<StoreError>> {
        let inner = self.inner.clone();

        self.grpc_server_properties()
            .or_else(move |e| {
                error!(error=%Report::new(e), "error while requesting server properties, returning stored server properties");

                async move {
                    inner.server_props().await.wrap_err(StoreError::ServerProps)
                }
            })
            .await
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Error<StoreError>> {
        self.inner.device_props().await
    }

    async fn interface_props(
        &self,
        interface: &Properties,
    ) -> Result<Vec<StoredProp>, Error<StoreError>> {
        if interface.ownership() == Ownership::Server {
            let server_prop = self.grpc_interface_properties(interface).await;

            match server_prop {
                Ok(p) => {
                    return Ok(p);
                }
                Err(e) => {
                    // NOTE if an error occurs we'll try to load a server property from the local store
                    error!(error = %Report::new(e), "error while requesting server properties, returning stored server properties");
                }
            }
        }

        self.inner.interface_props(interface).await
    }

    async fn delete_interface(&self, interface: &Properties) -> Result<(), Error<StoreError>> {
        self.inner.delete_interface(interface).await
    }

    async fn device_props_with_unset(
        &self,
        state: PropertyState,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<OptStoredProp>, Error<StoreError>> {
        self.inner
            .device_props_with_unset(state, limit, offset)
            .await
    }

    async fn reset_state(&self, ownership: Ownership) -> Result<(), Error<StoreError>> {
        self.inner.reset_state(ownership).await
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use astarte_interfaces::MappingPath;
    use astarte_interfaces::Properties;
    use astarte_interfaces::Schema;
    use astarte_interfaces::schema::Ownership;
    use astarte_message_hub_proto::PropertyFilter;
    use astarte_message_hub_proto::PropertyIdentifier;
    use astarte_message_hub_proto::tonic;
    use astarte_message_hub_proto_mock::mockall::{Sequence, predicate};

    use super::GrpcStore;
    use super::MsgHubClient;
    use super::PropertyStore;
    use crate::AstarteData;
    use crate::interfaces::MappingRef;
    use crate::store::PropertyMapping;
    use crate::store::StoredProp;
    use crate::store::memory::MemoryStore;
    use crate::test::DEVICE_PROPERTIES;
    use crate::test::E2E_DEVICE_PROPERTY;
    use crate::test::E2E_DEVICE_PROPERTY_NAME;
    use crate::test::E2E_SERVER_PROPERTY;
    use crate::test::E2E_SERVER_PROPERTY_NAME;
    use crate::test::SERVER_PROPERTIES;
    use crate::test::SERVER_PROPERTIES_NAME;

    #[tokio::test]
    async fn test_grpc_store_grpc_client_calls() {
        let device_interface = Properties::from_str(DEVICE_PROPERTIES).unwrap();
        let server_interface = Properties::from_str(SERVER_PROPERTIES).unwrap();

        const DEVICE_PATH: &str = "/sensor_1/name";
        const SERVER_PATH: &str = "/sensor_1/enable";

        let mut seq = Sequence::new();
        let mut mock_store_client = MsgHubClient::new();
        // server
        mock_store_client
            .expect_get_property::<astarte_message_hub_proto::PropertyIdentifier>()
            .times(1)
            .in_sequence(&mut seq)
            .with(predicate::eq(PropertyIdentifier {
                interface_name: SERVER_PROPERTIES_NAME.to_string(),
                path: SERVER_PATH.to_string(),
            }))
            .returning(|_i| {
                Ok(tonic::Response::new(
                    astarte_message_hub_proto::AstartePropertyIndividual { data: None },
                ))
            });
        // server
        mock_store_client
            .expect_get_all_properties::<PropertyFilter>()
            .times(1)
            .in_sequence(&mut seq)
            .with(predicate::function(|r: &PropertyFilter| {
                r.ownership == Some(astarte_message_hub_proto::Ownership::Server as i32)
            }))
            .returning(|_i| {
                Ok(tonic::Response::new(
                    astarte_message_hub_proto::StoredProperties {
                        properties: Vec::new(),
                    },
                ))
            });
        // server
        mock_store_client
            .expect_get_properties::<astarte_message_hub_proto::InterfaceName>()
            .times(1)
            .in_sequence(&mut seq)
            .with(predicate::eq(astarte_message_hub_proto::InterfaceName {
                name: server_interface.interface_name().to_string(),
            }))
            .returning(|_i| {
                Ok(tonic::Response::new(
                    astarte_message_hub_proto::StoredProperties {
                        properties: Vec::new(),
                    },
                ))
            });

        let grpc_store = GrpcStore::new(mock_store_client, MemoryStore::new());

        let device_mapping_path = MappingPath::try_from(DEVICE_PATH).unwrap();
        let server_mapping_path = MappingPath::try_from(SERVER_PATH).unwrap();

        let device_mapping_ref = MappingRef::new(&device_interface, &device_mapping_path).unwrap();
        let device_prop_info = PropertyMapping::from(&device_mapping_ref);
        // the server should be called
        let _device_prop = grpc_store.load_prop(&device_prop_info).await;

        let server_mapping_ref = MappingRef::new(&server_interface, &server_mapping_path).unwrap();
        let server_prop_info = PropertyMapping::from(&server_mapping_ref);
        // the server should be called
        let _server_prop = grpc_store.load_prop(&server_prop_info).await;

        // the server should be called
        let _device_properties = grpc_store.device_props().await.unwrap();
        // the server should be called
        let _server_properties = grpc_store.server_props().await.unwrap();

        let device_interface = Properties::from_str(DEVICE_PROPERTIES).unwrap();
        // the server should be called
        let _device_interface_properties =
            grpc_store.interface_props(&device_interface).await.unwrap();

        let server_interface = Properties::from_str(SERVER_PROPERTIES).unwrap();
        // the server should be called
        let _server_interface_properties =
            grpc_store.interface_props(&server_interface).await.unwrap();
    }

    #[tokio::test]
    async fn test_grpc_store_device_prop_not_stored() {
        let device_itf = Properties::from_str(E2E_DEVICE_PROPERTY).unwrap();
        let server_itf = Properties::from_str(E2E_SERVER_PROPERTY).unwrap();

        let value = AstarteData::Integer(1);
        const PATH: &str = "/sensor_1/integer_endpoint";

        let server_prop = StoredProp {
            interface: E2E_SERVER_PROPERTY_NAME,
            path: PATH,
            value: &value,
            interface_major: 0,
            ownership: Ownership::Server,
        };
        let server_prop_mapping = PropertyMapping::from(&server_prop);

        let device_prop = StoredProp {
            interface: E2E_DEVICE_PROPERTY_NAME,
            path: PATH,
            value: &value,
            interface_major: 0,
            ownership: Ownership::Device,
        };
        let device_interface_data = &(&device_prop).into();

        let mock_store_client = MsgHubClient::new();
        let grpc_store = GrpcStore::new(mock_store_client, MemoryStore::new());

        // we do not store anything locally so no action should be performed

        // no actions or calls to the server should be performed
        grpc_store.store_prop(server_prop).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.store_prop(device_prop).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.unset_prop(&server_prop_mapping).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.unset_prop(device_interface_data).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.delete_prop(&server_prop_mapping).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.delete_prop(device_interface_data).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.delete_interface(&server_itf).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.delete_interface(&device_itf).await.unwrap();
    }
}
