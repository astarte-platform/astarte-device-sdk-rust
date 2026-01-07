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

use astarte_interfaces::Properties;
use astarte_interfaces::Schema;
use astarte_interfaces::schema::Ownership;
use astarte_message_hub_proto::PropertyFilter;
use astarte_message_hub_proto::tonic;
use futures::TryFutureExt;
use tokio::sync::Mutex;
use tracing::error;

use crate::error::Report;
use crate::store::error::StoreError;
use crate::{
    AstarteData,
    store::{OptStoredProp, PropertyMapping, PropertyStore, StoreCapabilities, StoredProp},
};

use super::{
    MsgHubClient,
    convert::{self, MessageHubProtoError},
};

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
    ) -> Result<Option<AstarteData>, GrpcStoreError> {
        self.client
            .lock()
            .await
            .get_property(astarte_message_hub_proto::PropertyIdentifier {
                interface_name: property.interface_name().to_string(),
                path: property.path().to_owned(),
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(GrpcStoreError::from)
            .and_then(|i| {
                i.data
                    .map(|data| AstarteData::try_from(data).map_err(GrpcStoreError::from))
                    .transpose()
            })
    }

    async fn grpc_server_properties(&self) -> Result<Vec<StoredProp>, GrpcStoreError> {
        self.client
            .lock()
            .await
            .get_all_properties(PropertyFilter {
                ownership: Some(astarte_message_hub_proto::Ownership::Server.into()),
            })
            .await
            .map_err(GrpcStoreError::from)
            .and_then(|res| Ok(convert::map_set_stored_properties(res.into_inner())?))
    }

    async fn grpc_interface_properties(
        &self,
        interface: &Properties,
    ) -> Result<Vec<StoredProp>, GrpcStoreError> {
        self.client
            .lock()
            .await
            .get_properties(astarte_message_hub_proto::InterfaceName {
                name: interface.interface_name().to_string(),
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(GrpcStoreError::from)
            .and_then(|p| Ok(convert::map_set_stored_properties(p)?))
    }
}

/// Error returned while operating on the store of a grpc connection
/// This store needs to request properties from the message hub server
/// and has additional errors consequently
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum GrpcStoreError {
    /// Error while retrieving data from the message hub server
    #[error("Error while retrieving data from the message hub server: {0}")]
    Status(tonic::Status),
    /// Error while converting a proto received value to an internal type
    #[error("Error while converting a proto received value to an internal type: {0}")]
    Conversion(#[from] MessageHubProtoError),
    /// Error of the inner store
    #[error("Inner store error")]
    StoreError(#[from] StoreError),
}

impl From<tonic::Status> for GrpcStoreError {
    fn from(value: tonic::Status) -> Self {
        Self::Status(value)
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
    type Err = GrpcStoreError;

    async fn store_prop(&self, prop: StoredProp<&str, &AstarteData>) -> Result<(), Self::Err> {
        self.inner
            .store_prop(prop)
            .await
            .map_err(StoreError::store)?;

        Ok(())
    }

    async fn load_prop(
        &self,
        property: &PropertyMapping<'_>,
    ) -> Result<Option<AstarteData>, Self::Err> {
        if property.ownership() == Ownership::Server {
            let server_prop = self.grpc_server_prop(property).await;

            match server_prop {
                Ok(p) => {
                    return Ok(p);
                }
                Err(e) => {
                    // NOTE if an error occurs we'll try to load a server property from the local store
                    error!(err=%Report::new(e), "error while requesting server property, returning stored server property");
                }
            }
        }

        self.inner
            .load_prop(property)
            .await
            .map_err(|e| GrpcStoreError::StoreError(StoreError::load(e)))
    }

    async fn unset_prop(&self, prop: &PropertyMapping<'_>) -> Result<(), Self::Err> {
        self.inner
            .unset_prop(prop)
            .await
            .map_err(StoreError::unset)?;

        Ok(())
    }

    async fn delete_prop(&self, prop: &PropertyMapping<'_>) -> Result<(), Self::Err> {
        self.inner
            .delete_prop(prop)
            .await
            .map_err(StoreError::delete)?;

        Ok(())
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        self.inner.clear().await.map_err(StoreError::clear)?;

        Ok(())
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let mut device_props = self
            .inner
            .device_props()
            .await
            .map_err(StoreError::device_props)?;

        let server_props = self.server_props().await?;

        device_props.extend(server_props);

        Ok(device_props)
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let inner = self.inner.clone();

        self.grpc_server_properties()
            .or_else(move |e| {
                error!(err=%Report::new(e), "error while requesting server properties, returning stored server properties");

                async move {
                    inner.server_props().await.map_err(|e| GrpcStoreError::from(StoreError::device_props(e)))
                }
            })
            .await
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.inner
            .device_props()
            .await
            .map_err(|e| GrpcStoreError::from(StoreError::device_props(e)))
    }

    async fn interface_props(&self, interface: &Properties) -> Result<Vec<StoredProp>, Self::Err> {
        if interface.ownership() == Ownership::Server {
            let server_prop = self.grpc_interface_properties(interface).await;

            match server_prop {
                Ok(p) => {
                    return Ok(p);
                }
                Err(e) => {
                    // NOTE if an error occurs we'll try to load a server property from the local store
                    error!(err=%Report::new(e), "error while requesting server properties, returning stored server properties");
                }
            }
        }

        self.inner
            .interface_props(interface)
            .await
            .map_err(|e| GrpcStoreError::from(StoreError::interface_props(e)))
    }

    async fn delete_interface(&self, interface: &Properties) -> Result<(), Self::Err> {
        self.inner
            .delete_interface(interface)
            .await
            .map_err(StoreError::delete_interface)?;

        Ok(())
    }

    async fn device_props_with_unset(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<OptStoredProp>, Self::Err> {
        self.inner
            .device_props_with_unset(limit, offset)
            .await
            .map_err(|e| GrpcStoreError::from(StoreError::device_props(e)))
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
