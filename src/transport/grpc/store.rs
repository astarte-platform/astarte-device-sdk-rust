/*
 * This file is part of Astarte.
 *
 * Copyright 2025 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

//! # Astarte GRPC Store Module
//!
//! This module provides an implementation of the PropertyStore.
//! It defines the `GrpcStore` struct, which handles device properties differently than server
//! properties, to allow retrieving all device properties directly from the message hub server.

use std::sync::Arc;

use astarte_message_hub_proto::tonic;
use tokio::sync::Mutex;

use crate::{
    retention::Missing,
    store::{
        OptStoredProp, PropertyInterface, PropertyMapping, PropertyStore, StoreCapabilities,
        StoredProp,
    },
    AstarteType,
};

use super::{
    convert::{self, MessageHubProtoError},
    MsgHubClient,
};

// Store implementation designed specifically for the grpc connection
/// Used mainly to request device owned properties to the message hub instead of looking them up in the local storage
#[derive(Debug, Clone)]
pub struct GrpcStore {
    pub(crate) client: Arc<Mutex<MsgHubClient>>,
}

impl GrpcStore {
    pub(crate) fn new(client: MsgHubClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
        }
    }

    async fn load_properties(
        &self,
        ownership: Option<astarte_message_hub_proto::Ownership>,
    ) -> Result<Vec<StoredProp>, GrpcStoreError> {
        self.client
            .lock()
            .await
            .get_all_properties(astarte_message_hub_proto::StoredPropertiesFilter {
                ownership: ownership.map(|o| o.into()),
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
}

impl From<tonic::Status> for GrpcStoreError {
    fn from(value: tonic::Status) -> Self {
        Self::Status(value)
    }
}

impl StoreCapabilities for GrpcStore {
    type Retention = Missing;

    fn get_retention(&self) -> Option<&Self::Retention> {
        None
    }
}

/// We implement the PropertyStore to override the behavior when retrieving or storing
/// owned properties. Currently we do not store properties locally.
impl PropertyStore for GrpcStore {
    type Err = GrpcStoreError;

    async fn store_prop(&self, _prop: StoredProp<&str, &AstarteType>) -> Result<(), Self::Err> {
        // do not store properties locally when connected as a message hub node
        Ok(())
    }

    async fn load_prop(
        &self,
        property: &PropertyMapping<'_>,
        _interface_major: i32,
    ) -> Result<Option<AstarteType>, Self::Err> {
        let property = self
            .client
            .lock()
            .await
            .get_property(astarte_message_hub_proto::PropertyIdentifier {
                interface_name: property.name().to_owned(),
                path: property.path().to_owned(),
            })
            .await
            .map_err(GrpcStoreError::from)
            .map(tonic::Response::into_inner)?;

        Ok(convert::map_property_to_astarte_type(property)?)
    }

    async fn unset_prop(&self, _property: &PropertyMapping<'_>) -> Result<(), Self::Err> {
        // do not store properties locally when connected as a message hub node
        Ok(())
    }

    async fn delete_prop(&self, _interface: &PropertyMapping<'_>) -> Result<(), Self::Err> {
        // do not store properties locally when connected as a message hub node
        Ok(())
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        // the store is always empty
        Ok(())
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.load_properties(None).await
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.load_properties(Some(astarte_message_hub_proto::Ownership::Server))
            .await
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.load_properties(Some(astarte_message_hub_proto::Ownership::Device))
            .await
    }

    async fn interface_props(
        &self,
        interface: &PropertyInterface<'_>,
    ) -> Result<Vec<StoredProp>, Self::Err> {
        self.client
            .lock()
            .await
            .get_properties(astarte_message_hub_proto::InterfacesName {
                names: vec![interface.name().to_owned()],
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(GrpcStoreError::from)
            .and_then(|p| Ok(convert::map_set_stored_properties(p)?))
    }

    async fn delete_interface(&self, _interface: &PropertyInterface<'_>) -> Result<(), Self::Err> {
        // do not store properties locally when connected as a message hub node
        Ok(())
    }

    async fn device_props_with_unset(&self) -> Result<Vec<OptStoredProp>, Self::Err> {
        // unused for grpc connection
        Ok(vec![])
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use astarte_message_hub_proto::pbjson_types::Empty;
    use astarte_message_hub_proto::InterfacesName;
    use astarte_message_hub_proto::PropertyIdentifier;
    use astarte_message_hub_proto::StoredPropertiesFilter;

    use super::GrpcStore;
    use super::PropertyStore;
    use crate::interface::Ownership;
    use crate::store::PropertyMapping;
    use crate::store::StoredProp;
    use crate::transport::grpc::test::build_test_message_hub_server;
    use crate::transport::grpc::test::expect_messages;
    use crate::transport::grpc::test::ServerReceivedRequest;
    use crate::transport::grpc::test::ID;
    use crate::transport::Disconnect;
    use crate::transport::{
        grpc::test::{mock_astarte_grpc_client, mock_grpc_actors},
        Interfaces,
    };
    use crate::Interface;

    #[tokio::test]
    async fn test_grpc_store_grpc_client_calls() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        channels.server_response_sender.send(vec![]).await.unwrap();

        let client_operations = async move {
            let client = client_future.await;
            let grpc_store = GrpcStore::new(client.clone());
            // When the grpc connection gets created the attach methods is called
            let (mut client, _connection) =
                mock_astarte_grpc_client(client.clone(), &Interfaces::new(), grpc_store.clone())
                    .await
                    .unwrap();

            let _device_properties = grpc_store.device_props().await.unwrap();
            let _server_properties = grpc_store.server_props().await.unwrap();

            let device_interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();
            let _device_interface_properties = grpc_store
                .interface_props(&(&device_interface).into())
                .await
                .unwrap();

            let server_interface = Interface::from_str(crate::test::SERVER_PROPERTIES).unwrap();
            let _server_interface_properties = grpc_store
                .interface_props(&(&server_interface).into())
                .await
                .unwrap();

            let device_path = "/path1";
            let device_prop_info =
                PropertyMapping::new_unchecked((&device_interface).into(), device_path);
            let _device_prop = grpc_store.load_prop(&device_prop_info, 1).await;
            // no request should be made
            let server_path = "/path1";
            let server_prop_info =
                PropertyMapping::new_unchecked((&server_interface).into(), server_path);
            let _server_prop = grpc_store.load_prop(&server_prop_info, 1).await;

            // manually calling detach
            client.disconnect().await.unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        let device_interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();
        let server_interface = Interface::from_str(crate::test::SERVER_PROPERTIES).unwrap();
        expect_messages!(channels.server_request_receiver.try_recv();
            // connection creation attach
            ServerReceivedRequest::Attach((id, _)) if id == ID,
            // load device properties should call the message hub
            ServerReceivedRequest::GetAllProperties(StoredPropertiesFilter {
                ownership: Some(ownership),
            }) if ownership == astarte_message_hub_proto::Ownership::Device as i32,
            // load server properties should call the message hub
            ServerReceivedRequest::GetAllProperties(StoredPropertiesFilter {
                ownership: Some(ownership),
            }) if ownership == astarte_message_hub_proto::Ownership::Server as i32,
            // if a property interface is device owned the message hub should be called
            ServerReceivedRequest::GetProperties(InterfacesName {
                names,
            }) if names.len() == 1 && names.first().unwrap() == device_interface.interface_name(),
            // if a property interface is server owned the message hub should also be called
            ServerReceivedRequest::GetProperties(InterfacesName {
                names,
            }) if names.len() == 1 && names.first().unwrap() == server_interface.interface_name(),
            // loading a device property from the message hub
            ServerReceivedRequest::GetProperty(PropertyIdentifier {
                interface_name,
                path,
            }) if interface_name == device_interface.interface_name() && path == "/path1",
            // loading a server property from the message hub
            ServerReceivedRequest::GetProperty(PropertyIdentifier {
                interface_name,
                path,
            }) if interface_name == server_interface.interface_name() && path == "/path1",
            // detach
            ServerReceivedRequest::Detach(Empty {}),
        );
    }

    #[tokio::test]
    async fn test_grpc_store_device_prop_not_stored() {
        let (server_impl, mut _channels) = build_test_message_hub_server();
        let (_server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        let client = client_future.await;
        let grpc_store = GrpcStore::new(client);

        let inner_value = crate::AstarteType::Integer(1);
        let path = "/path1";
        let server_interface = "com.server.interface";
        let server_prop = StoredProp {
            interface: server_interface,
            path,
            value: &inner_value,
            interface_major: 1,
            ownership: Ownership::Server,
        };
        let server_interface_data = &(&server_prop).into();
        let device_interface = "com.device.interface";
        let device_prop = StoredProp {
            interface: device_interface,
            path,
            value: &inner_value,
            interface_major: 1,
            ownership: Ownership::Device,
        };
        let device_interface_data = &(&device_prop).into();

        // no actions or calls to the server should be performed
        grpc_store.store_prop(server_prop).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.store_prop(device_prop).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.delete_prop(server_interface_data).await.unwrap();
        // no actions or calls to the server should be performed
        grpc_store.delete_prop(device_interface_data).await.unwrap();
    }
}
