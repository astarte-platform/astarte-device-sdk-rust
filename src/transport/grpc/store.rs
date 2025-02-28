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
    store::{InterfaceInfo, OptStoredProp, PropertyStore, StoreCapabilities, StoredProp},
    AstarteType,
};

use super::{
    convert::{self, MessageHubProtoError},
    MsgHubClient,
};

// Store implementation designed specifically for the grpc connection
/// Used mainly to request device owned properties to the message hub instead of looking them up in the local storage
#[derive(Debug, Clone)]
pub struct GrpcStore<S> {
    pub(crate) store: S,
    pub(crate) client: Arc<Mutex<MsgHubClient>>,
}

impl<S> GrpcStore<S> {
    pub(crate) fn new(store: S, client: MsgHubClient) -> Self {
        Self {
            store,
            client: Arc::new(Mutex::new(client)),
        }
    }

    async fn load_device_properties(&self) -> Result<Vec<StoredProp>, GrpcStoreError<S::Err>>
    where
        S: PropertyStore,
    {
        self.client
            .lock()
            .await
            .get_all_properties(astarte_message_hub_proto::StoredPropertiesFilter {
                ownership: Some(astarte_message_hub_proto::Ownership::Device.into()),
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(GrpcStoreError::from_status)
            .and_then(|p| {
                convert::map_set_stored_properties(p).map_err(GrpcStoreError::from_conversion)
            })
    }
}

/// Error returned while operating on the store of a grpc connection
/// This store needs to request properties from the message hub server
/// and has additional errors consequently
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum GrpcStoreError<E> {
    /// Underlying store error
    #[error("Underlying store error: {0}")]
    Store(E),
    /// Error while retrieving data from the message hub server
    #[error("Error while retrieving data from the message hub server: {0}")]
    Status(tonic::Status),
    /// Error while converting a proto received value to an internal type
    #[error("Error while converting a proto received value to an internal type: {0}")]
    Conversion(MessageHubProtoError),
}

impl<E> GrpcStoreError<E> {
    fn from_err(err: E) -> Self {
        Self::Store(err)
    }

    fn from_status(status: tonic::Status) -> Self {
        Self::Status(status)
    }

    fn from_conversion(conversion: MessageHubProtoError) -> Self {
        Self::Conversion(conversion)
    }
}

impl<S> StoreCapabilities for GrpcStore<S>
where
    S: StoreCapabilities,
{
    type Retention = S::Retention;

    fn get_retention(&self) -> Option<&Self::Retention> {
        self.store.get_retention()
    }
}

/// We implement the PropertyStore to override the behavior when retrieving or storing
/// device owned properies. We only want to store and load from the store server owned properties.
/// Device owned properties won't be stored and will be requested to the message hub server.
impl<S> PropertyStore for GrpcStore<S>
where
    S: PropertyStore,
    S::Err: std::fmt::Display,
{
    type Err = GrpcStoreError<S::Err>;

    async fn store_prop(&self, prop: StoredProp<&str, &AstarteType>) -> Result<(), Self::Err> {
        if prop.ownership.is_device() {
            // do not store device owned properties
            return Ok(());
        }

        self.store
            .store_prop(prop)
            .await
            .map_err(GrpcStoreError::from_err)
    }

    async fn load_prop(
        &self,
        interface: &InterfaceInfo<'_>,
        path: &str,
        interface_major: i32,
    ) -> Result<Option<AstarteType>, Self::Err> {
        if interface.ownership.is_server() {
            return self
                .store
                .load_prop(interface, path, interface_major)
                .await
                .map_err(GrpcStoreError::from_err);
        }

        let property = self
            .client
            .lock()
            .await
            .get_property(astarte_message_hub_proto::PropertyIdentifier {
                interface_name: interface.name.to_owned(),
                path: path.to_owned(),
            })
            .await
            .map_err(GrpcStoreError::from_status)
            .map(tonic::Response::into_inner)?;

        convert::map_property_to_astarte_type(property).map_err(GrpcStoreError::from_conversion)
    }

    async fn unset_prop(&self, interface: &InterfaceInfo<'_>, path: &str) -> Result<(), Self::Err> {
        if interface.ownership.is_device() {
            // we won't store a device unset since we always request the properties to the server
            return Ok(());
        }

        self.store
            .unset_prop(interface, path)
            .await
            .map_err(GrpcStoreError::from_err)
    }

    async fn delete_prop(
        &self,
        interface: &InterfaceInfo<'_>,
        path: &str,
    ) -> Result<(), Self::Err> {
        if interface.ownership.is_device() {
            // we won't delete a device property since we always request the properties to the server
            return Ok(());
        }

        self.store
            .delete_prop(interface, path)
            .await
            .map_err(GrpcStoreError::from_err)
    }

    async fn clear(&self) -> Result<(), Self::Err> {
        self.store.clear().await.map_err(GrpcStoreError::from_err)
    }

    async fn load_all_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        let load_server = async {
            self.store
                .server_props()
                .await
                .map_err(GrpcStoreError::from_err)
        };

        let (server_result, device_result) =
            futures::future::join(load_server, self.load_device_properties()).await;

        let mut merged_properties = server_result?;
        merged_properties.append(&mut device_result?);

        Ok(merged_properties)
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.store
            .server_props()
            .await
            .map_err(GrpcStoreError::from_err)
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Self::Err> {
        self.load_device_properties().await
    }

    async fn interface_props(
        &self,
        interface: &InterfaceInfo<'_>,
    ) -> Result<Vec<StoredProp>, Self::Err> {
        if interface.ownership.is_server() {
            return self
                .store
                .interface_props(interface)
                .await
                .map_err(GrpcStoreError::from_err);
        }

        self.client
            .lock()
            .await
            .get_properties(astarte_message_hub_proto::InterfacesName {
                names: vec![interface.name.to_owned()],
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(GrpcStoreError::from_status)
            .and_then(|p| {
                convert::map_set_stored_properties(p).map_err(GrpcStoreError::from_conversion)
            })
    }

    async fn delete_interface(&self, interface: &InterfaceInfo<'_>) -> Result<(), Self::Err> {
        if interface.ownership.is_device() {
            // we don't delete a device interface locally
            return Ok(());
        }

        self.store
            .delete_interface(interface)
            .await
            .map_err(GrpcStoreError::from_err)
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
    use crate::store::StoredProp;
    use crate::transport::grpc::test::build_test_message_hub_server;
    use crate::transport::grpc::test::expect_messages;
    use crate::transport::grpc::test::ServerReceivedRequest;
    use crate::transport::grpc::test::ID;
    use crate::transport::Disconnect;
    use crate::Interface;
    use crate::{
        store::memory::MemoryStore,
        transport::{
            grpc::test::{mock_astarte_grpc_client, mock_grpc_actors},
            Interfaces,
        },
    };

    #[tokio::test]
    async fn test_grpc_store_grpc_client_calls() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client_future) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");
        let base_store = MemoryStore::new();

        channels.server_response_sender.send(vec![]).await.unwrap();

        let client_operations = async move {
            let client = client_future.await;
            let grpc_store = GrpcStore::new(base_store, client.clone());
            // When the grpc connection gets created the attach methods is called
            let (mut client, _connection) =
                mock_astarte_grpc_client(client.clone(), &Interfaces::new(), grpc_store.clone())
                    .await
                    .unwrap();

            let _device_properties = grpc_store.device_props().await.unwrap();
            // no call should be preformed if calling server properties
            let _server_properties = grpc_store.server_props().await.unwrap();

            let device_interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();
            let _device_interface_properties = grpc_store
                .interface_props(&(&device_interface).into())
                .await
                .unwrap();
            // if you get the properties of a server interface no request should be made
            let server_interface = Interface::from_str(crate::test::SERVER_PROPERTIES).unwrap();
            let _server_interface_properties = grpc_store
                .interface_props(&(&server_interface).into())
                .await
                .unwrap();

            let _device_prop = grpc_store
                .load_prop(&(&device_interface).into(), "/path1", 1)
                .await;
            // no requst should be made
            let _server_prop = grpc_store
                .load_prop(&(&server_interface).into(), "/path1", 1)
                .await;

            // manually calling detach
            client.disconnect().await.unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        let device_interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();
        expect_messages!(channels.server_request_receiver.try_recv();
            // connection creation attach
            ServerReceivedRequest::Attach((id, _)) if id == ID,
            // load device properties should call the message hub
            ServerReceivedRequest::GetAllProperties(StoredPropertiesFilter {
                ownership: Some(ownership),
            }) if ownership == astarte_message_hub_proto::Ownership::Device as i32,
            // if a property interface is device owned the message hub should be called
            ServerReceivedRequest::GetProperties(InterfacesName {
                names,
            }) if names.len() == 1 && names.first().unwrap() == device_interface.interface_name(),
            // loading a device property from the message hub
            ServerReceivedRequest::GetProperty(PropertyIdentifier {
                interface_name,
                path,
            }) if interface_name == device_interface.interface_name() && path == "/path1",
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

        let base_store = MemoryStore::new();
        let client = client_future.await;
        let grpc_store = GrpcStore::new(base_store.clone(), client);

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

        // test store server owned property
        grpc_store.store_prop(server_prop).await.unwrap();
        let prop = base_store
            .load_prop(server_interface_data, path, 1)
            .await
            .unwrap();
        assert_eq!(prop, Some(inner_value.clone()));
        // test store device owned property
        grpc_store.store_prop(device_prop).await.unwrap();
        let prop = base_store
            .load_prop(device_interface_data, path, 1)
            .await
            .unwrap();
        assert!(prop.is_none());

        // cleanup
        base_store.clear().await.unwrap();

        // delete server owned should delete the underlying prop
        base_store.store_prop(server_prop).await.unwrap();
        grpc_store
            .delete_prop(server_interface_data, path)
            .await
            .unwrap();
        let prop = base_store
            .load_prop(server_interface_data, path, 1)
            .await
            .unwrap();
        assert!(prop.is_none());
        // deleting a device owned prop should not delete the underlying prop
        // since those don't get stored
        base_store.store_prop(device_prop).await.unwrap();
        grpc_store
            .delete_prop(device_interface_data, path)
            .await
            .unwrap();
        let prop = base_store
            .load_prop(device_interface_data, path, 1)
            .await
            .unwrap();
        assert_eq!(prop, Some(inner_value.clone()));
    }
}
