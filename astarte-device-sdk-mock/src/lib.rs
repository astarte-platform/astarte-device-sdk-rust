// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
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

use std::path::Path;

use astarte_device_sdk::aggregate::AstarteObject;
use astarte_device_sdk::astarte_interfaces::Interface;
use astarte_device_sdk::error::AstarteError;
use astarte_device_sdk::introspection::DeviceIntrospection;
use astarte_device_sdk::properties::PropAccess;
use astarte_device_sdk::store::StoredProp;
use astarte_device_sdk::{AstarteData, Client, Connection, DeviceEvent};
use chrono::{DateTime, Utc};
use mockall::mock;

// Export public facing dependencies
pub use mockall;

mock! {
    pub DeviceClient {
        async fn get_interface_call(&self, interface_name: &str) -> Option<Interface>;

        async fn add_interface_call(&self, interface: Interface) -> Result<bool, AstarteError>;

        async fn extend_interfaces_call(&self, interfaces: Vec<Interface>) -> Result<Vec<String>, AstarteError>;

        async fn add_interface_from_file_call(&self, file_path: &Path) -> Result<bool, AstarteError>;

        async fn add_interface_from_str_call(&self, json_str: &str) -> Result<bool, AstarteError>;

        async fn remove_interface_call(&self, interface_name: &str) -> Result<bool, AstarteError>;

        async fn remove_interfaces_call(&self, interfaces_name: Vec<String>) -> Result<Vec<String>, AstarteError>;
    }

    impl Clone for DeviceClient {
        fn clone(&self) -> Self;
    }

    impl Client for DeviceClient {
        async fn send_object_with_timestamp(
            &self,
            interface_name: &str,
            base_path: &str,
            data: AstarteObject,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), AstarteError>;

        async fn send_object(
            &self,
            interface_name: &str,
            base_path: &str,
            data: AstarteObject,
        ) -> Result<(), AstarteError>;

        async fn send_individual_with_timestamp(
            &self,
            interface_name: &str,
            mapping_path: &str,
            data: AstarteData,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), AstarteError>;

        async fn send_individual(
            &self,
            interface_name: &str,
            mapping_path: &str,
            data: AstarteData,
        ) -> Result<(), AstarteError>;

        async fn set_property(
            &self,
            interface_name: &str,
            mapping_path: &str,
            data: AstarteData
        ) -> Result<(), AstarteError>;

        async fn unset_property(&self, interface_name: &str, mapping_path: &str) -> Result<(), AstarteError>;

        async fn recv(&self) -> Option<DeviceEvent>;

        async fn get_cert_expiry(&self) -> Option<DateTime<Utc>>;

        async fn is_valid_at(&self, check_dt: DateTime<Utc>) -> Option<bool>;

        async fn disconnect(&self) -> Result<(), AstarteError>;

        fn is_paired(&self) -> bool;
    }

    impl PropAccess for DeviceClient {
        async fn property(&self, interface: &str, path: &str) -> Result<Option<AstarteData>, AstarteError>;
        async fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, AstarteError>;
        async fn all_props(&self) -> Result<Vec<StoredProp>, AstarteError>;
        async fn device_props(&self) -> Result<Vec<StoredProp>, AstarteError>;
        async fn server_props(&self) -> Result<Vec<StoredProp>, AstarteError>;
    }
}

impl DeviceIntrospection for MockDeviceClient {
    async fn get_interface<F, O>(&self, interface_name: &str, mut f: F) -> O
    where
        F: FnMut(Option<&Interface>) -> O + Send,
    {
        let intf = self.get_interface_call(interface_name).await;

        (f)(intf.as_ref())
    }

    async fn add_interface(&self, interface: Interface) -> Result<bool, AstarteError> {
        self.add_interface_call(interface).await
    }

    async fn extend_interfaces<I>(&self, interfaces: I) -> Result<Vec<String>, AstarteError>
    where
        I: IntoIterator<Item = Interface> + Send,
    {
        let interfaces = interfaces.into_iter().collect();

        self.extend_interfaces_call(interfaces).await
    }

    async fn add_interface_from_file<P>(&self, file_path: P) -> Result<bool, AstarteError>
    where
        P: AsRef<Path> + Send + Sync,
    {
        self.add_interface_from_file_call(file_path.as_ref()).await
    }

    async fn add_interface_from_str(&self, json_str: &str) -> Result<bool, AstarteError> {
        self.add_interface_from_str_call(json_str).await
    }

    async fn remove_interface(&self, interface_name: &str) -> Result<bool, AstarteError> {
        self.remove_interface_call(interface_name).await
    }

    async fn remove_interfaces<I>(&self, interfaces_name: I) -> Result<Vec<String>, AstarteError>
    where
        I: IntoIterator<Item = String> + Send,
    {
        let interfaces = interfaces_name.into_iter().collect();

        self.remove_interfaces_call(interfaces).await
    }
}

mock! {
    pub DeviceConnection {}

    impl Connection for DeviceConnection {
        async fn handle_events(self) -> Result<(), AstarteError>;
    }
}
