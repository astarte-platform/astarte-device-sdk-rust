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

use std::path::Path;

use astarte_device_sdk::{AstarteAggregate, AstarteType, DeviceEvent, Error, Interface};
use async_trait::async_trait;
use mockall::mock;

// Export public facing dependencies
pub use mockall;

#[async_trait]
pub trait Client {
    async fn send_object_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send + 'static;

    async fn send_object<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send + 'static;

    async fn send_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send + 'static;

    async fn send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send + 'static;

    async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error>;

    async fn recv(&self) -> Result<DeviceEvent, Error>;
}

#[async_trait]
pub trait DeviceIntrospection {
    async fn get_interface<F, O>(&self, interface_name: &str, f: F) -> O
    where
        F: FnMut(Option<&Interface>) -> O + Send + 'static,
        O: 'static;
}

#[async_trait]
pub trait DynamicIntrospection {
    async fn add_interface(&self, interface: Interface) -> Result<bool, Error>;

    async fn extend_interfaces<I>(&self, interfaces: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = Interface> + Send + 'static;

    async fn extend_interfaces_vec(&self, interfaces: Vec<Interface>)
        -> Result<Vec<String>, Error>;

    async fn add_interface_from_file<P>(&self, file_path: P) -> Result<bool, Error>
    where
        P: AsRef<Path> + Send + Sync + 'static;

    async fn add_interface_from_str(&self, json_str: &str) -> Result<bool, Error>;

    async fn remove_interface(&self, interface_name: &str) -> Result<bool, Error>;

    async fn remove_interfaces<I>(&self, interfaces_name: I) -> Result<Vec<String>, Error>
    where
        I: IntoIterator<Item = String> + Send + 'static,
        I::IntoIter: Send;
}

mock! {
    pub DeviceClient<C: 'static> { }

    #[async_trait]
    impl<C: Send + Sync> Client for DeviceClient<C> {
        async fn send_object_with_timestamp<D>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>
        where
            D: AstarteAggregate + Send + 'static;

        async fn send_object<D>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
        ) -> Result<(), Error>
        where
            D: AstarteAggregate + Send + 'static;

        async fn send_with_timestamp<D>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>
        where
            D: TryInto<AstarteType> + Send + 'static;

        async fn send<D>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
        ) -> Result<(), Error>
        where
            D: TryInto<AstarteType> + Send + 'static;

        async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error>;

        async fn recv(&self) -> Result<DeviceEvent, Error>;
    }


    #[async_trait]
    impl<C: Send + Sync> DeviceIntrospection for DeviceClient<C> {
        async fn get_interface<F, O>(&self, interface_name: &str, f: F) -> O
        where
            F: FnMut(Option<&Interface>) -> O + Send + 'static,
            O: 'static;
    }

    #[async_trait]
    impl<C: Send + Sync> DynamicIntrospection for DeviceClient<C> {
        async fn add_interface(&self, interface: Interface) -> Result<bool, Error>;

        async fn extend_interfaces<I>(&self, interfaces: I) -> Result<Vec<String>, Error>
        where
            I: IntoIterator<Item = Interface> + Send + 'static;

        async fn extend_interfaces_vec(&self, interfaces: Vec<Interface>) -> Result<Vec<String>, Error>;

        async fn add_interface_from_file<P>(&self, file_path: P) -> Result<bool, Error>
        where
            P: AsRef<Path> + Send + Sync + 'static;

        async fn add_interface_from_str(&self, json_str: &str) -> Result<bool, Error>;

        async fn remove_interface(&self, interface_name: &str) -> Result<bool, Error>;

        async fn remove_interfaces<I>(&self, interfaces_name: I) -> Result<Vec<String>, Error>
            where
                I: IntoIterator<Item = String> + Send + 'static,
                I::IntoIter: Send;
    }

    impl<C> Clone for DeviceClient<C> {
        fn clone(&self) -> Self {}
    }
}

mock! {
    pub DeviceConnection<S: 'static, C: 'static> {}

    #[async_trait]
    impl<S: Send, C: Send> astarte_device_sdk::EventLoop for DeviceConnection<S,C> {
        async fn handle_events(&mut self) -> Result<(), crate::Error>;
    }

    #[async_trait]
    impl<S: Send, C: Send> astarte_device_sdk::connection::ClientDisconnect for DeviceConnection<S,C> {
        async fn disconnect(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Struct to keep the traits and mock consistent
    struct CheckMocks {}

    #[async_trait]
    impl Client for CheckMocks {
        async fn send_object_with_timestamp<D>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>
        where
            D: AstarteAggregate + Send + 'static,
        {
            astarte_device_sdk::Client::send_object_with_timestamp(
                self,
                interface_name,
                interface_path,
                data,
                timestamp,
            )
            .await
        }

        async fn send_object<D>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
        ) -> Result<(), Error>
        where
            D: AstarteAggregate + Send + 'static,
        {
            astarte_device_sdk::Client::send_object(self, interface_name, interface_path, data)
                .await
        }

        async fn send_with_timestamp<D>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>
        where
            D: TryInto<AstarteType> + Send + 'static,
        {
            astarte_device_sdk::Client::send_with_timestamp(
                self,
                interface_name,
                interface_path,
                data,
                timestamp,
            )
            .await
        }

        async fn send<D>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
        ) -> Result<(), Error>
        where
            D: TryInto<AstarteType> + Send + 'static,
        {
            astarte_device_sdk::Client::send(self, interface_name, interface_path, data).await
        }

        async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error> {
            astarte_device_sdk::Client::unset(self, interface_name, interface_path).await
        }

        async fn recv(&self) -> Result<DeviceEvent, Error> {
            astarte_device_sdk::Client::recv(self).await
        }
    }

    #[async_trait]
    impl astarte_device_sdk::Client for CheckMocks {
        async fn send_object_with_timestamp<D>(
            &self,
            _interface_name: &str,
            _interface_path: &str,
            _data: D,
            _timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>
        where
            D: AstarteAggregate + Send,
        {
            Ok(())
        }

        async fn send_object<D>(
            &self,
            _interface_name: &str,
            _interface_path: &str,
            _data: D,
        ) -> Result<(), Error>
        where
            D: AstarteAggregate + Send,
        {
            Ok(())
        }

        async fn send_with_timestamp<D>(
            &self,
            _interface_name: &str,
            _interface_path: &str,
            _data: D,
            _timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>
        where
            D: TryInto<AstarteType> + Send,
        {
            Ok(())
        }

        async fn send<D>(
            &self,
            _interface_name: &str,
            _interface_path: &str,
            _data: D,
        ) -> Result<(), Error>
        where
            D: TryInto<AstarteType> + Send,
        {
            Ok(())
        }

        async fn unset(&self, _interface_name: &str, _interface_path: &str) -> Result<(), Error> {
            Ok(())
        }

        async fn recv(&self) -> Result<DeviceEvent, Error> {
            Ok(DeviceEvent {
                interface: Default::default(),
                path: Default::default(),
                data: astarte_device_sdk::Value::Unset,
            })
        }
    }

    #[async_trait]
    impl DeviceIntrospection for CheckMocks {
        async fn get_interface<F, O>(&self, interface_name: &str, f: F) -> O
        where
            F: FnMut(Option<&Interface>) -> O + Send + 'static,
        {
            astarte_device_sdk::introspection::DeviceIntrospection::get_interface(
                self,
                interface_name,
                f,
            )
            .await
        }
    }

    #[async_trait]
    impl astarte_device_sdk::introspection::DeviceIntrospection for CheckMocks {
        async fn get_interface<F, O>(&self, _interface_name: &str, mut f: F) -> O
        where
            F: FnMut(Option<&Interface>) -> O + Send,
        {
            f(None)
        }
    }

    #[async_trait]
    impl DynamicIntrospection for CheckMocks {
        async fn add_interface(&self, interface: Interface) -> Result<bool, Error> {
            astarte_device_sdk::introspection::DynamicIntrospection::add_interface(self, interface)
                .await
        }

        async fn extend_interfaces<I>(&self, interfaces: I) -> Result<Vec<String>, Error>
        where
            I: IntoIterator<Item = Interface> + Send + 'static,
        {
            astarte_device_sdk::introspection::DynamicIntrospection::extend_interfaces(
                self, interfaces,
            )
            .await
        }

        async fn extend_interfaces_vec(
            &self,
            interfaces: Vec<Interface>,
        ) -> Result<Vec<String>, Error> {
            astarte_device_sdk::introspection::DynamicIntrospection::extend_interfaces_vec(
                self, interfaces,
            )
            .await
        }

        async fn add_interface_from_file<P>(&self, file_path: P) -> Result<bool, Error>
        where
            P: AsRef<Path> + Send + Sync + 'static,
        {
            astarte_device_sdk::introspection::DynamicIntrospection::add_interface_from_file(
                self, file_path,
            )
            .await
        }

        async fn add_interface_from_str(&self, json_str: &str) -> Result<bool, Error> {
            astarte_device_sdk::introspection::DynamicIntrospection::add_interface_from_str(
                self, json_str,
            )
            .await
        }

        async fn remove_interface(&self, interface_name: &str) -> Result<bool, Error> {
            astarte_device_sdk::introspection::DynamicIntrospection::remove_interface(
                self,
                interface_name,
            )
            .await
        }

        async fn remove_interfaces<I>(&self, interfaces_name: I) -> Result<Vec<String>, Error>
        where
            I: IntoIterator<Item = String> + Send + 'static,
            I::IntoIter: Send,
        {
            astarte_device_sdk::introspection::DynamicIntrospection::remove_interfaces(
                self,
                interfaces_name,
            )
            .await
        }
    }

    #[async_trait]
    impl astarte_device_sdk::introspection::DynamicIntrospection for CheckMocks {
        async fn add_interface(&self, _interface: Interface) -> Result<bool, Error> {
            Ok(Default::default())
        }

        async fn extend_interfaces<I>(&self, _interfaces: I) -> Result<Vec<String>, Error>
        where
            I: IntoIterator<Item = Interface> + Send,
        {
            Ok(Default::default())
        }

        async fn extend_interfaces_vec(
            &self,
            _interfaces: Vec<Interface>,
        ) -> Result<Vec<String>, Error> {
            Ok(Default::default())
        }

        async fn add_interface_from_file<P>(&self, _file_path: P) -> Result<bool, Error>
        where
            P: AsRef<Path> + Send + Sync,
        {
            Ok(Default::default())
        }

        async fn add_interface_from_str(&self, _json_str: &str) -> Result<bool, Error> {
            Ok(Default::default())
        }

        async fn remove_interface(&self, _interface_name: &str) -> Result<bool, Error> {
            Ok(Default::default())
        }

        async fn remove_interfaces<I>(&self, _interfaces_name: I) -> Result<Vec<String>, Error>
        where
            I: IntoIterator<Item = String> + Send,
            I::IntoIter: Send,
        {
            Ok(Default::default())
        }
    }

    #[test]
    fn should_construct() {
        let _c = CheckMocks {};
    }
}
