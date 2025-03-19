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

use std::{future::Future, path::Path};

use astarte_device_sdk::{
    aggregate::AstarteObject,
    client::{ClientDisconnect, RecvError},
    properties::PropAccess,
    store::StoredProp,
    AstarteType, DeviceEvent, Error, Interface,
};
use mockall::mock;

// Export public facing dependencies
pub use mockall;

pub trait Client {
    fn send_object_with_timestamp(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: AstarteObject,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn send_object(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: AstarteObject,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn send_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> impl Future<Output = Result<(), Error>> + Send
    where
        D: TryInto<AstarteType> + Send + 'static;

    fn send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> impl Future<Output = Result<(), Error>> + Send
    where
        D: TryInto<AstarteType> + Send + 'static;

    fn unset(
        &self,
        interface_name: &str,
        interface_path: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn recv(&self) -> impl Future<Output = Result<DeviceEvent, RecvError>> + Send;
}

pub trait DeviceIntrospection {
    fn get_interface<F, O>(&self, interface_name: &str, f: F) -> impl Future<Output = O> + Send
    where
        F: FnMut(Option<&Interface>) -> O + Send + 'static,
        O: 'static;
}

pub trait DynamicIntrospection {
    fn add_interface(
        &self,
        interface: Interface,
    ) -> impl Future<Output = Result<bool, Error>> + Send;

    fn extend_interfaces<I>(
        &self,
        interfaces: I,
    ) -> impl Future<Output = Result<Vec<String>, Error>> + Send
    where
        I: IntoIterator<Item = Interface> + Send + 'static;

    fn extend_interfaces_vec(
        &self,
        interfaces: Vec<Interface>,
    ) -> impl Future<Output = Result<Vec<String>, Error>> + Send;

    fn add_interface_from_file<P>(
        &self,
        file_path: P,
    ) -> impl Future<Output = Result<bool, Error>> + Send
    where
        P: AsRef<Path> + Send + Sync + 'static;

    fn add_interface_from_str(
        &self,
        json_str: &str,
    ) -> impl Future<Output = Result<bool, Error>> + Send;

    fn remove_interface(
        &self,
        interface_name: &str,
    ) -> impl Future<Output = Result<bool, Error>> + Send;

    fn remove_interfaces<I>(
        &self,
        interfaces_name: I,
    ) -> impl Future<Output = Result<Vec<String>, Error>> + Send
    where
        I: IntoIterator<Item = String> + Send + 'static,
        I::IntoIter: Send;

    fn remove_interfaces_vec(
        &self,
        interfaces_name: Vec<String>,
    ) -> impl Future<Output = Result<Vec<String>, Error>> + Send;
}

mock! {
    pub DeviceClient<S: 'static> { }

    impl<S: Send + Sync> Client for DeviceClient<S> {
        async fn send_object_with_timestamp(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: AstarteObject,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>;

        async fn send_object(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: AstarteObject,
        ) -> Result<(), Error>;

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

        async fn recv(&self) -> Result<DeviceEvent, RecvError>;
    }

    impl<S: Send + Sync> DeviceIntrospection for DeviceClient<S> {
        async fn get_interface<F, O>(&self, interface_name: &str, f: F) -> O
        where
            F: FnMut(Option<&Interface>) -> O + Send + 'static,
            O: 'static;
    }

    impl<S: Send + Sync> DynamicIntrospection for DeviceClient<S> {
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

        async fn remove_interfaces_vec(&self, interfaces_name: Vec<String>) -> Result<Vec<String>, Error>;
    }

    impl<S: Send + Sync> PropAccess for DeviceClient<S> {
        async fn property(&self, interface: &str, path: &str) -> Result<Option<AstarteType>, Error>;
        async fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, Error>;
        async fn all_props(&self) -> Result<Vec<StoredProp>, Error>;
        async fn device_props(&self) -> Result<Vec<StoredProp>, Error>;
        async fn server_props(&self) -> Result<Vec<StoredProp>, Error>;
    }

    impl<S: Send + Sync> ClientDisconnect for DeviceClient<S> {
        async fn disconnect(&self) -> Result<(), Error>;
    }

    impl<S> Clone for DeviceClient<S> {
        fn clone(&self) -> Self {}
    }
}

mock! {
    pub DeviceConnection<S: 'static, C: 'static> {}

    impl<S: Send, C: Send> astarte_device_sdk::EventLoop for DeviceConnection<S,C> {
        async fn handle_events(self) -> Result<(), crate::Error>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Struct to keep the traits and mock consistent
    struct CheckMocks {}

    impl Client for CheckMocks {
        async fn send_object_with_timestamp(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: AstarteObject,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error> {
            astarte_device_sdk::Client::send_object_with_timestamp(
                self,
                interface_name,
                interface_path,
                data,
                timestamp,
            )
            .await
        }

        async fn send_object(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: AstarteObject,
        ) -> Result<(), Error> {
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

        async fn recv(&self) -> Result<DeviceEvent, RecvError> {
            astarte_device_sdk::Client::recv(self).await
        }
    }

    impl astarte_device_sdk::Client for CheckMocks {
        async fn send_object_with_timestamp(
            &self,
            _interface_name: &str,
            _interface_path: &str,
            _data: AstarteObject,
            _timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn send_object(
            &self,
            _interface_name: &str,
            _interface_path: &str,
            _data: AstarteObject,
        ) -> Result<(), Error> {
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

        async fn recv(&self) -> Result<DeviceEvent, RecvError> {
            Ok(DeviceEvent {
                interface: Default::default(),
                path: Default::default(),
                data: astarte_device_sdk::Value::Unset,
            })
        }
    }

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

    impl astarte_device_sdk::introspection::DeviceIntrospection for CheckMocks {
        async fn get_interface<F, O>(&self, _interface_name: &str, mut f: F) -> O
        where
            F: FnMut(Option<&Interface>) -> O + Send,
        {
            f(None)
        }
    }

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

        async fn remove_interfaces_vec(
            &self,
            interfaces_name: Vec<String>,
        ) -> Result<Vec<String>, Error> {
            astarte_device_sdk::introspection::DynamicIntrospection::remove_interfaces_vec(
                self,
                interfaces_name,
            )
            .await
        }
    }

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

        async fn remove_interfaces_vec(
            &self,
            _interfaces_name: Vec<String>,
        ) -> Result<Vec<String>, Error> {
            Ok(Default::default())
        }
    }

    #[test]
    fn should_construct() {
        let _c = CheckMocks {};
    }
}
