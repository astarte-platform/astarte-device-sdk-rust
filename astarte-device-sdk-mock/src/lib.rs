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

use astarte_device_sdk::aggregate::AstarteObject;
use astarte_device_sdk::astarte_interfaces::Interface;
use astarte_device_sdk::client::{ClientDisconnect, RecvError};
use astarte_device_sdk::properties::PropAccess;
use astarte_device_sdk::store::StoredProp;
use astarte_device_sdk::transport::Connection;
use astarte_device_sdk::{AstarteData, DeviceEvent, Error};
use mockall::mock;

// Export public facing dependencies
pub use mockall;

// FIXME: remove, still present for backwards compatibility
pub use astarte_device_sdk::Client;

pub trait DeviceIntrospection {
    fn get_interface<F, O>(&self, interface_name: &str, f: F) -> impl Future<Output = O> + Send
    where
        F: FnMut(Option<&Interface>) -> O + Send + 'static,
        O: 'static;
}

pub trait DynamicIntrospection {
    fn add_interface(
        &mut self,
        interface: Interface,
    ) -> impl Future<Output = Result<bool, Error>> + Send;

    fn extend_interfaces<I>(
        &mut self,
        interfaces: I,
    ) -> impl Future<Output = Result<Vec<String>, Error>> + Send
    where
        I: IntoIterator<Item = Interface> + Send + 'static;

    fn add_interface_from_file<P>(
        &mut self,
        file_path: P,
    ) -> impl Future<Output = Result<bool, Error>> + Send
    where
        P: AsRef<Path> + Send + Sync + 'static;

    fn add_interface_from_str(
        &mut self,
        json_str: &str,
    ) -> impl Future<Output = Result<bool, Error>> + Send;

    fn remove_interface(
        &mut self,
        interface_name: &str,
    ) -> impl Future<Output = Result<bool, Error>> + Send;

    fn remove_interfaces<I>(
        &mut self,
        interfaces_name: I,
    ) -> impl Future<Output = Result<Vec<String>, Error>> + Send
    where
        I: IntoIterator<Item = String> + Send + 'static,
        I::IntoIter: Send;
}

mock! {
    pub DeviceClient<C: Connection + 'static> { }

    impl<C: Connection> Clone for DeviceClient<C> {
        fn clone(&self) -> Self;
    }

    impl<C: Connection> Client for DeviceClient<C> {
        async fn send_object_with_timestamp(
            &mut self,
            interface_name: &str,
            interface_path: &str,
            data: AstarteObject,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>;

        async fn send_object(
            &mut self,
            interface_name: &str,
            interface_path: &str,
            data: AstarteObject,
        ) -> Result<(), Error>;

        async fn send_individual_with_timestamp(
            &mut self,
            interface_name: &str,
            interface_path: &str,
            data: AstarteData,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>;

        async fn send_individual(
            &mut self,
            interface_name: &str,
            interface_path: &str,
            data: AstarteData,
        ) -> Result<(), Error>;

        async fn set_property(
            &mut self,
            interface_name: &str,
            interface_path: &str,
            data: AstarteData
        ) -> Result<(), Error>;

        async fn unset_property(&mut self, interface_name: &str, interface_path: &str) -> Result<(), Error>;

        async fn recv(&self) -> Result<DeviceEvent, RecvError>;
    }

    impl<C: Connection> DeviceIntrospection for DeviceClient<C> {
        async fn get_interface<F, O>(&self, interface_name: &str, f: F) -> O
        where
            F: FnMut(Option<&Interface>) -> O + Send + 'static,
            O: 'static;
    }

    impl<C: Connection> DynamicIntrospection for DeviceClient<C> {
        async fn add_interface(&mut self, interface: Interface) -> Result<bool, Error>;

        async fn extend_interfaces<I>(&mut self, interfaces: I) -> Result<Vec<String>, Error>
        where
            I: IntoIterator<Item = Interface> + Send + 'static;

        async fn add_interface_from_file<P>(&mut self, file_path: P) -> Result<bool, Error>
        where
            P: AsRef<Path> + Send + Sync + 'static;

        async fn add_interface_from_str(&mut self, json_str: &str) -> Result<bool, Error>;

        async fn remove_interface(&mut self, interface_name: &str) -> Result<bool, Error>;

        async fn remove_interfaces<I>(&mut self, interfaces_name: I) -> Result<Vec<String>, Error>
            where
                I: IntoIterator<Item = String> + Send + 'static,
                I::IntoIter: Send;
    }

    impl<C: Connection> PropAccess for DeviceClient<C> {
        async fn property(&self, interface: &str, path: &str) -> Result<Option<AstarteData>, Error>;
        async fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, Error>;
        async fn all_props(&self) -> Result<Vec<StoredProp>, Error>;
        async fn device_props(&self) -> Result<Vec<StoredProp>, Error>;
        async fn server_props(&self) -> Result<Vec<StoredProp>, Error>;
    }

    impl<C: Connection> ClientDisconnect for DeviceClient<C> {
        async fn disconnect(&mut self) -> Result<(), Error>;
    }
}

mock! {
    pub DeviceConnection<C: Connection + 'static> {}

    impl<C: Connection> astarte_device_sdk::EventLoop for DeviceConnection<C> {
        async fn handle_events(self) -> Result<(), crate::Error>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Struct to keep the traits and mock consistent
    #[derive(Debug, Clone)]
    struct CheckMocks {}

    impl astarte_device_sdk::Client for CheckMocks {
        async fn send_object_with_timestamp(
            &mut self,
            _interface_name: &str,
            _interface_path: &str,
            _data: AstarteObject,
            _timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn send_object(
            &mut self,
            _interface_name: &str,
            _interface_path: &str,
            _data: AstarteObject,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn send_individual_with_timestamp(
            &mut self,
            _interface_name: &str,
            _interface_path: &str,
            _data: AstarteData,
            _timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn send_individual(
            &mut self,
            _interface_name: &str,
            _interface_path: &str,
            _data: AstarteData,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn set_property(
            &mut self,
            _interface_name: &str,
            _mapping_path: &str,
            _data: AstarteData,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn unset_property(
            &mut self,
            _interface_name: &str,
            _mapping_path: &str,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn recv(&self) -> Result<DeviceEvent, RecvError> {
            Ok(DeviceEvent {
                interface: Default::default(),
                path: Default::default(),
                data: astarte_device_sdk::Value::Property(None),
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
        async fn add_interface(&mut self, interface: Interface) -> Result<bool, Error> {
            astarte_device_sdk::introspection::DynamicIntrospection::add_interface(self, interface)
                .await
        }

        async fn extend_interfaces<I>(&mut self, interfaces: I) -> Result<Vec<String>, Error>
        where
            I: IntoIterator<Item = Interface> + Send + 'static,
        {
            astarte_device_sdk::introspection::DynamicIntrospection::extend_interfaces(
                self, interfaces,
            )
            .await
        }

        async fn add_interface_from_file<P>(&mut self, file_path: P) -> Result<bool, Error>
        where
            P: AsRef<Path> + Send + Sync + 'static,
        {
            astarte_device_sdk::introspection::DynamicIntrospection::add_interface_from_file(
                self, file_path,
            )
            .await
        }

        async fn add_interface_from_str(&mut self, json_str: &str) -> Result<bool, Error> {
            astarte_device_sdk::introspection::DynamicIntrospection::add_interface_from_str(
                self, json_str,
            )
            .await
        }

        async fn remove_interface(&mut self, interface_name: &str) -> Result<bool, Error> {
            astarte_device_sdk::introspection::DynamicIntrospection::remove_interface(
                self,
                interface_name,
            )
            .await
        }

        async fn remove_interfaces<I>(&mut self, interfaces_name: I) -> Result<Vec<String>, Error>
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

    impl astarte_device_sdk::introspection::DynamicIntrospection for CheckMocks {
        async fn add_interface(&mut self, _interface: Interface) -> Result<bool, Error> {
            Ok(Default::default())
        }

        async fn extend_interfaces<I>(&mut self, _interfaces: I) -> Result<Vec<String>, Error>
        where
            I: IntoIterator<Item = Interface> + Send,
        {
            Ok(Default::default())
        }

        async fn add_interface_from_file<P>(&mut self, _file_path: P) -> Result<bool, Error>
        where
            P: AsRef<Path> + Send + Sync,
        {
            Ok(Default::default())
        }

        async fn add_interface_from_str(&mut self, _json_str: &str) -> Result<bool, Error> {
            Ok(Default::default())
        }

        async fn remove_interface(&mut self, _interface_name: &str) -> Result<bool, Error> {
            Ok(Default::default())
        }

        async fn remove_interfaces<I>(&mut self, _interfaces_name: I) -> Result<Vec<String>, Error>
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
