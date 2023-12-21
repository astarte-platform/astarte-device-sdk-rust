// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
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

//! Handles the properties for the device.

use async_trait::async_trait;
use flate2::bufread::ZlibDecoder;
use futures::{future, StreamExt, TryStreamExt};
use log::{error, warn};

use crate::{
    error::Error,
    interface::mapping::path::MappingPath,
    store::{PropertyStore, StoredProp},
    types::AstarteType,
    AstarteDeviceSdk,
};

/// Error handling the properties.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum PropertiesError {
    /// The payload is too short, it should be at least 4 bytes long.
    #[error("the payload should at least 4 bytes long, got {0}")]
    PayloadTooShort(usize),
    /// Couldn't convert the size from u32 to usize.
    #[error("error converting the size from u32 to usize")]
    Conversion(#[from] std::num::TryFromIntError),
    /// Error decoding the zlib compressed payload.
    #[error("error decoding the zlib compressed payload")]
    Decode(#[from] std::io::Error),
}

/// Trait to access the stored properties.
#[async_trait]
pub trait PropAccess {
    /// Get the value of a property given the interface and path.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{
    ///     AstarteDeviceSdk, store::sqlite::SqliteStore, builder::DeviceBuilder,
    ///     transport::mqtt::MqttConfig, types::AstarteType, prelude::*,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let database = SqliteStore::new("path/to/database/file.sqlite")
    ///         .await
    ///         .unwrap();
    ///     let mqtt_config = MqttConfig::new("realm_id", "device_id", "credential_secret", "pairing_url");
    ///
    ///     let (mut device, _rx_events) = DeviceBuilder::new().store(database)
    ///         .connect(mqtt_config).await.unwrap()
    ///         .build();
    ///
    ///     let property_value: Option<AstarteType> = device
    ///         .property("my.interface.name", "/endpoint/path")
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    async fn property(&self, interface: &str, path: &str) -> Result<Option<AstarteType>, Error>;
    /// Get all the properties of the given interface.
    async fn interface_props(&self, interface: &str) -> Result<Vec<StoredProp>, Error>;
    /// Get all the stored properties, device or server owners.
    async fn all_props(&self) -> Result<Vec<StoredProp>, Error>;
    /// Get all the stored device properties.
    async fn device_props(&self) -> Result<Vec<StoredProp>, Error>;
    /// Get all the stored server properties.
    async fn server_props(&self) -> Result<Vec<StoredProp>, Error>;
}

#[async_trait]
impl<S, C> PropAccess for AstarteDeviceSdk<S, C>
where
    S: PropertyStore,
    C: Sync,
{
    async fn property(
        &self,
        interface_name: &str,
        path: &str,
    ) -> Result<Option<AstarteType>, Error> {
        let path = MappingPath::try_from(path)?;

        let interfaces = &self.interfaces.read().await;
        let mapping = interfaces.property_mapping(interface_name, &path)?;

        self.try_load_prop(&mapping, &path).await
    }

    async fn interface_props(&self, interface_name: &str) -> Result<Vec<StoredProp>, Error> {
        let interfaces = &self.interfaces.read().await;
        let prop_if = interfaces
            .get_property(interface_name)
            .ok_or_else(|| Error::MissingInterface(interface_name.to_string()))?;

        let stored_prop = self.store.interface_props(prop_if.interface_name()).await?;

        futures::stream::iter(stored_prop)
            .then(|p| async {
                if p.interface_major != prop_if.version_major() {
                    warn!(
                        "version mismatch for property {}{} (stored {}, interface {}), deleting",
                        p.interface,
                        p.path,
                        p.interface_major,
                        prop_if.version_major()
                    );

                    self.store.delete_prop(&p.interface, &p.path).await?;

                    Ok(None)
                } else {
                    Ok(Some(p))
                }
            })
            .try_filter_map(future::ok)
            .try_collect()
            .await
    }

    async fn all_props(&self) -> Result<Vec<StoredProp>, Error> {
        self.store.load_all_props().await.map_err(Error::from)
    }

    async fn device_props(&self) -> Result<Vec<StoredProp>, Error> {
        self.store.device_props().await.map_err(Error::from)
    }

    async fn server_props(&self) -> Result<Vec<StoredProp>, Error> {
        self.store.server_props().await.map_err(Error::from)
    }
}

/// Extracts the properties from a set payload.
///
/// See https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#purge-properties
pub(crate) fn extract_set_properties(bdata: &[u8]) -> Result<Vec<String>, PropertiesError> {
    use std::io::Read;

    if bdata.len() < 4 {
        return Err(PropertiesError::PayloadTooShort(bdata.len()));
    }

    let (size, data) = bdata.split_at(4);
    // The size is a u32 in big endian, so we need to convert it to usize
    let size: u32 = u32::from_be_bytes([size[0], size[1], size[2], size[3]]);
    let size: usize = size.try_into()?;

    let mut d = ZlibDecoder::new(data);
    let mut s = String::new();
    let bytes_read = d.read_to_string(&mut s)?;

    debug_assert_eq!(
        bytes_read, size,
        "Byte red and size mismatch: {} != {}",
        bytes_read, size
    );
    // Signal the error in production
    if bytes_read != size {
        error!("Byte red and size mismatch: {} != {}", bytes_read, size);
    }

    Ok(s.split(';').map(|x| x.to_string()).collect())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;

    use crate::interface::Ownership;
    use crate::store::memory::MemoryStore;
    use crate::store::SqliteStore;
    use crate::test::mock_astarte_device_store;
    use crate::Interface;

    use crate::transport::mqtt::{AsyncClient, EventLoop};

    use super::*;

    pub(crate) const PROPERTIES_PAYLOAD: [u8; 66] = [
        0x00, 0x00, 0x00, 0x46, 0x78, 0x9c, 0x4b, 0xce, 0xcf, 0xd5, 0x4b, 0xad, 0x48, 0xcc, 0x2d,
        0xc8, 0x49, 0xd5, 0xf3, 0xad, 0xf4, 0xcc, 0x2b, 0x49, 0x2d, 0x4a, 0x4b, 0x4c, 0x4e, 0xd5,
        0x2f, 0xce, 0xcf, 0x4d, 0xd5, 0x2f, 0x48, 0x2c, 0xc9, 0xb0, 0xce, 0x2f, 0x4a, 0x87, 0xab,
        0x70, 0x29, 0x4a, 0x4c, 0x2b, 0x41, 0x28, 0xca, 0x2f, 0xc9, 0x48, 0x2d, 0x0a, 0x00, 0x2a,
        0x02, 0x00, 0xb2, 0x0c, 0x1a, 0xc9,
    ];

    #[test]
    fn test_deflate() {
        let example = b"com.example.MyInterface/some/path;org.example.DraftInterface/otherPath";

        let s = extract_set_properties(&PROPERTIES_PAYLOAD).unwrap();

        assert_eq!(s.join(";").as_bytes(), example);
    }

    const SERVER_PROP: &str = r#"{
    "interface_name": "org.Foo",
    "version_major": 1,
    "version_minor": 0,
    "type": "properties",
    "aggregation": "individual",
    "ownership": "server",
    "description": "Generic aggregated object data.",
    "mappings": [{
        "endpoint": "/bar",
        "type": "boolean",
        "explicit_timestamp": false
    }]
}"#;
    const DEVICE_PROP: &str = r#"{
    "interface_name": "org.Bar",
    "version_major": 1,
    "version_minor": 0,
    "type": "properties",
    "aggregation": "individual",
    "ownership": "device",
    "description": "Generic aggregated object data.",
    "mappings": [{
        "endpoint": "/foo",
        "type": "integer",
        "explicit_timestamp": false
    }]
}"#;

    async fn test_prop_access_for_store<S: PropertyStore>(store: S) {
        store
            .store_prop(StoredProp {
                interface: "org.Foo",
                path: "/bar",
                value: &AstarteType::Boolean(true),
                interface_major: 1,
                ownership: Ownership::Server,
            })
            .await
            .unwrap();

        store
            .store_prop(StoredProp {
                interface: "org.Bar",
                path: "/foo",
                value: &AstarteType::Integer(42),
                interface_major: 1,
                ownership: Ownership::Device,
            })
            .await
            .unwrap();

        let (sdk, _) = mock_astarte_device_store(
            AsyncClient::default(),
            EventLoop::default(),
            [
                Interface::from_str(SERVER_PROP).unwrap(),
                Interface::from_str(DEVICE_PROP).unwrap(),
            ],
            store,
        );

        let prop = sdk.property("org.Foo", "/bar").await.unwrap();
        assert_eq!(prop, Some(AstarteType::Boolean(true)));

        let prop = sdk.property("org.Bar", "/foo").await.unwrap();
        assert_eq!(prop, Some(AstarteType::Integer(42)));

        let mut props = sdk.all_props().await.unwrap();
        props.sort_unstable_by(|a, b| a.interface.cmp(&b.interface));
        let expected = [
            StoredProp::<&'static str> {
                interface: "org.Bar",
                path: "/foo",
                value: AstarteType::Integer(42),
                interface_major: 1,
                ownership: Ownership::Device,
            },
            StoredProp::<&'static str> {
                interface: "org.Foo",
                path: "/bar",
                value: AstarteType::Boolean(true),
                interface_major: 1,
                ownership: Ownership::Server,
            },
        ];
        assert_eq!(props, expected);

        let props = sdk.device_props().await.unwrap();
        let expected = [StoredProp {
            interface: "org.Bar",
            path: "/foo",
            value: AstarteType::Integer(42),
            interface_major: 1,
            ownership: Ownership::Device,
        }];
        assert_eq!(props, expected);

        let props = sdk.interface_props("org.Bar").await.unwrap();
        assert_eq!(props, expected);

        let props = sdk.server_props().await.unwrap();
        let expected = [StoredProp::<&'static str> {
            interface: "org.Foo",
            path: "/bar",
            value: AstarteType::Boolean(true),
            interface_major: 1,
            ownership: Ownership::Server,
        }];
        assert_eq!(props, expected);

        let props = sdk.interface_props("org.Foo").await.unwrap();
        assert_eq!(props, expected);
    }

    #[tokio::test]
    async fn test_in_memory_property_access() {
        let store = MemoryStore::new();

        test_prop_access_for_store(store).await;
    }

    #[tokio::test]
    async fn test_in_sqlite_property_access() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let path = db_path.as_path().to_str().unwrap();

        let store = SqliteStore::new(path).await.unwrap();

        test_prop_access_for_store(store).await;
    }
}
