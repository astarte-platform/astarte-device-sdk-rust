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

use std::{future::Future, io::Write};

use flate2::{bufread::ZlibDecoder, write::ZlibEncoder, Compression};
use futures::{future, StreamExt, TryStreamExt};
use tracing::{debug, error, warn};

use crate::{
    client::DeviceClient,
    error::Error,
    interface::mapping::path::MappingPath,
    store::{PropertyStore, StoredProp},
    types::AstarteType,
};

/// Error handling the properties.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum PropertiesError {
    /// The payload is too short, it should be at least 4 bytes long.
    #[error("the payload should at least 4 bytes long, got {0}")]
    PayloadTooShort(usize),
    /// The payload is too log, it should be at most a u32.
    #[error("the payload should be at most a u32")]
    PayloadTooLong,
    /// Couldn't convert the size from u32 to usize.
    #[error("error converting the size from u32 to usize")]
    Conversion(#[from] std::num::TryFromIntError),
    /// Error decoding the zlib compressed payload.
    #[error("error decoding the zlib compressed payload")]
    Decode(#[source] std::io::Error),
    /// Error encoding the zlib compressed payload.
    #[error("error encoding the zlib compressed payload")]
    Encode(#[source] std::io::Error),
}

/// Trait to access the stored properties.
pub trait PropAccess {
    /// Get the value of a property given the interface and path.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{
    ///     store::sqlite::SqliteStore, builder::DeviceBuilder,
    ///     transport::mqtt::MqttConfig, types::AstarteType, prelude::*,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let database = SqliteStore::connect_db("/path/to/database/store.db")
    ///         .await
    ///         .unwrap();
    ///     let mqtt_config = MqttConfig::with_credential_secret("realm_id", "device_id", "credential_secret", "pairing_url");
    ///
    ///     let (mut device, _connection) = DeviceBuilder::new().store(database)
    ///         .connection(mqtt_config)
    ///         .build().await.unwrap();
    ///
    ///     let property_value: Option<AstarteType> = device
    ///         .property("my.interface.name", "/endpoint/path")
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    fn property(
        &self,
        interface: &str,
        path: &str,
    ) -> impl Future<Output = Result<Option<AstarteType>, Error>> + Send;
    /// Get all the properties of the given interface.
    fn interface_props(
        &self,
        interface: &str,
    ) -> impl Future<Output = Result<Vec<StoredProp>, Error>> + Send;
    /// Get all the stored properties, device or server owners.
    fn all_props(&self) -> impl Future<Output = Result<Vec<StoredProp>, Error>> + Send;
    /// Get all the stored device properties.
    fn device_props(&self) -> impl Future<Output = Result<Vec<StoredProp>, Error>> + Send;
    /// Get all the stored server properties.
    fn server_props(&self) -> impl Future<Output = Result<Vec<StoredProp>, Error>> + Send;
}

impl<S> PropAccess for DeviceClient<S>
where
    S: PropertyStore,
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
        let prop_if =
            &interfaces
                .get_property(interface_name)
                .ok_or_else(|| Error::InterfaceNotFound {
                    name: interface_name.to_string(),
                })?;

        let stored_prop = self.store.interface_props(&prop_if.into()).await?;

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

                    self.store.delete_prop(&(&p).into()).await?;

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
    let bytes_read = d.read_to_string(&mut s).map_err(PropertiesError::Decode)?;

    debug_assert_eq!(
        bytes_read, size,
        "Byte red and size mismatch: {} != {}",
        bytes_read, size
    );
    // Signal the error in production
    if bytes_read != size {
        error!(
            bytes_read = bytes_read,
            size = size,
            "Byte red and size mismatch",
        );
    }

    if s.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(s.split(';').map(|x| x.to_string()).collect())
    }
}

/// Extracts the properties from a set payload.
///
/// See https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#purge-properties
pub(crate) fn encode_set_properties(
    interface_paths: impl IntoIterator<Item = String>,
) -> Result<Vec<u8>, PropertiesError> {
    let mut iter = interface_paths.into_iter();

    let mut uncompressed_size: u32 = 0;

    // pre-insert the 4 bytes for the size
    let buf: Vec<u8> = vec![0, 0, 0, 0];

    let mut encoder = ZlibEncoder::new(buf, Compression::default());

    let Some(first) = iter.next() else {
        debug!("no properties to retain, sending empty purge");

        return encoder.finish().map_err(PropertiesError::Encode);
    };

    uncompressed_size = encode_prop(&mut encoder, uncompressed_size, &first)?;

    for prop in iter {
        // encode the separator
        uncompressed_size = encode_prop(&mut encoder, uncompressed_size, ";")?;
        // encode the prop
        uncompressed_size = encode_prop(&mut encoder, uncompressed_size, &prop)?;
    }

    let mut res = encoder.finish().map_err(PropertiesError::Encode)?;

    let bytes = uncompressed_size.to_be_bytes();

    // we allocated the first 4 bytes
    res[..bytes.len()].copy_from_slice(&bytes);

    Ok(res)
}

fn encode_prop(
    encoder: &mut ZlibEncoder<Vec<u8>>,
    uncompressed_size: u32,
    value: &str,
) -> Result<u32, PropertiesError> {
    let bytes = value.as_bytes();

    let uncompressed_size = u32::try_from(bytes.len())
        .ok()
        .and_then(|val| uncompressed_size.checked_add(val))
        .ok_or(PropertiesError::PayloadTooLong)?;

    encoder.write_all(bytes).map_err(PropertiesError::Encode)?;

    Ok(uncompressed_size)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;

    use crate::interface::Ownership;
    use crate::store::memory::MemoryStore;
    use crate::store::SqliteStore;
    use crate::test::mock_astarte_device_store;
    use crate::Interface;

    use crate::transport::mqtt::client::{AsyncClient, EventLoop};

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

        let mut client = AsyncClient::default();

        client.expect_clone().once().returning(AsyncClient::default);

        let (sdk, _) = mock_astarte_device_store(
            client,
            EventLoop::default(),
            [
                Interface::from_str(SERVER_PROP).unwrap(),
                Interface::from_str(DEVICE_PROP).unwrap(),
            ],
            store,
        )
        .await;

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
    async fn test_in_sqlite_from_str() {
        let dir = tempfile::tempdir().unwrap();

        let db = dir.path().join("prop-cache.db");

        let store = SqliteStore::connect_db(&db).await.unwrap();

        test_prop_access_for_store(store).await;
    }

    #[tokio::test]
    async fn test_in_sqlite_property_access() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        test_prop_access_for_store(store).await;
    }

    #[test]
    fn should_encode_props() {
        let example = [
            "com.example.MyInterface/some/path".to_string(),
            "org.example.DraftInterface/otherPath".to_string(),
        ];

        let encoded = encode_set_properties(example.clone()).unwrap();

        let expected_snapshot: [u8; 71] = [
            0, 0, 0, 70, 120, 156, 69, 202, 33, 14, 192, 32, 12, 5, 208, 27, 193, 1, 102, 103, 38,
            150, 236, 10, 13, 249, 12, 65, 41, 41, 21, 112, 123, 80, 224, 95, 16, 118, 232, 196,
            53, 195, 189, 227, 41, 6, 141, 20, 224, 155, 48, 124, 37, 75, 151, 232, 191, 197, 173,
            20, 237, 32, 177, 4, 253, 22, 154, 178, 12, 26, 201,
        ];

        assert_eq!(
            encoded,
            expected_snapshot,
            "the two are different, decoded is {:?}",
            extract_set_properties(&encoded)
        );

        assert_eq!(extract_set_properties(&encoded).unwrap(), example);
    }
}
