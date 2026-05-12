// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
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

//! Handles the properties for the device.

use std::fmt::Display;
use std::io::Read;
use std::{future::Future, io::Write};

use astarte_device_error::{Error, ResultExt, WrapError};
use astarte_interfaces::{MappingPath, Schema, interface::InterfaceTypeAggregation};
use flate2::{Compression, bufread::ZlibDecoder, write::ZlibEncoder};
use futures::{StreamExt, TryStreamExt, future};
use tracing::{debug, error, instrument, warn};

use crate::client::DeviceClient;
use crate::error::{AstarteError, ErrorKind, InterfaceError};
use crate::store::{PropertyMapping, PropertyStore, StoredProp};
use crate::transport::Connection;
use crate::types::AstarteData;

/// Error handling the properties.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PurgePropError {
    /// The payload is too short, it should be at least 4 bytes long.
    Invalid,
    /// Error decoding the zlib compressed payload.
    Decode,
    /// Error encoding the zlib compressed payload.
    Encode,
}

impl Display for PurgePropError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PurgePropError::Invalid => write!(f, "invalid purge properties message"),
            PurgePropError::Decode => write!(f, "couldn't decode purge properties"),
            PurgePropError::Encode => write!(f, "couldn't encode purge properties"),
        }
    }
}

/// Trait to access the stored properties.
pub trait PropAccess {
    /// Get the value of a property given the interface and path.
    ///
    /// ```no_run
    /// use astarte_device_sdk::builder::DeviceBuilder;
    /// use astarte_device_sdk::prelude::*;
    /// use astarte_device_sdk::store::sqlite::SqliteStore;
    /// use astarte_device_sdk::transport::mqtt::{MqttConfig, MqttArgs, Credential};
    /// use astarte_device_sdk::types::AstarteData;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let database = SqliteStore::options().with_db_file("/path/to/database/store.db").await.unwrap();
    ///     let args = MqttArgs{
    ///         realm: "realm_id".to_string(),
    ///         device_id: "device_id".to_string(),
    ///         credential: Credential::secret("credential_secret"),
    ///         pairing_url: "http://api.astarte.localhost/pairing".parse().expect("a valid URL")
    ///     };
    ///     let mqtt_config = MqttConfig::new(args);
    ///
    ///
    ///     let (mut device, _connection) = DeviceBuilder::new().store(database)
    ///         .connection(mqtt_config)
    ///         .build().await.unwrap();
    ///
    ///     let property_value: Option<AstarteData> = device
    ///         .property("my.interface.name", "/endpoint/path")
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    fn property(
        &self,
        interface: &str,
        path: &str,
    ) -> impl Future<Output = Result<Option<AstarteData>, AstarteError>> + Send;
    /// Get all the properties of the given interface.
    fn interface_props(
        &self,
        interface: &str,
    ) -> impl Future<Output = Result<Vec<StoredProp>, AstarteError>> + Send;
    /// Get all the stored properties, device or server owners.
    fn all_props(&self) -> impl Future<Output = Result<Vec<StoredProp>, AstarteError>> + Send;
    /// Get all the stored device properties.
    fn device_props(&self) -> impl Future<Output = Result<Vec<StoredProp>, AstarteError>> + Send;
    /// Get all the stored server properties.
    fn server_props(&self) -> impl Future<Output = Result<Vec<StoredProp>, AstarteError>> + Send;
}

impl<C> PropAccess for DeviceClient<C>
where
    C: Connection,
{
    #[instrument(skip(self))]
    async fn property(
        &self,
        interface_name: &str,
        path: &str,
    ) -> Result<Option<AstarteData>, AstarteError> {
        let path = MappingPath::try_from(path)
            .wrap_err(crate::error::ErrorKind::Interface(InterfaceError::Path))?;

        let interfaces = self.state.interfaces().read().await;
        let mapping = interfaces
            .get_property(interface_name, &path)
            .map_kind(ErrorKind::Interface)?;

        self.try_load_prop(&mapping).await
    }

    #[instrument(skip(self))]
    async fn interface_props(&self, interface_name: &str) -> Result<Vec<StoredProp>, AstarteError> {
        let interfaces = self.state.interfaces().read().await;
        let interface = interfaces.get(interface_name).ok_or_else(|| {
            Error::new(ErrorKind::Interface(InterfaceError::InterfaceNotFound))
                .set_message(interface_name.to_string())
        })?;

        let InterfaceTypeAggregation::Properties(interface) = interface.inner() else {
            return Err(
                Error::new(ErrorKind::Interface(InterfaceError::InterfaceType)).set_message(
                    format!(
                        "for {interface_name} expected property but got {}",
                        interface.interface_type()
                    ),
                ),
            );
        };

        let stored_prop = self
            .store
            .interface_props(interface)
            .await
            .map_kind(ErrorKind::Store)?;

        futures::stream::iter(stored_prop)
            .then(|stored_prop| async {
                if stored_prop.interface_major != interface.version_major() {
                    warn!(
                        "version mismatch for property {}{} (stored {}, interface {}), deleting",
                        stored_prop.interface,
                        stored_prop.path,
                        stored_prop.interface_major,
                        interface.version_major()
                    );

                    self.store
                        .delete_prop(&PropertyMapping::from(&stored_prop))
                        .await
                        .map_kind(ErrorKind::Store)?;

                    Ok(None)
                } else {
                    Ok(Some(stored_prop))
                }
            })
            .try_filter_map(future::ok)
            .try_collect()
            .await
    }

    #[instrument(skip(self))]
    async fn all_props(&self) -> Result<Vec<StoredProp>, AstarteError> {
        self.store.load_all_props().await.map_kind(ErrorKind::Store)
    }

    #[instrument(skip(self))]
    async fn device_props(&self) -> Result<Vec<StoredProp>, AstarteError> {
        self.store.device_props().await.map_kind(ErrorKind::Store)
    }

    #[instrument(skip(self))]
    async fn server_props(&self) -> Result<Vec<StoredProp>, AstarteError> {
        self.store.server_props().await.map_kind(ErrorKind::Store)
    }
}

/// Extracts the properties from a set payload.
///
/// See https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#purge-properties
pub(crate) fn extract_set_properties(bdata: &[u8]) -> Result<Vec<String>, Error<PurgePropError>> {
    let (size, data) = bdata.split_first_chunk::<4>().ok_or_else(|| {
        Error::with(
            PurgePropError::Invalid,
            "payload should be at least 4 bytes",
        )
        .set_message(format!("got {} bytes", bdata.len()))
    })?;

    // The size is a u32 in big endian, so we need to convert it to usize
    let size = u32::from_be_bytes(*size);
    let size = usize::try_from(size)
        .wrap_err_ctx(PurgePropError::Invalid, "payload too big to use as usize")?;

    let mut d = ZlibDecoder::new(data);
    let mut s = String::new();
    let bytes_read = d.read_to_string(&mut s).wrap_err(PurgePropError::Decode)?;

    debug_assert_eq!(
        bytes_read, size,
        "Byte red and size mismatch: {bytes_read} != {size}"
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
pub(crate) fn encode_set_properties<'a, I>(
    interface_paths: I,
) -> Result<Vec<u8>, Error<PurgePropError>>
where
    I: IntoIterator<Item = &'a String>,
{
    let mut iter = interface_paths.into_iter();

    let mut uncompressed_size: u32 = 0;

    // pre-insert the 4 bytes for the size
    let buf: Vec<u8> = vec![0, 0, 0, 0];

    let mut encoder = ZlibEncoder::new(buf, Compression::default());

    let Some(first) = iter.next() else {
        debug!("no properties to retain, sending empty purge");

        return encoder.finish().wrap_err(PurgePropError::Encode);
    };

    uncompressed_size = encode_prop(&mut encoder, uncompressed_size, first)?;

    for prop in iter {
        // encode the separator
        uncompressed_size = encode_prop(&mut encoder, uncompressed_size, ";")?;
        // encode the prop
        uncompressed_size = encode_prop(&mut encoder, uncompressed_size, prop)?;
    }

    let mut res = encoder.finish().wrap_err(PurgePropError::Encode)?;

    let bytes = uncompressed_size.to_be_bytes();

    // we allocated the first 4 bytes
    res[..bytes.len()].copy_from_slice(&bytes);

    Ok(res)
}

fn encode_prop(
    encoder: &mut ZlibEncoder<Vec<u8>>,
    uncompressed_size: u32,
    value: &str,
) -> Result<u32, Error<PurgePropError>> {
    let bytes = value.as_bytes();

    let uncompressed_size = u32::try_from(bytes.len())
        .ok()
        .and_then(|val| uncompressed_size.checked_add(val))
        .ok_or_else(|| {
            Error::with(
                PurgePropError::Invalid,
                "overflow calculating uncompressed size",
            )
        })?;

    encoder.write_all(bytes).wrap_err(PurgePropError::Encode)?;

    Ok(uncompressed_size)
}

#[cfg(test)]
pub(crate) mod tests {
    use astarte_interfaces::schema::Ownership;
    use astarte_test_utils::{Hexdump, with_insta};

    use crate::client::tests::mock_client_with_store;
    use crate::state::ConnStatus;
    use crate::store::memory::MemoryStore;
    use crate::store::{SqliteStore, StoreCapabilities};

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
        "type": "boolean"
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
        "type": "integer"
    }]
}"#;

    async fn test_prop_access_for_store<S>(store: S)
    where
        S: StoreCapabilities,
    {
        store
            .store_prop(StoredProp {
                interface: "org.Foo",
                path: "/bar",
                value: &AstarteData::Boolean(true),
                interface_major: 1,
                ownership: Ownership::Server,
            })
            .await
            .unwrap();

        store
            .store_prop(StoredProp {
                interface: "org.Bar",
                path: "/foo",
                value: &AstarteData::Integer(42),
                interface_major: 1,
                ownership: Ownership::Device,
            })
            .await
            .unwrap();

        let sdk = mock_client_with_store(&[SERVER_PROP, DEVICE_PROP], ConnStatus::Connected, store);

        let prop = sdk.property("org.Foo", "/bar").await.unwrap();
        assert_eq!(prop, Some(AstarteData::Boolean(true)));

        let prop = sdk.property("org.Bar", "/foo").await.unwrap();
        assert_eq!(prop, Some(AstarteData::Integer(42)));

        let mut props = sdk.all_props().await.unwrap();
        props.sort_unstable_by(|a, b| a.interface.cmp(&b.interface));
        let expected = [
            StoredProp::<&'static str> {
                interface: "org.Bar",
                path: "/foo",
                value: AstarteData::Integer(42),
                interface_major: 1,
                ownership: Ownership::Device,
            },
            StoredProp::<&'static str> {
                interface: "org.Foo",
                path: "/bar",
                value: AstarteData::Boolean(true),
                interface_major: 1,
                ownership: Ownership::Server,
            },
        ];
        assert_eq!(props, expected);

        let props = sdk.device_props().await.unwrap();
        let expected = [StoredProp {
            interface: "org.Bar",
            path: "/foo",
            value: AstarteData::Integer(42),
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
            value: AstarteData::Boolean(true),
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

        let store = SqliteStore::options().with_db_file(&db).await.unwrap();

        test_prop_access_for_store(store).await;
    }

    #[tokio::test]
    async fn test_in_sqlite_property_access() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::options()
            .with_writable_dir(dir.path())
            .await
            .unwrap();

        test_prop_access_for_store(store).await;
    }

    #[test]
    fn should_encode_props() {
        let example = [
            "com.example.MyInterface/some/path".to_string(),
            "org.example.DraftInterface/otherPath".to_string(),
        ];

        let encoded = encode_set_properties(&example).unwrap();

        with_insta!({
            insta::assert_snapshot!(Hexdump(encoded.as_slice()));
        });

        assert_eq!(extract_set_properties(&encoded).unwrap(), example);
    }
}
