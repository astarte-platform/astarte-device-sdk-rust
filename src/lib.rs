// This file is part of Astarte.
//
// Copyright 2021 - 2025 SECO Mind Srl
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

#![doc = include_str!("../README.md")]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/assets/logos/clea-24.svg"
)]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/assets/logos/clea-24.ico"
)]
#![warn(
    clippy::dbg_macro,
    clippy::todo,
    missing_docs,
    rustdoc::missing_crate_level_docs
)]
#![cfg_attr(astarte_device_sdk_docsrs, feature(doc_cfg))]
#![cfg_attr(__coverage, feature(coverage_attribute))]

pub mod _docs;

pub mod aggregate;
pub mod builder;
pub mod client;
pub mod connection;
pub mod error;
pub mod event;
mod interfaces;
pub mod introspection;
pub(crate) mod logging;
pub mod pairing;
pub mod prelude;
pub mod properties;
pub mod retention;
mod retry;
pub mod session;
pub(crate) mod state;
pub mod store;
pub mod transport;
pub mod types;
mod validate;

/// Re-exported internal structs
pub use crate::client::{Client, DeviceClient};
pub use crate::connection::{DeviceConnection, EventLoop};
pub use crate::error::Error;
pub use crate::event::Value;
pub use crate::event::{DeviceEvent, FromEvent};
pub use crate::types::AstarteData;

// Re-export rumqttc since we return its types in some methods
pub use astarte_interfaces;
pub use chrono;
pub use rumqttc;

/// Timestamp returned in the astarte payload
pub(crate) type Timestamp = chrono::DateTime<chrono::Utc>;

#[cfg(feature = "derive")]
#[cfg_attr(astarte_device_sdk_docsrs, doc(cfg(feature = "derive")))]
pub use astarte_device_sdk_derive::*;

#[cfg(test)]
mod test {
    // Interfaces
    pub(crate) const DEVICE_OBJECT: &str = include_str!(
        "../examples/object_datastream/interfaces/org.astarte-platform.rust.examples.object-datastream.DeviceDatastream.json"
    );
    pub(crate) const DEVICE_PROPERTIES: &str = include_str!(
        "../examples/individual_properties/interfaces/org.astarte-platform.rust.examples.individual-properties.DeviceProperties.json"
    );
    pub(crate) const DEVICE_PROPERTIES_NAME: &str =
        "org.astarte-platform.rust.examples.individual-properties.DeviceProperties";

    pub(crate) const SERVER_OBJECT: &str = include_str!(
        "../examples/object_datastream/interfaces/org.astarte-platform.rust.examples.object-datastream.ServerDatastream.json"
    );
    pub(crate) const SERVER_OBJECT_NAME: &str =
        "org.astarte-platform.rust.examples.object-datastream.ServerDatastream";
    pub(crate) const SERVER_PROPERTIES: &str = include_str!(
        "../examples/individual_properties/interfaces/org.astarte-platform.rust.examples.individual-properties.ServerProperties.json"
    );
    #[cfg(feature = "message-hub")]
    pub(crate) const SERVER_PROPERTIES_NAME: &str =
        "org.astarte-platform.rust.examples.individual-properties.ServerProperties";
    pub(crate) const SERVER_INDIVIDUAL: &str = include_str!(
        "../examples/individual_datastream/interfaces/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream.json"
    );
    pub(crate) const SERVER_INDIVIDUAL_NAME: &str =
        "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream";

    // E2E Interfaces
    pub(crate) const E2E_DEVICE_DATASTREAM: &str = include_str!(
        "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceDatastream.json"
    );
    pub(crate) const E2E_DEVICE_DATASTREAM_NAME: &str =
        "org.astarte-platform.rust.e2etest.DeviceDatastream";

    pub(crate) const E2E_SERVER_DATASTREAM: &str = include_str!(
        "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.ServerDatastream.json"
    );
    pub(crate) const E2E_SERVER_DATASTREAM_NAME: &str =
        "org.astarte-platform.rust.e2etest.ServerDatastream";
    pub(crate) const E2E_DEVICE_AGGREGATE: &str = include_str!(
        "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceAggregate.json"
    );
    pub(crate) const E2E_DEVICE_AGGREGATE_NAME: &str =
        "org.astarte-platform.rust.e2etest.DeviceAggregate";
    pub(crate) const E2E_DEVICE_PROPERTY: &str = include_str!(
        "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.DeviceProperty.json"
    );
    pub(crate) const E2E_DEVICE_PROPERTY_NAME: &str =
        "org.astarte-platform.rust.e2etest.DeviceProperty";
    pub(crate) const E2E_SERVER_PROPERTY: &str = include_str!(
        "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.ServerProperty.json"
    );
    pub(crate) const E2E_SERVER_PROPERTY_NAME: &str =
        "org.astarte-platform.rust.e2etest.ServerProperty";

    pub(crate) mod for_update {
        pub(crate) const E2E_DEVICE_DATASTREAM_NAME: &str =
            "org.astarte-platform.rust.e2etest.ForUpdateDeviceDatastream";
        pub(crate) const E2E_DEVICE_DATASTREAM_0_1: &str = include_str!(
            "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.ForUpdateDeviceDatastream.json"
        );
        pub(crate) const E2E_DEVICE_DATASTREAM_1_0: &str = include_str!(
            "../e2e-test/interfaces/update/org.astarte-platform.rust.e2etest.ForUpdateDeviceDatastream.json"
        );
    }

    // Interfaces with retention
    pub(crate) const VOLATILE_DEVICE_DATASTREAM: &str = include_str!(
        "../examples/retention/interfaces/org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceDatastream.json"
    );
    pub(crate) const VOLATILE_DEVICE_DATASTREAM_NAME: &str =
        "org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceDatastream";
    pub(crate) const STORED_DEVICE_DATASTREAM: &str = include_str!(
        "../examples/retention/interfaces/org.astarte-platform.rust.examples.individual-datastream.StoredDeviceDatastream.json"
    );
    pub(crate) const STORED_DEVICE_DATASTREAM_NAME: &str =
        "org.astarte-platform.rust.examples.individual-datastream.StoredDeviceDatastream";
    pub(crate) const VOLATILE_DEVICE_OBJECT: &str = include_str!(
        "../examples/retention/interfaces/org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceObject.json"
    );
    pub(crate) const VOLATILE_DEVICE_OBJECT_NAME: &str =
        "org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceObject";
    pub(crate) const STORED_DEVICE_OBJECT: &str = include_str!(
        "../examples/retention/interfaces/org.astarte-platform.rust.examples.individual-datastream.StoredDeviceObject.json"
    );
    pub(crate) const STORED_DEVICE_OBJECT_NAME: &str =
        "org.astarte-platform.rust.examples.individual-datastream.StoredDeviceObject";

    pub(crate) const DEVICE_PROPERTIES_NO_UNSET: &str = r#"{
    "interface_name": "org.astarte-platform.rust.examples.individual-properties.DevicePropertyNoUnset",
    "version_major": 0,
    "version_minor": 1,
    "type": "properties",
    "ownership": "device",
    "mappings": [{
        "endpoint": "/%{sensor_id}/enable",
        "type": "boolean",
        "allow_unset": false
    }]
}"#;

    pub(crate) const SERVER_PROPERTIES_NO_UNSET: &str = r#"{
    "interface_name": "org.astarte-platform.rust.examples.individual-properties.ServerPropertyNoUnset",
    "version_major": 0,
    "version_minor": 1,
    "type": "properties",
    "ownership": "server",
    "mappings": [{
        "endpoint": "/%{sensor_id}/enable",
        "type": "boolean",
        "allow_unset": false
    }]
}"#;
    pub(crate) const SERVER_PROPERTIES_NO_UNSET_NAME: &str =
        "org.astarte-platform.rust.examples.individual-properties.ServerPropertyNoUnset";
}
