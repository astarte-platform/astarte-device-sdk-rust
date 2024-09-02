<!--
Copyright 2021,2022 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte Device SDK Rust &emsp;

[![Build Status]][actions] [![Latest Version]][crates.io] [![docs.rs]][docs] [![Code coverage]][codecov]

[Build Status]: https://img.shields.io/github/actions/workflow/status/astarte-platform/astarte-device-sdk-rust/ci.yaml?branch=master
[actions]: https://github.com/astarte-platform/astarte-device-sdk-rust/actions/workflows/ci.yaml?query=branch%3Amaster
[Latest Version]: https://img.shields.io/crates/v/astarte-device-sdk.svg
[crates.io]: https://crates.io/crates/astarte-device-sdk
[docs.rs]: https://img.shields.io/docsrs/astarte-device-sdk
[docs]: https://docs.rs/astarte-device-sdk/latest/astarte_device_sdk/
[Code coverage]: https://codecov.io/gh/astarte-platform/astarte-device-sdk-rust/branch/master/graph/badge.svg
[codecov]: https://codecov.io/gh/astarte-platform/astarte-device-sdk-rust

*Warning: this SDK is experimental, correctness and API stability are currently not guaranteed*

The Astarte Device SDK for Rust is a ready to use library that provides communication and
pairing primitives to an Astarte Cluster.

See the [Astarte documentation](https://docs.astarte-platform.org/latest/001-intro_user.html)
for more information regarding Astarte and the available SDKs.

## Basic usage

```rust
use std::error::Error as StdError;

use astarte_device_sdk::{
    builder::DeviceBuilder,
    transport::mqtt::MqttConfig,
    error::Error,
    prelude::*,
    store::sqlite::SqliteStore,
};

async fn run_astarte_device() -> Result<(), Box<dyn StdError>> {

    let realm = "realm_name";
    let device_id = "device_id";
    let credentials_secret = "device_credentials_secret";
    let pairing_url = "astarte_cluster_pairing_url";

    // Initializing an instance of a device can be performed as shown in the following three steps.

    // 1. (optional) Initialize a database to store the properties
    let db = SqliteStore::from_uri("sqlite::memory:").await?;

    // 2. Initialize device options and mqtt config (the ".database(db)" is not needed if 1 was skipped)
    let mut mqtt_config = MqttConfig::with_credential_secret(realm, device_id, credentials_secret, pairing_url);
    mqtt_config.ignore_ssl_errors();

    // 3. Create the device instance
    let (mut client, mut connection) = DeviceBuilder::new()
        .interface_directory("./examples/interfaces")?
        .store(db)
        .connect(mqtt_config).await?
        .build().await;

    // Publishing new values can be performed using the send and send_object functions.

    // Send individual datastream or set individual property
    let data: i32 = 12;
    client.send("interface.name", "/endpoint/path", data).await?;

    // Send aggregated object datastream
    use astarte_device_sdk::AstarteAggregate;
    // If the derive feature is not enabled
    #[cfg(not(feature = "derive"))]
    use astarte_device_sdk_derive::AstarteAggregate;

    #[derive(Debug, AstarteAggregate)]
    struct MyAggObj {
        endpoint1: f64,
        endpoint2: i32
    }

    let data = MyAggObj {endpoint1: 1.34, endpoint2: 22};
    client.send_object("interface.name", "/common/endpoint/path", data).await?;

    // Receive a server publish from the event channel
    tokio::spawn(async move {
        loop {
          match client.recv().await {
              Ok(data) => (), // Handle data
              Err(err) => (), // Handle errors
          }
        }
    });

    // Blocking call for the device event loop
    connection.handle_events().await?;

    Ok(())
}
```

## Building the library

You can build the library using:
```sh
cargo build
```

## Examples

Check out how to start with the SDK using one of the [included examples](./examples/README.md).
