<!--
Copyright 2021,2022 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte Device SDK Rust &emsp;

[![Build Status]][actions] [![Latest Version]][crates.io]

[Build Status]: https://img.shields.io/github/actions/workflow/status/astarte-platform/astarte-device-sdk-rust/build-workflow.yaml?branch=master
[actions]: https://github.com/astarte-platform/astarte-device-sdk-rust/actions/workflows/build-workflow.yaml?query=branch%3Amaster
[Latest Version]: https://img.shields.io/crates/v/astarte-device-sdk.svg
[crates.io]: https://crates.io/crates/astarte-device-sdk

*Warning: this SDK is experimental, correctness and API stability are currently not guaranteed*

The Astarte Device SDK for Rust is a ready to use library that provides communication and
pairing primitives to an Astarte Cluster.

See the [Astarte documentation](https://docs.astarte-platform.org/latest/001-intro_user.html)
for more information regarding Astarte and the available SDKs.

## Basic usage

```rust
use astarte_device_sdk::{
    database::AstarteSqliteDatabase,
    options::AstarteOptions,
    AstarteDeviceSdk,
    AstarteError
};

async fn run_astarte_device() -> Result<(), AstarteError> {
    
    let realm = "realm_name";
    let device_id = "device_id";
    let credentials_secret = "device_credentials_secret";
    let pairing_url = "astarte_cluster_pairing_url";

    // Initializing an instance of a device can be performed as shown in the following three steps.
    
    // 1. (optional) Initialize a database to store the properties
    let db = AstarteSqliteDatabase::new("sqlite::memory:").await?;
    
    // 2. Initialize device options (the ".database(db)" is not needed if 1 was skipped)
    let sdk_options = AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url)
        .interface_directory("./examples/interfaces")?
        .database(db);
    
    // 3. Create the device instance
    let mut device = AstarteDeviceSdk::new(&sdk_options).await.unwrap();
    
    // Publishing new values can be performed using the send and send_object functions.
    
    // Send individual datastream or set individual property
    let data: i32 = 12;
    device.send("interface.name", "/endpoint/path", data).await.unwrap();
    
    // Send aggregated object datastream
    use astarte_device_sdk::AstarteAggregate;
    use astarte_device_sdk_derive::AstarteAggregate; 
    
    #[derive(Debug, AstarteAggregate)]
    struct MyAggObj {
        endpoint1: f64,
        endpoint2: i32
    }
    
    let data = MyAggObj {endpoint1: 1.34, endpoint2: 22};
    device.send_object("interface.name", "/common/endpoint/path", data).await.unwrap();
    
    // Polling for new data can be performed using the function handle_events.
    
    // Receive a server publish
    loop {
        match device.handle_events().await {
            Ok(data) => (), // Handle data
            Err(err) => (), // Handle errors
        }
    }
}
```

## Building the library

You can build the library using:
```sh
cargo build
```

## Examples

You can execute one of the examples using the following command (seen for the *simple* example).
```sh
cargo run --example simple -- \
    --credentials-secret <credentials-secret>
    --device-id <device-id>
    --pairing-url <pairing-url>
    --realm <realm>
```
