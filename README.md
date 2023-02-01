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

### Create a device
Initializing an instance of a device can be performed in three steps, as seen below.
```Rust
use astarte_device_sdk::{
    database::AstarteSqliteDatabase,
    options::AstarteOptions,
    AstarteDeviceSdk};

// (optional) Initialize a database to store the properties
let db = AstarteSqliteDatabase::new("sqlite::memory:").await.unwrap();

// Initialize device options
let sdk_options = AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url)
    .interface_directory("./examples/interfaces")
    .unwrap()
    .database(db);

// Create the device instance
let mut device = AstarteDeviceSdk::new(&sdk_options).await.unwrap();
```
### Publish data from device
Publishing new values can be performed using the `send` and `send_object` functions.
```Rust
// Send individual datastream or set individual property
let data: i32 = 12;
device.send("interface.name", "/endpoint/path", data).await.unwrap();

// Send aggregated object datastream
#[derive(Debug, AstarteAggregate)]
struct MyAggObj {
    endpoint1: f64,
    endpoint2: i32
}
let data = MyAggObj {endpoint1: 1.34, endpoint2: 22};
device.send_object("interface.name", "/common/endpoint/path", data).await.unwrap();
```
### Receive a server publish
Polling for new data can be performed using the function `handle_events`.
```Rust
 loop {
    match device.handle_events().await {
        Ok(data) => (), // Handle data
        Err(err) => (), // Handle errors
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
