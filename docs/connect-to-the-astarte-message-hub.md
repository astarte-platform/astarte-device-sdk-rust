<!--
This file is part of Astarte.

Copyright 2025, 2026 SECO Mind Srl

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
-->

# Connect to the Astarte MessageHub

The Astarte Device SDK supports two different connection types:

- **Astarte MQTT Protocol**: this is a direct connection from one device to Astarte
- **Message Hub**: multiple applications or sensors connect to the Hub that appears as a single
  device to Astarte

Here we will go through, step by step, on how to connect and send data to the Message Hub.

> You can find the full code by going to the
> [message hub client example](https://github.com/astarte-platform/astarte-device-sdk-rust/tree/master/examples/message_hub_client)

## Before you begin

There are a few setup steps and requirements that are needed before start working on the example.

First, you should be already familiar on how Astarte and the Device SDK works, so you should pause
here and follow the
[Get Started](https://docs.rs/astarte-device-sdk/latest/astarte_device_sdk/_docs/_get_started/index.html)
tutorial if you haven't already.

### Local Astarte instance

To get started you'll need to setup a local Astarte instance that the message hub is going to
connect to. You can follow the
[Astarte quick instance guide](https://docs.astarte-platform.org/device-sdks/common/astarte_quick_instance.html).
If you follow the guide you'll have an Astarte instance, that must be accessible from the Message
Hub server, and all the keys necessary to register the Message Hub server as a new device.

### Message Hub Server

To register the server you can follow the guide and examples in
[the Astarte MessageHub repository](https://github.com/astarte-platform/astarte-message-hub).
Following this, we assume the Server is listening on the same machine IP v4 address `127.0.0.1` and
port `50051`.

#### Client Auth

At the moment the client doesn't require to Authenticate with the MessageHub, since we are assuming
they are on the same and secure LAN network. The only client's requirement for connecting to the
MessageHub is a unique `UUID`.

## System dependencies

The following dependencies are required to run the examples.

### Rust

We suggest using and up-to-date Rust toolchain, preferably the current stable version. Our
recommended way to install to install is through [rustup](https://rustup.rs/). If you prefer using
your system provided toolchain, make sure it's supported by the SDK Minimum Supported Rust Version
([MSRV](https://doc.rust-lang.org/cargo/reference/rust-version.html)).

### SQLite

> Currently we don't store the property in the SQLite store because the feature is not widely
> supported by other SDKs. This also implies messages with retention volatile ore stored are not
> saved if disconnected.
>
> Although they are not used, the SQLite libraries are still required to compile the application.

The device SDK uses [SQLite](https://www.sqlite.org/) as an in-process database to store Astarte
properties on disk. In order to compile the application, you need to provide a compatible `sqlite3`
library.

To use your system SQLite library, you need to have a C toolchain, `libsqlite3` and `pkg-config`
installed. This way you can link it with your Rust executable. For example, on a Debian/Ubuntu
system you install them through `apt`:

```sh
apt install build-essential pkg-config libsqlite3-dev
```

You can find more information on the [rusqlite GitHub page](https://github.com/rusqlite/rusqlite).

## Creating the project and adding the dependencies

Fist of all, we will create a new binary Rust project:

```bash
cargo new astarte-message-hub-client
cd astarte-message-hub-client
```

Then, we need to add the following dependencies to the project:

- `astarte-device-sdk`: the client library to connect to the MessageHub
- `tokio` async runtime to handle the connection
- `Uuid` library to handle and generate UUIDs.

We also suggest you to add the following dependencies

- `tracing` and `tracing_subscriber` for creating and consuming log events
- `eyre` and `color-eyre` to handle the Errors and convert them in a human readable report

You can run the following cargo add command:

```sh
cargo add astarte-device-sdk --features=derive,message-hub
cargo add tokio --features=full
cargo add tracing tracing-subscriber
cargo add eyre color-eyre
```

The features of the `astarte-device-sdk` used in this guide will be:

- `message-hub` will enable the connection to the MessageHub
- `derive` to derive the conversion of a Rust struct into a MessageHub message

## Creating a Client and Connection

Now that all the dependencies are installed, you need to create a
[`DeviceClient`](crate::client::DeviceClient) to send and receive events from the MessageHub and a
[`DeviceConnection`](crate::connection::DeviceConnection) to handle the connection/reconnection
events and lifetime.

You will use the [`DeviceBuilder`](crate::builder::DeviceBuilder) and
[`GrpcConfig`](crate::transport::grpc::GrpcConfig) to set all the connection parameters used to talk
to the MessageHub. The parameter to configure is the gRPC
[Endpoint](crate::transport::grpc::tonic::transport::Endpoint) where the MessageHub is listening on
(`https://127.0.0.1:50051`).

To store the Properties[^1] on the device we will use the
[`SqliteStore`](crate::store::sqlite::SqliteStore), which uses an SQLite database for persistent
storage. It can be configured by calling the [`store`](crate::builder::DeviceBuilder::store)
function on the builder.

[^1]: To know more on what Properties are, see the
    [Astarte documentation on Properties](https://docs.astarte-platform.org/astarte/latest/030-interface.html#properties)

On the [`DeviceConnection`](crate::connection::DeviceConnection) you need call the
[`handle_events`](crate::connection::DeviceConnection), which is a blocking method that will handle
all the connection events. So, to not block the main task, we will use the
[`JoinSet`](tokio::task::JoinSet) to spawn multiple tasks and join each one at the end of the
program.

```no_run
use astarte_device_sdk::{DeviceClient, DeviceConnection};
use astarte_device_sdk::builder::DeviceBuilder;
use astarte_device_sdk::prelude::*;
use astarte_device_sdk::store::sqlite::SqliteStore;
use astarte_device_sdk::transport::grpc::{tonic::transport::Endpoint, Grpc, GrpcConfig, store::GrpcStore};
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

/// Unique ID for the current application to identify to the message hub with.
const NODE_UUID: Uuid = uuid::uuid!("0444d8c3-f3f1-4b89-9e68-6ffb50ec1839");
/// URL the MessageHub is listening on
const MESSAGE_HUB_URL: &str = "http://127.0.0.1:50051";
/// Writable directory to store the persistent state of the device
const STORE_DIRECTORY: &str = "./store-dir";

async fn init() -> eyre::Result<(
    DeviceClient<Grpc>,
    DeviceConnection<Grpc>,
)> {
    tokio::fs::create_dir_all(STORE_DIRECTORY).await?;

    let endpoint = Endpoint::from_static(&MESSAGE_HUB_URL);
    let grpc_config = GrpcConfig::new(NODE_UUID, endpoint);

    let store = SqliteStore::options().with_writable_dir(STORE_DIRECTORY).await?;

    let (client, connection) = DeviceBuilder::new()
        .writable_dir(STORE_DIRECTORY)
        .store(store)
        .connection(grpc_config)
        .build()
        .await?;

    Ok((client, connection))
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .try_init()?;

    let (_client, connection) = init().await?;

    info!("connected to the MessageHub");

    let mut tasks = JoinSet::<eyre::Result<()>>::new();

    // task to poll updates from the connection
    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

    // Cleanly close all the other tasks
    tasks.spawn(async move {
        tokio::signal::ctrl_c().await?;

        info!("SIGINT received, exiting");

        Ok(())
    });

    // handle tasks termination
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {
                // Close all the other tasks
                tasks.abort_all();
            }
            Ok(Err(err)) => {
                error!("task returned an error");

                return Err(err);
            }
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                error!("task panicked");

                return Err(err.into());
            }
        }
    }

    info!("device disconnected");

    Ok(())
}
```

You can run the application with `cargo run` and see, in the MessageHub logs if the device attached
successfully.

## Sending the introspection

To communicate with Astarte we need to use the Interfaces which define the shape of our data. The
MessageHub doesn't know which Interfaces the applications are going to use. Each application will
register it's own interfaces in Astarte and send them to the MessageHub when connecting.

### Registering the interfaces

In the documentation we
[provided an interface of each type](https://github.com/astarte-platform/astarte-device-sdk-rust/tree/master/docs/interfaces)
to use to send and receive data between the Node and the MessageHub.

You can use the following command to download the interfaces from the repo and sync them with
Astarte.

```sh
# Inside the root of the project
mkdir interfaces
# Download the interfaces
curl --output-dir interfaces --fail --remote-name-all \
    'https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/docs/interfaces/org.astarte-platform.rust.get-started.IndividualDevice.json' \
    'https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/docs/interfaces/org.astarte-platform.rust.get-started.IndividualServer.json' \
    'https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/docs/interfaces/org.astarte-platform.rust.get-started.Aggregated.json' \
    'https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/docs/interfaces/org.astarte-platform.rust.get-started.Property.json'
# Sync them with Astarte
astartectl realm-management interfaces sync \
  --astarte-url http://astarte.localhost \
  --realm-name test \
  --realm-key test_private.pem \
  interfaces/*.json
```

Now all of the above interfaces must be visible in the Astarte dashboard.

### Sending the introspection

To send the Interfaces to the MessageHub and update the device introspection you can call the
[`interface_str`](crate::builder::DeviceBuilder::interface_str) method on the builder in the `init`
function that we declared previously.

```no_run
# use astarte_device_sdk::{
#     builder::DeviceBuilder,
#     prelude::*,
#     transport::grpc::{tonic::transport::Endpoint, Grpc, GrpcConfig, store::GrpcStore},
#     store::sqlite::SqliteStore,
#     DeviceClient, DeviceConnection,
# };
#
# const NODE_UUID: uuid::Uuid = uuid::uuid!("0444d8c3-f3f1-4b89-9e68-6ffb50ec1839");
# const MESSAGE_HUB_URL: &str = "http://127.0.0.1:50051";
# const STORE_DIRECTORY: &str = "./store-dir";
#
// NOTE: set the interface directory in your project, these are relative to this file
const AGGREGATED_DEVICE: &str = include_str!("../../docs/interfaces/org.astarte-platform.rust.get-started.Aggregated.json");
const INDIVIDUAL_DEVICE: &str = include_str!("../../docs/interfaces/org.astarte-platform.rust.get-started.IndividualDevice.json");
const INDIVIDUAL_SERVER: &str = include_str!("../../docs/interfaces/org.astarte-platform.rust.get-started.IndividualServer.json");
const PROPERTY_DEVICE: &str = include_str!("../../docs/interfaces/org.astarte-platform.rust.get-started.Property.json");

async fn init() -> eyre::Result<(
    DeviceClient<Grpc>,
    DeviceConnection<Grpc>,
)> {
    tokio::fs::create_dir_all(STORE_DIRECTORY).await?;

    let endpoint = Endpoint::from_static(&MESSAGE_HUB_URL);
    let grpc_config = GrpcConfig::new(NODE_UUID, endpoint);

    let store = SqliteStore::options().with_writable_dir(STORE_DIRECTORY).await?;

    let (client, connection) = DeviceBuilder::new()
        .writable_dir(STORE_DIRECTORY)
        .store(store)
        .interface_str(AGGREGATED_DEVICE)?
        .interface_str(INDIVIDUAL_DEVICE)?
        .interface_str(INDIVIDUAL_SERVER)?
        .interface_str(PROPERTY_DEVICE)?
        .connection(grpc_config)
        .build()
        .await?;

    Ok((client, connection))
}
```

Now if you try to `cargo run` again you will see the node attaching to the MessageHub, and all the
interfaces will appear in the Device introspection on the Astarte Dashboard.

## Receiving device events

We can now spawn a task to receive data from Astarte. Using the
[`FromEvent`](crate::event::FromEvent)

```no_run
# use astarte_device_sdk::{
#     builder::DeviceBuilder,
#     prelude::*,
#     transport::grpc::{tonic::transport::Endpoint, Grpc, GrpcConfig, store::GrpcStore},
#     DeviceClient, DeviceConnection,
# };
# #[cfg(not(feature = "derive"))]
# use astarte_device_sdk_derive::FromEvent;
# #[cfg(feature = "derive")]
use astarte_device_sdk::FromEvent;
use astarte_device_sdk::client::RecvError;
use eyre::OptionExt;
use tracing::{info, error, warn};

/// Used to receive the IndividualDevice data.
///
/// This need to be an enum because we deserialize each endpoint in it's own variables
#[derive(Debug, FromEvent)]
#[from_event(
    interface = "org.astarte-platform.rust.get-started.IndividualServer",
    aggregation = "individual"
)]
enum ServerIndividual {
    #[mapping(endpoint = "/%{id}/data")]
    Boolean(bool),
}

async fn receive_data(client: DeviceClient<Grpc>) -> eyre::Result<()> {
    loop {
        let event = match client.recv().await {
            Ok(event) => event,
            Err(RecvError::Disconnected) => {
                info!("client disconnected");
                return Ok(());
            }
            Err(err) => {
                error!(error = %eyre::Report::new(err), "received error from client");
                continue;
            }
        };

        match event.interface.as_str() {
            "org.astarte-platform.rust.get-started.IndividualServer" => {
                // parse the path to extract the id part
                // e.g. '/42/data' will strip the '/' and '/data' to return '42'
                let id = event
                    .path
                    .strip_prefix("/")
                    .and_then(|s| s.strip_suffix("/data"))
                    .ok_or_eyre("couldn't get endpoint id parameter")?
                    .to_string();

                let ServerIndividual::Boolean(event) = ServerIndividual::from_event(event)?;

                info!(event, id, "received new datastream on IndividualServer/");
            }
            interface => {
                warn!(interface, "unhandled interface event received");

                continue;
            }
        }
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
# let mut tasks = tokio::task::JoinSet::new();
# fn client() -> DeviceClient<Grpc> { todo!() }
# let client = client();
  // ...

  // receive events from the MessageHub
  tasks.spawn(receive_data(client.clone()));

  // ...
# Ok(())
}
```

You can send data from Astarte on a server-owned interface by using the
`astartectl appengine device publish-datastream` command and the Device Id of the message-hub (e.g.
`2TBn-jNESuuHamE2Zo1anA`), you need to specify the **interface name**
(`org.astarte-platform.rust.get-started.IndividualServer`) you want to publish on and the
**endpoint** (`/42/data` the `41` is in the place of the parameter `/%{id}/data`). Since in this
example the endpoint is of type double, we will send `3.14` as a value:

```sh
astartectl appengine \
    --appengine-url 'http://astarte.localhost' \
    --realm-key 'test_private.pem' \
    --realm-name 'test' \
    devices publish-datastream '2TBn-jNESuuHamE2Zo1anA' 'org.astarte-platform.rust.get-started.IndividualServer' '/42/data' '3.14'
```

The MessageHub will then relay this event to all the Nodes having the given interface in their
introspection.

## Sending data

Finally, we can send data to the MessageHub. We implement a task similar to the receive one, in a
loop every 2 seconds we send the data to all the interfaces. The property one will set and save the
value, and will only send it once to the Server since it doesn't change. While for the `Aggregated`
interface we create a struct and derive the [`IntoAstarteObject`](crate::IntoAstarteObject) that
will convert the Rust struct in an Object Aggregate to send.

```no_run
# use astarte_device_sdk::{
#     builder::DeviceBuilder,
#     prelude::*,
#     transport::grpc::{tonic::transport::Endpoint, Grpc, GrpcConfig, store::GrpcStore},
#     DeviceClient, DeviceConnection,
# };
#
use std::time::Duration;

# #[cfg(feature = "derive")]
use astarte_device_sdk::IntoAstarteObject;
# #[cfg(not(feature = "derive"))]
# use astarte_device_sdk_derive::IntoAstarteObject;

/// Aggregated object
#[derive(Debug, IntoAstarteObject)]
struct AggregatedDevice {
    double_endpoint: f64,
    string_endpoint: String,
}

/// Send data after an interval to every interface
async fn send_data(mut client: DeviceClient<Grpc>) -> eyre::Result<()> {
    // Every 2 seconds send the data
    let mut interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        // Publish on the IndividualDevice
        client
            .send_individual(
                "org.astarte-platform.rust.get-started.IndividualDevice",
                "/double_endpoint",
                3.14.try_into()?,
            )
            .await?;
        // Publish on the Aggregaed
        let obj_data = AggregatedDevice {
            double_endpoint: 42.0,
            string_endpoint: "Sensor 1".to_string(),
        };
        client
            .send_object(
                "org.astarte-platform.rust.get-started.Aggregated",
                "/group_data",
                obj_data.try_into()?,
            )
            .await?;
        // Set the Property
        client
            .send_individual(
                "org.astarte-platform.rust.get-started.Property",
                "/double_endpoint",
                42.0.try_into()?,
            )
            .await?;

        interval.tick().await;
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
# let mut tasks = tokio::task::JoinSet::new();
# fn client() -> DeviceClient<Grpc> { todo!() }
# let client = client();
  // ...

  // send events to the MessageHub
  tasks.spawn(send_data(client));

  // ...
# Ok(())
}
```
