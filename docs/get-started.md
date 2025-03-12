<!--
Copyright 2025 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Get started with Rust

Follow this guide to get started with the Astarte device SDK for the Rust programming language. We
will guide you through setting up a very basic Rust application creating a device, connecting it to
a local Astarte instance and transmitting some data.

## Before you begin

There are a few setup steps and requirements that are needed before start working on the example.

### Local Astarte instance

This get started will focus on creating a device and connecting it to an Astarte instance. If you
don't have access to an Astarte instance you can easily set up one following our
[Astarte quick instance guide](https://docs.astarte-platform.org/device-sdks/common/astarte_quick_instance.html).

From here on we will assume you have access to an Astarte instance, remote or on a local machine
connected to the same LAN where your device will be connected. Furthermore, we will assume you have
access to the Astarte dashboard for a realm. The next steps will install the required interfaces and
register a new device on Astarte using the dashboard. The same operations could be performed using
`astartectl` and the access token generated in the Astarte quick instance guide.

### Registering the device

Devices should be pre-registered to Astarte before their first connection. With the Astarte device
SDK for Rust this can be achieved in two ways:

- By registering the device on Astarte manually, obtaining a credentials secret and transferring it
  on the device.
- By using the included registration utilities provided by the SDK. Those utilities can make use of
  a registration JWT issued by Astarte and register the device automatically before the first
  connection.

To keep this guide as simple as possible we will use the first method, as a device can be registered
using the Astarte dashboard with a couple of clicks.

To install a new device start by opening the dashboard and navigate to the devices tab. Click on
register a new device, there you can input your own device ID or generate a random one. For example
you could use the device ID `2TBn-jNESuuHamE2Zo1anA`. Click on register device, this will register
the device and give you a credentials secret. The credentials secret will be used by the device to
authenticate itself with Astarte. Copy it somewhere safe as it will be used in the next steps.

## Creating a Rust project

First, make sure you have the proper Rust toolchain installed on your machine. We recommend using
[rustup](https://rustup.rs/), otherwise make sure you are using one supported by the
[MSRV](https://doc.rust-lang.org/cargo/reference/rust-version.html) of the Astarte device Rust SDK

Then, create a new project:

```bash
cargo new astarte-rust-project
```

Then, we need to add the following dependencies in the Cargo.toml file:

- `astarte-device-sdk`, to properly use the Astarte SDK
- `tokio`, a runtime for writing asynchronous applications
- `serde` and `serde_json`, for serializing and deserializing Rust data structures. They are useful
  since we want to retrieve some device configuration stored in a json file

We also suggest you to add the following dependencies

- `tracing` and `tracing_subscriber` for printing and showing the logs of the SDK
- `eyre` to convert and report the error into a single trait object

You can run the following cargo add command:

```sh
cargo add astarte-device-sdk --features=derive
cargo add tokio --features=full
cargo add serde serde-json --features=serde/derive
cargo add tracing tracing-subscriber
cargo add color-eyre
```

### System dependencies

The device SDK uses an [SQLite](https://www.sqlite.org/) as an in-process database to store Astarte
properties on disk. In order to compile the application, you need to provide a compatible `sqlite3`
library.

To use your system SQLite library, you need to have a C toolchain, `libsqlite3` and `pkg-config`
installed. This way you can link it with your Rust executable. For example, on a Debian/Ubuntu
system you install them through `apt`:

```sh
apt install build-essential pkg-config libsqlite3-dev
```

You can find more information on the [rusqlite GitHub page](https://github.com/rusqlite/rusqlite).

## Configuration

To easily load the Astarte configuration information, such as the realm name, the astarte instance
endpoint, the device id and the pairing url, you could set some environment variables or store them
in a `config.json` file, like the following:

```json
{
  "realm": "Realm name",
  "device_id": "Device ID",
  "credentials_secret": "Credentials secret",
  "pairing_url": "Pairing URL"
}
```

We previously added three interfaces to our Astarte instance. We also need to save the three
interfaces in JSON files, for instance in a `interfaces` folder in your working directory. These
will be retrieved, parsed and then used during the device connection

## Instantiating and connecting a device

Now we can start writing the source code of our device application. We will first create a new
device using the device ID and credentials secret we obtained in the previous steps. Then, the
device will need to be polled regularly to ensure the processing of MQTT messages. To this extent,
we can spawn a tokio task (the equivalent of an OS thread but managed by the Tokio runtime) to poll
connection messages. Ideally, two separate tasks should be used for both polling and transmission.

```no_run
use astarte_device_sdk::{
    builder::DeviceBuilder, prelude::*, store::SqliteStore, DeviceClient,
    transport::mqtt::{Mqtt, MqttConfig}, DeviceConnection,
};
use color_eyre::eyre;
use serde::Deserialize;
use tracing::{info, error};
use tracing_subscriber;
use tokio::task::JoinSet;

/// structure used to deserialize the content of the config.json file containing the
/// astarte device connection information
#[derive(Deserialize)]
struct Config {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
}

/// Load connection configuration and connect a device to Astarte.
async fn init() -> eyre::Result<(DeviceClient<SqliteStore>, DeviceConnection<SqliteStore, Mqtt<SqliteStore>>)> {
    // Load the device configuration
    let file = tokio::fs::read_to_string("config.json").await?;
    let cfg: Config = serde_json::from_str(&file)?;

    let mut mqtt_config = MqttConfig::with_credential_secret(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    );
    mqtt_config.ignore_ssl_errors();

    // connect to a db in the current working directory
    // if it doesn't exist, the method will create it
    let store = SqliteStore::connect_db("./store.db").await?;

    let (client, connection) = DeviceBuilder::new()
        .store(store)
        // NOTE: here we are not defining any Astarte interface, thus the device will not be able to
        // send or receive data to/from Astarte
        .connection(mqtt_config)
        .build()
        .await?;

    Ok((client, connection))
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let (client, connection) = init().await?;

    info!("Connection to Astarte established.");

    // define a set of tasks to be spawned
    let mut tasks = JoinSet::<eyre::Result<()>>::new();

    // task to poll updates from the connection
    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

    // ...
    // here we will insert other pieces of code to handle receiving and sending data to Astarte
    // ...

    // handle tasks termination
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                error!(error = %err, "Task returned an error");
                return Err(err);
            }
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                error!(error = %err, "Task panicked");
                return Err(err.into());
            }
        }
    }

    // disconnect the device once finished processing all the tasks
    client.disconnect().await?;

    info!("Device disconnected from Astarte");

    Ok(())
}
```

You can run the application with `cargo run` and see in the Astarte Dashboard that the device
appears as connected. You could also set the `RUST_LOG` env variable to the desired log level in
order to show some logs during the program execution.

## Installing the required interfaces

Up to now we have connected a device to Astarte, but we haven't installed any interface the device
must use to send and/or receive data to/from Astarte. Since we want to show how to stream individual
and aggregated data as well as how to set and unset properties, we first need to install the
required interfaces.

The following is the definition of the individually aggregated device-owned interface, used by the
device to send data to Astarte:

```json
{
  "interface_name": "org.astarte-platform.rust.get-started.IndividualDevice",
  "version_major": 0,
  "version_minor": 1,
  "type": "datastream",
  "ownership": "device",
  "description": "Individual device-owned interface for the get-started of the Astarte device SDK for the Rust programming language.",
  "mappings": [
    {
      "endpoint": "/double_endpoint",
      "type": "double",
      "explicit_timestamp": false
    }
  ]
}
```

The following is the definition of the individually aggregated server-owned interface, used by the
device to receive data from Astarte:

```json
{
  "interface_name": "org.astarte-platform.rust.get-started.IndividualServer",
  "version_major": 0,
  "version_minor": 1,
  "type": "datastream",
  "ownership": "server",
  "description": "Individual server-owned interface for the get-started of the Astarte device SDK for the Rust programming language.",
  "mappings": [
    {
      "endpoint": "/%{id}/data",
      "type": "double",
      "explicit_timestamp": true
    }
  ]
}
```

Next is the definition of an object aggregated interface:

```json
{
  "interface_name": "org.astarte-platform.rust.get-started.Aggregated",
  "version_major": 0,
  "version_minor": 1,
  "type": "datastream",
  "aggregation": "object",
  "ownership": "device",
  "description": "Aggregated interface for the get-started of the Astarte device SDK for the Rust programming language.",
  "mappings": [
    {
      "endpoint": "/group_data/double_endpoint",
      "type": "double",
      "explicit_timestamp": false
    },
    {
      "endpoint": "/group_data/string_endpoint",
      "type": "string",
      "explicit_timestamp": false
    }
  ]
}
```

And finally the definition of the property interface:

```json
{
  "interface_name": "org.astarte-platform.rust.get-started.Property",
  "version_major": 0,
  "version_minor": 1,
  "type": "properties",
  "ownership": "device",
  "description": "Property interface for the get-started of the Astarte device SDK for the Rust programming language.",
  "mappings": [
    {
      "endpoint": "/double_endpoint",
      "type": "double",
      "allow_unset": true
    }
  ]
}
```

These must also be saved as `JSON` files (in the format `<INTERFACE_NAME>.json`) in a directory
which will then be used when building th SDK. Thus:

1. Create an `interface` directory
   ```sh
   mkdir interfaces
   ```

2. Save the previously shown interfaces in JSON files or download them using the `curl` command as
   follows:
   ```bash
   curl --output-dir interfaces --fail --remote-name-all \
       'https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/docs/interfaces/org.astarte-platform.rust.get-started.IndividualDevice.json' \
       'https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/docs/interfaces/org.astarte-platform.rust.get-started.IndividualServer.json' \
       'https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/docs/interfaces/org.astarte-platform.rust.get-started.Aggregated.json' \
       'https://raw.githubusercontent.com/astarte-platform/astarte-device-sdk-rust/refs/heads/master/docs/interfaces/org.astarte-platform.rust.get-started.Property.json'
   ```

To install them in the Astarte instance, you could use one of the following methodologies:

- Open the Astarte dashboard, navigate to the interfaces tab and click on install new interface.
  Then copy and paste the JSON files for each interface in the right box overwriting the default
  template.
- Use the `astartectl` tool as follows:
  ```bash
  astartectl realm-management interfaces sync -u <ASTARTE_URL> -r <REALM_NAME> \
    -k <REALM_PRIV_KEY> <INTRERFACE_DIR/*json>
  ```

## Receiving device events

We can now spawn a task to receive data from Astarte.

NOTE: remember to tell the `DeviceBuilder` the directory from where to take the Astarte interfaces

```no_run
// ... imports, structs definition ...

# use astarte_device_sdk::{
#     builder::DeviceBuilder,
#     client::RecvError,
#     prelude::*,
#     store::SqliteStore,
#     transport::mqtt::{Mqtt, MqttConfig},
#     DeviceClient, DeviceConnection,
# };
# use color_eyre::eyre;
# use serde::Deserialize;
# use tokio::task::JoinSet;
# use tracing::{error, info};
# /// Load connection configuration and connect a device to Astarte.
# async fn init() -> eyre::Result<(DeviceClient<SqliteStore>,DeviceConnection<SqliteStore, Mqtt<SqliteStore>>)> {
#     todo!()
# }

#[tracing::instrument(skip_all)]
async fn receive_data(client: DeviceClient<SqliteStore>) -> eyre::Result<()> {
    loop {
        match client.recv().await {
            Ok(data) => {
                if let astarte_device_sdk::Value::Individual(var) = data.data {
                    // we want to analyze a mapping similar to "/id/data" so we split by '/' and use the
                    // parts of interest
                    let mut iter = data.path.splitn(3, '/').skip(1);

                    let id = iter
                        .next()
                        .to_owned()
                        .map(|s| s.to_string())
                        .ok_or(eyre::eyre!("Incorrect error received"))?;

                    match iter.next() {
                        Some("data") => {
                            let value: f64 = var.try_into()?;
                            info!(
                                "Received new data datastream for LED {}. LED data is now {}",
                                id, value
                            );
                        }
                        item => {
                            error!("unrecognized {item:?}")
                        }
                    }
                }
            }
            Err(RecvError::Disconnected) => return Ok(()),
            Err(err) => error!(%err),
        }
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // ... configure networking, instantiate the mqtt connection information ...
#   let (client, connection) = init().await?;
#   // define a set of tasks to be spawned
#   let mut tasks = JoinSet::<eyre::Result<()>>::new();

    // Modify the init() function by adding the astarte interfaces directory
    /*
    let (client, connection) = DeviceBuilder::new()
        .store(store)
        .interface_directory("interfaces")?
        .connection(mqtt_config)
        .build()
        .await;
    */

    // Spawn a task to receive data from Astarte
    let client_cl = client.clone();
    tasks.spawn(receive_data(client_cl));

    // ... handle tasks termination and client disconnection ...
#   Ok(())
}
```

You can simulate sending data from Astarte on a server-owned interface by using the
`publish-datastream` option of the `astartectl` tool:

```sh
astartectl appengine
    --appengine-url '<ASTARTE_APPENGINE_URL>' \
    --realm-management-url '<ASTARTE_REALM_MANAGEMENT_URL>' \
    --realm-key '<REALM>_private.pem' \
    --realm-name '<REALM>' \
    devices publish-datastream '<DEVICE_ID>' '<SARVER_OWNED_INTERFACE_NAME>' '<ENDPOINT>' '<VALUE>'
```

Where `<ASTARTE_APPENGINE_URL>` and `<ASTARTE_REALM_MANAGEMENT_URL>` are the appengine and
realm-management respective endpoints, `<REALM>` is your realm name, `<DEVICE_ID>` is the device ID
to send the data to, `<ENDPOINT>` is the endpoint to send data to, which in this example should be
composed by a LED id and the `data` endpoint, and `<VALUE>` is the value to send.

For instance, if you are using a local Astarte instance, created a realm named `test` and registered
the device `2TBn-jNESuuHamE2Zo1anA`, you could send data as follows:

```sh
astartectl appengine \
    --appengine-url 'http://api.astarte.localhost/appengine' \
    --realm-management-url 'http://api.astarte.localhost/realmmanagement' \
    --realm-key 'test_private.pem' \
    --realm-name 'test' \
    devices publish-datastream '2TBn-jNESuuHamE2Zo1anA' 'org.astarte-platform.rust.get-started.IndividualServer' '/id_123/data' '12.34'
```

## Streaming data

Streaming of data could be performed for device owned interfaces of `individual` or `object`
aggregation type.

### Streaming individual data

In Astarte interfaces with `individual` aggregation, each mapping is treated as an independent value
and is managed individually.

The snippet below shows how to send a value that will be inserted into the `"/double_endpoint"`
datastream, that is part of the `"org.astarte-platform.rust.get-started.Individual"` datastream
interface.

```no_run
// ... imports, structs definition ...
# use astarte_device_sdk::{
#     client::RecvError, prelude::*, store::SqliteStore, transport::mqtt::Mqtt, DeviceClient,
#     DeviceConnection,
# };
# use color_eyre::eyre;
# use tokio::task::JoinSet;
# use tracing::info;
# /// Load connection configuration and connect a device to Astarte.
# async fn init() -> eyre::Result<(DeviceClient<SqliteStore>,DeviceConnection<SqliteStore, Mqtt<SqliteStore>>)> {
#     todo!()
# }

#[tracing::instrument(skip_all)]
async fn send_individual(client: DeviceClient<SqliteStore>) -> eyre::Result<()> {
    // send data every 1 sec
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut data = 1.0;

    loop {
        client
            .send(
                "org.astarte-platform.rust.get-started.IndividualDevice",
                "/double_endpoint",
                data,
            )
            .await?;

        info!("Data sent on endpoint /double_endpoint, content: {data}");

        data += 3.14;
        interval.tick().await;
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // ... configure networking, instantiate and connect the device ...
    // ... spawn task to handle polling from the connection ...
#   let (client, _connection) = init().await?;
#   // define a set of tasks to be spawned
#   let mut tasks = JoinSet::<eyre::Result<()>>::new();

    // Create a task to send individual datastream to Astarte
    let client_cl = client.clone();
    tasks.spawn(send_individual(client_cl));

    // ... handle tasks termination and client disconnection ...
#   Ok(())
}
```

### Streaming aggregated data

In Astarte interfaces with `object` aggregation, Astarte expects the owner to send all of the
interface's mappings at the same time, packed in a single message.

The following snippet shows how to send a value for an object-aggregated interface. In this example,
two different data types will be sent together and will be inserted into the `"/group_data"`
datastream, which is part of the `"org.astarte-platform.rust.get-started.Aggregated"` datastream
interface.

```no_run
// ... imports, structs definition ...

# use astarte_device_sdk::{
#     client::RecvError, prelude::*, store::SqliteStore, transport::mqtt::Mqtt, DeviceClient,
#     DeviceConnection,
# };
# use color_eyre::eyre;
# use tokio::task::JoinSet;
# use tracing::info;
# #[cfg(not(feature = "derive"))]
# use astarte_device_sdk_derive::AstarteAggregate;
# /// Load connection configuration and connect a device to Astarte.
# async fn init() -> eyre::Result<(DeviceClient<SqliteStore>,DeviceConnection<SqliteStore, Mqtt<SqliteStore>>)> {
#     todo!()
# }

#[derive(Debug, AstarteAggregate)]
struct DataObject {
    double_endpoint: f64,
    string_endpoint: String,
}

#[tracing::instrument(skip_all)]
async fn send_aggregate(client: DeviceClient<SqliteStore>) -> eyre::Result<()> {
    // send data every 1 sec
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

    let mut value = 1.0;

    loop {

        let data = DataObject {
            double_endpoint: value,
            string_endpoint: "Hello world.".to_string(),
        };

        info!("Sending {data:?}");
        client
            .send_object(
                "org.astarte-platform.rust.get-started.Aggregated",
                "/group_data",
                data,
            )
            .await?;

        value += 3.14;
        interval.tick().await;
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // ... configure networking, instantiate and connect the device ...
    // ... spawn task to handle polling from the connection ...
#   let (client, _connection) = init().await?;
#   // define a set of tasks to be spawned
#   let mut tasks = JoinSet::<eyre::Result<()>>::new();

    // Create a task to send aggregate datastream to Astarte
    let client_cl = client.clone();
    tasks.spawn(send_aggregate(client_cl));

    // ... handle tasks termination and client disconnection ...
#   Ok(())
}
```

## Setting and unsetting properties

Interfaces of the `property` type represent a persistent, stateful, synchronized state with no
concept of history or timestamping. From a programming point of view, setting and unsetting
properties of device-owned interfaces is rather similar to sending messages on datastream
interfaces.

The following snippet shows how to set a value that will be inserted into the `"/double_endpoint"`
property, that is part of `"org.astarte-platform.rust.get-started.Property"` device-owned properties
interface.

```no_run
// ... imports, structs definition ...
# use astarte_device_sdk::{
#     builder::DeviceBuilder, client::{DeviceClient, RecvError}, prelude::*, store::SqliteStore,
#     transport::mqtt::{Mqtt, MqttConfig}, DeviceConnection,
# };
# use color_eyre::eyre;
# use serde::Deserialize;
# use tokio::task::JoinSet;
# use tracing::{error, info};
# use tracing_subscriber;
# async fn init() -> eyre::Result<(DeviceClient<SqliteStore>,DeviceConnection<SqliteStore, Mqtt<SqliteStore>>)> {
#     todo!()
# }

#[tracing::instrument(skip_all)]
async fn send_property(client: DeviceClient<SqliteStore>) -> eyre::Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

    let mut data = 1.0;

    loop {
        client
            .send(
                "org.astarte-platform.rust.get-started.Property",
                "/double_endpoint",
                data,
            )
            .await?;

        info!("Data sent on endpoint /double_endpoint, content: {data}");

        // wait 1 sec before unsetting the property
        interval.tick().await;

        client.unset("org.astarte-platform.rust.get-started.Property", "/double_endpoint").await?;

        info!("Unset property on /double_endpoint endpoint");

        data += 3.14;
        interval.tick().await;
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // ... configure networking, instantiate and connect the device ...
    // ... spawn task to handle polling from the connection ...
#   let (client, _connection) = init().await?;
#   // define a set of tasks to be spawned
#   let mut tasks = JoinSet::<eyre::Result<()>>::new();

    // Create a task to set and unset an Astarte property
    let client_cl = client.clone();
    tasks.spawn(send_property(client_cl));

    // ... handle tasks termination and client disconnection ...
#   Ok(())
}
```

It should be noted how a property should be marked as unsettable in its interface definition to be
able to use the unsetting method on it.

See the more complete code samples in the
[GitHub repository](https://github.com/astarte-platform/astarte-device-sdk-rust) of the Rust Astarte
device SDK for more information on how to receive data from Astarte, such as server owned
properties.
