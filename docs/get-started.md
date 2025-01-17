<!--
Copyright 2025 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Get started with Rust

Follow this guide to get started with the Astarte device SDK for the Rust programming language. We
will guide you through setting up a very basic Rust application creating a device, connecting it to
a local Astarte instance and transmitting some data.

## Before you begin

### Local Astarte instance

This get started will focus on creating a device and connecting it to an Astarte instance. If you
don't have access to an Astarte instance you can easily set up one following our
[Astarte quick instance guide](https://docs.astarte-platform.org/device-sdks/common/astarte_quick_instance.html).

From here on we will assume you have access to an Astarte instance, remote or on a host machine
connected to the same LAN where your device will be connected. Furthermore, we will assume you have
access to the Astarte dashboard for a realm. The next steps will install the required interfaces and
register a new device on Astarte using the dashboard. The same operations could be performed using
`astartectl` and the access token generated in the Astarte quick instance guide.

### Installing the required interfaces

The interfaces that our device will use must first be installed within the Astarte instance. In this
guide we will show how to stream individual and aggregated data as well as how to set and unset
properties. As a consequence we will need three separated interfaces, one for each data type.

The following is the definition of the individually aggregated interface:

```json
{
  "interface_name": "org.astarte-platform.rust.get-started.Individual",
  "version_major": 0,
  "version_minor": 1,
  "type": "datastream",
  "ownership": "device",
  "description": "Individual interface for the get-started of the Astarte device SDK for the Rust programming language.",
  "mappings": [
    {
      "endpoint": "/double_endpoint",
      "type": "double",
      "explicit_timestamp": false
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

To install the three interfaces in the Astarte instance, open the Astarte dashboard, navigate to the
interfaces tab and click on install new interface. You can copy and paste the JSON files for each
interface in the right box overwriting the default template.

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

First, make sure you have Rust v1.78.0 or later installed, since it is the MSRV of the Astarte
device Rust SDK. Then, create a new project:

```bash
cargo new astarte-rust-project
```

Then, we need to add the following dependencies in the Cargo.toml file:

- `astarte-device-sdk`, to properly use the Astarte SDK
- `tokio`, a runtime for writing asynchronous applications
- `serde` and `serde_json`, for serializing and deserializing Rust data structures. They are useful
  since we want to retrieve some device configuration stored in a json file.

We also suggest you to add the following dependencies

- `tracing` and `env_logger`, for a better logging experience

Thus, the content of the Cargo.toml file will:

```toml
[package]
name = "astarte-rust-project"
version = "0.1.0"
edition = "2024"

[dependencies]
astarte-device-sdk = { version = "0.9.2", features = ["derive"] }
tokio = { version = "1.42.0", features = ["signal"] }
serde = { version = "1.0.217", features = ["derive"] }
tracing = "0.1.41"
env_logger = "0.11.5"
serde_json = "1.0.135"
```

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

Finally, we can start with the source code of our device application. We will first create a new
device using the device ID and credentials secret we obtained in the previous steps.

```no_run
use std::time::{Duration, SystemTime};

use astarte_device_sdk::{
    builder::DeviceBuilder, client::RecvError, prelude::*, store::memory::MemoryStore,
    transport::mqtt::MqttConfig,
};
use serde::Deserialize;
use tokio::task::JoinSet;
use tracing::{error, info};

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// structure used to deserialize the content of the config.json file containing the
/// astarte device connection information
#[derive(Deserialize)]
struct Config {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    env_logger::init();
    let now = SystemTime::now();

    // Load the device configuration
    let file = std::fs::read_to_string("configuration.json")?;
    let cfg: Config = serde_json::from_str(&file)?;

    let mut mqtt_config = MqttConfig::with_credential_secret(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    );
    mqtt_config.ignore_ssl_errors();

    let (client, connection) = DeviceBuilder::new()
        .store(MemoryStore::new())
        .interface_directory("interfaces")?
        .connect(mqtt_config)
        .await?
        .build()
        .await;

    info!("Connection to Astarte established.");

    Ok(())
}
```

## Polling device events

After device initialization and connection, the device will need to be polled regularly to ensure
the processing of MQTT messages.

To this extent, we can spawn a tokio task (the equivalent of an OS thread but managed by the Tokio
runtime) to poll connection messages. Ideally, two separate tasks should be used for both polling
and transmission.

```no_run
// ... imports, structs definition ...

# use std::time::{Duration, SystemTime};
# use astarte_device_sdk::{
#     builder::DeviceBuilder, client::RecvError, prelude::*, store::memory::MemoryStore,
#     transport::mqtt::MqttConfig,
# };
# use serde::Deserialize;
# use tokio::task::JoinSet;
# use tracing::{error, info};
# type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
# /// structure used to deserialize the content of the config.json file containing the
# /// astarte device connection information
# #[derive(Deserialize)]
# struct Config {
#     realm: String,
#     device_id: String,
#     credentials_secret: String,
#     pairing_url: String,
# }
#[tokio::main]
async fn main() -> Result<(), DynError> {
#   env_logger::init();
#   let now = SystemTime::now();
#   // Load the device configuration
#   let file = std::fs::read_to_string("configuration.json")?;
#   let cfg: Config = serde_json::from_str(&file)?;
#   let mut mqtt_config = MqttConfig::with_credential_secret(
#       &cfg.realm,
#       &cfg.device_id,
#       &cfg.credentials_secret,
#       &cfg.pairing_url,
#   );
#   mqtt_config.ignore_ssl_errors();
#   let (client, connection) = DeviceBuilder::new()
#       .store(MemoryStore::new())
#       .interface_directory("interfaces")?
#       .connect(mqtt_config)
#       .await?
#       .build()
#       .await;
#   info!("Connection to Astarte established.");
    // ... configure networking, instantiate and connect the device ...

    // define a set of tasks to be spawned
    let mut tasks = JoinSet::<Result<(), DynError>>::new();

    // task to poll updates from the connection
    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

    // properly handle tasks termination
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(res) => {
                res?;

                tasks.abort_all();
            }
            Err(err) if err.is_cancelled() => {}
            Err(err) => return Err(err.into()),
        }
    }

    // disconnect the device once finished processing all the tasks
    client.disconnect().await?;

    Ok(())
}
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

# use std::time::{Duration, SystemTime};
# use astarte_device_sdk::{
#     builder::DeviceBuilder, client::RecvError, prelude::*, store::memory::MemoryStore,
#     transport::mqtt::MqttConfig,
# };
# use serde::Deserialize;
# use tokio::task::JoinSet;
# use tracing::{error, info};
# type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
# /// structure used to deserialize the content of the config.json file containing the
# /// astarte device connection information
# #[derive(Deserialize)]
# struct Config {
#     realm: String,
#     device_id: String,
#     credentials_secret: String,
#     pairing_url: String,
# }
#[tokio::main]
async fn main() -> Result<(), DynError> {
#   env_logger::init();
#   let now = SystemTime::now();
#   // Load the device configuration
#   let file = std::fs::read_to_string("configuration.json")?;
#   let cfg: Config = serde_json::from_str(&file)?;
#   let mut mqtt_config = MqttConfig::with_credential_secret(
#       &cfg.realm,
#       &cfg.device_id,
#       &cfg.credentials_secret,
#       &cfg.pairing_url,
#   );
#   mqtt_config.ignore_ssl_errors();
#   let (client, connection) = DeviceBuilder::new()
#       .store(MemoryStore::new())
#       .interface_directory("interfaces")?
#       .connect(mqtt_config)
#       .await?
#       .build()
#       .await;
#   info!("Connection to Astarte established.");
#   // define a set of tasks to be spawned
#   let mut tasks = JoinSet::<Result<(), DynError>>::new();
#   // task to poll updates from the connection
#   tasks.spawn(async move {
#       connection.handle_events().await?;
#       Ok(())
#   });
    // ... configure networking, instantiate and connect the device ...
    // ... spawn task to handle polling from the connection ...

    let client_cl = client.clone();

    // Create a task to send data to Astarte
    tasks.spawn(async move {
        // send data every 1 sec
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let data = now.elapsed()?.as_secs_f64();
            client_cl
                .send(
                    "org.astarte-platform.rust.get-started.Individual",
                    "/double_endpoint",
                    data,
                )
                .await?;

            info!("Data sent on endpoint /double_endpoint, content: {data}");
        }
    });

    // ... handle tasks termination and client disconnection ...
#   // properly handle tasks termination
#   while let Some(res) = tasks.join_next().await {
#       match res {
#           Ok(res) => {
#               res?;
#               tasks.abort_all();
#           }
#           Err(err) if err.is_cancelled() => {}
#           Err(err) => return Err(err.into()),
#       }
#   }
#   // disconnect the device once finished processing all the tasks
#   client.disconnect().await?;
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

# use std::time::{Duration, SystemTime};
# use astarte_device_sdk::{
#     builder::DeviceBuilder, client::RecvError, prelude::*, store::memory::MemoryStore,
#     transport::mqtt::MqttConfig,
# };
# use serde::Deserialize;
# use tokio::task::JoinSet;
# use tracing::{error, info};
# type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
# /// structure used to deserialize the content of the config.json file containing the
# /// astarte device connection information
# #[derive(Deserialize)]
# struct Config {
#     realm: String,
#     device_id: String,
#     credentials_secret: String,
#     pairing_url: String,
# }
#[derive(Debug, astarte_device_sdk_derive::AstarteAggregate)]
struct DataObject {
    double_endpoint: f64,
    string_endpoint: String,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
#   env_logger::init();
#   let now = SystemTime::now();
#   // Load the device configuration
#   let file = std::fs::read_to_string("configuration.json")?;
#   let cfg: Config = serde_json::from_str(&file)?;
#   let mut mqtt_config = MqttConfig::with_credential_secret(
#       &cfg.realm,
#       &cfg.device_id,
#       &cfg.credentials_secret,
#       &cfg.pairing_url,
#   );
#   mqtt_config.ignore_ssl_errors();
#   let (client, connection) = DeviceBuilder::new()
#       .store(MemoryStore::new())
#       .interface_directory("interfaces")?
#       .connect(mqtt_config)
#       .await?
#       .build()
#       .await;
#   info!("Connection to Astarte established.");
#   // define a set of tasks to be spawned
#   let mut tasks = JoinSet::<Result<(), DynError>>::new();
#   // task to poll updates from the connection
#   tasks.spawn(async move {
#       connection.handle_events().await?;
#       Ok(())
#   });
    // ... configure networking, instantiate and connect the device ...
    // ... spawn task to handle polling from the connection ...

    let client_cl = client.clone();

    // Create a task to send data to Astarte
    tasks.spawn(async move {
        // send data every 1 sec
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let data = DataObject {
                double_endpoint: 1.34,
                string_endpoint: "Hello world.".to_string(),
            };

            info!("Sending {data:?}");
            client_cl
                .send_object(
                    "org.astarte-platform.rust.get-started.Aggregated",
                    "/group_data",
                    data,
                )
                .await?;
        }
    });

    // ... handle tasks termination and client disconnection ...
#   // properly handle tasks termination
#   while let Some(res) = tasks.join_next().await {
#       match res {
#           Ok(res) => {
#               res?;
#               tasks.abort_all();
#           }
#           Err(err) if err.is_cancelled() => {}
#           Err(err) => return Err(err.into()),
#       }
#   }
#   // disconnect the device once finished processing all the tasks
#   client.disconnect().await?;
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

# use std::time::{Duration, SystemTime};
# use astarte_device_sdk::{
#     builder::DeviceBuilder, client::RecvError, prelude::*, store::memory::MemoryStore,
#     transport::mqtt::MqttConfig,
# };
# use serde::Deserialize;
# use tokio::task::JoinSet;
# use tracing::{error, info};
# type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
# /// structure used to deserialize the content of the config.json file containing the
# /// astarte device connection information
# #[derive(Deserialize)]
# struct Config {
#     realm: String,
#     device_id: String,
#     credentials_secret: String,
#     pairing_url: String,
# }
#[tokio::main]
async fn main() -> Result<(), DynError> {
#   env_logger::init();
#   let now = SystemTime::now();
#   // Load the device configuration
#   let file = std::fs::read_to_string("configuration.json")?;
#   let cfg: Config = serde_json::from_str(&file)?;
#   let mut mqtt_config = MqttConfig::with_credential_secret(
#       &cfg.realm,
#       &cfg.device_id,
#       &cfg.credentials_secret,
#       &cfg.pairing_url,
#   );
#   mqtt_config.ignore_ssl_errors();
#   let (client, connection) = DeviceBuilder::new()
#       .store(MemoryStore::new())
#       .interface_directory("interfaces")?
#       .connect(mqtt_config)
#       .await?
#       .build()
#       .await;
#   info!("Connection to Astarte established.");
#   // define a set of tasks to be spawned
#   let mut tasks = JoinSet::<Result<(), DynError>>::new();
#   // task to poll updates from the connection
#   tasks.spawn(async move {
#       connection.handle_events().await?;
#       Ok(())
#   });
    // ... configure networking, instantiate and connect the device ...
    // ... spawn task to handle polling from the connection ...

    let client_cl = client.clone();

    // Create a task to send data to Astarte
    tasks.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let data = now.elapsed()?.as_secs_f64();
            client_cl
                .send(
                    "org.astarte-platform.rust.get-started.Property",
                    "/double_endpoint",
                    data,
                )
                .await?;

            info!("Data sent on endpoint /double_endpoint, content: {data}");

            // wait 1 sec before unsetting the property
            interval.tick().await;

            client_cl.unset("org.astarte-platform.rust.get-started.Property", "/double_endpoint").await?;

            info!("Unset property on /double_endpoint endpoint");
        }
    });

    // ... handle tasks termination and client disconnection ...
#   // properly handle tasks termination
#   while let Some(res) = tasks.join_next().await {
#       match res {
#           Ok(res) => {
#               res?;
#               tasks.abort_all();
#           }
#           Err(err) if err.is_cancelled() => {}
#           Err(err) => return Err(err.into()),
#       }
#   }
#   // disconnect the device once finished processing all the tasks
#   client.disconnect().await?;
#   Ok(())
}
```

It should be noted how a property should be marked as unsettable in its interface definition to be
able to use the unsetting method on it.

See the more complete code samples in the
[GitHub repository](https://github.com/astarte-platform/astarte-device-sdk-rust) of the Rust Astarte
device SDK for more information on how to receive data from Astarte, such as server owned
properties.
