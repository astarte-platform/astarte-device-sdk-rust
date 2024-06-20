/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use serde::{Deserialize, Serialize};

#[cfg(feature = "derive")]
use astarte_device_sdk::AstarteAggregate;
use astarte_device_sdk::{
    builder::DeviceBuilder, error::Error, prelude::*, store::memory::MemoryStore,
    transport::mqtt::MqttConfig,
};
#[cfg(not(feature = "derive"))]
use astarte_device_sdk_derive::AstarteAggregate;
use tracing::{error, info};

#[derive(Serialize, Deserialize)]
struct Config {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
}

#[derive(Debug, AstarteAggregate)]
struct DataObject {
    endpoint1: f64,
    endpoint2: String,
    endpoint3: Vec<bool>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    // Load configuration
    let file = std::fs::read_to_string("./examples/object_datastream/configuration.json").unwrap();
    let cfg: Config = serde_json::from_str(&file).unwrap();

    let mut mqtt_config = MqttConfig::with_credential_secret(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    );

    mqtt_config.ignore_ssl_errors();

    // Create an Astarte Device (also performs the connection)
    let (client, mut connection) = DeviceBuilder::new()
        .store(MemoryStore::new())
        .interface_directory("./examples/object_datastream/interfaces")?
        .connect(mqtt_config)
        .await?
        .build();

    // Create an thread to transmit
    tokio::task::spawn(async move {
        loop {
            let data = DataObject {
                endpoint1: 1.34,
                endpoint2: "Hello world.".to_string(),
                endpoint3: vec![true, false, true, false],
            };

            println!("Sending {data:?}");
            client
                .send_object(
                    "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream",
                    "/23",
                    data,
                )
                .await
                .unwrap();

            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    // Use the current thread to receive (no incoming messages are expected in this example)
    loop {
        match connection.handle_events().await {
            Ok(data) => {
                info!("incoming: {data:?}");
            }
            Err(err) => error!("{err}"),
        }
    }
}
