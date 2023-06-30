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

use astarte_device_sdk::{error::Error, options::AstarteOptions, AstarteAggregate};
#[cfg(not(feature = "derive"))]
use astarte_device_sdk_derive::AstarteAggregate;

use serde::{Deserialize, Serialize};

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

    // Create Astarte Options
    let sdk_options = AstarteOptions::new(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    )
    .interface_directory("./examples/object_datastream/interfaces")?
    .ignore_ssl_errors();

    // Create an Astarte Device (also performs the connection)
    let mut device = astarte_device_sdk::AstarteDeviceSdk::new(sdk_options).await?;
    println!("Connection to Astarte established.");

    // Create an thread to transmit
    let w = device.clone();
    tokio::task::spawn(async move {
        loop {
            let data = DataObject {
                endpoint1: 1.34,
                endpoint2: "Hello world.".to_string(),
                endpoint3: vec![true, false, true, false],
            };

            println!("Sending {data:?}");
            w.send_object(
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
        match device.handle_events().await {
            Ok(data) => {
                println!("incoming: {data:?}");
            }
            Err(err) => log::error!("{err:?}"),
        }
    }
}
