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

use std::error::Error as StdError;

use serde::{Deserialize, Serialize};

use astarte_device_sdk::{
    builder::DeviceBuilder, client::RecvError, error::Error, prelude::*, store::SqliteStore,
    transport::mqtt::MqttConfig, Value,
};
use tracing::error;

type DynError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
}

// Getter function for the property "name" of a sensor.
async fn get_name_for_sensor(
    device: &impl PropAccess,
    sensor_n: i32,
) -> Result<String, crate::Error> {
    let interface = "org.astarte-platform.rust.examples.individual-properties.DeviceProperties";
    let path = format!("/{sensor_n}/name");

    let name = device
        .property(interface, &path)
        .await?
        .map(|t| t.try_into())
        .transpose()?
        .unwrap_or_else(|| "None".to_string());

    Ok(name)
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    env_logger::init();

    // Load configuration
    let file =
        std::fs::read_to_string("./examples/individual_properties/configuration.json").unwrap();
    let cfg: Config = serde_json::from_str(&file).unwrap();

    // Open the database, create it if it does not exists
    let db =
        SqliteStore::connect_db("examples/individual_properties/astarte-example-db.sqlite").await?;

    let mut mqtt_config = MqttConfig::with_credential_secret(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    );

    mqtt_config.ignore_ssl_errors();

    // Create an Astarte Device (also performs the connection)
    let (client, connection) = DeviceBuilder::new()
        .interface_directory("./examples/individual_properties/interfaces")?
        .store(db)
        .connection(mqtt_config)
        .build()
        .await?;
    let device_cpy = client.clone();

    println!("Connection to Astarte established.");

    // Create an thread to transmit
    tokio::task::spawn(async move {
        let mut i: u32 = 0;

        println!("Properties values at startup:");
        // Check the value of the name property for sensors 1
        if let Ok(name) = get_name_for_sensor(&device_cpy, 1).await {
            println!("  - Property \"name\" for sensor 1 has value: \"{name}\"");
            if name != *"None" {
                i = name.strip_prefix("name number ").unwrap().parse().unwrap();
            }
        }
        // Check the value of the name property for sensors 2
        if let Ok(name) = get_name_for_sensor(&device_cpy, 2).await {
            println!("  - Property \"name\" for sensor 2 has value: \"{name}\"");
        }

        // Wait for a couple of seconds for a nicer print order
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Send in a loop the change of the property "name" of sensor 1
        loop {
            device_cpy
                .send(
                    "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                    "/1/name",
                    format!("name number {i}"),
                )
                .await
                .unwrap();
            println!("Sent property \"name\" for sensor 1 with new value \"name number {i}\"");
            i += 1;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    // Use the current thread to receive changes in the server owned properties
    tokio::spawn(async move {
        loop {
            match client.recv().await {
                Ok(data) => {
                    if let Value::Individual(var) = data.data {
                        let mut iter = data.path.splitn(3, '/').skip(1);
                        let sensor_id = iter
                            .next()
                            .and_then(|id| id.parse::<u16>().ok())
                            .ok_or("Incorrect error received.")?;

                        match iter.next() {
                            Some("enable") => {
                                println!(
                                    "Sensor number {} has been {}",
                                    sensor_id,
                                    if var == true { "ENABLED" } else { "DISABLED" }
                                );
                            }
                            Some("samplingPeriod") => {
                                let value: i32 = var.try_into().unwrap();
                                println!("Sampling period for sensor {} is {}", sensor_id, value);
                            }
                            _ => {}
                        }
                    }
                }
                Err(RecvError::Disconnected) => break,
                Err(err) => error!(error = %err, "error returned by the client"),
            }
        }

        Ok::<_, DynError>(())
    });

    connection.handle_events().await?;

    Ok(())
}
