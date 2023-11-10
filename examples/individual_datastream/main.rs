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

use std::time::SystemTime;

use serde::Deserialize;

use astarte_device_sdk::{
    builder::DeviceBuilder, error::Error, prelude::*, store::memory::MemoryStore,
    transport::mqtt::MqttConfig,
};

#[derive(Deserialize)]
struct Config {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    let now = SystemTime::now();

    // Load configuration
    let file =
        std::fs::read_to_string("./examples/individual_datastream/configuration.json").unwrap();
    let cfg: Config = serde_json::from_str(&file).unwrap();

    let mut mqtt_config = MqttConfig::new(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    );
    mqtt_config.ignore_ssl_errors();

    let (mut device, mut rx_events) = DeviceBuilder::new()
        .store(MemoryStore::new())
        .interface_directory("./examples/individual_datastream/interfaces")?
        .connect(mqtt_config)
        .await?
        .build();

    let device_cpy = device.clone();
    println!("Connection to Astarte established.");

    // Create a task to transmit
    tokio::task::spawn(async move {
        loop {
            // Send endpoint 1
            let elapsed = now.elapsed().unwrap().as_secs() as i64;
            device_cpy
                .send(
                    "org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream",
                    "/endpoint1",
                    elapsed,
                )
                .await
                .unwrap();
            println!("Data sent on endpoint 1, content: {elapsed}");
            // Sleep 1 sec
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            // Send endpoint 2
            let elapsed = now.elapsed().unwrap().as_secs() as f64;
            device_cpy
                .send(
                    "org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream",
                    "/endpoint2",
                    elapsed,
                )
                .await
                .unwrap();
            println!("Data sent on endpoint 2, content: {elapsed}");
            // Sleep 1 sec
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    // Spawn a task to receive
    tokio::spawn(async move {
        while let Some(event) = rx_events.recv().await {
            match event {
                Ok(data) => {
                    if let astarte_device_sdk::Aggregation::Individual(var) = data.data {
                        let mut iter = data.path.splitn(3, '/').skip(1);
                        let led_id = iter
                            .next()
                            .and_then(|id| id.parse::<u16>().ok())
                            .ok_or_else(|| {
                                Error::ReceiveError("Incorrect error received.".to_string())
                            })?;

                        match iter.next() {
                            Some("enable") => {
                                println!(
                            "Received new enable datastream for LED number {}. LED status is now {}",
                            led_id,
                            if var == true { "ON" } else { "OFF" }
                        );
                            }
                            Some("intensity") => {
                                let value: f64 = var.try_into().unwrap();
                                println!(
                            "Received new intensity datastream for LED number {}. LED intensity is now {}",
                            led_id, value
                        );
                            }
                            _ => {}
                        }
                    }
                }
                Err(err) => log::error!("{:?}", err),
            }
        }

        Ok::<_, Error>(())
    });

    device.handle_events().await?;

    Ok(())
}
