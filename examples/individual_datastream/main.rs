// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, SystemTime};

use serde::Deserialize;

use astarte_device_sdk::{
    builder::DeviceBuilder, client::RecvError, prelude::*, store::memory::MemoryStore,
    transport::mqtt::MqttConfig,
};
use tokio::task::JoinSet;
use tracing::error;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

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

    // Load configuration
    let file =
        std::fs::read_to_string("./examples/individual_datastream/configuration.json").unwrap();
    let cfg: Config = serde_json::from_str(&file).unwrap();

    let mut mqtt_config = MqttConfig::with_credential_secret(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    );
    mqtt_config.ignore_ssl_errors();

    let (client, connection) = DeviceBuilder::new()
        .store(MemoryStore::new())
        .interface_directory("./examples/individual_datastream/interfaces")?
        .connect(mqtt_config)
        .await?
        .build()
        .await;

    let client_cl = client.clone();
    println!("Connection to Astarte established.");

    let mut tasks = JoinSet::<Result<(), DynError>>::new();

    // Create a task to transmit
    tasks.spawn(async move {
        // Sleep 1 sec
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            // Send endpoint 1
            let elapsed: i64 = now.elapsed()?.as_secs().try_into()?;
            client_cl
                .send(
                    "org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream",
                    "/endpoint1",
                    elapsed,
                )
                .await?;
            println!("Data sent on endpoint 1, content: {elapsed}");
            // Sleep 1 sec
            tokio::time::sleep(Duration::from_secs(1)).await;
            // Send endpoint 2
            let elapsed: f64 = now.elapsed()?.as_secs_f64();
            client_cl
                .send(
                    "org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream",
                    "/endpoint2",
                    elapsed,
                )
                .await?;
            println!("Data sent on endpoint 2, content: {elapsed}");
        }
    });

    // Spawn a task to receive
    let client_cl = client.clone();
    tasks.spawn(async move {
        loop {
            match client_cl.recv().await {
                Ok(data) => {
                    if let astarte_device_sdk::Value::Individual(var) = data.data {
                        let mut iter = data.path.splitn(3, '/').skip(1);
                        let led_id = iter
                            .next()
                            .and_then(|id| id.parse::<u16>().ok())
                            .ok_or("Incorrect error received.")?;

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
                            "Received new intensity datastream for LED number {led_id}. LED intensity is now {value}"
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
    });

    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

    tasks.spawn(async move { tokio::signal::ctrl_c().await.map_err(Into::into) });

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

    client.disconnect().await?;

    Ok(())
}
