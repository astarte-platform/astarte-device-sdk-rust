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

use std::{num::NonZeroU32, path::PathBuf, time::Duration};

use clap::Parser;
use eyre::OptionExt;
use futures::future::Either;
use serde::{Deserialize, Serialize};

use astarte_device_sdk::{
    builder::DeviceBuilder, client::RecvError, error::Error, prelude::*, store::SqliteStore,
    transport::mqtt::MqttConfig, Value,
};
use tokio::task::JoinSet;
use tracing::{error, info, level_filters::LevelFilter};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to the config file for the example
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Limit run time of the transmission of the example (in seconds)
    #[arg(short, long)]
    transmission_timeout_sec: Option<NonZeroU32>,
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
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    init_tracing()?;
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| eyre::eyre!("couldn't install default crypto provider"))?;

    // Load configuration
    let Args {
        config,
        transmission_timeout_sec,
    } = Args::parse();

    let transmission_timeout =
        transmission_timeout_sec.map(|s| Duration::from_secs(s.get().into()));

    // Load configuration
    let file_path = config
        .map(|p| p.into_os_string().into_string())
        .transpose()
        .map_err(|s| eyre::eyre!("cannot convert string '{s:?}'"))?
        .unwrap_or_else(|| "./examples/individual_properties/configuration.json".to_string());
    let file = std::fs::read_to_string(file_path)?;
    let cfg: Config = serde_json::from_str(&file)?;

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
    let mut client_cl = client.clone();

    println!("Connection to Astarte established.");

    let mut tasks = JoinSet::<eyre::Result<()>>::new();

    // Create a thread to transmit
    tasks.spawn(async move {
        let mut i: u32 = 0;

        println!("Properties values at startup:");
        // Check the value of the name property for sensors 1
        if let Ok(name) = get_name_for_sensor(&client_cl, 1).await {
            info!("  - Property \"name\" for sensor 1 has value: \"{name}\"");
            if name != *"None" {
                i = name
                    .strip_prefix("name number ")
                    .ok_or_eyre("couldn't strip prefix")?
                    .parse()?;
            }
        }
        // Check the value of the name property for sensors 2
        if let Ok(name) = get_name_for_sensor(&client_cl, 2).await {
            println!("  - Property \"name\" for sensor 2 has value: \"{name}\"");
        }

        // Wait for a couple of seconds for a nicer print order
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Send in a loop the change of the property "name" of sensor 1
        loop {
            client_cl
                .set_property(
                    "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                    "/1/name",
                    format!("name number {i}").into(),
                )
                .await?;

            println!("Sent property \"name\" for sensor 1 with new value \"name number {i}\"");
            i += 1;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    // Use the current thread to receive changes in the server owned properties
    tasks.spawn(async move {
        loop {
            match client.recv().await {
                Ok(event) => {
                    if let Value::Individual { data, timestamp: _ } = event.data {
                        let mut iter = event.path.splitn(3, '/').skip(1);
                        let sensor_id = iter
                            .next()
                            .and_then(|id| id.parse::<u16>().ok())
                            .ok_or_eyre("Incorrect error received.")?;

                        match iter.next() {
                            Some("enable") => {
                                println!(
                                    "Sensor number {} has been {}",
                                    sensor_id,
                                    if data == true { "ENABLED" } else { "DISABLED" }
                                );
                            }
                            Some("samplingPeriod") => {
                                let value: i32 = data.try_into()?;
                                println!("Sampling period for sensor {sensor_id} is {value}");
                            }
                            _ => {}
                        }
                    }
                }
                Err(RecvError::Disconnected) => break,
                Err(err) => error!(error = %err, "error returned by the client"),
            }
        }

        Ok(())
    });

    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

    if let Some(timeout) = transmission_timeout {
        let out = futures::future::select(
            Box::pin(tasks.join_next()),
            Box::pin(tokio::time::sleep(timeout)),
        )
        .await;

        match out {
            Either::Left((o, _)) => info!(o = ?o, "Task exited"),
            Either::Right(_) => info!("Reached timeout"),
        }
    } else {
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
    }

    Ok(())
}

fn init_tracing() -> eyre::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(concat!(env!("CARGO_PKG_NAME"), "=debug").parse()?)
                .from_env_lossy()
                .add_directive(LevelFilter::INFO.into()),
        )
        .try_init()?;

    Ok(())
}
