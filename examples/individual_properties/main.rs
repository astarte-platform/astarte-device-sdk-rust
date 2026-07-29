// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
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

use std::path::Path;
use std::{num::NonZeroU32, path::PathBuf, time::Duration};

use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::transport::mqtt::{Credential, MqttArgs};
use clap::Parser;
use eyre::{Context, OptionExt};
use serde::Deserialize;

use astarte_device_sdk::builder::DeviceBuilder;
use astarte_device_sdk::prelude::*;
use astarte_device_sdk::transport::mqtt::MqttConfig;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;

#[derive(Deserialize)]
struct Config {
    realm: String,
    device_id: String,
    #[serde(flatten)]
    credential: Credential,
    pairing_url: Url,
    store_dir: PathBuf,
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to the config file for the example
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Limit run time of the transmission of the example (in seconds)
    #[arg(short, long)]
    timeout_secs: Option<NonZeroU32>,
    /// Limit number of iteration of the send loop
    #[arg(short, long)]
    loop_times: Option<NonZeroU32>,
    /// Ignore ssl errors
    #[arg(short, long, default_value = "false")]
    ignore_ssl: bool,
}

async fn send_loop<C>(
    mut client: C,
    limit: Option<NonZeroU32>,
    cancel: CancellationToken,
) -> eyre::Result<()>
where
    C: Client + ClientConnection + PropAccess,
{
    let limit = limit.map(NonZeroU32::get).unwrap_or(u32::MAX);

    let mut i: u32 = 0;
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    info!("Properties values at startup:");
    // Check the value of the name property for sensors 1
    if let Ok(name) = get_name_for_sensor(&client, 1).await {
        info!("  - Property \"name\" for sensor 1 has value: \"{name}\"");
        if name != *"None" {
            i = name
                .strip_prefix("name number ")
                .ok_or_eyre("couldn't strip prefix")?
                .parse()?;
        }
    }
    // Check the value of the name property for sensors 2
    if let Ok(name) = get_name_for_sensor(&client, 2).await {
        info!(" Property 'name' for sensor 2 has value: {name}");
    }

    // Wait for a couple of seconds for a nicer print order
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Send in a loop the change of the property "name" of sensor 1
    for _ in 0..limit {
        client
            .set_property(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
                format!("name number {i}").into(),
            )
            .await?;

        info!("Sent property 'name' for sensor 1 with new value: 'name number {i}");
        i += 1;
        interval.tick().await;
    }

    client.disconnect().await?;
    cancel.cancel();

    Ok(())
}

#[derive(Debug, FromEvent)]
#[from_event(
    interface = "org.astarte-platform.rust.examples.individual-properties.ServerProperties",
    interface_type = "properties",
    aggregation = "individual"
)]
enum ServerProperties {
    #[mapping(endpoint = "/%{sensor_id}/enable", allow_unset = true)]
    Enable(Option<bool>),
    #[mapping(endpoint = "/%{sensor_id}/samplingPeriod", allow_unset = true)]
    SamplingPeriod(Option<i32>),
}

async fn receive_loop<C>(client: C, cancel: CancellationToken) -> eyre::Result<()>
where
    C: Client,
{
    while let Some(event) = cancel.run_until_cancelled(client.recv()).await.flatten() {
        match event.interface.as_str() {
            "org.astarte-platform.rust.examples.individual-properties.ServerProperties" => {
                let mut iter = event.path.splitn(3, '/').skip(1);

                let sensor_id = iter
                    .next()
                    .and_then(|id| id.parse::<u16>().ok())
                    .ok_or_eyre("Incorrect error received.")?;

                let prop = ServerProperties::from_event(event)?;

                match prop {
                    ServerProperties::Enable(enable) => {
                        info!(
                            "Sensor number {} has been {}",
                            sensor_id,
                            if enable == Some(true) {
                                "ENABLED"
                            } else {
                                "DISABLED"
                            }
                        );
                    }
                    ServerProperties::SamplingPeriod(sampling) => {
                        info!("Sampling period for sensor {sensor_id} is {sampling:?}");
                    }
                }
            }
            interface => {
                error!(interface, "unknown interface");
            }
        }
    }

    Ok(())
}

// Getter function for the property "name" of a sensor.
async fn get_name_for_sensor(device: &impl PropAccess, sensor_n: i32) -> eyre::Result<String> {
    let interface = "org.astarte-platform.rust.examples.individual-properties.DeviceProperties";
    let path = format!("/{sensor_n}/name");

    let name = device
        .property(interface, &path)
        .await?
        .map(String::try_from)
        .transpose()?
        .unwrap_or_else(|| "None".to_string());

    Ok(name)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    init_tracing()?;

    let Args {
        config,
        timeout_secs,
        loop_times,
        ignore_ssl,
    } = Args::parse();

    // Load configuration
    let file_path = config
        .as_deref()
        .unwrap_or_else(|| Path::new("./examples/object_datastream/configuration.json"));

    let file = tokio::fs::read_to_string(file_path).await?;
    let Config {
        realm,
        device_id,
        credential,
        pairing_url,
        store_dir,
    } = serde_json::from_str(&file)?;

    let args = MqttArgs {
        realm,
        device_id,
        credential,
        pairing_url,
    };

    let mut mqtt_config = MqttConfig::new(args);

    if ignore_ssl {
        mqtt_config = mqtt_config.ignore_ssl_errors();
    }

    let store = SqliteStore::options().with_writable_dir(&store_dir).await?;

    let (mut client, connection) = DeviceBuilder::new()
        .writable_dir(store_dir)
        .store(store)
        .interface_directory("./examples/individual_properties/interfaces")?
        .connection(mqtt_config)
        .build()
        .await?;

    info!("Connection to Astarte established.");
    let mut tasks = JoinSet::<eyre::Result<()>>::new();

    let cancel = CancellationToken::new();

    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

    tasks.spawn(send_loop(client.clone(), loop_times, cancel.clone()));

    tasks.spawn(receive_loop(client.clone(), cancel.clone()));

    tasks.spawn({
        let mut client = client.clone();
        let cancel = cancel.clone();

        async move {
            if let Some(res) = cancel.run_until_cancelled(tokio::signal::ctrl_c()).await {
                res?;

                client.disconnect().await?;
            }

            Ok(())
        }
    });

    if let Some(timeout) = timeout_secs {
        tasks.spawn({
            let cancel = cancel.clone();

            async move {
                tokio::time::timeout(
                    Duration::from_secs(timeout.get().into()),
                    cancel.cancelled(),
                )
                .await
                .wrap_err("timeout reached")?;

                Ok(())
            }
        });
    }

    while let Some(next) = tasks.join_next().await {
        match next {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                client.disconnect().await?;
                cancel.cancel();
                tasks.shutdown().await;

                return Err(error);
            }
            Err(err) => {
                error!(%err, "couldn't join tasks");

                client.disconnect().await?;
                cancel.cancel();
                tasks.shutdown().await;

                return Err(err).wrap_err("couldn't join tasks");
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
                .with_default_directive("astarte_device_sdk=debug".parse()?)
                .from_env_lossy()
                .add_directive(LevelFilter::INFO.into()),
        )
        .try_init()?;

    Ok(())
}
