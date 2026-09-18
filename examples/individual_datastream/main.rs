// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::{
    num::NonZeroU32,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use astarte_device_sdk::AstarteData;
use astarte_device_sdk::transport::mqtt::{Credential, MqttArgs};
use chrono::Utc;
use clap::Parser;
use eyre::{Context, OptionExt};
use serde::Deserialize;

use astarte_device_sdk::builder::DeviceBuilder;
use astarte_device_sdk::prelude::*;
use astarte_device_sdk::store::memory::MemoryStore;
use astarte_device_sdk::transport::mqtt::Mqtt;
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
    store_dir: Option<PathBuf>,
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
    client: C,
    limit: Option<NonZeroU32>,
    cancel: CancellationToken,
) -> eyre::Result<()>
where
    C: Client,
{
    let limit = limit.map(NonZeroU32::get).unwrap_or(u32::MAX);

    let now = SystemTime::now();
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    for _ in 0..limit {
        interval.tick().await;

        // Send endpoint 1
        let elapsed: i64 = now.elapsed()?.as_secs().try_into()?;
        client
            .send_individual_with_timestamp(
                "org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream",
                "/endpoint1",
                elapsed.into(),
                Utc::now(),
            )
            .await?;
        info!("Data sent on endpoint 1, content: {elapsed}");
        // Sleep 1 sec
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Send endpoint 2
        let elapsed = now.elapsed()?.as_secs_f64();
        client
            .send_individual_with_timestamp(
                "org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream",
                "/endpoint2",
                AstarteData::try_from(elapsed)?,
                Utc::now(),
            )
            .await?;
        info!("Data sent on endpoint 2, content: {elapsed}");
    }

    client.disconnect().await?;
    cancel.cancel();

    Ok(())
}

#[derive(Debug, FromEvent)]
#[from_event(
    interface = "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream"
)]
enum ServerDatastream {
    #[mapping(endpoint = "/%{led_id}/enable")]
    Enable(bool),
    #[mapping(endpoint = "/%{led_id}/intensity")]
    Intensity(f64),
}

async fn receive_loop<C>(client: C, cancel: CancellationToken) -> eyre::Result<()>
where
    C: Client,
{
    while let Some(event) = cancel.run_until_cancelled(client.recv()).await.flatten() {
        match event.interface.as_str() {
            "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream" => {
                let mut iter = event.path.splitn(3, '/').skip(1);

                let led_id = iter
                    .next()
                    .and_then(|id| id.parse::<u16>().ok())
                    .ok_or_eyre("Incorrect error received.")?;

                let data = ServerDatastream::from_event(event)?;

                match data {
                    ServerDatastream::Enable(enable) => {
                        info!(
                            "Received new enable datastream for LED number {led_id}. LED status is now {}",
                            if enable { "ON" } else { "OFF" }
                        );
                    }
                    ServerDatastream::Intensity(intensity) => {
                        info!(
                            "Received new intensity datastream for LED number {led_id}. LED intensity is now {intensity}"
                        );
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
        .unwrap_or_else(|| Path::new("./examples/individual_datastream/configuration.json"));

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

    let mqtt_config = Mqtt::new(args);

    let mut builder = DeviceBuilder::new();

    if ignore_ssl {
        builder = builder.insecure_tls()?;
    }

    if let Some(store_dir) = store_dir {
        builder = builder.writable_dir(store_dir);
    }

    let (client, connection) = builder
        .store(MemoryStore::new())
        .interface_directory("./examples/individual_datastream/interfaces")?
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

    // Create a task to transmit
    tasks.spawn(send_loop(client.clone(), loop_times, cancel.clone()));

    // Spawn a task to receive
    tasks.spawn(receive_loop(client.clone(), cancel.clone()));

    tasks.spawn({
        let client = client.clone();
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
