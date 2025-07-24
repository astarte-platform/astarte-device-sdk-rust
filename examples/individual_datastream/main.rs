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

use std::{
    num::NonZeroU32,
    path::PathBuf,
    time::{Duration, SystemTime},
};

use astarte_device_sdk::{AstarteData, Value};
use chrono::Utc;
use clap::Parser;
use eyre::OptionExt;
use futures::future::Either;
use serde::Deserialize;

use astarte_device_sdk::{
    builder::DeviceBuilder, client::RecvError, prelude::*, store::memory::MemoryStore,
    transport::mqtt::MqttConfig,
};
use tokio::task::JoinSet;
use tracing::{error, info, level_filters::LevelFilter};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Deserialize)]
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

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    init_tracing()?;
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| eyre::eyre!("couldn't install default crypto provider"))?;

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
        .unwrap_or_else(|| "./examples/individual_datastream/configuration.json".to_string());
    let file = std::fs::read_to_string(file_path)?;
    let cfg: Config = serde_json::from_str(&file)?;

    let mut mqtt_config = MqttConfig::with_credential_secret(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    );
    mqtt_config.ignore_ssl_errors();

    let (mut client, connection) = DeviceBuilder::new()
        .store(MemoryStore::new())
        .interface_directory("./examples/individual_datastream/interfaces")?
        .connection(mqtt_config)
        .build()
        .await?;

    let mut client_cl = client.clone();
    info!("Connection to Astarte established.");

    let mut tasks = JoinSet::<eyre::Result<()>>::new();
    let now = SystemTime::now();

    // Create a task to transmit
    tasks.spawn(async move {
        // Sleep 1 sec
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            // Send endpoint 1
            let elapsed: i64 = now.elapsed()?.as_secs().try_into()?;
            client_cl
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
            client_cl
                .send_individual_with_timestamp(
                    "org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream",
                    "/endpoint2",
                    AstarteData::try_from(elapsed)?,
                    Utc::now(),
                )
                .await?;
            info!("Data sent on endpoint 2, content: {elapsed}");
        }
    });

    // Spawn a task to receive
    let client_cl = client.clone();
    tasks.spawn(async move {
        loop {
            match client_cl.recv().await {
                Ok(event) => {
                    if let Value::Individual{data, timestamp: _ } = event.data {
                        let mut iter = event.path.splitn(3, '/').skip(1);

                        let led_id = iter
                            .next()
                            .and_then(|id| id.parse::<u16>().ok())
                            .ok_or_eyre("Incorrect error received.")?;

                        match iter.next() {
                            Some("enable") => {
                                 let status = if data == true { "ON" } else { "OFF" };

                                info!( "Received new enable datastream for LED number {led_id}. LED status is now {status}" );
                            }
                            Some("intensity") => {
                                let value: f64 = data.try_into()?;
                                info!(
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

    client.disconnect().await?;

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
