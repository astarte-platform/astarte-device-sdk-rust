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

use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use futures::future::Either;
use serde::{Deserialize, Serialize};

use astarte_device_sdk::IntoAstarteObject;
use astarte_device_sdk::{
    builder::DeviceBuilder, prelude::*, store::memory::MemoryStore, transport::mqtt::MqttConfig,
};
use tokio::task::JoinSet;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Serialize, Deserialize)]
struct Config {
    realm: String,
    device_id: String,
    credentials_secret: String,
    pairing_url: String,
}

#[derive(Debug, IntoAstarteObject)]
struct DataObject {
    endpoint1: f64,
    endpoint2: String,
    endpoint3: Vec<bool>,
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
        .unwrap_or_else(|| "./examples/object_datastream/configuration.json".to_string());
    let file = std::fs::read_to_string(file_path)?;
    let cfg: Config = serde_json::from_str(&file)?;

    let mut mqtt_config = MqttConfig::with_credential_secret(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    );

    mqtt_config.ignore_ssl_errors();

    info!("looping");
    // Create an Astarte Device (also performs the connection)
    let (client, connection) = DeviceBuilder::new()
        .store(MemoryStore::new())
        .interface_directory("./examples/object_datastream/interfaces")?
        .connection(mqtt_config)
        .build()
        .await?;

    let mut tasks = JoinSet::<eyre::Result<()>>::new();

    // Create a thread to transmit
    tasks.spawn({
        let mut client = client.clone();

        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                let data = DataObject {
                    endpoint1: 1.34,
                    endpoint2: "Hello world.".to_string(),
                    endpoint3: vec![true, false, true, false],
                };

                info!(?data, "sending");

                client
                    .send_object_with_timestamp(
                        "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream",
                        "/23",
                        data.try_into().unwrap(),
                        Utc::now(),
                    )
                    .await?;

                interval.tick().await;
            }
        }
    });

    // Spawn a task to receive
    tasks.spawn(async move {
        loop {
            let event = client.recv().await?;

            info!(?event, "received");
        }
    });

    // Spawn a task to handle the event loop
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
            Either::Left((res, _)) => {
                info!(res = ?res, "Task exited");
            }
            Either::Right(_) => info!("Reached timeout"),
        }
    } else {
        info!("looping");
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
