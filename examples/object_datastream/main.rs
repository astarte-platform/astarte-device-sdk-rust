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

use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use astarte_device_sdk::IntoAstarteObject;
use astarte_device_sdk::{
    builder::DeviceBuilder, prelude::*, store::memory::MemoryStore, transport::mqtt::MqttConfig,
};
use tokio::task::JoinSet;
use tracing::info;
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

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    init_tracing()?;
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| eyre::eyre!("couldn't install default crypto provider"))?;

    // Load configuration
    let file = std::fs::read_to_string("./examples/object_datastream/configuration.json")?;
    let cfg: Config = serde_json::from_str(&file)?;

    let mut mqtt_config = MqttConfig::with_credential_secret(
        &cfg.realm,
        &cfg.device_id,
        &cfg.credentials_secret,
        &cfg.pairing_url,
    );

    mqtt_config.ignore_ssl_errors();

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

    // Create a thread to receive
    tasks.spawn(async move {
        loop {
            let event = client.recv().await?;

            info!(?event, "received");
        }
    });

    // Use the current thread to handle the connection (no incoming messages are expected in this example)
    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

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

    Ok(())
}

fn init_tracing() -> eyre::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(concat!(env!("CARGO_PKG_NAME"), "=debug").parse()?)
                .from_env_lossy(),
        )
        .try_init()?;

    Ok(())
}
