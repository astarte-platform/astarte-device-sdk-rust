// This file is part of Astarte.
//
// Copyright 2024 - 2025 SECO Mind Srl
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

use astarte_device_sdk::aggregate::AstarteObject;
use astarte_device_sdk::{builder::DeviceBuilder, prelude::*, transport::mqtt::MqttConfig};
use clap::Parser;
use futures::future::Either;
use serde::{Deserialize, Serialize};
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const INDIVIDUAL_STORED: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.individual-datastream.StoredDeviceDatastream.json"
);
const INDIVIDUAL_STORED_NAME: &str =
    "org.astarte-platform.rust.examples.individual-datastream.StoredDeviceDatastream";

const INDIVIDUAL_VOLATILE: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceDatastream.json"
);
const INDIVIDUAL_VOLATILE_NAME: &str =
    "org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceDatastream";

const OBJECT_STORED: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.individual-datastream.StoredDeviceObject.json"
);
const OBJECT_STORED_NAME: &str =
    "org.astarte-platform.rust.examples.individual-datastream.StoredDeviceObject";

const OBJECT_UNIQ_STORED: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.individual-datastream.StoredUniqDeviceObject.json"
);
const OBJECT_UNIQ_STORED_NAME: &str =
    "org.astarte-platform.rust.examples.individual-datastream.StoredUniqDeviceObject";

const OBJECT_VOLATILE: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceObject.json"
);
const OBJECT_VOLATILE_NAME: &str =
    "org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceObject";

const OBJECT_UNIQ_VOLATILE: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.individual-datastream.VolatileUniqDeviceObject.json"
);
const OBJECT_UNIQ_VOLATILE_NAME: &str =
    "org.astarte-platform.rust.examples.individual-datastream.VolatileUniqDeviceObject";

#[derive(Debug, Clone, IntoAstarteObject)]
struct ObjectDatastream {
    longinteger: i64,
    boolean: bool,
}

impl ObjectDatastream {
    fn new(longinteger: i64, boolean: bool) -> Self {
        Self {
            longinteger,
            boolean,
        }
    }
}

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
    /// Limit number of iteration of the send loop
    #[arg(short, long)]
    loop_times: Option<NonZeroU32>,
}

async fn send_loop<I>(mut client: impl Client, iter: I) -> Result<(), astarte_device_sdk::Error>
where
    I: IntoIterator<Item = u32>,
{
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    let mut counter: i32 = 0;
    let mut flag: bool = false;

    for _ in iter {
        client
            .send_individual(INDIVIDUAL_STORED_NAME, "/endpoint1", counter.into())
            .await?;
        client
            .send_individual(INDIVIDUAL_STORED_NAME, "/endpoint2", flag.into())
            .await?;
        client
            .send_individual(INDIVIDUAL_VOLATILE_NAME, "/endpoint1", counter.into())
            .await?;
        client
            .send_individual(INDIVIDUAL_VOLATILE_NAME, "/endpoint2", flag.into())
            .await?;

        let object = ObjectDatastream::new(flag.into(), flag);
        let object = AstarteObject::try_from(object)?;

        client
            .send_object(OBJECT_STORED_NAME, "/endpoint", object.clone())
            .await?;
        client
            .send_object(OBJECT_UNIQ_STORED_NAME, "/endpoint", object.clone())
            .await?;
        client
            .send_object(OBJECT_VOLATILE_NAME, "/endpoint", object.clone())
            .await?;
        client
            .send_object(OBJECT_UNIQ_VOLATILE_NAME, "/endpoint", object)
            .await?;

        counter = counter.wrapping_add(1);
        flag = !flag;

        interval.tick().await;
    }

    Ok(())
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
        loop_times,
    } = Args::parse();

    let transmission_timeout =
        transmission_timeout_sec.map(|s| Duration::from_secs(s.get().into()));

    let file_path = config
        .map(|p| p.into_os_string().into_string())
        .transpose()
        .map_err(|s| eyre::eyre!("cannot convert string '{s:?}'"))?
        .unwrap_or_else(|| "./examples/retention/configuration.json".to_string());
    let file = std::fs::read_to_string(file_path).unwrap();
    let Config {
        realm,
        device_id,
        credentials_secret,
        pairing_url,
    } = serde_json::from_str(&file)?;

    let mut mqtt_config =
        MqttConfig::with_credential_secret(&realm, &device_id, &credentials_secret, &pairing_url);

    mqtt_config.ignore_ssl_errors();

    let mut tmp_dir = std::env::temp_dir();

    tmp_dir.push("astarte-example-retention");

    std::fs::create_dir_all(&tmp_dir)?;

    // Create an Astarte Device (also performs the connection)
    let (mut client, connection) = DeviceBuilder::new()
        .store_dir(&tmp_dir)
        .await?
        .interface_str(INDIVIDUAL_STORED)?
        .interface_str(INDIVIDUAL_VOLATILE)?
        .interface_str(OBJECT_STORED)?
        .interface_str(OBJECT_UNIQ_STORED)?
        .interface_str(OBJECT_VOLATILE)?
        .interface_str(OBJECT_UNIQ_VOLATILE)?
        .connection(mqtt_config)
        .build()
        .await?;

    let mut tasks = tokio::task::JoinSet::new();

    tasks.spawn(async move { connection.handle_events().await });

    if let Some(c) = loop_times {
        let client = client.clone();
        tasks.spawn(send_loop(client, 0..c.get()));
    } else {
        let client = client.clone();
        tasks.spawn(send_loop(client, 0u32..));
    }

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
                    break;
                }
                Err(err) if err.is_cancelled() => {}
                Err(err) => return Err(err.into()),
            }
        }
    }

    client.disconnect().await?;
    tasks.shutdown().await;

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
