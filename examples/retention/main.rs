// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
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
use std::path::{Path, PathBuf};
use std::time::Duration;

use astarte_device_sdk::aggregate::AstarteObject;
use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::transport::mqtt::{Credential, MqttArgs};
use astarte_device_sdk::{builder::DeviceBuilder, prelude::*, transport::mqtt::MqttConfig};
use clap::Parser;
use eyre::Context;
use serde::Deserialize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

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
    C: Client + ClientConnection,
{
    let limit = limit.map(NonZeroU32::get).unwrap_or(u32::MAX);
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    let mut counter: i32 = 0;
    let mut flag: bool = false;

    for _ in 0..limit {
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

    client.disconnect().await?;
    cancel.cancel();

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

    // Create an Astarte Device (also performs the connection)
    let (mut client, connection) = DeviceBuilder::new()
        .writable_dir(store_dir)
        .store(store)
        .interface_str(INDIVIDUAL_STORED)?
        .interface_str(INDIVIDUAL_VOLATILE)?
        .interface_str(OBJECT_STORED)?
        .interface_str(OBJECT_UNIQ_STORED)?
        .interface_str(OBJECT_VOLATILE)?
        .interface_str(OBJECT_UNIQ_VOLATILE)?
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

    // TODO: Spawn a task to receive
    // tasks.spawn(receive_loop(client.clone(), cancel.clone()));

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
