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

use std::time::Duration;

use astarte_device_sdk::aggregate::AstarteObject;
use astarte_device_sdk::{builder::DeviceBuilder, prelude::*, transport::mqtt::MqttConfig};
use eyre::Context;
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

fn get_env(name: &'static str) -> eyre::Result<String> {
    std::env::var(name).wrap_err_with(|| format!("couldn't get environment variable {name}"))
}

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

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    init_tracing()?;
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| eyre::eyre!("couldn't install default crypto provider"))?;

    let realm = get_env("ASTARTE_REALM")?;
    let device_id = get_env("ASTARTE_DEVICE_ID")?;
    let credentials_secret = get_env("ASTARTE_CREDENTIALS_SECRET")?;
    let pairing_url = get_env("ASTARTE_PAIRING_URL")?;

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
    tasks.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));

        let mut counter: i32 = 0;
        let mut flag: bool = false;

        loop {
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

            counter += 1;
            flag = !flag;

            interval.tick().await;
        }
    });

    while let Some(res) = tasks.join_next().await {
        res??;
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
