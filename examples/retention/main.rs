// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::{env::VarError, error::Error, time::Duration};

use astarte_device_sdk::{builder::DeviceBuilder, prelude::*, transport::mqtt::MqttConfig};

type DynError = Box<dyn Error + Send + Sync + 'static>;

const INTERFACE_STORED: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.individual-datastream.StoredDeviceDatastream.json"
);
const INTERFACE_VOLATILE: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceDatastream.json"
);

const INTERFACE_STORED_NAME: &str =
    "org.astarte-platform.rust.examples.individual-datastream.StoredDeviceDatastream";
const INTERFACE_VOLATILE_NAME: &str =
    "org.astarte-platform.rust.examples.individual-datastream.VolatileDeviceDatastream";

#[derive(Debug, thiserror::Error)]
#[error("couldn't find variable {name}")]
struct EnvError {
    name: &'static str,
    #[source]
    backtrace: VarError,
}

fn get_env(name: &'static str) -> Result<String, EnvError> {
    std::env::var(name).map_err(|backtrace| EnvError { name, backtrace })
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    env_logger::init();

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
    let (client, connection) = DeviceBuilder::new()
        .store_dir(&tmp_dir)
        .await?
        .interface_str(INTERFACE_STORED)?
        .interface_str(INTERFACE_VOLATILE)?
        .connect(mqtt_config)
        .await?
        .build()
        .await;

    let mut tasks = tokio::task::JoinSet::new();

    tasks.spawn(async move { connection.handle_events().await });
    tasks.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));

        let mut counter: i32 = 0;
        let mut flag: bool = false;

        loop {
            client
                .send(INTERFACE_STORED_NAME, "/endpoint1", counter)
                .await?;
            client
                .send(INTERFACE_STORED_NAME, "/endpoint2", flag)
                .await?;
            client
                .send(INTERFACE_VOLATILE_NAME, "/endpoint1", counter)
                .await?;
            client
                .send(INTERFACE_VOLATILE_NAME, "/endpoint2", flag)
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
