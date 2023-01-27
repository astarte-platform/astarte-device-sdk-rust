/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use astarte_device_sdk::{builder::AstarteOptions, database::AstarteSqliteDatabase, AstarteError};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Cli {
    // Realm name
    #[structopt(short, long)]
    realm: String,
    // Device id
    #[structopt(short, long)]
    device_id: String,
    // Credentials secret
    #[structopt(short, long)]
    credentials_secret: String,
    // Pairing URL
    #[structopt(short, long)]
    pairing_url: String,
}

#[tokio::main]
async fn main() -> Result<(), AstarteError> {
    env_logger::init();

    let Cli {
        realm,
        device_id,
        credentials_secret,
        pairing_url,
    } = Cli::from_args();

    let db = AstarteSqliteDatabase::new("sqlite://astarte-example-db.sqlite").await?;

    let sdk_options = AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url)
        .interface_directory("./examples/interfaces")?
        .database(db)
        .build();

    let mut device = astarte_device_sdk::AstarteDeviceSdk::new(&sdk_options).await?;

    let w = device.clone();
    tokio::task::spawn(async move {
        loop {
            w.send(
                "org.astarte-platform.genericsensors.AvailableSensors",
                "/1/name",
                "foo",
            )
            .await
            .unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            w.send(
                "org.astarte-platform.genericsensors.AvailableSensors",
                "/1/name",
                "foo",
            )
            .await
            .unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            w.send(
                "org.astarte-platform.genericsensors.AvailableSensors",
                "/1/name",
                "bar",
            )
            .await
            .unwrap();

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    loop {
        match device.handle_events().await {
            Ok(data) => {
                println!("incoming: {data:?}");
            }
            Err(err) => log::error!("{err:?}"),
        }
    }
}
