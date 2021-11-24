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
 */

use astarte_sdk::{builder::AstarteBuilder, database::AstarteSqliteDatabase};
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
async fn main() {
    env_logger::init();

    let Cli {
        realm,
        device_id,
        credentials_secret,
        pairing_url,
    } = Cli::from_args();

    let db = AstarteSqliteDatabase::new("sqlite://astarte-example-db.sqlite")
        .await
        .unwrap();

    let mut sdk_builder =
        AstarteBuilder::new(&realm, &device_id, &credentials_secret, &pairing_url);

    sdk_builder
        .add_interface_files("./examples/interfaces")
        .unwrap()
        .with_database(db);

    sdk_builder.build().await.unwrap();

    let mut device = sdk_builder.connect().await.unwrap();

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

            std::thread::sleep(std::time::Duration::from_millis(100));

            w.send(
                "org.astarte-platform.genericsensors.AvailableSensors",
                "/1/name",
                "foo",
            )
            .await
            .unwrap();

            std::thread::sleep(std::time::Duration::from_millis(100));

            w.send(
                "org.astarte-platform.genericsensors.AvailableSensors",
                "/1/name",
                "bar",
            )
            .await
            .unwrap();

            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    });

    loop {
        match device.poll().await {
            Ok(data) => {
                println!("incoming: {:?}", data);
            }
            Err(err) => log::error!("{:?}", err),
        }
    }
}
