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


use astarte_sdk::builder::AstarteBuilder;
use structopt::StructOpt;

use serde::Serialize;

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

    let mut sdk_options =
        AstarteBuilder::new(&realm, &device_id, &credentials_secret, &pairing_url);
    sdk_options
        .add_interface_files("./examples/interfaces")
        .unwrap();

    sdk_options.build().await.unwrap();

    let mut device = sdk_options.connect().await.unwrap();

    let w = device.clone();

    tokio::task::spawn(async move {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(5000));

            #[derive(Serialize)]
            #[serde(rename_all = "camelCase")]
            struct Geolocation {
                latitude: f64,
                longitude: f64,
                altitude: f64,
                accuracy: f64,
                altitude_accuracy: f64,
                heading: f64,
                speed: f64,
            }

            // object aggregation
            let data = Geolocation {
                latitude: 1.34,
                longitude: 2.34,
                altitude: 3.34,
                accuracy: 4.34,
                altitude_accuracy: 5.34,
                heading: 6.34,
                speed: 7.34,
            };

            w.send_object(
                "org.astarte-platform.genericsensors.Geolocation",
                "/1/",
                data,
            )
            .await
            .unwrap();
        }
    });

    loop {
        if let Ok(data) = device.poll().await {
            println!("incoming data: {:?}", data);
        }
    }
}
