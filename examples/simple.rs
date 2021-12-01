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

    let mut sdk_builder =
        AstarteBuilder::new(&realm, &device_id, &credentials_secret, &pairing_url);

    let mut device = sdk_builder
        .add_interface_files("./examples/interfaces")
        .unwrap()
        .build()
        .await
        .unwrap();

    device.connect().await.unwrap();

    let mut w = device.clone();

    tokio::task::spawn(async move {
        let mut i: i64 = 0;
        loop {
            println!("Sending {}", i);

            w.send("com.test.Everything", "/longinteger", i)
                .await
                .unwrap();

            println!("Sent {}", i);

            i += 11;

            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        }
    });

    loop {
        match device.poll().await {
            Ok(data) => {
                println!("incoming: {:?}", data);

                if let astarte_sdk::Aggregation::Individual(var) = data.data {
                    if data.path == "/1/enable" {
                        if var == true {
                            println!("sensor is ON");
                        } else {
                            println!("sensor is OFF");
                        }
                    }
                }
            }
            Err(err) => log::error!("{:?}", err),
        }
    }
}
