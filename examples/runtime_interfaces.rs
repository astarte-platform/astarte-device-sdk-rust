/*
 * This file is part of Astarte.
 *
 * Copyright 2022 SECO Mind Srl
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

use astarte_sdk::{builder::AstarteOptions, AstarteError};
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

    let sdk_options = AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url);

    let (device, mut eventloop) = astarte_sdk::AstarteSdk::new(sdk_options).await?;

    let w = device.clone();
    tokio::task::spawn(async move {
        let mut i: i64 = 0;
        loop {
            let mut ifc = astarte_sdk::Interfaces::default();
            ifc.add_interface_file("./examples/interfaces/com.test.Everything.json")
                .unwrap();
            w.update_interfaces(ifc).await.unwrap();

            w.send("com.test.Everything", "/longinteger", i)
                .await
                .unwrap();
            println!("Sent {}", i);

            i += 11;
            std::thread::sleep(std::time::Duration::from_millis(1000));

            let mut ifc = astarte_sdk::Interfaces::default();
            ifc.add_interface_file(
                "./examples/interfaces/org.astarte-platform.genericsensors.AvailableSensors.json",
            )
            .unwrap();
            w.update_interfaces(ifc).await.unwrap();

            w.send(
                "org.astarte-platform.genericsensors.AvailableSensors",
                "/1/name",
                format!("{}", i),
            )
            .await
            .unwrap();

            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    });

    loop {
        match device.poll(&mut eventloop).await {
            Ok(data) => {
                println!("incoming: {:?}", data);
            }
            Err(err) => log::error!("{:?}", err),
        }
    }
}
