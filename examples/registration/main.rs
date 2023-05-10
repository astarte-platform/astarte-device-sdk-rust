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
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    realm: String,
    device_id: String,
    pairing_token: String,
    pairing_url: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Load configuration
    let file = std::fs::read_to_string("./examples/registration/configuration.json").unwrap();
    let cfg: Config = serde_json::from_str(&file).unwrap();

    println!(
        "Attempting to register the device with the ID: {}",
        cfg.device_id
    );

    let credentials_secret = astarte_device_sdk::registration::register_device(
        &cfg.pairing_token,
        &cfg.pairing_url,
        &cfg.realm,
        &cfg.device_id,
    )
    .await
    .unwrap();

    println!("Device registered, received credentials secret is: {credentials_secret}");
}
