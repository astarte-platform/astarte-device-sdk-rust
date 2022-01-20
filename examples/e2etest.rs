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

use astarte_sdk::builder::AstarteOptions;
use astarte_sdk::types::AstarteType;
use log::error;

#[tokio::main]
async fn main() {
    env_logger::init();

    let realm = "test";
    let device_id = std::env::var("E2E_DEVICE_ID").unwrap();
    let credentials_secret = std::env::var("E2E_CREDENTIALS_SECRET").unwrap();
    let pairing_url = "https://api.autotest.astarte-platform.org/pairing";

    let sdk_options = AstarteOptions::new(realm, &device_id, &credentials_secret, pairing_url)
        .interface_file(std::path::Path::new(
            "./examples/interfaces/org.astarte-platform.test.Everything.json",
        ))
        .unwrap()
        .ignore_ssl_errors()
        .build();

    let mut device = astarte_sdk::AstarteSdk::new(&sdk_options).await.unwrap();

    let alltypes: Vec<AstarteType> = vec![
        AstarteType::Double(4.5),
        (-4).into(),
        true.into(),
        45543543534_i64.into(),
        "hello".into(),
        b"hello".to_vec().into(),
        chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0).into(),
        vec![1.2, 3.4, 5.6, 7.8].into(),
        vec![1, 3, 5, 7].into(),
        vec![true, false, true, true].into(),
        vec![45543543534_i64, 45543543535_i64, 45543543536_i64].into(),
        vec!["hello".to_owned(), "world".to_owned()].into(),
        vec![b"hello".to_vec(), b"world".to_vec()].into(),
        vec![
            chrono::TimeZone::timestamp(&chrono::Utc, 1627580808, 0),
            chrono::TimeZone::timestamp(&chrono::Utc, 1627580809, 0),
            chrono::TimeZone::timestamp(&chrono::Utc, 1627580810, 0),
        ]
        .into(),
    ];

    let allendpoints = vec![
        "double",
        "integer",
        "boolean",
        "longinteger",
        "string",
        "binaryblob",
        "datetime",
        "doublearray",
        "integerarray",
        "booleanarray",
        "longintegerarray",
        "stringarray",
        "binaryblobarray",
        "datetimearray",
    ];

    let w = device.clone();
    tokio::task::spawn(async move {
        for _ in 0..3 {
            let data = alltypes.iter().zip(allendpoints.iter());

            // individual aggregation
            for i in data {
                let ret = w
                    .send(
                        "org.astarte-platform.test.Everything",
                        &format!("/{}", i.1),
                        i.0.clone(),
                    )
                    .await;

                if let Err(err) = ret {
                    error!("send error {:?}", err);
                    std::process::exit(1);
                }

                std::thread::sleep(std::time::Duration::from_millis(5));
            }

            std::thread::sleep(std::time::Duration::from_millis(300));
        }

        std::thread::sleep(std::time::Duration::from_millis(1000));
        println!("Test completed successfully");
        std::process::exit(0);
    });

    loop {
        match device.poll().await {
            Ok(data) => {
                println!("incoming: {:?}", data);
            }
            Err(err) => {
                log::error!("poll error {:?}", err);
                std::process::exit(1);
            }
        }
    }
}
