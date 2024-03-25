/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
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
//! End to end tests for the Astarte SDK.
//!
//! Three sequential and bidirectional tests are performed:
//! - A test over datastreams
//! - A test over aggregates
//! - A test over properties

use core::panic;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use astarte_device_sdk::{Interface, Value};
use eyre::WrapErr;
use itertools::Itertools;
use log::{debug, error, info};
use reqwest::StatusCode;
use serde_json::Value as JsonValue;
use tokio::task::JoinSet;
use tokio::time;

use astarte_device_sdk::{
    builder::DeviceBuilder, prelude::*, store::memory::MemoryStore, transport::mqtt::MqttConfig,
    types::AstarteType,
};

mod mock_data_aggregate;
mod mock_data_datastream;
mod mock_data_property;
mod utils;

use mock_data_aggregate::MockDataAggregate;
use mock_data_datastream::MockDataDatastream;
use mock_data_property::MockDataProperty;

const INTERFACE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/interfaces");

#[derive(Clone)]
struct TestCfg {
    realm: String,
    device_id: String,
    credentials_secret: String,
    api_url: String,
    pairing_url: String,
    interfaces_fld: PathBuf,
    interface_datastream_so: String,
    interface_datastream_do: String,
    interface_aggregate_so: String,
    interface_aggregate_do: String,
    interface_property_so: String,
    interface_property_do: String,
    appengine_token: String,
}

fn read_env(name: &str) -> eyre::Result<String> {
    env::var(name).wrap_err_with(|| format!("culdn't read environment variable {name}"))
}

impl TestCfg {
    pub fn init() -> eyre::Result<Self> {
        let realm = read_env("E2E_REALM")?;
        let device_id = read_env("E2E_DEVICE_ID")?;
        let credentials_secret = read_env("E2E_CREDENTIALS_SECRET")?;
        let api_url = read_env("E2E_API_URL")?;
        let pairing_url = read_env("E2E_PAIRING_URL")?;

        let interfaces_fld = Path::new(INTERFACE_DIR).to_owned();

        let interface_datastream_so =
            "org.astarte-platform.rust.e2etest.ServerDatastream".to_string();
        let interface_datastream_do =
            "org.astarte-platform.rust.e2etest.DeviceDatastream".to_string();
        let interface_aggregate_so =
            "org.astarte-platform.rust.e2etest.ServerAggregate".to_string();
        let interface_aggregate_do =
            "org.astarte-platform.rust.e2etest.DeviceAggregate".to_string();
        let interface_property_do = "org.astarte-platform.rust.e2etest.DeviceProperty".to_string();
        let interface_property_so = "org.astarte-platform.rust.e2etest.ServerProperty".to_string();

        let appengine_token = read_env("E2E_TOKEN")?;

        Ok(TestCfg {
            realm,
            device_id,
            credentials_secret,
            api_url,
            pairing_url,
            interfaces_fld,
            interface_datastream_so,
            interface_datastream_do,
            interface_aggregate_so,
            interface_aggregate_do,
            interface_property_so,
            interface_property_do,
            appengine_token,
        })
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    env_logger::try_init()?;

    let test_cfg = TestCfg::init().wrap_err("Failed configuration initialization")?;

    let mut mqtt_config = MqttConfig::new(
        &test_cfg.realm,
        &test_cfg.device_id,
        &test_cfg.credentials_secret,
        &test_cfg.pairing_url,
    );

    // Ignore SSL for local testing
    if env::var("E2E_IGNORE_SSL").is_ok() {
        mqtt_config.ignore_ssl_errors();
    }

    let (mut device, mut rx_events) = DeviceBuilder::new()
        .store(MemoryStore::new())
        .interface_directory(&test_cfg.interfaces_fld)
        .unwrap()
        .connect(mqtt_config)
        .await
        .unwrap()
        .build();

    let rx_data_ind_datastream = Arc::new(Mutex::new(HashMap::new()));
    let rx_data_agg_datastream = Arc::new(Mutex::new((String::new(), HashMap::new())));
    let rx_data_ind_prop = Arc::new(Mutex::new((String::new(), HashMap::new())));

    let device_cpy = device.clone();
    let test_cfg_cpy = test_cfg.clone();
    let rx_data_ind_datastream_cpy = rx_data_ind_datastream.clone();
    let rx_data_agg_datastream_cpy = rx_data_agg_datastream.clone();
    let rx_data_ind_prop_cpy = rx_data_ind_prop.clone();

    let mut tasks = JoinSet::new();

    let handle_events = tasks.spawn(async move { device.handle_events().await });

    // Add the remaining interfaces
    let additional_interfaces = read_additional_interfaces()?;
    debug!("adding {} interfaces", additional_interfaces.len());
    device_cpy.extend_interfaces(additional_interfaces).await?;

    tasks.spawn(async move {
        // Run datastream tests
        test_datastream_device_to_server(&device_cpy, &test_cfg_cpy)
            .await
            .unwrap();
        test_datastream_server_to_device(&test_cfg_cpy, &rx_data_ind_datastream_cpy)
            .await
            .unwrap();
        // Run aggregate tests
        test_aggregate_device_to_server(&device_cpy, &test_cfg_cpy)
            .await
            .unwrap();
        test_aggregate_server_to_device(&test_cfg_cpy, &rx_data_agg_datastream_cpy)
            .await
            .unwrap();
        // Run properties tests
        test_property_device_to_server(&device_cpy, &test_cfg_cpy)
            .await
            .unwrap();
        test_property_server_to_device(&test_cfg_cpy, &rx_data_ind_prop_cpy)
            .await
            .unwrap();

        info!("Test datastreams completed successfully");
        handle_events.abort();

        Ok(())
    });

    tasks.spawn(async move {
        // Poll any astarte message and store its content in the correct shared data structure
        while let Some(event) = rx_events.recv().await {
            match event {
                Ok(data) => {
                    if data.interface == test_cfg.interface_datastream_so {
                        if let Value::Individual(var) = data.data {
                            let mut rx_data = rx_data_ind_datastream.lock().unwrap();
                            let mut key = data.path.clone();
                            key.remove(0);
                            rx_data.insert(key, var);
                        } else {
                            panic!("Received unexpected message!");
                        }
                    } else if data.interface == test_cfg.interface_aggregate_so {
                        if let Value::Object(var) = data.data {
                            let mut rx_data = rx_data_agg_datastream.lock().unwrap();
                            let mut sensor_n = data.path.clone();
                            sensor_n.remove(0);
                            rx_data.0 = sensor_n;
                            for (key, value) in var {
                                rx_data.1.insert(key, value);
                            }
                        } else {
                            panic!("Received unexpected message!");
                        }
                    } else if data.interface == test_cfg.interface_property_so {
                        let mut rx_data = rx_data_ind_prop.lock().unwrap();

                        let mut path = data.path.clone();
                        path.remove(0); // Remove first forward slash
                        let (sensor_n, key) = path
                            .split_once('/')
                            .unwrap_or_else(|| panic!("Incorrect path in message {:?}", data));

                        rx_data.0 = sensor_n.to_string();

                        match data.clone().data {
                            Value::Individual(var) => {
                                rx_data.1.insert(key.to_string(), var);
                            }
                            Value::Unset => {
                                rx_data.1.remove(key);
                            }
                            _ => panic!("Received unexpected message!"),
                        };
                    } else {
                        panic!("Received unexpected message!");
                    }
                }
                Err(err) => {
                    panic!("poll error {err:?}");
                }
            }
        }

        Ok(())
    });

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                error!("Task panicked {err}");

                return Err(err.into());
            }
            Ok(Err(err)) => {
                error!("Task returned an error {err}");

                return Err(err.into());
            }
        }
    }

    Ok(())
}

fn read_additional_interfaces() -> eyre::Result<Vec<Interface>> {
    let interfaces = [
        include_str!(
            "../interfaces/additional/org.astarte-platform.rust.e2etest.DeviceProperty.json"
        ),
        include_str!(
            "../interfaces/additional/org.astarte-platform.rust.e2etest.ServerProperty.json"
        ),
        include_str!(
            "../interfaces/additional/org.astarte-platform.rust.e2etest.ServerDatastream.json"
        ),
    ]
    .into_iter()
    .map(Interface::from_str)
    .try_collect()?;

    Ok(interfaces)
}

/// Run the end to end tests from device to server for individual datastreams.
///
/// # Arguments
/// - *device*: the Astarte SDK instance to use for the test.
/// - *test_cfg*: struct containing configuration settings for the tests.
async fn test_datastream_device_to_server(
    device: &impl Client,
    test_cfg: &TestCfg,
) -> Result<(), String> {
    let mock_data = MockDataDatastream::init();
    let tx_data = mock_data.get_device_to_server_data_as_astarte();

    // Send all the mock test data
    debug!("Sending device owned datastreams from device to server.");
    for (key, value) in tx_data.clone() {
        device
            .send(&test_cfg.interface_datastream_do, &format!("/{key}"), value)
            .await
            .map_err(|e| e.to_string())?;
        time::sleep(Duration::from_millis(5)).await;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Get the stored data using http requests
    debug!("Checking data stored on the server.");
    let http_get_response = http_get_intf(test_cfg, &test_cfg.interface_datastream_do).await?;

    // Check if the sent and received data match
    let data_json: JsonValue = serde_json::from_str(&http_get_response)
        .map_err(|_| "Reply from server is a bad json.".to_string())?;

    let rx_data = MockDataDatastream::init()
        .fill_device_to_server_data_from_json(&data_json)?
        .get_device_to_server_data_as_astarte();

    if tx_data != rx_data {
        Err([
            "Mismatch between server and device.",
            &format!("Expected data: {tx_data:?}. Server data: {rx_data:?}."),
        ]
        .join(" "))
    } else {
        Ok(())
    }
}

/// Run the end to end tests from server to device for individual datastreams.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the tests.
/// - *rx_data*: shared memory containing the received datastreams.
/// A different process will poll the device and then store the matching received messages
/// in this shared memory location.
async fn test_datastream_server_to_device(
    test_cfg: &TestCfg,
    rx_data: &Arc<Mutex<HashMap<String, AstarteType>>>,
) -> Result<(), String> {
    let mock_data = MockDataDatastream::init();

    // Send the data using http requests
    debug!("Sending server owned datastreams from server to device.");
    for (key, value) in mock_data.get_server_to_device_data_as_json() {
        http_post_to_intf(test_cfg, &test_cfg.interface_datastream_so, &key, value).await?;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Lock the shared data and check if everything sent has been correctly received

    debug!("Checking data received by the device.");
    let rx_data_rw_acc = rx_data
        .lock()
        .map_err(|e| format!("Failed to lock the shared data. {e}"))?;

    let exp_data = mock_data.get_server_to_device_data_as_astarte();
    if exp_data != *rx_data_rw_acc {
        Err([
            "Mismatch between expected and received data.",
            &format!("Expected data: {exp_data:?}. Server data: {rx_data_rw_acc:?}."),
        ]
        .join(" "))
    } else {
        Ok(())
    }
}

/// Run the end to end tests from device to server for aggregate datastreams.
///
/// # Arguments
/// - *device*: the Astarte SDK instance to use for the test.
/// - *test_cfg*: struct containing configuration settings for the tests.
async fn test_aggregate_device_to_server(
    device: &impl Client,
    test_cfg: &TestCfg,
) -> Result<(), String> {
    let mock_data = MockDataAggregate::init();
    let tx_data = mock_data.get_device_to_server_data_as_struct();
    let sensor_number: i8 = 45;

    // Send all the mock test data
    debug!("Sending device owned aggregate from device to server");
    device
        .send_object(
            &test_cfg.interface_aggregate_do,
            &format!("/{sensor_number}"),
            tx_data.clone(),
        )
        .await
        .map_err(|e| e.to_string())?;

    time::sleep(Duration::from_secs(1)).await;

    // Get the stored data using http requests
    debug!("Checking data stored on the server.");
    let http_get_response = http_get_intf(test_cfg, &test_cfg.interface_aggregate_do).await?;

    // Check if the sent and received data match
    let data_json: JsonValue = serde_json::from_str(&http_get_response)
        .map_err(|_| "Reply from server is a bad json.".to_string())?;

    let rx_data = MockDataAggregate::init()
        .fill_device_to_server_data_from_json(&data_json, sensor_number)?
        .get_device_to_server_data_as_struct();

    if tx_data != rx_data {
        Err([
            "Mismatch between server and device.",
            &format!("Expected data: {tx_data:?}. Server data: {rx_data:?}."),
        ]
        .join(" "))
    } else {
        Ok(())
    }
}

/// Run the end to end tests from server to device for aggregate datastreams.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the tests.
/// - *rx_data*: shared memory containing the received datastreams.
/// A different process will poll the device and then store the matching received messages
/// in this shared memory location.
async fn test_aggregate_server_to_device(
    test_cfg: &TestCfg,
    rx_data: &Arc<Mutex<(String, HashMap<String, AstarteType>)>>,
) -> Result<(), String> {
    let mock_data = MockDataAggregate::init();
    let sensor_number: i8 = 11;

    // Send the data using http requests
    debug!("Sending server owned aggregate from server to device.");
    http_post_to_intf(
        test_cfg,
        &test_cfg.interface_aggregate_so,
        &format!("{sensor_number}"),
        mock_data.get_server_to_device_data_as_json(),
    )
    .await?;

    time::sleep(Duration::from_secs(1)).await;

    // Lock the shared data and check if everything sent has been correctly received
    debug!("Checking data received by the device.");
    let rx_data_rw_acc = rx_data
        .lock()
        .map_err(|e| format!("Failed to lock the shared data. {e}"))?;

    let exp_data = mock_data.get_server_to_device_data_as_astarte();

    if (sensor_number.to_string() != rx_data_rw_acc.0) || (exp_data != rx_data_rw_acc.1) {
        Err([
            "Mismatch between expected and received data.",
            &format!("Expected data: {exp_data:?}. Server data: {rx_data_rw_acc:?}."),
        ]
        .join(" "))
    } else {
        Ok(())
    }
}

/// Run the end to end tests from device to server for properties.
///
/// # Arguments
/// - *device*: the Astarte SDK instance to use for the test.
/// - *test_cfg*: struct containing configuration settings for the tests.
async fn test_property_device_to_server(
    device: &impl Client,
    test_cfg: &TestCfg,
) -> Result<(), String> {
    let mock_data = MockDataProperty::init();
    let tx_data = mock_data.get_device_to_server_data_as_astarte();
    let sensor_number = 1;

    // Send all the mock test data
    debug!("Set device owned property (will be also sent to server).");
    for (key, value) in tx_data.clone() {
        device
            .send(
                &test_cfg.interface_property_do,
                &format!("/{sensor_number}/{key}"),
                value,
            )
            .await
            .map_err(|e| e.to_string())?;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Get the stored data using http requests
    debug!("Checking data stored on the server.");
    let http_get_response = http_get_intf(test_cfg, &test_cfg.interface_property_do).await?;

    // Check if the sent and received data match
    let data_json: JsonValue = serde_json::from_str(&http_get_response)
        .map_err(|_| "Reply from server is a bad json.".to_string())?;

    let rx_data = MockDataProperty::init()
        .fill_device_to_server_data_from_json(&data_json, sensor_number)?
        .get_device_to_server_data_as_astarte();

    if tx_data != rx_data {
        return Err([
            "Mismatch between server and device.",
            &format!("Expected data: {tx_data:?}. Server data: {rx_data:?}."),
        ]
        .join(" "));
    }

    // Unset one specific property
    debug!("Unset all the device owned property (will be also sent to server).");
    for (key, _) in tx_data.clone() {
        device
            .unset(
                &test_cfg.interface_property_do,
                &format!("/{sensor_number}/{key}"),
            )
            .await
            .map_err(|e| e.to_string())?;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Get the stored data using http requests
    debug!("Checking data stored on the server.");
    let http_get_response = http_get_intf(test_cfg, &test_cfg.interface_property_do).await?;

    if http_get_response != "{\"data\":{}}" {
        Err([
            "Mismatch between server and device.",
            &format!("Expected data: {{\"data\":{{}}}}. Server data: {http_get_response:?}."),
        ]
        .join(" "))
    } else {
        Ok(())
    }
}

/// Run the end to end tests from server to device for properties.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the tests.
/// - *rx_data*: shared memory containing the received properties.
/// A different process will poll the device and then store the matching received messages
/// in this shared memory location.
async fn test_property_server_to_device(
    test_cfg: &TestCfg,
    rx_data: &Arc<Mutex<(String, HashMap<String, AstarteType>)>>,
) -> Result<(), String> {
    let mock_data = MockDataProperty::init();
    let sensor_number: i8 = 42;

    // Send the data using http requests
    debug!("Sending server owned properties from server to device.");
    for (key, value) in mock_data.get_server_to_device_data_as_json() {
        http_post_to_intf(
            test_cfg,
            &test_cfg.interface_property_so,
            &format!("{sensor_number}/{key}"),
            value,
        )
        .await?;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Lock the shared data and check if everything sent has been correctly received
    {
        debug!("Checking data received by the device.");
        let rx_data_rw_acc = rx_data
            .lock()
            .map_err(|e| format!("Failed to lock the shared data. {e}"))?;

        let exp_data = mock_data.get_server_to_device_data_as_astarte();
        if (sensor_number.to_string() != rx_data_rw_acc.0) || (exp_data != rx_data_rw_acc.1) {
            return Err([
                "Mismatch between expected and received data.",
                &format!("Expected data: {exp_data:?}. Server data: {rx_data_rw_acc:?}."),
            ]
            .join(" "));
        }
    }

    // Unset all the properties
    debug!("Unsetting all the server owned properties (will be also sent to device).");
    for (key, _) in mock_data.get_server_to_device_data_as_json() {
        http_delete_to_intf(
            test_cfg,
            &test_cfg.interface_property_so,
            &format!("{sensor_number}/{key}"),
        )
        .await?;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Lock the shared data and check if everything sent has been correctly received
    {
        debug!("Checking data received by the device.");
        let rx_data_rw_acc = rx_data
            .lock()
            .map_err(|e| format!("Failed to lock the shared data. {e}"))?;

        if sensor_number.to_string() != rx_data_rw_acc.0 {
            return Err(format!(
                "Incorrect received data. Server data: {rx_data_rw_acc:?}."
            ));
        }
    }
    Ok(())
}

/// Perform an HTTP GET request to an Astarte interface.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the request.
/// - *interface*: interface for which to perform the GET request.
async fn http_get_intf(test_cfg: &TestCfg, interface: &str) -> Result<String, String> {
    let get_cmd = format!(
        "{}/v1/{}/devices/{}/interfaces/{}",
        test_cfg.api_url, test_cfg.realm, test_cfg.device_id, interface
    );
    debug!("Sending HTTP GET request: {get_cmd}");
    reqwest::Client::new()
        .get(get_cmd)
        .header(
            "Authorization",
            "Bearer ".to_string() + &test_cfg.appengine_token,
        )
        .send()
        .await
        .map_err(|e| format!("HTTP GET failure: {e}"))?
        .text()
        .await
        .map_err(|e| format!("Failure in parsing the HTTP GET result: {e}"))
}

/// Perform an HTTP POST request to an Astarte interface.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the request.
/// - *interface*: interface on which to perform the POST request.
/// - *path*: path for the endpoint on which the data should be written.
/// - *value_json*: value to be sent, already formatted as a json string.
async fn http_post_to_intf(
    test_cfg: &TestCfg,
    interface: &str,
    path: &str,
    value_json: String,
) -> Result<(), String> {
    let post_cmd = format!(
        "{}/v1/{}/devices/{}/interfaces/{}/{}",
        test_cfg.api_url, test_cfg.realm, test_cfg.device_id, interface, path
    );
    debug!("Sending HTTP POST request: {post_cmd} {value_json}");
    let response = reqwest::Client::new()
        .post(post_cmd)
        .header(
            "Authorization",
            "Bearer ".to_string() + &test_cfg.appengine_token,
        )
        .header("Content-Type", "application/json")
        .body(value_json.clone())
        .send()
        .await
        .map_err(|e| format!("HTTP POST failure: {e}"))?;
    if response.status() != StatusCode::OK {
        let response_text = response
            .text()
            .await
            .map_err(|e| format!("Failure in parsing the HTTP POST result: {e}"))?;
        return Err(format!(
            "Failure in POST command. Server response: {response_text}"
        ));
    }
    Ok(())
}

/// Perform an HTTP DELETE request to an Astarte interface.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the request.
/// - *interface*: interface on which to perform the DELETE request.
/// - *path*: path for the endpoint for which the data should be deleted.
async fn http_delete_to_intf(
    test_cfg: &TestCfg,
    interface: &str,
    path: &str,
) -> Result<(), String> {
    let post_cmd = format!(
        "{}/v1/{}/devices/{}/interfaces/{}/{}",
        test_cfg.api_url, test_cfg.realm, test_cfg.device_id, interface, path
    );
    debug!("Sending HTTP DELETE request: {post_cmd}");
    let response = reqwest::Client::new()
        .delete(post_cmd)
        .header(
            "Authorization",
            "Bearer ".to_string() + &test_cfg.appengine_token,
        )
        .send()
        .await
        .map_err(|e| format!("HTTP DELETE failure: {e}"))?;
    if response.status() != StatusCode::NO_CONTENT {
        let response_text = response
            .text()
            .await
            .map_err(|e| format!("Failure in parsing the HTTP DELETE result: {e}"))?;
        return Err(format!(
            "Failure in DELETE command. Server response: {response_text}"
        ));
    }
    Ok(())
}
