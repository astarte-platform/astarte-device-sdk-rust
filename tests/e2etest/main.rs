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

//! End to end tests for the Astarte SDK.
//!
//! Three separated tests are run, one after the other:
//! - A test over datastreams
//! - A test over aggregates
//! - A test over properties
//!
//! All the test run only check for transmission from the Device to the Astarte remote instance.
//! It is never checked if messages from the server are correctly handled.

use std::collections::HashMap;
use std::time::Duration;
use std::{env, fs, panic, process};

use base64::Engine;
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::{task, time};

use astarte_sdk::builder::AstarteOptions;
use astarte_sdk::types::AstarteType;
use astarte_sdk::AstarteSdk;

#[tokio::main]
async fn main() {
    // Set hook for panic with a custom message
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        println!("Test failed");
        orig_hook(panic_info);
        process::exit(1);
    }));

    time::sleep(Duration::from_secs(1)).await;

    let realm = "test";
    let device_id = env::var("E2E_DEVICE_ID").unwrap();
    let credentials_secret = env::var("E2E_CREDENTIALS_SECRET").unwrap();
    let pairing_url = "https://api.autotest.astarte-platform.org/pairing";

    let sdk_options = AstarteOptions::new(realm, &device_id, &credentials_secret, pairing_url)
        .interface_directory("./tests/e2etest/interfaces/")
        .unwrap()
        .ignore_ssl_errors()
        .build();

    let mut device = AstarteSdk::new(&sdk_options).await.unwrap();

    let device_cpy = device.clone();
    task::spawn(async move {
        test_datastreams(&device_cpy, realm, &device_id).await;

        time::sleep(Duration::from_secs(1)).await;

        test_aggregates(&device_cpy, realm, &device_id).await;

        time::sleep(Duration::from_secs(1)).await;

        test_properties(&device_cpy).await;

        process::exit(0);
    });

    loop {
        match device.poll().await {
            Ok(data) => {
                println!("incoming: {:?}", data);
            }
            Err(err) => {
                println!("poll error {:?}", err);
                process::exit(1);
            }
        }
    }
}

/// Run the end to end test over for the datastream types.
///
/// # Arguments
/// - *device*: the Astarte SDK instance to use for the test.
/// - *realm*: the name of the Astarte realm on which the device is connected.
/// - *device_id*: the device ID associated with the Astarte SDK instance.
///
async fn test_datastreams(device: &AstarteSdk, realm: &str, device_id: &str) {
    // Retrive the mock data to use for testing from a file
    let test_data_json_str = fs::read_to_string("./tests/e2etest/mock_data/data_datastream.json")
        .expect("Unable to read json file");
    let test_data_json: Value =
        serde_json::from_str(&test_data_json_str).expect("JSON does not have correct format.");
    let test_data = parse_datastream_data_from_json(test_data_json).unwrap();

    // Send all the mock test data
    for (key, value) in test_data.clone() {
        device
            .send(
                "org.astarte-platform.e2etest.Datastream",
                &format!("/{}", key),
                value,
            )
            .await
            .unwrap();
        time::sleep(Duration::from_millis(5)).await;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Get the data stored in the server using an HTTPs GET command
    let json_str = get_interface_data_from_astarte(
        realm,
        device_id,
        "org.astarte-platform.e2etest.Datastream",
    )
    .await;

    // Check if the sent and received data match
    let json: Value = serde_json::from_str(&json_str).unwrap();
    let received_data = parse_datastream_data_from_json(json).unwrap();
    for (key, exp_value) in test_data.clone() {
        if let Some(rcv_value) = received_data.get(&key) {
            if exp_value.clone() != rcv_value.clone() {
                panic!("Can't find {} in json {:?}", key, json_str);
            }
        } else {
            panic!("Missing key: {} in json {:?}", key, json_str);
        }
    }

    println!("Test datastreams completed successfully");
}

/// Run the end to end test over for the aggregates types.
///
/// # Arguments
/// - *device*: the Astarte SDK instance to use for the test.
/// - *realm*: the name of the Astarte realm on which the device is connected.
/// - *device_id*: the device ID associated with the Astarte SDK instance.
///
async fn test_aggregates(device: &AstarteSdk, realm: &str, device_id: &str) {
    // Retrive the mock data to use for testing from a file
    let test_data_json_str = fs::read_to_string("./tests/e2etest/mock_data/data_aggregate.json")
        .expect("Unable to read json file");
    let test_data_json: Value =
        serde_json::from_str(&test_data_json_str).expect("JSON does not have correct format.");
    let test_data = parse_aggregate_data_from_json(test_data_json).unwrap();

    // Send the mock test data
    device
        .send_object(
            "org.astarte-platform.e2etest.Aggregate",
            "/45",
            test_data.clone(),
        )
        .await
        .unwrap();

    time::sleep(Duration::from_secs(1)).await;

    // Get the data stored in the server using an HTTPs GET command
    let json_str =
        get_interface_data_from_astarte(realm, device_id, "org.astarte-platform.e2etest.Aggregate")
            .await;

    // Check if the sent and received data match
    let json: Value = serde_json::from_str(&json_str).unwrap();
    let received_data = parse_aggregate_data_from_json(json).unwrap();
    for (key, exp_value) in test_data.clone() {
        if let Some(rcv_value) = received_data.get(&key) {
            if exp_value != *rcv_value {
                panic!("Can't find {} in json {:?}", key, json_str);
            }
        } else {
            panic!("Missing key: {} in json {:?}", key, json_str);
        }
    }

    println!("Test aggregates completed successfully");
}

/// Run the end to end test over for the properties types.
///
/// **N.B.** these tests are very shallow compared to the others.
///
/// # Arguments
/// - *device*: the Astarte SDK instance to use for the test.
///
async fn test_properties(device: &AstarteSdk) {
    device
        .send("org.astarte-platform.e2etest.Property", "/1/enable", true)
        .await
        .unwrap();

    device
        .unset("org.astarte-platform.e2etest.Property", "/1/enable")
        .await
        .unwrap();

    println!("Test properties completed successfully");
}

/// Parse a json file containing only datastreams to a hash map.
///
/// This function is tailor made for the data_datastream.json file.
///
/// # Arguments
/// - *json*: the input json file already parsed to a `serde_json::Value` type.
///
fn parse_datastream_data_from_json(json: Value) -> Result<HashMap<String, AstarteType>, String> {
    let err = "Unable to parse the json data.";
    let mut json_parsed: HashMap<String, AstarteType> = HashMap::new();

    let data = json.get("data").ok_or(err)?;
    if let Value::Object(data) = data {
        for (key, value) in data {
            let value = value.get("value").ok_or(err)?.clone();
            let parsed_value = astarte_type_from_json_value(key, value)?;
            json_parsed.insert(key.clone(), parsed_value);
        }
    } else {
        return Err(err.to_string());
    }
    Ok(json_parsed)
}

/// Parse a single value to an Astarte type.
///
/// This function supports all base types of Astarte.
///
/// # Arguments
/// - *astype*: the name of the astarte type to convert to.
/// - *jsvalue*: the value to parse to an Astarte type.
///
fn astarte_type_from_json_value(astype: &str, jsvalue: Value) -> Result<AstarteType, String> {
    let err = "Incorrect astarte type";
    match astype {
        "double" => Ok(AstarteType::Double(jsvalue.as_f64().ok_or(err)?)),
        "integer" => Ok(AstarteType::Integer(
            jsvalue.as_i64().map(|v| v as i32).ok_or(err)?,
        )),
        "boolean" => Ok(AstarteType::Boolean(jsvalue.as_bool().ok_or(err)?)),
        "longinteger" => Ok(AstarteType::LongInteger(jsvalue.as_i64().ok_or(err)?)),
        "string" => Ok(AstarteType::String(
            jsvalue.as_str().map(|v| v.to_string()).ok_or(err)?,
        )),
        "binaryblob" => {
            let bin_blob_str = jsvalue.as_str().ok_or(err)?;
            let bin_blob = base64::engine::general_purpose::STANDARD
                .decode(bin_blob_str)
                .map_err(|err| err.to_string())?;
            Ok(AstarteType::BinaryBlob(bin_blob))
        }
        "datetime" => {
            let date_time_str = jsvalue.as_str().ok_or(err)?;
            let date_time =
                DateTime::parse_from_rfc3339(date_time_str).map_err(|err| err.to_string())?;
            Ok(AstarteType::DateTime(DateTime::<Utc>::from(date_time)))
        }
        "doublearray" => {
            let unparsed_vec: Result<Vec<f64>, &str> = jsvalue
                .as_array()
                .ok_or(err)?
                .iter()
                .map(|v| v.as_f64().ok_or(err))
                .collect();
            unparsed_vec
                .map(AstarteType::DoubleArray)
                .map_err(|e| e.to_string())
        }
        "integerarray" => {
            let unparsed_vec: Result<Vec<i32>, &str> = jsvalue
                .as_array()
                .ok_or(err)?
                .iter()
                .map(|v| v.as_i64().map(|v| v as i32).ok_or(err))
                .collect();
            unparsed_vec
                .map(AstarteType::IntegerArray)
                .map_err(|e| e.to_string())
        }
        "booleanarray" => {
            let unparsed_vec: Result<Vec<bool>, &str> = jsvalue
                .as_array()
                .ok_or(err)?
                .iter()
                .map(|v| v.as_bool().ok_or(err))
                .collect();
            unparsed_vec
                .map(AstarteType::BooleanArray)
                .map_err(|e| e.to_string())
        }
        "longintegerarray" => {
            let unparsed_vec: Result<Vec<i64>, &str> = jsvalue
                .as_array()
                .ok_or(err)?
                .iter()
                .map(|v| v.as_i64().ok_or(err))
                .collect();
            unparsed_vec
                .map(AstarteType::LongIntegerArray)
                .map_err(|e| e.to_string())
        }
        "stringarray" => {
            let unparsed_vec: Result<Vec<String>, &str> = jsvalue
                .as_array()
                .ok_or(err)?
                .iter()
                .map(|v| v.as_str().map(|v| v.to_string()).ok_or(err))
                .collect();
            unparsed_vec
                .map(AstarteType::StringArray)
                .map_err(|e| e.to_string())
        }
        "binaryblobarray" => {
            let unparsed_vec: Result<Vec<Vec<u8>>, String> = jsvalue
                .as_array()
                .ok_or(err)?
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(|v| {
                            base64::engine::general_purpose::STANDARD
                                .decode(v)
                                .map_err(|err| err.to_string())
                        })
                        .ok_or(err)?
                })
                .collect();
            unparsed_vec.map(AstarteType::BinaryBlobArray)
        }
        "datetimearray" => {
            let unparsed_vec = jsvalue
                .as_array()
                .ok_or(err)?
                .iter()
                .map(|v| v.as_str().ok_or(err))
                .collect::<Result<Vec<&str>, &str>>()?;
            let unparsed_vec = unparsed_vec
                .iter()
                .map(|v| DateTime::parse_from_rfc3339(v))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| err.to_string())?;
            let parsed_vec = unparsed_vec
                .iter()
                .map(|dt| DateTime::<Utc>::from(*dt))
                .collect::<Vec<_>>();
            Ok(AstarteType::DateTimeArray(parsed_vec))
        }
        _ => Err(err.to_string()),
    }
}

/// Parse a json file containing a single aggregate to a hash map.
///
/// This function is tailor made for the data_aggregate.json file.
///
/// # Arguments
/// - *json*: the input json file already parsed to a `serde_json::Value` type.
///
fn parse_aggregate_data_from_json(json: Value) -> Result<HashMap<String, f64>, String> {
    let err0 = "Unable to parse the json data.";
    let err1 = "Incompatible astarte type";
    let mut json_parsed: HashMap<String, f64> = HashMap::new();

    let data = json
        .get("data")
        .ok_or(err0)?
        .get("45")
        .ok_or(err0)?
        .get(0)
        .ok_or(err0)?;

    if let Value::Object(data) = data {
        for (key, value) in data {
            if let Value::Number(value) = value {
                json_parsed.insert(key.clone(), value.as_f64().ok_or(err1)?);
            }
        }
    } else {
        return Err(err0.to_string());
    }
    Ok(json_parsed)
}

/// Use HTTPS to fetch the content of an interface from an Astarte instance.
///
/// Returns the response to GET, a json file containing the interface content.
///
/// # Arguments
/// - *device*: the Astarte SDK instance to use for the test.
/// - *realm*: the name of the Astarte realm on which the device is connected.
/// - *interface*: The interface whose data should be retrived.
///
async fn get_interface_data_from_astarte(realm: &str, device_id: &str, interface: &str) -> String {
    reqwest::Client::new()
        .get(format!(
            "https://api.autotest.astarte-platform.org/appengine/v1/{}/devices/{}/interfaces/{}",
            realm, device_id, interface
        ))
        .header(
            "Authorization",
            "Bearer ".to_string() + &env::var("E2E_TOKEN").unwrap(),
        )
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use chrono::{TimeZone, Utc};
    use serde_json::Value;

    use crate::parse_aggregate_data_from_json;
    use crate::parse_datastream_data_from_json;
    use astarte_sdk::types::AstarteType;

    #[rustfmt::skip]
    fn get_expected_datastream_data() -> HashMap<String, AstarteType> {
        let mut data_map: HashMap<String, AstarteType> = HashMap::new();

        data_map.insert(
            "double".to_string(),
            AstarteType::Double(4.5)
        );
        data_map.insert(
            "integer".to_string(),
            AstarteType::Integer(-4));
        data_map.insert(
            "boolean".to_string(),
            AstarteType::Boolean(true)
        );
        data_map.insert(
            "longinteger".to_string(),
            AstarteType::LongInteger(45543543534_i64),
        );
        data_map.insert(
            "string".to_string(),
            AstarteType::String("hello".into())
        );
        data_map.insert(
            "binaryblob".to_string(),
            AstarteType::BinaryBlob(b"hello".to_vec()),
        );
        data_map.insert(
            "datetime".to_string(),
            AstarteType::DateTime(TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap()),
        );
        data_map.insert(
            "doublearray".to_string(),
            AstarteType::DoubleArray(vec![1.2, 3.4, 5.6, 7.8]),
        );
        data_map.insert(
            "integerarray".to_string(),
            AstarteType::IntegerArray(vec![1, 3, 5, 7]),
        );
        data_map.insert(
            "booleanarray".to_string(),
            AstarteType::BooleanArray(vec![true, false, true, true]),
        );
        data_map.insert(
            "longintegerarray".to_string(),
            AstarteType::LongIntegerArray(vec![45543543534_i64, 45543543535_i64, 45543543536_i64]),
        );
        data_map.insert(
            "stringarray".to_string(),
            AstarteType::StringArray(vec!["hello".to_string(), "world".to_string()])
        );
        data_map.insert(
            "binaryblobarray".to_string(),
            AstarteType::BinaryBlobArray(vec![b"hello".to_vec(), b"world".to_vec()])
        );
        data_map.insert(
            "datetimearray".to_string(),
            AstarteType::DateTimeArray(vec![
                TimeZone::timestamp_opt(&Utc, 1627580808, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580809, 0).unwrap(),
                TimeZone::timestamp_opt(&Utc, 1627580810, 0).unwrap(),
            ])
        );

        data_map
    }

    fn get_expected_aggregate_data() -> HashMap<String, f64> {
        let mut data_map: HashMap<String, f64> = HashMap::new();

        data_map.insert("latitude".to_string(), 1.34);
        data_map.insert("longitude".to_string(), 2.34);
        data_map.insert("altitude".to_string(), 3.34);
        data_map.insert("accuracy".to_string(), 4.34);
        data_map.insert("altitudeAccuracy".to_string(), 5.34);
        data_map.insert("heading".to_string(), 6.34);
        data_map.insert("speed".to_string(), 7.34);

        data_map
    }

    #[test]
    fn test_get_datastream_data_from_json() {
        let json_str = fs::read_to_string("./tests/e2etest/mock_data/data_datastream.json")
            .expect("Unable to read file");
        let json: Value =
            serde_json::from_str(&json_str).expect("JSON does not have correct format.");
        let test_data = parse_datastream_data_from_json(json).unwrap();
        let expected_data = get_expected_datastream_data();

        assert_eq!(test_data, expected_data);
    }

    #[test]
    fn test_get_aggregate_data_from_json() {
        let json_str = fs::read_to_string("./tests/e2etest/mock_data/data_aggregate.json")
            .expect("Unable to read file");
        let json: Value =
            serde_json::from_str(&json_str).expect("JSON does not have correct format.");

        let test_data = parse_aggregate_data_from_json(json).unwrap();
        let expected_data = get_expected_aggregate_data();

        assert_eq!(test_data, expected_data);
    }
}
