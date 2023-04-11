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
//! Provides mock data to be used for testing the Astarte aggregates.
use std::collections::HashMap;

use base64::Engine;
use chrono::{DateTime, Utc};

use astarte_device_sdk::types::AstarteType;
use astarte_device_sdk::AstarteAggregate;
#[cfg(not(feature = "derive"))]
use astarte_device_sdk_derive::AstarteAggregate;

use crate::utils;

#[derive(Debug, Clone, PartialEq, AstarteAggregate)]
pub struct TestAggregateData {
    double_endpoint: f64,
    integer_endpoint: i32,
    boolean_endpoint: bool,
    longinteger_endpoint: i64,
    string_endpoint: String,
    binaryblob_endpoint: Vec<u8>,
    datetime_endpoint: DateTime<Utc>,
    doublearray_endpoint: Vec<f64>,
    integerarray_endpoint: Vec<i32>,
    booleanarray_endpoint: Vec<bool>,
    longintegerarray_endpoint: Vec<i64>,
    stringarray_endpoint: Vec<String>,
    binaryblobarray_endpoint: Vec<Vec<u8>>,
    datetimearray_endpoint: Vec<DateTime<Utc>>,
}

pub struct MockDataAggregate {
    device_to_server: TestAggregateData,
    server_to_device: HashMap<String, AstarteType>,
}

impl MockDataAggregate {
    /// Initialize a new instance for the MockDataAggregate struct.
    ///
    /// Fills the data structs with predefined data.
    pub fn init() -> Self {
        let device_to_server = TestAggregateData {
            double_endpoint: 4.34,
            integer_endpoint: 1,
            boolean_endpoint: true,
            longinteger_endpoint: 45543543534,
            string_endpoint: "Hello".to_string(),
            binaryblob_endpoint: base64::engine::general_purpose::STANDARD
                .decode("aGVsbG8=")
                .unwrap(),
            datetime_endpoint: DateTime::<Utc>::from(
                DateTime::parse_from_rfc3339("2021-09-29T17:46:48.000Z").unwrap(),
            ),
            doublearray_endpoint: Vec::from([43.5, 10.5, 11.9]),
            integerarray_endpoint: Vec::from([-4, 123, -2222, 30]),
            booleanarray_endpoint: Vec::from([true, false]),
            longintegerarray_endpoint: Vec::from([53267895478, 53267895428, 53267895118]),
            stringarray_endpoint: Vec::from(["Test ".to_string(), "String".to_string()]),
            binaryblobarray_endpoint: Vec::from([
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
            ]),
            datetimearray_endpoint: Vec::from([
                DateTime::<Utc>::from(
                    DateTime::parse_from_rfc3339("2021-10-23T17:46:48.000Z").unwrap(),
                ),
                DateTime::<Utc>::from(
                    DateTime::parse_from_rfc3339("2021-11-11T17:46:48.000Z").unwrap(),
                ),
            ]),
        };
        let server_to_device = utils::initialize_hash_map(
            Some(("double_endpoint".to_string(), 95.8)),
            Some(("integer_endpoint".to_string(), 20)),
            Some(("boolean_endpoint".to_string(), true)),
            Some(("longinteger_endpoint".to_string(), 45993543534)),
            Some(("string_endpoint".to_string(), "string for test".to_string())),
            None,
            None,
            Some((
                "doublearray_endpoint".to_string(),
                Vec::from([143.3, 11.8, 24.1, 33.4]),
            )),
            Some((
                "integerarray_endpoint".to_string(),
                Vec::from([12, 0, -4, 3]),
            )),
            Some((
                "booleanarray_endpoint".to_string(),
                Vec::from([false, false, false, true]),
            )),
            Some((
                "longintegerarray_endpoint".to_string(),
                Vec::from([56167895478, 56567895473, 56567815478]),
            )),
            Some((
                "stringarray_endpoint".to_string(),
                Vec::from(["I am ".to_string(), "a string".to_string()]),
            )),
            None,
            None,
        );
        MockDataAggregate {
            device_to_server,
            server_to_device,
        }
    }
    /// Fill the device to server data from a json file. Consumes the MockDataAggregate struct.
    ///
    /// The input is expected to be in the following format:
    /// ```
    /// Object {
    ///     "data" : Object {
    ///         <SENSOR_N> : Array [
    ///             Object {
    ///                 <ENDPOINT>: Type(<VALUE>)
    ///                 <ENDPOINT_ARRAY>: Array[Type(<VALUE>), Type(<VALUE>), ...]
    ///                 ...
    ///                 "timestamp" : String(...)
    ///                 ...
    ///             }
    ///             Object {
    ///                 ...
    ///             }
    ///             ...
    ///         ]
    ///     }
    /// }
    /// ```
    /// Where `Type` is one of `String`, `Bool` or `Number`.
    /// Each element of the outermost array is a single value of the aggregate, ordered as older
    /// first.
    ///
    /// # Arguments
    /// - *json_obj*: A json object formatted using the serde library.
    /// - *sensor_n*: Sensor number.
    pub fn fill_device_to_server_data_from_json(
        mut self,
        json_obj: &serde_json::Value,
        sensor_number: i8,
    ) -> Result<Self, String> {
        let err = format!("Incorrectly formatted json: {json_obj:#?}.");
        let json_map = json_obj
            .get("data")
            .ok_or(&err)?
            .get(sensor_number.to_string())
            .ok_or(&err)?
            .as_array()
            .ok_or(&err)?
            .last()
            .ok_or(&err)?
            .as_object()
            .ok_or(&err)?;
        let mut data = TestAggregateData {
            double_endpoint: 0.0,
            integer_endpoint: 0,
            boolean_endpoint: true,
            longinteger_endpoint: 0,
            string_endpoint: "".to_string(),
            binaryblob_endpoint: Vec::new(),
            datetime_endpoint: chrono::offset::Utc::now(),
            doublearray_endpoint: Vec::new(),
            integerarray_endpoint: Vec::new(),
            booleanarray_endpoint: Vec::new(),
            longintegerarray_endpoint: Vec::new(),
            stringarray_endpoint: Vec::new(),
            binaryblobarray_endpoint: Vec::new(),
            datetimearray_endpoint: Vec::new(),
        };
        for (key, value) in json_map {
            if key == "timestamp" {
                continue;
            }
            let err = format!("Failed converting the json value: {value} with key: {key}");
            match key.as_str() {
                "double_endpoint" => {
                    data.double_endpoint = value.as_f64().ok_or(err)?;
                }
                "integer_endpoint" => {
                    data.integer_endpoint = value.as_i64().ok_or(err)? as i32;
                }
                "boolean_endpoint" => {
                    data.boolean_endpoint = value.as_bool().ok_or(err)?;
                }
                "longinteger_endpoint" => {
                    data.longinteger_endpoint = value
                        .as_str()
                        .ok_or(&err)?
                        .parse::<i64>()
                        .map_err(|e| err + &e.to_string())?;
                }
                "string_endpoint" => {
                    data.string_endpoint = value.as_str().ok_or(err)?.to_string();
                }
                "binaryblob_endpoint" => {
                    data.binaryblob_endpoint = base64::engine::general_purpose::STANDARD
                        .decode(value.as_str().ok_or(&err)?)
                        .map_err(|e| err + &e.to_string())?;
                }
                "datetime_endpoint" => {
                    data.datetime_endpoint =
                        chrono::DateTime::parse_from_rfc3339(value.as_str().ok_or(&err)?)
                            .map_err(|e| err + &e.to_string())?
                            .into();
                }
                "doublearray_endpoint" => {
                    data.doublearray_endpoint = value
                        .as_array()
                        .ok_or(&err)?
                        .iter()
                        .map(|d| d.as_f64().ok_or_else(|| err.clone()))
                        .collect::<Result<Vec<f64>, String>>()?
                }
                "integerarray_endpoint" => {
                    data.integerarray_endpoint = value
                        .as_array()
                        .ok_or(&err)?
                        .iter()
                        .map(|d| d.as_i64().map(|b| b as i32).ok_or_else(|| err.clone()))
                        .collect::<Result<Vec<i32>, String>>()?;
                }
                "booleanarray_endpoint" => {
                    data.booleanarray_endpoint = value
                        .as_array()
                        .ok_or(&err)?
                        .iter()
                        .map(|d| d.as_bool().ok_or_else(|| err.clone()))
                        .collect::<Result<Vec<bool>, String>>()?;
                }
                "longintegerarray_endpoint" => {
                    data.longintegerarray_endpoint = value
                        .as_array()
                        .ok_or(&err)?
                        .iter()
                        .map(|d| {
                            d.as_str()
                                .map(|s| s.parse::<i64>())
                                .ok_or_else(|| err.clone())
                        })
                        .collect::<Result<Result<Vec<i64>, _>, String>>()?
                        .map_err(|e| err + &e.to_string())?;
                }
                "stringarray_endpoint" => {
                    data.stringarray_endpoint = value
                        .as_array()
                        .ok_or(&err)?
                        .iter()
                        .map(|d| d.as_str().map(|s| s.to_string()).ok_or_else(|| err.clone()))
                        .collect::<Result<Vec<String>, String>>()?;
                }

                "binaryblobarray_endpoint" => {
                    data.binaryblobarray_endpoint = value
                        .as_array()
                        .ok_or(&err)?
                        .iter()
                        .map(|d| {
                            base64::engine::general_purpose::STANDARD
                                .decode(d.as_str().ok_or(&err)?)
                                .map_err(|e| err.to_string() + &e.to_string())
                        })
                        .collect::<Result<Vec<Vec<u8>>, String>>()?;
                }

                "datetimearray_endpoint" => {
                    data.datetimearray_endpoint = value
                        .as_array()
                        .ok_or(&err)?
                        .iter()
                        .map(|d| {
                            Ok(
                                chrono::DateTime::parse_from_rfc3339(d.as_str().ok_or(&err)?)
                                    .map_err(|e| err.clone() + &e.to_string())?
                                    .into(),
                            )
                        })
                        .collect::<Result<Vec<DateTime<Utc>>, String>>()?;
                }
                _ => {
                    return Err(format!("Unrecongnized key :{key}"));
                }
            };
        }
        self.device_to_server = data;
        Ok(self)
    }

    /// Getter function for the mock data to be sent from device to server.
    ///
    /// Returns a filled TestAggregateData struct.
    pub fn get_device_to_server_data_as_struct(&self) -> TestAggregateData {
        self.device_to_server.clone()
    }

    /// Getter function for the mock data to be sent from server to device.
    ///
    /// Returns values as AstarteType.
    pub fn get_server_to_device_data_as_astarte(&self) -> HashMap<String, AstarteType> {
        self.server_to_device.clone()
    }

    /// Getter function for the mock data to be sent from server to device.
    ///
    /// Returns values as json string.
    pub fn get_server_to_device_data_as_json(&self) -> String {
        let mut elements = Vec::new();
        for (key, value) in self.server_to_device.clone() {
            elements.push(format!(
                "\"{key}\":{}",
                utils::json_string_from_astarte_type(value)
            ));
        }
        format!("{{\"data\":{{{}}}}}", elements.join(","))
    }
}
