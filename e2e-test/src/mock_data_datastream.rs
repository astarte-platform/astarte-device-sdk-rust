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
//! Provides mock data to be used for testing the Astarte datastream.
use std::collections::HashMap;

use astarte_device_sdk::types::AstarteType;
use base64::Engine;
use chrono::{DateTime, Utc};

use crate::utils;

pub struct MockDataDatastream {
    device_to_server: HashMap<String, AstarteType>,
    server_to_device: HashMap<String, AstarteType>,
}

impl MockDataDatastream {
    /// Initialize a new instance for the MockDataDatastream struct.
    ///
    /// Fills the data structs with predefined data.
    pub fn init() -> Self {
        let device_to_server = utils::initialize_hash_map(
            Some(("double_endpoint".to_string(), 4.5)),
            Some(("integer_endpoint".to_string(), -4)),
            Some(("boolean_endpoint".to_string(), true)),
            Some(("longinteger_endpoint".to_string(), 45543543534)),
            Some(("string_endpoint".to_string(), "hello".to_string())),
            Some((
                "binaryblob_endpoint".to_string(),
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
            )),
            Some((
                "datetime_endpoint".to_string(),
                DateTime::<Utc>::from(
                    DateTime::parse_from_rfc3339("2021-07-29T17:46:48.000Z").unwrap(),
                ),
            )),
            Some((
                "doublearray_endpoint".to_string(),
                Vec::from([1.2, 3.4, 5.6, 7.8]),
            )),
            Some((
                "integerarray_endpoint".to_string(),
                Vec::from([1, -3, 5, 7]),
            )),
            Some((
                "booleanarray_endpoint".to_string(),
                Vec::from([true, false, true, true]),
            )),
            Some((
                "longintegerarray_endpoint".to_string(),
                Vec::from([45543543534, 45543543535, 45543543536]),
            )),
            Some((
                "stringarray_endpoint".to_string(),
                Vec::from(["hello".to_string(), "world".to_string()]),
            )),
            Some((
                "binaryblobarray_endpoint".to_string(),
                Vec::from([
                    base64::engine::general_purpose::STANDARD
                        .decode("aGVsbG8=")
                        .unwrap(),
                    base64::engine::general_purpose::STANDARD
                        .decode("d29ybGQ=")
                        .unwrap(),
                ]),
            )),
            Some((
                "datetimearray_endpoint".to_string(),
                Vec::from([
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2021-07-29T17:46:48.000Z").unwrap(),
                    ),
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2021-07-29T17:46:49.000Z").unwrap(),
                    ),
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2021-07-29T17:46:50.000Z").unwrap(),
                    ),
                ]),
            )),
        );
        let server_to_device = utils::initialize_hash_map(
            Some(("double_endpoint".to_string(), 43.32)),
            Some(("integer_endpoint".to_string(), -5)),
            Some(("boolean_endpoint".to_string(), false)),
            Some(("longinteger_endpoint".to_string(), 56567895478)),
            Some(("string_endpoint".to_string(), "I am a string".to_string())),
            Some((
                "binaryblob_endpoint".to_string(),
                base64::engine::general_purpose::STANDARD
                    .decode("d29ybGQ=")
                    .unwrap(),
            )),
            Some((
                "datetime_endpoint".to_string(),
                DateTime::<Utc>::from(
                    DateTime::parse_from_rfc3339("2022-08-29T17:46:48.000Z").unwrap(),
                ),
            )),
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
            Some((
                "binaryblobarray_endpoint".to_string(),
                Vec::from([
                    base64::engine::general_purpose::STANDARD
                        .decode("aGVsbG8=")
                        .unwrap(),
                    base64::engine::general_purpose::STANDARD
                        .decode("aGVsbG8=")
                        .unwrap(),
                ]),
            )),
            Some((
                "datetimearray_endpoint".to_string(),
                Vec::from([
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2022-06-29T17:46:48.000Z").unwrap(),
                    ),
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2022-11-29T17:46:49.000Z").unwrap(),
                    ),
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2022-10-29T17:46:50.000Z").unwrap(),
                    ),
                ]),
            )),
        );
        MockDataDatastream {
            device_to_server,
            server_to_device,
        }
    }

    /// Fill the device to server data from a json file. Consumes the MockDataDatastream struct.
    ///
    /// The input is expected to be in the following format:
    /// ```
    /// Object {
    ///     "data" : Object {
    ///         <ENDPOINT>: Object {
    ///             "value": Type(<VALUE>)
    ///             ...
    ///         }
    ///         <ENDPOINT_ARRAY>: Object {
    ///             "value": Array[Type(<VALUE>), Type(<VALUE>), ...]
    ///             ...
    ///         }
    ///     }
    /// }
    /// ```
    /// Where `Type` is one of `String`, `Bool` or `Number`.
    ///
    /// # Arguments
    /// - *json_obj*: A json object formatted using the serde library.
    pub fn fill_device_to_server_data_from_json(
        mut self,
        json_obj: &serde_json::Value,
    ) -> Result<Self, String> {
        let err = format!("Incorrectly formatted json: {json_obj:#?}.");

        let json_map = json_obj.get("data").ok_or(&err)?.as_object().ok_or(&err)?;
        let mut data = HashMap::new();
        for (key, value) in json_map {
            let astarte_value = utils::astarte_type_from_json_value(
                key.strip_suffix("_endpoint").ok_or(&err)?,
                value.get("value").ok_or(&err)?.clone(),
            )?;
            data.insert(key.to_string(), astarte_value);
        }
        self.device_to_server = data;
        Ok(self)
    }

    /// Getter function for the mock data to be sent from device to server.
    ///
    /// Returns values as AstarteType.
    pub fn get_device_to_server_data_as_astarte(&self) -> HashMap<String, AstarteType> {
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
    pub fn get_server_to_device_data_as_json(&self) -> HashMap<String, String> {
        let mut data = HashMap::new();
        for (key, value) in self.server_to_device.clone() {
            let val_json = format!(
                "{{\"data\":{}}}",
                utils::json_string_from_astarte_type(value)
            );
            data.insert(key, val_json);
        }
        data
    }
}
