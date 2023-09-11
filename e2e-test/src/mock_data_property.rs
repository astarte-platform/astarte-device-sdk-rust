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
//! Provides mock data to be used for testing the Astarte propertiy.
use std::collections::HashMap;

use base64::Engine;
use chrono::{DateTime, Utc};

use astarte_device_sdk::types::AstarteType;

use crate::utils;

pub struct MockDataProperty {
    device_to_server: HashMap<String, AstarteType>,
    server_to_device: HashMap<String, AstarteType>,
}

impl MockDataProperty {
    /// Initialize a new instance for the MockDataProperty struct.
    ///
    /// Fills the data structs with predefined data.
    pub fn init() -> Self {
        let device_to_server = utils::initialize_hash_map(
            Some(("double_endpoint".to_string(), 11.3)),
            Some(("integer_endpoint".to_string(), -321)),
            Some(("boolean_endpoint".to_string(), true)),
            Some(("longinteger_endpoint".to_string(), 41133543534)),
            Some(("string_endpoint".to_string(), "string for prop".to_string())),
            Some((
                "binaryblob_endpoint".to_string(),
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
            )),
            Some((
                "datetime_endpoint".to_string(),
                DateTime::<Utc>::from(
                    DateTime::parse_from_rfc3339("2019-07-29T17:46:48.000Z").unwrap(),
                ),
            )),
            Some((
                "doublearray_endpoint".to_string(),
                Vec::from([43.2, 11.4, 0.6, 7.8]),
            )),
            Some((
                "integerarray_endpoint".to_string(),
                Vec::from([32, 121, -5, 7]),
            )),
            Some((
                "booleanarray_endpoint".to_string(),
                Vec::from([true, true, true, true]),
            )),
            Some((
                "longintegerarray_endpoint".to_string(),
                Vec::from([45543543500, 40043543535, 45543543116]),
            )),
            Some((
                "stringarray_endpoint".to_string(),
                Vec::from(["world ".to_string(), "hello".to_string()]),
            )),
            Some((
                "binaryblobarray_endpoint".to_string(),
                Vec::from([
                    base64::engine::general_purpose::STANDARD
                        .decode("d29ybGQ=")
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
                        DateTime::parse_from_rfc3339("2011-07-29T17:46:48.000Z").unwrap(),
                    ),
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2022-07-29T17:46:49.000Z").unwrap(),
                    ),
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2090-07-29T17:46:50.000Z").unwrap(),
                    ),
                ]),
            )),
        );
        let server_to_device = utils::initialize_hash_map(
            Some(("double_endpoint".to_string(), 52.3)),
            Some(("integer_endpoint".to_string(), -98)),
            Some(("boolean_endpoint".to_string(), true)),
            Some(("longinteger_endpoint".to_string(), 41100003534)),
            Some((
                "string_endpoint".to_string(),
                "string n2 for prop".to_string(),
            )),
            Some((
                "binaryblob_endpoint".to_string(),
                base64::engine::general_purpose::STANDARD
                    .decode("d29ybGQ=")
                    .unwrap(),
            )),
            Some((
                "datetime_endpoint".to_string(),
                DateTime::<Utc>::from(
                    DateTime::parse_from_rfc3339("2019-07-11T17:46:48.000Z").unwrap(),
                ),
            )),
            Some((
                "doublearray_endpoint".to_string(),
                Vec::from([0.3, 21.8, 24.1, 33.4]),
            )),
            Some((
                "integerarray_endpoint".to_string(),
                Vec::from([9, 0, 1, 37]),
            )),
            Some((
                "booleanarray_endpoint".to_string(),
                Vec::from([true, false, false, true]),
            )),
            Some((
                "longintegerarray_endpoint".to_string(),
                Vec::from([56161195478, 56567895473, 56567815411]),
            )),
            Some((
                "stringarray_endpoint".to_string(),
                Vec::from(["I am ".to_string(), "the string".to_string()]),
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
                        DateTime::parse_from_rfc3339("2009-06-29T17:46:48.000Z").unwrap(),
                    ),
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2009-08-29T17:46:49.000Z").unwrap(),
                    ),
                    DateTime::<Utc>::from(
                        DateTime::parse_from_rfc3339("2095-09-29T17:46:50.000Z").unwrap(),
                    ),
                ]),
            )),
        );
        MockDataProperty {
            device_to_server,
            server_to_device,
        }
    }

    /// Fill the device to server data from a json file. Consumes the MockDataProperty struct.
    ///
    /// The input is expected to be in the following format:
    /// ```
    /// Object {
    ///     "data" : Object {
    ///         <SENSOR_N> : Object {
    ///             <ENDPOINT>: Type(<VALUE>)
    ///             <ENDPOINT_ARRAY>: Array[Type(<VALUE>), Type(<VALUE>), ...]
    ///             ...
    ///         }
    ///     }
    /// }
    /// ```
    /// Where `Type` is one of `String`, `Bool` or `Number`.
    ///
    /// # Arguments
    /// - *json_obj*: A json object formatted using the serde library.
    /// - *sensor_number*: Sensor number.
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
            .as_object()
            .ok_or(&err)?;
        let mut data = HashMap::new();
        for (key, value) in json_map {
            let astarte_value = utils::astarte_type_from_json_value(
                key.strip_suffix("_endpoint").ok_or(&err)?,
                value.clone(),
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
