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
use std::collections::HashMap;

use base64::Engine;
use chrono::{DateTime, Utc};
use itertools::Itertools;

use astarte_device_sdk::types::AstarteType;

/// Produces a newly initialized HashMap containing the specified arguments.
///
/// All arguments are optional.
#[allow(clippy::too_many_arguments)]
pub fn initialize_hash_map(
    double: Option<(String, f64)>,
    integer: Option<(String, i32)>,
    boolean: Option<(String, bool)>,
    longinteger: Option<(String, i64)>,
    string: Option<(String, String)>,
    binaryblob: Option<(String, Vec<u8>)>,
    datetime: Option<(String, chrono::DateTime<chrono::Utc>)>,
    doublearray: Option<(String, Vec<f64>)>,
    integerarray: Option<(String, Vec<i32>)>,
    booleanarray: Option<(String, Vec<bool>)>,
    longintegerarray: Option<(String, Vec<i64>)>,
    stringarray: Option<(String, Vec<String>)>,
    binaryblobarray: Option<(String, Vec<Vec<u8>>)>,
    datetimearray: Option<(String, Vec<chrono::DateTime<chrono::Utc>>)>,
) -> HashMap<String, AstarteType> {
    let mut data = HashMap::new();
    if let Some((k, v)) = double {
        data.insert(k, AstarteType::Double(v));
    }
    if let Some((k, v)) = integer {
        data.insert(k, AstarteType::Integer(v));
    }
    if let Some((k, v)) = boolean {
        data.insert(k, AstarteType::Boolean(v));
    }
    if let Some((k, v)) = longinteger {
        data.insert(k, AstarteType::LongInteger(v));
    }
    if let Some((k, v)) = string {
        data.insert(k, AstarteType::String(v));
    }
    if let Some((k, v)) = binaryblob {
        data.insert(k, AstarteType::BinaryBlob(v));
    }
    if let Some((k, v)) = datetime {
        data.insert(k, AstarteType::DateTime(v));
    }
    if let Some((k, v)) = doublearray {
        data.insert(k, AstarteType::DoubleArray(v));
    }
    if let Some((k, v)) = integerarray {
        data.insert(k, AstarteType::IntegerArray(v));
    }
    if let Some((k, v)) = booleanarray {
        data.insert(k, AstarteType::BooleanArray(v));
    }
    if let Some((k, v)) = longintegerarray {
        data.insert(k, AstarteType::LongIntegerArray(v));
    }
    if let Some((k, v)) = stringarray {
        data.insert(k, AstarteType::StringArray(v));
    }
    if let Some((k, v)) = binaryblobarray {
        data.insert(k, AstarteType::BinaryBlobArray(v));
    }
    if let Some((k, v)) = datetimearray {
        data.insert(k, AstarteType::DateTimeArray(v));
    }
    data
}

/// Parse a single value from a serde json format to an astarte type.
///
/// The serde json is expected to be in the following format:
/// ```
/// Type(<VALUE>)
/// ```
/// or
/// ```
/// Array [Type(<VALUE>), Type(<VALUE>), ...]
/// ```
/// Where `Type` shall be one of: `String`, `Number`, `Bool`.
///
/// # Arguments
/// - *astype*: The name of the Astarte Type to convert to.
/// - *jsvalue*: A json value formatted using the serde library.
pub fn astarte_type_from_json_value(
    astype: &str,
    jsvalue: serde_json::Value,
) -> Result<AstarteType, String> {
    let err =
        format!("Failure while trying to convert from type {astype} the json value: {jsvalue}");
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
                .ok_or(err.as_str())?
                .iter()
                .map(|v| v.as_f64().ok_or(err.as_str()))
                .collect();
            unparsed_vec
                .map(AstarteType::DoubleArray)
                .map_err(|e| e.to_string())
        }
        "integerarray" => {
            let unparsed_vec: Result<Vec<i32>, &str> = jsvalue
                .as_array()
                .ok_or(err.as_str())?
                .iter()
                .map(|v| v.as_i64().map(|v| v as i32).ok_or(err.as_str()))
                .collect();
            unparsed_vec
                .map(AstarteType::IntegerArray)
                .map_err(|e| e.to_string())
        }
        "booleanarray" => {
            let unparsed_vec: Result<Vec<bool>, &str> = jsvalue
                .as_array()
                .ok_or(err.as_str())?
                .iter()
                .map(|v| v.as_bool().ok_or(err.as_str()))
                .collect();
            unparsed_vec
                .map(AstarteType::BooleanArray)
                .map_err(|e| e.to_string())
        }
        "longintegerarray" => {
            let unparsed_vec: Result<Vec<i64>, &str> = jsvalue
                .as_array()
                .ok_or(err.as_str())?
                .iter()
                .map(|v| v.as_i64().ok_or(err.as_str()))
                .collect();
            unparsed_vec
                .map(AstarteType::LongIntegerArray)
                .map_err(|e| e.to_string())
        }
        "stringarray" => {
            let unparsed_vec: Result<Vec<String>, &str> = jsvalue
                .as_array()
                .ok_or(err.as_str())?
                .iter()
                .map(|v| v.as_str().map(|v| v.to_string()).ok_or(err.as_str()))
                .collect();
            unparsed_vec
                .map(AstarteType::StringArray)
                .map_err(|e| e.to_string())
        }
        "binaryblobarray" => {
            let unparsed_vec: Result<Vec<Vec<u8>>, String> = jsvalue
                .as_array()
                .ok_or(err.as_str())?
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(|v| {
                            base64::engine::general_purpose::STANDARD
                                .decode(v)
                                .map_err(|err| err.to_string())
                        })
                        .ok_or(err.as_str())?
                })
                .collect();
            unparsed_vec.map(AstarteType::BinaryBlobArray)
        }
        "datetimearray" => {
            let unparsed_vec = jsvalue
                .as_array()
                .ok_or(err.as_str())?
                .iter()
                .map(|v| v.as_str().ok_or(err.as_str()))
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
        _ => Err(err),
    }
}

/// Parse a single Astarte type to a json string.
///
/// # Arguments
/// - *astype*: The AstarteType that should be converted to a json string.
pub fn json_string_from_astarte_type(atype: AstarteType) -> String {
    match atype {
        AstarteType::Double(v) => v.to_string(),
        AstarteType::Integer(v) => v.to_string(),
        AstarteType::Boolean(v) => v.to_string(),
        AstarteType::LongInteger(v) => v.to_string(),
        AstarteType::String(v) => format!("\"{v}\""),
        AstarteType::BinaryBlob(v) => format!(
            "\"{}\"",
            base64::engine::general_purpose::STANDARD.encode(v)
        ),
        AstarteType::DateTime(v) => {
            format!("\"{}\"", DateTime::to_rfc3339(&v))
        }
        AstarteType::DoubleArray(v_list) => {
            format!("[{}]", &v_list.iter().map(|e| e.to_string()).join(","))
        }
        AstarteType::IntegerArray(v_list) => {
            format!("[{}]", &v_list.iter().map(|e| e.to_string()).join(","))
        }
        AstarteType::BooleanArray(v_list) => {
            format!("[{}]", &v_list.iter().map(|e| e.to_string()).join(","))
        }
        AstarteType::LongIntegerArray(v_list) => {
            format!("[{}]", &v_list.iter().map(|e| e.to_string()).join(","))
        }
        AstarteType::StringArray(v_list) => {
            format!("[{}]", &v_list.iter().map(|e| format!("\"{e}\"")).join(","))
        }
        AstarteType::BinaryBlobArray(v_list) => {
            String::from("[")
                + &v_list
                    .iter()
                    .map(|e| {
                        format!(
                            "\"{}\"",
                            base64::engine::general_purpose::STANDARD.encode(e)
                        )
                    })
                    .join(",")
                + "]"
        }
        AstarteType::DateTimeArray(v_list) => {
            String::from("[")
                + &v_list
                    .iter()
                    .map(|e| format!("\"{}\"", DateTime::to_rfc3339(e)))
                    .join(",")
                + "]"
        }
        _ => "".to_string(),
    }
}
