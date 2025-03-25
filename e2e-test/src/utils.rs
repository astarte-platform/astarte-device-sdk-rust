use std::future::Future;
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
use std::str::FromStr;
use std::time::Duration;

use base64::prelude::*;
use base64::Engine;
use chrono::{DateTime, Utc};
use eyre::bail;
use eyre::eyre;

use astarte_device_sdk::types::AstarteType;
use serde_json::Value;
use tracing::warn;

pub type Timestamp = DateTime<Utc>;

pub fn base64_decode<T>(input: T) -> Result<Vec<u8>, base64::DecodeError>
where
    T: AsRef<[u8]>,
{
    BASE64_STANDARD.decode(input)
}

pub fn base64_encode<T>(input: T) -> String
where
    T: AsRef<[u8]>,
{
    BASE64_STANDARD.encode(input)
}

pub fn timestamp_from_rfc3339(input: &str) -> chrono::ParseResult<Timestamp> {
    DateTime::parse_from_rfc3339(input).map(|d| d.to_utc())
}

/// Retry the future multiple times
pub(crate) async fn retry<F, T, U>(times: usize, mut f: F) -> eyre::Result<U>
where
    F: FnMut() -> T,
    T: Future<Output = eyre::Result<U>>,
{
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    for i in 1..=times {
        match (f)().await {
            Ok(o) => return Ok(o),
            Err(err) => {
                warn!("failed retry {i} for: {err}");

                interval.tick().await;
            }
        }
    }

    bail!("to many attempts")
}

pub(crate) fn check_astarte_value(data: &AstarteType, value: &Value) -> eyre::Result<()> {
    let eq = match data {
        AstarteType::Double(exp) => value.as_f64().is_some_and(|v| v == *exp),
        AstarteType::Integer(exp) => value.as_i64().is_some_and(|v| v == i64::from(*exp)),
        AstarteType::Boolean(exp) => value.as_bool().is_some_and(|v| v == *exp),
        AstarteType::LongInteger(exp) => value.as_i64().is_some_and(|v| v == *exp),
        AstarteType::String(exp) => value.as_str().is_some_and(|v| v == exp),
        AstarteType::BinaryBlob(exp) => value
            .as_str()
            .map(base64_decode)
            .transpose()?
            .is_some_and(|blob| blob == *exp),
        AstarteType::DateTime(exp) => value
            .as_str()
            .map(Timestamp::from_str)
            .transpose()?
            .is_some_and(|date_time| date_time == *exp),
        AstarteType::DoubleArray(exp) => {
            let arr: Vec<f64> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteType::IntegerArray(exp) => {
            let arr: Vec<i32> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteType::BooleanArray(exp) => {
            let arr: Vec<bool> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteType::LongIntegerArray(exp) => {
            let arr: Vec<i64> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteType::StringArray(exp) => {
            let arr: Vec<String> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteType::BinaryBlobArray(exp) => {
            let arr: Vec<String> = serde_json::from_value(value.clone())?;
            let arr = arr
                .into_iter()
                .map(base64_decode)
                .collect::<Result<Vec<_>, _>>()?;

            arr == *exp
        }
        AstarteType::DateTimeArray(exp) => {
            let arr: Vec<String> = serde_json::from_value(value.clone())?;
            let arr = arr
                .into_iter()
                .map(|v| Timestamp::from_str(&v))
                .collect::<Result<Vec<_>, _>>()?;

            arr == *exp
        }
    };

    if !eq {
        Err(eyre!("data {data:?} != {value}"))
    } else {
        Ok(())
    }
}

pub(crate) fn convert_type_to_json(data: &AstarteType) -> serde_json::Value {
    match data {
        AstarteType::Double(v) => Value::from(*v),
        AstarteType::Integer(v) => Value::from(*v),
        AstarteType::Boolean(v) => Value::from(*v),
        AstarteType::LongInteger(v) => Value::from(*v),
        AstarteType::String(v) => Value::from(v.as_str()),
        AstarteType::BinaryBlob(v) => Value::from(base64_encode(v)),
        AstarteType::DateTime(v) => Value::from(v.to_rfc3339()),
        AstarteType::DoubleArray(v) => Value::from(v.as_slice()),
        AstarteType::IntegerArray(v) => Value::from(v.as_slice()),
        AstarteType::BooleanArray(v) => Value::from(v.as_slice()),
        AstarteType::LongIntegerArray(v) => Value::from(v.as_slice()),
        AstarteType::StringArray(v) => Value::from(v.as_slice()),
        AstarteType::BinaryBlobArray(v) => {
            Value::from(v.iter().map(base64_encode).collect::<Vec<String>>())
        }
        AstarteType::DateTimeArray(v) => {
            Value::from(v.iter().map(|d| d.to_rfc3339()).collect::<Vec<String>>())
        }
    }
}
