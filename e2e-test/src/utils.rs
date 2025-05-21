// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::str::FromStr;
use std::time::Duration;

use base64::prelude::*;
use base64::Engine;
use chrono::{DateTime, Utc};
use eyre::bail;
use eyre::eyre;

use astarte_device_sdk::types::AstarteData;
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

pub(crate) fn check_astarte_value(data: &AstarteData, value: &Value) -> eyre::Result<()> {
    let eq = match data {
        AstarteData::Double(exp) => value.as_f64().is_some_and(|v| v == *exp),
        AstarteData::Integer(exp) => value.as_i64().is_some_and(|v| v == i64::from(*exp)),
        AstarteData::Boolean(exp) => value.as_bool().is_some_and(|v| v == *exp),
        AstarteData::LongInteger(exp) => value.as_i64().is_some_and(|v| v == *exp),
        AstarteData::String(exp) => value.as_str().is_some_and(|v| v == exp),
        AstarteData::BinaryBlob(exp) => value
            .as_str()
            .map(base64_decode)
            .transpose()?
            .is_some_and(|blob| blob == *exp),
        AstarteData::DateTime(exp) => value
            .as_str()
            .map(Timestamp::from_str)
            .transpose()?
            .is_some_and(|date_time| date_time == *exp),
        AstarteData::DoubleArray(exp) => {
            let arr: Vec<f64> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteData::IntegerArray(exp) => {
            let arr: Vec<i32> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteData::BooleanArray(exp) => {
            let arr: Vec<bool> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteData::LongIntegerArray(exp) => {
            let arr: Vec<i64> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteData::StringArray(exp) => {
            let arr: Vec<String> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteData::BinaryBlobArray(exp) => {
            let arr: Vec<String> = serde_json::from_value(value.clone())?;
            let arr = arr
                .into_iter()
                .map(base64_decode)
                .collect::<Result<Vec<_>, _>>()?;

            arr == *exp
        }
        AstarteData::DateTimeArray(exp) => {
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

pub(crate) fn convert_type_to_json(data: &AstarteData) -> serde_json::Value {
    match data {
        AstarteData::Double(v) => Value::from(f64::from(*v)),
        AstarteData::Integer(v) => Value::from(*v),
        AstarteData::Boolean(v) => Value::from(*v),
        AstarteData::LongInteger(v) => Value::from(*v),
        AstarteData::String(v) => Value::from(v.as_str()),
        AstarteData::BinaryBlob(v) => Value::from(base64_encode(v)),
        AstarteData::DateTime(v) => Value::from(v.to_rfc3339()),
        AstarteData::DoubleArray(v) => {
            Value::from(v.iter().map(|v| f64::from(*v)).collect::<Vec<f64>>())
        }
        AstarteData::IntegerArray(v) => Value::from(v.as_slice()),
        AstarteData::BooleanArray(v) => Value::from(v.as_slice()),
        AstarteData::LongIntegerArray(v) => Value::from(v.as_slice()),
        AstarteData::StringArray(v) => Value::from(v.as_slice()),
        AstarteData::BinaryBlobArray(v) => {
            Value::from(v.iter().map(base64_encode).collect::<Vec<String>>())
        }
        AstarteData::DateTimeArray(v) => {
            Value::from(v.iter().map(|d| d.to_rfc3339()).collect::<Vec<String>>())
        }
    }
}
