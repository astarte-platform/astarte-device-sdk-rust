// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Provides the structs for the Astarte MQTT Protocol.
//!
//! You can find more information about the protocol v1 in the [Astarte MQTT v1 Protocol](https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html).

use std::collections::HashMap;

use bson::Bson;
use chrono::{DateTime, Utc};
use log::trace;
use serde::{Deserialize, Serialize};

use crate::{types::AstarteType, Aggregation, Error};

/// The payload of an MQTT message.
///
/// It is serialized as a BSON object when sent over the wire.
///
/// The payload BSON specification can be found here: [BSON](https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#bson)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    #[serde(rename = "v")]
    pub value: AstarteType,
    #[serde(rename = "t", default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,
}

impl Payload {
    pub fn to_vec(&self) -> Result<Vec<u8>, Error> {
        let res = bson::to_vec(self)?;

        Ok(res)
    }
}

impl From<AstarteType> for Payload {
    fn from(value: AstarteType) -> Self {
        Self {
            value,
            timestamp: None,
        }
    }
}

pub fn serialize_individual(
    data: &AstarteType,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<Vec<u8>, Error> {
    let b_data = Bson::from(data);

    serialize(b_data, timestamp)
}

pub fn serialize(
    data: Bson,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<Vec<u8>, Error> {
    if let Bson::Null = data {
        return Ok(Vec::new());
    }

    let doc = match timestamp {
        Some(timestamp) => bson::doc! {
           "t": timestamp,
           "v": data
        },
        None => bson::doc! {
           "v": data,
        },
    };

    let buf = bson::to_vec(&doc)?;

    Ok(buf)
}

pub fn deserialize_individual(bdata: &[u8]) -> Result<AstarteType, Error> {
    if bdata.is_empty() {
        trace!("empty document");

        return Ok(AstarteType::Unset);
    }

    let mut document: bson::Document = bson::from_slice(bdata)?;

    trace!("{:?}", document);

    // Take the value without cloning
    let value = document
        .remove("v")
        .ok_or_else(|| Error::DeserializationMissingValue(document))?;

    AstarteType::try_from(value)
}

pub fn deserialize(bdata: &[u8]) -> Result<Aggregation, Error> {
    if bdata.is_empty() {
        return Ok(Aggregation::Individual(AstarteType::Unset));
    }

    let mut document: bson::Document = bson::from_slice(bdata)?;

    trace!("{:?}", document);

    // Take the value without cloning
    let value = document
        .remove("v")
        .ok_or_else(|| Error::DeserializationMissingValue(document))?;

    match value {
        Bson::Document(doc) => {
            let hmap = doc
                .into_iter()
                .map(|(name, value)| AstarteType::try_from(value).map(|v| (name, v)))
                .collect::<Result<HashMap<String, AstarteType>, Error>>()?;

            Ok(Aggregation::Object(hmap))
        }
        _ => {
            let individual = AstarteType::try_from(value)?;

            Ok(Aggregation::Individual(individual))
        }
    }
}

pub fn serialize_object(
    data: HashMap<String, AstarteType>,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<Vec<u8>, Error> {
    let iter = data.into_iter().map(|(k, v)| (k, Bson::from(v)));

    let doc: bson::Document = bson::Document::from_iter(iter);
    let bson: Bson = Bson::Document(doc);

    serialize(bson, timestamp)
}
