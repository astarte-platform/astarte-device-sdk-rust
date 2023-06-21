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

//! Utilities for the Astarte Device SDK.

use std::collections::HashMap;

use bson::Bson;
use log::trace;

use crate::{types::AstarteType, Aggregation, AstarteError};
use flate2::read::ZlibDecoder;

pub fn extract_set_properties(bdata: &[u8]) -> Vec<String> {
    use std::io::prelude::*;

    let mut d = ZlibDecoder::new(&bdata[4..]);
    let mut s = String::new();
    d.read_to_string(&mut s).unwrap();

    s.split(';').map(|x| x.to_owned()).collect()
}

pub fn serialize_individual(
    data: &AstarteType,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<Vec<u8>, AstarteError> {
    let b_data = Bson::from(data);

    serialize(b_data, timestamp)
}

pub fn serialize(
    data: Bson,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<Vec<u8>, AstarteError> {
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

pub fn deserialize_individual(bdata: &[u8]) -> Result<AstarteType, AstarteError> {
    if bdata.is_empty() {
        trace!("empty document");

        return Ok(AstarteType::Unset);
    }

    let mut document: bson::Document = bson::from_slice(bdata)?;

    trace!("{:?}", document);

    // Take the value without cloning
    let value = document
        .remove("v")
        .ok_or_else(|| AstarteError::DeserializationMissingValue(document))?;

    AstarteType::try_from(value)
}

pub fn deserialize(bdata: &[u8]) -> Result<Aggregation, AstarteError> {
    if bdata.is_empty() {
        return Ok(Aggregation::Individual(AstarteType::Unset));
    }

    let mut document: bson::Document = bson::from_slice(bdata)?;

    trace!("{:?}", document);

    // Take the value without cloning
    let value = document
        .remove("v")
        .ok_or_else(|| AstarteError::DeserializationMissingValue(document))?;

    match value {
        Bson::Document(doc) => {
            let hmap = doc
                .into_iter()
                .map(|(name, value)| AstarteType::try_from(value).map(|v| (name, v)))
                .collect::<Result<HashMap<String, AstarteType>, AstarteError>>()?;

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
) -> Result<Vec<u8>, AstarteError> {
    let iter = data.into_iter().map(|(k, v)| (k, Bson::from(v)));

    let doc: bson::Document = bson::Document::from_iter(iter);
    let bson: Bson = Bson::Document(doc);

    serialize(bson, timestamp)
}
