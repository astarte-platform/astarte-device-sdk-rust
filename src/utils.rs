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
use log::{error, trace};

use crate::{types::AstarteType, Aggregation, Error};
use flate2::read::ZlibDecoder;

/// Error parsing the /control/consumer/properties payload.
#[derive(thiserror::Error, Debug)]
pub enum PurgePropertiesError {
    /// The payload is too short, it should be at least 4 bytes long.
    #[error("the payload should at least 4 bytes long, got {0}")]
    PayloadTooShort(usize),
    /// Couldn't convert the size from u32 to usize.
    #[error("error converting the size from u32 to usize")]
    Conversion(#[from] std::num::TryFromIntError),

    /// Error decoding the zlib compressed payload.
    #[error("error decoding the zlib compressed payload")]
    Decode(#[from] std::io::Error),
}

/// Extracts the properties from a set payload.
///
/// See https://docs.astarte-platform.org/astarte/latest/080-mqtt-v1-protocol.html#purge-properties
pub fn extract_set_properties(bdata: &[u8]) -> Result<Vec<String>, PurgePropertiesError> {
    use std::io::prelude::*;

    if bdata.len() < 4 {
        return Err(PurgePropertiesError::PayloadTooShort(bdata.len()));
    }

    let (size, data) = bdata.split_at(4);
    // The size is a u32 jn big endian, so we need to convert it to usize
    let size: u32 = u32::from_be_bytes([size[0], size[1], size[2], size[3]]);
    let size: usize = size.try_into()?;

    let mut d = ZlibDecoder::new(data);
    let mut s = String::new();
    let bytes_red = d.read_to_string(&mut s)?;

    debug_assert_eq!(
        bytes_red, size,
        "Byte red and size mismatch: {} != {}",
        bytes_red, size
    );
    // Signal the error in production
    if bytes_red != size {
        error!("Byte red and size mismatch: {} != {}", bytes_red, size);
    }

    Ok(s.split(';').map(|x| x.to_string()).collect())
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
