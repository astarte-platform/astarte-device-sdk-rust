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

//! Handles the properties for the device.

use flate2::bufread::ZlibDecoder;
use log::error;

/// Error handling the properties.
#[derive(thiserror::Error, Debug)]
pub enum Error {
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
pub fn extract_set_properties(bdata: &[u8]) -> Result<Vec<String>, Error> {
    use std::io::Read;

    if bdata.len() < 4 {
        return Err(Error::PayloadTooShort(bdata.len()));
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
