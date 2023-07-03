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
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum PropertiesError {
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
pub(crate) fn extract_set_properties(bdata: &[u8]) -> Result<Vec<String>, PropertiesError> {
    use std::io::Read;

    if bdata.len() < 4 {
        return Err(PropertiesError::PayloadTooShort(bdata.len()));
    }

    let (size, data) = bdata.split_at(4);
    // The size is a u32 in big endian, so we need to convert it to usize
    let size: u32 = u32::from_be_bytes([size[0], size[1], size[2], size[3]]);
    let size: usize = size.try_into()?;

    let mut d = ZlibDecoder::new(data);
    let mut s = String::new();
    let bytes_read = d.read_to_string(&mut s)?;

    debug_assert_eq!(
        bytes_read, size,
        "Byte red and size mismatch: {} != {}",
        bytes_read, size
    );
    // Signal the error in production
    if bytes_read != size {
        error!("Byte red and size mismatch: {} != {}", bytes_read, size);
    }

    Ok(s.split(';').map(|x| x.to_string()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) const PROPERTIES_PAYLOAD: [u8; 66] = [
        0x00, 0x00, 0x00, 0x46, 0x78, 0x9c, 0x4b, 0xce, 0xcf, 0xd5, 0x4b, 0xad, 0x48, 0xcc, 0x2d,
        0xc8, 0x49, 0xd5, 0xf3, 0xad, 0xf4, 0xcc, 0x2b, 0x49, 0x2d, 0x4a, 0x4b, 0x4c, 0x4e, 0xd5,
        0x2f, 0xce, 0xcf, 0x4d, 0xd5, 0x2f, 0x48, 0x2c, 0xc9, 0xb0, 0xce, 0x2f, 0x4a, 0x87, 0xab,
        0x70, 0x29, 0x4a, 0x4c, 0x2b, 0x41, 0x28, 0xca, 0x2f, 0xc9, 0x48, 0x2d, 0x0a, 0x00, 0x2a,
        0x02, 0x00, 0xb2, 0x0c, 0x1a, 0xc9,
    ];

    #[test]
    fn test_deflate() {
        let example = b"com.example.MyInterface/some/path;org.example.DraftInterface/otherPath";

        let s = extract_set_properties(&PROPERTIES_PAYLOAD).unwrap();

        assert_eq!(s.join(";").as_bytes(), example);
    }
}
