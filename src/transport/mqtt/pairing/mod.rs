// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Module to pair a new device to the transport

use std::fmt::Display;

use crate::transport::mqtt::crypto::CryptoError;

pub(crate) mod client;
pub(crate) mod mk_connection;
pub mod registration;

/// Error returned during pairing.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairingApiError {
    /// Couldn't pair for invalid argument
    InvalidArgument,
    /// The pairing request failed.
    Request,
    /// The API returned an error.
    Api,
    /// Couldn't configure the TLS store
    Tls,
    /// Couldn't join task
    Join,
    /// Couldn't read the or write the credentials
    Io(std::io::ErrorKind),
    /// Crypto operation failed
    Crypto(CryptoError),
}

impl Display for PairingApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PairingApiError::InvalidArgument => write!(f, "invalid argument"),
            PairingApiError::Request => write!(f, "couldn't send the request"),
            PairingApiError::Tls => write!(f, "couldn't configure TLS"),
            PairingApiError::Api => write!(f, "the api responded with an error"),
            PairingApiError::Join => write!(f, "couldn't join task"),
            PairingApiError::Io(error) => write!(f, "io error {error}"),
            PairingApiError::Crypto(error) => write!(f, "crypto error {error}"),
        }
    }
}
