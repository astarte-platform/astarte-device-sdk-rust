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

//! Handshake with Astarte for the encrypted endpoints.

use std::fmt::Display;

mod derive;
pub(crate) mod messages;
pub(crate) mod state;

/// Encryption error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncError {
    /// Couldn't init the handshake
    Init,
    /// Couldn't derive the secret
    Secret,
    /// Couldn't encrypt the message
    Encrypt,
    /// Couldn't decrypt the message
    Decrypt,
    /// Couldn't encode the message
    Encode,
    /// Couldn't encode the message
    Decode,
}

impl Display for EncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncError::Init => write!(f, "couldn't init the handshake"),
            EncError::Secret => write!(f, "couldn't derive the secret"),
            EncError::Encrypt => write!(f, "couldn't encrypt the message"),
            EncError::Decrypt => write!(f, "couldn't decrypt the message"),
            EncError::Encode => write!(f, "couldn't encode the message"),
            EncError::Decode => write!(f, "couldn't decode the message"),
        }
    }
}
