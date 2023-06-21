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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{types::AstarteType, AstarteError};

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
    pub fn to_vec(&self) -> Result<Vec<u8>, AstarteError> {
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
