// This file is part of Astarte.
//
// Copyright 2024, 2026 SECO Mind Srl
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

//! Errors returned by the MQTT connection

use std::fmt::Display;

use crate::properties::PurgePropError;

use super::{PairingApiError, PayloadError};

/// Errors raised during construction of the [`Mqtt`](super::Mqtt) struct
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttError {
    /// Connection error
    Connection,
    /// Error while pairing with Astarte
    PairingApi(PairingApiError),
    /// Couldn't parse purge property payload
    PurgeProp(PurgePropError),
    /// Errors that can occur handling the payload.
    Payload(PayloadError),
    /// Couldn't pair device.
    ///
    /// This is a general error when the device is paired via the old API or FDO.
    DevicePairing,
    /// Failed to subscribe to topic
    Subscribe,
    /// Failed to unsubscribe to topic
    Unsubscribe,
    /// Failed to publish on topic
    Publish,
    /// Couldn't parse the topic
    Topic,
    /// Couldn't send the disconnect
    Disconnect,
    /// Token error while waiting for ack
    PubAckToken,
    /// The client is currently disconnected
    NoClient,
    /// Timeout reached
    Timeout,
    /// Couldn't join task
    Task,
}

impl Display for MqttError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MqttError::Connection => write!(f, "connection error"),
            MqttError::PairingApi(error) => write!(f, "couldn't pair with Astarte {error}"),
            MqttError::Payload(error) => write!(f, "couldn't process payload {error}"),
            MqttError::PurgeProp(error) => write!(f, "couldn't purge properties {error}"),
            MqttError::DevicePairing => write!(f, "couldn't pair the device"),
            MqttError::Subscribe => write!(f, "couldn't subscribe to topic"),
            MqttError::Unsubscribe => write!(f, "couldn't unsubscribe to topic"),
            MqttError::Publish => write!(f, "couldn't publish on topic"),
            MqttError::Topic => write!(f, "couldn't parse the topic"),
            MqttError::Disconnect => write!(f, "disconnect error"),
            MqttError::PubAckToken => write!(f, "token error while waiting for ack"),
            MqttError::NoClient => write!(
                f,
                "no client, connection with the server was not established"
            ),
            MqttError::Timeout => write!(f, "the configured timeout was reached"),
            MqttError::Task => write!(f, "couldn't join task"),
        }
    }
}
