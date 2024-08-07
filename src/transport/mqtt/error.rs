// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
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

//! Errors returned by the MQTT connection

use rumqttc::ClientError;

use crate::store::error::StoreError;

use super::{topic::TopicError, PairingError, PayloadError};

/// Errors raised during construction of the [`Mqtt`](super::Mqtt) struct
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    /// Error while pairing with Astarte
    #[error("couldn't pair with Astarte")]
    Pairing(#[from] PairingError),
    /// Error while loading the property for the session data.
    #[error("Error while loading session data to perform the mqtt connection: {0}")]
    PropLoad(#[from] StoreError),
    /// Failed to subscribe to topic
    #[error["Couldn't subscribe to topic"]]
    Subscribe(#[source] ClientError),
    /// Failed to unsubscribe to topic
    #[error["Couldn't unsubscribe to topic"]]
    Unsubscribe(#[source] ClientError),
    /// Failed to publish on topic
    #[error("Couldn't publish on topic {ctx}")]
    Publish {
        /// The topic we tried to publish on.
        ctx: &'static str,
        /// Reason why the publish failed.
        #[source]
        backtrace: ClientError,
    },
    /// Errors that can occur handling the payload.
    #[error("couldn't process payload")]
    Payload(#[from] PayloadError),
    /// Couldn't parse the topic
    #[error("couldn't parse the topic")]
    Topic(#[from] TopicError),
    /// Couldn't authenticate with the pairing token, because we are missing a writable directory
    ///
    /// See the [`ParingToken`](super::Credential::ParingToken) for more information.
    #[error("missing writable directory to store credentials to use the pairing token")]
    NoStorePairintToken,
    /// Couldn't send the disconnect
    #[error("couldn't send the disconnect")]
    Disconnect(#[source] ClientError),
}

impl MqttError {
    pub(crate) const fn publish(ctx: &'static str, error: ClientError) -> Self {
        Self::Publish {
            ctx,
            backtrace: error,
        }
    }
}
