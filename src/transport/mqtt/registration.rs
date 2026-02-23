// This file is part of Astarte.
//
// Copyright 2021, 2026 SECO Mind Srl
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

//! Provides static functions for registering a new device to an Astarte Cluster.

use base64::Engine;
use reqwest::Url;
use uuid::Uuid;

use crate::{builder::Config, transport::mqtt::PairingError};

use super::pairing::{ApiClient, ClientArgs};

/// Arguments for the register device call
pub struct RegisterDevice<'a> {
    /// TLS client configuration
    pub tls: rustls::ClientConfig,
    /// Pairing url for astarte.
    ///
    /// For example <http://api.astarte.localhost/pairing>
    pub pairing_url: &'a Url,
    /// Pairing token.
    ///
    /// A JWT with the claim to register a device.
    pub token: &'a str,
    /// Realm to register the device to
    pub realm: &'a str,
    /// Device ID to register.
    pub device_id: &'a str,
}

impl<'a> From<&RegisterDevice<'a>> for ClientArgs<'a> {
    fn from(value: &RegisterDevice<'a>) -> Self {
        let RegisterDevice {
            pairing_url,
            token,
            realm,
            device_id,
            ..
        } = value;

        ClientArgs {
            realm,
            device_id,
            pairing_url,
            token,
        }
    }
}

/// Obtain a credentials secret from the astarte API
pub async fn register_device<'a>(args: RegisterDevice<'a>) -> Result<String, PairingError> {
    let config = Config::default();

    ApiClient::create(ClientArgs::from(&args), &config, args.tls)?
        .register_device()
        .await
}

/// Generate a random device Id with UUIDv4.
pub fn generate_random_uuid() -> String {
    let uuid = Uuid::new_v4();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(uuid.as_bytes())
}

/// Generate a device Id based on UUID namespace identifier and a uniqueData.
pub fn generate_uuid(namespace: uuid::Uuid, unique_data: &str) -> String {
    let uuid = Uuid::new_v5(&namespace, unique_data.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(uuid.as_bytes())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_uuid() {
        let uuid = uuid::Uuid::parse_str("f79ad91f-c638-4889-ae74-9d001a3b4cf8").unwrap();
        let expected_device_id = "AJInS0w3VpWpuOqkXhgZdA";

        let device_id = generate_uuid(uuid, "myidentifierdata");

        assert_eq!(device_id, expected_device_id);
    }

    #[test]
    fn test_uuid_2() {
        let uuid = uuid::Uuid::parse_str("b068931c-c450-342b-a3f5-b3d276ea4297").unwrap();
        let expected_device_id = "dvt9mLDaWb2vW7bdBJwKCg";

        let device_id = generate_uuid(uuid, "0099112233");

        assert_eq!(device_id, expected_device_id);
    }

    #[test]
    fn test_random_uuid() {
        let device_id = generate_random_uuid();

        assert!(!device_id.is_empty());
    }
}
