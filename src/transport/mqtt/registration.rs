/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
//! Provides static functions for registering a new device to an Astarte Cluster.

use std::time::Duration;

use base64::Engine;
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::transport::mqtt::PairingError;

use super::pairing::ApiData;

#[derive(Debug, Serialize)]
struct MqttV1HwId<'a> {
    hw_id: &'a str,
}

#[derive(Debug, Deserialize)]
struct MqttV1Credential {
    credentials_secret: String,
}

/// Obtain a credentials secret from the astarte API
pub async fn register_device_timeout(
    token: &str,
    pairing_url: &str,
    realm: &str,
    device_id: &str,
    timeout: Duration,
) -> Result<String, PairingError> {
    let mut url = Url::parse(pairing_url)?;

    url.path_segments_mut()
        .map_err(|_| url::ParseError::RelativeUrlWithCannotBeABaseBase)?
        .push("v1")
        .push(realm)
        .push("agent")
        .push("devices");

    let payload = ApiData::new(MqttV1HwId { hw_id: device_id });

    let client = reqwest::Client::builder().timeout(timeout).build()?;
    let response = client
        .post(url)
        .bearer_auth(token)
        .json(&payload)
        .send()
        .await?;

    match response.status() {
        StatusCode::CREATED => {
            let res: ApiData<MqttV1Credential> = response.json().await?;

            Ok(res.data.credentials_secret)
        }
        status_code => {
            let raw_response = response.text().await?;
            Err(PairingError::Api {
                status: status_code,
                body: raw_response,
            })
        }
    }
}

/// Obtain a credentials secret from the astarte API with a default timeout of 10 seconds
pub async fn register_device(
    token: &str,
    pairing_url: &str,
    realm: &str,
    device_id: &str,
) -> Result<String, PairingError> {
    register_device_timeout(
        token,
        pairing_url,
        realm,
        device_id,
        // TODO check if it does make sense to add a random default timeout?
        Duration::from_secs(10),
    )
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
