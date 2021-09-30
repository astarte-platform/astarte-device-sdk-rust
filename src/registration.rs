use http::StatusCode;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::pairing::PairingError;

#[derive(Serialize, Deserialize, Debug)]
struct RegisterApiResponse {
    data: RegisterData,
}
#[derive(Serialize, Deserialize, Debug)]
struct RegisterData {
    credentials_secret: String,
}

pub async fn register_device(
    token: &str,
    pairing_url: &str,
    realm: &str,
    device_id: &str,
) -> Result<String, PairingError> {
    let mut url = Url::parse(pairing_url)?;

    url.path_segments_mut()
        .map_err(|_| url::ParseError::RelativeUrlWithCannotBeABaseBase)?
        .push("v1")
        .push(realm)
        .push("agent")
        .push("devices");

    let payload = json!({
        "data": {
            "hw_id": device_id,
        }
    });

    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .bearer_auth(&token)
        .json(&payload)
        .send()
        .await?;

    match response.status() {
        StatusCode::CREATED => {
            let RegisterData { credentials_secret } =
                response.json::<RegisterApiResponse>().await?.data;

            Ok(credentials_secret)
        }

        status_code => {
            let raw_response = response.text().await?;
            Err(PairingError::ApiError(status_code, raw_response))
        }
    }
}

/// Generate a device Id in a random way based on UUIDv4.
pub fn generate_random_uuid() -> String {
    let uuid = Uuid::new_v4();
    base64::encode_config(uuid.as_bytes(), base64::URL_SAFE_NO_PAD)
}

/// Generate a device Id based on UUID namespace identifier and a uniqueData.
pub fn generate_uuid(namespace: uuid::Uuid, unique_data: &str) -> String {
    let uuid = Uuid::new_v5(&namespace, unique_data.as_bytes());
    base64::encode_config(uuid.as_bytes(), base64::URL_SAFE_NO_PAD)
}

#[cfg(test)]

mod test {
    use crate::registration::generate_random_uuid;

    use super::generate_uuid;

    #[test]
    fn test_uuid() {
        let uuid = uuid::Uuid::parse_str("f79ad91f-c638-4889-ae74-9d001a3b4cf8").unwrap();
        let expected_device_id = "AJInS0w3VpWpuOqkXhgZdA";

        let deviceid = generate_uuid(uuid, "myidentifierdata");

        assert_eq!(deviceid, expected_device_id);
    }

    #[test]
    fn test_uuid_2() {
        let uuid = uuid::Uuid::parse_str("b068931c-c450-342b-a3f5-b3d276ea4297").unwrap();
        let expected_device_id = "dvt9mLDaWb2vW7bdBJwKCg";

        let deviceid = generate_uuid(uuid, "0099112233");

        assert_eq!(deviceid, expected_device_id);
    }

    #[test]
    fn test_random_uuid() {
        let deviceid = generate_random_uuid();

        assert!(!deviceid.is_empty());
    }
}
