use http::StatusCode;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;

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
