use crate::{crypto::Bundle, Device};
use http::StatusCode;
use openssl::error::ErrorStack;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::ParseError;

#[derive(Serialize, Deserialize, Debug)]
struct AstarteMQTTV1Credentials {
    client_crt: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct CredentialsResponse {
    data: AstarteMQTTV1Credentials,
}

#[derive(thiserror::Error, Debug)]
pub enum PairingError {
    #[error("invalid credentials secret")]
    InvalidCredentials,
    #[error("invalid pairing URL")]
    InvalidUrl(#[from] ParseError),
    #[error("error while sending or receiving request")]
    RequestError(#[from] reqwest::Error),
    #[error("API returned an error code")]
    ApiError(StatusCode, String),
    #[error("crypto error")]
    Crypto(#[from] ErrorStack),
}

pub async fn fetch_credentials(device: &Device) -> Result<String, PairingError> {
    let Device {
        realm,
        device_id,
        credentials_secret,
        pairing_url,
        crypto: Bundle(_, csr),
        ..
    } = device;

    let csr_bytes = csr.to_pem()?;
    let csr_pem = String::from_utf8(csr_bytes).unwrap();

    let mut url = Url::parse(&pairing_url)?;
    // We have to do this this way to avoid unconsistent behaviour depending
    // on the user putting the trailing slash or not
    url.path_segments_mut()
        .map_err(|_| ParseError::RelativeUrlWithCannotBeABaseBase)?
        .push("v1")
        .push(&realm)
        .push("devices")
        .push(&device_id)
        .push("protocols")
        .push("astarte_mqtt_v1")
        .push("credentials");

    let payload = json!({
        "data": {
            "csr": csr_pem,
        }
    });

    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .bearer_auth(&credentials_secret)
        .json(&payload)
        .send()
        .await?;

    match response.status() {
        StatusCode::CREATED => {
            let certificate = response
                .json::<CredentialsResponse>()
                .await?
                .data
                .client_crt;
            Ok(String::from(certificate))
        }

        status_code => {
            let raw_response = response.text().await?;
            Err(PairingError::ApiError(status_code, raw_response))
        }
    }
}
