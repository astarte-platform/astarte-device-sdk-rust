use crate::{crypto::Bundle, Device};
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
    #[error("error during API request")]
    RequestError(#[from] reqwest::Error),
    #[error("crypto error")]
    Crypto(#[from] ErrorStack),
}

pub fn fetch_credentials(device: &Device) -> Result<String, PairingError> {
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

    let client = reqwest::blocking::Client::new();
    let response: CredentialsResponse = client
        .post(url)
        .bearer_auth(&credentials_secret)
        .json(&payload)
        .send()?
        .json()?;

    Ok(String::from(response.data.client_crt))
}
