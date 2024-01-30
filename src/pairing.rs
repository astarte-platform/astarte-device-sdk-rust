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

use std::sync::Arc;

use http::StatusCode;
use openssl::error::ErrorStack;
use reqwest::Url;
use rumqttc::MqttOptions;
use rustls::{Certificate, PrivateKey};
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::ParseError;

use crate::{
    crypto::Bundle,
    options::{AstarteOptions, AstarteOptionsError},
};

#[derive(Serialize, Deserialize, Debug)]
struct ApiResponse {
    data: ResponseContents,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum ResponseContents {
    AstarteMqttV1Credentials {
        client_crt: String,
    },
    StatusInfo {
        version: String,
        status: String,
        protocols: ProtocolsInfo,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct ProtocolsInfo {
    astarte_mqtt_v1: AstarteMqttV1Info,
}

#[derive(Serialize, Deserialize, Debug)]
struct AstarteMqttV1Info {
    broker_url: String,
}

#[derive(thiserror::Error, Debug)]
pub enum PairingError {
    #[error("invalid credentials secret")]
    InvalidCredentials,
    #[error("invalid pairing URL")]
    InvalidUrl(#[from] ParseError),
    #[error("error while sending or receiving request")]
    RequestError(#[from] reqwest::Error),
    #[error("API response can't be deserialized")]
    UnexpectedResponse,
    #[error("API returned an error code")]
    ApiError(StatusCode, String),
    #[error("crypto error")]
    Crypto(#[from] ErrorStack),
    #[error("configuration error")]
    ConfigError(String),
}

pub async fn fetch_credentials(opts: &AstarteOptions, csr: &str) -> Result<String, PairingError> {
    let mut url = Url::parse(&opts.pairing_url)?;
    // We have to do this this way to avoid inconsistent behaviour depending
    // on the user putting the trailing slash or not
    url.path_segments_mut()
        .map_err(|_| ParseError::RelativeUrlWithCannotBeABaseBase)?
        .push("v1")
        .push(&opts.realm)
        .push("devices")
        .push(&opts.device_id)
        .push("protocols")
        .push("astarte_mqtt_v1")
        .push("credentials");

    let payload = json!({
        "data": {
            "csr": csr,
        }
    });

    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .bearer_auth(&opts.credentials_secret)
        .json(&payload)
        .send()
        .await?;

    match response.status() {
        StatusCode::CREATED => {
            if let ResponseContents::AstarteMqttV1Credentials { client_crt } =
                response.json::<ApiResponse>().await?.data
            {
                Ok(client_crt)
            } else {
                Err(PairingError::UnexpectedResponse)
            }
        }

        status_code => {
            let raw_response = response.text().await?;
            Err(PairingError::ApiError(status_code, raw_response))
        }
    }
}

pub async fn fetch_broker_url(opts: &AstarteOptions) -> Result<String, PairingError> {
    let mut url = Url::parse(&opts.pairing_url)?;
    // We have to do this this way to avoid inconsistent behaviour depending
    // on the user putting the trailing slash or not
    url.path_segments_mut()
        .map_err(|_| ParseError::RelativeUrlWithCannotBeABaseBase)?
        .push("v1")
        .push(&opts.realm)
        .push("devices")
        .push(&opts.device_id);

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .bearer_auth(&opts.credentials_secret)
        .send()
        .await?;

    match response.status() {
        StatusCode::OK => {
            if let ResponseContents::StatusInfo {
                protocols:
                    ProtocolsInfo {
                        astarte_mqtt_v1: AstarteMqttV1Info { broker_url },
                    },
                ..
            } = response.json::<ApiResponse>().await?.data
            {
                Ok(broker_url)
            } else {
                Err(PairingError::UnexpectedResponse)
            }
        }

        status_code => {
            let raw_response = response.text().await?;
            Err(PairingError::ApiError(status_code, raw_response))
        }
    }
}

async fn populate_credentials(
    opts: &AstarteOptions,
) -> Result<(Vec<Certificate>, PrivateKey), PairingError> {
    let cn = format!("{}/{}", opts.realm, opts.device_id);

    let Bundle(pkey_bytes, csr_bytes) = Bundle::new(&cn)?;

    let private_key = rustls_pemfile::pkcs8_private_keys(&mut pkey_bytes.as_slice())
        .map_err(|_| PairingError::ConfigError("failed pkcs8 key extraction".into()))?
        .remove(0);

    let csr = String::from_utf8(csr_bytes)
        .map_err(|_| PairingError::ConfigError("bad csr bytes format".into()))?;

    let cert_pem = fetch_credentials(opts, &csr).await?;
    let mut cert_pem_bytes = cert_pem.as_bytes();
    let certs = rustls_pemfile::certs(&mut cert_pem_bytes)
        .map_err(|_| PairingError::InvalidCredentials)?
        .iter()
        .map(|c| Certificate(c.clone()))
        .collect();
    Ok((certs, PrivateKey(private_key)))
}

async fn populate_broker_url(opts: &AstarteOptions) -> Result<Url, PairingError> {
    let broker_url = fetch_broker_url(opts).await?;
    let parsed_broker_url = Url::parse(&broker_url)?;
    Ok(parsed_broker_url)
}

fn build_mqtt_opts(
    options: &AstarteOptions,
    certificate_pem: &[Certificate],
    private_key: &PrivateKey,
    broker_url: &Url,
) -> Result<MqttOptions, AstarteOptionsError> {
    let AstarteOptions {
        realm, device_id, ..
    } = options;

    let client_id = format!("{realm}/{device_id}");
    let host = broker_url
        .host_str()
        .ok_or_else(|| AstarteOptionsError::ConfigError("bad broker url".into()))?;
    let port = broker_url
        .port()
        .ok_or_else(|| AstarteOptionsError::ConfigError("bad broker url".into()))?;

    let mut root_cert_store = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs") {
        root_cert_store.add(&rustls::Certificate(cert.0))?;
    }

    let mut tls_client_config = rumqttc::tokio_rustls::rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store)
        .with_single_cert(certificate_pem.to_owned(), private_key.to_owned())
        .map_err(|_| AstarteOptionsError::ConfigError("cannot setup client auth".into()))?;

    let mut mqtt_opts = MqttOptions::new(client_id, host, port);

    let keep_alive = options.keepalive.as_secs();
    let conn_timeout = options.conn_timeout.as_secs();
    if keep_alive <= conn_timeout {
        return Err(AstarteOptionsError::ConfigError(format!(
            "Keep alive ({keep_alive}s) should be greater than the connection timeout ({conn_timeout}s)"
        )));
    }

    mqtt_opts
        .set_keep_alive(options.keepalive)
        .set_connection_timeout(conn_timeout);

    if options.ignore_ssl_errors || std::env::var("IGNORE_SSL_ERRORS") == Ok("true".to_string()) {
        struct OkVerifier {}
        impl rustls::client::ServerCertVerifier for OkVerifier {
            fn verify_server_cert(
                &self,
                _: &Certificate,
                _: &[Certificate],
                _: &rustls::ServerName,
                _: &mut dyn Iterator<Item = &[u8]>,
                _: &[u8],
                _: std::time::SystemTime,
            ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::ServerCertVerified::assertion())
            }
        }

        let mut clientconfig = tls_client_config.dangerous();
        clientconfig.set_certificate_verifier(Arc::new(OkVerifier {}));

        let tls_config = rumqttc::TlsConfiguration::Rustls(Arc::new(clientconfig.cfg.to_owned()));
        let transport = rumqttc::Transport::tls_with_config(tls_config);

        mqtt_opts.set_transport(transport);
    } else {
        mqtt_opts.set_transport(rumqttc::Transport::tls_with_config(
            tls_client_config.into(),
        ));
    }

    Ok(mqtt_opts)
}

pub async fn get_transport_config(
    opts: &AstarteOptions,
) -> Result<MqttOptions, AstarteOptionsError> {
    let (certificate_pem, private_key) = populate_credentials(opts).await?;

    let broker_url = populate_broker_url(opts).await?;

    let mqtt_opts = build_mqtt_opts(opts, &certificate_pem, &private_key, &broker_url)?;

    Ok(mqtt_opts)
}
