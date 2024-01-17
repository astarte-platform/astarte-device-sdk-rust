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

//! Provides the functionalities to pair a device with the Astarte Cluster.

use std::sync::Arc;

use reqwest::{StatusCode, Url};
use rumqttc::MqttOptions;
use rustls::{Certificate, PrivateKey};
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::ParseError;

use super::{
    crypto::{Bundle, CryptoError},
    error::MqttError,
    MqttConfig,
};

/// Api response from astarte
#[derive(Debug, Serialize, Deserialize)]
struct ApiResponse<C> {
    data: C,
}

/// Response to the pairing request
#[derive(Debug, Serialize, Deserialize)]
struct MqttV1Credentials {
    client_crt: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StatusInfo {
    version: String,
    status: String,
    protocols: ProtocolsInfo,
}

#[derive(Serialize, Deserialize, Debug)]
struct ProtocolsInfo {
    astarte_mqtt_v1: AstarteMqttV1Info,
}

#[derive(Serialize, Deserialize, Debug)]
struct AstarteMqttV1Info {
    broker_url: String,
}

/// Error returned during pairing.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum PairingError {
    #[error("invalid credentials secret")]
    InvalidCredentials(#[source] std::io::Error),
    #[error("invalid pairing URL")]
    InvalidUrl(#[from] ParseError),
    #[error("error while sending or receiving request")]
    Request(#[from] reqwest::Error),
    #[error("API returned an error code {status}")]
    Api { status: StatusCode, body: String },
    #[error("crypto error")]
    Crypto(#[from] CryptoError),
    /// Couldn't configure the TLS store
    #[error("failed to configure TLS")]
    Tls(#[from] rustls::Error),
    /// Couldn't add certificate to the TLS store
    #[error("failed to add certificate")]
    Pki(#[from] webpki::Error),
    /// Couldn't load native certs
    #[error("couldn't load native certificates")]
    Native(#[source] std::io::Error),
    #[error("configuration error, {0}")]
    Config(&'static str),
}

async fn fetch_credentials(opts: &MqttConfig, csr: &str) -> Result<String, PairingError> {
    let mut url = Url::parse(&opts.pairing_url)?;
    // We have to do this this way to avoid inconsistent behaviour depending
    // on the user putting the trailing slash or not
    url.path_segments_mut()
        .map_err(|()| ParseError::RelativeUrlWithCannotBeABaseBase)?
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
            let res: ApiResponse<MqttV1Credentials> = response.json().await?;

            Ok(res.data.client_crt)
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

async fn fetch_broker_url(opts: &MqttConfig) -> Result<String, PairingError> {
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
            let res: ApiResponse<StatusInfo> = response.json().await?;

            Ok(res.data.protocols.astarte_mqtt_v1.broker_url)
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

async fn populate_credentials(
    opts: &MqttConfig,
) -> Result<(Vec<Certificate>, PrivateKey), PairingError> {
    let Bundle { private_key, csr } = Bundle::new(&opts.realm, &opts.device_id)?;

    let certificate = fetch_credentials(opts, &csr).await?;
    let certs = rustls_pemfile::certs(&mut certificate.as_bytes())
        .map_err(PairingError::InvalidCredentials)?
        .into_iter()
        .map(Certificate)
        .collect();

    Ok((certs, private_key))
}

async fn populate_broker_url(opts: &MqttConfig) -> Result<Url, PairingError> {
    let broker_url = fetch_broker_url(opts).await?;
    let parsed_broker_url = Url::parse(&broker_url)?;
    Ok(parsed_broker_url)
}

fn build_mqtt_opts(
    opts: &MqttConfig,
    certificate: Vec<Certificate>,
    private_key: PrivateKey,
    broker_url: &Url,
) -> Result<MqttOptions, PairingError> {
    let MqttConfig {
        realm, device_id, ..
    } = opts;

    let client_id = format!("{realm}/{device_id}");
    let host = broker_url
        .host_str()
        .ok_or_else(|| PairingError::Config("missing host in url"))?;
    let port = broker_url
        .port()
        .ok_or_else(|| PairingError::Config("missing port in url"))?;

    let mut root_cert_store = rustls::RootCertStore::empty();
    let native_certs = rustls_native_certs::load_native_certs().map_err(PairingError::Native)?;
    for cert in native_certs {
        root_cert_store.add(&rustls::Certificate(cert.0))?;
    }

    let mut tls_client_config = rumqttc::tokio_rustls::rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store)
        .with_single_cert(certificate, private_key)?;

    let mut mqtt_opts = MqttOptions::new(client_id, host, port);

    if opts.keepalive.as_secs() < 5 {
        return Err(PairingError::Config("Keepalive should be >= 5 secs"));
    }

    mqtt_opts.set_keep_alive(opts.keepalive);

    if opts.ignore_ssl_errors || is_env_ignore_ssl() {
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

fn is_env_ignore_ssl() -> bool {
    matches!(
        std::env::var("IGNORE_SSL_ERRORS").as_deref(),
        Ok("1" | "true")
    )
}

/// Returns a MqttOptions struct that can be used to connect to the broker.
pub(crate) async fn get_transport_config(opts: &MqttConfig) -> Result<MqttOptions, MqttError> {
    let (certificate, private_key) = populate_credentials(opts).await?;

    let broker_url = populate_broker_url(opts).await?;

    let mqtt_opts = build_mqtt_opts(opts, certificate, private_key, &broker_url)?;

    Ok(mqtt_opts)
}
