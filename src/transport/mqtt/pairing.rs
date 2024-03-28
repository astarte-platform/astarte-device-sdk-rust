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

use itertools::Itertools;
use reqwest::{StatusCode, Url};
use rumqttc::{MqttOptions, NetworkOptions};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::ParseError;

use super::{
    crypto::{Bundle, CryptoError},
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
    /// Couldn't load native certs
    #[error("couldn't load native certificates")]
    Native(#[source] std::io::Error),
    #[error("configuration error, {0}")]
    Config(String),
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
) -> Result<(Vec<CertificateDer<'static>>, PrivatePkcs8KeyDer<'static>), PairingError> {
    let Bundle { private_key, csr } = Bundle::new(&opts.realm, &opts.device_id)?;

    let certificate = fetch_credentials(opts, &csr).await?;
    let certs = rustls_pemfile::certs(&mut certificate.as_bytes())
        .try_collect()
        .map_err(PairingError::InvalidCredentials)?;

    Ok((certs, private_key))
}

async fn populate_broker_url(opts: &MqttConfig) -> Result<Url, PairingError> {
    let broker_url = fetch_broker_url(opts).await?;
    let parsed_broker_url = Url::parse(&broker_url)?;
    Ok(parsed_broker_url)
}

fn build_mqtt_opts(
    opts: &MqttConfig,
    certificate: Vec<CertificateDer<'static>>,
    private_key: PrivatePkcs8KeyDer<'static>,
    broker_url: &Url,
) -> Result<(MqttOptions, NetworkOptions), PairingError> {
    let MqttConfig {
        realm, device_id, ..
    } = opts;

    let client_id = format!("{realm}/{device_id}");
    let host = broker_url
        .host_str()
        .ok_or_else(|| PairingError::Config("missing host in url".to_string()))?;
    let port = broker_url
        .port()
        .ok_or_else(|| PairingError::Config("missing port in url".to_string()))?;

    let mut mqtt_opts = MqttOptions::new(client_id, host, port);

    let keep_alive = opts.keepalive.as_secs();
    let conn_timeout = opts.conn_timeout.as_secs();
    if keep_alive <= conn_timeout {
        return Err(PairingError::Config(
            format!("Keep alive ({keep_alive}s) should be greater than the connection timeout ({conn_timeout}s)")
        ));
    }

    let mut net_opts = NetworkOptions::new();
    net_opts.set_connection_timeout(conn_timeout);

    mqtt_opts.set_keep_alive(opts.keepalive);

    if opts.ignore_ssl_errors || is_env_ignore_ssl() {
        #[derive(Debug)]
        struct NoVerifier;

        impl rustls::client::danger::ServerCertVerifier for NoVerifier {
            fn verify_server_cert(
                &self,
                _end_entity: &rustls::pki_types::CertificateDer<'_>,
                _intermediates: &[rustls::pki_types::CertificateDer<'_>],
                _server_name: &rustls::pki_types::ServerName<'_>,
                _ocsp_response: &[u8],
                _now: rustls::pki_types::UnixTime,
            ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
            {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
            {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
                vec![
                    rustls::SignatureScheme::RSA_PKCS1_SHA1,
                    rustls::SignatureScheme::ECDSA_SHA1_Legacy,
                    rustls::SignatureScheme::RSA_PKCS1_SHA256,
                    rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                    rustls::SignatureScheme::RSA_PKCS1_SHA384,
                    rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
                    rustls::SignatureScheme::RSA_PKCS1_SHA512,
                    rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
                    rustls::SignatureScheme::RSA_PSS_SHA256,
                    rustls::SignatureScheme::RSA_PSS_SHA384,
                    rustls::SignatureScheme::RSA_PSS_SHA512,
                    rustls::SignatureScheme::ED25519,
                    rustls::SignatureScheme::ED448,
                ]
            }
        }

        let clientconfig = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier {}))
            .with_client_auth_cert(certificate, private_key.into())
            .map_err(PairingError::Tls)?;

        let tls_config = rumqttc::TlsConfiguration::Rustls(Arc::new(clientconfig));
        let transport = rumqttc::Transport::tls_with_config(tls_config);

        mqtt_opts.set_transport(transport);
    } else {
        let mut root_cert_store = rustls::RootCertStore::empty();
        let native_certs =
            rustls_native_certs::load_native_certs().map_err(PairingError::Native)?;
        for cert in native_certs {
            root_cert_store.add(cert)?;
        }

        let tls_client_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_client_auth_cert(certificate, private_key.into())?;

        mqtt_opts.set_transport(rumqttc::Transport::tls_with_config(
            tls_client_config.into(),
        ));
    }

    Ok((mqtt_opts, net_opts))
}

fn is_env_ignore_ssl() -> bool {
    matches!(
        std::env::var("IGNORE_SSL_ERRORS").as_deref(),
        Ok("1" | "true")
    )
}

/// Returns a MqttOptions struct that can be used to connect to the broker.
pub(crate) async fn get_transport_config(
    opts: &MqttConfig,
) -> Result<(MqttOptions, NetworkOptions), PairingError> {
    let (certificate, private_key) = populate_credentials(opts).await?;

    let broker_url = populate_broker_url(opts).await?;

    build_mqtt_opts(opts, certificate, private_key, &broker_url)
}
