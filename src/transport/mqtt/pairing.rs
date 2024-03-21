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

use std::{io, path::PathBuf};

use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use url::ParseError;

use super::{config::connection::TransportProvider, crypto::CryptoError};

/// Error returned during pairing.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum PairingError {
    /// Invalid credential secret.
    #[error("invalid credentials secret")]
    InvalidCredentials(#[source] io::Error),
    /// Couldn't parse the pairing URL.
    #[error("invalid pairing URL")]
    InvalidUrl(#[from] ParseError),
    /// The pairing request failed.
    #[error("error while sending or receiving request")]
    Request(#[from] reqwest::Error),
    /// The API returned an error.
    #[error("API returned an error code {status}")]
    Api {
        /// The status code of the response.
        status: StatusCode,
        /// The body of the response.
        body: String,
    },
    /// Failed to generate the CSR.
    #[error("crypto error")]
    Crypto(#[from] CryptoError),
    /// Couldn't configure the TLS store
    #[error("failed to configure TLS")]
    Tls(#[from] rustls::Error),
    /// Couldn't load native certs
    #[error("couldn't load native certificates")]
    Native(#[source] io::Error),
    /// Invalid configuration.
    #[error("configuration error, {0}")]
    Config(String),
    /// Couldn't read the credentials secret from the file
    #[error("couldn't read credential secret from {path}")]
    ReadCredential {
        /// The path where the credential is stored.
        path: PathBuf,
        /// The reason why we couldn't read the file.
        #[source]
        backtrace: io::Error,
    },
    /// Couldn't read the certificate from the file
    #[error("couldn't read certificate from {path}")]
    ReadCertificate {
        /// The path where the certificate is stored.
        path: PathBuf,
        /// The reason why we couldn't read the file.
        #[source]
        backtrace: io::Error,
    },
    /// Couldn't write the credentials secret to the file
    #[error("couldn't write credential secret to {path}")]
    WriteCredential {
        /// The path where the credential is stored.
        path: std::path::PathBuf,
        /// The reason why we couldn't write the file.
        #[source]
        backtrace: io::Error,
    },
    /// Couldn't read native certificates
    #[error("couldn't read native certificates")]
    ReadNativeCerts(#[from] tokio::task::JoinError),
}

/// Struct with the information for the pairing
pub(crate) struct ApiClient<'a> {
    pub(crate) realm: &'a str,
    pub(crate) device_id: &'a str,
    pairing_url: &'a Url,
    credentials_secret: &'a str,
}

impl<'a> ApiClient<'a> {
    pub(crate) fn from_transport(
        provider: &'a TransportProvider,
        realm: &'a str,
        device_id: &'a str,
    ) -> Self {
        Self {
            realm,
            device_id,
            pairing_url: provider.pairing_url(),
            credentials_secret: provider.credential_secret(),
        }
    }

    fn url<'i, I>(&self, segments: I) -> Result<Url, PairingError>
    where
        'a: 'i,
        I: IntoIterator<Item = &'i str>,
    {
        let mut url = self.pairing_url.clone();

        {
            // We have to do this this way to avoid inconsistent behaviour depending
            // on the user putting the trailing slash or not
            let mut path = url
                .path_segments_mut()
                .map_err(|()| ParseError::RelativeUrlWithCannotBeABaseBase)?;

            let iter = ["v1", self.realm, "devices", self.device_id]
                .into_iter()
                .chain(segments);

            path.extend(iter);
        }

        Ok(url)
    }

    pub async fn create_certificate(&self, csr: &str) -> Result<String, PairingError> {
        let url = self.url(["protocols", "astarte_mqtt_v1", "credentials"])?;

        let payload = ApiData::new(MqttV1Csr { csr });

        let client = reqwest::Client::new();
        let response = client
            .post(url)
            .bearer_auth(self.credentials_secret)
            .json(&payload)
            .send()
            .await?;

        match response.status() {
            StatusCode::CREATED => {
                let res: ApiData<MqttV1Certificate> = response.json().await?;

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

    pub async fn get_broker_url(&self) -> Result<Url, PairingError> {
        let url = self.url([])?;

        let client = reqwest::Client::new();
        let response = client
            .get(url)
            .bearer_auth(self.credentials_secret)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => {
                let res: ApiData<StatusInfo> = response.json().await?;

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
}

/// Api request or response for the Astarte API
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ApiData<C> {
    pub(crate) data: C,
}

impl<C> ApiData<C> {
    pub(crate) fn new(data: C) -> Self {
        Self { data }
    }
}

/// CSR request for the MQTT certificate
#[derive(Debug, Serialize)]
struct MqttV1Csr<'a> {
    csr: &'a str,
}

/// Response to the pairing request.
#[derive(Debug, Serialize, Deserialize)]
struct MqttV1Certificate<S = String> {
    client_crt: S,
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
    broker_url: Url,
}
