// This file is part of Astarte.
//
// Copyright 2021 - 2025 SECO Mind Srl
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

//! Provides the functionalities to pair a device with the Astarte Cluster.

use std::{io, path::PathBuf};

use reqwest::{
    header::{HeaderMap, HeaderValue},
    StatusCode, Url,
};
use serde::{Deserialize, Serialize};
use url::ParseError;

use super::{config::transport::TransportProvider, crypto::CryptoError};

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
    /// Invalid credential secret
    #[error("couldn't set bearer header, invalid credential secret")]
    Header(#[from] reqwest::header::InvalidHeaderValue),
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
    ReadNativeCerts(#[source] tokio::task::JoinError),
}

/// Struct with the information for the pairing
pub(crate) struct ApiClient<'a> {
    pub(crate) realm: &'a str,
    pub(crate) device_id: &'a str,
    pairing_url: &'a Url,
    client: reqwest::Client,
}

impl<'a> ApiClient<'a> {
    pub(crate) fn from_transport(
        provider: &'a TransportProvider,
        realm: &'a str,
        device_id: &'a str,
    ) -> Result<Self, PairingError> {
        let tls_config = provider.api_tls_config()?;

        let mut headers = HeaderMap::new();
        let auth = format!("Bearer {}", provider.credential_secret());
        let mut value = HeaderValue::from_str(&auth)?;
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);

        let client = reqwest::Client::builder()
            .use_preconfigured_tls(tls_config.clone())
            .default_headers(headers)
            .build()?;

        Ok(Self {
            realm,
            device_id,
            pairing_url: provider.pairing_url(),
            client,
        })
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

        let response = self.client.post(url).json(&payload).send().await?;

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

        let response = self.client.get(url).send().await?;

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

#[cfg(test)]
pub(crate) mod tests {
    use crate::transport::mqtt::crypto::Bundle;

    use super::*;

    use mockito::Server;
    use pretty_assertions::assert_eq;

    const ASTARTE_CA_PEM: &str = include_str!("../../../tests/ca/astarte-ca.pem");
    const ASTARTE_CA_KEY_PEM: &str = include_str!("../../../tests/ca/astarte-ca-key.pem");

    pub(crate) fn mock_get_broker_url(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock(
                "GET",
                "/v1/realm/devices/device_id",
            )
            .with_status(200)
            .with_body(
                r#"{"data":{"protocols":{"astarte_mqtt_v1":{"__unknown_fields__":[],"broker_url":"mqtts://broker.astarte.localhost:8883/"}},"status":"pending","version":"1.1.1"}}"#,
            )
            .match_header("authorization", "Bearer secret")
    }

    // Returns the self signed certificate from a CSR
    fn self_sign_csr_to_pem(csr_pem: &str) -> String {
        // NOTE: read the doc for this function if crypto backend changes
        let issuer_key = rcgen::KeyPair::from_pem_and_sign_algo(
            ASTARTE_CA_KEY_PEM,
            &rcgen::PKCS_ECDSA_P256_SHA256,
        )
        .expect("couldn't parse Astarte CA private key");
        let issuer = rcgen::CertificateParams::from_ca_cert_pem(ASTARTE_CA_PEM)
            .expect("couldn't parse Astarte CA")
            .self_signed(&issuer_key)
            .expect("couldn't self sign CA");

        rcgen::CertificateSigningRequestParams::from_pem(csr_pem)
            .expect("couldn't parse csr")
            .signed_by(&issuer, &issuer_key)
            .unwrap()
            .pem()
    }

    // owned version of the req
    #[derive(Debug, Deserialize)]
    struct MqttV1CsrOwned {
        csr: String,
    }

    pub(crate) fn mock_create_certificate(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock(
                "POST",
                "/v1/realm/devices/device_id/protocols/astarte_mqtt_v1/credentials",
            )
            .with_status(201)
            .with_body_from_request(|req| {
                let body = req.body().expect("couln't read req body");
                let ApiData {
                    data: MqttV1CsrOwned { csr },
                } = serde_json::from_slice(body).expect("couldn't parse json request");

                let client_crt = self_sign_csr_to_pem(&csr);

                serde_json::to_vec(&ApiData::new(MqttV1Certificate { client_crt }))
                    .expect("couldn't serialize response")
            })
            .match_header("authorization", "Bearer secret")
    }

    #[tokio::test]
    async fn should_create_certificate() {
        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server).create_async().await;

        let parse = Url::parse(&server.url()).unwrap();

        let provider = TransportProvider::configure(parse, "secret".to_string(), None, true)
            .await
            .expect("couldn't configure provider");

        let client = ApiClient::from_transport(&provider, "realm", "device_id")
            .expect("couldn't create api client");

        let bundle = Bundle::generate_key("test", "device_id").unwrap();

        let res = client.create_certificate(&bundle.csr).await.unwrap();

        rustls_pemfile::certs(&mut res.as_bytes())
            .next()
            .unwrap()
            .unwrap();

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn should_error_not_create_certificate() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock(
                "POST",
                "/v1/realm/devices/device_id/protocols/astarte_mqtt_v1/credentials",
            )
            .with_status(202)
            .with_body(r#"{"error":"error"}"#)
            .match_header("authorization", "Bearer secret")
            .match_body(r#"{"data":{"csr":"csr"}}"#)
            .create_async()
            .await;

        let url = Url::parse(&server.url()).unwrap();

        let provider = TransportProvider::configure(url, "secret".to_string(), None, true)
            .await
            .expect("couldn't configure provider");

        let client = ApiClient::from_transport(&provider, "realm", "device_id")
            .expect("couldn't create api client");

        let res = client
            .create_certificate("csr")
            .await
            .expect_err("error expected");

        assert!(matches!(res, PairingError::Api { status, body: _ } if status == 202));

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn should_get_broker_url() {
        let mut server = Server::new_async().await;

        let mock = mock_get_broker_url(&mut server).create_async().await;

        let url = Url::parse(&server.url()).unwrap();

        let provider = TransportProvider::configure(url, "secret".to_string(), None, true)
            .await
            .expect("couldn't configure provider");

        let client = ApiClient::from_transport(&provider, "realm", "device_id")
            .expect("couldn't create api client");

        let res = client.get_broker_url().await.unwrap();

        let expected = Url::parse("mqtts://broker.astarte.localhost:8883/").unwrap();

        assert_eq!(res, expected);

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn forbidden_get_broker_url() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/v1/realm/devices/device_id")
            .with_status(401)
            .match_header("authorization", "Bearer secret")
            .create_async()
            .await;

        let url = Url::parse(&server.url()).unwrap();

        let provider = TransportProvider::configure(url, "secret".to_string(), None, true)
            .await
            .expect("couldn't configure provider");

        let client = ApiClient::from_transport(&provider, "realm", "device_id")
            .expect("couldn't create api client");

        let res = client.get_broker_url().await.expect_err("should error");

        assert!(matches!(res, PairingError::Api { status, body: _ } if status == 401));

        mock.assert_async().await;
    }
}
