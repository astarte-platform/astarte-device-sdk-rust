// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
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

//! Configures how to register the device to Astarte

use std::io;
use std::path::{Path, PathBuf};

use reqwest::StatusCode;
use tokio::fs;
use tracing::{debug, info};

use self::client::{ApiClient, ClientArgs};

use crate::transport::mqtt::components::ClientId;
use crate::transport::mqtt::connection::context::Ctx;
use crate::transport::mqtt::crypto::CryptoError;
use crate::transport::mqtt::{Credential, MqttConfig};

use super::{Pairing, PairingConfig};

pub(crate) mod client;
pub mod registration;

/// Error returned during pairing.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum PairingError {
    /// Couldn't authenticate with the pairing token, because we are missing a writable directory
    #[error("missing writable directory to store credentials to use the pairing token")]
    NoStorePairingToken,
    /// Invalid credential secret.
    #[error("invalid credentials secret")]
    InvalidCredentials(#[source] io::Error),
    /// Missing certificate credential.
    #[error("missing certificate credential")]
    MissingCredentials,
    /// Couldn't parse the pairing URL.
    #[error("invalid pairing URL")]
    InvalidUrl(#[from] url::ParseError),
    /// The pairing request failed.
    #[error("error while sending or receiving request")]
    Request(reqwest::Error),
    /// The pairing request failed with a timeout (missing connection).
    #[error("got a timeout or connection error")]
    RequestNoNetwork(reqwest::Error),
    /// Invalid credential secret
    #[error("couldn't set bearer header, invalid credential secret")]
    Header(#[from] reqwest::header::InvalidHeaderValue),
    /// The API returned an error.
    #[error("API returned an error code {status}")]
    Api {
        /// The status code of the response.
        status: StatusCode,
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

impl From<reqwest::Error> for PairingError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() || err.is_request() {
            PairingError::RequestNoNetwork(err)
        } else {
            PairingError::Request(err)
        }
    }
}

/// File where the credential secret is stored
pub const CREDENTIAL_FILE: &str = "credential";
/// File where the certificate is stored in PEM format
pub const CERTIFICATE_FILE: &str = "certificate.pem";
/// File where the private key is stored in PEM format
pub const PRIVATE_KEY_FILE: &str = "priv-key.der";

/// Uses the legacy pairing API to register the device.
///
/// To the API you either have to:
///
/// - Provide a credential secret
/// - Provide a pairing token and a store directory
#[derive(Debug)]
pub struct PairingApi {
    mqtt_config: MqttConfig,
}

impl PairingApi {
    pub(crate) fn new(mqtt_config: MqttConfig) -> Self {
        Self { mqtt_config }
    }

    /// Retrieves the credentials for the connection
    async fn credentials<S>(&mut self, ctx: &mut Ctx<'_, S>) -> Result<String, PairingError> {
        // We need to clone to not return something owning a mutable reference to self
        match &self.mqtt_config.credential {
            Credential::Secret { credentials_secret } => Ok(credentials_secret.clone()),
            Credential::ParingToken { pairing_token } => {
                debug!("pairing token provided, retrieving credentials secret");
                let Some(dir) = &ctx.state.config.writable_dir else {
                    return Err(PairingError::NoStorePairingToken);
                };

                let secret = self
                    .read_secret_or_register(ctx, dir, pairing_token)
                    .await?;

                Ok(secret)
            }
        }
    }

    /// Register the device and stores the credentials secret in the given directory
    async fn read_secret_or_register<S>(
        &self,
        ctx: &mut Ctx<'_, S>,
        store_dir: &Path,
        pairing_token: &str,
    ) -> Result<String, PairingError> {
        let credential_file = store_dir.join(CREDENTIAL_FILE);

        match fs::read_to_string(&credential_file).await {
            Ok(secret) => {
                info!("secret read from file");

                return Ok(secret);
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                info!("no credential file {}", credential_file.display())
            }
            Err(err) => {
                return Err(PairingError::ReadCredential {
                    path: credential_file,
                    backtrace: err,
                });
            }
        }

        let args = ClientArgs {
            realm: &self.mqtt_config.realm,
            device_id: &self.mqtt_config.device_id,
            pairing_url: &self.mqtt_config.pairing_url,
            token: pairing_token,
        };

        let client = ApiClient::from_transport(&ctx.state.config, ctx.provider, args)?;

        let secret = client.register_device().await?;

        // We can register the device multiple times with the same pairing token if the device
        // hasn't connected. If the call to write the file fails, we will just re-register the
        // device.
        fs::write(&credential_file, &secret).await.map_err(|err| {
            PairingError::WriteCredential {
                path: credential_file,
                backtrace: err,
            }
        })?;

        Ok(secret)
    }
}

impl Pairing for PairingApi {
    type Error = PairingError;

    async fn config<S>(&mut self, ctx: &mut Ctx<'_, S>) -> Result<PairingConfig, Self::Error>
    where
        S: Send + Sync,
    {
        let secret = self.credentials(ctx).await?;

        Ok(PairingConfig {
            client_id: ClientId {
                realm: self.mqtt_config.realm.clone(),
                device_id: self.mqtt_config.device_id.clone(),
            },
            pairing_url: self.mqtt_config.pairing_url.clone(),
            keepalive: self.mqtt_config.keepalive,
            secret,
        })
    }
}
