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

use std::fmt::Display;
use std::io;
use std::path::Path;

use astarte_device_error::{Error, WrapError};
use tokio::fs;
use tracing::{debug, error, info, instrument};

use self::client::{ApiClient, ClientArgs};

use crate::transport::mqtt::components::ClientId;
use crate::transport::mqtt::connection::context::ConnCtx;
use crate::transport::mqtt::crypto::CryptoError;
use crate::transport::mqtt::{Credential, MqttConfig};

use super::{Pairing, PairingConfig};

pub(crate) mod client;
pub mod registration;

/// Error returned during pairing.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairingApiError {
    /// Couldn't pair for invalid argument
    InvalidArgument,
    /// The pairing request failed.
    Request,
    /// The API returned an error.
    Api,
    /// Couldn't configure the TLS store
    Tls,
    /// Couldn't join task
    Join,
    /// Couldn't read the or write the credentials
    Io(std::io::ErrorKind),
    /// Crypto operation failed
    Crypto(CryptoError),
}

impl Display for PairingApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PairingApiError::InvalidArgument => write!(f, "invalid argument"),
            PairingApiError::Request => write!(f, "couldn't send the request"),
            PairingApiError::Tls => write!(f, "couldn't configure TLS"),
            PairingApiError::Api => write!(f, "the api responded with an error"),
            PairingApiError::Join => write!(f, "couldn't join task"),
            PairingApiError::Io(error) => write!(f, "io error {error}"),
            PairingApiError::Crypto(error) => write!(f, "crypto error {error}"),
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
    async fn credentials<S>(
        &mut self,
        ctx: &mut ConnCtx<'_, S>,
    ) -> Result<String, Error<PairingApiError>> {
        // We need to clone to not return something owning a mutable reference to self
        match &self.mqtt_config.credential {
            Credential::Secret { credentials_secret } => Ok(credentials_secret.clone()),
            Credential::ParingToken { pairing_token } => {
                debug!("pairing token provided, retrieving credentials secret");

                let secret = self.read_secret_or_register(ctx, pairing_token).await?;

                Ok(secret)
            }
        }
    }

    /// Register the device and stores the credentials secret in the given directory
    async fn read_secret_or_register<S>(
        &self,
        ctx: &mut ConnCtx<'_, S>,
        pairing_token: &str,
    ) -> Result<String, Error<PairingApiError>> {
        let credential_file = ctx
            .state
            .config
            .writable_dir
            .as_ref()
            .map(|dir| dir.join(CERTIFICATE_FILE))
            .ok_or(Error::with(
                PairingApiError::InvalidArgument,
                "missing writable dir to store credentials",
            ))?;

        match fs::read_to_string(&credential_file).await {
            Ok(secret) => {
                info!("secret read from file");

                return Ok(secret);
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                info!("no credential file {}", credential_file.display())
            }
            Err(err) => {
                return Err(Error::with(
                    PairingApiError::Io(err.kind()),
                    "while reading credential file",
                )
                .set_source(err)
                .set_ctx(format!("from {}", credential_file.display())));
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
        fs::write(&credential_file, &secret)
            .await
            .wrap_err_with(|err| {
                Error::with(
                    PairingApiError::Io(err.kind()),
                    "while writing credential secret",
                )
                .set_ctx(format!("to {}", credential_file.display()))
            })?;

        Ok(secret)
    }
}

impl Pairing for PairingApi {
    type Error = Error<PairingApiError>;

    async fn config<S>(&mut self, ctx: &mut ConnCtx<'_, S>) -> Result<PairingConfig, Self::Error>
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

    #[instrument(skip(self), ret)]
    async fn is_paired(&self, store_dir: Option<&Path>) -> std::io::Result<bool> {
        if matches!(self.mqtt_config.credential, Credential::Secret { .. }) {
            return Ok(true);
        }

        let credential_file = store_dir.map(|p| p.join(CREDENTIAL_FILE)).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "store directory not configured for pairing with token",
            )
        })?;

        tokio::fs::try_exists(&credential_file)
            .await
            .inspect_err(|_| {
                error!(
                    path = %credential_file.display(),
                    "couldn't read credentials file"
                )
            })
    }
}
