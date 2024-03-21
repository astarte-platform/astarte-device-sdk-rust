// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use log::error;
use rumqttc::Transport;
use tokio::fs;
use url::Url;

use crate::transport::mqtt::{crypto::Bundle, pairing::ApiClient, PairingError};

use super::{tls::ClientAuth, CERTIFICATE_FILE, PRIVATE_KEY_FILE};

/// Structure to create an authenticated [`Transport`]
pub(crate) struct TransportProvider {
    pairing_url: Url,
    credential_secret: String,
    store_dir: Option<PathBuf>,
    insecure_ssl: bool,
}

impl TransportProvider {
    pub(crate) fn new(
        pairing_url: Url,
        credential_secret: String,
        store_dir: Option<PathBuf>,
        insecure_ssl: bool,
    ) -> Self {
        Self {
            pairing_url,
            credential_secret,
            store_dir,
            insecure_ssl,
        }
    }

    /// Create the certificate using the Astarte API
    async fn create_certificate(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<(Bundle, String), PairingError> {
        let bundle = Bundle::new(client.realm, client.device_id)?;

        let certificate = client.create_certificate(&bundle.csr).await?;

        Ok((bundle, certificate))
    }

    /// Creates and stores a new credential
    async fn create_and_store(
        &self,
        client: &ApiClient<'_>,
        certificate_file: &Path,
        private_key_file: &Path,
    ) -> Result<ClientAuth, PairingError> {
        let (bundle, certificate) = self.create_certificate(client).await?;

        // Don't fail here since the SDK can always regenerate the certificate,
        if let Err(err) = fs::write(&certificate_file, &certificate).await {
            error!(
                "couldn't write certificate file {}, {err}",
                certificate_file.display()
            );
        }
        if let Err(err) = fs::write(&private_key_file, &bundle.private_key.secret_pkcs8_der()).await
        {
            error!(
                "couldn't write private key file {}, {err}",
                private_key_file.display()
            );
        }

        ClientAuth::try_from_pem_cert(certificate, bundle.private_key)
            .map_err(PairingError::InvalidCredentials)
    }

    /// Creates a new credential and if a store directory is set, it stores it
    async fn create_credentials(&self, client: &ApiClient<'_>) -> Result<ClientAuth, PairingError> {
        // If no store dir is set we just create a new certificate
        let Some(store_dir) = &self.store_dir else {
            let (bundle, certificate) = self.create_certificate(client).await?;

            return ClientAuth::try_from_pem_cert(certificate, bundle.private_key)
                .map_err(PairingError::InvalidCredentials);
        };

        let certificate_file = store_dir.join(CERTIFICATE_FILE);
        let private_key_file = store_dir.join(PRIVATE_KEY_FILE);

        self.create_and_store(client, &certificate_file, &private_key_file)
            .await
    }

    /// Retrieves an already stored certificate or creates a new one
    async fn retrieve_credentials(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<ClientAuth, PairingError> {
        // If no store dir is set we just create a new certificate
        let Some(store_dir) = &self.store_dir else {
            let (bundle, certificate) = self.create_certificate(client).await?;

            return ClientAuth::try_from_pem_cert(certificate, bundle.private_key)
                .map_err(PairingError::InvalidCredentials);
        };

        let certificate_file = store_dir.join(CERTIFICATE_FILE);
        let private_key_file = store_dir.join(PRIVATE_KEY_FILE);

        if let Some(auth) =
            ClientAuth::try_read(certificate_file.clone(), private_key_file.clone()).await
        {
            return Ok(auth);
        }

        self.create_and_store(client, &certificate_file, &private_key_file)
            .await
    }

    async fn config_transport(&self, client_auth: ClientAuth) -> Result<Transport, PairingError> {
        let config = if self.insecure_ssl {
            client_auth.insecure_tls_config().await?
        } else {
            client_auth.tls_config().await?
        };

        Ok(Transport::tls_with_config(
            rumqttc::TlsConfiguration::Rustls(Arc::new(config)),
        ))
    }

    /// Create a new transport with the given credentials
    pub(crate) async fn transport(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<Transport, PairingError> {
        let client_auth = self.retrieve_credentials(client).await?;

        self.config_transport(client_auth).await
    }

    /// Create a new transport, including the creation of new credentials.
    pub(crate) async fn recreate_transport(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<Transport, PairingError> {
        let client_auth = self.create_credentials(client).await?;

        self.config_transport(client_auth).await
    }

    pub(crate) fn pairing_url(&self) -> &Url {
        &self.pairing_url
    }

    pub(crate) fn credential_secret(&self) -> &str {
        &self.credential_secret
    }
}
