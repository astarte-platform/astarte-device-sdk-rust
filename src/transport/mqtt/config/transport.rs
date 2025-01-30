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

use std::path::PathBuf;
use std::sync::Arc;

use rumqttc::Transport;
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::fs;
use tracing::{debug, error, info};
use url::Url;

use super::tls::ClientAuth;
use super::{CertificateFile, PrivateKeyFile};
use crate::error::Report;
use crate::transport::mqtt::crypto::Bundle;
use crate::transport::mqtt::pairing::ApiClient;
use crate::transport::mqtt::PairingError;

/// Structure to create an authenticated [`Transport`]
#[derive(Debug)]
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

        debug!("credentials created");

        Ok((bundle, certificate))
    }

    /// Store the credentials to files.
    async fn store_credentials(
        &self,
        private_key_file: &PrivateKeyFile,
        private_key: &PrivatePkcs8KeyDer<'_>,
        certificate_file: &CertificateFile,
        certificate: &str,
    ) {
        // Don't fail here since the SDK can always regenerate the certificate,
        if let Err(err) = fs::write(&certificate_file, &certificate).await {
            error!(error = %Report::new(&err), file = %certificate_file, "couldn't write certificate file");
        }
        if let Err(err) = fs::write(&private_key_file, &private_key.secret_pkcs8_der()).await {
            error!(error = %Report::new(err), file = %private_key_file, "couldn't write private key file");
        }
    }

    /// Read credentials from the filesystem.
    async fn read_credentials(&self) -> Option<ClientAuth> {
        let Some(store_dir) = &self.store_dir else {
            debug!("no store directory");

            return None;
        };

        debug!("reading existing credentials from {}", store_dir.display());

        let certificate_file = CertificateFile::new(store_dir);
        let private_key_file = PrivateKeyFile::new(store_dir);

        ClientAuth::try_read(certificate_file, private_key_file).await
    }

    /// Config the TLS for the transport.
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

    /// Creates a new credential and if a store directory is set, it stores it
    async fn create_credentials(&self, client: &ApiClient<'_>) -> Result<ClientAuth, PairingError> {
        debug!("creating new transport credentials");

        let (bundle, certificate) = self.create_certificate(client).await?;

        // If no store dir is set we just create a new certificate
        if let Some(store_dir) = &self.store_dir {
            let certificate_file = CertificateFile::new(store_dir);
            let private_key_file = PrivateKeyFile::new(store_dir);

            self.store_credentials(
                &private_key_file,
                &bundle.private_key,
                &certificate_file,
                &certificate,
            )
            .await
        }

        ClientAuth::try_from_pem_cert(certificate, bundle.private_key)
            .map_err(PairingError::InvalidCredentials)
    }

    /// Retrieves an already stored certificate or creates a new one
    async fn retrieve_credentials(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<ClientAuth, PairingError> {
        debug!("retrieving credentials");

        // Return with the existing certificate
        if let Some(auth) = self.read_credentials().await {
            info!("existing certificate found");

            return Ok(auth);
        }

        self.create_credentials(client).await
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

#[cfg(test)]
mod tests {
    use mockito::Server;
    use tempfile::TempDir;

    use super::*;
    use crate::transport::mqtt::pairing::tests::mock_create_certificate;

    #[tokio::test]
    async fn should_create_transport_insecure() {
        let dir = TempDir::new().unwrap();

        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(2)
            .create_async()
            .await;

        // With store
        let provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().to_owned()),
            true,
        );

        let api = ApiClient::from_transport(&provider, "realm", "device_id");

        let _ = provider.transport(&api).await.unwrap();

        let certificate_file = CertificateFile::new(dir.path());
        let private_key_file = PrivateKeyFile::new(dir.path());

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();
        assert_eq!(cert, "certificate");
        let key = tokio::fs::read(&private_key_file).await.unwrap();
        assert!(!key.is_empty());

        // Without store
        let provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            None,
            true,
        );

        let api = ApiClient::from_transport(&provider, "realm", "device_id");

        let _ = provider.transport(&api).await.unwrap();

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn should_create_transport() {
        let dir = TempDir::new().unwrap();

        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(2)
            .create_async()
            .await;

        // With store
        let provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().to_owned()),
            false,
        );

        let api = ApiClient::from_transport(&provider, "realm", "device_id");

        let _ = provider.transport(&api).await.unwrap();

        let certificate_file = CertificateFile::new(dir.path());
        let private_key_file = PrivateKeyFile::new(dir.path());

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();
        assert_eq!(cert, "certificate");
        let key = tokio::fs::read(&private_key_file).await.unwrap();
        assert!(!key.is_empty());

        // Without store
        let provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            None,
            false,
        );

        let api = ApiClient::from_transport(&provider, "realm", "device_id");

        let _ = provider.transport(&api).await.unwrap();

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn should_recreate_transport() {
        let dir = TempDir::new().unwrap();

        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(2)
            .create_async()
            .await;

        // With store
        let provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().to_owned()),
            false,
        );

        let api = ApiClient::from_transport(&provider, "realm", "device_id");

        let _ = provider.recreate_transport(&api).await.unwrap();

        let certificate_file = CertificateFile::new(dir.path());
        let private_key_file = PrivateKeyFile::new(dir.path());

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();
        assert_eq!(cert, "certificate");
        let key = tokio::fs::read(&private_key_file).await.unwrap();
        assert!(!key.is_empty());

        // Without store
        let provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            None,
            false,
        );

        let api = ApiClient::from_transport(&provider, "realm", "device_id");

        let _ = provider.recreate_transport(&api).await.unwrap();

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn should_succeed_if_fs_error() {
        let dir = TempDir::new().unwrap();

        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(2)
            .create_async()
            .await;

        let provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().join("non existing")),
            false,
        );

        let api = ApiClient::from_transport(&provider, "realm", "device_id");

        let _ = provider.transport(&api).await.unwrap();

        let _ = provider.recreate_transport(&api).await.unwrap();

        mock.assert_async().await;
    }
}
