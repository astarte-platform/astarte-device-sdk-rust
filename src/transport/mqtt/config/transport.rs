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

use std::{path::PathBuf, sync::Arc};

use itertools::Itertools;
use rumqttc::Transport;
use rustls::pki_types::CertificateDer;
use tracing::{debug, info};
use url::Url;

use super::tls::{read_root_cert_store, NoVerifier};
use crate::transport::mqtt::crypto::default::DefaultCryptoProvider;
#[cfg(feature = "keystore-tss")]
use crate::transport::mqtt::crypto::tpm::TpmCryptoProvider;
use crate::transport::mqtt::crypto::CryptoProvider;
use crate::transport::mqtt::{pairing::ApiClient, PairingError};

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
    async fn create_certificate<T>(
        &self,
        client: &ApiClient<'_>,
        provider: &impl CryptoProvider<Bundle = T>,
    ) -> Result<(String, T), PairingError> {
        let (csr, item) = provider.create_csr(client.realm, client.device_id)?;

        let certificate = client.create_certificate(&csr).await?;

        debug!("credentials created");

        Ok((certificate, item))
    }

    /// Read credentials from the filesystem.
    async fn read_credentials<T>(
        &self,
        provider: &impl CryptoProvider<Bundle = T>,
    ) -> Option<(Vec<CertificateDer<'static>>, T)> {
        let Some(store_dir) = &self.store_dir else {
            debug!("no store directory");

            return None;
        };

        debug!("reading existing credentials from {}", store_dir.display());

        provider.read_credentials(store_dir.clone()).await
    }

    /// Config the TLS for the transport.
    async fn config_transport<T>(
        &self,
        certs: Vec<CertificateDer<'static>>,
        provider: &impl CryptoProvider<Bundle = T>,
        pr: T,
    ) -> Result<Transport, PairingError> {
        let config = if self.insecure_ssl {
            provider
                .insecure_tls_config(NoVerifier::init_insecure_tls_config(), certs, pr)
                .await?
        } else {
            let roots = read_root_cert_store().await?;
            provider.tls_config(roots, certs, pr).await?
        };

        Ok(Transport::tls_with_config(
            rumqttc::TlsConfiguration::Rustls(Arc::new(config)),
        ))
    }

    /// Creates a new credential and if a store directory is set, it stores it
    async fn create_credentials<T>(
        &self,
        client: &ApiClient<'_>,
        provider: &impl CryptoProvider<Bundle = T>,
    ) -> Result<(Vec<CertificateDer<'static>>, T), PairingError> {
        debug!("creating new transport credentials");

        let (certificate, bundle) = self.create_certificate(client, provider).await?;

        provider
            .store_credential(&self.store_dir, &certificate, &bundle)
            .await;

        let certs = rustls_pemfile::certs(&mut certificate.as_bytes())
            .try_collect()
            .map_err(PairingError::InvalidCredentials)?;

        Ok((certs, bundle))
    }

    /// Retrieves an already stored certificate or creates a new one
    async fn retrieve_credentials<T>(
        &self,
        client: &ApiClient<'_>,
        provider: &impl CryptoProvider<Bundle = T>,
    ) -> Result<(Vec<CertificateDer<'static>>, T), PairingError> {
        debug!("retrieving credentials");
        // Return with the existing certificate
        if let Some(auth) = self.read_credentials(provider).await {
            info!("existing certificate found");

            return Ok(auth);
        }

        self.create_credentials(client, provider).await
    }

    /// Create a new transport with the given credentials
    pub(crate) async fn transport(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<Transport, PairingError> {
        if !cfg!(feature = "keystore-none") && cfg!(feature = "keystore-tss") {
            #[cfg(feature = "keystore-tss")]
            {
                let crypto_provider = TpmCryptoProvider;
                let (client_auth, item) =
                    self.retrieve_credentials(client, &crypto_provider).await?;
                return self
                    .config_transport(client_auth, &crypto_provider, item)
                    .await;
            }
        }

        let crypto_provider = DefaultCryptoProvider::new()?;
        let (client_auth, item) = self.retrieve_credentials(client, &crypto_provider).await?;
        self.config_transport(client_auth, &crypto_provider, item)
            .await
    }

    /// Create a new transport, including the creation of new credentials.
    pub(crate) async fn recreate_transport(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<Transport, PairingError> {
        if !cfg!(feature = "keystore-none") && cfg!(feature = "keystore-tss") {
            #[cfg(feature = "keystore-tss")]
            {
                let crypto_provider = TpmCryptoProvider;
                let (client_auth, item) = self.create_credentials(client, &crypto_provider).await?;
                return self
                    .config_transport(client_auth, &crypto_provider, item)
                    .await;
            }
        }

        let crypto_provider = DefaultCryptoProvider::new()?;
        let (client_auth, item) = self.create_credentials(client, &crypto_provider).await?;
        self.config_transport(client_auth, &crypto_provider, item)
            .await
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
    use crate::transport::mqtt::config::{CertificateFile, PrivateKeyFile};
    use crate::transport::mqtt::pairing::tests::mock_create_certificate;
    use mockito::Server;
    use tempfile::TempDir;

    use super::*;

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
