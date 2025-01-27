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

use rumqttc::Transport;
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::RootCertStore;
use tokio::fs;
use tracing::{debug, error, info, instrument, warn};
use url::Url;

use super::{
    tls::{ClientAuth, NoVerifier},
    CertificateFile, PrivateKeyFile,
};
use crate::transport::mqtt::config::tls::read_root_cert_store;
use crate::{
    error::Report,
    transport::mqtt::{crypto::Bundle, pairing::ApiClient, PairingError},
};

/// Structure to create an authenticated [`Transport`]
#[derive(Debug)]
pub(crate) struct TransportProvider {
    pairing_url: Url,
    credential_secret: String,
    store_dir: Option<PathBuf>,
    insecure_ssl: bool,
    root_cert_store: Arc<RootCertStore>,
}

impl TransportProvider {
    #[allow(dead_code)]
    pub(crate) fn new(
        pairing_url: Url,
        credential_secret: String,
        store_dir: Option<PathBuf>,
        insecure_ssl: bool,
        root_cert_store: RootCertStore,
    ) -> Self {
        Self {
            pairing_url,
            credential_secret,
            store_dir,
            insecure_ssl,
            root_cert_store: Arc::new(root_cert_store),
        }
    }

    #[instrument(skip_all)]
    pub(crate) async fn configure(
        pairing_url: Url,
        credential_secret: String,
        store_dir: Option<PathBuf>,
        insecure_ssl: bool,
    ) -> Result<Self, PairingError> {
        debug!("reading root cert store from native certs");
        let root_certs = read_root_cert_store().await?;

        Ok(Self {
            pairing_url,
            credential_secret,
            store_dir,
            insecure_ssl,
            root_cert_store: Arc::new(root_certs),
        })
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

    #[instrument(skip_all)]
    fn root_cert_store(&self) -> Arc<RootCertStore> {
        Arc::clone(&self.root_cert_store)
    }

    /// Config the TLS for the transport.
    async fn config_transport(
        &mut self,
        client_auth: ClientAuth,
    ) -> Result<Transport, PairingError> {
        let config = if self.insecure_ssl {
            client_auth.insecure_tls_config().await?
        } else {
            let roots = self.root_cert_store();
            client_auth.tls_config(roots).await?
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
        &mut self,
        client: &ApiClient<'_>,
    ) -> Result<Transport, PairingError> {
        let client_auth = self.retrieve_credentials(client).await?;

        self.config_transport(client_auth).await
    }

    /// Create a new transport, including the creation of new credentials.
    pub(crate) async fn recreate_transport(
        &mut self,
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

    #[instrument(skip_all)]
    pub(crate) fn api_tls_config(&mut self) -> rustls::ClientConfig {
        let client_cfg = if self.insecure_ssl {
            warn!("INSECURE: ignore TLS certificates");
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier {}))
                .with_no_client_auth()
        } else {
            rustls::ClientConfig::builder()
                .with_root_certificates(Arc::clone(&self.root_cert_store))
                .with_no_client_auth()
        };

        debug!("TLS client config read");

        client_cfg
    }
}

#[cfg(test)]
mod tests {
    use mockito::Server;
    use tempfile::TempDir;

    use crate::transport::mqtt::pairing::tests::mock_create_certificate;

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
        let mut provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().to_owned()),
            true,
            RootCertStore::empty(),
        );

        let api = ApiClient::new(
            "realm",
            "device_id",
            provider.pairing_url().clone(),
            provider.credential_secret().to_string(),
            provider.api_tls_config(),
        );

        let _ = provider.transport(&api).await.unwrap();

        let certificate_file = CertificateFile::new(dir.path());
        let private_key_file = PrivateKeyFile::new(dir.path());

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();
        assert_eq!(cert, "certificate");
        let key = tokio::fs::read(&private_key_file).await.unwrap();
        assert!(!key.is_empty());

        // Without store
        let mut provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            None,
            true,
            RootCertStore::empty(),
        );

        let api = ApiClient::new(
            "realm",
            "device_id",
            provider.pairing_url().clone(),
            provider.credential_secret().to_string(),
            provider.api_tls_config(),
        );

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
        let mut provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().to_owned()),
            false,
            RootCertStore::empty(),
        );

        let api = ApiClient::new(
            "realm",
            "device_id",
            provider.pairing_url().clone(),
            provider.credential_secret().to_string(),
            provider.api_tls_config(),
        );

        let _ = provider.transport(&api).await.unwrap();

        let certificate_file = CertificateFile::new(dir.path());
        let private_key_file = PrivateKeyFile::new(dir.path());

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();
        assert_eq!(cert, "certificate");
        let key = tokio::fs::read(&private_key_file).await.unwrap();
        assert!(!key.is_empty());

        // Without store
        let mut provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            None,
            false,
            RootCertStore::empty(),
        );

        let api = ApiClient::new(
            "realm",
            "device_id",
            provider.pairing_url().clone(),
            provider.credential_secret().to_string(),
            provider.api_tls_config(),
        );

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
        let mut provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().to_owned()),
            false,
            RootCertStore::empty(),
        );

        let api = ApiClient::new(
            "realm",
            "device_id",
            provider.pairing_url().clone(),
            provider.credential_secret().to_string(),
            provider.api_tls_config(),
        );

        let _ = provider.recreate_transport(&api).await.unwrap();

        let certificate_file = CertificateFile::new(dir.path());
        let private_key_file = PrivateKeyFile::new(dir.path());

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();
        assert_eq!(cert, "certificate");
        let key = tokio::fs::read(&private_key_file).await.unwrap();
        assert!(!key.is_empty());

        // Without store
        let mut provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            None,
            false,
            RootCertStore::empty(),
        );

        let api = ApiClient::new(
            "realm",
            "device_id",
            provider.pairing_url().clone(),
            provider.credential_secret().to_string(),
            provider.api_tls_config(),
        );

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

        let mut provider = TransportProvider::new(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().join("non existing")),
            false,
            RootCertStore::empty(),
        );

        let tls_cfg = provider.api_tls_config();
        let api = ApiClient::new(
            "realm",
            "device_id",
            provider.pairing_url().clone(),
            provider.credential_secret().to_string(),
            tls_cfg,
        );

        let _ = provider.transport(&api).await.unwrap();

        let _ = provider.recreate_transport(&api).await.unwrap();

        mock.assert_async().await;
    }
}
