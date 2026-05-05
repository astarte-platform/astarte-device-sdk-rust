// This file is part of Astarte.
//
// Copyright 2024, 2026 SECO Mind Srl
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

use core::str;
use std::path::PathBuf;
use std::sync::Arc;

use rumqttc::Transport;
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::fs;
use tracing::{debug, error, info, instrument};

use super::ClientId;
use super::{CertificateFile, PrivateKeyFile, tls::ClientAuth};
use crate::error::Report;
use crate::logging::security::{SecurityEvent, notify_security_event};
use crate::pairing::api::PairingError;
use crate::pairing::api::client::ApiClient;
use crate::transport::mqtt::config::tls::{
    insecure_tls_config_builder, is_env_ignore_ssl, read_root_cert_store, tls_config_builder,
};
use crate::transport::mqtt::crypto::Bundle;

/// Structure to create an authenticated [`Transport`]
#[derive(Debug)]
pub(crate) struct TransportProvider {
    store_dir: Option<PathBuf>,
    insecure_ssl: bool,
    root_cert_store: Arc<RootCertStore>,
}

impl TransportProvider {
    #[instrument(skip_all)]
    pub(crate) async fn configure(
        store_dir: Option<PathBuf>,
        insecure_ssl: bool,
    ) -> Result<Self, PairingError> {
        let insecure_ssl = insecure_ssl || is_env_ignore_ssl();

        if insecure_ssl {
            notify_security_event(SecurityEvent::TlsValidationCheckDisabledSuccessfully);
        }

        debug!("reading root cert store from native certs");
        let root_certs = read_root_cert_store().await?;

        Ok(Self {
            insecure_ssl,
            root_cert_store: Arc::new(root_certs),
            store_dir,
        })
    }

    pub(crate) fn api_tls_config(&self) -> Result<rustls::ClientConfig, PairingError> {
        let client_cfg = if self.insecure_ssl {
            insecure_tls_config_builder()?.with_no_client_auth()
        } else {
            let roots = Arc::clone(&self.root_cert_store);

            tls_config_builder(roots)?.with_no_client_auth()
        };

        debug!("TLS client config read");

        Ok(client_cfg)
    }

    /// Config the TLS for the transport.
    ///
    /// It  will be passed to the MQTT connection.
    pub(crate) fn config_transport(
        &self,
        client_auth: ClientAuth,
    ) -> Result<Transport, PairingError> {
        let config = if self.insecure_ssl {
            notify_security_event(SecurityEvent::AlarmUnsecureCommunication);

            client_auth.insecure_tls_config()?
        } else {
            let roots = Arc::clone(&self.root_cert_store);

            client_auth.tls_config(roots)?
        };

        Ok(Transport::tls_with_config(
            rumqttc::TlsConfiguration::Rustls(Arc::new(config)),
        ))
    }

    /// Retrieves an already stored certificate or creates a new one
    ///
    /// It also verifies certificate validity.
    pub(crate) async fn retrieve_credentials(
        &self,
        client: &ApiClient<'_>,
        client_id: ClientId<&str>,
    ) -> Result<Option<ClientAuth>, PairingError> {
        debug!("retrieving credentials");

        let auth = self.read_credentials(client_id).await;

        let Some(auth) = auth else {
            notify_security_event(SecurityEvent::AlarmCertificateUnavailable);

            return Ok(None);
        };

        debug!("existing certificate found");

        let is_valid = client.verify_certificate(auth.pem()).await?;

        if is_valid {
            notify_security_event(SecurityEvent::CertificateValidationSucceeded);

            Ok(Some(auth))
        } else {
            notify_security_event(SecurityEvent::CertificateValidationFailed);

            Ok(None)
        }
    }

    /// Creates new credentials and if a store directory is set, it stores it
    pub(crate) async fn create_credentials(
        &self,
        client: &ApiClient<'_>,
        client_id: ClientId<&str>,
    ) -> Result<ClientAuth, PairingError> {
        debug!("creating new transport credentials");

        let (bundle, certificate) = self.create_certificate(client).await?;

        // If no store dir is set we just create a new certificate
        if let Some(store_dir) = &self.store_dir {
            debug!("storing credentials");

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

        match ClientAuth::try_from_pem_cert(certificate, bundle.private_key, client_id) {
            Ok(Some(auth)) => Ok(auth),
            Ok(None) => Err(PairingError::MissingCredentials),
            Err(e) => Err(PairingError::InvalidCredentials(e)),
        }
    }

    /// Create the certificate using the Astarte API
    async fn create_certificate(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<(Bundle, String), PairingError> {
        let bundle = Bundle::generate_key(client.realm, client.device_id)?;
        notify_security_event(SecurityEvent::CsrPendingApproval);

        let certificate = client
            .create_certificate(&bundle.csr)
            .await
            .inspect_err(|_| {
                notify_security_event(SecurityEvent::CsrFailed);
                notify_security_event(SecurityEvent::CertificateTransferFailed);
            })?;

        notify_security_event(SecurityEvent::CsrApproved);
        notify_security_event(SecurityEvent::CertificateTransferredSuccessfully);
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
        let store_cert = fs::write(&certificate_file, &certificate).await;
        let store_key = fs::write(&private_key_file, &private_key.secret_pkcs8_der()).await;

        if store_cert.is_ok() && store_key.is_ok() {
            notify_security_event(SecurityEvent::CertificateStoredSuccessfully);
        } else {
            notify_security_event(SecurityEvent::CertificateWriteFailed);
        }

        // Don't fail here since the SDK can always regenerate the certificate,
        if let Err(err) = store_cert {
            error!(error = %Report::new(&err), file = %certificate_file, "couldn't write certificate file");
        }
        if let Err(err) = store_key {
            error!(error = %Report::new(err), file = %private_key_file, "couldn't write private key file");
        }
    }

    /// Read credentials from the filesystem.
    async fn read_credentials(&self, client_id: ClientId<&str>) -> Option<ClientAuth> {
        let Some(store_dir) = &self.store_dir else {
            debug!("no store directory");

            return None;
        };

        debug!("reading existing credentials from {}", store_dir.display());

        let certificate_file = CertificateFile::new(store_dir);
        let private_key_file = PrivateKeyFile::new(store_dir);

        ClientAuth::try_read(certificate_file, private_key_file, client_id).await
    }
}

// TODO: test the certificate validation fail
#[cfg(test)]
mod tests {

    use std::path::Path;

    use mockito::Server;
    use rumqttc::TlsConfiguration;
    use tempfile::TempDir;
    use url::Url;

    use crate::builder::Config;
    use crate::pairing::api::client::ClientArgs;
    use crate::pairing::api::client::tests::mock_create_certificate;

    use super::*;

    fn mock_args(url: &Url) -> (ClientId<&str>, ClientArgs<'_>) {
        let client_id = ClientId {
            realm: "realm",
            device_id: "device_id",
        };

        let args = ClientArgs {
            realm: "realm",
            device_id: "device_id",
            pairing_url: url,
            token: "secret",
        };

        (client_id, args)
    }

    async fn check_stored_keys(dir: &Path) {
        let certificate_file = CertificateFile::new(dir);
        let private_key_file = PrivateKeyFile::new(dir);

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();

        rustls_pemfile::certs(&mut cert.as_bytes())
            .next()
            .unwrap()
            .unwrap();

        let key = tokio::fs::read(&private_key_file).await.unwrap();

        assert!(!key.is_empty());
    }

    #[tokio::test]
    async fn should_create_transport_insecure() {
        let dir = TempDir::new().unwrap();

        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(1)
            .create_async()
            .await;

        // With store
        let provider = TransportProvider::configure(Some(dir.path().to_owned()), true)
            .await
            .expect("failed to configure transport provider");

        let url = server.url().parse().unwrap();

        let (client_id, args) = mock_args(&url);

        let api = ApiClient::from_transport(&Config::default(), &provider, args)
            .expect("failed to create api client");

        let auth = provider.create_credentials(&api, client_id).await.unwrap();

        let transport = provider.config_transport(auth).unwrap();

        assert!(matches!(
            transport,
            Transport::Tls(TlsConfiguration::Rustls(..))
        ));

        mock.assert_async().await;

        check_stored_keys(dir.path()).await;
    }

    #[tokio::test]
    async fn should_create_transport_insecure_no_store() {
        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(1)
            .create_async()
            .await;

        // Without store
        let provider = TransportProvider::configure(None, true)
            .await
            .expect("failed to configure transport provider");

        let url = server.url().parse().unwrap();
        let (client_id, args) = mock_args(&url);

        let api = ApiClient::from_transport(&Config::default(), &provider, args)
            .expect("failed to create api client");

        let auth = provider.create_credentials(&api, client_id).await.unwrap();
        let transport = provider.config_transport(auth).unwrap();

        assert!(matches!(
            transport,
            Transport::Tls(TlsConfiguration::Rustls(..))
        ));

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn should_create_transport() {
        let dir = TempDir::new().unwrap();

        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(1)
            .create_async()
            .await;

        // With store
        let provider = TransportProvider::configure(Some(dir.path().to_owned()), false)
            .await
            .expect("failed to configure transport provider");

        let url = server.url().parse().unwrap();
        let (client_id, args) = mock_args(&url);

        let api = ApiClient::from_transport(&Config::default(), &provider, args)
            .expect("failed to create api client");

        let auth = provider.create_credentials(&api, client_id).await.unwrap();
        let transport = provider.config_transport(auth).unwrap();

        assert!(matches!(
            transport,
            Transport::Tls(TlsConfiguration::Rustls(..))
        ));

        mock.assert_async().await;

        check_stored_keys(dir.path()).await;
    }

    #[tokio::test]
    async fn should_create_transport_no_store() {
        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(1)
            .create_async()
            .await;
        // Without store
        let provider = TransportProvider::configure(None, false)
            .await
            .expect("failed to configure transport provider");

        let url = server.url().parse().unwrap();

        let (client_id, args) = mock_args(&url);

        let api = ApiClient::from_transport(&Config::default(), &provider, args)
            .expect("failed to create api client");

        let auth = provider.create_credentials(&api, client_id).await.unwrap();
        let transport = provider.config_transport(auth).unwrap();

        assert!(matches!(
            transport,
            Transport::Tls(TlsConfiguration::Rustls(..))
        ));

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn should_succeed_if_fs_error() {
        let dir = TempDir::new().unwrap();

        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(1)
            .create_async()
            .await;

        let provider = TransportProvider::configure(Some(dir.path().join("non existing")), false)
            .await
            .expect("failed to configure transport provider");

        let url = server.url().parse().unwrap();
        let (client_id, args) = mock_args(&url);

        let api = ApiClient::from_transport(&Config::default(), &provider, args)
            .expect("failed to create api client");

        let auth = provider.create_credentials(&api, client_id).await.unwrap();
        let transport = provider.config_transport(auth).unwrap();

        assert!(matches!(
            transport,
            Transport::Tls(TlsConfiguration::Rustls(..))
        ));

        mock.assert_async().await;
    }
}
