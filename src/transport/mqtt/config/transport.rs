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

use core::str;
use std::{path::PathBuf, sync::Arc};

use chrono::{DateTime, Utc};
use rumqttc::Transport;
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::RootCertStore;
use tokio::fs;
use tracing::{debug, error, info, instrument, warn};
use url::Url;

use super::ClientId;
use super::{tls::ClientAuth, CertificateFile, PrivateKeyFile};
use crate::logging::security::{notify_security_event, SecurityEvent};
use crate::transport::mqtt::config::tls::read_root_cert_store;
use crate::{
    error::Report,
    transport::mqtt::{
        config::tls::{insecure_tls_config_builder, tls_config_builder},
        crypto::Bundle,
        pairing::ApiClient,
        PairingError,
    },
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

    pub(crate) fn api_tls_config(&self) -> Result<rustls::ClientConfig, PairingError> {
        let client_cfg = if self.insecure_ssl {
            insecure_tls_config_builder()?.with_no_client_auth()
        } else {
            tls_config_builder(self.root_cert_store())?.with_no_client_auth()
        };

        debug!("TLS client config read");

        Ok(client_cfg)
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

    fn root_cert_store(&self) -> Arc<RootCertStore> {
        Arc::clone(&self.root_cert_store)
    }

    /// Config the TLS for the transport.
    fn config_transport(&self, client_auth: ClientAuth) -> Result<Transport, PairingError> {
        let config = if self.insecure_ssl {
            notify_security_event(SecurityEvent::AlarmUnsecureCommunication);
            client_auth.insecure_tls_config()?
        } else {
            let roots = self.root_cert_store();
            client_auth.tls_config(roots)?
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

        match ClientAuth::try_from_pem_cert(
            certificate,
            bundle.private_key,
            ClientId {
                realm: client.realm,
                device_id: client.device_id,
            },
        ) {
            Ok(Some(auth)) => Ok(auth),
            Ok(None) => Err(PairingError::MissingCredentials),
            Err(e) => Err(PairingError::InvalidCredentials(e)),
        }
    }

    /// Retrieves an already stored certificate or creates a new one
    /// It also verifies certificate data that can be checked for validity locally
    async fn retrieve_credentials(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<ClientAuth, PairingError> {
        debug!("retrieving credentials");

        let auth = self
            .read_credentials(ClientId {
                realm: client.realm,
                device_id: client.device_id,
            })
            .await;

        // Return with the existing certificate
        if let Some(auth) = auth {
            info!("existing certificate found");

            return Ok(auth);
        } else {
            notify_security_event(SecurityEvent::AlarmCertificateUnavailable);
        }

        self.create_credentials(client).await
    }

    /// Create a new transport with the given credentials
    pub(crate) async fn transport(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<Transport, PairingError> {
        let client_auth = self.retrieve_credentials(client).await?;

        self.config_transport(client_auth)
    }

    pub(crate) async fn fetch_cert_expiry(
        &self,
        client_id: ClientId<&str>,
    ) -> Option<DateTime<Utc>> {
        let client_auth = self.read_credentials(client_id).await?;

        client_auth.parse_validity_not_after()
    }

    // validate the existing certificate is valid, if valid use it for the transport
    // if it's invalid recreate the certificate
    pub(crate) async fn validate_transport(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<Transport, PairingError> {
        let client_auth = if let Some(client_auth) = self.verify_certificate(client).await? {
            client_auth
        } else {
            self.create_credentials(client).await?
        };

        self.config_transport(client_auth)
    }

    /// Verify the stored certificate first by checking the Subject common name
    /// and comparing it to the client id then by using the provided astarte api
    /// Returns the [`ClientAuth`] struct if valid, None otherwise
    async fn verify_certificate(
        &self,
        client: &ApiClient<'_>,
    ) -> Result<Option<ClientAuth>, PairingError> {
        let Some(store_dir) = &self.store_dir else {
            warn!(
                store_dir = ?self.store_dir,
                "no device store directory, assuming invalid"
            );
            return Ok(None);
        };

        let certificate_file = CertificateFile::new(store_dir);
        let key_file = PrivateKeyFile::new(store_dir);

        let Some(client_auth) = ClientAuth::try_read(
            certificate_file,
            key_file,
            ClientId {
                realm: client.realm,
                device_id: client.device_id,
            },
        )
        .await
        else {
            warn!(
                ?store_dir,
                "no device certificate file in store dir, assuming invalid"
            );
            notify_security_event(SecurityEvent::AlarmCertificateUnavailable);
            return Ok(None);
        };

        client
            .verify_certificate(client_auth.pem())
            .await
            .map(|res| {
                if res {
                    notify_security_event(SecurityEvent::CertificateValidationSucceeded);
                } else {
                    notify_security_event(SecurityEvent::CertificateValidationFailed);
                }

                res.then_some(client_auth)
            })
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

    use std::time::Duration;

    use mockito::Server;
    use rumqttc::TlsConfiguration;
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
        let provider = TransportProvider::configure(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().to_owned()),
            true,
        )
        .await
        .expect("failed to configure transport provider");

        let api =
            ApiClient::from_transport(&provider, "realm", "device_id", Duration::from_secs(10))
                .expect("failed to create api client");

        let transport = provider.transport(&api).await.unwrap();

        assert!(matches!(
            transport,
            Transport::Tls(TlsConfiguration::Rustls(..))
        ));

        let certificate_file = CertificateFile::new(dir.path());
        let private_key_file = PrivateKeyFile::new(dir.path());

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();
        rustls_pemfile::certs(&mut cert.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        let key = tokio::fs::read(&private_key_file).await.unwrap();
        assert!(!key.is_empty());

        // Without store
        let provider = TransportProvider::configure(
            server.url().parse().unwrap(),
            "secret".to_string(),
            None,
            true,
        )
        .await
        .expect("failed to configure transport provider");

        let api =
            ApiClient::from_transport(&provider, "realm", "device_id", Duration::from_secs(10))
                .expect("failed to create api client");

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
        let provider = TransportProvider::configure(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().to_owned()),
            false,
        )
        .await
        .expect("failed to configure transport provider");

        let api =
            ApiClient::from_transport(&provider, "realm", "device_id", Duration::from_secs(10))
                .expect("failed to create api client");

        let _ = provider.transport(&api).await.unwrap();

        let certificate_file = CertificateFile::new(dir.path());
        let private_key_file = PrivateKeyFile::new(dir.path());

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();
        rustls_pemfile::certs(&mut cert.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        let key = tokio::fs::read(&private_key_file).await.unwrap();
        assert!(!key.is_empty());

        // Without store
        let provider = TransportProvider::configure(
            server.url().parse().unwrap(),
            "secret".to_string(),
            None,
            false,
        )
        .await
        .expect("failed to configure transport provider");

        let api =
            ApiClient::from_transport(&provider, "realm", "device_id", Duration::from_secs(10))
                .expect("failed to create api client");

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
        let provider = TransportProvider::configure(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().to_owned()),
            false,
        )
        .await
        .expect("failed to configure transport provider");

        let api =
            ApiClient::from_transport(&provider, "realm", "device_id", Duration::from_secs(10))
                .expect("failed to create api client");

        let _ = provider.validate_transport(&api).await.unwrap();

        let certificate_file = CertificateFile::new(dir.path());
        let private_key_file = PrivateKeyFile::new(dir.path());

        let cert = tokio::fs::read_to_string(&certificate_file).await.unwrap();
        rustls_pemfile::certs(&mut cert.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        let key = tokio::fs::read(&private_key_file).await.unwrap();
        assert!(!key.is_empty());

        // Without store
        let provider = TransportProvider::configure(
            server.url().parse().unwrap(),
            "secret".to_string(),
            None,
            false,
        )
        .await
        .expect("failed to configure transport provider");

        let api =
            ApiClient::from_transport(&provider, "realm", "device_id", Duration::from_secs(10))
                .expect("failed to create api client");

        let _ = provider.validate_transport(&api).await.unwrap();

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

        let provider = TransportProvider::configure(
            server.url().parse().unwrap(),
            "secret".to_string(),
            Some(dir.path().join("non existing")),
            false,
        )
        .await
        .expect("failed to configure transport provider");

        let api =
            ApiClient::from_transport(&provider, "realm", "device_id", Duration::from_secs(10))
                .expect("failed to create api client");

        let _ = provider.transport(&api).await.unwrap();

        let _ = provider.validate_transport(&api).await.unwrap();

        mock.assert_async().await;
    }
}
