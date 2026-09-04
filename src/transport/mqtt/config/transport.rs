// This file is part of Astarte.
//
// Copyright 2024, 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use core::str;
use std::io;
use std::path::Path;
use std::sync::Arc;

use astarte_device_error::{Error, ResultExt, WrapError};
use rumqttc::Transport;
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error};

use super::ClientId;
use super::{CertificateFile, PrivateKeyFile, tls::ClientAuth};
use crate::error::Report;
use crate::logging::security::{SecurityEvent, notify_security_event};
use crate::state::ConnectionState;
use crate::transport::mqtt::crypto::Bundle;
use crate::transport::mqtt::pairing::PairingApiError;
use crate::transport::mqtt::pairing::client::ApiClient;

/// Structure to create an authenticated [`Transport`]
#[derive(Debug)]
pub(crate) struct TransportProvider {}

impl TransportProvider {
    /// Config the TLS for the transport.
    ///
    /// It  will be passed to the MQTT connection.
    pub(crate) fn config_transport<S>(
        state: &ConnectionState<S>,
        client_auth: ClientAuth,
    ) -> Result<Transport, Error<PairingApiError>> {
        let config = client_auth.tls_config(state.tls().clone())?;

        Ok(Transport::tls_with_config(
            rumqttc::TlsConfiguration::Rustls(Arc::new(config)),
        ))
    }

    /// Retrieves an already stored certificate or creates a new one
    ///
    /// It also verifies certificate validity.
    pub(crate) async fn retrieve_credentials(
        client: &ApiClient<'_>,
        client_id: ClientId<&str>,
        store_dir: Option<&Path>,
    ) -> Result<Option<ClientAuth>, Error<PairingApiError>> {
        debug!("retrieving credentials");

        let auth = Self::read_credentials(client_id, store_dir).await;

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
    pub(crate) async fn create_credentials<S>(
        state: &ConnectionState<S>,
        client: &ApiClient<'_>,
        client_id: ClientId<&str>,
    ) -> Result<ClientAuth, Error<PairingApiError>> {
        debug!("creating new transport credentials");

        let (bundle, certificate) = Self::create_certificate(client).await?;

        // If no store dir is set we just create a new certificate
        if let Some(store_dir) = &state.config().writable_dir {
            debug!("storing credentials");

            let certificate_file = CertificateFile::new(store_dir);
            let private_key_file = PrivateKeyFile::new(store_dir);

            Self::store_credentials(
                &private_key_file,
                &bundle.private_key,
                &certificate_file,
                &certificate,
            )
            .await
        }

        ClientAuth::try_from_pem_cert(certificate, bundle.private_key, client_id)
            .wrap_err_with(|err| {
                Error::with(
                    PairingApiError::Io(err.kind()),
                    "reading credentials certificate",
                )
            })
            .and_then(|auth| {
                auth.ok_or_else(|| {
                    Error::with(
                        PairingApiError::InvalidArgument,
                        "while parsing client auth",
                    )
                })
            })
    }

    /// Create the certificate using the Astarte API
    async fn create_certificate(
        client: &ApiClient<'_>,
    ) -> Result<(Bundle, String), Error<PairingApiError>> {
        let bundle = Bundle::generate_key(&client.client_id).map_kind(PairingApiError::Crypto)?;
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
        private_key_file: &PrivateKeyFile,
        private_key: &PrivatePkcs8KeyDer<'_>,
        certificate_file: &CertificateFile,
        certificate: &str,
    ) {
        let store_cert =
            safe_write_private(certificate_file.as_ref(), certificate.as_bytes()).await;
        let store_key =
            safe_write_private(private_key_file.as_ref(), private_key.secret_pkcs8_der()).await;

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
    async fn read_credentials(
        client_id: ClientId<&str>,
        store_dir: Option<&Path>,
    ) -> Option<ClientAuth> {
        let Some(store_dir) = store_dir else {
            debug!("no store directory");

            return None;
        };

        debug!("reading existing credentials from {}", store_dir.display());

        let certificate_file = CertificateFile::new(store_dir);
        let private_key_file = PrivateKeyFile::new(store_dir);

        ClientAuth::try_read(certificate_file, private_key_file, client_id).await
    }
}

pub(crate) async fn safe_write_private(path: &Path, src: &[u8]) -> io::Result<()> {
    let tmp_dir = tempfile::Builder::new().prefix(".astarte-sdk").tempdir()?;

    let tmp_file = tmp_dir.path().join("tmpfile.tmp");

    let mut file = tokio::fs::File::options();
    file.create(true).write(true).truncate(true);

    #[cfg(unix)]
    file.mode(0o600);

    let mut file = file.open(&tmp_file).await?;

    file.write_all(src).await?;
    file.sync_all().await?;

    tokio::fs::rename(tmp_file, path).await?;

    Ok(())
}

// TODO: test the certificate validation fail
#[cfg(test)]
mod tests {

    use std::path::Path;

    use mockito::Server;
    use rumqttc::TlsConfiguration;
    use tempfile::TempDir;
    use url::Url;

    use crate::interfaces::Interfaces;
    use crate::state::ConnStatus;
    use crate::state::tests::mock_state;
    use crate::store::mock::MockStore;
    use crate::transport::mqtt::pairing::client::ClientArgs;
    use crate::transport::mqtt::pairing::client::tests::mock_create_certificate;

    use super::*;

    fn mock_args(url: &Url) -> (ClientId<&str>, ClientArgs<'_>) {
        let client_id = ClientId {
            realm: "realm",
            device_id: "device_id",
        };

        let args = ClientArgs {
            client_id: ClientId {
                realm: "realm",
                device_id: "device_id",
            },
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
    async fn should_create_transport() {
        let dir = TempDir::new().unwrap();

        let mut server = Server::new_async().await;

        let mock = mock_create_certificate(&mut server)
            .expect(1)
            .create_async()
            .await;

        let url = server.url().parse().unwrap();
        let (client_id, args) = mock_args(&url);

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);

        let mut state = mock_state(MockStore::new(), status_tx, Interfaces::new());
        state.config.writable_dir = Some(dir.path().to_path_buf());
        let state = ConnectionState::new(Arc::new(state));

        let api = ApiClient::create(args, state.config(), astarte_device_tls::config().unwrap())
            .expect("failed to create api client");

        let auth = TransportProvider::create_credentials(&state, &api, client_id)
            .await
            .unwrap();
        let transport = TransportProvider::config_transport(&state, auth).unwrap();

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

        let url = server.url().parse().unwrap();

        let (client_id, args) = mock_args(&url);

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);

        let state = ConnectionState::new(Arc::new(mock_state(
            MockStore::new(),
            status_tx,
            Interfaces::new(),
        )));

        let api = ApiClient::create(args, state.config(), astarte_device_tls::config().unwrap())
            .expect("failed to create api client");

        let auth = TransportProvider::create_credentials(&state, &api, client_id)
            .await
            .unwrap();
        let transport = TransportProvider::config_transport(&state, auth).unwrap();

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

        let url = server.url().parse().unwrap();
        let (client_id, args) = mock_args(&url);

        let (status_tx, _status_rx) = tokio::sync::watch::channel(ConnStatus::Online);

        let mut state = mock_state(MockStore::new(), status_tx, Interfaces::new());
        state.config.writable_dir = Some(dir.path().join("non existing"));
        let state = ConnectionState::new(Arc::new(state));

        let api = ApiClient::create(args, state.config(), astarte_device_tls::config().unwrap())
            .expect("failed to create api client");

        let auth = TransportProvider::create_credentials(&state, &api, client_id)
            .await
            .unwrap();
        let transport = TransportProvider::config_transport(&state, auth).unwrap();

        assert!(matches!(
            transport,
            Transport::Tls(TlsConfiguration::Rustls(..))
        ));

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn should_write_safe_private() {
        let tmp = TempDir::new().unwrap();

        let file = tmp.path().join("file");
        let exp = "Hello, world!";

        safe_write_private(&file, exp.as_bytes()).await.unwrap();

        let content = tokio::fs::read_to_string(&file).await.unwrap();
        assert_eq!(content, exp);

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            let mode = tokio::fs::metadata(&file).await.unwrap().mode();

            // remove selinux etc
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
