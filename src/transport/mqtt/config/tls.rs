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

//! MQTT TLS configuration

use super::{CertificateFile, PrivateKeyFile};
use std::path::PathBuf;
use std::{
    fs::File,
    io::{self, BufReader},
    sync::Arc,
};

use crate::error::Report;
#[cfg(feature = "keystore-tss")]
use crate::transport::mqtt::tpm::CertResolver;
use crate::transport::mqtt::PairingError;
use itertools::Itertools;
use rustls::{
    pki_types::{CertificateDer, PrivatePkcs8KeyDer},
    RootCertStore,
};
use tokio::fs;
use tracing::{debug, error, warn};

pub(crate) fn is_env_ignore_ssl() -> bool {
    matches!(
        std::env::var("IGNORE_SSL_ERRORS").as_deref(),
        Ok("1" | "true")
    )
}

pub(crate) struct ClientAuth {
    certs: Vec<CertificateDer<'static>>,
    private_key: Option<PrivatePkcs8KeyDer<'static>>,
}

pub(crate) struct ClientAuthBuilder {
    client_auth: ClientAuth,
}

impl ClientAuthBuilder {
    pub fn with_certs(certs: String) -> Result<Self, io::Error> {
        let certs = rustls_pemfile::certs(&mut certs.as_bytes()).try_collect()?;
        Ok(Self {
            client_auth: ClientAuth {
                certs,
                private_key: None,
            },
        })
    }

    pub fn with_key(
        mut self,
        private_key: Option<PrivatePkcs8KeyDer<'static>>,
    ) -> Result<ClientAuth, io::Error> {
        self.client_auth.private_key = private_key;
        Ok(self.client_auth)
    }
}

impl ClientAuth {
    /// Blocking function to read the certificate and the key
    pub(crate) async fn try_read(store_dir: PathBuf) -> Option<Self> {
        let res = tokio::task::spawn_blocking(move || {
            let certificates = CertificateFile::new(&store_dir);
            let c_f = File::open(certificates)?;

            let mut c_r = BufReader::new(c_f);
            let certs: Vec<CertificateDer<'static>> =
                rustls_pemfile::certs(&mut c_r).try_collect()?;

            if certs.is_empty() {
                debug!("no certificate found");

                return Err(io::Error::new(io::ErrorKind::Other, "no certificate found"));
            }

            let private_key = if cfg!(feature = "keystore-none") {
                let key = PrivateKeyFile::new(store_dir);
                let k_r = std::fs::read(key)?;
                if k_r.is_empty() {
                    debug!("no private key found");

                    return Err(io::Error::new(io::ErrorKind::Other, "no private key found"));
                }

                Some(PrivatePkcs8KeyDer::from(k_r))
            } else {
                None
            };

            Ok(ClientAuth { certs, private_key })
        })
        .await
        .ok()?;

        match res {
            Ok(auth) => Some(auth),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                debug!("credential files are missing");

                None
            }
            Err(err) => {
                error!("couldn't read certificates {err}");

                None
            }
        }
    }

    /// Store the credentials to files.
    pub(crate) async fn store(&self, store_dir: &Option<PathBuf>, certificate: &str) {
        // Don't fail here since the SDK can always regenerate the certificate

        if let Some(store_dir) = store_dir {
            let certificate_file = CertificateFile::new(store_dir);
            let private_key_file = PrivateKeyFile::new(store_dir);

            if let Err(err) = fs::write(&certificate_file, certificate).await {
                error!(error = %Report::new(&err), file = %certificate_file, "couldn't write certificate file");
            }

            if let Some(private_key) = &self.private_key {
                if let Err(err) = fs::write(&private_key_file, private_key.secret_pkcs8_der()).await
                {
                    error!(error = %Report::new(err), file = %private_key_file, "couldn't write private key file");
                }
            }
        }
    }

    pub(crate) async fn tls_config(self) -> Result<rustls::ClientConfig, PairingError> {
        let roots = read_root_cert_store().await?;

        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);

        if cfg!(feature = "keystore-tss") {
            #[cfg(feature = "keystore-tss")]
            return Ok(
                builder.with_client_cert_resolver(Arc::new(CertResolver { certs: self.certs }))
            );
        }

        let private_key = self
            .private_key
            .ok_or_else(|| "key not found".to_string())
            .map_err(PairingError::Config)?;

        builder
            .with_client_auth_cert(self.certs, private_key.into())
            .map_err(PairingError::Tls)
    }

    pub(crate) async fn insecure_tls_config(self) -> Result<rustls::ClientConfig, PairingError> {
        warn!("INSECURE: ignore TLS certificates");

        let private_key = self
            .private_key
            .ok_or_else(|| "key not found".to_string())
            .map_err(PairingError::Config)?;

        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier {}))
            .with_client_auth_cert(self.certs, private_key.into())
            .map_err(PairingError::Tls)
    }
}

async fn read_root_cert_store() -> Result<RootCertStore, PairingError> {
    tokio::task::spawn_blocking(|| {
        let mut root_cert_store = RootCertStore::empty();

        let native_certs =
            rustls_native_certs::load_native_certs().map_err(PairingError::Native)?;

        for cert in native_certs {
            root_cert_store.add(cert).map_err(PairingError::Tls)?;
        }

        Ok(root_cert_store)
    })
    .await?
}

#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA1,
            rustls::SignatureScheme::ECDSA_SHA1_Legacy,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ED448,
        ]
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use tempfile::TempDir;

    use super::*;

    pub(crate) const TEST_CERTIFICATE: &str = include_str!("../../../../tests/certificate.pem");
    pub(crate) const TEST_PRIVATE_KEY: &[u8] = include_bytes!("../../../../tests/priv-key.der");

    #[tokio::test]
    async fn should_read_keys() {
        let dir = TempDir::new().unwrap();

        let cert = CertificateFile::new(dir.path());
        tokio::fs::write(&cert, TEST_CERTIFICATE).await.unwrap();

        let key = PrivateKeyFile::new(dir.path());
        tokio::fs::write(&key, TEST_PRIVATE_KEY).await.unwrap();

        let client = ClientAuth::try_read(dir.path().to_path_buf())
            .await
            .unwrap();

        assert_eq!(client.certs.len(), 1);

        let cert_der = client.certs.first().unwrap();

        let rustls_pemfile::Item::X509Certificate(exp) =
            rustls_pemfile::read_one_from_slice(TEST_CERTIFICATE.as_bytes())
                .unwrap()
                .unwrap()
                .0
        else {
            panic!("expected cert");
        };

        assert_eq!(*cert_der, exp);

        if let Some(private_key) = &client.private_key {
            assert_eq!(private_key.secret_pkcs8_der(), TEST_PRIVATE_KEY);
        } else {
            panic!("expected private key");
        }

        client.tls_config().await.unwrap();

        // Reuse the file setup
        let client = ClientAuth::try_read(dir.into_path()).await.unwrap();

        client.insecure_tls_config().await.unwrap();
    }
}
