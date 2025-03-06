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

use std::{
    fs::File,
    io::{self, BufReader},
    sync::Arc,
};

use itertools::Itertools;
use rustls::{
    client::WantsClientCert,
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer},
    ClientConfig, ConfigBuilder, RootCertStore,
};
use tracing::{debug, error, instrument, warn};

use crate::transport::mqtt::PairingError;

use super::{CertificateFile, PrivateKeyFile};

pub(crate) fn is_env_ignore_ssl() -> bool {
    matches!(
        std::env::var("IGNORE_SSL_ERRORS").as_deref(),
        Ok("1" | "true")
    )
}

pub(crate) struct ClientAuth {
    certs: Vec<CertificateDer<'static>>,
    private_key: PrivatePkcs8KeyDer<'static>,
}

impl ClientAuth {
    pub(crate) async fn try_read(
        certificates: CertificateFile,
        key: PrivateKeyFile,
    ) -> Option<Self> {
        let res = tokio::task::spawn_blocking(|| Self::read_cert_and_key(certificates, key))
            .await
            .ok()?;

        match res {
            Ok(auth) => auth,
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

    pub(crate) fn try_from_pem_cert(
        certs: String,
        private_key: PrivatePkcs8KeyDer<'static>,
    ) -> Result<Self, io::Error> {
        let certs = rustls_pemfile::certs(&mut certs.as_bytes()).try_collect()?;

        Ok(Self { certs, private_key })
    }

    /// Blocking function to read the certificate and the key
    fn read_cert_and_key(
        certificates: CertificateFile,
        key: PrivateKeyFile,
    ) -> Result<Option<Self>, io::Error> {
        let c_f = File::open(certificates)?;

        let mut c_r = BufReader::new(c_f);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut c_r).try_collect()?;

        if certs.is_empty() {
            debug!("no certificate found");

            return Ok(None);
        }

        let k_r = std::fs::read(key)?;
        if k_r.is_empty() {
            debug!("no private key found");

            return Ok(None);
        }
        let private_key = PrivatePkcs8KeyDer::from(k_r);

        Ok(Some(ClientAuth { private_key, certs }))
    }

    pub(crate) fn tls_config(
        self,
        roots: Arc<RootCertStore>,
    ) -> Result<rustls::ClientConfig, PairingError> {
        tls_config_builder(roots)?
            .with_client_auth_cert(self.certs, self.private_key.into())
            .map_err(PairingError::Tls)
    }

    pub(crate) fn insecure_tls_config(self) -> Result<rustls::ClientConfig, PairingError> {
        warn!("INSECURE: ignore TLS certificates");

        insecure_tls_config_builder()?
            .with_client_auth_cert(self.certs, self.private_key.into())
            .map_err(PairingError::Tls)
    }
}

#[cfg(feature = "webpki")]
#[instrument]
pub(crate) async fn read_root_cert_store() -> Result<RootCertStore, PairingError> {
    debug!("reading root cert store from webpki");

    let root_cert_store = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };

    Ok(root_cert_store)
}

#[cfg(not(feature = "webpki"))]
#[instrument]
pub(crate) async fn read_root_cert_store() -> Result<RootCertStore, PairingError> {
    debug!("reading root cert store from native certs");

    tokio::task::spawn_blocking(|| {
        let mut root_cert_store = RootCertStore::empty();

        let res = rustls_native_certs::load_native_certs();
        for err in res.errors {
            error!(error = %crate::error::Report::new(err), "couldn't load root certificate");
        }

        let (added, ignored) = root_cert_store.add_parsable_certificates(res.certs);

        tracing::trace!("loaded {added} certs and {ignored} ignored");

        root_cert_store
    })
    .await
    .map_err(PairingError::ReadNativeCerts)
}

pub(crate) fn tls_config_builder(
    roots: Arc<RootCertStore>,
) -> Result<ConfigBuilder<ClientConfig, WantsClientCert>, PairingError> {
    let provider = CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));

    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(PairingError::Tls)?
        .with_root_certificates(roots);

    Ok(builder)
}

pub(crate) fn insecure_tls_config_builder(
) -> Result<ConfigBuilder<ClientConfig, WantsClientCert>, PairingError> {
    let provider = CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));

    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .map_err(PairingError::Tls)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier {}));

    Ok(builder)
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

        let client = ClientAuth::try_read(cert.clone(), key.clone())
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

        assert_eq!(client.private_key.secret_pkcs8_der(), TEST_PRIVATE_KEY);

        let root_cert_store = Arc::new(rustls::RootCertStore::empty());
        client.tls_config(root_cert_store).unwrap();

        // Reuse the file setup
        let client = ClientAuth::try_read(cert, key).await.unwrap();

        client.insecure_tls_config().unwrap();
    }
}
