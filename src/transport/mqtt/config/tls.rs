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

use std::{io, sync::Arc};

use chrono::{DateTime, Utc};
use rustls::{
    client::WantsClientCert,
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer},
    ClientConfig, ConfigBuilder, RootCertStore,
};
use tracing::{debug, error, info, instrument, warn};
use x509_parser::prelude::X509Certificate;

use crate::{
    error::Report,
    logging::security::{notify_security_event, SecurityEvent},
    transport::mqtt::PairingError,
};

use super::{CertificateFile, ClientId, PrivateKeyFile};

pub(crate) fn is_env_ignore_ssl() -> bool {
    matches!(
        std::env::var("IGNORE_SSL_ERRORS").as_deref(),
        Ok("1" | "true")
    )
}

pub(crate) struct ClientAuth {
    pem: String,
    der: CertificateDer<'static>,
    private_key: PrivatePkcs8KeyDer<'static>,
}

impl ClientAuth {
    pub(crate) async fn try_read(
        certificates: CertificateFile,
        key: PrivateKeyFile,
        client_id: ClientId<&str>,
    ) -> Option<Self> {
        let res = Self::read_cert_and_key(certificates, key, client_id).await;

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

    /// Function to read the certificate and the key
    async fn read_cert_and_key(
        certificates: CertificateFile,
        key: PrivateKeyFile,
        client_id: ClientId<&str>,
    ) -> Result<Option<Self>, io::Error> {
        let pem = tokio::fs::read_to_string(certificates.path()).await?;

        let k_r = tokio::fs::read(key).await?;
        if k_r.is_empty() {
            debug!("no private key found");
            return Ok(None);
        }
        let private_key = PrivatePkcs8KeyDer::from(k_r);

        Self::try_from_pem_cert(pem, private_key, client_id)
    }

    pub(crate) fn try_from_pem_cert(
        pem: String,
        private_key: PrivatePkcs8KeyDer<'static>,
        client_id: ClientId<&str>,
    ) -> Result<Option<Self>, io::Error> {
        let bytes = &mut pem.as_bytes();
        let mut certificates = rustls_pemfile::certs(bytes);
        let Some(cert) = certificates.next() else {
            warn!("expected one certificate in the chain");
            return Ok(None);
        };

        if certificates.next().is_some() {
            warn!("expected exactly one certificate in the chain, found more");
            return Ok(None);
        }

        drop(certificates);

        cert.map(|cert| {
            let auth = Self {
                der: cert,
                private_key,
                pem,
            };

            let auth = auth.verify_certificate_data(client_id).then_some(auth);

            if auth.is_none() {
                notify_security_event(SecurityEvent::CertificateValidationFailed);
            }

            auth
        })
    }

    fn verify_certificate_data(&self, client_id: ClientId<&str>) -> bool {
        let parsed = match x509_parser::parse_x509_certificate(&self.der) {
            Ok((_remaining, cert)) => cert,
            Err(e) => {
                warn!(error=%Report::new(e), "parsing certificate error assuming invalid");
                return false;
            }
        };

        // NOTE this validity check is performed using the local datetime and it's not relevant to the actual validity check
        if !parsed.validity.is_valid() {
            notify_security_event(SecurityEvent::AlarmExpiredCertificate);
            notify_security_event(SecurityEvent::CertificateValidationFailedExpired);
        }

        self.verify_certificate_subject(&parsed, client_id)
    }

    fn verify_certificate_subject(
        &self,
        cert: &X509Certificate<'_>,
        client_id: ClientId<&str>,
    ) -> bool {
        let Some(subject_cn) = cert.subject.iter_common_name().next() else {
            warn!("no subject common name assuming invalid");
            return false;
        };

        let subject_cn_str = subject_cn.as_str();
        info!("subject common name as str = {:?}", subject_cn_str);

        subject_cn_str.is_ok_and(|subject_cn_str| subject_cn_str == client_id.to_string())
    }

    pub(crate) fn tls_config(
        self,
        roots: Arc<RootCertStore>,
    ) -> Result<rustls::ClientConfig, PairingError> {
        tls_config_builder(roots)?
            .with_client_auth_cert(vec![self.der], self.private_key.into())
            .map_err(PairingError::Tls)
    }

    pub(crate) fn insecure_tls_config(self) -> Result<rustls::ClientConfig, PairingError> {
        warn!("INSECURE: ignore TLS certificates");

        insecure_tls_config_builder()?
            .with_client_auth_cert(vec![self.der], self.private_key.into())
            .map_err(PairingError::Tls)
    }

    /// verify if the certificate will be expired after the specified duration
    /// if the certificate can't be read this method returns None
    pub(crate) fn parse_validity_not_after(&self) -> Option<DateTime<Utc>> {
        let parsed = match x509_parser::parse_x509_certificate(&self.der) {
            Ok((_remaining, cert)) => cert,
            Err(e) => {
                warn!(error=%Report::new(e), "parsing certificate error, assume it is not about to expire");
                return None;
            }
        };

        DateTime::from_timestamp(parsed.validity.not_after.timestamp(), 0)
    }

    pub(crate) fn pem(&self) -> &str {
        &self.pem
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

    use crate::transport::mqtt::{crypto::Bundle, pairing::tests::self_sign_csr_to_pem};

    use super::*;

    pub(crate) const TEST_CERTIFICATE: &str = include_str!("../../../../tests/certificate.pem");
    pub(crate) const TEST_PRIVATE_KEY: &[u8] = include_bytes!("../../../../tests/priv-key.der");
    pub(crate) const TEST_CLIENT_ID: ClientId<&'static str> = ClientId {
        realm: "test",
        device_id: "2TBn-jNESuuHamE2Zo1anA",
    };

    #[tokio::test]
    async fn should_read_keys() {
        let dir = TempDir::new().unwrap();

        let cert = CertificateFile::new(dir.path());
        tokio::fs::write(&cert, TEST_CERTIFICATE).await.unwrap();

        let key = PrivateKeyFile::new(dir.path());
        tokio::fs::write(&key, TEST_PRIVATE_KEY).await.unwrap();

        let client = ClientAuth::try_read(cert.clone(), key.clone(), TEST_CLIENT_ID)
            .await
            .unwrap();

        let cert_der = &client.der;

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
        let client = ClientAuth::try_read(cert, key, TEST_CLIENT_ID)
            .await
            .unwrap();

        client.insecure_tls_config().unwrap();
    }

    #[tokio::test]
    async fn test_valid_certificate() {
        let client_id = ClientId {
            realm: "realm",
            device_id: "device_id",
        };
        let bundle = Bundle::generate_key(client_id.realm, client_id.device_id).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let certificate_file = CertificateFile::new(&dir);
        let private_key_file = PrivateKeyFile::new(&dir);

        let cert = self_sign_csr_to_pem(&bundle.csr);

        tokio::fs::write(certificate_file.path(), cert)
            .await
            .unwrap();
        tokio::fs::write(
            private_key_file.path(),
            bundle.private_key.secret_pkcs8_der(),
        )
        .await
        .unwrap();

        let cert = ClientAuth::try_read(certificate_file, private_key_file, client_id)
            .await
            .unwrap();

        assert!(cert.verify_certificate_data(client_id))
    }

    #[tokio::test]
    async fn test_invalid_certificate_subject_cn() {
        let bundle = Bundle::generate_key(TEST_CLIENT_ID.realm, TEST_CLIENT_ID.device_id).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let certificate_file = CertificateFile::new(&dir);
        let private_key_file = PrivateKeyFile::new(&dir);

        let cert = self_sign_csr_to_pem(&bundle.csr);

        tokio::fs::write(certificate_file.path(), cert)
            .await
            .unwrap();
        tokio::fs::write(
            private_key_file.path(),
            bundle.private_key.secret_pkcs8_der(),
        )
        .await
        .unwrap();

        let diff_client_id = ClientId {
            realm: "realm",
            device_id: "different_device_id",
        };
        let cert = ClientAuth::try_read(certificate_file, private_key_file, diff_client_id).await;

        assert!(cert.is_none())
    }
}
