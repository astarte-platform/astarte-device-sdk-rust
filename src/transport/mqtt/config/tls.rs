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

use std::sync::Arc;

use crate::transport::mqtt::PairingError;
use rustls::client::WantsClientCert;
use rustls::{ClientConfig, ConfigBuilder, RootCertStore};

pub(crate) fn is_env_ignore_ssl() -> bool {
    matches!(
        std::env::var("IGNORE_SSL_ERRORS").as_deref(),
        Ok("1" | "true")
    )
}

pub async fn read_root_cert_store() -> Result<RootCertStore, PairingError> {
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
pub struct NoVerifier;

impl NoVerifier {
    pub(crate) fn init_insecure_tls_config() -> ConfigBuilder<ClientConfig, WantsClientCert> {
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(Self {}))
    }
}

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
    use super::*;
    use crate::transport::mqtt::config::{CertificateFile, PrivateKeyFile};
    use crate::transport::mqtt::crypto::default::DefaultCryptoProvider;
    use crate::transport::mqtt::crypto::CryptoProvider;
    use tempfile::TempDir;

    pub(crate) const TEST_CERTIFICATE: &str = include_str!("../../../../tests/certificate.pem");
    pub(crate) const TEST_PRIVATE_KEY: &[u8] = include_bytes!("../../../../tests/priv-key.der");

    #[tokio::test]
    async fn should_read_keys() {
        let dir = TempDir::new().unwrap();

        let cert = CertificateFile::new(dir.path());
        tokio::fs::write(&cert, TEST_CERTIFICATE).await.unwrap();

        let key = PrivateKeyFile::new(dir.path());
        tokio::fs::write(&key, TEST_PRIVATE_KEY).await.unwrap();

        let crypto_provider = DefaultCryptoProvider::new().unwrap();

        let (certs, private_key) = crypto_provider
            .read_credentials(dir.path().to_path_buf())
            .await
            .unwrap();

        assert_eq!(certs.len(), 1);

        let cert_der = certs.first().unwrap();

        let rustls_pemfile::Item::X509Certificate(exp) =
            rustls_pemfile::read_one_from_slice(TEST_CERTIFICATE.as_bytes())
                .unwrap()
                .unwrap()
                .0
        else {
            panic!("expected cert");
        };

        assert_eq!(*cert_der, exp);

        assert_eq!(private_key.secret_pkcs8_der(), TEST_PRIVATE_KEY);

        crypto_provider
            .tls_config(read_root_cert_store().await.unwrap(), certs, private_key)
            .await
            .unwrap();

        // Reuse the file setup
        let (certs, private_key) = crypto_provider
            .read_credentials(dir.path().to_path_buf())
            .await
            .unwrap();

        crypto_provider
            .insecure_tls_config(NoVerifier::init_insecure_tls_config(), certs, private_key)
            .await
            .unwrap();
    }
}
