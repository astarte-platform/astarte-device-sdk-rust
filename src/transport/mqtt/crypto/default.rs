/*
 * This file is part of Astarte.
 *
 * Copyright 2021-2025 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

//! Crypto module to generate the CSR to authenticate the device to the Astarte.

use crate::error::Report;
use crate::transport::mqtt::config::PrivateKeyFile;
use crate::transport::mqtt::crypto::{CryptoError, CryptoProvider};
use crate::transport::mqtt::PairingError;
#[cfg(feature = "openssl")]
#[cfg_attr(docsrs, doc(cfg(feature = "openssl")))]
pub use openssl;
#[cfg(feature = "openssl")]
use openssl::{
    ec::{EcGroup, EcKey},
    hash::MessageDigest,
    nid::Nid,
    pkey::PKey,
    x509::{X509NameBuilder, X509ReqBuilder},
};
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, PKCS_ECDSA_P384_SHA384};
use rustls::client::WantsClientCert;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use rustls::{ClientConfig, RootCertStore};
use std::path::Path;
use std::{fs, io, path::PathBuf};
use tracing::{debug, error};

pub struct DefaultCryptoProvider;

impl CryptoProvider for DefaultCryptoProvider {
    type Bundle = PrivatePkcs8KeyDer<'static>;
    fn create_csr(
        &self,
        realm: &str,
        device_id: &str,
    ) -> Result<(String, Self::Bundle), CryptoError> {
        if cfg!(feature = "openssl") {
            #[cfg(feature = "openssl")]
            return Self::create_csr_openssl(realm, device_id);
        }

        Self::create_csr_def(realm, device_id)
    }

    async fn read_bundle(
        &self,
        store_dir: PathBuf,
    ) -> Result<(Vec<CertificateDer<'static>>, Self::Bundle), io::Error> {
        tokio::task::spawn_blocking(move || {
            let certificate = Self::read_certificate(&store_dir)?;

            let key = PrivateKeyFile::new(store_dir);
            let k_r = std::fs::read(key)?;
            if k_r.is_empty() {
                debug!("no private key found");

                return Err(io::Error::new(io::ErrorKind::Other, "no private key found"));
            }

            let private_key = PrivatePkcs8KeyDer::from(k_r);

            Ok((certificate, private_key))
        })
        .await?
    }

    async fn store_bundle(&self, store_dir: &Path, bundle: &Self::Bundle) {
        let private_key_file = PrivateKeyFile::new(store_dir);

        let private_key = &bundle;
        if let Err(err) = fs::write(&private_key_file, private_key.secret_pkcs8_der()) {
            error!(error = %Report::new(err), file = %private_key_file, "couldn't write private key file");
        }
    }

    async fn insecure_tls_config(
        &self,
        builder: rustls::ConfigBuilder<ClientConfig, WantsClientCert>,
        certificates: Vec<CertificateDer<'static>>,
        item: Self::Bundle,
    ) -> Result<rustls::ClientConfig, PairingError> {
        builder
            .with_client_auth_cert(certificates, item.into())
            .map_err(PairingError::Tls)
    }

    async fn tls_config(
        &self,
        roots: RootCertStore,
        certificates: Vec<CertificateDer<'static>>,
        item: Self::Bundle,
    ) -> Result<rustls::ClientConfig, PairingError> {
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, item.into())
            .map_err(PairingError::Tls)
    }
}

impl DefaultCryptoProvider {
    pub fn new() -> Result<Self, CryptoError> {
        Ok(Self {})
    }

    fn create_csr_def(
        realm: &str,
        device_id: &str,
    ) -> Result<(String, PrivatePkcs8KeyDer<'static>), CryptoError> {
        // The realm/device_id for the certificate
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, format!("{}/{}", realm, device_id));

        // Generate a random private key
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P384_SHA384)?;

        let mut csr_param = CertificateParams::new([])?;
        csr_param.distinguished_name = dn;

        // Singed CSR
        let csr = csr_param.serialize_request(&key_pair)?.pem()?;
        // Subject key_pair
        let private_key = PrivatePkcs8KeyDer::from(key_pair.serialize_der());

        Ok((csr, private_key))
    }

    #[cfg(feature = "openssl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "openssl")))]
    fn create_csr_openssl(
        realm: &str,
        device_id: &str,
    ) -> Result<(String, PrivatePkcs8KeyDer<'static>), CryptoError> {
        let group = EcGroup::from_curve_name(Nid::SECP384R1)?;
        let ec_key = EcKey::generate(&group)?;

        let mut subject_builder = X509NameBuilder::new()?;
        subject_builder.append_entry_by_nid(Nid::COMMONNAME, &format!("{realm}/{device_id}"))?;

        let subject_name = subject_builder.build();

        let pkey = PKey::from_ec_key(ec_key)?;
        let mut req_builder = X509ReqBuilder::new()?;
        req_builder.set_pubkey(&pkey)?;
        req_builder.set_subject_name(&subject_name)?;
        req_builder.sign(&pkey, MessageDigest::sha512())?;
        let pkey_bytes = pkey.private_key_to_pkcs8()?;

        let csr_bytes = req_builder.build().to_pem()?;

        Ok((
            String::from_utf8(csr_bytes)?,
            PrivatePkcs8KeyDer::from(pkey_bytes),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::transport::mqtt::crypto::default::DefaultCryptoProvider;
    use crate::transport::mqtt::crypto::CryptoProvider;

    #[test]
    fn test_new_cert() {
        let crypto_provider = DefaultCryptoProvider::new().unwrap();
        let bundle = crypto_provider.create_csr("realm", "device_id");

        assert!(
            bundle.is_ok(),
            "Failed to generate certificate: {}",
            bundle.unwrap_err()
        );

        let bundle = bundle.unwrap();

        assert!(!bundle.1.secret_pkcs8_der().is_empty());
        assert!(!bundle.0.is_empty());
    }

    #[test]
    fn test_bundle() {
        let crypto_provider = DefaultCryptoProvider::new().unwrap();
        let (csr, private_key) = crypto_provider.create_csr("realm", "device_id").unwrap();

        assert!(!private_key.secret_pkcs8_der().is_empty());
        assert!(!csr.is_empty());

        rustls_pemfile::csr(&mut csr.clone().as_bytes())
            .unwrap()
            .unwrap();
    }

    #[cfg(feature = "openssl")]
    #[test]
    fn test_bundle_sanity_test() {
        let crypto_provider = DefaultCryptoProvider::new().unwrap();
        let (csr, private_key) = crypto_provider.create_csr("realm", "device_id").unwrap();

        assert!(!private_key.secret_pkcs8_der().is_empty());
        assert!(!csr.is_empty());

        let private_key =
            openssl::pkey::PKey::private_key_from_der(private_key.secret_pkcs8_der()).unwrap();
        let csr = openssl::x509::X509Req::from_pem(csr.as_bytes()).unwrap();

        assert!(csr.verify(&private_key).unwrap());

        let subject_name = csr.subject_name();
        let cn = subject_name
            .entries_by_nid(openssl::nid::Nid::COMMONNAME)
            .next()
            .unwrap();
        assert_eq!(cn.data().as_slice(), b"realm/device_id");
    }

    #[cfg(feature = "openssl")]
    #[test]
    fn test_bundle_openssl() {
        let crypto_provider = DefaultCryptoProvider::new().unwrap();
        let (csr, private_key) = crypto_provider.create_csr("realm", "device_id").unwrap();

        assert!(!private_key.secret_pkcs8_der().is_empty());
        assert!(!csr.is_empty());

        let private_key =
            openssl::pkey::PKey::private_key_from_der(private_key.secret_pkcs8_der()).unwrap();
        let csr = openssl::x509::X509Req::from_pem(csr.as_bytes()).unwrap();

        assert!(csr.verify(&private_key).unwrap());

        let subject_name = csr.subject_name();
        let cn = subject_name
            .entries_by_nid(openssl::nid::Nid::COMMONNAME)
            .next()
            .unwrap();
        assert_eq!(cn.data().as_slice(), b"realm/device_id");
    }
}
