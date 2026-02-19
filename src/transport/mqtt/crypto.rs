// This file is part of Astarte.
//
// Copyright 2021-2026 SECO Mind Srl
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

//! Crypto module to generate the CSR to authenticate the device to the Astarte.

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, PKCS_ECDSA_P256_SHA256};
use rustls::pki_types::PrivatePkcs8KeyDer;
use tracing::warn;

// TODO: remove in next major version
#[allow(unused)]
type OpensslError =
    Box<dyn std::error::Error + Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe>;

/// Errors that can occur while generating the Certificate and CSR.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum CryptoError {
    #[cfg(feature = "openssl")]
    #[cfg_attr(astarte_device_sdk_docsrs, doc(cfg(feature = "openssl")))]
    #[doc(hidden)]
    /// Openssl CSR generation failed.
    #[error("Openssl error")]
    #[deprecated(since = "0.9.5", note = "Openssl is no longer a dependency")]
    Openssl(OpensslError),
    /// Failed to generate the CSR.
    #[error("Failed to create Certificate and CSR")]
    Certificate(#[from] rcgen::Error),
    /// Invalid UTF-8 character in the PEM file.
    #[error("Invalid UTF-8 encoded PEM")]
    Utf8(#[from] std::string::FromUtf8Error),
}

/// Generate a Certificate and CSR bundle in PEM format.
#[derive(Debug)]
pub(crate) struct Bundle {
    pub private_key: PrivatePkcs8KeyDer<'static>,
    /// PEM encoded CSR
    pub csr: String,
}

impl Bundle {
    pub(crate) fn new(realm: &str, device_id: &str) -> Result<Bundle, CryptoError> {
        if cfg!(feature = "openssl") {
            warn!("the 'openssl' feature is deprecated and will be removed in a future version");
        }

        Self::generate_key(realm, device_id)
    }

    pub(crate) fn generate_key(realm: &str, device_id: &str) -> Result<Bundle, CryptoError> {
        // The realm/device_id for the certificate
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, format!("{realm}/{device_id}"));

        // Generate a random private key
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;

        let mut csr_param = CertificateParams::new([])?;
        csr_param.distinguished_name = dn;

        // Singed CSR
        let csr = csr_param.serialize_request(&key_pair)?.pem()?;
        // Subject key_pair
        let private_key = PrivatePkcs8KeyDer::from(key_pair.serialize_der());

        Ok(Bundle { private_key, csr })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cert() {
        let bundle = Bundle::new("realm", "device_id");

        assert!(
            bundle.is_ok(),
            "Failed to generate certificate: {}",
            bundle.unwrap_err()
        );

        let bundle = bundle.unwrap();

        assert!(!bundle.private_key.secret_pkcs8_der().is_empty());
        assert!(!bundle.csr.is_empty());

        rustls_pemfile::csr(&mut bundle.csr.clone().as_bytes())
            .unwrap()
            .unwrap();
    }

    #[test]
    fn test_bundle() {
        let Bundle { private_key, csr } = Bundle::generate_key("realm", "device_id").unwrap();
        assert!(!private_key.secret_pkcs8_der().is_empty());
        assert!(!csr.is_empty());

        rustls_pemfile::csr(&mut csr.clone().as_bytes())
            .unwrap()
            .unwrap();
    }
}
