/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
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

use std::str::FromStr;

#[cfg(feature = "openssl")]
use openssl::{
    ec::{EcGroup, EcKey},
    hash::MessageDigest,
    nid::Nid,
    pkey::PKey,
    x509::{X509NameBuilder, X509ReqBuilder},
};
use p384::{
    ecdsa::{DerSignature, SigningKey},
    pkcs8::{EncodePrivateKey, LineEnding},
    SecretKey,
};
use rustls::PrivateKey;
use x509_cert::{
    builder::{Builder, RequestBuilder},
    der::EncodePem,
    name::Name,
};

/// Errors that can occur while generating the Certificate and CSR.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum CryptoError {
    #[cfg(feature = "openssl")]
    #[error("Openssl error")]
    Openssl(#[from] openssl::error::ErrorStack),

    #[error("Invalid COMMONNAME")]
    InvalidCn(#[from] x509_cert::der::Error),

    #[error("Failed to create Certificate and CSR")]
    Certificate(#[from] x509_cert::builder::Error),

    #[error("Failed to encode key to PKCS8 pem")]
    Pem(#[from] p384::pkcs8::Error),

    #[error("Invalid UTF-8 encoded PEM")]
    Utf8(#[from] std::string::FromUtf8Error),
}

/// Generate a Certificate and CSR bundle in PEM format.
#[derive(Debug)]
pub(crate) struct Bundle {
    pub private_key: PrivateKey,
    /// PEM encoded CSR
    pub csr: String,
}

impl Bundle {
    pub(crate) fn new(realm: &str, device_id: &str) -> Result<Bundle, CryptoError> {
        // This is written this way so when all features are enable, the generate_key function is
        // not marked as unused. The if will be optimized out by the compiler in release.
        if cfg!(feature = "openssl") {
            #[cfg(feature = "openssl")]
            return Self::openssl_key(realm, device_id);
        }

        Self::generate_key(realm, device_id)
    }

    pub(crate) fn generate_key(realm: &str, device_id: &str) -> Result<Bundle, CryptoError> {
        // The realm/device_id for the certificate
        let subject = Name::from_str(&format!("CN={}/{}", realm, device_id))?;

        // Generate a random private key
        let private_key = SecretKey::random(&mut rand_core::OsRng);

        // This need to define the signer key type
        let signer = SigningKey::from(&private_key);

        let csr = RequestBuilder::new(subject, &signer)?
            .build::<DerSignature>()?
            // Platform dependant line ending
            .to_pem(LineEnding::default())?;

        // The private_key is held into a Zeroing wrapper as a security measure so that the key is
        // note kept in memory. We nee to extract the key from the wrapper to use it normally.
        let mut z_private_key = private_key.to_pkcs8_der()?.to_bytes();
        // Replace the private_key with an empty string to take it out without cloning it.
        let private_key = PrivateKey(std::mem::take(&mut z_private_key));

        Ok(Bundle { private_key, csr })
    }

    #[cfg(feature = "openssl")]
    pub(crate) fn openssl_key(realm: &str, device_id: &str) -> Result<Bundle, CryptoError> {
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

        Ok(Bundle {
            private_key: PrivateKey(pkey_bytes),
            csr: String::from_utf8(csr_bytes)?,
        })
    }
}

#[cfg(not(taurpaulin_include))]
#[doc(hidden)]
pub mod bench {
    use rustls::PrivateKey;

    use super::Bundle;

    pub fn generate_key(realm: &str, device: &str) -> (PrivateKey, String) {
        let Bundle { private_key, csr } =
            Bundle::generate_key(realm, device).expect("Failed to generate key");

        (private_key, csr)
    }

    #[cfg(feature = "openssl")]
    pub fn openssl_key(realm: &str, device: &str) -> (PrivateKey, String) {
        let Bundle { private_key, csr } =
            Bundle::openssl_key(realm, device).expect("Failed to generate key");

        (private_key, csr)
    }
}

#[cfg(test)]
mod tests {
    use ecdsa::SigningKey;
    use p384::{
        ecdsa::{signature::Verifier, Signature, VerifyingKey},
        pkcs8::DecodePrivateKey,
        SecretKey,
    };
    use x509_cert::{
        der::{DecodePem, Encode},
        request::CertReq,
    };

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

        assert!(!bundle.private_key.0.is_empty());
        assert!(!bundle.csr.is_empty());
    }

    #[test]
    fn test_bundle() {
        let Bundle {
            private_key: PrivateKey(private_key),
            csr,
        } = Bundle::generate_key("realm", "device_id").unwrap();
        assert!(!private_key.is_empty());
        assert!(!csr.is_empty());

        let csr = CertReq::from_pem(csr.as_bytes()).unwrap();
        assert_eq!(csr.info.subject.to_string(), "CN=realm/device_id");

        let private_key = SecretKey::from_pkcs8_der(&private_key).unwrap();
        let signer = SigningKey::from(&private_key);
        let verifier = VerifyingKey::from(signer);

        let sign_bytes = csr
            .signature
            .as_bytes()
            .expect("Failed to get signature bytes");
        let signature: Signature = Signature::from_der(sign_bytes).expect("Failed to decode DER");

        let info = csr.info.to_der().expect("Failed to encode CSR info");

        assert!(verifier.verify(&info, &signature).is_ok());
    }

    #[cfg(feature = "openssl")]
    #[test]
    fn test_bundle_sanity_test() {
        // This will check both implementation are compatible
        let Bundle {
            private_key: PrivateKey(private_key),
            csr,
        } = Bundle::generate_key("realm", "device_id").unwrap();
        assert!(!private_key.is_empty());
        assert!(!csr.is_empty());

        let private_key = openssl::pkey::PKey::private_key_from_der(&private_key).unwrap();
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
        let Bundle {
            private_key: PrivateKey(private_key),
            csr,
        } = Bundle::openssl_key("realm", "device_id").unwrap();
        assert!(!private_key.is_empty());
        assert!(!csr.is_empty());

        let private_key = openssl::pkey::PKey::private_key_from_der(&private_key).unwrap();
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
