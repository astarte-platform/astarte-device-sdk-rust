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

pub(crate) mod default;
#[cfg(feature = "keystore-tss")]
pub(crate) mod tpm;

use itertools::Itertools;
#[cfg(feature = "openssl")]
use openssl::{
    ec::{EcGroup, EcKey},
    hash::MessageDigest,
    nid::Nid,
    pkey::PKey,
    x509::{X509NameBuilder, X509ReqBuilder},
};
use rustls::pki_types::CertificateDer;
use std::path::Path;
use std::{
    fs,
    fs::File,
    io::{self, BufReader},
    path::PathBuf,
};

use crate::error::Report;
use crate::transport::mqtt::config::CertificateFile;
use crate::transport::mqtt::PairingError;
#[cfg(feature = "openssl")]
#[cfg_attr(docsrs, doc(cfg(feature = "openssl")))]
pub use openssl;
use rustls::client::WantsClientCert;
use rustls::{ClientConfig, RootCertStore};
use tracing::{debug, error};

/// Errors that can occur while generating the Certificate and CSR.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum CryptoError {
    #[cfg(feature = "openssl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "openssl")))]
    /// Openssl CSR generation failed.
    #[error("Openssl error")]
    Openssl(#[from] openssl::error::ErrorStack),
    /// Failed to generate the CSR.
    #[error("Failed to create Certificate and CSR")]
    Certificate(#[from] rcgen::Error),
    /// Invalid UTF-8 character in the PEM file.
    #[error("Invalid UTF-8 encoded PEM")]
    Utf8(#[from] std::string::FromUtf8Error),
    /// TPM.
    #[error("TPM")]
    Tpm,
}

pub(crate) trait CryptoProvider {
    type Bundle;
    fn create_csr(
        &self,
        realm: &str,
        device_id: &str,
    ) -> Result<(String, Self::Bundle), CryptoError>;
    async fn read_bundle(
        &self,
        store_dir: PathBuf,
    ) -> Result<(Vec<CertificateDer<'static>>, Self::Bundle), io::Error>;
    async fn store_bundle(&self, store_dir: &Path, bundle: &Self::Bundle);
    async fn insecure_tls_config(
        &self,
        builder: rustls::ConfigBuilder<ClientConfig, WantsClientCert>,
        certificates: Vec<CertificateDer<'static>>,
        item: Self::Bundle,
    ) -> Result<rustls::ClientConfig, PairingError>;
    async fn tls_config(
        &self,
        roots: RootCertStore,
        certificates: Vec<CertificateDer<'static>>,
        item: Self::Bundle,
    ) -> Result<rustls::ClientConfig, PairingError>;

    fn read_certificate(store_dir: &PathBuf) -> Result<Vec<CertificateDer<'static>>, io::Error> {
        let certificates = CertificateFile::new(store_dir);
        let c_f = File::open(certificates)?;

        let mut c_r = BufReader::new(c_f);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut c_r).try_collect()?;

        if certs.is_empty() {
            debug!("no certificate found");

            return Err(io::Error::new(io::ErrorKind::Other, "no certificate found"));
        }

        Ok(certs)
    }

    async fn read_credentials(
        &self,
        store_dir: PathBuf,
    ) -> Option<(Vec<CertificateDer<'static>>, Self::Bundle)> {
        let res = self.read_bundle(store_dir).await;

        match res {
            Ok(credentials) => Some(credentials),
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

    async fn store_credential(
        &self,
        store_dir: &Option<PathBuf>,
        certificate: &str,
        bundle: &Self::Bundle,
    ) {
        if let Some(store_dir) = store_dir {
            let certificate_file = CertificateFile::new(store_dir);

            if let Err(err) = fs::write(&certificate_file, certificate) {
                error!(error = %Report::new(&err), file = %certificate_file, "couldn't write certificate file");
            }

            self.store_bundle(store_dir, bundle).await;
        }
    }
}
