// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

//! TPM

use std::fmt::{Debug, Formatter};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use picky_asn1::wrapper::IntegerAsn1;
use rcgen::RemoteKeyPair;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::client::{ResolvesClientCert, WantsClientCert};
use rustls::pki_types::CertificateDer;
use rustls::sign::{CertifiedKey, Signer, SigningKey};
use rustls::{ClientConfig, RootCertStore, SignatureAlgorithm};
use serde::{Deserialize, Serialize};
use sha2::Digest as Sha2Digest;
use tss_esapi::abstraction::public::DecodedKey;
use tss_esapi::constants::tss::{TPM2_RH_NULL, TPM2_ST_HASHCHECK};
use tss_esapi::interface_types::resource_handles::Hierarchy;
use tss_esapi::structures::{HashcheckTicket, Public, Signature};
use tss_esapi::tss2_esys::TPMT_TK_HASHCHECK;
use tss_esapi::WrapperErrorKind;
use tss_esapi::{
    attributes::ObjectAttributesBuilder,
    handles::KeyHandle,
    interface_types::{
        algorithm::{HashingAlgorithm, PublicAlgorithm},
        ecc::EccCurve,
    },
    structures::{
        Digest, EccPoint, EccScheme, HashScheme, PublicBuilder, PublicEccParametersBuilder,
        SignatureScheme, SymmetricCipherParameters, SymmetricDefinitionObject,
    },
    Context, TctiNameConf,
};

use crate::transport::mqtt::crypto::{CryptoError, CryptoProvider};
use crate::transport::mqtt::PairingError;

pub struct TpmCryptoProvider;

impl CryptoProvider for TpmCryptoProvider {
    type Bundle = TpmHandle;

    fn create_csr(
        &self,
        realm: &str,
        device_id: &str,
    ) -> Result<(String, Self::Bundle), CryptoError> {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, format!("{}/{}", realm, device_id));

        let tpm_key = TpmHandle::new();
        let key_pair = KeyPair::from_remote(Box::new(RemoteTpmKey {
            tpm_key: tpm_key.clone(),
        }))?;

        let mut csr_param = CertificateParams::new([])?;
        csr_param.distinguished_name = dn;

        // Signed CSR
        let csr = csr_param.serialize_request(&key_pair)?.pem()?;
        Ok((csr, tpm_key))
    }

    async fn read_bundle(
        &self,
        store_dir: PathBuf,
    ) -> Result<(Vec<CertificateDer<'static>>, Self::Bundle), io::Error> {
        let res: Result<Vec<CertificateDer<'_>>, io::Error> =
            tokio::task::spawn_blocking(move || {
                let certificate = Self::read_certificate(&store_dir)?;
                Ok(certificate)
            })
            .await?;

        let tpm_handle = TpmHandle::new();
        Ok((res?, tpm_handle))
    }

    async fn store_bundle(&self, _: &Path, _: &Self::Bundle) {}

    async fn insecure_tls_config(
        &self,
        builder: rustls::ConfigBuilder<ClientConfig, WantsClientCert>,
        certificates: Vec<CertificateDer<'static>>,
        item: Self::Bundle,
    ) -> Result<rustls::ClientConfig, PairingError> {
        Ok(builder.with_client_cert_resolver(Arc::new(CertResolver {
            certs: certificates,
            tpm_key: item,
        })))
    }

    async fn tls_config(
        &self,
        roots: RootCertStore,
        certificates: Vec<CertificateDer<'static>>,
        item: Self::Bundle,
    ) -> Result<ClientConfig, PairingError> {
        Ok(ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_cert_resolver(Arc::new(CertResolver {
                certs: certificates,
                tpm_key: item,
            })))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TpmHandle {
    context: Arc<RwLock<Context>>,
    key_handle: KeyHandle,
    pub_key: Vec<u8>,
}

impl TpmHandle {
    pub fn new() -> Self {
        let mut tpm_context = Context::new(
            TctiNameConf::from_environment_variable()
                .expect("Failed to get TCTI / TPM2TOOLS_TCTI from environment. Try `export TCTI=device:/dev/tpmrm0`"),
        )
            .expect("Failed to create Context");

        let (key_handle, public) = create_key(&mut tpm_context);
        let decoded_key: DecodedKey = DecodedKey::try_from(public.clone()).unwrap();
        let buffer = if let DecodedKey::EcPoint(ec_point) = decoded_key {
            ec_point.0
        } else {
            todo!()
        };

        Self {
            context: Arc::new(RwLock::new(tpm_context)),
            key_handle,
            pub_key: buffer,
        }
    }

    fn sign(&self, msg: &[u8]) -> Result<Vec<u8>, tss_esapi::Error> {
        let sig_scheme = SignatureScheme::EcDsa {
            hash_scheme: HashScheme::new(HashingAlgorithm::Sha256),
        };

        let mut hasher = sha2::Sha256::new();
        hasher.update(msg);
        let hash = hasher.finalize().to_vec();

        let digest = Digest::try_from(hash)?;

        self.context
            .write()
            .unwrap()
            .execute_with_nullauth_session(|ctx| {
                let validation = TPMT_TK_HASHCHECK {
                    tag: TPM2_ST_HASHCHECK,
                    hierarchy: TPM2_RH_NULL,
                    digest: Default::default(),
                };

                let val = HashcheckTicket::try_from(validation).unwrap();

                let sign = ctx.sign(self.key_handle, digest.clone(), sig_scheme, val);

                ctx.verify_signature(self.key_handle, digest, sign.clone().unwrap())
                    .expect("Failed to verify attestation");

                if let Ok(Signature::EcDsa(ecc_signature)) = &sign {
                    picky_asn1_der::to_vec(&EccSignatureAsn1 {
                        r: IntegerAsn1::from_bytes_be_unsigned(
                            ecc_signature.signature_r().to_vec(),
                        ),
                        s: IntegerAsn1::from_bytes_be_unsigned(
                            ecc_signature.signature_s().to_vec(),
                        ),
                    })
                    .map_err(|_| tss_esapi::Error::WrapperError(WrapperErrorKind::InternalError))
                } else {
                    todo!()
                }
            })
    }
}

pub(crate) struct RemoteTpmKey {
    pub tpm_key: TpmHandle,
}

impl RemoteKeyPair for RemoteTpmKey {
    fn public_key(&self) -> &[u8] {
        &self.tpm_key.pub_key
    }

    fn sign(&self, msg: &[u8]) -> Result<Vec<u8>, rcgen::Error> {
        self.tpm_key
            .sign(msg)
            .map_err(|err| rcgen::Error::RemoteKeyError)
    }

    fn algorithm(&self) -> &'static rcgen::SignatureAlgorithm {
        &rcgen::PKCS_ECDSA_P256_SHA256
    }
}

struct TpmSigner {
    tpm_key: TpmHandle,
}

pub struct CertResolver {
    pub certs: Vec<CertificateDer<'static>>,
    pub tpm_key: TpmHandle,
}

impl Debug for CertResolver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ResolvesClientCert for CertResolver {
    fn resolve(&self, _: &[&[u8]], _: &[rustls::SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        Some(Arc::new(CertifiedKey {
            cert: self.certs.clone(),
            key: Arc::new(TpmSigner {
                tpm_key: self.tpm_key.clone(),
            }),
            ocsp: None,
        }))
    }

    fn has_certs(&self) -> bool {
        true
    }
}

impl Debug for TpmSigner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
struct EccSignatureAsn1 {
    r: IntegerAsn1,
    s: IntegerAsn1,
}

impl Signer for TpmSigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        self.tpm_key
            .sign(message)
            .map_err(|_| rustls::Error::General("signing failed".into()))
    }

    fn scheme(&self) -> rustls::SignatureScheme {
        rustls::SignatureScheme::ECDSA_NISTP256_SHA256
    }
}

impl SigningKey for TpmSigner {
    fn choose_scheme(&self, offered: &[rustls::SignatureScheme]) -> Option<Box<dyn Signer>> {
        Some(Box::new(TpmSigner {
            tpm_key: self.tpm_key.clone(),
        }))
    }

    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::ECDSA
    }
}

fn create_key(context: &mut Context) -> (KeyHandle, Public) {
    let object_attributes = ObjectAttributesBuilder::new()
        .with_fixed_tpm(true)
        .with_fixed_parent(true)
        .with_st_clear(false)
        .with_sensitive_data_origin(true)
        .with_user_with_auth(true)
        .with_decrypt(true)
        .with_restricted(true)
        .build()
        .expect("Failed to build object attributes");

    let primary_pub = PublicBuilder::new()
        .with_public_algorithm(PublicAlgorithm::SymCipher)
        .with_name_hashing_algorithm(HashingAlgorithm::Sha256)
        .with_object_attributes(object_attributes)
        .with_symmetric_cipher_parameters(SymmetricCipherParameters::new(
            SymmetricDefinitionObject::AES_128_CFB,
        ))
        .with_symmetric_cipher_unique_identifier(Digest::default())
        .build()
        .unwrap();

    let primary = context
        .execute_with_nullauth_session(|ctx| {
            ctx.create_primary(Hierarchy::Owner, primary_pub, None, None, None, None)
        })
        .unwrap();

    let object_attributes = ObjectAttributesBuilder::new()
        .with_fixed_tpm(true)
        .with_fixed_parent(true)
        .with_st_clear(false)
        .with_sensitive_data_origin(true)
        .with_user_with_auth(true)
        // The key is used only for signing.
        .with_sign_encrypt(true)
        .build()
        .expect("Failed to build object attributes");

    let ecc_params = PublicEccParametersBuilder::new_unrestricted_signing_key(
        EccScheme::EcDsa(HashScheme::new(HashingAlgorithm::Sha256)),
        EccCurve::NistP256,
    )
    .build()
    .expect("Failed to build ecc params");

    let key_pub = PublicBuilder::new()
        .with_public_algorithm(PublicAlgorithm::Ecc)
        .with_name_hashing_algorithm(HashingAlgorithm::Sha256)
        .with_object_attributes(object_attributes)
        .with_ecc_parameters(ecc_params)
        .with_ecc_unique_identifier(EccPoint::default())
        .build()
        .unwrap();

    let (key_handle, public) = context
        .execute_with_nullauth_session(|ctx| {
            let (private, public) = ctx
                .create(primary.key_handle, key_pub, None, None, None, None)
                .map(|key| (key.out_private, key.out_public))?;

            let key_handle = ctx.load(primary.key_handle, private, public.clone())?;
            // Unload the primary to make space for objects.
            ctx.flush_context(primary.key_handle.into())
                // And return the key_handle.
                .map(|()| (key_handle, public))
        })
        .unwrap();
    (key_handle, public)
}
