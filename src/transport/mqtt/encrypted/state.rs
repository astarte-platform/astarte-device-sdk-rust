// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
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

//! Encrypted connection state

use std::num::Wrapping;

use astarte_device_error::{Error, WrapError};
use aws_lc_rs::agreement::ECDH_P256;
use coset::{CoseEncrypt0, CoseKey, CoseKeyBuilder, TaggedCborSerializable};
use tracing::{debug, info, instrument};
use zeroize::Zeroizing;

use crate::transport::mqtt::components::ClientId;
use crate::transport::mqtt::deps::AsyncClient;
use crate::transport::mqtt::encrypted::messages::{Alg, SUBSCRIBE_TOPICS};
use crate::transport::mqtt::error::MqttError;

use super::EncError;
use super::messages::InitExchange;

#[derive(Debug, Default)]
pub(crate) struct EncState {
    seq: Wrapping<u16>,
    salt: [u8; 32],
    priv_key: Option<aws_lc_rs::agreement::PrivateKey>,
    // TODO: store the secret
    secret: Option<Zeroizing<[u8; aws_lc_rs::cipher::AES_256_KEY_LEN]>>,
}

impl EncState {
    fn fetch_add(&mut self) -> u16 {
        let seq = self.seq.0;

        self.seq += 1;

        seq
    }

    /// Subscribes on the encrypted endpoints
    #[instrument(skip_all)]
    pub(crate) async fn subscribe(
        client: &AsyncClient,
        client_id: &ClientId<&str>,
    ) -> Result<(), Error<MqttError>> {
        let topics = SUBSCRIBE_TOPICS.map(|topic| rumqttc::SubscribeFilter {
            path: format!("{client_id}/{topic}"),
            qos: rumqttc::QoS::ExactlyOnce,
        });

        client
            .subscribe_many(topics)
            .await
            .wrap_err_msg(MqttError::Subscribe, "on encrypted endpoints")?;

        debug!("subscribed on encrypted topics");

        Ok(())
    }

    #[instrument(skip(self))]
    pub(crate) fn init(&mut self) -> Result<InitExchange, Error<EncError>> {
        let seq = self.fetch_add();

        let priv_key = self.priv_key.insert(
            aws_lc_rs::agreement::PrivateKey::generate(&ECDH_P256)
                .wrap_err_msg(EncError::Init, "generating private key")?,
        );

        let pub_key = priv_key
            .compute_public_key()
            .wrap_err_msg(EncError::Init, "computing public key")?;

        let pub_key = CoseKeyBuilder::new_ec2_pub_key_sec1_octet_string(
            coset::iana::EllipticCurve::P_256,
            pub_key.as_ref(),
        )
        .wrap_err_msg(EncError::Init, "parsing cose public key")?
        .build();

        aws_lc_rs::rand::fill(&mut self.salt).wrap_err_msg(EncError::Init, "creating hkdf salt")?;

        let init = InitExchange {
            seq,
            alg: Alg::EcdhP256HkdfSha256Aes256Gcm,
            pub_key,
            hkdf_salt: self.salt,
        };

        debug!("init exchange message");

        Ok(init)
    }

    pub(crate) fn secret(&mut self, peer_public_key: &CoseKey) -> Result<(), Error<EncError>> {
        let peer_public_key = peer_public_key
            .to_sec1_octet_string()
            .wrap_err_msg(EncError::Secret, "converting public key to sec1")?;

        let peer_public_key = aws_lc_rs::agreement::UnparsedPublicKey::new(
            &aws_lc_rs::agreement::ECDH_P256,
            peer_public_key,
        );
        let peer_public_key = aws_lc_rs::agreement::ParsedPublicKey::try_from(peer_public_key)
            .wrap_err_msg(EncError::Secret, "parsing peer public key")?;

        let priv_key = self
            .priv_key
            .take()
            .ok_or_else(|| Error::with(EncError::Secret, "missing private key"))?;

        info!("start agree");
        // TODO: save secret and zeroize
        let key = aws_lc_rs::agreement::agree(
            &priv_key,
            peer_public_key,
            Error::with(EncError::Secret, "in agreement"),
            |sh_se| {
                info!("start");

                let salt = aws_lc_rs::hkdf::Salt::new(aws_lc_rs::hkdf::HKDF_SHA256, &self.salt);

                let pseudo_random_key = salt.extract(sh_se);

                let out_key = pseudo_random_key
                    .expand(&[b"astarte-kdf"], &aws_lc_rs::aead::AES_256_GCM)
                    .wrap_err_msg(EncError::Secret, "expanding key")?;

                let mut key = Zeroizing::new([0u8; aws_lc_rs::cipher::AES_256_KEY_LEN]);

                out_key
                    .fill(key.as_mut_slice())
                    .wrap_err_msg(EncError::Secret, "filling the key")?;

                info!("key derived");

                Ok(key)
            },
        )?;

        self.secret = Some(key);

        Ok(())
    }

    #[instrument(skip_all)]
    pub(crate) fn encrypt(&self, in_out: &mut Vec<u8>) -> Result<(), Error<EncError>> {
        let cose = self.encrypt_cose(in_out)?;

        *in_out = cose
            .to_tagged_vec()
            .wrap_err_msg(EncError::Encrypt, "failed to encode cose encrypt")?;

        Ok(())
    }

    #[instrument(skip_all)]
    pub(crate) fn decrypt(&self, in_out: &mut Vec<u8>) -> Result<(), Error<EncError>> {
        let cose = CoseEncrypt0::from_tagged_slice(in_out)
            .wrap_err_msg(EncError::Decode, "failed to decode cose encrypt")?;

        self.decrypt_cose(in_out, &cose)?;

        Ok(())
    }

    pub(crate) fn encrypt_cose(&self, payload: &[u8]) -> Result<CoseEncrypt0, Error<EncError>> {
        let key = self
            .secret
            .as_deref()
            .ok_or_else(|| Error::with(EncError::Encrypt, "missing shared secret"))?;

        let key = aws_lc_rs::aead::RandomizedNonceKey::new(&aws_lc_rs::aead::AES_256_GCM, key)
            .wrap_err_msg(EncError::Encrypt, "to create randomized nonce")?;

        let protected = coset::HeaderBuilder::new()
            .algorithm(coset::iana::Algorithm::A256GCM)
            .build();

        let mut nonce = Vec::new();

        let builder = coset::CoseEncrypt0Builder::new()
            .protected(protected)
            .try_create_ciphertext(payload, &[], |plain, aad| {
                let mut in_out = Vec::from(plain);

                let gen_nonce = key
                    .seal_in_place_append_tag(aws_lc_rs::aead::Aad::from(aad), &mut in_out)
                    .wrap_err_msg(EncError::Encrypt, "to encrypt message")?;

                nonce = gen_nonce.as_ref().to_vec();

                Ok(in_out)
            })?;

        let unprotected = coset::HeaderBuilder::new().iv(nonce).build();

        let enc = builder.unprotected(unprotected).build();

        Ok(enc)
    }

    /// Decrypts a COSE Encrypted objects.
    pub(crate) fn decrypt_cose(
        &self,
        in_out: &mut Vec<u8>,
        enc: &CoseEncrypt0,
    ) -> Result<(), Error<EncError>> {
        let key = self
            .secret
            .as_deref()
            .ok_or_else(|| Error::with(EncError::Encrypt, "missing shared secret"))?;

        let alg =
            enc.protected.header.alg.as_ref().ok_or_else(|| {
                Error::with(EncError::Encrypt, "missing alg header in cose object")
            })?;

        debug!(?alg);

        if *alg != coset::RegisteredLabelWithPrivate::Assigned(coset::iana::Algorithm::A256GCM) {
            return Err(Error::with(EncError::Encrypt, "invalid cose algorithm")
                .set_ctx(format!("got {alg:?}")));
        }

        let key = aws_lc_rs::aead::RandomizedNonceKey::new(&aws_lc_rs::aead::AES_256_GCM, key)
            .wrap_err_msg(EncError::Encrypt, "to create randomized nonce")?;

        debug!("key ready");

        let nonce = aws_lc_rs::aead::Nonce::try_assume_unique_for_key(&enc.unprotected.iv)
            .wrap_err_msg(EncError::Encrypt, "to create aead nonce")?;

        debug!("nonce created");

        let mut buf = std::mem::take(in_out);

        *in_out = enc.decrypt_ciphertext(
            &[],
            || Error::with(EncError::Encrypt, "missing cypher text"),
            |ciphertext, aad| {
                let aad = aws_lc_rs::aead::Aad::from(aad);

                buf.clear();
                buf.extend(ciphertext);

                let len = key
                    .open_in_place(nonce, aad, &mut buf)
                    .wrap_err_msg(EncError::Encrypt, "to decrypt message")?
                    .len();

                // remove the length
                buf.resize(len, 0);

                Ok(buf)
            },
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mockall::{Sequence, predicate};
    use rumqttc::{SubAck, SubscribeFilter};

    use crate::transport::mqtt::deps::AsyncClient;
    use crate::transport::mqtt::encrypted::messages::tests::cose_key;
    use crate::transport::mqtt::test::notify_success;

    use super::*;

    #[test]
    fn should_init() {
        let mut state = EncState {
            seq: Wrapping(0),
            salt: [0; 32],
            secret: None,
            priv_key: None,
        };

        let res = state.init().unwrap();

        assert_eq!(res.seq, 0);
        assert_eq!(res.alg, Alg::EcdhP256HkdfSha256Aes256Gcm);
        assert_ne!(res.hkdf_salt, [0; 32]);

        res.pub_key.to_sec1_octet_string().unwrap();

        assert_ne!(state.salt, [0; 32]);
        assert_eq!(state.seq.0, 1);
        assert!(state.priv_key.is_some());
        assert_eq!(state.secret, None);
    }

    #[test]
    fn should_derive_secret() {
        let mut state = EncState {
            seq: Wrapping(0),
            salt: [0; 32],
            secret: None,
            priv_key: None,
        };

        state.init().unwrap();
        state.secret(&cose_key()).unwrap();

        assert!(state.priv_key.is_none());
        state.secret.unwrap();
    }

    #[test]
    fn should_encrypt_and_decrypt() {
        let mut alice = EncState {
            seq: Wrapping(0),
            salt: [0; 32],
            secret: None,
            priv_key: None,
        };

        let mut bob = EncState {
            seq: Wrapping(0),
            salt: [0; 32],
            secret: None,
            priv_key: None,
        };

        let a_init = alice.init().unwrap();
        let b_init = bob.init().unwrap();

        bob.salt = a_init.hkdf_salt;

        alice.secret(&b_init.pub_key).unwrap();
        bob.secret(&a_init.pub_key).unwrap();

        let msg = b"Hello World!";

        let mut res = Vec::from(msg);
        alice.encrypt(&mut res).unwrap();
        bob.decrypt(&mut res).unwrap();

        assert_eq!(res, msg);
    }

    #[tokio::test]
    async fn should_subscribe_to_topics() {
        let mut client = AsyncClient::default();
        let mut seq = Sequence::new();

        client
            .expect_subscribe_many::<[SubscribeFilter; 2]>()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq([
                SubscribeFilter {
                    path: "realm/device_id/control/keyAgreement/1".to_string(),
                    qos: rumqttc::QoS::ExactlyOnce,
                },
                SubscribeFilter {
                    path: "realm/device_id/control/keyAgreement/4".to_string(),
                    qos: rumqttc::QoS::ExactlyOnce,
                },
            ]))
            .returning(|_| notify_success(SubAck::new(0, Vec::new())));

        let client_id = ClientId {
            realm: "realm",
            device_id: "device_id",
        };

        EncState::subscribe(&client, &client_id).await.unwrap();
    }
}
