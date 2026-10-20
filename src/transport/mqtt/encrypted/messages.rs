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

use std::borrow::Cow;
use std::fmt::Display;

use astarte_device_error::{Error, WrapError};
use coset::CoseKey;
use rumqttc::QoS;

use crate::transport::mqtt::components::ClientId;
use crate::transport::mqtt::deps::AsyncClient;
use crate::transport::mqtt::error::MqttError;

use super::EncError;

/// Encrypted messages subscribe topics.
pub const SUBSCRIBE_TOPICS: [&str; 2] = [ExchangeResp::TOPIC, ExchangeFailed::TOPIC];

#[derive(Debug, Clone, PartialEq, minicbor::Encode, minicbor::Decode)]
pub(crate) struct InitExchange {
    #[n(0)]
    pub(crate) seq: u16,
    #[n(1)]
    pub(crate) alg: Alg,
    #[cbor(n(2), with = "super::derive::cose_key")]
    pub(crate) pub_key: CoseKey,
    #[cbor(n(3), with = "minicbor::bytes")]
    pub(crate) hkdf_salt: [u8; 32],
}

impl InitExchange {
    pub(crate) async fn send(
        &self,
        client: &AsyncClient,
        client_id: &ClientId<&str>,
    ) -> Result<(), Error<MqttError>> {
        let buf = minicbor::to_vec(self)
            .wrap_err_msg(MqttError::Encryption(EncError::Encode), "for InitExchange")?;

        client
            .publish(
                format!("{client_id}/control/keyAgreement/0"),
                QoS::ExactlyOnce,
                false,
                buf,
            )
            .await
            .wrap_err_msg(MqttError::Publish, "for InitExchange")?;

        Ok(())
    }
}

impl Display for InitExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let InitExchange {
            seq,
            alg,
            pub_key,
            hkdf_salt,
        } = self;

        // TODO: should we hex encode the salt?
        write!(
            f,
            "InitExchange [ {seq}, {alg}, {pub_key:?}, {hkdf_salt:?} ]"
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
#[cbor(index_only)]
pub(crate) enum Alg {
    #[n(0)]
    EcdhP256HkdfSha256Aes256Gcm = 0,
    #[n(1)]
    EcdhX25519HkdfSha256Aes256Gcm = 1,
}

impl std::fmt::Display for Alg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Alg::EcdhX25519HkdfSha256Aes256Gcm => {
                write!(f, "ECDH_X25518-HKDF_SHA256-AES_256_GCM")
            }
            Alg::EcdhP256HkdfSha256Aes256Gcm => {
                write!(f, "ECDH_P256-HKDF_SHA256-AES_256_GCM")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, minicbor::Encode, minicbor::Decode)]
pub(crate) struct ExchangeResp {
    #[n(0)]
    seq: u16,
    #[cbor(n(1), with = "super::derive::cose_key")]
    pub_key: CoseKey,
}

impl ExchangeResp {
    pub(crate) const TOPIC: &'static str = "control/keyAgreement/1";

    pub(crate) fn decode_msg(buf: &[u8]) -> Result<Self, Error<EncError>> {
        minicbor::decode(buf).wrap_err_msg(EncError::Decode, "for ExchangeResp")
    }

    pub(crate) fn pub_key(&self) -> &CoseKey {
        &self.pub_key
    }
}

impl Display for ExchangeResp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ExchangeResp { seq, pub_key } = self;
        write!(f, "ExchangeResp [ {seq}, {pub_key:?} ]")
    }
}

#[derive(Debug, Clone, PartialEq, minicbor::Encode, minicbor::Decode)]
pub(crate) struct ExchangeFailed<'a> {
    #[n(0)]
    seq: u16,
    #[cbor(n(1))]
    error_code: u8,
    #[cbor(n(2))]
    error_message: Cow<'a, str>,
}

impl<'a> ExchangeFailed<'a> {
    pub(crate) const TOPIC: &'static str = "control/keyAgreement/4";

    pub(crate) fn decode_msg(buf: &[u8]) -> Result<Self, Error<EncError>> {
        minicbor::decode(buf).wrap_err_msg(EncError::Decode, "for ExchangeResp")
    }
}

impl<'a> Display for ExchangeFailed<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ExchangeFailed {
            seq,
            error_code,
            error_message,
        } = self;

        write!(
            f,
            "ExchangeFailed [ {seq}, {error_code}, {error_message:?} ]"
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use astarte_test_utils::{Hexdump, with_insta};
    use coset::CoseKeyBuilder;
    use insta::assert_snapshot;

    use super::*;

    pub(crate) const PUB_ECC_KEY_PARAMS: &str =
        include_str!("../../../../assets/test-keys/ec-pub-key.params.hex");

    pub(crate) fn cose_key() -> CoseKey {
        let (x, y) = ecc_p256_params();

        CoseKeyBuilder::new_ec2_pub_key(coset::iana::EllipticCurve::P_256, x.to_vec(), y.to_vec())
            .build()
    }

    pub(crate) fn ecc_p256_params() -> ([u8; 32], [u8; 32]) {
        let params = ecc_sec1_uncompressed();

        assert_eq!(params.len(), 1 + 32 + 32);

        // skip the 0x04 ecc ansi encoding
        let x = params[1..33].try_into().unwrap();
        let y = params[33..].try_into().unwrap();

        assert_ne!(x, y);

        (x, y)
    }

    pub(crate) fn ecc_sec1_uncompressed() -> Vec<u8> {
        PUB_ECC_KEY_PARAMS
            .lines()
            .flat_map(|l| l.trim_matches(':').split(':'))
            .map(|s| u8::from_str_radix(s, 16).expect("should be hex"))
            .collect()
    }

    #[test]
    fn init_exchange_roundrip() {
        let case = InitExchange {
            seq: 42,
            alg: Alg::EcdhP256HkdfSha256Aes256Gcm,
            pub_key: cose_key(),
            hkdf_salt: [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7,
                8, 9, 4, 2,
            ],
        };

        let buf = minicbor::to_vec(&case).unwrap();

        let res: InitExchange = minicbor::decode(&buf).unwrap();

        assert_eq!(res, case);

        with_insta!({
            assert_snapshot!(case);

            assert_snapshot!(Hexdump(buf));
        });
    }

    #[test]
    fn exchange_resp_roundrip() {
        let case = ExchangeResp {
            seq: 42,
            pub_key: cose_key(),
        };

        let buf = minicbor::to_vec(&case).unwrap();

        let res: ExchangeResp = ExchangeResp::decode_msg(&buf).unwrap();

        assert_eq!(res, case);

        with_insta!({
            assert_snapshot!(case);

            assert_snapshot!(Hexdump(buf));
        });
    }

    #[test]
    fn exchange_fail_roundrip() {
        let case = ExchangeFailed {
            seq: 42,
            error_code: 1,
            error_message: "failed".into(),
        };

        let buf = minicbor::to_vec(&case).unwrap();

        let res = ExchangeFailed::decode_msg(&buf).unwrap();

        assert_eq!(res, case);

        with_insta!({
            assert_snapshot!(case);

            assert_snapshot!(Hexdump(buf));
        });
    }
}
