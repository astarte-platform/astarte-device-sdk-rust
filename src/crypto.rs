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

use openssl::{
    ec::{EcGroup, EcKey},
    error::ErrorStack,
    hash::MessageDigest,
    nid::Nid,
    pkey::PKey,
    x509::{X509NameBuilder, X509ReqBuilder},
};

pub struct Bundle(pub Vec<u8>, pub Vec<u8>);

impl Bundle {
    pub fn new(cn: &str) -> Result<Bundle, ErrorStack> {
        let group = EcGroup::from_curve_name(Nid::SECP384R1)?;
        let ec_key = EcKey::generate(&group)?;

        let mut subject_builder = X509NameBuilder::new()?;
        subject_builder.append_entry_by_nid(Nid::COMMONNAME, cn)?;

        let subject_name = subject_builder.build();

        let pkey = PKey::from_ec_key(ec_key)?;
        let mut req_builder = X509ReqBuilder::new()?;
        req_builder.set_pubkey(&pkey)?;
        req_builder.set_subject_name(&subject_name)?;
        req_builder.sign(&pkey, MessageDigest::sha512())?;
        let pkey_bytes = pkey.private_key_to_pem_pkcs8()?;

        let req_bytes = req_builder.build().to_pem()?;

        Ok(Bundle(pkey_bytes, req_bytes))
    }
}
