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

pub(crate) mod cose_key {
    use coset::{CborSerializable, CoseKey};
    use minicbor::Decoder;
    use minicbor::bytes::ByteSlice;
    use minicbor::encode::{Encoder, Write};

    pub(crate) fn decode<'b, Ctx>(
        d: &mut Decoder<'b>,
        ctx: &mut Ctx,
    ) -> Result<CoseKey, minicbor::decode::Error> {
        let buf: &ByteSlice = minicbor::bytes::decode(d, ctx)?;

        CoseKey::from_slice(buf).map_err(minicbor::decode::Error::custom)
    }

    pub(crate) fn encode<Ctx, W: Write>(
        value: &CoseKey,
        e: &mut Encoder<W>,
        ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        // FIXME: the clone is for an issue in the coset library on how the expose serializzation
        let value = value
            .clone()
            .to_vec()
            .map_err(minicbor::encode::Error::custom)?;

        minicbor::bytes::encode(&value, e, ctx)
    }
}
