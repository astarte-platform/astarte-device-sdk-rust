// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
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

use astarte_device_sdk::crypto::bench;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn crypto_benchmark(c: &mut Criterion) {
    c.bench_function("generate cert rust-crypto", |b| {
        b.iter(|| bench::generate_key(black_box("test"), black_box("test")))
    });

    #[cfg(feature = "openssl")]
    c.bench_function("generate cert openssl", |b| {
        b.iter(|| bench::openssl_key(black_box("test"), black_box("test")))
    });
}

criterion_group!(crypto, crypto_benchmark);
criterion_main!(crypto);
