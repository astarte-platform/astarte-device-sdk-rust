// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

//! Conversions for const context
//!
//! Necessary for rust 1.78 const compatibility

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

pub(crate) const fn const_non_zero_usize(v: usize) -> NonZeroUsize {
    let Some(v) = NonZeroUsize::new(v) else {
        panic!("value cannot be zero");
    };

    v
}

pub(crate) const fn const_non_zero_u64(v: u64) -> NonZeroU64 {
    let Some(v) = NonZeroU64::new(v) else {
        panic!("value cannot be zero");
    };

    v
}

pub(crate) const fn const_non_zero_u32(v: u32) -> NonZeroU32 {
    let Some(v) = NonZeroU32::new(v) else {
        panic!("value cannot be zero");
    };

    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "value cannot be zero")]
    fn const_non_zero_usize_should_panic() {
        const_non_zero_usize(0);
    }

    #[test]
    #[should_panic(expected = "value cannot be zero")]
    fn const_non_zero_u64_should_panic() {
        const_non_zero_u64(0);
    }

    #[test]
    #[should_panic(expected = "value cannot be zero")]
    fn const_non_zero_u32_should_panic() {
        const_non_zero_u32(0);
    }
}
