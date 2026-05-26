// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
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

//! Test utilities for multiple repositories.
//!
//! This crate is used to share some test utilities across multiple projects.

#![warn(
    missing_docs,
    rustdoc::missing_crate_level_docs,
    missing_debug_implementations,
    clippy::dbg_macro,
    clippy::todo
)]

use std::borrow::Borrow;
use std::fmt::Display;

/// Wrapper to display Hexdumps.
///
/// Wraps a slice of bytes and displays it with many representations.
#[derive(Debug, Clone, Copy)]
pub struct Hexdump<T>(pub T)
where
    T: Borrow<[u8]>;

impl<T> Display for Hexdump<T>
where
    T: Borrow<[u8]>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let buf = self.0.borrow();
        writeln!(f, "Length: {} ({:#x}) bytes", buf.len(), buf.len())?;

        for (i, b) in buf.iter().map(|b| b.to_le()).enumerate() {
            let b_h = b >> 4;
            let b_l = b & 0x0f;

            let c = if b.is_ascii_graphic() { b as char } else { '.' };

            writeln!(f, "{i:04}: | {b_h:04b} {b_l:04b} | ({b:#04x}) '{c}'")?;
        }

        Ok(())
    }
}

/// Setup snapshots in the crate dir.
///
/// It will store the snapshots in the same directory of the crate's `Cargo.toml` under
/// `snapshots/`.
#[macro_export]
macro_rules! with_insta {
    ($asserts:block) => {
        ::insta::with_settings!({
            snapshot_path => concat!(env!("CARGO_MANIFEST_DIR"), "/snapshots")
        }, $asserts);
    };
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use super::*;

    #[test]
    fn use_macro() {
        self::with_insta!({
            assert_snapshot!("using the macro");
        });
    }

    #[test]
    fn hexdump() {
        self::with_insta!({
            assert_snapshot!(Hexdump("42".as_bytes()));
        });
    }
}
