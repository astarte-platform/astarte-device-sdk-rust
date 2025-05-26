// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
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

//! Module to handle the retry of the MQTT connection

use std::time::{Duration, Instant};

use tracing::trace;

/// Iterator that yields a delay that will increase exponentially till the max,
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExponentialIter {
    n: u32,
    max: u32,
    reset_after: Duration,
    last: Option<Instant>,
}

impl ExponentialIter {
    pub(crate) fn next(&mut self) -> u64 {
        if self
            .last
            .is_some_and(|instant| instant.elapsed() > self.reset_after)
        {
            trace!("resetting timeout");

            // Start from the beginning
            self.n = 0;
        }

        self.last = Some(Instant::now());

        let v = ((self.n > 0) as u64).wrapping_shl(self.n.saturating_sub(1));

        self.n = self.n.saturating_add(1).min(self.max);

        v
    }
}

impl Default for ExponentialIter {
    fn default() -> Self {
        Self {
            n: 0,
            max: 9,
            reset_after: Duration::from_secs(256 * 4),
            last: None,
        }
    }
}

impl Iterator for ExponentialIter {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next())
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::ExponentialIter;

    #[test]
    fn iter_delays() {
        let expected = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 256, 256, 256];
        let delay: Vec<u64> = ExponentialIter::default().take(13).collect();

        assert_eq!(delay, expected);
    }
}
