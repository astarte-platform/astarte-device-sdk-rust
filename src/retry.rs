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

//! Module to handle the retry of the MQTT connection

/// Iterator that yields a delay that will increase exponentially till the max,
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExponentialIter {
    max: u64,
    delay: u64,
    exp: u32,
    base: u64,
}

impl ExponentialIter {
    pub(crate) fn next(&mut self) -> u64 {
        if self.delay >= self.max {
            return self.max;
        }

        // Calculate the delay base^exp capped at max
        self.delay = self.base.saturating_pow(self.exp).min(self.max);
        // Increase the delay exponentially
        self.exp += 1;

        self.delay
    }
}

impl Default for ExponentialIter {
    fn default() -> Self {
        Self {
            max: 360,
            delay: 0,
            exp: 0,
            base: 2,
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
    use super::ExponentialIter;

    #[test]
    fn iter_delays() {
        let expected = [1, 2, 4, 8, 16];
        let delay: Vec<u64> = ExponentialIter::default().take(5).collect();

        assert_eq!(delay, expected);
    }
}
