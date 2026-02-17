// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
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

use rand::{RngExt, rngs::SmallRng};
use tracing::trace;

use crate::builder::{DEFAULT_BACKOFF_MAXIMUM_DELAY, DEFAULT_BACKOFF_RESET_INTERVAL};

/// Iterator that yields a delay that will increase exponentially till the max,
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExponentialIter {
    n: u32,
    max: Duration,
    reset_after: Duration,
    last: Option<Instant>,
}

impl ExponentialIter {
    pub(crate) fn new(max: Duration, reset_after: Duration) -> Self {
        Self {
            n: 0,
            max,
            reset_after,
            last: None,
        }
    }

    pub(crate) fn next(&mut self) -> Duration {
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

        let next = Duration::from_secs(v);

        if next < self.max {
            self.n = self.n.saturating_add(1);

            next
        } else {
            self.max
        }
    }
}

impl Default for ExponentialIter {
    fn default() -> Self {
        Self {
            n: 0,
            max: DEFAULT_BACKOFF_MAXIMUM_DELAY,
            reset_after: DEFAULT_BACKOFF_RESET_INTERVAL,
            last: None,
        }
    }
}

impl Iterator for ExponentialIter {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next())
    }
}

/// Iterator that yields a delay
#[derive(Debug, Clone)]
pub(crate) struct RandomExponentialIter {
    iter: ExponentialIter,
    rng: SmallRng,
    jitter: u8,
}

impl RandomExponentialIter {
    pub const DEFAULT_RANDOM_JITTER_RANGE: u8 = 50;

    fn new(iter: ExponentialIter, rng: SmallRng, jitter: u8) -> Self {
        let jitter = jitter.clamp(0, 100);

        Self { iter, rng, jitter }
    }

    pub(crate) fn with_jitter(iter: ExponentialIter, jitter: u8) -> Self {
        Self {
            iter,
            jitter,
            ..Default::default()
        }
    }

    pub(crate) fn next(&mut self) -> Duration {
        let exponential = self.iter.next();

        let jitter = i64::from(self.jitter);
        let jitter = self.rng.random_range(-jitter..jitter);
        let jitter = i64::try_from(exponential.as_millis())
            .ok()
            .map(|j| j.div_euclid(100))
            .and_then(|j| j.checked_mul(jitter))
            .unwrap_or(i64::MAX);

        let millis = u64::try_from(exponential.as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add_signed(jitter);

        Duration::from_millis(millis)
    }
}

impl Default for RandomExponentialIter {
    fn default() -> Self {
        Self::new(
            ExponentialIter::default(),
            rand::make_rng(),
            Self::DEFAULT_RANDOM_JITTER_RANGE,
        )
    }
}

impl Iterator for RandomExponentialIter {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use rand::{SeedableRng, rngs::SmallRng};

    use crate::retry::RandomExponentialIter;

    use super::ExponentialIter;

    const EXPONENTIAL_EXPECTED: [u64; 13] = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 256, 256, 256];

    #[test]
    fn iter_delays() {
        let delay: Vec<Duration> = ExponentialIter::default().take(13).collect();

        let expected: Vec<Duration> = EXPONENTIAL_EXPECTED
            .iter()
            .copied()
            .map(Duration::from_secs)
            .collect();

        assert_eq!(delay, expected);
    }

    #[test]
    fn iter_random_delays() {
        const SEED: u64 = 1;
        const JITTER: u8 = 50;

        let random_exponential_iter = RandomExponentialIter::new(
            ExponentialIter::default(),
            SmallRng::seed_from_u64(SEED),
            JITTER,
        );

        EXPONENTIAL_EXPECTED
            .iter()
            .copied()
            .map(|secs| {
                let millis = secs * 1000;
                let jitter = millis.div_euclid(100).saturating_mul(u64::from(JITTER));

                let lower = millis.saturating_sub(jitter);
                let higher = millis.saturating_add(jitter);

                lower..=higher
            })
            .zip(random_exponential_iter)
            .for_each(|(range, duration)| {
                println!("{:?} duration {:?}", range, duration);
                assert!(range.contains(&u64::try_from(duration.as_millis()).unwrap()))
            });
    }

    #[test]
    fn iter_random_test_max() {
        const SEED: u64 = 1;

        let random_exponential_iter = RandomExponentialIter::new(
            ExponentialIter::new(Duration::MAX, Duration::MAX),
            SmallRng::seed_from_u64(SEED),
            100,
        );

        // test that maximum values don't make the iterator panic
        for i in random_exponential_iter.take(256) {
            println!("{i:?}");
        }
    }
}
