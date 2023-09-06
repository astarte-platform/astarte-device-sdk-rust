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

use std::time::Duration;

use log::{debug, error};
use rumqttc::Event;

use crate::{error::Error, AstarteDeviceSdk};

/// Iterator that yields a delay that will increase exponentially till the max,
#[derive(Debug, Clone, Copy)]
pub(crate) struct DelaiedPoll {
    max: u32,
    delay: u32,
    exp: u32,
    base: u32,
}

impl DelaiedPoll {
    /// Retry to pool the connection after an error occurred
    pub(crate) async fn retry_poll_event<T>(sdk: &AstarteDeviceSdk<T>) -> Result<Event, Error> {
        for delay in Self::default() {
            debug!("waiting for {delay} seconds before retry");

            tokio::time::sleep(Duration::from_secs(delay.into())).await;

            match sdk.eventloop.lock().await.poll().await {
                Ok(event) => return Ok(event),
                Err(err) => {
                    error!("couldn't poll for next event: {err:#?}");
                }
            }
        }

        Err(Error::ConnectionTimeout)
    }
}

impl Default for DelaiedPoll {
    fn default() -> Self {
        Self {
            max: 16,
            delay: 0,
            exp: 0,
            base: 2,
        }
    }
}

impl Iterator for DelaiedPoll {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.delay >= self.max {
            return Some(self.max);
        }

        // Calculate the delay base^exp capped at max
        self.delay = self.base.saturating_pow(self.exp).min(self.max);
        // Increase the delay exponentially
        self.exp += 1;

        Some(self.delay)
    }
}

#[cfg(test)]
mod tests {
    use super::DelaiedPoll;

    #[test]
    fn iter_delays() {
        let expected = [1, 2, 4, 8, 16, 16, 16];
        let delay: Vec<u32> = DelaiedPoll::default().take(7).collect();

        assert_eq!(delay, expected);
    }
}
