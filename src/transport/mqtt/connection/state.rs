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

use super::connected::Connected;
use super::wait_sends::{TaskHandle, WaitTask};
use super::{ConnError, Disconnected};

/// This cannot be a type state machine, because any additional data cannot be moved out of the enum
/// when polling. The only data that can be easily changed is the current state into the next one.
#[derive(Debug)]
pub(super) enum State {
    // State transition
    Transition,
    // Disconnected state
    Disconnected(Disconnected),
    // The device is disconnected from Astarte, it will need to recreate the connection.
    // Disconnected(Disconnected),
    WaitAcks(WaitTask),
    /// Connected with Astarte.
    Connected(Connected),
}

impl State {
    pub(super) fn set_disconnected(&mut self) {
        let disconnected = match std::mem::replace(self, State::Transition) {
            State::Transition => Disconnected { connection: None },
            State::Disconnected(disconnected) => disconnected,
            State::WaitAcks(wait_task) => Disconnected {
                connection: Some(wait_task.connection),
            },
            State::Connected(connected) => Disconnected {
                connection: Some(connected.connection),
            },
        };

        *self = State::Disconnected(disconnected);
    }

    pub(super) fn set_wait_task(
        &mut self,
        session_present: bool,
        handle: TaskHandle,
    ) -> Result<&mut WaitTask, ConnError> {
        let connection = match std::mem::replace(self, State::Transition) {
            State::Disconnected(Disconnected {
                connection: Some(connection),
            }) => connection,
            state => {
                *self = state;

                return Err(ConnError::State);
            }
        };

        *self = State::WaitAcks(WaitTask {
            connection,
            handle,
            session_present,
        });

        let State::WaitAcks(wait_task) = self else {
            unreachable!()
        };

        Ok(wait_task)
    }

    pub(super) fn set_connected(&mut self) -> Result<(), ConnError> {
        let connected = match std::mem::replace(self, State::Transition) {
            State::WaitAcks(wait_task) => Connected {
                connection: wait_task.connection,
            },
            State::Connected(connected) => connected,
            state => {
                *self = state;

                return Err(ConnError::State);
            }
        };

        *self = State::Connected(connected);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::transport::mqtt::client::{AsyncClient, EventLoop};
    use crate::transport::mqtt::connection::connected::tests::mock_connected;
    use crate::transport::mqtt::connection::wait_sends::tests::mock_wait_task;

    use super::*;

    #[rstest]
    // To spawn the task in mock_wait_task
    #[tokio::test]
    #[case(State::Disconnected(Disconnected { connection: None }))]
    #[case(State::WaitAcks(mock_wait_task(AsyncClient::default(), EventLoop::default(), true)))]
    #[case(State::Connected(mock_connected(AsyncClient::default(), EventLoop::default())))]
    async fn to_disconnected(#[case] mut state: State) {
        state.set_disconnected();

        assert!(matches!(state, State::Disconnected(_)));
    }
}
