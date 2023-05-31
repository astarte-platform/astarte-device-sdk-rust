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

//! Mocks for the Astarte Device SDK.

use mockall::mock;
use rumqttc::{ClientError, ConnectionError, Event, MqttOptions, QoS};

mock!(
    pub AsyncClient {
        // NOTE: This is mocked for compatibility, but it will be hard to test
        pub fn new(options: MqttOptions, cap: usize) -> (MockAsyncClient, MockEventLoop);
        pub async fn subscribe<S: Into<String> + 'static>(&self, topic: S, qos: QoS) -> Result<(), ClientError>;
        pub async fn publish<S, V>(&self, topic: S, qos: QoS, retain: bool, payload: V,) -> Result<(), ClientError> where S: Into<String> + 'static, V: Into<Vec<u8>> + 'static;
        pub async fn unsubscribe<S: Into<String> + 'static>(&self, topic: S) -> Result<(), ClientError>;
    }
    impl Clone for AsyncClient {
        fn clone(&self) -> Self;
    }
);

mock! {
    pub EventLoop{
        pub async fn poll(&mut self) -> Result<Event, ConnectionError>;
    }
}
