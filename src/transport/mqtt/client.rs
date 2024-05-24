// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
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

#[cfg(test)]
pub(crate) use self::mock::{MockAsyncClient as AsyncClient, MockEventLoop as EventLoop};
#[cfg(not(test))]
pub(crate) use rumqttc::{AsyncClient, EventLoop};

#[cfg(test)]
/// Lock for synchronizing the calls to [`AsyncClient::new`]
pub(crate) static NEW_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
pub(crate) mod mock {
    use std::fmt::{Debug, Formatter};

    use mockall::mock;
    use rumqttc::{
        ClientError, ConnectionError, Event, MqttOptions, NetworkOptions, NoticeFuture, QoS,
        SubscribeFilter,
    };

    mock!(
        pub AsyncClient {
            pub fn new(options: MqttOptions, cap: usize) -> (MockAsyncClient, MockEventLoop);
            pub async fn subscribe<S: Into<String> + 'static>(&self, topic: S, qos: QoS) -> Result<NoticeFuture, ClientError>;
            pub async fn publish<S, V>(&self, topic: S, qos: QoS, retain: bool, payload: V,) -> Result<NoticeFuture, ClientError> where S: Into<String> + 'static, V: Into<Vec<u8>> + 'static;
            pub async fn unsubscribe<S: Into<String> + 'static>(&self, topic: S) -> Result<NoticeFuture, ClientError>;
            pub async fn subscribe_many<T>(&self, topics: T) -> Result<NoticeFuture, ClientError> where T: IntoIterator<Item = SubscribeFilter> + 'static;
        }
        impl Clone for AsyncClient {
            fn clone(&self) -> Self;
        }
    );

    mock! {
        pub EventLoop{
            pub async fn poll(&mut self) -> Result<Event, ConnectionError>;
            pub fn set_network_options(&mut self, network_options: NetworkOptions) -> &mut Self;
            pub fn clean(&mut self);
        }
    }
}
