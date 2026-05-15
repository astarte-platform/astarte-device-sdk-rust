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

use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

use rumqttc::{MqttOptions, NetworkOptions};
use sync_wrapper::SyncWrapper;

use crate::builder::Config;
use crate::interfaces::Interfaces;
use crate::state::SharedState;
use crate::store::wrapper::StoreWrapper;
use crate::transport::mqtt::ClientSender;
use crate::transport::mqtt::client::{AsyncClient, EventLoop};
use crate::transport::mqtt::config::transport::TransportProvider;

/// Context for the connection
#[derive(Debug)]
pub struct Ctx<'a, S> {
    pub(crate) sender: &'a Arc<OnceLock<ClientSender>>,
    pub(crate) state: &'a SharedState,
    pub(crate) provider: &'a TransportProvider,
    pub(crate) store: &'a StoreWrapper<S>,
    pub(crate) interfaces: &'a Interfaces,
    /// Whether the stored introspection matches the current one
    pub(crate) session_synced: bool,
}

/// Struct to hold the connection and client to be passed to the state.
///
/// We pass a mutable reference to the connection from outside the state so we can freely move from
/// one state to the other, returning a new one. This without having to move the connection from
/// behind the mutable reference.
pub(super) struct Connection {
    pub(super) client: AsyncClient,
    // NOTE: this should be replaces by Exclusive<EventLoop> when the feature `exclusive_wrapper`
    //       is stabilized or the EventLoop becomes Sync
    //       https://doc.rust-lang.org/std/sync/struct.Exclusive.html
    pub(super) eventloop: SyncWrapper<EventLoop>,
}

impl Connection {
    pub(crate) fn new(config: &Config, opt: MqttOptions, net: NetworkOptions) -> Connection {
        let (client, mut eventloop) = AsyncClient::new(opt, config.channel_size.get());

        eventloop.set_network_options(net);

        Connection {
            client,
            eventloop: SyncWrapper::new(eventloop),
        }
    }

    /// Set the transport for the rumqttc
    pub(crate) fn set_transport(&mut self, transport: rumqttc::Transport) {
        cfg_if::cfg_if! {
            if #[cfg(test)] {
                let _ = transport;
            } else {
                self.eventloop.get_mut().mqtt_options.set_transport(transport);
            }
        }
    }

    pub(crate) fn eventloop_mut(&mut self) -> &mut EventLoop {
        self.eventloop.get_mut()
    }

    pub(crate) fn set_clean_session(&mut self, clean_session: bool) {
        cfg_if::cfg_if! {
            if #[cfg(test)] {
                let _ = clean_session;
            } else {
                self.eventloop.get_mut().mqtt_options.set_clean_session(clean_session);
            }
        }
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            client: _,
            eventloop: _,
        } = self;

        f.debug_struct("Connection").finish_non_exhaustive()
    }
}
