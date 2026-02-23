// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

use std::fmt::Display;

use astarte_interfaces::schema::Reliability;
use rumqttc::QoS;

/// Borrowing wrapper for the client id
///
/// To avoid directly allocating and returning a [`String`] each time
/// the client id is needed this trait implements [`Display`]
/// while only borrowing the field needed to construct the client id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClientId<S = String> {
    pub(crate) realm: S,
    pub(crate) device_id: S,
}

impl ClientId<String> {
    pub(crate) fn as_ref(&self) -> ClientId<&str> {
        ClientId {
            realm: &self.realm,
            device_id: &self.device_id,
        }
    }
}

impl<S> ClientId<S>
where
    S: Display,
{
    /// Create a topic to subscribe on an interface
    pub(crate) fn make_interface_wildcard(&self, interface_name: &str) -> String {
        format!("{self}/{interface_name}/#")
    }
}

impl<S> Display for ClientId<S>
where
    S: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.realm, self.device_id)
    }
}

impl From<ClientId<&str>> for ClientId<String> {
    fn from(value: ClientId<&str>) -> Self {
        ClientId {
            realm: value.realm.to_owned(),
            device_id: value.device_id.to_owned(),
        }
    }
}

/// Converts the reliability into and MQTT [`QoS`]
pub(crate) fn to_qos(reliability: Reliability) -> QoS {
    match reliability {
        Reliability::Unreliable => QoS::AtMostOnce,
        Reliability::Guaranteed => QoS::AtLeastOnce,
        Reliability::Unique => QoS::ExactlyOnce,
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn to_qos_check() {
        let cases = [
            (Reliability::Unreliable, QoS::AtMostOnce),
            (Reliability::Guaranteed, QoS::AtLeastOnce),
            (Reliability::Unique, QoS::ExactlyOnce),
        ];

        for (rel, qos) in cases {
            assert_eq!(to_qos(rel), qos);
        }
    }

    #[test]
    fn client_id_as_ref() {
        let client_id = ClientId {
            realm: "realm".to_string(),
            device_id: "device_id".to_string(),
        };

        let r = client_id.as_ref();

        assert_eq!(r.realm, client_id.realm);
        assert_eq!(r.device_id, client_id.device_id);

        let owned = ClientId::<String>::from(r);
        assert_eq!(owned, client_id)
    }

    #[test]
    fn interface_wildcard() {
        let client_id = ClientId {
            realm: "realm".to_string(),
            device_id: "device".to_string(),
        };

        let wild = client_id.make_interface_wildcard("interface");

        assert_eq!(wild, "realm/device/interface/#");
    }
}
