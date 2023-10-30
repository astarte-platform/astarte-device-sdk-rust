/*

* This file is part of Astarte.
*
* Copyright 2023 SECO Mind Srl
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* SPDX-License-Identifier: Apache-2.0
*/

pub mod payload;

use std::{collections::HashMap, fmt::Display, ops::Deref, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, error, info, trace, warn};
use once_cell::sync::OnceCell;
use rumqttc::{Event as MqttEvent, Packet};
use tokio::sync::Mutex;

#[cfg(test)]
pub(crate) use crate::mock::{MockAsyncClient as AsyncClient, MockEventLoop as EventLoop};
#[cfg(not(test))]
pub(crate) use rumqttc::{AsyncClient, EventLoop};

use crate::{
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
    },
    properties,
    retry::DelayedPoll,
    shared::SharedDevice,
    store::{PropertyStore, StoredProp},
    topic::{parse_topic, ParsedTopic},
    types::AstarteType,
    validate::{ValidatedIndividual, ValidatedObject},
    Interface, Timestamp,
};

use payload::Payload;

use super::{Connection, Introspection, ReceivedEvent, Registry};

#[derive(Debug, Clone, Copy)]
struct ClientId<'a> {
    realm: &'a str,
    device_id: &'a str,
}

impl<'a> Display for ClientId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.realm, self.device_id)
    }
}

pub struct SharedMqtt {
    realm: String,
    device_id: String,
    eventloop: Mutex<EventLoop>,
}

#[derive(Clone)]
pub struct Mqtt {
    shared: Arc<SharedMqtt>,
    client: AsyncClient,
}

impl Mqtt {
    pub(crate) fn new(
        realm: String,
        device_id: String,
        eventloop: EventLoop,
        client: AsyncClient,
    ) -> Self {
        Self {
            shared: Arc::new(SharedMqtt {
                realm,
                device_id,
                eventloop: Mutex::new(eventloop),
            }),
            client,
        }
    }

    fn client_id(&self) -> ClientId {
        ClientId {
            realm: &self.realm,
            device_id: &self.device_id,
        }
    }

    async fn connack(
        &self,
        Introspection {
            interfaces,
            server_interfaces,
            device_properties,
        }: Introspection,
        connack: rumqttc::ConnAck,
    ) -> Result<(), crate::Error> {
        if connack.session_present {
            return Ok(());
        }

        self.subscribe_server_interfaces(&server_interfaces).await?;
        self.send_introspection(interfaces).await?;
        self.send_emptycache().await?;
        self.send_device_properties(&device_properties).await?;

        info!("connack done");

        Ok(())
    }

    async fn subscribe_server_interfaces(
        &self,
        server_interfaces: &[String],
    ) -> Result<(), crate::Error> {
        self.client
            .subscribe(
                format!("{}/control/consumer/properties", self.client_id()),
                rumqttc::QoS::ExactlyOnce,
            )
            .await?;

        for iface in server_interfaces {
            self.client
                .subscribe(
                    format!("{}/{iface}/#", self.client_id()),
                    rumqttc::QoS::ExactlyOnce,
                )
                .await?;
        }

        Ok(())
    }

    async fn send_emptycache(&self) -> Result<(), crate::Error> {
        let url = format!("{}/control/emptyCache", self.client_id());
        debug!("sending emptyCache to {}", url);

        self.client
            .publish(url, rumqttc::QoS::ExactlyOnce, false, "1")
            .await?;

        Ok(())
    }

    async fn send_device_properties(
        &self,
        device_properties: &[StoredProp],
    ) -> Result<(), crate::Error> {
        for prop in device_properties {
            let topic = format!("{}/{}{}", self.client_id(), prop.interface, prop.path);

            debug!(
                "sending device-owned property = {}{}",
                prop.interface, prop.path
            );

            let payload = Payload::new(&prop.value).to_vec()?;

            self.client
                .publish(topic, rumqttc::QoS::ExactlyOnce, false, payload)
                .await?;
        }

        Ok(())
    }

    async fn purge_properties<S>(
        &self,
        device: &SharedDevice<S>,
        bdata: &[u8],
    ) -> Result<(), crate::Error>
    where
        S: PropertyStore,
    {
        let stored_props = device.store.load_all_props().await?;

        let paths = properties::extract_set_properties(bdata)?;

        for stored_prop in stored_props {
            if paths.contains(&format!("{}{}", stored_prop.interface, stored_prop.path)) {
                continue;
            }

            device
                .store
                .delete_prop(&stored_prop.interface, &stored_prop.path)
                .await?;
        }

        Ok(())
    }

    async fn poll_mqtt_event(&self) -> Result<MqttEvent, crate::Error> {
        let mut lock = self.eventloop.lock().await;

        match lock.poll().await {
            Ok(event) => Ok(event),
            Err(err) => {
                error!("couldn't poll the event loop: {err:#?}");

                DelayedPoll::retry_poll_event(&mut lock).await
            }
        }
    }

    async fn poll(&self) -> Result<Packet, crate::Error> {
        loop {
            match self.poll_mqtt_event().await? {
                MqttEvent::Incoming(packet) => return Ok(packet),
                MqttEvent::Outgoing(outgoing) => trace!("MQTT Outgoing = {:?}", outgoing),
            }
        }
    }

    async fn send(
        &self,
        interface: &Interface,
        path: &str,
        reliability: rumqttc::QoS,
        payload: Vec<u8>,
    ) -> Result<(), crate::Error> {
        self.client
            .publish(
                format!(
                    "{}/{}{}",
                    self.client_id(),
                    interface.interface_name(),
                    path
                ),
                reliability,
                false,
                payload,
            )
            .await?;

        Ok(())
    }
}

impl Deref for Mqtt {
    type Target = SharedMqtt;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

#[async_trait]
impl<S> Connection<S> for Mqtt {
    type Payload = Bytes;

    async fn connect(&self, introspection: Introspection) -> Result<(), crate::Error>
    where
        S: PropertyStore,
    {
        loop {
            match self.poll().await? {
                rumqttc::Packet::ConnAck(connack) => {
                    self.connack(introspection, connack).await?;

                    return Ok(());
                }
                packet => warn!("Received incoming packet while waiting for connack: {packet:?}"),
            }
        }
    }

    async fn next_event(
        &self,
        device: &SharedDevice<S>,
    ) -> Result<ReceivedEvent<Self::Payload>, crate::Error>
    where
        S: PropertyStore,
    {
        static PURGE_PROPERTIES_TOPIC: OnceCell<String> = OnceCell::new();
        static CLIENT_ID: OnceCell<String> = OnceCell::new();

        // Keep consuming packets until we have an actual "data" event
        loop {
            match self.poll().await? {
                rumqttc::Packet::ConnAck(connack) => {
                    self.connack(
                        Introspection::from(&device.interfaces, &device.store).await?,
                        connack,
                    )
                    .await?
                }
                rumqttc::Packet::Publish(publish) => {
                    let purge_topic = PURGE_PROPERTIES_TOPIC.get_or_init(|| {
                        format!("{}/control/consumer/properties", self.client_id())
                    });

                    debug!("Incoming publish = {} {:x}", publish.topic, publish.payload);

                    if purge_topic == &publish.topic {
                        debug!("Purging properties");

                        self.purge_properties(device, &publish.payload).await?;
                    } else {
                        let client_id = CLIENT_ID.get_or_init(|| self.client_id().to_string());
                        let ParsedTopic { interface, path } =
                            parse_topic(client_id, &publish.topic)?;

                        return Ok(ReceivedEvent {
                            interface: interface.to_string(),
                            path: path.to_string(),
                            payload: publish.payload,
                        });
                    }
                }
                packet => {
                    trace!("packet received {packet:?}");
                }
            }
        }
    }

    fn deserialize_individual(
        &self,
        mapping: MappingRef<'_, &Interface>,
        payload: &Self::Payload,
    ) -> Result<(AstarteType, Option<Timestamp>), crate::Error> {
        // TODO all validation that is independant from the transport should be moved outside and this function should just be called with a validated object
        payload::deserialize_individual(mapping, payload).map_err(|err| err.into())
    }

    fn deserialize_object(
        &self,
        object: ObjectRef,
        path: &MappingPath<'_>,
        payload: &Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), crate::Error> {
        // TODO all validation that is independant from the transport should be moved outside and this function should just be called with a validated object
        payload::deserialize_object(object, path, payload).map_err(|err| err.into())
    }

    async fn send_individual(&self, validated: ValidatedIndividual<'_>) -> Result<(), crate::Error>
    where
        S: 'static,
    {
        let buf = payload::serialize_individual(validated.data(), validated.timestamp())?;

        self.send(
            validated.mapping().interface(),
            validated.path().as_str(),
            validated.mapping().reliability().into(),
            buf,
        )
        .await
    }

    async fn send_object(&self, validated: ValidatedObject<'_>) -> Result<(), crate::Error>
    where
        S: 'static,
    {
        let buf = payload::serialize_object(validated.data(), validated.timestamp())?;

        self.send(
            validated.object().interface,
            validated.path().as_str(),
            validated.object().reliability().into(),
            buf,
        )
        .await
    }
}

#[async_trait]
impl Registry for Mqtt {
    async fn subscribe(&self, interface_name: &str) -> Result<(), crate::Error> {
        self.client
            .subscribe(
                format!("{}/{interface_name}/#", self.client_id()),
                rumqttc::QoS::ExactlyOnce,
            )
            .await
            .map_err(crate::Error::from)
    }

    async fn unsubscribe(&self, interface_name: &str) -> Result<(), crate::Error> {
        self.client
            .unsubscribe(format!("{}/{interface_name}/#", self.client_id()))
            .await
            .map_err(crate::Error::from)
    }

    async fn send_introspection(&self, introspection: String) -> Result<(), crate::Error> {
        debug!("sending introspection = {}", introspection);

        let path = self.client_id().to_string();

        self.client
            .publish(path, rumqttc::QoS::ExactlyOnce, false, introspection)
            .await
            .map_err(crate::Error::from)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use mockall::predicate;
    use rumqttc::Packet;
    use tokio::sync::{mpsc, RwLock};

    use crate::{
        connection::Introspection,
        interfaces::Interfaces,
        shared::SharedDevice,
        store::{memory::MemoryStore, wrapper::StoreWrapper, PropertyStore, StoredProp},
        types::AstarteType,
        Interface,
    };

    use super::{AsyncClient, EventLoop, Mqtt, MqttEvent};

    fn mock_mqtt_connection(client: AsyncClient, eventl: EventLoop) -> Mqtt {
        Mqtt::new("realm".to_string(), "device_id".to_string(), eventl, client)
    }

    #[tokio::test]
    async fn test_poll_server_connack() {
        let mut eventl = EventLoop::default();
        let client = AsyncClient::default();

        eventl.expect_poll().once().returning(|| {
            Ok(MqttEvent::Incoming(rumqttc::Packet::ConnAck(
                rumqttc::ConnAck {
                    session_present: false,
                    code: rumqttc::ConnectReturnCode::Success,
                },
            )))
        });

        let mqtt_connection = mock_mqtt_connection(client, eventl);

        let ack = mqtt_connection
            .poll()
            .await
            .expect("Error while receiving the connack");

        if let Packet::ConnAck(ack) = ack {
            assert!(!ack.session_present);
            assert_eq!(ack.code, rumqttc::ConnectReturnCode::Success);
        }
    }

    #[tokio::test]
    async fn test_connect_client_response() {
        let mut eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        // Connak resnponse for loop in connect method
        eventl.expect_poll().returning(|| {
            Ok(MqttEvent::Incoming(rumqttc::Packet::ConnAck(
                rumqttc::ConnAck {
                    session_present: false,
                    code: rumqttc::ConnectReturnCode::Success,
                },
            )))
        });

        client
            .expect_subscribe::<String>()
            .with(
                predicate::eq("realm/device_id/control/consumer/properties".to_string()),
                predicate::always(),
            )
            .returning(|_topic, _qos| Ok(()));

        client
            .expect_subscribe()
            .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()), predicate::always())
            .returning(|_: String, _| Ok(()));

        // Client id
        client
            .expect_publish::<String, String>()
            .with(
                predicate::eq("realm/device_id".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::always(),
            )
            .returning(|_, _, _, _| Ok(()));

        // empty cache
        client
            .expect_publish::<String, &str>()
            .with(
                predicate::eq("realm/device_id/control/emptyCache".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::eq("1"),
            )
            .returning(|_, _, _, _| Ok(()));

        // device property publish
        client
            .expect_publish::<String, Vec<u8>>()
            .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/sensor1/name".to_string()), predicate::always(), predicate::always(), predicate::always())
            .returning(|_, _, _, _| Ok(()));

        let interfaces = [
            Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap(),
            Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap(),
            Interface::from_str(crate::test::INDIVIDUAL_SERVER_DATASTREAM).unwrap(),
        ];

        let (tx, _rx) = mpsc::channel(2);

        let mqtt_connection = mock_mqtt_connection(client, eventl);
        let shared_device = SharedDevice {
            interfaces: RwLock::new(Interfaces::from_iter(interfaces)),
            store: StoreWrapper {
                store: MemoryStore::new(),
            },
            tx,
        };

        let interface = Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap();

        let prop = StoredProp {
            interface: interface.interface_name(),
            path: "/sensor1/name",
            value: &AstarteType::String("temperature".to_string()),
            interface_major: 0,
            ownership: interface.ownership(),
        };

        shared_device
            .store
            .store_prop(prop)
            .await
            .expect("Error while storing test property");

        let introspection = Introspection::from(&shared_device.interfaces, &shared_device.store)
            .await
            .unwrap();

        mqtt_connection
            .connack(
                introspection,
                rumqttc::ConnAck {
                    session_present: false,
                    code: rumqttc::ConnectReturnCode::Success,
                },
            )
            .await
            .unwrap();
    }
}
