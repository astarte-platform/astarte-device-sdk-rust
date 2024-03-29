/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
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
#![doc = include_str!("../README.md")]
#![warn(clippy::dbg_macro, missing_docs, rustdoc::missing_crate_level_docs)]

pub mod aggregate;
pub mod builder;
pub mod client;
pub mod connection;
pub mod error;
pub mod event;
pub mod interface;
mod interfaces;
pub mod introspection;
pub mod prelude;
pub mod properties;
mod retry;
pub mod store;
mod topic;
pub mod transport;
pub mod types;
mod validate;

/// Re-exported internal structs
pub use crate::aggregate::{AstarteAggregate, Value};
pub use crate::client::{Client, DeviceClient};
pub use crate::connection::{DeviceConnection, EventLoop};
pub use crate::error::Error;
pub use crate::event::{DeviceEvent, FromEvent};
pub use crate::interface::Interface;
pub use crate::types::AstarteType;

// Re-export rumqttc since we return its types in some methods
pub use chrono;
pub use rumqttc;

/// Timestamp returned in the astarte payload
pub(crate) type Timestamp = chrono::DateTime<chrono::Utc>;

// Re-export #[derive(AstarteAggregate)].
//
// The reason re-exporting is not enabled by default is that disabling it would
// be annoying for crates that provide handwritten impls or data formats. They
// would need to disable default features and then explicitly re-enable std.
#[cfg(feature = "derive")]
#[allow(unused_imports)]
#[macro_use]
extern crate astarte_device_sdk_derive;

/// Derive macros enable with the `feature = ["derive"]`.
#[cfg(feature = "derive")]
pub use astarte_device_sdk_derive::*;

#[cfg(test)]
mod test {
    use base64::Engine;
    use mockall::predicate;
    use rumqttc::Event;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::sync::{mpsc, RwLock};

    use crate::interfaces::Interfaces;
    use crate::properties::tests::PROPERTIES_PAYLOAD;
    use crate::properties::PropAccess;
    use crate::store::memory::MemoryStore;
    use crate::store::wrapper::StoreWrapper;
    use crate::store::PropertyStore;
    use crate::transport::mqtt::payload::Payload as MqttPayload;
    use crate::transport::mqtt::test::mock_mqtt_connection;
    use crate::transport::mqtt::Mqtt;
    use crate::{
        self as astarte_device_sdk, Client, DeviceClient, DeviceConnection, EventLoop, Interface,
    };
    use crate::{types::AstarteType, Value};
    use astarte_device_sdk::AstarteAggregate;
    use astarte_device_sdk_derive::astarte_aggregate;
    #[cfg(not(feature = "derive"))]
    use astarte_device_sdk_derive::AstarteAggregate;

    use crate::transport::mqtt::client::{AsyncClient, EventLoop as MqttEventLoop};

    // Interfaces
    pub(crate) const OBJECT_DEVICE_DATASTREAM: &str = include_str!("../examples/object_datastream/interfaces/org.astarte-platform.rust.examples.object-datastream.DeviceDatastream.json");
    pub(crate) const INDIVIDUAL_SERVER_DATASTREAM: &str = include_str!("../examples/individual_datastream/interfaces/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream.json");
    pub(crate) const DEVICE_PROPERTIES: &str = include_str!("../examples/individual_properties/interfaces/org.astarte-platform.rust.examples.individual-properties.DeviceProperties.json");
    pub(crate) const ADDITIONAL_DEVICE_PROPERTIES: &str = include_str!("../examples/additional_interfaces/org.astarte-platform.rust.examples.individual-properties.AdditionalDeviceProperties.json");

    pub(crate) const SERVER_PROPERTIES: &str = include_str!("../examples/individual_properties/interfaces/org.astarte-platform.rust.examples.individual-properties.ServerProperties.json");
    // E2E Interfaces
    pub(crate) const E2E_DEVICE_DATASTREAM: &str = include_str!(
        "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceDatastream.json"
    );
    pub(crate) const E2E_DEVICE_AGGREGATE: &str = include_str!(
        "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceAggregate.json"
    );
    pub(crate) const E2E_DEVICE_PROPERTY: &str = include_str!(
        "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.DeviceProperty.json"
    );

    pub(crate) fn mock_astarte_device<I>(
        client: AsyncClient,
        eventloop: MqttEventLoop,
        interfaces: I,
    ) -> (
        DeviceClient<MemoryStore>,
        DeviceConnection<MemoryStore, Mqtt>,
    )
    where
        I: IntoIterator<Item = Interface>,
    {
        mock_astarte_device_store(client, eventloop, interfaces, MemoryStore::new())
    }

    pub(crate) fn mock_astarte_device_store<I, S>(
        async_client: AsyncClient,
        eventloop: MqttEventLoop,
        interfaces: I,
        store: S,
    ) -> (DeviceClient<S>, DeviceConnection<S, Mqtt>)
    where
        I: IntoIterator<Item = Interface>,
        S: PropertyStore,
    {
        let (tx_connection, rx_client) = flume::bounded(50);
        let (tx_client, rx_connection) = mpsc::channel(50);

        let interfaces = Arc::new(RwLock::new(Interfaces::from_iter(interfaces)));

        let store = StoreWrapper::new(store);

        let client =
            DeviceClient::new(Arc::clone(&interfaces), rx_client, tx_client, store.clone());
        let device = DeviceConnection::new(
            interfaces,
            tx_connection,
            rx_connection,
            store,
            mock_mqtt_connection(async_client, eventloop),
        );

        (client, device)
    }

    #[derive(AstarteAggregate)]
    #[astarte_aggregate(rename_all = "lowercase")]
    struct MyLowerCasedAggregate {
        endpoint01: f64,
        endpoint02: i32,
        endpoint03: bool,
        endpoint04: i64,
        endpoint05: String,
        endpoint06: Vec<u8>,
        endpoint07: chrono::DateTime<chrono::Utc>,
        endpoint08: Vec<f64>,
        endpoint09: Vec<i32>,
        endpoint10: Vec<bool>,
        endpoint11: Vec<i64>,
        endpoint12: Vec<String>,
        endpoint13: Vec<Vec<u8>>,
        endpoint14: Vec<chrono::DateTime<chrono::Utc>>,
    }

    #[test]
    fn test_astarte_aggregate_trait_lower_case_attribute() {
        let my_aggregate = MyLowerCasedAggregate {
            endpoint01: 4.34,
            endpoint02: 1,
            endpoint03: true,
            endpoint04: 45543543534,
            endpoint05: "Hello".to_string(),
            endpoint06: base64::engine::general_purpose::STANDARD
                .decode("aGVsbG8=")
                .unwrap(),
            endpoint07: chrono::offset::Utc::now(),
            endpoint08: Vec::from([43.5, 10.5, 11.9]),
            endpoint09: Vec::from([-4, 123, -2222, 30]),
            endpoint10: Vec::from([true, false]),
            endpoint11: Vec::from([53267895478, 53267895428, 53267895118]),
            endpoint12: Vec::from(["Test ".to_string(), "String".to_string()]),
            endpoint13: Vec::from([
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
            ]),
            endpoint14: Vec::from([chrono::offset::Utc::now(), chrono::offset::Utc::now()]),
        };
        let expected_res = HashMap::from([
            (
                "endpoint01".to_string(),
                AstarteType::Double(my_aggregate.endpoint01),
            ),
            (
                "endpoint02".to_string(),
                AstarteType::Integer(my_aggregate.endpoint02),
            ),
            (
                "endpoint03".to_string(),
                AstarteType::Boolean(my_aggregate.endpoint03),
            ),
            (
                "endpoint04".to_string(),
                AstarteType::LongInteger(my_aggregate.endpoint04),
            ),
            (
                "endpoint05".to_string(),
                AstarteType::String(my_aggregate.endpoint05.clone()),
            ),
            (
                "endpoint06".to_string(),
                AstarteType::BinaryBlob(my_aggregate.endpoint06.clone()),
            ),
            (
                "endpoint07".to_string(),
                AstarteType::DateTime(my_aggregate.endpoint07),
            ),
            (
                "endpoint08".to_string(),
                AstarteType::DoubleArray(my_aggregate.endpoint08.clone()),
            ),
            (
                "endpoint09".to_string(),
                AstarteType::IntegerArray(my_aggregate.endpoint09.clone()),
            ),
            (
                "endpoint10".to_string(),
                AstarteType::BooleanArray(my_aggregate.endpoint10.clone()),
            ),
            (
                "endpoint11".to_string(),
                AstarteType::LongIntegerArray(my_aggregate.endpoint11.clone()),
            ),
            (
                "endpoint12".to_string(),
                AstarteType::StringArray(my_aggregate.endpoint12.clone()),
            ),
            (
                "endpoint13".to_string(),
                AstarteType::BinaryBlobArray(my_aggregate.endpoint13.clone()),
            ),
            (
                "endpoint14".to_string(),
                AstarteType::DateTimeArray(my_aggregate.endpoint14.clone()),
            ),
        ]);
        assert_eq!(expected_res, my_aggregate.astarte_aggregate().unwrap());
        println!("{expected_res:?}");
    }

    #[derive(AstarteAggregate)]
    #[astarte_aggregate(rename_all = "UPPERCASE")]
    struct MyUpperCasedAggregate {
        first_endpoint: f64,
        second_endpoint: f64,
    }

    #[test]
    fn test_astarte_aggregate_trait_upper_case_attribute() {
        let my_aggregate = MyUpperCasedAggregate {
            first_endpoint: 4.34,
            second_endpoint: 23.0,
        };
        let expected_res = HashMap::from([
            (
                "FIRST_ENDPOINT".to_string(),
                AstarteType::Double(my_aggregate.first_endpoint),
            ),
            (
                "SECOND_ENDPOINT".to_string(),
                AstarteType::Double(my_aggregate.second_endpoint),
            ),
        ]);
        assert_eq!(expected_res, my_aggregate.astarte_aggregate().unwrap());
    }

    #[derive(AstarteAggregate)]
    #[astarte_aggregate(rename_all = "PascalCase")]
    struct MyPascalCasedAggregate {
        first_endpoint: f64,
        second_endpoint: f64,
    }

    #[test]
    fn test_astarte_aggregate_trait_pascal_case_attribute() {
        let my_aggregate = MyPascalCasedAggregate {
            first_endpoint: 4.34,
            second_endpoint: 23.0,
        };
        let expected_res = HashMap::from([
            (
                "FirstEndpoint".to_string(),
                AstarteType::Double(my_aggregate.first_endpoint),
            ),
            (
                "SecondEndpoint".to_string(),
                AstarteType::Double(my_aggregate.second_endpoint),
            ),
        ]);
        assert_eq!(expected_res, my_aggregate.astarte_aggregate().unwrap());
    }

    #[tokio::test]
    async fn test_property_set_unset() {
        let eventloop = MqttEventLoop::default();

        let mut client = AsyncClient::default();

        client
            .expect_publish::<String, Vec<u8>>()
            .times(2)
            .returning(|_, _, _, _| Ok(()));

        let (device, mut connection) = mock_astarte_device(
            client,
            eventloop,
            [Interface::from_str(DEVICE_PROPERTIES).unwrap()],
        );

        let expected = AstarteType::String("value".to_string());
        device
            .send(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
                expected.clone(),
            )
            .await
            .expect("Failed to send property");

        let msg = connection.client.recv().await.unwrap();
        connection.handle_client_msg(msg).await.unwrap();

        let val = device
            .property(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
            )
            .await
            .expect("Failed to get property")
            .expect("Property not found");
        assert_eq!(expected, val);

        device
            .unset(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
            )
            .await
            .expect("Failed to unset property");

        let msg = connection.client.recv().await.unwrap();
        connection.handle_client_msg(msg).await.unwrap();

        let val = device
            .property(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
            )
            .await
            .expect("Failed to get property");

        assert_eq!(None, val);
    }

    #[tokio::test]
    async fn test_handle_event() {
        let mut client = AsyncClient::default();

        client
            .expect_clone()
            // number of calls not limited since the clone it's inside a loop
            .returning(AsyncClient::default);

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/1/name".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::function(|buf: &Vec<u8>| {
                    let doc= bson::Document::from_reader(buf.as_slice()).unwrap();

                    let value = doc.get("v").unwrap().as_str().unwrap();

                    value == "name number 1"
                }),
            )
            .returning(|_, _, _, _| Ok(()));

        let mut eventloop = MqttEventLoop::default();

        let data = bson::doc! {
            "v": true
        };

        // Purge properties
        eventloop.expect_poll().once().returning(|| {
            Ok(Event::Incoming(rumqttc::Packet::Publish(
                rumqttc::Publish::new(
                    "realm/device_id/control/consumer/properties",
                    rumqttc::QoS::AtLeastOnce,
                    PROPERTIES_PAYLOAD,
                ),
            )))
        });

        // Send properties
        eventloop.expect_poll().once().returning(move || {
            Ok(Event::Incoming(rumqttc::Packet::Publish(
                rumqttc::Publish::new(
                    "realm/device_id/org.astarte-platform.rust.examples.individual-properties.ServerProperties/1/enable",
                    rumqttc::QoS::AtLeastOnce,
                    bson::to_vec(&data).unwrap()
                ),
            )))
        });

        let (client, mut connection) = mock_astarte_device(
            client,
            eventloop,
            [
                Interface::from_str(DEVICE_PROPERTIES).unwrap(),
                Interface::from_str(SERVER_PROPERTIES).unwrap(),
            ],
        );

        client
            .send(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
                "name number 1".to_string(),
            )
            .await
            .unwrap();

        let handle_events = tokio::spawn(async move {
            connection
                .handle_events()
                .await
                .expect("failed to poll events");
        });

        let event = client.recv().await.expect("no event received");

        assert_eq!("/1/enable", event.path);

        match event.data {
            Value::Individual(AstarteType::Boolean(val)) => {
                assert!(val);
            }
            _ => panic!("Wrong data type {:?}", event.data),
        }

        handle_events.abort();
        let _ = handle_events.await;
    }

    #[tokio::test]
    async fn test_unset_property() {
        let mut client = AsyncClient::default();

        let value = AstarteType::String(String::from("name number 1"));
        let buf = MqttPayload::new(&value).to_vec().unwrap();

        let unset = Vec::new();

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .withf(move |topic,_,_,payload |
                topic == "realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/1/name"
                && *payload == buf
            )
            .returning(|_, _, _, _| Ok(()));

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/1/name".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::eq(unset)
            )
            .returning(|_, _, _, _| Ok(()));

        let eventloop = MqttEventLoop::default();

        let (client, mut connection) = mock_astarte_device(
            client,
            eventloop,
            [Interface::from_str(DEVICE_PROPERTIES).unwrap()],
        );

        client
            .send(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
                "name number 1".to_string(),
            )
            .await
            .unwrap();

        client
            .unset(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
            )
            .await
            .unwrap();

        let msg = connection.client.recv().await.unwrap();
        connection.handle_client_msg(msg).await.unwrap();
        let msg = connection.client.recv().await.unwrap();
        connection.handle_client_msg(msg).await.unwrap();
    }

    #[tokio::test]
    async fn test_receive_object() {
        let mut client = AsyncClient::default();

        client
            .expect_clone()
            // number of calls not limited since the clone it's inside a loop
            .returning(AsyncClient::default);

        let mut eventloop = MqttEventLoop::default();

        let data = bson::doc! {
            "v": {
                "endpoint1": 4.2,
                "endpoint2": "obj",
                "endpoint3": [true],
            }
        };

        // Send object
        eventloop.expect_poll().returning(move || {
            Ok(Event::Incoming(rumqttc::Packet::Publish(
                rumqttc::Publish::new(
                    "realm/device_id/org.astarte-platform.rust.examples.object-datastream.DeviceDatastream/1",
                    rumqttc::QoS::AtLeastOnce,
                    bson::to_vec(&data).unwrap()
                ),
            )))
        });

        let (client, mut connection) = mock_astarte_device(
            client,
            eventloop,
            [Interface::from_str(OBJECT_DEVICE_DATASTREAM).unwrap()],
        );

        let handle_events = tokio::spawn(async move {
            connection
                .handle_events()
                .await
                .expect("failed to poll events");
        });

        let event = client.recv().await.expect("no event received");

        let mut obj = HashMap::new();
        obj.insert("endpoint1".to_string(), AstarteType::Double(4.2));
        obj.insert(
            "endpoint2".to_string(),
            AstarteType::String("obj".to_string()),
        );
        obj.insert(
            "endpoint3".to_string(),
            AstarteType::BooleanArray(vec![true]),
        );
        let expected = Value::Object(obj);

        assert_eq!(
            "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream",
            event.interface
        );
        assert_eq!("/1", event.path);
        assert_eq!(expected, event.data);

        handle_events.abort();
        let _ = handle_events.await;
    }

    #[tokio::test]
    async fn test_send_object() {
        struct MockObject {}

        impl AstarteAggregate for MockObject {
            fn astarte_aggregate(
                self,
            ) -> Result<HashMap<String, AstarteType>, astarte_device_sdk::error::Error>
            {
                let mut obj = HashMap::new();
                obj.insert("endpoint1".to_string(), AstarteType::Double(4.2));
                obj.insert(
                    "endpoint2".to_string(),
                    AstarteType::String("obj".to_string()),
                );
                obj.insert(
                    "endpoint3".to_string(),
                    AstarteType::BooleanArray(vec![true]),
                );

                Ok(obj)
            }
        }

        let mut client = AsyncClient::default();
        let eventloop = MqttEventLoop::default();

        client
            .expect_clone()
            // number of calls not limited since the clone it's inside a loop
            .returning(AsyncClient::default);

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.object-datastream.DeviceDatastream/1".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::always()
            )
            .returning(|_, _, _, _| Ok(()));

        let (device, mut connection) = mock_astarte_device(
            client,
            eventloop,
            [Interface::from_str(OBJECT_DEVICE_DATASTREAM).unwrap()],
        );

        device
            .send_object_with_timestamp(
                "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream",
                "/1",
                MockObject {},
                chrono::offset::Utc::now(),
            )
            .await
            .unwrap();

        let msg = connection.client.recv().await.unwrap();
        connection.handle_client_msg(msg).await.unwrap();
    }
}
