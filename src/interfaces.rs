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

use std::collections::HashMap;

use itertools::Itertools;

use crate::{interface::traits::Mapping, types::AstarteType, AstarteError, Interface};

#[derive(Clone, Debug)]
pub struct Interfaces {
    pub interfaces: HashMap<String, Interface>,
}

impl Interfaces {
    pub fn new(interfaces: HashMap<String, Interface>) -> Self {
        Interfaces { interfaces }
    }

    pub fn get_introspection_string(&self) -> String {
        use crate::interface::traits::Interface;

        self.interfaces
            .iter()
            .map(|f| format!("{}:{}:{}", f.0, f.1.version().0, f.1.version().1))
            .join(";")
    }

    /// gets mapping from the json description, given the path
    pub(crate) fn get_mapping(
        &self,
        interface_name: &str,
        interface_path: &str,
    ) -> Option<crate::interface::Mapping> {
        self.interfaces
            .iter()
            .find(|i| i.0 == interface_name)
            .and_then(|f| f.1.mapping(interface_path))
    }

    pub fn get_mqtt_reliability(&self, interface_name: &str, interface_path: &str) -> rumqttc::QoS {
        use rumqttc::QoS;

        let mapping = self.get_mapping(interface_name, interface_path);

        let reliability = match mapping {
            Some(crate::interface::Mapping::Datastream(m)) => m.reliability,
            _ => Default::default(),
        };

        match reliability {
            crate::interface::Reliability::Unreliable => QoS::AtMostOnce,
            crate::interface::Reliability::Guaranteed => QoS::AtLeastOnce,
            crate::interface::Reliability::Unique => QoS::ExactlyOnce,
        }
    }

    /// returns major version if the property exists, None otherwise
    pub fn get_property_major(&self, interface: &str, path: &str) -> Option<i32> {
        use crate::interface::traits::Interface;

        let iface = self.interfaces.get(interface)?;
        iface.mapping(path)?;

        Some(iface.version().0)
    }

    /// returns ownership if the interface is present in device introspection, None otherwise
    pub(crate) fn get_ownership(&self, interface: &str) -> Option<crate::interface::Ownership> {
        let iface = self.interfaces.get(interface)?;
        Some(iface.get_ownership())
    }

    pub fn validate_float(data: &AstarteType) -> Result<(), AstarteError> {
        fn validate_float(d: &f64) -> Result<(), AstarteError> {
            let error = Err(AstarteError::SendError(
                "You are sending the wrong type for this mapping".into(),
            ));

            if d.is_infinite() || d.is_nan() || d.is_subnormal() {
                error
            } else {
                Ok(())
            }
        }

        match data {
            AstarteType::Double(d) => validate_float(d)?,
            AstarteType::DoubleArray(d) => {
                for n in d {
                    validate_float(n)?
                }
            }
            _ => {}
        };

        Ok(())
    }

    pub fn validate_send(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: &[u8],
        timestamp: &Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), AstarteError> {
        let data_deserialized = crate::AstarteDeviceSdk::deserialize(data)?;

        let interface = self
            .interfaces
            .get(interface_name)
            .ok_or_else(|| AstarteError::SendError("Interface does not exists".into()))?;

        match data_deserialized {
            crate::Aggregation::Individual(individual) => {
                let mapping = self
                    .get_mapping(interface_name, interface_path)
                    .ok_or_else(|| AstarteError::SendError("Mapping doesn't exist".into()))?;

                if individual != AstarteType::Unset && individual != mapping.mapping_type() {
                    return Err(AstarteError::SendError(format!(
                        "You are sending the wrong type for this mapping: got {:?}, expected {:?}",
                        individual,
                        mapping.mapping_type()
                    )));
                }

                Interfaces::validate_float(&individual)?;

                match mapping {
                    crate::interface::Mapping::Datastream(map) => {
                        if !map.explicit_timestamp && timestamp.is_some() {
                            return Err(AstarteError::SendError(
                                "Do not send timestamp to a mapping without explicit timestamp"
                                    .into(),
                            ));
                        }
                    }
                    crate::interface::Mapping::Properties(map) => {
                        if data.is_empty() && !map.allow_unset {
                            return Err(AstarteError::SendError(
                                "Do not unset a mapping without allow_unset".into(),
                            ));
                        }
                    }
                }
            }
            crate::Aggregation::Object(object) => {
                for obj in &object {
                    Interfaces::validate_float(obj.1)?;
                    let mapping_path = format!("{}/{}", interface_path, obj.0);

                    let mapping =
                        self.get_mapping(interface_name, &mapping_path)
                            .ok_or_else(|| {
                                AstarteError::SendError(format!(
                                    "Mapping '{mapping_path}' doesn't exist"
                                ))
                            })?;

                    if *obj.1 != mapping.mapping_type() {
                        return Err(AstarteError::SendError(
                            format!("You are sending the wrong type for this object mapping: got {:?}, expected {:?}", *obj.1, mapping.mapping_type()),
                        ));
                    }

                    match mapping {
                        crate::interface::Mapping::Datastream(map) => {
                            if !map.explicit_timestamp && timestamp.is_some() {
                                return Err(AstarteError::SendError(
                                    "Do not send timestamp to a mapping without explicit timestamp"
                                        .into(),
                                ));
                            }
                        }
                        crate::interface::Mapping::Properties(_) => {
                            return Err(AstarteError::SendError(
                                "Can't send object to properties".into(),
                            ));
                        }
                    }
                }

                if object.len() < interface.mappings_len() {
                    return Err(AstarteError::SendError(
                        "You are missing some mappings from the object".into(),
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn validate_receive(
        &self,
        interface_name: &str,
        interface_path: &str,
        bdata: &[u8],
    ) -> Result<(), AstarteError> {
        if interface_name == "control" {
            return Ok(());
        }

        self.interfaces.get(interface_name).ok_or_else(|| {
            AstarteError::ReceiveError(format!("Interface '{interface_name}' does not exists"))
        })?;

        let data = crate::AstarteDeviceSdk::deserialize(bdata)?;

        match data {
            crate::Aggregation::Individual(individual) => {
                let mapping = self
                    .get_mapping(interface_name, interface_path)
                    .ok_or_else(|| {
                        AstarteError::ReceiveError(format!(
                            "Mapping '{interface_path}' doesn't exist",
                        ))
                    })?;

                if let crate::interface::Mapping::Properties(map) = mapping {
                    if individual == AstarteType::Unset {
                        if !map.allow_unset {
                            return Err(AstarteError::ReceiveError(
                                "Do not unset a mapping without allow_unset".into(),
                            ));
                        } else {
                            return Ok(());
                        }
                    }
                }

                if individual != mapping.mapping_type() {
                    return Err(AstarteError::ReceiveError(
                        "You are receiving the wrong type for this mapping".into(),
                    ));
                }

                Interfaces::validate_float(&individual)?;
            }
            crate::Aggregation::Object(object) => {
                for obj in &object {
                    Interfaces::validate_float(obj.1)?;

                    let mapping_path = format!("{}/{}", interface_path, obj.0);
                    let mapping =
                        self.get_mapping(interface_name, &mapping_path)
                            .ok_or_else(|| {
                                AstarteError::ReceiveError(format!(
                                    "Mapping '{mapping_path}' doesn't exist",
                                ))
                            })?;

                    if *obj.1 != mapping.mapping_type() {
                        return Err(AstarteError::ReceiveError(
                            "You are receiving the wrong type for this object mapping".into(),
                        ));
                    }

                    if let crate::interface::Mapping::Properties(_) = mapping {
                        return Err(AstarteError::ReceiveError(
                            "Can't receive object aggregation from properties interface".into(),
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use chrono::{TimeZone, Utc};
    use std::{collections::HashMap, str::FromStr};

    use crate::{
        interface::traits::Interface, interfaces::Interfaces, options::AstarteOptions,
        types::AstarteType, AstarteDeviceSdk,
    };

    #[test]
    fn test_get_property() {
        let interface_json = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 12,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;
        let deser_interface = crate::Interface::from_str(interface_json).unwrap();
        let mut ifa: HashMap<String, crate::Interface> = HashMap::new();
        ifa.insert(deser_interface.name().into(), deser_interface);
        let ifa = Interfaces::new(ifa);
        assert!(
            ifa.get_property_major("org.astarte-platform.test.test", "/button")
                .unwrap()
                == 12
        );
        assert!(
            ifa.get_property_major("org.astarte-platform.test.test", "/uptimeSeconds")
                .unwrap()
                == 12
        );
        assert!(ifa
            .get_property_major("org.astarte-platform.test.test", "/button/foo")
            .is_none());
        assert!(ifa
            .get_property_major("org.astarte-platform.test.test", "/buttonfoo")
            .is_none());
        assert!(ifa
            .get_property_major("org.astarte-platform.test.test", "/foo/button")
            .is_none());
        assert!(ifa
            .get_property_major("org.astarte-platform.test.test", "/")
            .is_none());
        let interface_json = r#"
        {
            "interface_name": "org.astarte-platform.genericsensors.SamplingRate",
            "version_major": 12,
            "version_minor": 0,
            "type": "properties",
            "ownership": "server",
            "description": "Configure sensors sampling rate and enable/disable.",
            "doc": "",
            "mappings": [
                {
                    "endpoint": "/%{sensor_id}/enable",
                    "type": "boolean",
                    "allow_unset": true,
                    "description": "Enable/disable sensor data transmission.",
                    "doc": ""
                },
                {
                    "endpoint": "/%{sensor_id}/samplingPeriod",
                    "type": "integer",
                    "allow_unset": true,
                    "description": "Sensor sample transmission period.",
                    "doc": ""
                }
            ]
        }
        "#;
        let deser_interface = crate::Interface::from_str(interface_json).unwrap();
        let mut ifa: HashMap<String, crate::Interface> = HashMap::new();
        ifa.insert(deser_interface.name().into(), deser_interface);
        let ifa = Interfaces::new(ifa);
        assert!(
            ifa.get_property_major(
                "org.astarte-platform.genericsensors.SamplingRate",
                "/1/enable"
            )
            .unwrap()
                == 12
        );
        assert!(
            ifa.get_property_major(
                "org.astarte-platform.genericsensors.SamplingRate",
                "/999999/enable"
            )
            .unwrap()
                == 12
        );
        assert!(
            ifa.get_property_major(
                "org.astarte-platform.genericsensors.SamplingRate",
                "/foobar/enable"
            )
            .unwrap()
                == 12
        );
        assert!(ifa
            .get_property_major(
                "org.astarte-platform.genericsensors.SamplingRate",
                "/foo/bar/enable"
            )
            .is_none());
        assert!(ifa
            .get_property_major("org.astarte-platform.genericsensors.SamplingRate", "/")
            .is_none());
    }
    #[test]
    fn test_get_ownership() {
        let server_owned_interface_json = r#"
        {
            "interface_name": "org.astarte-platform.server-owned.test",
            "version_major": 12,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;
        let deser_interface = crate::Interface::from_str(server_owned_interface_json).unwrap();
        let mut ifa: HashMap<String, crate::Interface> = HashMap::new();
        ifa.insert(deser_interface.name().into(), deser_interface);
        let ifa = Interfaces::new(ifa);
        assert!(
            ifa.get_ownership("org.astarte-platform.server-owned.test")
                .unwrap()
                == crate::interface::Ownership::Server
        );
        let device_owned_interface_json = r#"
        {
            "interface_name": "org.astarte-platform.device-owned.test",
            "version_major": 12,
            "version_minor": 1,
            "type": "properties",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;
        let deser_interface = crate::Interface::from_str(device_owned_interface_json).unwrap();
        let mut ifa: HashMap<String, crate::Interface> = HashMap::new();
        ifa.insert(deser_interface.name().into(), deser_interface);
        let ifa = Interfaces::new(ifa);
        assert!(
            ifa.get_ownership("org.astarte-platform.device-owned.test")
                .unwrap()
                == crate::interface::Ownership::Device
        );
    }

    #[test]
    fn test_validate_float() {
        Interfaces::validate_float(&AstarteType::Double(f64::NAN)).unwrap_err();
        Interfaces::validate_float(&AstarteType::DoubleArray(vec![1.0, 2.0, f64::NAN, 4.0]))
            .unwrap_err();
        Interfaces::validate_float(&AstarteType::Double(54.4)).unwrap();
        Interfaces::validate_float(&AstarteType::Integer(12)).unwrap();
    }

    #[test]
    fn test_validate_send_for_aggregate_datastream() {
        let (_, interface_name, _, _, _, interfaces) = helper_prepare_interfaces();

        let mut aggregate: HashMap<String, AstarteType> = HashMap::new();
        aggregate.insert(
            "double_endpoint".to_string(),
            AstarteType::Double(37.534543),
        );
        aggregate.insert("integer_endpoint".to_string(), AstarteType::Integer(45));
        aggregate.insert("boolean_endpoint".to_string(), AstarteType::Boolean(true));
        aggregate.insert(
            "booleanarray_endpoint".to_string(),
            AstarteType::BooleanArray(vec![true, false, true]),
        );

        // Test non existant interface
        interfaces
            .validate_send("gibberish", "/1", &Vec::new(), &None)
            .unwrap_err();

        // Test non existant endpoint
        let aggregate_data = AstarteDeviceSdk::serialize_object(aggregate.clone(), None).unwrap();
        interfaces
            .validate_send(&interface_name, "/1/25", &aggregate_data, &None)
            .unwrap_err();

        // Test sending an aggregate (with and without timestamp)
        let timestamp = Some(TimeZone::timestamp_opt(&Utc, 1537449422, 0).unwrap());
        interfaces
            .validate_send(&interface_name, "/1", &aggregate_data, &None)
            .unwrap();
        interfaces
            .validate_send(&interface_name, "/1", &aggregate_data, &timestamp)
            .unwrap();

        // Test sending an aggregate with an object field with incorrect type
        aggregate.insert("integer_endpoint".to_string(), AstarteType::Boolean(false));
        let aggregate_data = AstarteDeviceSdk::serialize_object(aggregate.clone(), None).unwrap();
        interfaces
            .validate_send(&interface_name, "/1", &aggregate_data, &None)
            .unwrap_err();
        aggregate.insert("integer_endpoint".to_string(), AstarteType::Integer(45));

        // Test sending an aggregate with an non existing object field
        aggregate.insert("gibberish".to_string(), AstarteType::Boolean(false));
        let aggregate_data = AstarteDeviceSdk::serialize_object(aggregate.clone(), None).unwrap();
        interfaces
            .validate_send(&interface_name, "/1", &aggregate_data, &None)
            .unwrap_err();
        aggregate.remove("gibberish");

        // Test sending an aggregate with a missing object field
        aggregate.remove("integer_endpoint");
        let aggregate_data = AstarteDeviceSdk::serialize_object(aggregate.clone(), None).unwrap();
        interfaces
            .validate_send(&interface_name, "/1", &aggregate_data, &None)
            .unwrap_err();
    }

    #[test]
    fn test_validate_send_for_individual_datastream() {
        let (interface_name, _, _, _, _, interfaces) = helper_prepare_interfaces();

        // Test non existant interface
        interfaces
            .validate_send("gibberish", "/boolean", &Vec::new(), &None)
            .unwrap_err();

        // Test sending a value on an unexisting endpoint
        interfaces
            .validate_send(&interface_name, "/gibberish", &Vec::new(), &None)
            .unwrap_err();

        // Test sending a value (with and without timestamp)
        let boolean_endpoint_data =
            AstarteDeviceSdk::serialize_individual(AstarteType::Boolean(true), None).unwrap();
        interfaces
            .validate_send(&interface_name, "/boolean", &boolean_endpoint_data, &None)
            .unwrap();
        let double_endpoint_data =
            AstarteDeviceSdk::serialize_individual(AstarteType::Double(23.2), None).unwrap();
        let timestamp = Some(TimeZone::timestamp_opt(&Utc, 1537449422, 0).unwrap());
        interfaces
            .validate_send(
                &interface_name,
                "/double",
                &double_endpoint_data,
                &timestamp,
            )
            .unwrap();

        // Test sending a value of the wrong type
        interfaces
            .validate_send(&interface_name, "/double", &boolean_endpoint_data, &None)
            .unwrap_err();
        interfaces
            .validate_send(
                &interface_name,
                "/booleanarray",
                &boolean_endpoint_data,
                &None,
            )
            .unwrap_err();
    }

    #[test]
    fn test_validate_receive_for_individual_datastream() {
        let (_, _, _, _, interface_name, interfaces) = helper_prepare_interfaces();

        // Test non existant interface
        interfaces
            .validate_receive("gibberish", "/boolean_endpoint", &Vec::new())
            .unwrap_err();

        // Test non existant path
        interfaces
            .validate_receive(&interface_name, "/gibberish", &Vec::new())
            .unwrap_err();

        // Test receiving a new value
        let boolean_endpoint_data =
            AstarteDeviceSdk::serialize_individual(AstarteType::Boolean(true), None).unwrap();
        interfaces
            .validate_receive(&interface_name, "/boolean_endpoint", &boolean_endpoint_data)
            .unwrap();

        // Test receiving a new value with the wrong type
        let integer_endpoint_data =
            AstarteDeviceSdk::serialize_individual(AstarteType::Integer(23), None).unwrap();
        interfaces
            .validate_receive(&interface_name, "/boolean_endpoint", &integer_endpoint_data)
            .unwrap_err();
    }

    #[test]
    fn test_validate_receive_for_aggregate_datastream() {
        let (_, _, _, interface_name, _, interfaces) = helper_prepare_interfaces();

        // Test non existant interface
        interfaces
            .validate_receive("gibberish", "/boolean_endpoint", &Vec::new())
            .unwrap_err();

        // Test non existant path
        interfaces
            .validate_receive(&interface_name, "/gibberish", &Vec::new())
            .unwrap_err();

        // Test non existant path for aggregate
        interfaces
            .validate_receive(&interface_name, "/gibberish", &Vec::new())
            .unwrap_err();

        // Test receiving an aggregate
        let aggr_data: HashMap<String, AstarteType> = HashMap::from([
            ("boolean_endpoint".to_string(), AstarteType::Boolean(false)),
            ("integer_endpoint".to_string(), AstarteType::Integer(324)),
        ]);
        let aggr_data = AstarteDeviceSdk::serialize_object(aggr_data, None).unwrap();
        interfaces
            .validate_receive(&interface_name, "", &aggr_data)
            .unwrap();

        // Test receiving an aggregate with wrong type
        let aggr_data: HashMap<String, AstarteType> = HashMap::from([
            ("boolean_endpoint".to_string(), AstarteType::Boolean(false)),
            ("integer_endpoint".to_string(), AstarteType::Boolean(false)),
        ]);
        let aggr_data = AstarteDeviceSdk::serialize_object(aggr_data, None).unwrap();
        interfaces
            .validate_receive(&interface_name, "", &aggr_data)
            .unwrap_err();
    }

    #[test]
    fn test_validate_receive_for_individual_property() {
        let (_, _, interface_name, _, _, interfaces) = helper_prepare_interfaces();

        // Test non existant interface
        interfaces
            .validate_receive("gibberish", "/boolean_endpoint", &Vec::new())
            .unwrap_err();

        // Test non existant path
        interfaces
            .validate_receive(&interface_name, "/gibberish", &Vec::new())
            .unwrap_err();

        // Test receiving a set property
        let boolean_endpoint_data =
            AstarteDeviceSdk::serialize_individual(AstarteType::Boolean(true), None).unwrap();
        interfaces
            .validate_receive(&interface_name, "/boolean_endpoint", &boolean_endpoint_data)
            .unwrap();

        // Test receiving an unset property
        interfaces
            .validate_receive(&interface_name, "/boolean_endpoint", &Vec::new())
            .unwrap();

        // Test receiving an unset property for a property that can't be unset
        interfaces
            .validate_receive(&interface_name, "/integer_endpoint", &Vec::new())
            .unwrap_err();

        // Test receiving a set property with the wrong type
        let integer_endpoint_data =
            AstarteDeviceSdk::serialize_individual(AstarteType::Integer(23), None).unwrap();
        interfaces
            .validate_receive(&interface_name, "/boolean_endpoint", &integer_endpoint_data)
            .unwrap_err();

        // Test receiving an aggregate on an property interface
        let aggr_data: HashMap<String, AstarteType> = HashMap::from([
            ("boolean_endpoint".to_string(), AstarteType::Boolean(false)),
            ("integer_endpoint".to_string(), AstarteType::Integer(324)),
        ]);
        let aggr_data = AstarteDeviceSdk::serialize_object(aggr_data, None).unwrap();
        interfaces
            .validate_receive(&interface_name, "", &aggr_data)
            .unwrap_err();
    }

    fn helper_prepare_interfaces() -> (String, String, String, String, String, Interfaces) {
        let device_datastream_interface_json = r#"
        {
            "interface_name": "test.device.datastream",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/boolean",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/booleanarray",
                    "type": "booleanarray",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/double",
                    "type": "double",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;
        let device_datastream_interface =
            crate::Interface::from_str(device_datastream_interface_json).unwrap();
        let device_datastream_interface_name = device_datastream_interface.name().to_string();

        let device_aggregate_interface_json = r#"
        {
            "interface_name": "test.device.object",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "aggregation": "object",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/%{sensor_id}/double_endpoint",
                    "type": "double",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/%{sensor_id}/integer_endpoint",
                    "type": "integer",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/%{sensor_id}/boolean_endpoint",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/%{sensor_id}/booleanarray_endpoint",
                    "type": "booleanarray",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;
        let device_aggregate_interface =
            crate::Interface::from_str(device_aggregate_interface_json).unwrap();
        let device_aggregate_interface_name = device_aggregate_interface.name().to_string();

        let server_property_interface_json = r#"
        {
            "interface_name": "test.server.property",
            "version_major": 0,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/boolean_endpoint",
                    "type": "boolean",
                    "allow_unset": true,
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/integer_endpoint",
                    "type": "integer",
                    "allow_unset": false,
                    "explicit_timestamp": true
                }
            ]
        }
        "#;
        let server_property_interface =
            crate::Interface::from_str(server_property_interface_json).unwrap();
        let server_property_interface_name = server_property_interface.name().to_string();

        let server_aggregate_interface_json = r#"
        {
            "interface_name": "test.server.object",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "server",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": "/boolean_endpoint",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/integer_endpoint",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;
        let server_aggregate_interface =
            crate::Interface::from_str(server_aggregate_interface_json).unwrap();
        let server_aggregate_interface_name = server_aggregate_interface.name().to_string();

        let server_datastream_interface_json = r#"
        {
            "interface_name": "test.server.datastream",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/boolean_endpoint",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/double_endpoint",
                    "type": "double",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;
        let server_datastream_interface =
            crate::Interface::from_str(server_datastream_interface_json).unwrap();
        let server_datastream_interface_name = server_datastream_interface.name().to_string();

        let interfaces = Interfaces::new(HashMap::from([
            (
                device_datastream_interface_name.clone(),
                device_datastream_interface,
            ),
            (
                device_aggregate_interface_name.clone(),
                device_aggregate_interface,
            ),
            (
                server_property_interface_name.clone(),
                server_property_interface,
            ),
            (
                server_aggregate_interface_name.clone(),
                server_aggregate_interface,
            ),
            (
                server_datastream_interface_name.clone(),
                server_datastream_interface,
            ),
        ]));

        (
            device_datastream_interface_name,
            device_aggregate_interface_name,
            server_property_interface_name,
            server_aggregate_interface_name,
            server_datastream_interface_name,
            interfaces,
        )
    }

    #[test]
    fn test_get_instrospection_string() {
        let mut options = AstarteOptions::new("test", "test", "test", "test");
        options = options
            .interface_directory("examples/individual_datastream/interfaces")
            .expect("Failed to set interface directory");

        let ifa = Interfaces::new(options.interfaces);

        let expected = [
            "org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream:0:1",
            "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream:0:1",
        ];

        let intro = ifa.get_introspection_string();
        let mut res: Vec<&str> = intro.split(';').collect();

        res.sort_unstable();

        assert_eq!(res, expected);
    }
}
