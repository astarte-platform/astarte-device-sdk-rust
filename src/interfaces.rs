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

use std::collections::{hash_map::Entry, HashMap};

use itertools::Itertools;
use log::debug;

use crate::{
    interface::{mapping::path::MappingPath, InterfaceError, Mapping, Ownership},
    payload,
    types::AstarteType,
    Aggregation, Error, Interface,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct Interfaces {
    interfaces: HashMap<String, Interface>,
}

/// Validates that a float is a valid Astarte float
fn validate_float(d: &f64) -> Result<(), Error> {
    if d.is_infinite() || d.is_nan() || d.is_subnormal() {
        Err(Error::SendError(
            "You are sending the wrong type for this mapping".into(),
        ))
    } else {
        Ok(())
    }
}

impl Interfaces {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn from<I>(interfaces: I) -> Result<Self, InterfaceError>
    where
        I: IntoIterator<Item = Interface>,
    {
        let mut ints = Self {
            interfaces: HashMap::new(),
        };

        for interface in interfaces {
            ints.add(interface)?;
        }

        Ok(ints)
    }

    /// Inserts an interface and returns the previous one if present.
    ///
    /// ## Error
    ///
    /// If the interface is already present and is not a valid new version, returns a
    /// [`ValidationError`].
    pub(crate) fn add(
        &mut self,
        interface: Interface,
    ) -> Result<Option<Interface>, InterfaceError> {
        let entry = self
            .interfaces
            .entry(interface.interface_name().to_string());

        let prev = match entry {
            Entry::Occupied(mut entry) => {
                debug!(
                    "Interface {} already present, validating new version",
                    interface.interface_name()
                );

                let prev_interface = entry.get();

                interface.validate_with(prev_interface)?;

                Some(entry.insert(interface))
            }
            Entry::Vacant(entry) => {
                debug!("Interface {} not present, adding it", entry.key());

                entry.insert(interface);

                None
            }
        };

        Ok(prev)
    }

    pub(crate) fn remove(&mut self, interface_name: &str) -> Option<Interface> {
        self.interfaces.remove(interface_name)
    }

    pub(crate) fn get_introspection_string(&self) -> String {
        self.interfaces
            .iter()
            .map(|(name, interface)| {
                format!(
                    "{}:{}:{}",
                    name,
                    interface.version_major(),
                    interface.version_minor()
                )
            })
            .join(";")
    }

    pub(crate) fn get(&self, interface_name: &str) -> Option<&Interface> {
        self.interfaces.get(interface_name)
    }

    pub(crate) fn get_property(&self, interface_name: &str) -> Option<&Interface> {
        self.interfaces.get(interface_name).and_then(|interface| {
            if !interface.is_property() {
                None
            } else {
                Some(interface)
            }
        })
    }

    /// Gets mapping from the json description, given the path
    pub(crate) fn get_mapping<'a: 's, 's>(
        &'s self,
        interface_name: &str,
        interface_path: &MappingPath<'a>,
    ) -> Option<Mapping> {
        self.interfaces
            .get(interface_name)
            .and_then(|interface| interface.mapping(interface_path))
    }

    /// Get a property mapping
    pub(crate) fn get_propertiy_mapping<'a: 's, 's>(
        &'s self,
        interface: &str,
        path: &MappingPath<'a>,
    ) -> Option<Mapping> {
        self.get_property(interface)
            .and_then(|interface| interface.mapping(path))
    }

    pub(crate) fn get_mqtt_reliability(
        &self,
        interface_name: &str,
        interface_path: &MappingPath,
    ) -> rumqttc::QoS {
        self.get_mapping(interface_name, interface_path)
            .map(|mapping| mapping.reliability())
            .unwrap_or_default()
            .into()
    }

    /// returns major version if the property exists, None otherwise
    pub fn get_property_major(&self, interface: &str, path: &MappingPath) -> Option<i32> {
        let interface = self.get(interface)?;

        if !interface.contains(path) || !interface.is_property() {
            return None;
        }

        Some(interface.version_major())
    }

    /// returns ownership if the interface is present in device introspection, None otherwise
    pub(crate) fn get_ownership(&self, interface: &str) -> Option<Ownership> {
        self.interfaces
            .get(interface)
            .map(|interface| interface.ownership())
    }

    pub(crate) fn validate_float(data: &AstarteType) -> Result<(), Error> {
        match data {
            AstarteType::Double(d) => validate_float(d),
            AstarteType::DoubleArray(d) => d.iter().try_for_each(validate_float),
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_send(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: &[u8],
        timestamp: &Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error> {
        let data_deserialized = payload::deserialize(data)?;

        let interface = self
            .interfaces
            .get(interface_name)
            .ok_or_else(|| Error::SendError("Interface does not exists".into()))?;

        match data_deserialized {
            Aggregation::Individual(individual) => {
                let mapping = interface
                    .mapping(&interface_path.try_into()?)
                    .ok_or_else(|| Error::SendError("Mapping doesn't exist".into()))?;

                if individual != AstarteType::Unset && individual != mapping.mapping_type() {
                    return Err(Error::SendError(format!(
                        "You are sending the wrong type for this mapping: got {:?}, expected {:?}",
                        individual,
                        mapping.mapping_type()
                    )));
                }

                Interfaces::validate_float(&individual)?;

                if !mapping.explicit_timestamp() && timestamp.is_some() {
                    return Err(Error::SendError(
                        "Do not send timestamp to a mapping without explicit timestamp".into(),
                    ));
                }

                if !mapping.allow_unset() && data.is_empty() {
                    return Err(Error::SendError(
                        "Do not unset a mapping without allow_unset".into(),
                    ));
                }
            }
            Aggregation::Object(object) => {
                for (obj_key, obj_value) in &object {
                    Interfaces::validate_float(obj_value)?;
                    let object_path = format!("{}/{}", interface_path, obj_key);

                    let mapping_path = MappingPath::try_from(object_path.as_str())?;

                    let mapping = interface.mapping(&mapping_path).ok_or_else(|| {
                        Error::SendError(format!("Mapping '{mapping_path}' doesn't exist"))
                    })?;

                    if *obj_value != mapping.mapping_type() {
                        return Err(Error::SendError(
                            format!("You are sending the wrong type for this object mapping: got {:?}, expected {}", obj_value, mapping.mapping_type()),
                        ));
                    }

                    if !mapping.explicit_timestamp() && timestamp.is_some() {
                        return Err(Error::SendError(
                            "Do not send timestamp to a mapping without explicit timestamp".into(),
                        ));
                    }
                }

                if object.len() < interface.mappings_len() {
                    return Err(Error::SendError(
                        "You are missing some mappings from the object".into(),
                    ));
                }
            }
        }

        Ok(())
    }

    pub(crate) fn validate_receive(
        &self,
        interface_name: &str,
        path: &MappingPath,
        bdata: &[u8],
    ) -> Result<(), Error> {
        if interface_name == "control" {
            return Ok(());
        }

        let interface = self.interfaces.get(interface_name).ok_or_else(|| {
            Error::ReceiveError(format!("Interface '{interface_name}' does not exists"))
        })?;

        let data = payload::deserialize(bdata)?;

        match data {
            Aggregation::Individual(individual) => {
                let mapping = interface.mapping(path).ok_or_else(|| {
                    Error::ReceiveError(format!("Mapping '{path}' doesn't exist",))
                })?;

                if individual == AstarteType::Unset {
                    if !mapping.allow_unset() {
                        return Err(Error::ReceiveError(
                            "Do not unset a mapping without allow_unset".into(),
                        ));
                    } else {
                        return Ok(());
                    }
                }

                if individual != mapping.mapping_type() {
                    return Err(Error::ReceiveError(
                        "You are receiving the wrong type for this mapping".into(),
                    ));
                }

                Interfaces::validate_float(&individual)?;
            }
            Aggregation::Object(object) => {
                for (name, value) in &object {
                    Interfaces::validate_float(value)?;

                    let mapping_path = format!("{}/{}", path, name);
                    let mapping = interface
                        .mapping(&mapping_path.as_str().try_into()?)
                        .ok_or_else(|| {
                            Error::ReceiveError(format!("Mapping '{mapping_path}' doesn't exist",))
                        })?;

                    if *value != mapping.mapping_type() {
                        return Err(Error::ReceiveError(
                            "You are receiving the wrong type for this object mapping".into(),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn iter_interfaces(&self) -> impl Iterator<Item = &Interface> {
        self.interfaces.values()
    }
}

#[cfg(test)]
mod test {
    use chrono::{TimeZone, Utc};
    use std::{collections::HashMap, str::FromStr};

    use crate::{
        interfaces::Interfaces, mapping, options::AstarteOptions, payload, types::AstarteType,
        Interface,
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
        let deser_interface = Interface::from_str(interface_json).unwrap();
        let mut ifa = Interfaces::default();
        ifa.add(deser_interface).expect("valid interface");

        assert!(
            ifa.get_property_major("org.astarte-platform.test.test", mapping!("/button"))
                .unwrap()
                == 12
        );
        assert!(
            ifa.get_property_major("org.astarte-platform.test.test", mapping!("/uptimeSeconds"))
                .unwrap()
                == 12
        );
        assert!(ifa
            .get_property_major("org.astarte-platform.test.test", mapping!("/button/foo"))
            .is_none());
        assert!(ifa
            .get_property_major("org.astarte-platform.test.test", mapping!("/buttonfoo"))
            .is_none());
        assert!(ifa
            .get_property_major("org.astarte-platform.test.test", mapping!("/foo/button"))
            .is_none());
        assert!(ifa
            .get_property_major("org.astarte-platform.test.test", mapping!("/obj"))
            .is_none());

        let interface_json = r#"{
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
        }"#;

        let deser_interface = Interface::from_str(interface_json).unwrap();
        let mut ifa = Interfaces::default();
        ifa.add(deser_interface).expect("valid interface");

        assert_eq!(
            ifa.get_property_major(
                "org.astarte-platform.genericsensors.SamplingRate",
                mapping!("/1/enable")
            )
            .unwrap(),
            12
        );
        assert_eq!(
            ifa.get_property_major(
                "org.astarte-platform.genericsensors.SamplingRate",
                mapping!("/999999/enable")
            )
            .unwrap(),
            12
        );
        assert_eq!(
            ifa.get_property_major(
                "org.astarte-platform.genericsensors.SamplingRate",
                mapping!("/foobar/enable")
            )
            .unwrap(),
            12
        );
        assert!(ifa
            .get_property_major(
                "org.astarte-platform.genericsensors.SamplingRate",
                mapping!("/foo/bar/enable")
            )
            .is_none());
        assert!(ifa
            .get_property_major(
                "org.astarte-platform.genericsensors.SamplingRate",
                mapping!("/obj")
            )
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

        let deser_interface = Interface::from_str(server_owned_interface_json).unwrap();
        let mut ifa = Interfaces::default();
        ifa.add(deser_interface).expect("valid interface");

        assert_eq!(
            ifa.get_ownership("org.astarte-platform.server-owned.test")
                .unwrap(),
            crate::interface::Ownership::Server
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
        let deser_interface = Interface::from_str(device_owned_interface_json).unwrap();
        let mut ifa = Interfaces::default();
        ifa.add(deser_interface).expect("valid interface");

        assert_eq!(
            ifa.get_ownership("org.astarte-platform.device-owned.test")
                .unwrap(),
            crate::interface::Ownership::Device
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
        let aggregate_data = payload::serialize_object(&aggregate, None).unwrap();
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
        let aggregate_data = payload::serialize_object(&aggregate, None).unwrap();
        interfaces
            .validate_send(&interface_name, "/1", &aggregate_data, &None)
            .unwrap_err();
        aggregate.insert("integer_endpoint".to_string(), AstarteType::Integer(45));

        // Test sending an aggregate with an non existing object field
        aggregate.insert("gibberish".to_string(), AstarteType::Boolean(false));
        let aggregate_data = payload::serialize_object(&aggregate, None).unwrap();
        interfaces
            .validate_send(&interface_name, "/1", &aggregate_data, &None)
            .unwrap_err();
        aggregate.remove("gibberish");

        // Test sending an aggregate with a missing object field
        aggregate.remove("integer_endpoint");
        let aggregate_data = payload::serialize_object(&aggregate, None).unwrap();
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
            payload::serialize_individual(&AstarteType::Boolean(true), None).unwrap();
        interfaces
            .validate_send(&interface_name, "/boolean", &boolean_endpoint_data, &None)
            .unwrap();
        let double_endpoint_data =
            payload::serialize_individual(&AstarteType::Double(23.2), None).unwrap();
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
            .validate_receive("gibberish", mapping!("/boolean_endpoint"), &Vec::new())
            .unwrap_err();

        // Test non existant path
        interfaces
            .validate_receive(&interface_name, mapping!("/gibberish"), &Vec::new())
            .unwrap_err();

        // Test receiving a new value
        let boolean_endpoint_data =
            payload::serialize_individual(&AstarteType::Boolean(true), None).unwrap();
        interfaces
            .validate_receive(
                &interface_name,
                mapping!("/boolean_endpoint"),
                &boolean_endpoint_data,
            )
            .unwrap();

        // Test receiving a new value with the wrong type
        let integer_endpoint_data =
            payload::serialize_individual(&AstarteType::Integer(23), None).unwrap();
        interfaces
            .validate_receive(
                &interface_name,
                mapping!("/boolean_endpoint"),
                &integer_endpoint_data,
            )
            .unwrap_err();
    }

    #[test]
    fn test_validate_receive_for_aggregate_datastream() {
        let (_, _, _, interface_name, _, interfaces) = helper_prepare_interfaces();

        // Test non existant interface
        interfaces
            .validate_receive("gibberish", mapping!("/boolean_endpoint"), &Vec::new())
            .unwrap_err();

        // Test non existant path
        interfaces
            .validate_receive(&interface_name, mapping!("/gibberish"), &Vec::new())
            .unwrap_err();

        // Test non existant path for aggregate
        interfaces
            .validate_receive(&interface_name, mapping!("/gibberish"), &Vec::new())
            .unwrap_err();

        // Test receiving an aggregate
        let aggr_data: HashMap<String, AstarteType> = HashMap::from([
            ("boolean_endpoint".to_string(), AstarteType::Boolean(false)),
            ("integer_endpoint".to_string(), AstarteType::Integer(324)),
        ]);
        let aggr_data = payload::serialize_object(&aggr_data, None).unwrap();
        interfaces
            .validate_receive(&interface_name, mapping!("/obj"), &aggr_data)
            .unwrap();

        // Test receiving an aggregate with wrong type
        let aggr_data: HashMap<String, AstarteType> = HashMap::from([
            ("boolean_endpoint".to_string(), AstarteType::Boolean(false)),
            ("integer_endpoint".to_string(), AstarteType::Boolean(false)),
        ]);
        let aggr_data = payload::serialize_object(&aggr_data, None).unwrap();
        interfaces
            .validate_receive(&interface_name, mapping!("/foo"), &aggr_data)
            .unwrap_err();
    }

    #[test]
    fn test_validate_receive_for_individual_property() {
        let (_, _, interface_name, _, _, interfaces) = helper_prepare_interfaces();

        // Test non existant interface
        interfaces
            .validate_receive("gibberish", mapping!("/boolean_endpoint"), &[])
            .unwrap_err();

        // Test non existant path
        interfaces
            .validate_receive(&interface_name, mapping!("/gibberish"), &[])
            .unwrap_err();

        // Test receiving a set property
        let boolean_endpoint_data =
            payload::serialize_individual(&AstarteType::Boolean(true), None).unwrap();
        interfaces
            .validate_receive(
                &interface_name,
                mapping!("/boolean_endpoint"),
                &boolean_endpoint_data,
            )
            .unwrap();

        // Test receiving an unset property
        interfaces
            .validate_receive(&interface_name, mapping!("/boolean_endpoint"), &[])
            .unwrap();

        // Test receiving an unset property for a property that can't be unset
        interfaces
            .validate_receive(&interface_name, mapping!("/integer_endpoint"), &[])
            .unwrap_err();

        // Test receiving a set property with the wrong type
        let integer_endpoint_data =
            payload::serialize_individual(&AstarteType::Integer(23), None).unwrap();
        interfaces
            .validate_receive(
                &interface_name,
                mapping!("/boolean_endpoint"),
                &integer_endpoint_data,
            )
            .unwrap_err();

        // Test receiving an aggregate on an property interface
        let aggr_data: HashMap<String, AstarteType> = HashMap::from([
            ("boolean_endpoint".to_string(), AstarteType::Boolean(false)),
            ("integer_endpoint".to_string(), AstarteType::Integer(324)),
        ]);
        let aggr_data = payload::serialize_object(&aggr_data, None).unwrap();
        interfaces
            .validate_receive(&interface_name, mapping!("/obj"), &aggr_data)
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
            Interface::from_str(device_datastream_interface_json).unwrap();
        let device_datastream_interface_name =
            device_datastream_interface.interface_name().to_string();

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
            Interface::from_str(device_aggregate_interface_json).unwrap();
        let device_aggregate_interface_name =
            device_aggregate_interface.interface_name().to_string();

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
            Interface::from_str(server_property_interface_json).unwrap();
        let server_property_interface_name = server_property_interface.interface_name().to_string();

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
                    "endpoint": "/obj/boolean_endpoint",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/obj/integer_endpoint",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;
        let server_aggregate_interface =
            Interface::from_str(server_aggregate_interface_json).unwrap();
        let server_aggregate_interface_name =
            server_aggregate_interface.interface_name().to_string();

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
            Interface::from_str(server_datastream_interface_json).unwrap();
        let server_datastream_interface_name =
            server_datastream_interface.interface_name().to_string();

        let interfaces = Interfaces::from([
            device_datastream_interface,
            device_aggregate_interface,
            server_property_interface,
            server_aggregate_interface,
            server_datastream_interface,
        ])
        .expect("valid interfaces");

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

        let ifa = options.interfaces;

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
