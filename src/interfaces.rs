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

use crate::{interface::traits::Mapping, types::AstarteType, AstarteError, Interface};

#[derive(Clone)]
pub struct Interfaces {
    pub interfaces: HashMap<String, Interface>,
}

impl Interfaces {
    pub fn new(interfaces: HashMap<String, Interface>) -> Self {
        Interfaces { interfaces }
    }

    pub fn get_introspection_string(&self) -> String {
        use crate::interface::traits::Interface;

        let mut introspection: String = self
            .interfaces
            .iter()
            .map(|f| format!("{}:{}:{};", f.0, f.1.version().0, f.1.version().1))
            .collect();
        introspection.pop(); // remove last ";"
        introspection
    }

    /// gets mapping from the json description, given the path
    pub fn get_mapping(
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
    pub fn get_ownership(&self, interface: &str) -> Option<crate::interface::Ownership> {
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
        let data_deserialized = crate::AstarteSdk::deserialize(data)?;

        let interface = self
            .interfaces
            .get(interface_name)
            .ok_or_else(|| AstarteError::SendError("Interface does not exists".into()))?;

        match data_deserialized {
            crate::Aggregation::Individual(individual) => {
                let mapping = self
                    .get_mapping(interface_name, interface_path)
                    .ok_or_else(|| AstarteError::SendError("Mapping doesn't exist".into()))?;

                if individual != mapping.mapping_type() {
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
                                    "Mapping '{}' doesn't exist",
                                    mapping_path
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
            AstarteError::ReceiveError(format!("Interface '{}' does not exists", interface_name))
        })?;

        let data = crate::AstarteSdk::deserialize(bdata)?;

        match data {
            crate::Aggregation::Individual(individual) => {
                let mapping = self
                    .get_mapping(interface_name, interface_path)
                    .ok_or_else(|| {
                        AstarteError::ReceiveError(format!(
                            "Mapping '{}' doesn't exist",
                            interface_path
                        ))
                    })?;

                match mapping {
                    crate::interface::Mapping::Datastream(_) => {}
                    crate::interface::Mapping::Properties(map) => {
                        if bdata.is_empty() && !map.allow_unset {
                            return Err(AstarteError::ReceiveError(
                                "Do not unset a mapping without allow_unset".into(),
                            ));
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
                                    "Mapping '{}' doesn't exist",
                                    mapping_path
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
    use std::{collections::HashMap, convert::TryInto, str::FromStr};

    use crate::{
        builder::AstarteOptions, interface::traits::Interface, types::AstarteType, AstarteSdk,
    };

    #[test]
    fn test_individual() {
        let mut options = AstarteOptions::new("test", "test", "test", "test");
        options.interface_directory("examples/interfaces/").unwrap();
        let ifa = super::Interfaces::new(options.interfaces);

        let buf = AstarteSdk::serialize_individual(AstarteType::Boolean(true), None).unwrap();

        ifa.validate_send("com.test.Everything", "/boolean", &buf, &None)
            .unwrap();
        ifa.validate_send("com.test.Everything", "/double", &buf, &None)
            .unwrap_err();
        ifa.validate_send("com.test.Everything", "/booleanarray", &buf, &None)
            .unwrap_err();
        ifa.validate_send("com.test.Everything", "/fake", &buf, &None)
            .unwrap_err();
        ifa.validate_send("com.fake.fake", "/fake", &buf, &None)
            .unwrap_err();
        ifa.validate_send("com.fake.fake", "/boolean", &buf, &None)
            .unwrap_err();

        let timestamp = Some(chrono::TimeZone::timestamp(&chrono::Utc, 1537449422, 0));

        ifa.validate_send("com.test.Everything", "/boolean", &buf, &timestamp)
            .unwrap();
        ifa.validate_send("com.test.Everything", "/double", &buf, &timestamp)
            .unwrap_err();
        ifa.validate_send("com.test.Everything", "/booleanarray", &buf, &timestamp)
            .unwrap_err();
        ifa.validate_send("com.test.Everything", "/fake", &buf, &timestamp)
            .unwrap_err();
        ifa.validate_send("com.fake.fake", "/fake", &buf, &timestamp)
            .unwrap_err();
        ifa.validate_send("com.fake.fake", "/boolean", &buf, &timestamp)
            .unwrap_err();

        let buf = AstarteSdk::serialize_individual(AstarteType::Double(f64::NAN), None).unwrap(); // NaN

        ifa.validate_send("com.test.Everything", "/double", &buf, &None)
            .unwrap_err();

        let buf = AstarteSdk::serialize_individual(
            AstarteType::DoubleArray(vec![1.0, 2.0, f64::NAN, 4.0]),
            None,
        )
        .unwrap(); // NaN

        ifa.validate_send("com.test.Everything", "/double", &buf, &None)
            .unwrap_err();
    }

    #[test]
    fn test_object() {
        let mut options = AstarteOptions::new("test", "test", "test", "test");
        options.interface_directory("examples/interfaces/").unwrap();
        let ifa = super::Interfaces::new(options.interfaces);

        let mut obj: std::collections::HashMap<&str, AstarteType> =
            std::collections::HashMap::new();
        obj.insert("latitude", 37.534543.try_into().unwrap());
        obj.insert("longitude", 45.543.try_into().unwrap());
        obj.insert("altitude", 650.6.try_into().unwrap());
        obj.insert("accuracy", 12.0.try_into().unwrap());
        obj.insert("altitudeAccuracy", 10.0.try_into().unwrap());
        obj.insert("heading", 237.0.try_into().unwrap());
        obj.insert("speed", 250.0.try_into().unwrap());

        let buf = AstarteSdk::serialize_object(AstarteSdk::to_bson_map(obj.clone()), None).unwrap();

        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1",
            &buf,
            &None,
        )
        .unwrap();

        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1/2/3",
            &buf,
            &None,
        )
        .unwrap_err();

        ifa.validate_send("org.doesnotexists.doesnotexists", "/1/", &buf, &None)
            .unwrap_err();

        // nonexisting object field
        let mut obj2 = obj.clone();
        obj2.insert("latitudef", 37.534543.try_into().unwrap());
        let buf = AstarteSdk::serialize_object(AstarteSdk::to_bson_map(obj2), None).unwrap();
        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1",
            &buf,
            &None,
        )
        .unwrap_err();

        // wrong type
        let mut obj2 = obj.clone();
        obj2.insert("latitude", AstarteType::Boolean(false));
        let buf = AstarteSdk::serialize_object(AstarteSdk::to_bson_map(obj2), None).unwrap();
        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1",
            &buf,
            &None,
        )
        .unwrap_err();

        // missing object field
        let mut obj2 = obj.clone();
        obj2.remove("latitude");
        let buf = AstarteSdk::serialize_object(AstarteSdk::to_bson_map(obj2), None).unwrap();
        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1",
            &buf,
            &None,
        )
        .unwrap_err();

        // invalid float
        let mut obj2 = obj.clone();
        obj2.insert("latitude", AstarteType::Double(f64::NAN));
        let buf = AstarteSdk::serialize_object(AstarteSdk::to_bson_map(obj2), None).unwrap();
        ifa.validate_send(
            "org.astarte-platform.genericsensors.Geolocation",
            "/1",
            &buf,
            &None,
        )
        .unwrap_err();
    }

    #[test]
    fn test_individual_recv() {
        let mut options = AstarteOptions::new("test", "test", "test", "test");
        options.interface_directory("examples/interfaces/").unwrap();
        let ifa = super::Interfaces::new(options.interfaces);

        let boolean_buf =
            AstarteSdk::serialize_individual(AstarteType::Boolean(true), None).unwrap();
        let integer_buf = AstarteSdk::serialize_individual(AstarteType::Integer(23), None).unwrap();

        ifa.validate_receive(
            "org.astarte-platform.genericsensors.SamplingRate",
            "/2/enable",
            &boolean_buf,
        )
        .unwrap();

        ifa.validate_receive(
            "org.astarte-platform.genericsensors.SamplingRate",
            "/2/enable",
            &integer_buf,
        )
        .unwrap_err();

        ifa.validate_receive(
            "org.astarte-platform.genericsensors.SamplingRate",
            "/2/3/4/enable",
            &boolean_buf,
        )
        .unwrap_err();

        ifa.validate_receive(
            "org.astarte-platform.genericsensors.SamplingRate",
            "/nope/enable",
            &boolean_buf,
        )
        .unwrap();
    }

    #[test]
    fn test_object_recv() {
        use std::str::FromStr;

        let interface_json = r#"
        {
            "interface_name": "com.test.object",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "server",
            "aggregation": "object",
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

        let ifa = super::Interfaces::new(ifa);

        let inner_data: HashMap<&str, AstarteType> = [
            ("button", AstarteType::Boolean(false)),
            ("uptimeSeconds", AstarteType::Integer(324)),
        ]
        .iter()
        .cloned()
        .collect();
        let buf = AstarteSdk::serialize_object(AstarteSdk::to_bson_map(inner_data), None).unwrap();

        ifa.validate_receive("com.test.object", "/", &buf).unwrap();
        ifa.validate_receive("com.test.object", "/no", &buf)
            .unwrap_err();
        ifa.validate_receive("com.test.no", "/", &buf).unwrap_err();

        let inner_data: HashMap<&str, AstarteType> = [
            ("buttonfoo", AstarteType::Boolean(false)),
            ("uptimeSeconds", AstarteType::Integer(324)),
        ]
        .iter()
        .cloned()
        .collect();
        let buf = AstarteSdk::serialize_object(AstarteSdk::to_bson_map(inner_data), None).unwrap();

        ifa.validate_receive("com.test.object", "/", &buf)
            .unwrap_err();

        let inner_data: HashMap<&str, AstarteType> = [
            ("button", AstarteType::Double(3.3)),
            ("uptimeSeconds", AstarteType::Integer(324)),
        ]
        .iter()
        .cloned()
        .collect();
        let buf = AstarteSdk::serialize_object(AstarteSdk::to_bson_map(inner_data), None).unwrap();

        ifa.validate_receive("com.test.object", "/", &buf)
            .unwrap_err();
    }

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

        let ifa = super::Interfaces::new(ifa);

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

        let ifa = super::Interfaces::new(ifa);

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

        let ifa = super::Interfaces::new(ifa);

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

        let ifa = super::Interfaces::new(ifa);

        assert!(
            ifa.get_ownership("org.astarte-platform.device-owned.test")
                .unwrap()
                == crate::interface::Ownership::Device
        );
    }
}
