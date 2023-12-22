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
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, PropertyRef},
        InterfaceError,
    },
    Error, Interface,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct Interfaces {
    interfaces: HashMap<String, Interface>,
}

impl Interfaces {
    pub(crate) fn new() -> Self {
        Self::default()
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

    pub(crate) fn get_property(&self, interface_name: &str) -> Option<PropertyRef> {
        self.interfaces
            .get(interface_name)
            .and_then(|interface| interface.is_property().then_some(PropertyRef(interface)))
    }

    /// Gets an interface and mapping given the name and path.
    ///
    /// # Errors
    ///
    /// Will return error if either the interface or the mapping is missing.
    pub(crate) fn interface_mapping(
        &self,
        interface_name: &str,
        interface_path: &MappingPath,
    ) -> Result<MappingRef<&Interface>, Error> {
        self.interfaces
            .get(interface_name)
            .ok_or_else(|| Error::MissingInterface(interface_name.to_string()))
            .and_then(|interface| {
                MappingRef::new(interface, interface_path).ok_or_else(|| Error::MissingMapping {
                    interface: interface_name.to_string(),
                    mapping: interface_path.to_string(),
                })
            })
    }

    /// Gets a property and mapping given the name and path.
    ///
    /// # Errors
    ///
    /// Will return error if either the interface or the mapping is missing.
    pub(crate) fn property_mapping(
        &self,
        interface_name: &str,
        interface_path: &MappingPath,
    ) -> Result<MappingRef<PropertyRef>, Error> {
        self.interfaces
            .get(interface_name)
            .ok_or_else(|| Error::MissingInterface(interface_name.to_string()))
            .and_then(|interface| {
                interface.as_prop_ref().ok_or_else(|| Error::InterfaceType {
                    exp: crate::interface::InterfaceTypeDef::Properties,
                    got: interface.interface_type(),
                })
            })
            .and_then(|interface| {
                MappingRef::with_prop(interface, interface_path).ok_or_else(|| {
                    Error::MissingMapping {
                        interface: interface_name.to_string(),
                        mapping: interface_path.to_string(),
                    }
                })
            })
    }

    pub(crate) fn iter_interfaces(&self) -> impl Iterator<Item = &Interface> {
        self.interfaces.values()
    }
}

impl FromIterator<Interface> for Interfaces {
    fn from_iter<T: IntoIterator<Item = Interface>>(iter: T) -> Self {
        Self {
            interfaces: iter
                .into_iter()
                .map(|i| (i.interface_name().to_string(), i))
                .collect(),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;

    use crate::{
        builder::DeviceBuilder, error::Error, interface::MappingType, interfaces::Interfaces,
        mapping, Interface,
    };

    pub(crate) const PROPERTIES_SERVER: &str = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 12,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/button",
                    "type": "boolean"
                },
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer"
                },
                {
                    "endpoint": "/%{sensor_id}/enable",
                    "type": "boolean",
                    "allow_unset": true
                },
                {
                    "endpoint": "/%{sensor_id}/samplingPeriod",
                    "type": "integer",
                    "allow_unset": true
                }
            ]
        }"#;

    pub(crate) const DEVICE_OBJECT: &str = r#"
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

    pub(crate) fn create_interfaces(interfaces: &[&str]) -> Interfaces {
        Interfaces::from_iter(interfaces.iter().map(|i| Interface::from_str(i).unwrap()))
    }

    pub(crate) struct CheckEndpoint<'a> {
        interfaces: &'a Interfaces,
        name: &'a str,
        path: &'a str,
        version_major: i32,
        mapping_type: MappingType,
        endpoint: &'a str,
        explicit_timestamp: bool,
        allow_unset: bool,
    }

    impl<'a> CheckEndpoint<'a> {
        pub(crate) fn check(&self) {
            let path = mapping!(self.path);
            let mapping = self.interfaces.interface_mapping(self.name, path).unwrap();
            assert_eq!(mapping.interface().interface_name(), self.name);
            assert_eq!(mapping.interface().version_major(), self.version_major);
            assert_eq!(mapping.mapping_type(), self.mapping_type);
            assert_eq!(*mapping.endpoint(), self.endpoint);
            assert_eq!(mapping.explicit_timestamp(), self.explicit_timestamp);
            assert_eq!(mapping.allow_unset(), self.allow_unset);
        }
    }

    #[test]
    fn test_get_property_simple() {
        let ifa = create_interfaces(&[PROPERTIES_SERVER]);

        CheckEndpoint {
            interfaces: &ifa,
            name: "org.astarte-platform.test.test",
            path: "/button",
            version_major: 12,
            mapping_type: MappingType::Boolean,
            endpoint: "/button",
            explicit_timestamp: false,
            allow_unset: false,
        }
        .check();

        CheckEndpoint {
            interfaces: &ifa,
            name: "org.astarte-platform.test.test",
            path: "/uptimeSeconds",
            version_major: 12,
            mapping_type: MappingType::Integer,
            endpoint: "/uptimeSeconds",
            explicit_timestamp: false,
            allow_unset: false,
        }
        .check();

        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", mapping!("/button/foo")),
            Err(Error::MissingMapping { .. })
        ));
        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", mapping!("/foo/button")),
            Err(Error::MissingMapping { .. })
        ));
        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", mapping!("/obj")),
            Err(Error::MissingMapping { .. })
        ));
    }

    #[test]
    fn test_get_property_endpoint() {
        let ifa = create_interfaces(&[PROPERTIES_SERVER]);

        let parameters = ["1", "999999", "foobar"];

        for parameter in parameters {
            CheckEndpoint {
                interfaces: &ifa,
                name: "org.astarte-platform.test.test",
                path: &format!("/{parameter}/enable"),
                version_major: 12,
                mapping_type: MappingType::Boolean,
                endpoint: "/%{sensor_id}/enable",
                explicit_timestamp: false,
                allow_unset: true,
            }
            .check();
        }

        assert!(matches!(
            ifa.interface_mapping(
                "org.astarte-platform.test.test",
                mapping!("/foo/bar/enable")
            ),
            Err(Error::MissingMapping { .. })
        ));
        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", mapping!("/obj")),
            Err(Error::MissingMapping { .. })
        ));
    }

    #[test]
    fn test_get_introspection_string() {
        let mut options = DeviceBuilder::new();
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
