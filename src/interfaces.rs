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

use std::{
    borrow::Borrow,
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    ops::Deref,
};

use log::debug;

use crate::{
    interface::{mapping::path::MappingPath, DatastreamObject, InterfaceError, Mapping},
    Error, Interface,
};

/// Struct to hold a reference to an interface, which is a property.
///
/// This type can be use to guaranty that the interface is a property when accessed from the
/// [`Interfaces`] struct with the [get_property](`Interfaces::get_property`) method.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PropertyRef<'a>(pub(crate) &'a Interface);

impl<'a> Borrow<Interface> for PropertyRef<'a> {
    fn borrow(&self) -> &Interface {
        self.0
    }
}

impl<'a> Deref for PropertyRef<'a> {
    type Target = Interface;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

/// Reference to an [`Interface`] and a [`DatastreamObject`] that is guaranty to belong to the interface.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ObjectRef<'a> {
    pub(crate) interface: &'a Interface,
    pub(crate) object: &'a DatastreamObject,
}

impl<'a> ObjectRef<'a> {
    /// Create a new reference only if the interface is an object.
    pub(crate) fn new(interface: &'a Interface) -> Option<Self> {
        match &interface.inner {
            crate::interface::InterfaceType::DatastreamIndividual(_)
            | crate::interface::InterfaceType::Properties(_) => None,
            crate::interface::InterfaceType::DatastreamObject(object) => {
                Some(Self { interface, object })
            }
        }
    }
}

impl<'a> Deref for ObjectRef<'a> {
    type Target = DatastreamObject;

    fn deref(&self) -> &Self::Target {
        self.object
    }
}

/// Reference to an [`Interface`] and a [`Mapping`] that is guaranty to belong to the interface.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MappingRef<'a, I> {
    interface: I,
    mapping: Mapping<&'a str>,
}

impl<'a> MappingRef<'a, &'a Interface> {
    pub(crate) fn new(interface: &'a Interface, path: &MappingPath) -> Option<Self> {
        interface
            .mapping(path)
            .map(|mapping| Self { interface, mapping })
    }

    pub(crate) fn as_prop(&self) -> Option<MappingRef<'a, PropertyRef<'a>>> {
        self.interface().as_prop_ref().map(|interface| MappingRef {
            interface,
            mapping: self.mapping,
        })
    }
}

impl<'a> MappingRef<'a, PropertyRef<'a>> {
    pub(crate) fn with_prop(interface: PropertyRef<'a>, path: &MappingPath) -> Option<Self> {
        let mapping = interface.0.mapping(path)?;

        Some(Self { interface, mapping })
    }
}

impl<'a, I> MappingRef<'a, I> {
    #[inline]
    pub(crate) fn mapping(&self) -> &Mapping<&str> {
        &self.mapping
    }

    #[inline]
    pub(crate) fn interface(&self) -> &I {
        &self.interface
    }
}

impl<'a, I> Deref for MappingRef<'a, I> {
    type Target = Mapping<&'a str>;

    fn deref(&self) -> &Self::Target {
        &self.mapping
    }
}

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
        Introspection {
            interfaces: &self.interfaces,
        }
        .to_string()
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

impl FromIterator<(String, Interface)> for Interfaces {
    fn from_iter<T: IntoIterator<Item = (String, Interface)>>(iter: T) -> Self {
        Self {
            interfaces: iter.into_iter().collect(),
        }
    }
}

struct Introspection<'a> {
    interfaces: &'a HashMap<String, Interface>,
}

impl<'a> Display for Introspection<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.interfaces.iter();

        let Some((name, interface)) = iter.next() else {
            return Ok(());
        };

        let major = interface.version_major();
        let minor = interface.version_minor();
        write!(f, "{}:{}:{}", name, major, minor)?;

        for (name, interface) in iter {
            let major = interface.version_major();
            let minor = interface.version_minor();
            write!(f, ";{}:{}:{}", name, major, minor)?;
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;

    use crate::{
        error::Error, interface::MappingType, interfaces::Interfaces, mapping,
        options::AstarteOptions, Interface,
    };

    pub(crate) const PROPERTIES_SERVER: (&str, &str) = (
        "org.astarte-platform.test.test",
        r#"
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
        }"#,
    );

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

    pub(crate) fn create_interfaces(interfaces: &[(&str, &str)]) -> Interfaces {
        Interfaces::from_iter(
            interfaces
                .iter()
                .map(|(n, i)| (n.to_string(), Interface::from_str(i).unwrap())),
        )
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
