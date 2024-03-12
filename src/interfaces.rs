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

use std::borrow::Borrow;
use std::collections::{hash_map::Entry, HashMap};
use std::fmt::Display;
use std::ops::Deref;

use itertools::Itertools;
use log::debug;

use crate::{
    interface::{
        error::InterfaceError,
        mapping::path::MappingPath,
        reference::{MappingRef, PropertyRef},
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
        Introspection::new(self.interfaces.values()).to_string()
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
            .ok_or_else(|| Error::InterfaceNotFound {
                name: interface_name.to_string(),
            })
            .and_then(|interface| {
                MappingRef::new(interface, interface_path).ok_or_else(|| Error::MappingNotFound {
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
            .ok_or_else(|| Error::InterfaceNotFound {
                name: interface_name.to_string(),
            })
            .and_then(|interface| {
                interface.as_prop_ref().ok_or_else(|| Error::InterfaceType {
                    exp: crate::interface::InterfaceTypeDef::Properties,
                    got: interface.interface_type(),
                })
            })
            .and_then(|interface| {
                MappingRef::with_prop(interface, interface_path).ok_or_else(|| {
                    Error::MappingNotFound {
                        interface: interface_name.to_string(),
                        mapping: interface_path.to_string(),
                    }
                })
            })
    }

    /// Iterate over the interfaces
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Interface> {
        self.interfaces.values()
    }

    /// Validate that one or more interfaces can be inserted in the collection.
    pub(crate) fn validate_many<I>(&self, interfaces: I) -> Result<ValidatedCollection, Error>
    where
        I: IntoIterator<Item = Interface>,
    {
        interfaces
            .into_iter()
            .map(|i| {
                match self.get(i.interface_name()) {
                    Some(prev) => {
                        debug!(
                            "Interface {} already present, validating new version",
                            i.interface_name()
                        );

                        i.validate_with(prev)?;
                    }
                    None => {
                        debug!("Interface {} not present, adding it", i.interface_name());
                    }
                }
                Ok((i.interface_name().to_string(), i))
            })
            .try_collect()
            .map(ValidatedCollection)
    }

    /// Extend the interfaces with the validated ones.
    pub(crate) fn extend(&mut self, interfaces: ValidatedCollection) {
        self.interfaces.extend(interfaces.0);
    }

    /// Iterator over the resulting added interfaces
    pub(crate) fn iter_with_added<'a>(
        &'a self,
        added: &'a HashMap<String, Interface>,
    ) -> impl Iterator<Item = &'a Interface> + Clone {
        self.interfaces
            .values()
            .filter(|i| !added.contains_key(i.interface_name()))
            .chain(added.values())
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

#[derive(Debug)]
pub(crate) struct ValidatedCollection(HashMap<String, Interface>);

impl Borrow<HashMap<String, Interface>> for ValidatedCollection {
    fn borrow(&self) -> &HashMap<String, Interface> {
        &self.0
    }
}

impl Deref for ValidatedCollection {
    type Target = HashMap<String, Interface>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct Introspection<I> {
    iter: I,
}

impl<I> Introspection<I> {
    pub(crate) fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<'a, I> Display for Introspection<I>
where
    I: Iterator<Item = &'a Interface> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.iter.clone();

        if let Some(interface) = iter.next() {
            let name = interface.interface_name();
            let major = interface.version_major();
            let minor = interface.version_minor();
            write!(f, "{}:{}:{}", name, major, minor)?;
        } else {
            return Ok(());
        };

        for interface in iter {
            let name = interface.interface_name();
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

    use itertools::Itertools;

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
            Err(Error::MappingNotFound { .. })
        ));
        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", mapping!("/foo/button")),
            Err(Error::MappingNotFound { .. })
        ));
        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", mapping!("/obj")),
            Err(Error::MappingNotFound { .. })
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
            Err(Error::MappingNotFound { .. })
        ));
        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", mapping!("/obj")),
            Err(Error::MappingNotFound { .. })
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

    #[tokio::test]
    async fn success_extend_interfaces() {
        let itfs = [
            Interface::from_str(include_str!(
                "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceProperty.json"
            ))
            .unwrap(),
            Interface::from_str(include_str!(
                "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.ServerProperty.json"
            ))
            .unwrap(),
        ];

        let mut interfaces = Interfaces::from_iter(itfs);

        let ifaces = [
            Interface::from_str(include_str!(
                "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceAggregate.json"
            ))
            .unwrap(),
            Interface::from_str(include_str!(
                "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.ServerAggregate.json"
            ))
            .unwrap(),
        ];

        let validated = interfaces.validate_many(ifaces).unwrap();

        interfaces.extend(validated);

        let mut interfaces = interfaces.iter().map(|i| i.interface_name()).collect_vec();

        interfaces.sort_unstable();

        let expected = [
            "org.astarte-platform.rust.e2etest.DeviceAggregate",
            "org.astarte-platform.rust.e2etest.DeviceProperty",
            "org.astarte-platform.rust.e2etest.ServerAggregate",
            "org.astarte-platform.rust.e2etest.ServerProperty",
        ];

        assert_eq!(expected.as_slice(), interfaces);
    }
}
