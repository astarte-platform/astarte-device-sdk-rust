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
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Deref;

use itertools::Itertools;
use tracing::{debug, trace};

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
    pub(crate) fn add(&mut self, interface: Validated) -> Option<Interface> {
        self.interfaces
            .insert(interface.interface_name().to_string(), interface.interface)
    }

    /// Validate that an interface can be added.
    ///
    /// It will return [`None`] if the interface is already present.
    pub(crate) fn validate(
        &self,
        interface: Interface,
    ) -> Result<Option<Validated>, InterfaceError> {
        let mut major_change = false;

        match self.interfaces.get(interface.interface_name()) {
            Some(prev) => {
                trace!(
                    "Interface {} already present, validating new version",
                    interface.interface_name()
                );

                // Filter the interfaces that are already present
                if interface == *prev {
                    debug!("Interfaces are equal");

                    return Ok(None);
                }

                interface.validate_with(prev)?;

                major_change = interface.version_major() > prev.version_major();
            }
            None => {
                trace!("Interface {} not present", interface.interface_name());
            }
        }

        Ok(Some(Validated {
            interface,
            major_change,
        }))
    }

    pub(crate) fn remove(&mut self, interface_name: &str) -> Option<Interface> {
        self.interfaces.remove(interface_name)
    }

    /// Remove the interfaces which name is in the HashSet
    pub(crate) fn remove_many<I, S>(&mut self, to_remove: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for i in to_remove {
            self.remove(i.as_ref());
        }
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
    pub(crate) fn interface_mapping<'a>(
        &'a self,
        interface_name: &str,
        interface_path: &'a MappingPath,
    ) -> Result<MappingRef<'a, &'a Interface>, Error> {
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
    pub(crate) fn property_mapping<'a>(
        &'a self,
        interface_name: &str,
        interface_path: &'a MappingPath,
    ) -> Result<MappingRef<'a, PropertyRef<'a>>, Error> {
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
    pub(crate) fn validate_many<I>(
        &self,
        interfaces: I,
    ) -> Result<ValidatedCollection, InterfaceError>
    where
        I: IntoIterator<Item = Interface>,
    {
        interfaces
            .into_iter()
            .filter_map(|i| {
                let res = self.validate(i).transpose()?;

                Some(res.map(|v| (v.interface_name().to_string(), v)))
            })
            .try_collect()
            .map(ValidatedCollection)
    }

    /// Extend the interfaces with the validated ones.
    pub(crate) fn extend(&mut self, interfaces: ValidatedCollection) {
        self.interfaces
            .extend(interfaces.0.into_iter().map(|(k, v)| (k, v.interface)));
    }

    /// Iterator over the interface with the added one
    pub(crate) fn iter_with_added<'a>(
        &'a self,
        added: &'a Validated,
    ) -> impl Iterator<Item = &'a Interface> + Clone {
        // The validated is an interfaces that is not present
        self.interfaces
            .values()
            .chain(std::iter::once(&added.interface))
    }

    /// Iterator over the resulting added interfaces
    pub(crate) fn iter_with_added_many<'a>(
        &'a self,
        added: &'a ValidatedCollection,
    ) -> impl Iterator<Item = &'a Interface> + Clone {
        // The validated collection contains only interfaces that are not already present
        self.interfaces
            .values()
            .chain(added.values().map(|v| &v.interface))
    }

    /// Iter without removed interface
    pub(crate) fn iter_without_removed<'a>(
        &'a self,
        removed: &'a Interface,
    ) -> impl Iterator<Item = &'a Interface> + Clone {
        self.interfaces
            .values()
            .filter(|i| i.interface_name() != removed.interface_name())
    }

    /// Iter without many removed interfaces
    pub(crate) fn iter_without_removed_many<'a>(
        &'a self,
        removed: &'a HashMap<&str, &Interface>,
    ) -> impl Iterator<Item = &'a Interface> + Clone {
        self.interfaces
            .values()
            .filter(|i| !removed.contains_key(i.interface_name()))
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
pub(crate) struct Validated {
    interface: Interface,
    major_change: bool,
}

impl Validated {
    pub(crate) fn is_major_change(&self) -> bool {
        self.major_change
    }
}

impl Borrow<Interface> for Validated {
    fn borrow(&self) -> &Interface {
        &self.interface
    }
}

impl Deref for Validated {
    type Target = Interface;

    fn deref(&self) -> &Self::Target {
        &self.interface
    }
}

#[derive(Debug)]
pub(crate) struct ValidatedCollection(HashMap<String, Validated>);

impl ValidatedCollection {
    #[cfg(feature = "message-hub")]
    pub(crate) fn iter_interfaces(&self) -> impl Iterator<Item = &Interface> {
        self.0.values().map(|v| &v.interface)
    }
}

impl Borrow<HashMap<String, Validated>> for ValidatedCollection {
    fn borrow(&self) -> &HashMap<String, Validated> {
        &self.0
    }
}

impl Deref for ValidatedCollection {
    type Target = HashMap<String, Validated>;

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

    use super::*;

    use crate::{
        builder::DeviceBuilder,
        interface::{mapping::path::tests::mapping, MappingType},
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

    impl CheckEndpoint<'_> {
        pub(crate) fn check(&self) {
            let path = mapping(self.path);
            let mapping = self.interfaces.interface_mapping(self.name, &path).unwrap();
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
            ifa.interface_mapping("org.astarte-platform.test.test", &mapping("/button/foo")),
            Err(Error::MappingNotFound { .. })
        ));
        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", &mapping("/foo/button")),
            Err(Error::MappingNotFound { .. })
        ));
        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", &mapping("/obj")),
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
                &mapping("/foo/bar/enable")
            ),
            Err(Error::MappingNotFound { .. })
        ));
        assert!(matches!(
            ifa.interface_mapping("org.astarte-platform.test.test", &mapping("/obj")),
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
                "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.DeviceProperty.json"
            ))
            .unwrap(),
            Interface::from_str(include_str!(
                "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.ServerProperty.json"
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

    #[test]
    fn should_iter_with_added() {
        let itfs = [
            Interface::from_str(include_str!(
                "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.DeviceProperty.json"
            ))
            .unwrap(),
            Interface::from_str(include_str!(
                "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.ServerProperty.json"
            ))
            .unwrap(),
        ];

        let interfaces = Interfaces::from_iter(itfs);

        let i = Interface::from_str(include_str!(
            "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceAggregate.json"
        ))
        .unwrap();

        let v = interfaces.validate(i).unwrap().unwrap();

        let mut res = interfaces
            .iter_with_added(&v)
            .map(|i| i.interface_name())
            .collect_vec();

        res.sort_unstable();

        let ex = [
            "org.astarte-platform.rust.e2etest.DeviceAggregate",
            "org.astarte-platform.rust.e2etest.DeviceProperty",
            "org.astarte-platform.rust.e2etest.ServerProperty",
        ];
        assert_eq!(res, ex)
    }

    #[test]
    fn should_iter_without_removed() {
        let r = Interface::from_str(include_str!(
                "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.DeviceProperty.json"
            ))
            .unwrap();
        let itfs = [
            r.clone(),
            Interface::from_str(include_str!(
                "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.ServerProperty.json"
            ))
            .unwrap()
        ];

        let interfaces = Interfaces::from_iter(itfs);

        let mut res = interfaces
            .iter_without_removed(&r)
            .map(|i| i.interface_name())
            .collect_vec();

        res.sort_unstable();

        let ex = ["org.astarte-platform.rust.e2etest.ServerProperty"];
        assert_eq!(res, ex)
    }
}
