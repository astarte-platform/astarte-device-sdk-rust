// This file is part of Astarte.
//
// Copyright 2021 - 2025 SECO Mind Srl
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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Deref;

use astarte_interfaces::interface::InterfaceTypeAggregation;
use astarte_interfaces::schema::{Aggregation, InterfaceType};
use astarte_interfaces::{
    AggregationIndividual, DatastreamIndividual, DatastreamObject, MappingPath, Properties, Schema,
};
use astarte_interfaces::{Interface, error::Error as InterfaceError};
use itertools::Itertools;
use tracing::{debug, trace, warn};

use crate::error::AggregationError;
use crate::session::IntrospectionInterface;
use crate::validate::UserValidationError;
use crate::{Error, error::InterfaceTypeError};

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

        match self.get(interface.interface_name()) {
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
        DeviceIntrospection::new(self.interfaces.values()).to_string()
    }

    pub(crate) fn get(&self, interface_name: &str) -> Option<&Interface> {
        #[cfg(debug_assertions)]
        if interface_name.ends_with(".json") {
            warn!(
                interface_name,
                "the interface name passed ends in .json, this is commonly a BUG when the extension is copied from the interface file and should probably be removed"
            );
        }

        self.interfaces.get(interface_name)
    }

    /// Retrieves a datastream individual mapping and checks that it's valid.
    pub(crate) fn get_object<'a>(
        &'a self,
        interface_name: &str,
        path: &MappingPath<'_>,
    ) -> Result<&'a DatastreamObject, Error> {
        let interface = self
            .get(interface_name)
            .ok_or_else(|| Error::InterfaceNotFound {
                name: interface_name.to_string(),
            })?;

        let interface = match interface.inner() {
            InterfaceTypeAggregation::DatastreamIndividual(_) => {
                return Err(Error::Aggregation(AggregationError::new(
                    interface_name,
                    path.as_str(),
                    Aggregation::Object,
                    Aggregation::Individual,
                )));
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => datastream_object,
            InterfaceTypeAggregation::Properties(_) => {
                return Err(Error::InterfaceType(InterfaceTypeError::with_path(
                    interface_name,
                    path.as_str(),
                    InterfaceType::Datastream,
                    InterfaceType::Properties,
                )));
            }
        };

        if !interface.is_object_path(path) {
            return Err(Error::Validation(UserValidationError::ObjectPath {
                interface: interface.interface_name().to_string(),
                path: path.to_string(),
            }));
        }

        Ok(interface)
    }

    /// Retrieves a datastream individual mapping and checks that it's valid.
    pub(crate) fn get_individual<'a>(
        &'a self,
        interface_name: &str,
        path: &'a MappingPath<'_>,
    ) -> Result<MappingRef<'a, DatastreamIndividual>, Error> {
        let interface = self
            .get(interface_name)
            .ok_or_else(|| Error::InterfaceNotFound {
                name: interface_name.to_string(),
            })?;

        let interface = match interface.inner() {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                datastream_individual
            }
            InterfaceTypeAggregation::DatastreamObject(_) => {
                return Err(Error::Aggregation(AggregationError::new(
                    interface_name,
                    path.as_str(),
                    Aggregation::Individual,
                    Aggregation::Object,
                )));
            }
            InterfaceTypeAggregation::Properties(_) => {
                return Err(Error::InterfaceType(InterfaceTypeError::with_path(
                    interface_name,
                    path.as_str(),
                    InterfaceType::Datastream,
                    InterfaceType::Properties,
                )));
            }
        };

        let mapping = interface
            .mapping(path)
            .ok_or_else(|| Error::MappingNotFound {
                interface: interface_name.to_string(),
                mapping: path.to_string(),
            })?;

        Ok(MappingRef {
            interface,
            mapping,
            path,
        })
    }

    /// Retrieves a property mapping and checks that it's valid.
    pub(crate) fn get_property<'a>(
        &'a self,
        interface_name: &str,
        path: &'a MappingPath<'_>,
    ) -> Result<MappingRef<'a, Properties>, Error> {
        let interface = self
            .get(interface_name)
            .ok_or_else(|| Error::InterfaceNotFound {
                name: interface_name.to_string(),
            })?;

        let prop = interface.as_properties().ok_or_else(|| {
            InterfaceTypeError::new(
                interface_name,
                InterfaceType::Properties,
                interface.interface_type(),
            )
        })?;

        let mapping = prop.mapping(path).ok_or_else(|| Error::MappingNotFound {
            interface: interface_name.to_string(),
            mapping: path.to_string(),
        })?;

        Ok(MappingRef {
            interface: prop,
            mapping,
            path,
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

    pub(crate) fn matches<S: AsRef<str>>(&self, stored: &[IntrospectionInterface<S>]) -> bool {
        stored.len() == self.len()
            && stored.iter().all(|stored_i| {
                self.get(stored_i.name().as_ref()).is_some_and(|i| {
                    i.version_major() == stored_i.version_major()
                        && i.version_minor() == stored_i.version_minor()
                })
            })
    }

    pub(crate) fn len(&self) -> usize {
        self.interfaces.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.interfaces.is_empty()
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Validated {
    interface: Interface,
    major_change: bool,
}

impl Validated {
    pub(crate) fn interface(&self) -> &Interface {
        &self.interface
    }
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

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ValidatedCollection(HashMap<String, Validated>);

impl ValidatedCollection {
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

pub(crate) struct DeviceIntrospection<I> {
    iter: I,
}

impl<I> DeviceIntrospection<I> {
    pub(crate) fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<'a, I> Display for DeviceIntrospection<I>
where
    I: Iterator<Item = &'a Interface> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.iter.clone();

        if let Some(interface) = iter.next() {
            let name = interface.interface_name();
            let major = interface.version_major();
            let minor = interface.version_minor();
            write!(f, "{name}:{major}:{minor}")?;
        } else {
            return Ok(());
        };

        for interface in iter {
            let name = interface.interface_name();
            let major = interface.version_major();
            let minor = interface.version_minor();
            write!(f, ";{name}:{major}:{minor}")?;
        }

        Ok(())
    }
}

/// Reference to an [`Interface`] and a Mapping that is guaranty to belong to the interface.
#[derive(Debug)]
pub(crate) struct MappingRef<'a, I>
where
    I: astarte_interfaces::AggregationIndividual,
{
    interface: &'a I,
    mapping: &'a I::Mapping,
    path: &'a MappingPath<'a>,
}

impl<'a, I> MappingRef<'a, I>
where
    I: astarte_interfaces::AggregationIndividual,
{
    pub(crate) fn new(interface: &'a I, path: &'a MappingPath<'a>) -> Option<Self> {
        interface.mapping(path).map(|mapping| Self {
            interface,
            mapping,
            path,
        })
    }

    pub(crate) fn interface(&self) -> &I {
        self.interface
    }

    pub(crate) fn mapping(&self) -> &I::Mapping {
        self.mapping
    }

    pub(crate) fn path(&self) -> &MappingPath<'_> {
        self.path
    }
}

/// Wrong derive trait bounds.
impl<I> Clone for MappingRef<'_, I>
where
    I: astarte_interfaces::AggregationIndividual,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<I> Copy for MappingRef<'_, I> where I: astarte_interfaces::AggregationIndividual {}

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;

    use astarte_interfaces::schema::MappingType;
    use astarte_interfaces::{Endpoint, InterfaceMapping};
    use pretty_assertions::assert_eq;

    use super::*;

    use crate::builder::DeviceBuilder;
    use crate::test::{
        E2E_DEVICE_AGGREGATE, E2E_DEVICE_AGGREGATE_NAME, E2E_DEVICE_DATASTREAM,
        E2E_DEVICE_DATASTREAM_NAME, E2E_DEVICE_PROPERTY, E2E_DEVICE_PROPERTY_NAME,
        E2E_SERVER_PROPERTY,
    };

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

    pub(crate) fn mock_validated_interface(interface: Interface, major_change: bool) -> Validated {
        Validated {
            interface,
            major_change,
        }
    }

    pub(crate) fn mock_validated_collection(interfaces: &[Validated]) -> ValidatedCollection {
        ValidatedCollection(
            interfaces
                .iter()
                .cloned()
                .map(|i| (i.interface.interface_name().to_string(), i))
                .collect(),
        )
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
        let r = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();
        let itfs = [r.clone(), Interface::from_str(E2E_SERVER_PROPERTY).unwrap()];

        let interfaces = Interfaces::from_iter(itfs);

        let mut res = interfaces
            .iter_without_removed(&r)
            .map(|i| i.interface_name())
            .collect_vec();

        res.sort_unstable();

        let ex = ["org.astarte-platform.rust.e2etest.ServerProperty"];
        assert_eq!(res, ex)
    }

    #[test]
    fn get_individual() {
        let exp = Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap();
        let interfaces = Interfaces::from_iter([
            exp.clone(),
            Interface::from_str(E2E_DEVICE_PROPERTY).unwrap(),
        ]);

        let path = MappingPath::try_from("/double_endpoint").unwrap();

        let mapping = interfaces
            .get_individual(E2E_DEVICE_DATASTREAM_NAME, &path)
            .unwrap();

        let individual = DatastreamIndividual::from_str(E2E_DEVICE_DATASTREAM).unwrap();
        assert_eq!(*mapping.path(), path);
        assert_eq!(*mapping.interface(), individual);
        assert_eq!(
            *mapping.mapping().endpoint(),
            Endpoint::from_str("/double_endpoint").unwrap()
        );
        assert_eq!(mapping.mapping().mapping_type(), MappingType::Double);

        // Not found
        let err = interfaces
            .get_property(E2E_DEVICE_AGGREGATE_NAME, &path)
            .unwrap_err();
        assert!(matches!(err, Error::InterfaceNotFound { .. }));
        // Type
        let err = interfaces
            .get_property(E2E_DEVICE_DATASTREAM_NAME, &path)
            .unwrap_err();
        assert!(matches!(err, Error::InterfaceType { .. }), "{err:?}");
    }

    #[test]
    fn get_property() {
        let exp = Interface::from_str(E2E_DEVICE_PROPERTY).unwrap();
        let interfaces = Interfaces::from_iter([
            exp.clone(),
            Interface::from_str(E2E_DEVICE_DATASTREAM).unwrap(),
        ]);

        let path = MappingPath::try_from("/sensor1/double_endpoint").unwrap();

        let prop_mapping = interfaces
            .get_property(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap();

        assert_eq!(*prop_mapping.path(), path);
        assert_eq!(*prop_mapping.interface(), *exp.as_properties().unwrap());
        assert_eq!(prop_mapping.mapping().mapping_type(), MappingType::Double);

        // Not found
        let err = interfaces
            .get_property(E2E_DEVICE_AGGREGATE_NAME, &path)
            .unwrap_err();
        assert!(matches!(err, Error::InterfaceNotFound { .. }));
        // Type
        let err = interfaces
            .get_property(E2E_DEVICE_DATASTREAM_NAME, &path)
            .unwrap_err();
        assert!(matches!(err, Error::InterfaceType { .. }));
    }

    #[test]
    fn get_object() {
        let exp = Interface::from_str(E2E_DEVICE_AGGREGATE).unwrap();
        let interfaces = Interfaces::from_iter([
            exp.clone(),
            Interface::from_str(E2E_DEVICE_PROPERTY).unwrap(),
        ]);

        let path = MappingPath::try_from("/sendor_1").unwrap();

        let object = interfaces
            .get_object(E2E_DEVICE_AGGREGATE_NAME, &path)
            .unwrap();

        let exp = DatastreamObject::from_str(E2E_DEVICE_AGGREGATE).unwrap();
        assert_eq!(*object, exp);

        // is object path
        let wrong_path = MappingPath::try_from("/foo/bar").unwrap();
        let err = interfaces
            .get_object(E2E_DEVICE_AGGREGATE_NAME, &wrong_path)
            .unwrap_err();
        assert!(matches!(
            err,
            Error::Validation(UserValidationError::ObjectPath { .. })
        ));

        // Not found
        let err = interfaces
            .get_object(E2E_DEVICE_DATASTREAM_NAME, &path)
            .unwrap_err();
        assert!(matches!(err, Error::InterfaceNotFound { .. }));
        // Type
        let err = interfaces
            .get_object(E2E_DEVICE_PROPERTY_NAME, &path)
            .unwrap_err();
        assert!(matches!(err, Error::InterfaceType { .. }), "{err:?}");
    }
}
