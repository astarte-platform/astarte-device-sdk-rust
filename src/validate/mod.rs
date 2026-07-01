// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
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

//! Validate the submission and reception of a payload.

use astarte_device_error::Error;
use astarte_interfaces::interface::Retention;
use astarte_interfaces::schema::{Ownership, Reliability};
use astarte_interfaces::{
    DatastreamIndividual, DatastreamObject, InterfaceMapping, MappingPath, Properties, Schema,
};
use tracing::trace;

use crate::Timestamp;
use crate::aggregate::AstarteObject;
use crate::error::InterfaceError;
use crate::interfaces::MappingRef;
use crate::types::AstarteData;

use self::object::Iter;

mod object;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValidatedIndividual {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) version_major: i32,
    pub(crate) reliability: Reliability,
    pub(crate) retention: Retention,
    pub(crate) data: AstarteData,
    pub(crate) timestamp: Option<Timestamp>,
}

impl ValidatedIndividual {
    pub(crate) fn validate(
        mapping: MappingRef<'_, DatastreamIndividual>,
        data: AstarteData,
        timestamp: Option<Timestamp>,
    ) -> Result<ValidatedIndividual, Error<InterfaceError>> {
        let interface = mapping.interface();
        let path = mapping.path();
        let mapping = mapping.mapping();

        let ownership = interface.ownership();
        if ownership != Ownership::Device {
            return Err(Error::new(InterfaceError::Ownership).set_message(format!(
                "for sending on {}, not a device interface",
                interface.name(),
            )));
        }

        if !data.eq_mapping_type(mapping.mapping_type()) {
            return Err(Error::new(InterfaceError::MappingType).set_message(format!(
                "for interface {interface}{path}, expected {} but got {}",
                mapping.mapping_type(),
                data.display_type()
            )));
        }

        validate_timestamp(
            interface.interface_name().as_str(),
            path.as_str(),
            &timestamp,
            mapping.explicit_timestamp(),
        )?;

        Ok(ValidatedIndividual {
            interface: interface.interface_name().to_string(),
            path: path.to_string(),
            version_major: interface.version_major(),
            reliability: mapping.reliability(),
            retention: mapping.retention(),
            data,
            timestamp,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValidatedObject {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) version_major: i32,
    pub(crate) reliability: Reliability,
    pub(crate) retention: Retention,
    pub(crate) data: AstarteObject,
    pub(crate) timestamp: Option<Timestamp>,
}

impl ValidatedObject {
    pub(crate) fn validate(
        interface: &DatastreamObject,
        path: &MappingPath<'_>,
        mut data: AstarteObject,
        timestamp: Option<Timestamp>,
    ) -> Result<ValidatedObject, Error<InterfaceError>> {
        let ownership = interface.ownership();
        if ownership != Ownership::Device {
            return Err(Error::new(InterfaceError::Ownership).set_message(format!(
                "for sending on {}, not a device interface",
                interface.name(),
            )));
        }

        if !interface.is_object_path(path) {
            return Err(Error::new(InterfaceError::ObjectPath).set_message(format!(
                "for interface {} and path {path}",
                interface.name()
            )));
        }

        validate_timestamp(
            interface.name(),
            path.as_str(),
            &timestamp,
            interface.explicit_timestamp(),
        )?;

        data.inner.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        Self::check_mappings(interface, path, &data)?;

        Ok(ValidatedObject {
            interface: interface.interface_name().to_string(),
            path: path.to_string(),
            version_major: interface.version_major(),
            reliability: interface.reliability(),
            retention: interface.retention(),
            data,
            timestamp,
        })
    }

    /// Check the mappings of a DataStreamObject
    ///
    /// We assume the interface mappings and the Astarte object mappings are sorted beforehand, so
    /// we can compare them two by two.
    fn check_mappings(
        interface: &DatastreamObject,
        path: &MappingPath<'_>,
        data: &AstarteObject,
    ) -> Result<(), Error<InterfaceError>> {
        debug_assert!(data.inner.is_sorted_by(|(a, _), (b, _)| a <= b));
        debug_assert!(
            interface
                .iter_mappings()
                .is_sorted_by(|a, b| a.endpoint() < b.endpoint())
        );

        Iter::new(data.iter(), interface.iter_mappings()).try_for_each(|(item, mapping)| {
            let Some(mapping) = mapping else {
                debug_assert!(item.is_some());

                let key = item.map(|(k, _)| k.as_str()).unwrap_or_default();

                return Err(Error::new(InterfaceError::MappingNotFound)
                    .set_message(format!("for interface {interface}{path} with key {key}")));
            };

            match item {
                Some((key, value)) => {
                    if !value.eq_mapping_type(mapping.mapping_type()) {
                        return Err(Error::new(InterfaceError::MappingType).set_message(format!(
                            "for interface {interface}{path}{key}, expected {} but got {}",
                            mapping.mapping_type(),
                            value.display_type()
                        )));
                    }

                    trace!("valid object field {path} {}", value.display_type());
                }
                None => {
                    if mapping.required() {
                        return Err(Error::new(InterfaceError::MappingRequired).set_message(
                            format!("for interface {interface} endpoint {}", mapping.endpoint()),
                        ));
                    }
                }
            }

            Ok(())
        })?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValidatedProperty {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) version_major: i32,
    pub(crate) data: AstarteData,
}

impl ValidatedProperty {
    pub(crate) fn validate(
        mapping: MappingRef<'_, Properties>,
        data: AstarteData,
    ) -> Result<Self, Error<InterfaceError>> {
        let interface = mapping.interface();
        let path = mapping.path();
        let mapping = mapping.mapping();

        let ownership = interface.ownership();
        if ownership != Ownership::Device {
            return Err(Error::new(InterfaceError::Ownership).set_message(format!(
                "for sending on {}, not a device interface",
                interface.name(),
            )));
        }

        if !data.eq_mapping_type(mapping.mapping_type()) {
            return Err(Error::new(InterfaceError::MappingType).set_message(format!(
                "for interface {interface}{path}, expected {} but got {}",
                mapping.mapping_type(),
                data.display_type()
            )));
        }

        Ok(Self {
            interface: interface.interface_name().to_string(),
            path: path.to_string(),
            version_major: interface.version_major(),
            data,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedUnset {
    pub(crate) interface: String,
    pub(crate) path: String,
}

impl ValidatedUnset {
    pub(crate) fn validate(
        mapping: MappingRef<'_, Properties>,
    ) -> Result<Self, Error<InterfaceError>> {
        let interface = mapping.interface();
        let path = mapping.path();
        let mapping = mapping.mapping();

        let ownership = interface.ownership();
        if ownership != Ownership::Device {
            return Err(Error::new(InterfaceError::Ownership).set_message(format!(
                "for sending on {}, not a device interface",
                interface.name(),
            )));
        }

        if !mapping.allow_unset() {
            return Err(Error::new(InterfaceError::Unset)
                .set_message(format!("for {}{path}, not allowed", interface.name(),)));
        }

        Ok(Self {
            interface: interface.interface_name().to_string(),
            path: path.to_string(),
        })
    }
}

fn validate_timestamp(
    name: &str,
    path: &str,
    timestamp: &Option<Timestamp>,
    explicit_timestamp: bool,
) -> Result<(), Error<InterfaceError>> {
    match (timestamp, explicit_timestamp) {
        (Some(_), true) | (None, false) => Ok(()),
        (None, true) => Err(Error::with(
            InterfaceError::Timestamp,
            "missing timestamp when set to true",
        )
        .set_message(format!("for {name}{path}"))),
        (Some(_), false) => Err(Error::with(
            InterfaceError::Timestamp,
            "sent timestamp when set to false",
        )
        .set_message(format!("for {name}{path}"))),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::test::{DEVICE_OBJECT, DEVICE_PROPERTIES_NO_UNSET, SERVER_PROPERTIES};

    use super::*;

    use astarte_interfaces::{Interface, MappingPath};
    use chrono::Utc;

    const DEVICE_DATASTREAM: &str = include_str!(
        "../../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceDatastream.json"
    );

    const SERVER_DATASTREAM: &str = include_str!(
        "../../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.ServerDatastream.json"
    );

    fn initialize_aggregate() -> (DatastreamObject, AstarteObject) {
        let aggregate = AstarteObject::from_iter([
            (
                "endpoint1".to_string(),
                AstarteData::try_from(37.534543).unwrap(),
            ),
            (
                "endpoint2".to_string(),
                AstarteData::String("Hello".to_string()),
            ),
            (
                "endpoint3".to_string(),
                AstarteData::BooleanArray(vec![true, false, true]),
            ),
        ]);

        let interface = DatastreamObject::from_str(DEVICE_OBJECT).unwrap();

        (interface, aggregate)
    }

    #[test]
    fn test_validate_send_for_aggregate_datastream() {
        let (object, aggregate) = initialize_aggregate();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        // Test sending an aggregate (with and without timestamp)
        ValidatedObject::validate(&object, &path, aggregate.clone(), Some(Utc::now())).unwrap();
        let _ = ValidatedObject::validate(&object, &path, aggregate, None).unwrap_err();
    }

    #[test]
    fn test_validate_send_for_aggregate_datastream_extra_field() {
        let (object, mut aggregate) = initialize_aggregate();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        // Test sending an aggregate with an non existing object field
        let invalid_key = "gibberish";
        aggregate.insert(invalid_key.to_string(), AstarteData::Boolean(false));

        let err =
            ValidatedObject::validate(&object, &path, aggregate, Some(Utc::now())).unwrap_err();

        assert_eq!(*err.kind(), InterfaceError::MappingNotFound);
    }

    #[test]
    fn test_validate_send_for_individual_datastream() {
        let interface = Interface::from_str(DEVICE_DATASTREAM).unwrap();
        let interface = interface.as_datastream_individual().unwrap();

        let path = MappingPath::try_from("/boolean_endpoint").unwrap();
        let mapping = MappingRef::new(interface, &path).unwrap();

        ValidatedIndividual::validate(mapping, AstarteData::Boolean(false), Some(Utc::now()))
            .unwrap();
        // Check timestamp
        let _ =
            ValidatedIndividual::validate(mapping, AstarteData::Boolean(false), None).unwrap_err();
    }

    #[test]
    fn individual_invalid_mapping_type() {
        let interface = DatastreamIndividual::from_str(DEVICE_DATASTREAM).unwrap();

        let path = MappingPath::try_from("/boolean_endpoint").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let err =
            ValidatedIndividual::validate(mapping, AstarteData::Integer(42), Some(Utc::now()))
                .unwrap_err();

        assert_eq!(*err.kind(), InterfaceError::MappingType)
    }

    #[test]
    fn test_validate_send_for_server() {
        let interface = DatastreamIndividual::from_str(SERVER_DATASTREAM).unwrap();

        let path = MappingPath::try_from("/boolean_endpoint").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let res =
            ValidatedIndividual::validate(mapping, AstarteData::Boolean(false), Some(Utc::now()));
        assert!(res.is_err());
    }

    #[test]
    fn object_datastream_invalid_type() {
        let (object, mut aggregate) = initialize_aggregate();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        aggregate.insert("endpoint1".to_string(), AstarteData::Boolean(false));

        let err = ValidatedObject::validate(&object, &path, aggregate.clone(), Some(Utc::now()))
            .unwrap_err();

        assert_eq!(*err.kind(), InterfaceError::MappingType, "{err}")
    }

    #[test]
    fn object_datastream_missing_mapping() {
        let (object, mut aggregate) = initialize_aggregate();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        aggregate.remove("endpoint1").unwrap();

        ValidatedObject::validate(&object, &path, aggregate.clone(), Some(Utc::now())).unwrap();
    }

    #[test]
    fn check_validate_explicit_timestamp() {
        let timestamp = Some(Utc::now());
        validate_timestamp("name", "path", &timestamp, true).unwrap();
        validate_timestamp("name", "path", &None, false).unwrap();

        let err = validate_timestamp("name", "path", &None, true).unwrap_err();
        assert_eq!(*err.kind(), InterfaceError::Timestamp);

        let err = validate_timestamp("name", "path", &timestamp, false).unwrap_err();
        assert_eq!(*err.kind(), InterfaceError::Timestamp);
    }

    #[test]
    fn validate_unset_invalid_server_prop() {
        let interface = Properties::from_str(SERVER_PROPERTIES).unwrap();

        let path = MappingPath::try_from("/sensor_1/enable").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let err = ValidatedUnset::validate(mapping).unwrap_err();

        assert_eq!(*err.kind(), InterfaceError::Ownership);
    }

    #[test]
    fn validate_unset_invalid_no_allow_unset() {
        let interface = Properties::from_str(DEVICE_PROPERTIES_NO_UNSET).unwrap();

        let path = MappingPath::try_from("/sensor_1/enable").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let err = ValidatedUnset::validate(mapping).unwrap_err();

        assert_eq!(*err.kind(), InterfaceError::Unset);
    }
}
