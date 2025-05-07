// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
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

use astarte_interfaces::{
    interface::Retention,
    schema::{Ownership, Reliability},
    DatastreamIndividual, DatastreamObject, InterfaceMapping, MappingPath, Properties, Schema,
};
use tracing::{error, trace};

use crate::{
    aggregate::AstarteObject,
    error::{AggregationError, InterfaceTypeError, OwnershipError},
    interfaces::MappingRef,
    types::AstarteType,
    Timestamp,
};

/// Errors returned while validating a send payload
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum UserValidationError {
    /// Sending timestamp to a mapping without `explicit_timestamp`
    #[error("{ctx} timestamp on {interface}{path}, but interface has 'explicit_timestamp: {explicit_timestamp}'")]
    Timestamp {
        /// Missing or sending context
        ctx: &'static str,
        /// Name of the interface.
        interface: String,
        /// Optional path the data is sent on
        path: String,
        /// Interface explicit timestamp.
        explicit_timestamp: bool,
    },
    /// Sending data on an interface not owned by the device
    #[error("sending data on an interface with an invalid ownership")]
    Ownership(#[from] OwnershipError),
    /// Using invalid method to send data of a different interface type.
    #[error("invalid method used to send data of a different interface type")]
    InterfaceType(#[from] InterfaceTypeError),
    /// Using invalid method to send data of a different aggregation.
    #[error("invalid method used to send data of a different aggregation")]
    Aggregation(#[from] AggregationError),
    /// The path provided is invalid for the object interface.
    #[error("invalid path {path} for Object Aggregate {interface}")]
    ObjectPath { interface: String, path: String },
    /// Missing mapping in the object data
    #[error("the data sent for the object {interface}{path} is missing {missing} mappings")]
    ObjectMissingMappings {
        interface: String,
        path: String,
        missing: usize,
    },
    /// Trying to send data on a mapping with a different type
    #[error("mismatching type while sending data on {interface}{path}, expected {expected} but got {got}")]
    MappingType {
        interface: String,
        path: String,
        expected: String,
        got: String,
    },
    /// Couldn't accept unset for mapping without `allow_unset`
    #[error("couldn't unset property {interface}{mapping} without `allow_unset`")]
    Unset { interface: String, mapping: String },
    /// Couldn't get the object mapping with the given key.
    #[error("couldn't get object mapping for {interface}{path} with key {key}")]
    ObjectInvalidMapping {
        interface: String,
        path: String,
        key: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValidatedIndividual {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) version_major: i32,
    pub(crate) reliability: Reliability,
    pub(crate) retention: Retention,
    pub(crate) data: AstarteType,
    pub(crate) timestamp: Option<Timestamp>,
}

impl ValidatedIndividual {
    pub(crate) fn validate(
        mapping: MappingRef<'_, DatastreamIndividual>,
        data: AstarteType,
        timestamp: Option<Timestamp>,
    ) -> Result<ValidatedIndividual, UserValidationError> {
        let interface = mapping.interface();
        let path = mapping.path();
        let mapping = mapping.mapping();

        let ownership = interface.ownership();
        if ownership != Ownership::Device {
            return Err(UserValidationError::Ownership(OwnershipError::new(
                interface.interface_name().clone(),
                Ownership::Device,
                ownership,
            )));
        }

        if data != mapping.mapping_type() {
            return Err(UserValidationError::MappingType {
                interface: interface.interface_name().to_string(),
                path: path.as_str().to_string(),
                expected: mapping.mapping_type().to_string(),
                got: data.display_type().to_string(),
            });
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
        data: AstarteObject,
        timestamp: Option<Timestamp>,
    ) -> Result<ValidatedObject, UserValidationError> {
        if !interface.is_object_path(path) {
            return Err(UserValidationError::ObjectPath {
                interface: interface.interface_name().to_string(),
                path: path.to_string(),
            });
        }

        validate_timestamp(
            interface.name(),
            path.as_str(),
            &timestamp,
            interface.explicit_timestamp(),
        )?;

        if data.len() < interface.mappings_len() {
            // TODO: return the missing mappings
            return Err(UserValidationError::ObjectMissingMappings {
                interface: interface.interface_name().to_string(),
                path: path.to_string(),
                missing: interface.mappings_len().saturating_sub(data.len()),
            });
        }

        // Check that all the fields are valid
        data.iter().try_for_each(|(key, value)| {
            let Some(mapping) = interface.mapping(key) else {
                return Err(UserValidationError::ObjectInvalidMapping {
                    interface: interface.interface_name().to_string(),
                    path: path.to_string(),
                    key: key.to_string(),
                });
            };

            if *value != mapping.mapping_type() {
                return Err(UserValidationError::MappingType {
                    interface: interface.interface_name().to_string(),
                    path: format!("{path}/{key}"),
                    expected: mapping.mapping_type().to_string(),
                    got: value.display_type().to_string(),
                });
            }

            trace!("valid object field {path} {}", value.display_type());

            Ok(())
        })?;

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
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValidatedProperty {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) version_major: i32,
    pub(crate) data: AstarteType,
}

impl ValidatedProperty {
    pub(crate) fn validate(
        mapping: MappingRef<'_, Properties>,
        data: AstarteType,
    ) -> Result<Self, UserValidationError> {
        let interface = mapping.interface();
        let path = mapping.path();
        let mapping = mapping.mapping();

        let ownership = interface.ownership();
        if ownership != Ownership::Device {
            return Err(UserValidationError::Ownership(OwnershipError::new(
                interface.interface_name().to_string(),
                Ownership::Device,
                ownership,
            )));
        }

        if data != mapping.mapping_type() {
            return Err(UserValidationError::MappingType {
                interface: interface.interface_name().to_string(),
                path: path.as_str().to_string(),
                expected: mapping.mapping_type().to_string(),
                got: data.display_type().to_string(),
            });
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
    ) -> Result<Self, UserValidationError> {
        let interface = mapping.interface();
        let path = mapping.path();
        let mapping = mapping.mapping();

        let ownership = interface.ownership();
        if ownership != Ownership::Device {
            return Err(UserValidationError::Ownership(OwnershipError::new(
                interface.interface_name().clone(),
                Ownership::Device,
                ownership,
            )));
        }

        if !mapping.allow_unset() {
            return Err(UserValidationError::Unset {
                interface: interface.interface_name().to_string(),
                mapping: path.to_string(),
            });
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
) -> Result<(), UserValidationError> {
    match (timestamp, explicit_timestamp) {
        (Some(_), true) | (None, false) => Ok(()),
        (None, true) => Err(UserValidationError::Timestamp {
            ctx: "missing",
            interface: name.to_string(),
            path: path.to_string(),
            explicit_timestamp,
        }),
        (Some(_), false) => Err(UserValidationError::Timestamp {
            ctx: "sending",
            interface: name.to_string(),
            path: path.to_string(),
            explicit_timestamp,
        }),
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
        "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceDatastream.json"
    );

    const SERVER_DATASTREAM: &str = include_str!(
        "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.ServerDatastream.json"
    );

    fn initialize_aggregate() -> (DatastreamObject, AstarteObject) {
        let aggregate = AstarteObject::from_iter([
            ("endpoint1".to_string(), AstarteType::Double(37.534543)),
            (
                "endpoint2".to_string(),
                AstarteType::String("Hello".to_string()),
            ),
            (
                "endpoint3".to_string(),
                AstarteType::BooleanArray(vec![true, false, true]),
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
        ValidatedObject::validate(&object, &path, aggregate, None).unwrap_err();
    }

    #[test]
    fn test_validate_send_for_aggregate_datastream_extra_field() {
        let (object, mut aggregate) = initialize_aggregate();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        // Test sending an aggregate with an non existing object field
        let invalid_key = "gibberish";
        aggregate.insert(invalid_key.to_string(), AstarteType::Boolean(false));
        let res =
            ValidatedObject::validate(&object, &path, aggregate, Some(Utc::now())).unwrap_err();
        assert!(matches!(
            res,
            UserValidationError::ObjectInvalidMapping { key, .. } if key == invalid_key
        ))
    }

    #[test]
    fn test_validate_send_for_individual_datastream() {
        let interface = Interface::from_str(DEVICE_DATASTREAM).unwrap();
        let interface = interface.as_datastream_individual().unwrap();

        let path = MappingPath::try_from("/boolean_endpoint").unwrap();
        let mapping = MappingRef::new(interface, &path).unwrap();

        ValidatedIndividual::validate(mapping, AstarteType::Boolean(false), Some(Utc::now()))
            .unwrap();
        // Check timestamp
        ValidatedIndividual::validate(mapping, AstarteType::Boolean(false), None).unwrap_err();
    }

    #[test]
    fn individual_invalid_mapping_type() {
        let interface = DatastreamIndividual::from_str(DEVICE_DATASTREAM).unwrap();

        let path = MappingPath::try_from("/boolean_endpoint").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let err =
            ValidatedIndividual::validate(mapping, AstarteType::Integer(42), Some(Utc::now()))
                .unwrap_err();

        assert!(matches!(err, UserValidationError::MappingType { .. }))
    }

    #[test]
    fn test_validate_send_for_server() {
        let interface = DatastreamIndividual::from_str(SERVER_DATASTREAM).unwrap();

        let path = MappingPath::try_from("/boolean_endpoint").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let res =
            ValidatedIndividual::validate(mapping, AstarteType::Boolean(false), Some(Utc::now()));
        assert!(res.is_err());
    }

    #[test]
    fn object_datastream_invalid_type() {
        let (object, mut aggregate) = initialize_aggregate();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        aggregate.insert("endpoint1".to_string(), AstarteType::Boolean(false));

        let err = ValidatedObject::validate(&object, &path, aggregate.clone(), Some(Utc::now()))
            .unwrap_err();

        assert!(
            matches!(err, UserValidationError::MappingType { .. }),
            "{err}"
        )
    }

    #[test]
    fn object_datastream_missing_mapping() {
        let (object, mut aggregate) = initialize_aggregate();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        aggregate.remove("endpoint1").unwrap();

        let err = ValidatedObject::validate(&object, &path, aggregate.clone(), Some(Utc::now()))
            .unwrap_err();

        assert!(matches!(
            err,
            UserValidationError::ObjectMissingMappings { .. }
        ))
    }

    #[test]
    fn check_validate_explicit_timestamp() {
        let timestamp = Some(Utc::now());
        validate_timestamp("name", "path", &timestamp, true).unwrap();
        validate_timestamp("name", "path", &None, false).unwrap();
        validate_timestamp("name", "path", &None, true).unwrap_err();
        validate_timestamp("name", "path", &timestamp, false).unwrap_err();
    }

    #[test]
    fn validate_unset_invalid_server_prop() {
        let interface = Properties::from_str(SERVER_PROPERTIES).unwrap();

        let path = MappingPath::try_from("/sensor_1/enable").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let err = ValidatedUnset::validate(mapping).unwrap_err();

        assert!(matches!(err, UserValidationError::Ownership(_)));
    }

    #[test]
    fn validate_unset_invalid_no_allow_unset() {
        let interface = Properties::from_str(DEVICE_PROPERTIES_NO_UNSET).unwrap();

        let path = MappingPath::try_from("/sensor_1/enable").unwrap();
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let err = ValidatedUnset::validate(mapping).unwrap_err();

        assert!(matches!(err, UserValidationError::Unset { .. }));
    }
}
