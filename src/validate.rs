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

use tracing::{error, trace};

use crate::{
    aggregate::AstarteObject,
    error::{AggregationError, InterfaceTypeError, OwnershipError},
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef, PropertyRef},
        Aggregation, MappingAccess, Ownership, Reliability, Retention,
    },
    store::PropertyInterface,
    types::AstarteType,
    Interface, Timestamp,
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
    #[error("couldn't get object mapping for {interface} with key {key}")]
    ObjectInvalidMapping { interface: String, key: String },
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
        mapping: MappingRef<'_, &Interface>,
        data: AstarteType,
        timestamp: Option<Timestamp>,
    ) -> Result<ValidatedIndividual, UserValidationError> {
        let interface = mapping.interface();
        let path = mapping.path();
        let mapping = mapping.mapping();

        let aggregation = interface.aggregation();
        if aggregation != Aggregation::Individual {
            return Err(UserValidationError::Aggregation(AggregationError::new(
                interface.interface_name().to_string(),
                path.to_string(),
                Aggregation::Individual,
                aggregation,
            )));
        }

        let ownership = interface.ownership();
        if ownership != Ownership::Device {
            return Err(UserValidationError::Ownership(OwnershipError::new(
                interface.interface_name(),
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
            interface.interface_name(),
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
        interface: ObjectRef<'_>,
        path: &MappingPath<'_>,
        data: AstarteObject,
        timestamp: Option<Timestamp>,
    ) -> Result<ValidatedObject, UserValidationError> {
        validate_timestamp(
            interface.interface.interface_name(),
            path.as_str(),
            &timestamp,
            interface.explicit_timestamp(),
        )?;

        if data.len() < interface.len() {
            // TODO: return the missing mappings
            return Err(UserValidationError::ObjectMissingMappings {
                interface: interface.interface.interface_name().to_string(),
                path: path.to_string(),
                missing: interface.len().saturating_sub(data.len()),
            });
        }

        // Check that all the fields are valid
        data.iter().try_for_each(|(key, value)| {
            let Some(mapping) = interface.get_field(path, key) else {
                return Err(UserValidationError::ObjectInvalidMapping {
                    interface: interface.interface.interface_name().to_string(),
                    key: key.to_string(),
                });
            };

            if *value != mapping.mapping_type() {
                return Err(UserValidationError::MappingType {
                    interface: interface.interface.interface_name().to_string(),
                    path: format!("{path}/{key}"),
                    expected: mapping.mapping_type().to_string(),
                    got: value.display_type().to_string(),
                });
            }

            trace!("valid object field {path} {}", value.display_type());

            Ok(())
        })?;

        Ok(ValidatedObject {
            interface: interface.interface.interface_name().to_string(),
            path: path.to_string(),
            version_major: interface.interface.version_major(),
            reliability: interface.reliability(),
            retention: interface.retention(),
            data,
            timestamp,
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
        mapping: MappingRef<'_, PropertyRef<'_>>,
    ) -> Result<Self, UserValidationError> {
        let interface = mapping.interface();
        let path = mapping.path();
        let mapping = mapping.mapping();

        let ownership = interface.ownership();
        if ownership != Ownership::Device {
            return Err(UserValidationError::Ownership(OwnershipError::new(
                interface.interface_name(),
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

impl<'a> From<&'a ValidatedUnset> for PropertyInterface<'a> {
    /// an unset is allowed to be sent only by the device [`ValidatedUnset::validate`]
    fn from(value: &'a ValidatedUnset) -> Self {
        Self::new(&value.interface, Ownership::Device)
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

    use crate::test::{DEVICE_PROPERTIES_NO_UNSET, SERVER_PROPERTIES};
    use crate::{interface::mapping::path::tests::mapping, interfaces::tests::DEVICE_OBJECT};

    use super::*;

    use chrono::Utc;

    const DEVICE_DATASTREAM: &str = include_str!(
        "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceDatastream.json"
    );

    const SERVER_DATASTREAM: &str = include_str!(
        "../e2e-test/interfaces/additional/org.astarte-platform.rust.e2etest.ServerDatastream.json"
    );

    fn initialize_aggregate() -> (Interface, AstarteObject) {
        let aggregate = AstarteObject::from_iter([
            (
                "double_endpoint".to_string(),
                AstarteType::Double(37.534543),
            ),
            ("integer_endpoint".to_string(), AstarteType::Integer(45)),
            ("boolean_endpoint".to_string(), AstarteType::Boolean(true)),
            (
                "booleanarray_endpoint".to_string(),
                AstarteType::BooleanArray(vec![true, false, true]),
            ),
        ]);

        let interface = Interface::from_str(DEVICE_OBJECT).unwrap();

        (interface, aggregate)
    }

    #[test]
    fn test_validate_send_for_aggregate_datastream() {
        let (interface, aggregate) = initialize_aggregate();
        let object = interface.as_object_ref().unwrap();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        // Test sending an aggregate (with and without timestamp)
        ValidatedObject::validate(object, &path, aggregate.clone(), Some(Utc::now())).unwrap();
        ValidatedObject::validate(object, &path, aggregate, None).unwrap_err();
    }

    #[test]
    fn validate_individual_check_aggregation() {
        let (interface, _aggregate) = initialize_aggregate();
        let path = MappingPath::try_from("/sensor_1/boolean_endpoint").unwrap();
        let object = MappingRef::new(&interface, &path).unwrap();

        // Test sending an aggregate (with and without timestamp)
        let err =
            ValidatedIndividual::validate(object, AstarteType::Boolean(false), Some(Utc::now()))
                .unwrap_err();

        assert!(matches!(err, UserValidationError::Aggregation(_)))
    }

    #[test]
    fn test_validate_send_for_aggregate_datastream_extra_field() {
        let (interface, mut aggregate) = initialize_aggregate();
        let object = interface.as_object_ref().unwrap();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        // Test sending an aggregate with an non existing object field
        let invalid_key = "gibberish";
        aggregate.insert(invalid_key.to_string(), AstarteType::Boolean(false));
        let res =
            ValidatedObject::validate(object, &path, aggregate, Some(Utc::now())).unwrap_err();
        assert!(matches!(
            res,
            UserValidationError::ObjectInvalidMapping { key, .. } if key == invalid_key
        ))
    }

    #[test]
    fn test_validate_send_for_individual_datastream() {
        let interface = Interface::from_str(DEVICE_DATASTREAM).unwrap();

        let path = mapping("/boolean_endpoint");
        let mapping = MappingRef::new(&interface, &path).unwrap();

        ValidatedIndividual::validate(mapping, AstarteType::Boolean(false), Some(Utc::now()))
            .unwrap();
        // Check timestamp
        ValidatedIndividual::validate(mapping, AstarteType::Boolean(false), None).unwrap_err();
    }

    #[test]
    fn individual_invalid_mapping_type() {
        let interface = Interface::from_str(DEVICE_DATASTREAM).unwrap();

        let path = mapping("/boolean_endpoint");
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let err =
            ValidatedIndividual::validate(mapping, AstarteType::Integer(42), Some(Utc::now()))
                .unwrap_err();

        assert!(matches!(err, UserValidationError::MappingType { .. }))
    }

    #[test]
    fn test_validate_send_for_server() {
        let interface = Interface::from_str(SERVER_DATASTREAM).unwrap();

        let path = mapping("/boolean_endpoint");
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let res =
            ValidatedIndividual::validate(mapping, AstarteType::Boolean(false), Some(Utc::now()));
        assert!(res.is_err());
    }

    #[test]
    fn object_datastream_invalid_type() {
        let (interface, mut aggregate) = initialize_aggregate();
        let object = interface.as_object_ref().unwrap();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        aggregate
            .insert("double_endpoint".to_string(), AstarteType::Boolean(false))
            .unwrap();

        let err = ValidatedObject::validate(object, &path, aggregate.clone(), Some(Utc::now()))
            .unwrap_err();

        assert!(matches!(
            err,
            UserValidationError::MappingType {
                path,
                ..
            } if path == "/sensor_1/double_endpoint",
        ))
    }

    #[test]
    fn object_datastream_missing_mapping() {
        let (interface, mut aggregate) = initialize_aggregate();
        let object = interface.as_object_ref().unwrap();
        let path = MappingPath::try_from("/sensor_1").unwrap();

        aggregate.remove("double_endpoint").unwrap();

        let err = ValidatedObject::validate(object, &path, aggregate.clone(), Some(Utc::now()))
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
        let interface = Interface::from_str(SERVER_PROPERTIES).unwrap();

        let path = mapping("/sensor_1/enable");
        let mapping = MappingRef::new(&interface, &path)
            .unwrap()
            .as_prop()
            .unwrap();

        let err = ValidatedUnset::validate(mapping).unwrap_err();

        assert!(matches!(err, UserValidationError::Ownership(_)));
    }

    #[test]
    fn validate_unset_invalid_no_allow_unset() {
        let interface = Interface::from_str(DEVICE_PROPERTIES_NO_UNSET).unwrap();

        let path = mapping("/sensor_1/enable");
        let mapping = MappingRef::new(&interface, &path)
            .unwrap()
            .as_prop()
            .unwrap();

        let err = ValidatedUnset::validate(mapping).unwrap_err();

        assert!(matches!(err, UserValidationError::Unset { .. }));
    }
}
