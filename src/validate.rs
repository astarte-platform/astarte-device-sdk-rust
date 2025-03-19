// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Validate the submission and reception of a payload.

use tracing::{error, trace, warn};

use crate::{
    aggregate::AstarteObject,
    error::Report,
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef, PropertyRef},
        MappingAccess, Ownership, Reliability, Retention,
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
    #[error("sending timestamp to a mapping without `explicit_timestamp`")]
    Timestamp,
    /// Sending data on an interface not owned by the device
    #[error("expected interface with ownership {exp}, but got {got}")]
    Ownership { exp: Ownership, got: Ownership },
    /// Missing mapping in send payload
    #[error["missing mappings in the payload"]]
    MissingMapping,
    /// Mismatching type while serializing
    #[error("mismatching type while serializing, expected {expected} but got {got}")]
    SerializeType { expected: String, got: String },
    /// Couldn't accept unset for mapping without `allow_unset`
    #[error("couldn't unset property {interface}{mapping} without `allow_unset`")]
    Unset { interface: String, mapping: String },
}

/// Optionals check on the sent individual payload.
fn optional_individual_checks(
    mapping: MappingRef<&Interface>,
    timestamp: &Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), UserValidationError> {
    if mapping.interface().ownership() != Ownership::Device {
        return Err(UserValidationError::Ownership {
            exp: Ownership::Device,
            got: mapping.interface().ownership(),
        });
    }

    if !mapping.mapping().explicit_timestamp() && timestamp.is_some() {
        return Err(UserValidationError::Timestamp);
    }

    Ok(())
}

/// Optionals check on the sent object payload.
fn optional_object_checks(
    object: ObjectRef,
    timestamp: &Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), UserValidationError> {
    if !object.explicit_timestamp() && timestamp.is_some() {
        return Err(UserValidationError::Timestamp);
    }

    Ok(())
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
        path: &MappingPath<'_>,
        data: AstarteType,
        timestamp: Option<Timestamp>,
    ) -> Result<ValidatedIndividual, UserValidationError> {
        if let Err(err) = optional_individual_checks(mapping, &timestamp) {
            error!(error = %Report::new(&err), "send validation failed");

            #[cfg(debug_assertions)]
            return Err(err);
        }

        if data != mapping.mapping_type() {
            return Err(UserValidationError::SerializeType {
                expected: mapping.mapping_type().to_string(),
                got: data.display_type().to_string(),
            });
        }

        let interface = mapping.interface();
        Ok(ValidatedIndividual {
            interface: interface.interface_name().to_string(),
            path: path.to_string(),
            version_major: interface.version_major(),
            reliability: mapping.mapping().reliability(),
            retention: mapping.mapping().retention(),
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
        if let Err(err) = optional_object_checks(interface, &timestamp) {
            error!(error = %Report::new(&err), "send validation failed");

            #[cfg(debug_assertions)]
            return Err(err);
        }

        if data.len() != interface.len() {
            return Err(UserValidationError::MissingMapping);
        }

        // Filter only the valid fields
        data.iter().try_for_each(|(key, value)| {
            let Some(mapping) = interface.get_field(path, key) else {
                warn!("unrecognized mapping {path}/{key}, ignoring");

                return Ok(());
            };

            if *value != mapping.mapping_type() {
                return Err(UserValidationError::SerializeType {
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
        path: &MappingPath<'_>,
    ) -> Result<Self, UserValidationError> {
        if mapping.interface().ownership() != Ownership::Device {
            return Err(UserValidationError::Ownership {
                exp: Ownership::Device,
                got: mapping.interface().ownership(),
            });
        }

        if !mapping.allow_unset() {
            return Err(UserValidationError::Unset {
                interface: mapping.interface().interface_name().to_string(),
                mapping: path.to_string(),
            });
        }

        Ok(Self {
            interface: mapping.interface().interface_name().to_string(),
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{interface::mapping::path::tests::mapping, interfaces::tests::DEVICE_OBJECT};

    use super::*;

    use chrono::{TimeZone, Utc};

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
        let (interface, _aggregate) = initialize_aggregate();
        let object = interface.as_object_ref().unwrap();

        // Test sending an aggregate (with and without timestamp)
        let timestamp = Some(TimeZone::timestamp_opt(&Utc, 1537449422, 0).unwrap());
        optional_object_checks(object, &None).unwrap();
        optional_object_checks(object, &timestamp).unwrap();
    }

    #[test]
    fn test_validate_send_for_aggregate_datastream_extra_field() {
        let (interface, mut aggregate) = initialize_aggregate();
        let object = interface.as_object_ref().unwrap();

        // Test sending an aggregate with an non existing object field
        aggregate.insert("gibberish".to_string(), AstarteType::Boolean(false));
        let res = optional_object_checks(object, &None);
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_send_for_individual_datastream() {
        let interface = Interface::from_str(DEVICE_DATASTREAM).unwrap();

        let path = mapping("/boolean_endpoint");
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let res = optional_individual_checks(mapping, &None);
        assert!(res.is_ok(), "error: {}", res.unwrap_err());

        let res = optional_individual_checks(mapping, &Some(Utc::now()));
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_send_for_server() {
        let interface = Interface::from_str(SERVER_DATASTREAM).unwrap();

        let path = mapping("/boolean_endpoint");
        let mapping = MappingRef::new(&interface, &path).unwrap();

        let res = optional_individual_checks(mapping, &None);
        assert!(res.is_err());
    }
}
