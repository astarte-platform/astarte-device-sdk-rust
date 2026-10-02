// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
use astarte_interfaces::Interface;

use crate::Timestamp;
use crate::error::InterfaceError;
use crate::interfaces::ValidatedCollection;
use crate::retention::RetentionId;
use crate::transport::RemovedInterface;

use self::individual::ValidatedIndividual;
use self::object::ValidatedObject;
use self::properties::{ValidatedProperty, ValidatedUnset};

pub(crate) mod individual;
pub(crate) mod object;
pub(crate) mod properties;

/// Used to send across to connection channel.
// TODO: not sure if all the fields in validated are required to send the data
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Validated {
    /// Individual datastream to publish.
    Individual {
        retention: Option<RetentionId>,
        data: ValidatedIndividual,
    },
    /// Object datastream to publish.
    Object {
        retention: Option<RetentionId>,
        data: ValidatedObject,
    },
    /// Property to set.
    Property {
        /// Stored epoch to mark the value as sent
        epoch: u8,
        /// Property data
        data: ValidatedProperty,
    },
    /// Property to unset.
    Unset {
        /// Stored epoch to mark the value as sent
        epoch: u8,
        /// Property data
        data: ValidatedUnset,
    },
    /// Interface to add.
    ///
    /// Pass the full interface since on gRPC we need to send the full interfaces as JSON
    AddInterface(Interface),
    /// Interface to add.
    ///
    /// Pass the full interface since on gRPC we need to send the full interfaces as JSON
    // TODO: remove the hashmap
    ExtendInterfaces(ValidatedCollection),
    /// Name of the interface to remove
    RemoveInterface(RemovedInterface),
    /// Name of the interface to remove
    RemoveInterfaceMany(Vec<RemovedInterface>),
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
        .set_ctx(format!("for {name}{path}"))),
        (Some(_), false) => Err(Error::with(
            InterfaceError::Timestamp,
            "sent timestamp when set to false",
        )
        .set_ctx(format!("for {name}{path}"))),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::AstarteData;
    use crate::aggregate::AstarteObject;
    use crate::interfaces::MappingRef;
    use crate::test::{DEVICE_OBJECT, DEVICE_PROPERTIES_NO_UNSET, SERVER_PROPERTIES};
    use crate::validate::properties::ValidatedUnset;

    use super::*;

    use astarte_interfaces::{
        DatastreamIndividual, DatastreamObject, Interface, MappingPath, Properties,
    };
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
