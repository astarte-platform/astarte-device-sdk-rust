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

use crate::{
    interface::reference::{MappingRef, ObjectRef},
    Interface,
};

/// Errors returning while validating a send or a receive
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    /// Sending timestamp to a mapping without `explicit_timestamp`
    #[error("sending timestamp to a mapping without `explicit_timestamp`")]
    Timestamp,
}

/// Optionals check on the sent individual payload.
pub(crate) fn validate_send_individual(
    mapping: MappingRef<&Interface>,
    timestamp: &Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), ValidationError> {
    if !mapping.mapping().explicit_timestamp() && timestamp.is_some() {
        return Err(ValidationError::Timestamp);
    }

    Ok(())
}

/// Optionals check on the sent object payload.
pub(crate) fn validate_send_object(
    object: ObjectRef,
    timestamp: &Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), ValidationError> {
    if !object.explicit_timestamp() && timestamp.is_some() {
        return Err(ValidationError::Timestamp);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use crate::{interfaces::tests::DEVICE_OBJECT, mapping, types::AstarteType};

    use super::*;

    use chrono::{TimeZone, Utc};

    const DEVICE_DATASTREAM: &str = include_str!(
        "../tests/e2etest/interfaces/org.astarte-platform.rust.e2etest.DeviceDatastream.json"
    );

    fn inititialize_aggregate() -> (Interface, HashMap<String, AstarteType>) {
        let aggregate = HashMap::from_iter([
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
        let (interface, _aggregate) = inititialize_aggregate();
        let object = interface.as_object_ref().unwrap();

        // Test sending an aggregate (with and without timestamp)
        let timestamp = Some(TimeZone::timestamp_opt(&Utc, 1537449422, 0).unwrap());
        validate_send_object(object, &None).unwrap();
        validate_send_object(object, &timestamp).unwrap();
    }

    #[test]
    fn test_validate_send_for_aggregate_datastream_extra_field() {
        let (interface, mut aggregate) = inititialize_aggregate();
        let object = interface.as_object_ref().unwrap();

        // Test sending an aggregate with an non existing object field
        aggregate.insert("gibberish".to_string(), AstarteType::Boolean(false));
        let res = validate_send_object(object, &None);
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_send_for_individual_datastream() {
        let interface = Interface::from_str(DEVICE_DATASTREAM).unwrap();

        let mapping = MappingRef::new(&interface, mapping!("/boolean_endpoint")).unwrap();

        let res = validate_send_individual(mapping, &None);
        assert!(res.is_ok());

        let res = validate_send_individual(mapping, &Some(Utc::now()));
        assert!(res.is_ok());
    }
}
