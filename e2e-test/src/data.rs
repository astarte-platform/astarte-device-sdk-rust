// This file is part of Astarte.
//
// Copyright 2024 - 2025 SECO Mind Srl
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

use std::collections::HashMap;

use astarte_device_sdk::aggregate::AstarteObject;
use astarte_device_sdk::types::AstarteData;

use crate::utils::{base64_decode, timestamp_from_rfc3339};

pub(crate) trait InterfaceData {
    fn interface() -> String;

    fn data() -> eyre::Result<HashMap<String, AstarteData>> {
        let data = all_type_data().map(|(name, v)| (format!("/{name}"), v));

        Ok(HashMap::from_iter(data))
    }
}

pub(crate) trait InterfaceDataObject: InterfaceData {
    fn base_path() -> String {
        "/sensor_1".to_string()
    }

    fn data() -> eyre::Result<AstarteObject> {
        let data = all_type_data().map(|(n, v)| (n.to_string(), v));

        Ok(AstarteObject::from_iter(data))
    }
}

pub(crate) fn all_type_data() -> [(&'static str, AstarteData); 14] {
    [
        ("double_endpoint", AstarteData::try_from(4.35).unwrap()),
        ("integer_endpoint", AstarteData::Integer(1)),
        ("boolean_endpoint", AstarteData::Boolean(true)),
        (
            "longinteger_endpoint",
            AstarteData::LongInteger(45543543534),
        ),
        ("string_endpoint", AstarteData::String("Hello".to_string())),
        (
            "binaryblob_endpoint",
            AstarteData::BinaryBlob(base64_decode("aGVsbG8=").unwrap()),
        ),
        (
            "datetime_endpoint",
            AstarteData::DateTime(timestamp_from_rfc3339("2021-09-29T17:46:48.000Z").unwrap()),
        ),
        (
            "doublearray_endpoint",
            AstarteData::try_from(vec![43.5, 10.5, 11.9]).unwrap(),
        ),
        (
            "integerarray_endpoint",
            AstarteData::IntegerArray([-4, 123, -2222, 30].to_vec()),
        ),
        (
            "booleanarray_endpoint",
            AstarteData::BooleanArray([true, false].to_vec()),
        ),
        (
            "longintegerarray_endpoint",
            AstarteData::LongIntegerArray([53267895478, 53267895428, 53267895118].to_vec()),
        ),
        (
            "stringarray_endpoint",
            AstarteData::StringArray(["Test ".to_string(), "String".to_string()].to_vec()),
        ),
        (
            "binaryblobarray_endpoint",
            AstarteData::BinaryBlobArray(
                ["aGVsbG8=", "aGVsbG8="]
                    .map(|s| base64_decode(s).unwrap())
                    .to_vec(),
            ),
        ),
        (
            "datetimearray_endpoint",
            AstarteData::DateTimeArray(
                ["2021-10-23T17:46:48.000Z", "2021-11-11T17:46:48.000Z"]
                    .map(|s| timestamp_from_rfc3339(s).unwrap())
                    .to_vec(),
            ),
        ),
    ]
}
