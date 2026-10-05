// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
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

use crate::error::InterfaceError;
use crate::interfaces::MappingRef;
use crate::types::AstarteData;
use astarte_device_error::Error;
use astarte_interfaces::schema::Ownership;
use astarte_interfaces::{InterfaceMapping, Properties, Schema};

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
            return Err(Error::new(InterfaceError::Ownership).set_ctx(format!(
                "for sending on {}, not a device interface",
                interface.name(),
            )));
        }

        if !data.eq_mapping_type(mapping.mapping_type()) {
            return Err(Error::new(InterfaceError::MappingType).set_ctx(format!(
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
            return Err(Error::new(InterfaceError::Ownership).set_ctx(format!(
                "for sending on {}, not a device interface",
                interface.name(),
            )));
        }

        if !mapping.allow_unset() {
            return Err(Error::new(InterfaceError::Unset)
                .set_ctx(format!("for {}{path}, not allowed", interface.name(),)));
        }

        Ok(Self {
            interface: interface.interface_name().to_string(),
            path: path.to_string(),
        })
    }
}
