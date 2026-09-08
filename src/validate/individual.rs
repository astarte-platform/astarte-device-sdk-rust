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

use astarte_device_error::{Error, ResultExt};
use astarte_interfaces::interface::Retention;
use astarte_interfaces::schema::{Ownership, Reliability};
use astarte_interfaces::{DatastreamIndividual, InterfaceMapping, Schema};

use crate::client::ClientPacket;
use crate::error::{AstarteError, ErrorKind, InterfaceError};
use crate::interfaces::MappingRef;
use crate::retention::{Id, RetentionId, StoredRetention, StoredRetentionExt};
use crate::transport::Encode;
use crate::{AstarteData, Timestamp};

use super::{Validated, validate_timestamp};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValidatedIndividual {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) version_major: i32,
    pub(crate) reliability: Reliability,
    pub(crate) retention: Retention,
    pub(crate) timestamp: Option<Timestamp>,
    pub(crate) data: AstarteData,
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

impl ClientPacket for ValidatedIndividual {
    fn get_retention(&self) -> Retention {
        self.retention
    }

    fn serialize<E>(&self, encoder: &E) -> Result<Vec<u8>, AstarteError>
    where
        E: Encode,
    {
        encoder.serialize_individual(self)
    }

    fn validated(self, retention: Option<RetentionId>) -> Validated {
        Validated::Individual {
            retention,
            data: self,
        }
    }

    async fn store_publish<R, E>(
        &self,
        retention: &R,
        encoder: &E,
        id: &Id,
        sent: bool,
    ) -> Result<(), AstarteError>
    where
        R: StoredRetention,
        E: Encode,
    {
        let serialized = self.serialize(encoder)?;

        retention
            .store_publish_individual(id, self, &serialized, sent)
            .await
            .map_kind(ErrorKind::Retention)
    }
}
