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

use std::cmp::Ordering;
use std::iter::{FusedIterator, Peekable};

use crate::aggregate::AstarteObject;
use crate::client::ClientPacket;
use crate::error::{AstarteError, ErrorKind, InterfaceError};
use crate::retention::{Id, RetentionId, StoredRetention, StoredRetentionExt};
use crate::transport::Encode;
use crate::{AstarteData, Timestamp};
use astarte_device_error::{Error, ResultExt};
use astarte_interfaces::interface::Retention;
use astarte_interfaces::schema::{Ownership, Reliability};
use astarte_interfaces::{
    DatastreamObject, DatastreamObjectMapping, InterfaceMapping, MappingPath, Schema,
};
use tracing::trace;

use super::{Validated, validate_timestamp};

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
            return Err(Error::new(InterfaceError::Ownership).set_ctx(format!(
                "for sending on {}, not a device interface",
                interface.name(),
            )));
        }

        if !interface.is_object_path(path) {
            return Err(Error::new(InterfaceError::ObjectPath).set_ctx(format!(
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

                return Err(Error::new(InterfaceError::MappingNotFound).set_ctx(format!(
                    "for interface {interface}{path}/{key}, mapping not found",
                )));
            };

            match item {
                Some((key, value)) => {
                    if !value.eq_mapping_type(mapping.mapping_type()) {
                        return Err(Error::new(InterfaceError::MappingType).set_ctx(format!(
                            "for interface {interface}{path}/{key}, expected {} but got {}",
                            mapping.mapping_type(),
                            value.display_type()
                        )));
                    }

                    trace!("valid object field {path} {}", value.display_type());
                }
                None => {
                    if mapping.required() {
                        return Err(Error::new(InterfaceError::MappingRequired).set_ctx(format!(
                            "for interface {interface} endpoint {}",
                            mapping.endpoint()
                        )));
                    }
                }
            }

            Ok(())
        })?;

        Ok(())
    }
}

impl ClientPacket for ValidatedObject {
    fn get_retention(&self) -> Retention {
        self.retention
    }

    fn serialize<E>(&self, encoder: &E) -> Result<Vec<u8>, AstarteError>
    where
        E: Encode,
    {
        encoder.serialize_object(self)
    }

    fn validated(self, retention: Option<RetentionId>) -> Validated {
        Validated::Object {
            retention,
            data: self,
        }
    }

    async fn store_publish<S, E>(
        &self,
        retention: &S,
        encoder: &E,
        id: &Id,
        sent: bool,
    ) -> Result<(), AstarteError>
    where
        S: StoredRetention,
        E: Encode,
    {
        let serialized = self.serialize(encoder)?;

        retention
            .store_publish_object(id, self, &serialized, sent)
            .await
            .map_kind(ErrorKind::Retention)
    }
}

/// Iterate over elements in ordered lists.
///
/// Iterator of two ordered lists returning tuples of the element A and B when they are equals,
/// otherwise the smaller one is returned first and the other is none.
struct Iter<'a, M, D>
where
    D: Iterator<Item = &'a (String, AstarteData)>,
    M: Iterator<Item = &'a DatastreamObjectMapping>,
{
    data: Peekable<D>,
    mapping: Peekable<M>,
}

impl<'a, M, D> Iter<'a, M, D>
where
    D: Iterator<Item = &'a (String, AstarteData)>,
    M: Iterator<Item = &'a DatastreamObjectMapping>,
{
    pub(super) fn new(data: D, mapping: M) -> Self {
        Self {
            data: data.peekable(),
            mapping: mapping.peekable(),
        }
    }
}

impl<'a, M, D> Iterator for Iter<'a, M, D>
where
    D: Iterator<Item = &'a (String, AstarteData)>,
    M: Iterator<Item = &'a DatastreamObjectMapping>,
{
    type Item = (Option<D::Item>, Option<M::Item>);

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.data.peek();
        let b = self.mapping.peek();

        match (a, b) {
            (None, None) => None,
            (None, Some(_)) => Some((None, (self.mapping.next()))),
            (Some(_), None) => Some((self.data.next(), None)),
            (Some((a, _)), Some(b)) => match b.cmp_object_field(a) {
                // Mapping is less than the data, advance the mapping
                Ordering::Less => Some((None, self.mapping.next())),
                // Mapping and data are equal, advance both
                Ordering::Equal => Some((self.data.next(), self.mapping.next())),
                // Mapping is greater than the data, advance the data
                Ordering::Greater => Some((self.data.next(), None)),
            },
        }
    }
}

impl<'a, M, D> FusedIterator for Iter<'a, M, D>
where
    D: Iterator<Item = &'a (String, AstarteData)>,
    M: Iterator<Item = &'a DatastreamObjectMapping>,
{
}
