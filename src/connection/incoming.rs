// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

use astarte_device_error::{Error, ResultExt, WrapError};
use astarte_interfaces::interface::InterfaceTypeAggregation;
use astarte_interfaces::{DatastreamIndividual, DatastreamObject, MappingPath, Properties, Schema};
use tracing::{debug, info, instrument};

use crate::Value;
use crate::error::{AstarteError, ErrorKind, InterfaceError};
use crate::interfaces::MappingRef;
use crate::store::{PropertyMapping, PropertyStore, StoredProp};
use crate::transport::{Connection, Receive};

use super::DeviceConnection;

impl<C> DeviceConnection<C>
where
    C: Connection,
{
    #[instrument(skip(self, payload))]
    pub(crate) async fn handle_event(
        &self,
        interface: &str,
        path: &str,
        payload: C::Payload,
    ) -> Result<Value, AstarteError>
    where
        C: Receive + Sync,
    {
        let path = MappingPath::try_from(path).wrap_err_with(|_| {
            Error::new(ErrorKind::Interface(InterfaceError::Path)).set_ctx(path.to_string())
        })?;

        let interfaces = self.state.interfaces().read().await;
        let interface = interfaces.get(interface).ok_or_else(|| {
            Error::new(ErrorKind::Interface(InterfaceError::InterfaceNotFound))
                .set_ctx(interface.to_string())
        })?;

        if interface.ownership().is_device() {
            return Err(Error::new(ErrorKind::Interface(InterfaceError::Ownership))
                .set_ctx(format!("{interface}{path}")));
        }

        let data = match interface.inner() {
            InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                self.handle_individual(datastream_individual, &path, payload)
                    .await?
            }
            InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                self.handle_object(datastream_object, &path, payload)
                    .await?
            }
            InterfaceTypeAggregation::Properties(properties) => {
                self.handle_property(properties, &path, payload).await?
            }
        };

        info!("event received");

        Ok(data)
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Individual`]
    #[instrument(skip_all)]
    async fn handle_property(
        &self,
        interface: &Properties,
        path: &MappingPath<'_>,
        payload: C::Payload,
    ) -> Result<Value, AstarteError>
    where
        C: Receive + Sync,
    {
        let mapping = MappingRef::new(interface, path).ok_or_else(|| {
            Error::new(ErrorKind::Interface(InterfaceError::MappingNotFound))
                .set_ctx(format!("for {}{path}", interface.name()))
        })?;

        match self.connection.deserialize_property(&mapping, payload)? {
            Some(value) => {
                let prop = StoredProp::from_mapping(&mapping, &value);

                self.store
                    .store_prop(prop)
                    .await
                    .map_kind(ErrorKind::Store)?;

                debug!(
                    "property stored {}{path}:{}",
                    interface.interface_name(),
                    interface.version_major()
                );

                Ok(Value::Property(Some(value)))
            }
            None => {
                if !mapping.mapping().allow_unset() {
                    return Err(Error::with(
                        ErrorKind::Interface(InterfaceError::Unset),
                        "on received property",
                    )
                    .set_ctx(format!("for {interface}{path}")));
                }

                // Unset can only be received for a property
                self.store
                    .delete_prop(&PropertyMapping::from(&mapping))
                    .await
                    .map_kind(ErrorKind::Store)?;

                debug!(
                    "property unset {}{path}:{}",
                    interface.interface_name(),
                    interface.version_major()
                );

                Ok(Value::Property(None))
            }
        }
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Individual`]
    #[instrument(skip_all)]
    async fn handle_individual(
        &self,
        interface: &DatastreamIndividual,
        path: &MappingPath<'_>,
        payload: C::Payload,
    ) -> Result<Value, AstarteError>
    where
        C: Receive + Sync,
    {
        let mapping = MappingRef::new(interface, path).ok_or_else(|| {
            Error::with(
                ErrorKind::Interface(InterfaceError::MappingNotFound),
                "on received individual",
            )
            .set_ctx(format!("for {interface}{path}"))
        })?;

        let (data, timestamp) = self.connection.deserialize_individual(&mapping, payload)?;

        let timestamp = Self::validate_timestamp(
            interface.interface_name().as_str(),
            path.as_str(),
            mapping.mapping().explicit_timestamp(),
            timestamp,
        )?;

        Ok(Value::Individual { data, timestamp })
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Object`]
    #[instrument(skip_all)]
    async fn handle_object(
        &self,
        interface: &DatastreamObject,
        path: &MappingPath<'_>,
        payload: C::Payload,
    ) -> Result<Value, AstarteError>
    where
        C: Receive + Sync,
    {
        if !interface.is_object_path(path) {
            return Err(Error::new(ErrorKind::Interface(InterfaceError::ObjectPath))
                .set_ctx(format!("for interface {interface} and path {path}",)));
        }

        let (data, timestamp) = self
            .connection
            .deserialize_object(interface, path, payload)?;

        let timestamp = Self::validate_timestamp(
            interface.interface_name().as_str(),
            path.as_str(),
            interface.explicit_timestamp(),
            timestamp,
        )?;

        Ok(Value::Object { data, timestamp })
    }
}

#[cfg(test)]
mod tests {
    use astarte_interfaces::schema::Ownership;
    use chrono::{DateTime, Utc};
    use mockall::Sequence;
    use pretty_assertions::assert_eq;

    use crate::AstarteData;
    use crate::aggregate::AstarteObject;
    use crate::connection::tests::mock_connection;
    use crate::state::ConnStatus;
    use crate::test::{
        E2E_DEVICE_DATASTREAM, E2E_DEVICE_DATASTREAM_NAME, E2E_SERVER_DATASTREAM,
        E2E_SERVER_DATASTREAM_NAME, E2E_SERVER_PROPERTY, E2E_SERVER_PROPERTY_NAME, SERVER_OBJECT,
        SERVER_OBJECT_NAME, SERVER_PROPERTIES_NO_UNSET, SERVER_PROPERTIES_NO_UNSET_NAME,
    };

    use super::*;

    #[tokio::test]
    async fn handle_individual() {
        let mut connection = mock_connection(&[E2E_SERVER_DATASTREAM], ConnStatus::Connected);

        let timestamp = Utc::now();
        let value = Box::new((42, timestamp));
        let endpoint = "/integer_endpoint";

        let mut seq = Sequence::new();

        connection
            .connection
            .expect_deserialize_individual()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let value = *value.as_ref();

                move |mapping, payload| {
                    mapping.interface().name() == E2E_SERVER_DATASTREAM_NAME
                        && mapping.path().as_str() == endpoint
                        && *payload.downcast_ref::<(i32, DateTime<Utc>)>().unwrap() == value
                }
            })
            .returning(|_mapping, payload| {
                let (value, timestamp) = *payload.downcast::<(i32, DateTime<Utc>)>().unwrap();

                Ok((value.into(), Some(timestamp)))
            });

        let event = connection
            .handle_event(E2E_SERVER_DATASTREAM_NAME, endpoint, value)
            .await
            .unwrap();

        let exp = AstarteData::Integer(42);

        // Timestamp cannot be checked
        let data = event.try_into_individual().unwrap().0;

        assert_eq!(data, exp)
    }

    #[tokio::test]
    async fn handle_event_missing_interface() {
        let connection = mock_connection(&[], ConnStatus::Connected);

        let timestamp = Utc::now();
        let value = Box::new((42, timestamp));
        let endpoint = "/integer_endpoint";

        let event = connection
            .handle_event(E2E_SERVER_DATASTREAM_NAME, endpoint, value)
            .await
            .unwrap_err();

        assert_eq!(
            *event.kind(),
            ErrorKind::Interface(InterfaceError::InterfaceNotFound)
        );
    }

    #[tokio::test]
    async fn handle_event_missing_mapping() {
        let connection = mock_connection(&[E2E_SERVER_DATASTREAM], ConnStatus::Connected);

        let timestamp = Utc::now();
        let value = Box::new((42, timestamp));
        let endpoint = "/not_found";

        let event = connection
            .handle_event(E2E_SERVER_DATASTREAM_NAME, endpoint, value)
            .await
            .unwrap_err();

        assert_eq!(
            *event.kind(),
            ErrorKind::Interface(InterfaceError::MappingNotFound),
            "{event:?}"
        );
    }

    #[tokio::test]
    async fn handle_object() {
        let mut connection = mock_connection(&[SERVER_OBJECT], ConnStatus::Connected);

        let timestamp = Utc::now();
        let obj = AstarteObject::from_iter(
            [
                ("endpoint1", AstarteData::try_from(42.1).unwrap()),
                ("endpoint2", AstarteData::String("value".to_string())),
                ("endpoint3", AstarteData::BooleanArray(vec![true, false])),
            ]
            .map(|(n, v)| (n.to_string(), v)),
        );
        let value = Box::new((obj.clone(), timestamp));
        let endpoint = "/sensor1";

        let mut seq = Sequence::new();

        connection
            .connection
            .expect_deserialize_object()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let value = value.as_ref().clone();

                move |interface, path, payload| {
                    interface.name() == SERVER_OBJECT_NAME
                        && path.as_str() == endpoint
                        && *payload
                            .downcast_ref::<(AstarteObject, DateTime<Utc>)>()
                            .unwrap()
                            == value
                }
            })
            .returning(|_, _mapping, payload| {
                let (value, timestamp) = *payload
                    .downcast::<(AstarteObject, DateTime<Utc>)>()
                    .unwrap();

                Ok((value, Some(timestamp)))
            });

        let event = connection
            .handle_event(SERVER_OBJECT_NAME, endpoint, value)
            .await
            .unwrap();

        // Timestamp cannot be expected
        let data = event.try_into_object().unwrap().0;

        assert_eq!(data, obj)
    }

    #[tokio::test]
    async fn handle_property_set() {
        let mut connection = mock_connection(&[E2E_SERVER_PROPERTY], ConnStatus::Connected);

        let value = Box::new(42);
        let endpoint = "/sensor1/integer_endpoint";

        let mut seq = Sequence::new();

        connection
            .connection
            .expect_deserialize_property()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let value = *value.as_ref();

                move |mapping, payload| {
                    mapping.interface().name() == E2E_SERVER_PROPERTY_NAME
                        && mapping.path().as_str() == endpoint
                        && *payload.downcast_ref::<i32>().unwrap() == value
                }
            })
            .returning(|_mapping, payload| {
                let value = *payload.downcast::<i32>().unwrap();

                Ok(Some(AstarteData::Integer(value)))
            });

        let event = connection
            .handle_event(E2E_SERVER_PROPERTY_NAME, endpoint, value)
            .await
            .unwrap();

        let exp = Value::Property(Some(AstarteData::Integer(42)));

        assert_eq!(event, exp);

        let interfaces = connection.state.interfaces().read().await;
        let path = MappingPath::try_from(endpoint).unwrap();
        let mapping = interfaces
            .get_property(E2E_SERVER_PROPERTY_NAME, &path)
            .unwrap();

        let prop = connection
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(prop, AstarteData::Integer(42));
    }

    #[tokio::test]
    async fn handle_property_unset_success() {
        let mut connection = mock_connection(&[E2E_SERVER_PROPERTY], ConnStatus::Connected);

        let value = Box::new([0u8; 0]);
        let endpoint = "/sensor1/integer_endpoint";

        connection
            .store
            .store_prop(StoredProp {
                interface: E2E_SERVER_PROPERTY_NAME,
                path: endpoint,
                value: &AstarteData::Integer(42),
                interface_major: 0,
                ownership: Ownership::Device,
            })
            .await
            .unwrap();

        let mut seq = Sequence::new();

        connection
            .connection
            .expect_deserialize_property()
            .once()
            .in_sequence(&mut seq)
            .withf(move |mapping, payload| {
                mapping.interface().name() == E2E_SERVER_PROPERTY_NAME
                    && mapping.path().as_str() == endpoint
                    && payload.downcast_ref::<[u8; 0]>().unwrap().is_empty()
            })
            .returning(|_mapping, _payload| Ok(None));

        let event = connection
            .handle_event(E2E_SERVER_PROPERTY_NAME, endpoint, value)
            .await
            .unwrap();

        let exp = Value::Property(None);

        assert_eq!(event, exp);

        let interfaces = connection.state.interfaces().read().await;
        let path = MappingPath::try_from(endpoint).unwrap();
        let mapping = interfaces
            .get_property(E2E_SERVER_PROPERTY_NAME, &path)
            .unwrap();

        let prop = connection
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap();

        assert!(prop.is_none());
    }

    #[tokio::test]
    async fn handle_property_unset_error() {
        let mut connection = mock_connection(&[SERVER_PROPERTIES_NO_UNSET], ConnStatus::Connected);

        let value = Box::new([0u8; 0]);
        let endpoint = "/sensor1/enable";

        connection
            .store
            .store_prop(StoredProp {
                interface: SERVER_PROPERTIES_NO_UNSET_NAME,
                path: endpoint,
                value: &AstarteData::Integer(42),
                interface_major: 0,
                ownership: Ownership::Server,
            })
            .await
            .unwrap();

        let mut seq = Sequence::new();

        connection
            .connection
            .expect_deserialize_property()
            .once()
            .in_sequence(&mut seq)
            .withf(move |mapping, payload| {
                mapping.interface().name() == SERVER_PROPERTIES_NO_UNSET_NAME
                    && mapping.path().as_str() == endpoint
                    && payload.downcast_ref::<[u8; 0]>().unwrap().is_empty()
            })
            .returning(|_mapping, _payload| Ok(None));

        let err = connection
            .handle_event(SERVER_PROPERTIES_NO_UNSET_NAME, endpoint, value)
            .await
            .unwrap_err();

        assert_eq!(
            *err.kind(),
            ErrorKind::Interface(InterfaceError::Unset),
            "got {err:?}"
        );

        let path = MappingPath::try_from(endpoint).unwrap();
        let interfaces = connection.state.interfaces().read().await;
        let mapping = interfaces
            .get_property(SERVER_PROPERTIES_NO_UNSET_NAME, &path)
            .unwrap();

        let prop = connection
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(prop, AstarteData::Integer(42));
    }

    #[tokio::test]
    async fn handle_wrong_ownership() {
        let connection = mock_connection(&[E2E_DEVICE_DATASTREAM], ConnStatus::Connected);

        let timestamp = Utc::now();
        let value = Box::new((42, timestamp));
        let endpoint = "/integer_endpoint";

        let err = connection
            .handle_event(E2E_DEVICE_DATASTREAM_NAME, endpoint, value)
            .await
            .unwrap_err();

        assert_eq!(*err.kind(), ErrorKind::Interface(InterfaceError::Ownership));
    }
}
