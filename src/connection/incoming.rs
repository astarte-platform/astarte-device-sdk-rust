// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

use astarte_interfaces::interface::InterfaceTypeAggregation;
use astarte_interfaces::{DatastreamIndividual, DatastreamObject, MappingPath, Properties, Schema};
use tracing::{debug, error, instrument, warn};

use crate::client::RecvError;
use crate::interfaces::MappingRef;
use crate::store::{PropertyMapping, PropertyStore, StoredProp};
use crate::transport::{Connection, Receive, TransportError};
use crate::{Error, Value};

use super::DeviceConnection;

impl<C> DeviceConnection<C>
where
    C: Connection,
{
    // Solves https://github.com/rust-lang/rust/issues/110486
    #[cfg_attr(not(__coverage), instrument(skip(self, payload)))]
    pub(crate) async fn handle_event(
        &self,
        interface: &str,
        path: &str,
        payload: C::Payload,
    ) -> Result<Value, TransportError>
    where
        C: Receive + Sync,
    {
        let path = MappingPath::try_from(path)
            .map_err(|err| TransportError::Recv(RecvError::InvalidEndpoint(err)))?;

        let interfaces = self.state.interfaces.read().await;
        let Some(interface) = interfaces.get(interface) else {
            warn!("publish on missing interface");

            return Err(TransportError::Recv(RecvError::InterfaceNotFound {
                name: interface.to_string(),
            }));
        };

        if interface.ownership().is_device() {
            error!("received event on device owned interface");

            return Err(TransportError::Recv(RecvError::Ownership {
                interface: interface.to_string(),
                path: path.to_string(),
            }));
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

        debug!("event received");

        Ok(data)
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Individual`]
    async fn handle_property(
        &self,
        interface: &Properties,
        path: &MappingPath<'_>,
        payload: C::Payload,
    ) -> Result<Value, TransportError>
    where
        C: Receive + Sync,
    {
        let mapping = MappingRef::new(interface, path).ok_or_else(|| {
            TransportError::Recv(RecvError::MappingNotFound {
                interface: interface.interface_name().to_string(),
                mapping: path.to_string(),
            })
        })?;

        match self.connection.deserialize_property(&mapping, payload)? {
            Some(value) => {
                let prop = StoredProp::from_mapping(&mapping, &value);

                self.store
                    .store_prop(prop)
                    .await
                    .map_err(|err| TransportError::Transport(Error::Store(err)))?;

                debug!(
                    "property stored {}{path}:{}",
                    interface.interface_name(),
                    interface.version_major()
                );

                Ok(Value::Property(Some(value)))
            }
            None => {
                if !mapping.mapping().allow_unset() {
                    return Err(TransportError::Recv(RecvError::Unset {
                        interface_name: interface.interface_name().to_string(),
                        path: path.to_string(),
                    }));
                }

                // Unset can only be received for a property
                self.store
                    .delete_prop(&PropertyMapping::from(&mapping))
                    .await
                    .map_err(|err| TransportError::Transport(Error::Store(err)))?;

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
    async fn handle_individual(
        &self,
        interface: &DatastreamIndividual,
        path: &MappingPath<'_>,
        payload: C::Payload,
    ) -> Result<Value, TransportError>
    where
        C: Receive + Sync,
    {
        let mapping = MappingRef::new(interface, path).ok_or_else(|| {
            TransportError::Recv(RecvError::MappingNotFound {
                interface: interface.interface_name().to_string(),
                mapping: path.to_string(),
            })
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
    async fn handle_object(
        &self,
        interface: &DatastreamObject,
        path: &MappingPath<'_>,
        payload: C::Payload,
    ) -> Result<Value, TransportError>
    where
        C: Receive + Sync,
    {
        if !interface.is_object_path(path) {
            return Err(TransportError::Recv(RecvError::MappingNotFound {
                interface: interface.interface_name().to_string(),
                mapping: path.to_string(),
            }));
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

    use crate::aggregate::AstarteObject;
    use crate::connection::tests::mock_connection;
    use crate::test::{
        E2E_DEVICE_DATASTREAM, E2E_DEVICE_DATASTREAM_NAME, E2E_SERVER_DATASTREAM,
        E2E_SERVER_DATASTREAM_NAME, E2E_SERVER_PROPERTY, E2E_SERVER_PROPERTY_NAME, SERVER_OBJECT,
        SERVER_OBJECT_NAME, SERVER_PROPERTIES_NO_UNSET, SERVER_PROPERTIES_NO_UNSET_NAME,
    };
    use crate::AstarteType;

    use super::*;

    #[tokio::test]
    async fn handle_individual() {
        let (mut connection, _rx) = mock_connection(&[E2E_SERVER_DATASTREAM]);

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

        let exp = AstarteType::Integer(42);

        // Timestamp cannot be checked
        let data = event.try_into_individual().unwrap().0;

        assert_eq!(data, exp)
    }

    #[tokio::test]
    async fn handle_event_missing_interface() {
        let (connection, _rx) = mock_connection(&[]);

        let timestamp = Utc::now();
        let value = Box::new((42, timestamp));
        let endpoint = "/integer_endpoint";

        let event = connection
            .handle_event(E2E_SERVER_DATASTREAM_NAME, endpoint, value)
            .await
            .unwrap_err();

        assert!(matches!(
            event,
            TransportError::Recv(RecvError::InterfaceNotFound { name }) if name == E2E_SERVER_DATASTREAM_NAME
        ))
    }

    #[tokio::test]
    async fn handle_event_missing_mapping() {
        let (connection, _rx) = mock_connection(&[E2E_SERVER_DATASTREAM]);

        let timestamp = Utc::now();
        let value = Box::new((42, timestamp));
        let endpoint = "/not_found";

        let event = connection
            .handle_event(E2E_SERVER_DATASTREAM_NAME, endpoint, value)
            .await
            .unwrap_err();

        assert!(
            matches!(
                &event,
                TransportError::Recv(RecvError::MappingNotFound { interface, mapping })
                    if interface == E2E_SERVER_DATASTREAM_NAME && mapping == endpoint
            ),
            "{event:?}"
        )
    }

    #[tokio::test]
    async fn handle_object() {
        let (mut connection, _rx) = mock_connection(&[SERVER_OBJECT]);

        let timestamp = Utc::now();
        let obj = AstarteObject::from_iter(
            [
                ("endpoint1", AstarteType::try_from(42.1).unwrap()),
                ("endpoint2", AstarteType::String("value".to_string())),
                ("endpoint3", AstarteType::BooleanArray(vec![true, false])),
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
        let (mut connection, _rx) = mock_connection(&[E2E_SERVER_PROPERTY]);

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

                Ok(Some(AstarteType::Integer(value)))
            });

        let event = connection
            .handle_event(E2E_SERVER_PROPERTY_NAME, endpoint, value)
            .await
            .unwrap();

        let exp = Value::Property(Some(AstarteType::Integer(42)));

        assert_eq!(event, exp);

        let interfaces = connection.state.interfaces.read().await;
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

        assert_eq!(prop, AstarteType::Integer(42));
    }

    #[tokio::test]
    async fn handle_property_unset_success() {
        let (mut connection, _rx) = mock_connection(&[E2E_SERVER_PROPERTY]);

        let value = Box::new([0u8; 0]);
        let endpoint = "/sensor1/integer_endpoint";

        connection
            .store
            .store_prop(StoredProp {
                interface: E2E_SERVER_PROPERTY_NAME,
                path: endpoint,
                value: &AstarteType::Integer(42),
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

        let interfaces = connection.state.interfaces.read().await;
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
        let (mut connection, _rx) = mock_connection(&[SERVER_PROPERTIES_NO_UNSET]);

        let value = Box::new([0u8; 0]);
        let endpoint = "/sensor1/enable";

        connection
            .store
            .store_prop(StoredProp {
                interface: SERVER_PROPERTIES_NO_UNSET_NAME,
                path: endpoint,
                value: &AstarteType::Integer(42),
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

        assert!(
            matches!(err, TransportError::Recv(RecvError::Unset { .. })),
            "got {err:?}"
        );

        let path = MappingPath::try_from(endpoint).unwrap();
        let interfaces = connection.state.interfaces.read().await;
        let mapping = interfaces
            .get_property(SERVER_PROPERTIES_NO_UNSET_NAME, &path)
            .unwrap();

        let prop = connection
            .store
            .load_prop(&PropertyMapping::from(&mapping))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(prop, AstarteType::Integer(42));
    }

    #[tokio::test]
    async fn handle_wrong_ownership() {
        let (connection, _rx) = mock_connection(&[E2E_DEVICE_DATASTREAM]);

        let timestamp = Utc::now();
        let value = Box::new((42, timestamp));
        let endpoint = "/integer_endpoint";

        let err = connection
            .handle_event(E2E_DEVICE_DATASTREAM_NAME, endpoint, value)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            TransportError::Recv(RecvError::Ownership { .. })
        ));
    }
}
