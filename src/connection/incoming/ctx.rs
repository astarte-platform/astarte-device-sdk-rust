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

use std::pin::pin;

use astarte_device_error::{Error, ResultExt, WrapError};
use astarte_interfaces::interface::InterfaceTypeAggregation;
use astarte_interfaces::{DatastreamIndividual, DatastreamObject, MappingPath, Properties, Schema};
use chrono::Utc;
use tracing::{debug, error, info, instrument, warn};

use crate::error::{AstarteError, ErrorKind, InterfaceError};
use crate::interfaces::MappingRef;
use crate::logging::security::{SecurityEvent, notify_security_event};
use crate::state::ConnectionState;
use crate::store::{Prop, StoreCapabilities};
use crate::transport::Decode;
use crate::{DeviceEvent, Timestamp, Value};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectionCtx<'a, S> {
    pub(crate) state: &'a ConnectionState<S>,
    pub(crate) events: &'a async_channel::Sender<DeviceEvent>,
}

impl<'a, S> ConnectionCtx<'a, S> {
    #[instrument(skip(self, payload))]
    pub(crate) async fn handle_event<P>(
        &self,
        interface: &str,
        path: &str,
        payload: P,
    ) -> Result<(), AstarteError>
    where
        S: StoreCapabilities,
        P: Decode,
    {
        let path = match MappingPath::try_from(path) {
            Ok(path) => path,
            Err(error) => {
                error!(%error, "couldn't parse mapping");

                notify_security_event(SecurityEvent::UnexpectedMessageReceived);

                return Ok(());
            }
        };

        // Locks the interfaces
        let res = {
            let interfaces = self.state.interfaces().read().await;
            let interface = match interfaces.try_get(interface) {
                Ok(interface) => interface,
                Err(error) => {
                    error!(%error, "couldn't handle event");

                    return Ok(());
                }
            };

            if interface.ownership().is_device() {
                error!("interface has device ownership");

                notify_security_event(SecurityEvent::UnexpectedMessageReceived);

                return Ok(());
            }

            info!("event received");

            match interface.inner() {
                InterfaceTypeAggregation::DatastreamIndividual(datastream_individual) => {
                    self.handle_individual(datastream_individual, &path, payload)
                        .await
                }
                InterfaceTypeAggregation::DatastreamObject(datastream_object) => {
                    self.handle_object(datastream_object, &path, payload).await
                }
                InterfaceTypeAggregation::Properties(properties) => {
                    self.handle_property(properties, &path, payload).await
                }
            }
        };

        match res {
            Ok(data) => {
                self.send_to_clients(DeviceEvent {
                    interface: interface.to_string(),
                    path: path.to_string(),
                    data,
                })
                .await?;
            }
            Err(error) => {
                error!(%error, "couldn't handle event");
            }
        }

        Ok(())
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Individual`]
    #[instrument(skip_all)]
    async fn handle_property<P>(
        &self,
        interface: &Properties,
        path: &MappingPath<'_>,
        payload: P,
    ) -> Result<Value, AstarteError>
    where
        S: StoreCapabilities,
        P: Decode,
    {
        let mapping = MappingRef::new(interface, path).ok_or_else(|| {
            Error::new(ErrorKind::Interface(InterfaceError::MappingNotFound))
                .set_ctx(format!("for {}{path}", interface.name()))
        })?;

        match payload.deserialize_property(&mapping)? {
            Some(value) => {
                let prop = Prop::from_mapping(
                    &mapping,
                    value.clone(),
                    self.state.property_ctx().next_updated_at(),
                );

                self.state
                    .store()
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
                self.state
                    .store()
                    .delete_server_prop(mapping.interface().name(), mapping.path().as_str())
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
    async fn handle_individual<P>(
        &self,
        interface: &DatastreamIndividual,
        path: &MappingPath<'_>,
        payload: P,
    ) -> Result<Value, AstarteError>
    where
        P: Decode,
    {
        let mapping = MappingRef::new(interface, path).ok_or_else(|| {
            Error::with(
                ErrorKind::Interface(InterfaceError::MappingNotFound),
                "on received individual",
            )
            .set_ctx(format!("for {interface}{path}"))
        })?;

        let (data, timestamp) = payload.deserialize_individual(&mapping)?;

        let timestamp = validate_timestamp(
            interface.interface_name().as_str(),
            path.as_str(),
            mapping.mapping().explicit_timestamp(),
            timestamp,
        )?;

        Ok(Value::Individual { data, timestamp })
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Object`]
    #[instrument(skip_all)]
    async fn handle_object<P>(
        &self,
        interface: &DatastreamObject,
        path: &MappingPath<'_>,
        payload: P,
    ) -> Result<Value, AstarteError>
    where
        P: Decode,
    {
        if !interface.is_object_path(path) {
            return Err(Error::new(ErrorKind::Interface(InterfaceError::ObjectPath))
                .set_ctx(format!("for interface {interface} and path {path}",)));
        }

        let (data, timestamp) = payload.deserialize_object(interface, path)?;

        let timestamp = validate_timestamp(
            interface.interface_name().as_str(),
            path.as_str(),
            interface.explicit_timestamp(),
            timestamp,
        )?;

        Ok(Value::Object { data, timestamp })
    }

    async fn send_to_clients(&self, event: DeviceEvent) -> Result<(), AstarteError> {
        let mut send = pin!(tokio::task::coop::cooperative(self.events.send(event)));
        let timeout = tokio::time::sleep(self.state.config().slow_receive);

        tokio::select! {
            res = &mut send => {
                res.wrap_err_msg(ErrorKind::Disconnected, "when sending events")
            },
            () = timeout => {
                warn!(
                    duration = ?self.state.config().slow_receive,
                    "slow to send Astarte events to client, maybe no one is consuming them"
                );

                send.await.wrap_err_msg(ErrorKind::Disconnected, "when sending events")
            }
        }
    }
}

/// Validate a timestamp based on the mapping explicit_timestamp value.
///
// The order of incoming message is guaranteed so, even if we generate the reception
// timestamp late, we still (should) have a consistent order of timestamp between messages
fn validate_timestamp(
    interface_name: &str,
    path: &str,
    explicit_timestamp: bool,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<Timestamp, AstarteError> {
    match (timestamp, explicit_timestamp) {
        (None, false) => Ok(Utc::now()),
        (Some(timestamp), true) => Ok(timestamp),
        (Some(_), false) => {
            warn!("received timestamp on interface without `explicit_timestamp`, ignoring");

            Ok(Utc::now())
        }
        (None, true) => {
            error!("missing timestamp on interface with `explicit_timestamp`");

            if cfg!(debug_assertions) {
                Err(Error::with(
                    ErrorKind::Interface(InterfaceError::Timestamp),
                    "set but missing timestamp on received data",
                )
                .set_ctx(format!("for {interface_name}{path}")))
            } else {
                Ok(Utc::now())
            }
        }
    }
}

// TODO: server timestamp when supported by Astarte
#[cfg(test)]
mod tests {
    use astarte_interfaces::schema::Ownership;
    use mockall::{Sequence, predicate};
    use pretty_assertions::assert_eq;

    use crate::AstarteData;
    use crate::aggregate::AstarteObject;
    use crate::connection::incoming::tests::mock_receiver_task;
    use crate::state::ConnStatus;
    use crate::store::PropMetadata;
    use crate::store::mock::MockStore;
    use crate::test::{
        E2E_DEVICE_DATASTREAM, E2E_DEVICE_DATASTREAM_NAME, E2E_SERVER_DATASTREAM,
        E2E_SERVER_DATASTREAM_NAME, E2E_SERVER_PROPERTY, E2E_SERVER_PROPERTY_NAME, SERVER_OBJECT,
        SERVER_OBJECT_NAME, SERVER_PROPERTIES_NO_UNSET, SERVER_PROPERTIES_NO_UNSET_NAME,
    };
    use crate::transport::mock::MockDecoder;

    use super::*;

    #[tokio::test]
    async fn handle_individual() {
        let (this, client_rx) = mock_receiver_task(
            MockStore::new(),
            &[E2E_SERVER_DATASTREAM],
            ConnStatus::Online,
        );

        let exp_interface = E2E_SERVER_DATASTREAM_NAME;
        let endpoint = "/integer_endpoint";
        let value = AstarteData::Integer(42);

        let mut payload = MockDecoder::new();

        let mut seq = Sequence::new();

        payload
            .expect_deserialize_individual()
            .once()
            .in_sequence(&mut seq)
            .withf(move |m| m.interface().name() == exp_interface && m.path().as_str() == endpoint)
            .returning({
                let value = value.clone();

                move |_| Ok((value.clone(), None))
            });

        let ctx = ConnectionCtx {
            state: &this.state,
            events: &this.events,
        };

        ctx.handle_event(exp_interface, endpoint, payload)
            .await
            .unwrap();

        let DeviceEvent {
            interface,
            path,
            data,
        } = client_rx.try_recv().unwrap();

        assert_eq!(interface, exp_interface);
        assert_eq!(path, endpoint);
        // Timestamp cannot be expected
        assert_eq!(data.try_into_individual().unwrap().0, value);
    }

    #[tokio::test]
    async fn handle_event_missing_interface() {
        let (this, client_rx) = mock_receiver_task(MockStore::new(), &[], ConnStatus::Online);

        let interface = E2E_DEVICE_DATASTREAM;
        let endpoint = "/integer_endpoint";

        let payload = MockDecoder::new();

        let ctx = ConnectionCtx {
            state: &this.state,
            events: &this.events,
        };

        ctx.handle_event(interface, endpoint, payload)
            .await
            .unwrap();

        assert!(client_rx.is_empty());
    }

    #[tokio::test]
    async fn handle_event_missing_mapping() {
        let (this, client_rx) = mock_receiver_task(MockStore::new(), &[], ConnStatus::Online);

        let endpoint = "/not_found";

        let payload = MockDecoder::new();

        let ctx = ConnectionCtx {
            state: &this.state,
            events: &this.events,
        };

        ctx.handle_event(E2E_SERVER_DATASTREAM_NAME, endpoint, payload)
            .await
            .unwrap();

        assert!(client_rx.is_empty());
    }

    #[tokio::test]
    async fn handle_object() {
        let (this, client_rx) =
            mock_receiver_task(MockStore::new(), &[SERVER_OBJECT], ConnStatus::Online);

        let obj = AstarteObject::from_iter(
            [
                ("endpoint1", AstarteData::try_from(42.1).unwrap()),
                ("endpoint2", AstarteData::String("value".to_string())),
                ("endpoint3", AstarteData::BooleanArray(vec![true, false])),
            ]
            .map(|(n, v)| (n.to_string(), v)),
        );
        let exp_interface = SERVER_OBJECT_NAME;
        let endpoint = "/sensor1";

        let mut payload = MockDecoder::new();

        let mut seq = Sequence::new();

        payload
            .expect_deserialize_object()
            .once()
            .in_sequence(&mut seq)
            .withf({
                move |interface, path| {
                    interface.name() == exp_interface && path.as_str() == endpoint
                }
            })
            .returning({
                let value = obj.clone();

                move |_, _| Ok((value.clone(), None))
            });

        let ctx = ConnectionCtx {
            state: &this.state,
            events: &this.events,
        };

        ctx.handle_event(exp_interface, endpoint, payload)
            .await
            .unwrap();

        let DeviceEvent {
            interface,
            path,
            data,
        } = client_rx.try_recv().unwrap();

        assert_eq!(interface, exp_interface);
        assert_eq!(path, endpoint);
        // Timestamp cannot be expected
        assert_eq!(data.try_into_object().unwrap().0, obj);
    }

    #[tokio::test]
    async fn handle_property_set() {
        let exp_interface = E2E_SERVER_PROPERTY_NAME;
        let exp_endpoint = "/sensor1/integer_endpoint";
        let exp_value = AstarteData::Integer(42);

        let exp = DeviceEvent {
            interface: exp_interface.to_string(),
            path: exp_endpoint.to_string(),
            data: Value::Property(Some(exp_value.clone())),
        };

        let mut payload = MockDecoder::new();
        let mut store = MockStore::new();

        let mut seq = Sequence::new();

        payload
            .expect_deserialize_property()
            .once()
            .in_sequence(&mut seq)
            .withf({
                move |mapping| {
                    mapping.interface().name() == exp_interface
                        && mapping.path().as_str() == exp_endpoint
                }
            })
            .returning({
                let value = exp_value.clone();

                move |_| Ok(Some(value.clone()))
            });

        store
            .expect_store_prop()
            .once()
            .in_sequence(&mut seq)
            .withf({
                let exp_value = exp_value.clone();

                move |Prop {
                          interface,
                          path,
                          value,
                          interface_major,
                          ownership,
                          updated_at: _,
                      }| {
                    interface == exp_interface
                        && path == exp_endpoint
                        && *value == exp_value
                        && *interface_major == 0
                        && *ownership == Ownership::Server
                }
            })
            .returning(|_| Ok(PropMetadata { epoch: Some(42) }));

        let (this, client_rx) =
            mock_receiver_task(store, &[E2E_SERVER_PROPERTY], ConnStatus::Online);

        let ctx = ConnectionCtx {
            state: &this.state,
            events: &this.events,
        };

        ctx.handle_event(exp_interface, exp_endpoint, payload)
            .await
            .unwrap();

        let event = client_rx.try_recv().unwrap();

        assert_eq!(event, exp);
    }

    #[tokio::test]
    async fn handle_property_unset_success() {
        let exp_interface = E2E_SERVER_PROPERTY_NAME;
        let exp_endpoint = "/sensor1/integer_endpoint";

        let exp = DeviceEvent {
            interface: exp_interface.to_string(),
            path: exp_endpoint.to_string(),
            data: Value::Property(None),
        };

        let mut payload = MockDecoder::new();
        let mut store = MockStore::new();
        let mut seq = Sequence::new();

        payload
            .expect_deserialize_property()
            .once()
            .in_sequence(&mut seq)
            .withf({
                move |mapping| {
                    mapping.interface().name() == exp_interface
                        && mapping.path().as_str() == exp_endpoint
                }
            })
            .returning(|_| Ok(None));

        store
            .expect_delete_server_prop()
            .once()
            .in_sequence(&mut seq)
            .with(predicate::eq(exp_interface), predicate::eq(exp_endpoint))
            .returning(|_, _| Ok(true));

        let (this, client_rx) =
            mock_receiver_task(store, &[E2E_SERVER_PROPERTY], ConnStatus::Online);

        let ctx = ConnectionCtx {
            state: &this.state,
            events: &this.events,
        };

        ctx.handle_event(exp_interface, exp_endpoint, payload)
            .await
            .unwrap();

        let event = client_rx.try_recv().unwrap();

        assert_eq!(event, exp);
    }

    #[tokio::test]
    async fn handle_property_unset_error() {
        let exp_interface = SERVER_PROPERTIES_NO_UNSET_NAME;
        let exp_endpoint = "/sensor1/enable";

        let mut payload = MockDecoder::new();
        let mut seq = Sequence::new();

        payload
            .expect_deserialize_property()
            .once()
            .in_sequence(&mut seq)
            .withf(move |mapping| {
                mapping.interface().name() == exp_interface
                    && mapping.path().as_str() == exp_endpoint
            })
            .returning(|_| Ok(None));

        let (this, client_rx) = mock_receiver_task(
            MockStore::new(),
            &[SERVER_PROPERTIES_NO_UNSET],
            ConnStatus::Online,
        );

        let ctx = ConnectionCtx {
            state: &this.state,
            events: &this.events,
        };

        ctx.handle_event(exp_interface, exp_endpoint, payload)
            .await
            .unwrap();

        assert!(client_rx.is_empty());
    }

    #[tokio::test]
    async fn handle_wrong_ownership() {
        let (this, client_rx) = mock_receiver_task(
            MockStore::new(),
            &[E2E_DEVICE_DATASTREAM],
            ConnStatus::Online,
        );

        let endpoint = "/integer_endpoint";

        let payload = MockDecoder::new();

        let ctx = ConnectionCtx {
            state: &this.state,
            events: &this.events,
        };

        ctx.handle_event(E2E_DEVICE_DATASTREAM_NAME, endpoint, payload)
            .await
            .unwrap();

        assert!(client_rx.is_empty());
    }
}
