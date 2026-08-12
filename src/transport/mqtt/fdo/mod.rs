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

//! FIDO Device Onboarding protocol.

use std::fmt::Display;
use std::ops::ControlFlow;
use std::path::Path;
use std::time::Duration;

use astarte_device_error::{Error, WrapError};
use astarte_device_fdo::client::http::InitialClient;
use astarte_device_fdo::di::Di;
use astarte_device_fdo::srv_info::{AstarteMod, AstarteModBuilder};
use astarte_device_fdo::storage::FileStorage;
use astarte_device_fdo::to1::To1;
use astarte_device_fdo::to2::{Hello, To2};
use astarte_device_fdo::{Crypto, Ctx as FdoCtx};
use tracing::{error, info, instrument};
use url::Url;

use crate::builder::{BuildConfig, ConnectionConfig};
use crate::error::{AstarteError, ErrorKind};
use crate::state::{ConnectionState, SharedState};
use crate::store::StoreCapabilities;
use crate::transport::mqtt::Credential;
use crate::transport::mqtt::components::ClientId;
use crate::transport::mqtt::config::transport::TransportProvider;
use crate::transport::mqtt::connection::MqttConnection;
use crate::transport::mqtt::pairing::client::{ApiClient, ClientArgs};

use self::builder::{AddManufacturingUrl, FdoConfigBuilder};

use super::Mqtt;
use super::client::{MqttClient, MqttEncoder};

pub mod builder;

/// FIDO Device Ownership protocol  error.
///
/// Returned while initializing the device.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FdoError {
    /// Protocol operation failed
    Failed,
    /// Invalid argument provided
    InvalidArgument,
}

impl Display for FdoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FdoError::Failed => write!(f, "couldn't complete operation"),
            FdoError::InvalidArgument => write!(f, "invalid argument"),
        }
    }
}

/// Initialize the device to pair with FDO.
#[derive(Debug)]
pub struct FdoDi<'a, C> {
    model_no: &'a str,
    serial_no: &'a str,
    manufacturing_url: Url,
    keepalive: Duration,
    crypto: C,
}

impl<'a, C> FdoDi<'a, C> {
    /// Initializes a device to be paired to a cloud.
    #[instrument(skip_all)]
    pub async fn device_initialize(
        mut self,
        storage: &Path,
        tls: rustls::ClientConfig,
    ) -> Result<FdoConfig<C>, AstarteError>
    where
        C: Crypto,
    {
        let mut storage = FileStorage::open(storage.join("fdo"))
            .await
            .map_err(|error| {
                error!(%error, "couldn't open file storage");

                Error::with(ErrorKind::Io(error.kind()), "while opening file storage")
            })?;

        let mut ctx = FdoCtx::new(&mut self.crypto, &mut storage, tls.clone());

        let client = InitialClient::create(self.manufacturing_url.clone(), tls.clone())
            .wrap_err_msg(ErrorKind::Fdo(FdoError::Failed), "while creating client")?;

        let di = Di::create(&mut ctx, client, self.model_no, self.serial_no)
            .await
            .wrap_err_msg(
                ErrorKind::Fdo(FdoError::Failed),
                "while initializing Device",
            )?;

        let _cred = di.create_credentials(&mut ctx).await.wrap_err_msg(
            ErrorKind::Fdo(FdoError::Failed),
            "while storing credentials",
        )?;

        info!("Device Initialized");

        Ok(FdoConfig {
            serial_no: self.serial_no.to_string(),
            keepalive: self.keepalive,
            crypto: self.crypto,
        })
    }
}

/// Configuration to register a device using FDO.
#[derive(Debug)]
pub struct FdoConfig<C> {
    serial_no: String,
    keepalive: Duration,
    crypto: C,
}

impl<C> FdoConfig<C> {
    /// Returns the builder for the FDO config.
    pub fn build<'a>(
        model_no: &'a str,
        serial_no: &'a str,
    ) -> FdoConfigBuilder<'a, C, AddManufacturingUrl> {
        FdoConfigBuilder::new(model_no, serial_no)
    }

    /// Register the device to the cloud.
    #[instrument(skip_all)]
    async fn transfer_ownership<S>(
        &mut self,
        state: &ConnectionState<S>,
    ) -> Result<ControlFlow<Mqtt>, AstarteError>
    where
        C: Crypto,
    {
        let storage = state.config().writable_dir.as_ref().ok_or(Error::with(
            ErrorKind::Fdo(FdoError::InvalidArgument),
            "missing writable directory",
        ))?;

        let mut storage = FileStorage::open(storage.join("fdo"))
            .await
            .map_err(|error| {
                error!(%error, "couldn't open file storage");

                Error::with(ErrorKind::Io(error.kind()), "while opening file storage")
            })?;

        let mut fdo_ctx = FdoCtx::new(&mut self.crypto, &mut storage, state.tls().clone());

        let cred = Di::read_existing(&mut fdo_ctx)
            .await
            .wrap_err_msg(
                ErrorKind::Fdo(FdoError::Failed),
                "while reading stored Device credentials",
            )
            .and_then(|opt| {
                opt.ok_or(Error::with(
                    ErrorKind::Fdo(FdoError::InvalidArgument),
                    "missing Device credentials",
                ))
            })?;

        if !cred.dc_active {
            info!("device change TO already run to completion");

            let opt_amod = To2::<'_, AstarteModBuilder, Hello>::read_existing(&mut fdo_ctx)
                .await
                .wrap_err_msg(
                    ErrorKind::Fdo(FdoError::Failed),
                    "while reading stored Astarte module",
                )?;
            if let Some(amod) = opt_amod {
                info!(device_id = amod.device_id, "Astarte mod already stored ");

                let pairing_url =
                    format!("{}/pairing", amod.base_url)
                        .parse()
                        .map_err(|error| {
                            error!(%error, "couldn't parse astarte pairing url");

                            Error::with(
                                ErrorKind::Fdo(FdoError::InvalidArgument),
                                "for astarte pairing url",
                            )
                        })?;

                return Ok(ControlFlow::Break(Mqtt {
                    client_id: ClientId {
                        realm: amod.realm,
                        device_id: amod.device_id,
                    },
                    credential: Credential::secret(amod.secret),
                    pairing_url,
                    keepalive: self.keepalive,
                }));
            }
        }

        let to1 = To1::new(&cred);

        let rv = match to1.rv_owner(&mut fdo_ctx).await {
            Ok(rv) => rv,
            Err(error) => {
                error!(%error, "couldn't get rv redirect");

                return Ok(ControlFlow::Continue(()));
            }
        };

        let to2 = To2::create(cred, rv, &self.serial_no, AstarteMod::builder()).wrap_err_msg(
            ErrorKind::Fdo(FdoError::Failed),
            "while creating TO2 client",
        )?;

        let (to2, amod) = match to2.to2_change(&mut fdo_ctx).await {
            Ok(value) => value,
            Err(error) => {
                error!(%error,"couldn't finish TO2");

                return Ok(ControlFlow::Continue(()));
            }
        };

        info!("Astarte mod received with device_id: {}", amod.device_id);

        let pairing_url = format!("{}/pairing", amod.base_url)
            .parse()
            .map_err(|error| {
                error!(%error, "couldn't parse astarte pairing url");

                Error::with(
                    ErrorKind::Fdo(FdoError::InvalidArgument),
                    "astarte pairing url",
                )
            })?;

        let args = ClientArgs {
            client_id: ClientId {
                realm: &amod.realm,
                device_id: &amod.device_id,
            },
            pairing_url: &pairing_url,
            token: &amod.secret,
        };

        let api =
            ApiClient::create(args, state.config(), state.tls().clone()).map_err(|error| {
                error!(%error, "couldn't create pairing api client");

                Error::with(
                    ErrorKind::Fdo(FdoError::InvalidArgument),
                    "pairing api client",
                )
            })?;

        let client_id = ClientId::<&str> {
            realm: &amod.realm,
            device_id: &amod.device_id,
        };

        // Make sure the credentials are valid and we can connect to astarte,
        //
        // As per: https://fidoalliance.org/specs/FDO/FIDO-Device-Onboard-PS-v1.1-20220419/FIDO-Device-Onboard-PS-v1.1-20220419.html#to2done2-type-71
        if let Err(error) = TransportProvider::create_credentials(state, &api, client_id).await {
            error!(%error, "couldn't configure transport provider");

            return Ok(ControlFlow::Continue(()));
        };

        info!("certificate created");

        if let Err(error) = to2.done(&mut fdo_ctx).await {
            error!(%error, "couldn't finish FDO");

            return Ok(ControlFlow::Continue(()));
        }

        Ok(ControlFlow::Break(Mqtt {
            client_id: ClientId {
                realm: amod.realm,
                device_id: amod.device_id,
            },
            credential: Credential::secret(amod.secret),
            pairing_url,
            keepalive: self.keepalive,
        }))
    }
}

impl<C> ConnectionConfig for FdoConfig<C>
where
    C: Crypto + Send + Sync + 'static,
{
    type Connection = MqttConnection;
    type Client = MqttClient;
    type Store<S>
        = S
    where
        S: Send + Sync + 'static;
    type Encoder = MqttEncoder;

    /// Configures the connection.
    async fn configure<S>(
        &mut self,
        state: SharedState<S>,
    ) -> Result<BuildConfig<Self::Store<S>, Self::Encoder>, AstarteError>
    where
        S: StoreCapabilities,
    {
        Ok(BuildConfig {
            state,
            encoder: MqttEncoder {},
        })
    }

    /// Check if the device is already paired
    async fn is_registered<S>(&mut self, state: &SharedState<S>) -> Result<bool, AstarteError>
    where
        S: StoreCapabilities,
    {
        let storage = state.config().writable_dir.as_ref().ok_or(Error::with(
            ErrorKind::Fdo(FdoError::InvalidArgument),
            "missing writable directory",
        ))?;

        let mut storage = FileStorage::open(storage.join("fdo"))
            .await
            .map_err(|error| {
                error!(%error, "couldn't open file storage");

                Error::with(ErrorKind::Io(error.kind()), "while opening file storage")
            })?;

        let mut fdo_ctx = FdoCtx::new(&mut self.crypto, &mut storage, state.tls.clone());

        let cred = Di::read_existing(&mut fdo_ctx)
            .await
            .wrap_err_msg(
                ErrorKind::Fdo(FdoError::Failed),
                "while reading stored Device credentials",
            )
            .and_then(|opt| {
                opt.ok_or(Error::with(
                    ErrorKind::Fdo(FdoError::InvalidArgument),
                    "missing Device credentials",
                ))
            })?;

        Ok(!cred.dc_active)
    }

    /// Register the device and returns the connection.
    async fn register<S>(
        &mut self,
        state: &ConnectionState<S>,
    ) -> Result<ControlFlow<(Self::Client, Self::Connection)>, AstarteError>
    where
        S: StoreCapabilities,
    {
        let ControlFlow::Break(mut config) = self.transfer_ownership(state).await? else {
            return Ok(ControlFlow::Continue(()));
        };

        config.register(state).await
    }
}
