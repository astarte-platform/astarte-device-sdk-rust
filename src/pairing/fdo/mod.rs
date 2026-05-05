// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
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

//! FIDO Device Onboarding protocol.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use astarte_device_fdo::astarte_fdo_protocol::Error;
use astarte_device_fdo::astarte_fdo_protocol::error::ErrorKind;
use astarte_device_fdo::client::http::InitialClient;
use astarte_device_fdo::di::Di;
use astarte_device_fdo::srv_info::{AstarteMod, AstarteModBuilder};
use astarte_device_fdo::storage::FileStorage;
use astarte_device_fdo::to1::To1;
use astarte_device_fdo::to2::{Hello, To2};
use astarte_device_fdo::{Crypto, Ctx as FdoCtx};
use tracing::{error, info, instrument};
use url::Url;

use crate::builder::{BuildConfig, Config, ConnectionConfig, DeviceTransport};
use crate::pairing::api::client::{ApiClient, ClientArgs};
use crate::store::StoreCapabilities;
use crate::store::wrapper::StoreWrapper;
use crate::transport::mqtt::components::ClientId;
use crate::transport::mqtt::config::transport::TransportProvider;
use crate::transport::mqtt::connection::MqttState;
use crate::transport::mqtt::connection::context::Ctx as PairingCtx;
use crate::transport::mqtt::error::MqttError;
use crate::transport::mqtt::retention::MqttRetention;
use crate::transport::mqtt::{Mqtt, MqttClient};

use self::builder::{AddManufacturingUrl, FdoConfigBuilder};

use super::{Pairing, PairingConfig};

pub mod builder;

/// Initialize the device to pair with FDO.
#[derive(Debug)]
pub struct FdoDi<'a, C> {
    model_no: &'a str,
    serial_no: &'a str,
    manufacturing_url: Url,
    keepalive: Duration,
    insecure_ssl: bool,
    crypto: C,
}

impl<'a, C> FdoDi<'a, C> {
    /// Initializes a device to be paired to be then bind to a cloud.
    #[instrument(skip_all)]
    pub async fn device_initialize(
        mut self,
        storage: &Path,
        tls: rustls::ClientConfig,
    ) -> Result<FdoConfig<C>, Error>
    where
        C: Crypto,
    {
        let mut storage = FileStorage::open(storage.join("fdo"))
            .await
            .map_err(|error| {
                error!(%error, "couldn't open file storage");

                Error::new(ErrorKind::Io, "while opening file storage")
            })?;

        let mut ctx = FdoCtx::new(&mut self.crypto, &mut storage, tls.clone());

        let client = InitialClient::create(self.manufacturing_url.clone(), tls.clone())?;

        let di = Di::create(&mut ctx, client, self.model_no, self.serial_no).await?;

        let _cred = di.create_credentials(&mut ctx).await?;

        info!("Device Initialize");

        Ok(FdoConfig {
            serial_no: self.serial_no.to_string(),
            keepalive: self.keepalive,
            insecure_ssl: self.insecure_ssl,
            crypto: self.crypto,
        })
    }
}

/// Configuration to register a device using FDO.
#[derive(Debug)]
pub struct FdoConfig<C> {
    serial_no: String,
    keepalive: Duration,
    insecure_ssl: bool,
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
    async fn register<S>(
        &mut self,
        pairing_ctx: &mut PairingCtx<'_, S>,
    ) -> Result<PairingConfig, Error>
    where
        C: Crypto,
    {
        let storage = pairing_ctx
            .state
            .config
            .writable_dir
            .as_ref()
            .ok_or(Error::new(ErrorKind::Invalid, "missing writable directory"))?;

        let mut storage = FileStorage::open(storage.join("fdo"))
            .await
            .map_err(|error| {
                error!(%error, "couldn't open file storage");

                Error::new(ErrorKind::Io, "while opening file storage")
            })?;

        let tls = pairing_ctx.provider.api_tls_config().map_err(|error| {
            error!(%error, "couldn't configure tls");

            Error::new(ErrorKind::Io, "while configuring TLS")
        })?;

        let mut fdo_ctx = FdoCtx::new(&mut self.crypto, &mut storage, tls.clone());

        let cred = Di::read_existing(&mut fdo_ctx).await.and_then(|opt| {
            opt.ok_or(Error::new(ErrorKind::Invalid, "missing Device credentials"))
        })?;

        if !cred.dc_active {
            info!("device change TO already run to completion");

            if let Some(amod) =
                To2::<'_, AstarteModBuilder, Hello>::read_existing(&mut fdo_ctx).await?
            {
                info!(
                    "Astarte mod already stored with device_id: {}",
                    amod.device_id
                );

                let pairing_url =
                    format!("{}/pairing", amod.base_url)
                        .parse()
                        .map_err(|error| {
                            error!(%error, "couldn't parse astarte pairing url");

                            Error::new(ErrorKind::Invalid, "astarte pairing url")
                        })?;

                return Ok(PairingConfig {
                    client_id: ClientId {
                        realm: amod.realm,
                        device_id: amod.device_id,
                    },
                    secret: amod.secret,
                    pairing_url,
                    keepalive: self.keepalive,
                });
            }
        }

        let to1 = To1::new(&cred);

        let rv = to1.rv_owner(&mut fdo_ctx).await?;

        let to2 = To2::create(cred, rv, &self.serial_no, AstarteMod::builder())?;

        let (to2, amod) = to2.to2_change(&mut fdo_ctx).await?;

        info!("Astarte mod received with device_id: {}", amod.device_id);

        let pairing_url = format!("{}/pairing", amod.base_url)
            .parse()
            .map_err(|error| {
                error!(%error, "couldn't parse astarte pairing url");

                Error::new(ErrorKind::Invalid, "astarte pairing url")
            })?;

        let args = ClientArgs {
            realm: &amod.realm,
            device_id: &amod.device_id,
            pairing_url: &pairing_url,
            token: &amod.secret,
        };

        let api = ApiClient::from_transport(&Config::default(), pairing_ctx.provider, args)
            .map_err(|error| {
                error!(%error, "couldn't create pairing api client");
                Error::new(ErrorKind::Invalid, "pairing api client")
            })?;

        let client_id = ClientId::<&str> {
            realm: &amod.realm,
            device_id: &amod.realm,
        };

        // Make sure the credentials are valid and we can connect to astarte,
        //
        // As per: https://fidoalliance.org/specs/FDO/FIDO-Device-Onboard-PS-v1.1-20220419/FIDO-Device-Onboard-PS-v1.1-20220419.html#to2done2-type-71
        let _creds = pairing_ctx
            .provider
            .create_credentials(&api, client_id)
            .await
            .map_err(|error| {
                error!(%error, "couldn't configure transport provider");

                Error::new(ErrorKind::Io, "while configuring transport")
            })?;

        info!("certificate created");

        to2.done(&mut fdo_ctx).await?;

        Ok(PairingConfig {
            client_id: ClientId {
                realm: amod.realm,
                device_id: amod.device_id,
            },
            secret: amod.secret,
            pairing_url,
            keepalive: self.keepalive,
        })
    }
}

impl<C> Pairing for FdoConfig<C>
where
    C: Crypto + Send + Sync,
{
    type Error = Error;

    async fn config<S>(&mut self, ctx: &mut PairingCtx<'_, S>) -> Result<PairingConfig, Self::Error>
    where
        S: Send + Sync,
    {
        self.register(ctx).await
    }
}

impl<S, C> ConnectionConfig<S> for FdoConfig<C>
where
    C: Crypto + Send + Sync,
    S: StoreCapabilities,
{
    type Conn = Mqtt<Self::Store, FdoConfig<C>>;
    type Store = S;
    type Err = MqttError;

    async fn connect(
        self,
        config: crate::builder::BuildConfig<S>,
    ) -> Result<DeviceTransport<Self::Conn>, Self::Err>
    where
        S: crate::prelude::PropertyStore,
    {
        let BuildConfig { store, state } = config;

        let store_wrapper = StoreWrapper::new(store);

        let (retention_tx, retention_rx) = async_channel::bounded(state.config.channel_size.get());
        let retention = MqttRetention::new(retention_rx);

        let client = MqttClient::new(retention_tx, store_wrapper.clone(), Arc::clone(&state));

        let provider =
            TransportProvider::configure(state.config.writable_dir.clone(), self.insecure_ssl)
                .await
                .map_err(MqttError::Pairing)?;

        let mqtt_state = MqttState::new(self);

        let connection = Mqtt {
            connection: mqtt_state,
            client_sender: Arc::clone(&client.sender),
            provider,
            retention,
            store: store_wrapper.clone(),
            state,
        };

        Ok(DeviceTransport {
            sender: client,
            connection,
            store: store_wrapper,
        })
    }
}
