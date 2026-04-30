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
use std::time::Duration;

use astarte_device_fdo::astarte_fdo_protocol::Error;
use astarte_device_fdo::astarte_fdo_protocol::error::ErrorKind;
use astarte_device_fdo::client::http::InitialClient;
use astarte_device_fdo::di::Di;
use astarte_device_fdo::srv_info::{AstarteMod, AstarteModBuilder};
use astarte_device_fdo::storage::FileStorage;
use astarte_device_fdo::to1::To1;
use astarte_device_fdo::to2::{Hello, To2};
use astarte_device_fdo::{Crypto, Ctx};
use tracing::{error, info, instrument};
use url::Url;

use crate::builder::Config;
use crate::transport::mqtt::config::transport::TransportProvider;
use crate::transport::mqtt::pairing::ApiClient;
use crate::transport::mqtt::{Credential, MqttConfig};

use self::builder::{AddStorageDir, FdoConfigBuilder};

pub mod builder;

/// Configuration to register a device using FDO.
#[derive(Debug)]
pub struct FdoConfig<'a, C> {
    model_no: &'a str,
    serial_no: &'a str,
    manufacturing_url: Url,
    keepalive: Duration,
    insecure_ssl: bool,
    tls: &'a rustls::ClientConfig,
    storage: &'a Path,
    crypto: C,
}

// TODO: most of this should be done inside the builder
impl<'a, C> FdoConfig<'a, C> {
    /// Returns the builder for the FDO config.
    pub fn build(model_no: &'a str, serial_no: &'a str) -> FdoConfigBuilder<'a, C, AddStorageDir> {
        FdoConfigBuilder::new(model_no, serial_no)
    }

    /// Register the device and configures the MQTT connection
    #[instrument(skip_all)]
    pub async fn mqtt(&mut self) -> Result<MqttConfig, Error>
    where
        C: Crypto,
    {
        let astarte_mod = self.register().await?;

        let pairing_url = format!("{}/pairing", astarte_mod.base_url)
            .parse()
            .map_err(|error| {
                error!(%error, base_url = %astarte_mod.base_url, "couldn't parse pairing url");

                Error::new(ErrorKind::Invalid, "pairing url")
            })?;

        Ok(MqttConfig {
            realm: astarte_mod.realm.to_string(),
            device_id: astarte_mod.device_id.to_string(),
            credential: Credential::secret(astarte_mod.secret),
            pairing_url,
            ignore_ssl_errors: self.insecure_ssl,
            keepalive: self.keepalive,
        })
    }

    async fn register(&mut self) -> Result<AstarteMod<'static>, Error>
    where
        C: Crypto,
    {
        let mut storage = FileStorage::open(self.storage.join("fdo"))
            .await
            .map_err(|error| {
                error!(%error, "couldn't open file storage");

                Error::new(ErrorKind::Io, "while opening file storage")
            })?;

        let mut ctx = Ctx::new(&mut self.crypto, &mut storage, self.tls.clone());

        let client = InitialClient::create(self.manufacturing_url.clone(), self.tls.clone())?;

        let di = Di::create(&mut ctx, client, self.model_no, self.serial_no).await?;

        let cred = di.create_credentials(&mut ctx).await?;

        if !cred.dc_active {
            info!("device change TO already run to completion");

            if let Some(dv) = To2::<'_, AstarteModBuilder, Hello>::read_existing(&mut ctx).await? {
                info!(
                    "Astarte mod already stored with device_id: {}",
                    dv.device_id
                );

                return Ok(dv);
            }
        }

        let to1 = To1::new(&cred);

        let rv = to1.rv_owner(&mut ctx).await?;

        let to2 = To2::create(cred, rv, self.serial_no, AstarteMod::builder())?;

        let (to2, amod) = to2.to2_change(&mut ctx).await?;

        info!("Astarte mod received with device_id: {}", amod.device_id);

        let pairing_url = format!("{}/pairing", amod.base_url)
            .parse()
            .map_err(|error| {
                error!(%error, "couldn't parse astarte pairing url");

                Error::new(ErrorKind::Invalid, "astarte pairing url")
            })?;

        let provider = TransportProvider::configure(
            pairing_url,
            amod.secret.to_string(),
            Some(self.storage.to_path_buf()),
            self.insecure_ssl,
        )
        .await
        .map_err(|error| {
            error!(%error, "couldn't configure transport provider");
            Error::new(ErrorKind::Io, "while configuring transport")
        })?;

        let api =
            ApiClient::from_transport(&Config::default(), &provider, &amod.realm, &amod.device_id)
                .map_err(|error| {
                    error!(%error, "couldn't create pairing api client");
                    Error::new(ErrorKind::Invalid, "pairing api client")
                })?;

        // Make sure the credentials are valid and we can connect to astarte,
        //
        // As per: https://fidoalliance.org/specs/FDO/FIDO-Device-Onboard-PS-v1.1-20220419/FIDO-Device-Onboard-PS-v1.1-20220419.html#to2done2-type-71
        let _creds = provider.retrieve_credentials(&api).await.map_err(|error| {
            error!(%error, "couldn't configure transport provider");

            Error::new(ErrorKind::Io, "while configuring transport")
        })?;

        info!("certificate created");

        to2.done(&mut ctx).await?;

        Ok(amod)
    }
}
