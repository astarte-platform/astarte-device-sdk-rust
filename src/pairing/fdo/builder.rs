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

//! Builder for the [`FdoDi`]

use std::marker::PhantomData;
use std::time::Duration;

use astarte_device_fdo::Crypto;
use astarte_device_fdo::astarte_fdo_protocol::Error;
use astarte_device_fdo::astarte_fdo_protocol::error::ErrorKind;
use url::Url;

use crate::transport::mqtt::DEFAULT_KEEP_ALIVE;

use super::FdoDi;

/// You can call [`FdoConfigBuilder::set_manufacturing_url`].
#[derive(Debug)]
pub struct AddManufacturingUrl;

/// You can call [`FdoConfigBuilder::set_crypto`].
#[derive(Debug)]
pub struct AddCrypto;

/// You can call [`FdoConfigBuilder::build`].
#[derive(Debug)]
pub struct Build;

/// Builder for the FDO config.
#[derive(Debug)]
pub struct FdoConfigBuilder<'a, C, T> {
    model_no: &'a str,
    serial_no: &'a str,
    crypto: Option<C>,
    manufacturing_url: Option<&'a Url>,
    keepalive: Duration,
    insecure_ssl: bool,
    // NOTE: state for the builder to make sure that at compile time the field is set before build
    _mark: PhantomData<T>,
}

impl<'a, C> FdoConfigBuilder<'a, C, AddManufacturingUrl> {
    pub(crate) fn new(model_no: &'a str, serial_no: &'a str) -> Self {
        FdoConfigBuilder {
            model_no,
            serial_no,
            crypto: None,
            manufacturing_url: None,
            keepalive: DEFAULT_KEEP_ALIVE,
            insecure_ssl: false,
            _mark: PhantomData,
        }
    }
}

impl<'a, C, T> FdoConfigBuilder<'a, C, T> {
    fn bind<U>(self) -> FdoConfigBuilder<'a, C, U> {
        FdoConfigBuilder {
            model_no: self.model_no,
            serial_no: self.serial_no,
            crypto: self.crypto,
            manufacturing_url: self.manufacturing_url,
            keepalive: self.keepalive,
            insecure_ssl: self.insecure_ssl,
            _mark: PhantomData,
        }
    }

    /// Configure the MQTT keepalive
    pub fn set_keepalive(mut self, keepalive: Duration) -> Self {
        self.keepalive = keepalive;

        self
    }

    /// Configure the insecure ssl
    pub fn set_insecure_ssl(mut self, insecure_ssl: bool) -> Self {
        self.insecure_ssl = insecure_ssl;

        self
    }
}

impl<'a, C> FdoConfigBuilder<'a, C, AddManufacturingUrl> {
    /// Sets the manufacturing url
    pub fn set_manufacturing_url(mut self, url: &'a Url) -> FdoConfigBuilder<'a, C, AddCrypto> {
        self.manufacturing_url = Some(url);

        self.bind()
    }
}

impl<'a, C> FdoConfigBuilder<'a, C, AddCrypto> {
    /// Configure the crypto provider
    pub fn set_crypto(mut self, crypto: C) -> FdoConfigBuilder<'a, C, Build>
    where
        C: Crypto,
    {
        self.crypto = Some(crypto);

        self.bind()
    }
}

impl<'a, C> FdoConfigBuilder<'a, C, Build> {
    /// Creates the FDO configuration
    pub fn build(self) -> Result<FdoDi<'a, C>, Error>
    where
        C: Crypto,
    {
        let manufacturing_url = self
            .manufacturing_url
            .ok_or(Error::new(ErrorKind::Invalid, "missing manufacturing url"))?
            .clone();

        let crypto = self
            .crypto
            .ok_or(Error::new(ErrorKind::Invalid, "missing crypto"))?;

        Ok(FdoDi {
            model_no: self.model_no,
            serial_no: self.serial_no,
            manufacturing_url,
            keepalive: self.keepalive,
            insecure_ssl: self.insecure_ssl,
            crypto,
        })
    }
}
