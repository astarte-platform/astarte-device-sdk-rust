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

//! Builder for the [`FdoConfig`]

use std::marker::PhantomData;
use std::path::Path;
use std::time::Duration;

use astarte_device_fdo::Crypto;
use astarte_device_fdo::astarte_fdo_protocol::Error;
use astarte_device_fdo::astarte_fdo_protocol::error::ErrorKind;
use url::Url;

use crate::builder::{DEFAULT_CHANNEL_SIZE, DEFAULT_REQUEST_TIMEOUT};
use crate::transport::mqtt::DEFAULT_KEEP_ALIVE;

use super::FdoConfig;

/// You can call [`FdoConfigBuilder::storage`].
#[derive(Debug)]
pub struct AddStorageDir;

/// You can call [`FdoConfigBuilder::manufactoring_url`].
#[derive(Debug)]
pub struct AddManufactoringUrl;

/// You can call [`FdoConfigBuilder::tls`].
#[derive(Debug)]
pub struct AddTls;

/// You can call [`FdoConfigBuilder::crypto`].
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
    storage_dir: Option<&'a Path>,
    tls: Option<&'a rustls::ClientConfig>,
    manufactoring_url: Option<&'a Url>,
    timeout: Duration,
    keepalive: Duration,
    channel_size: usize,
    insecure_ssl: bool,
    // NOTE: state for the builder to make sure that at compile time the field is set before build
    _mark: PhantomData<T>,
}

impl<'a, C> FdoConfigBuilder<'a, C, AddStorageDir> {
    pub(crate) fn new(model_no: &'a str, serial_no: &'a str) -> Self {
        FdoConfigBuilder {
            model_no,
            serial_no,
            crypto: None,
            storage_dir: None,
            tls: None,
            manufactoring_url: None,
            timeout: DEFAULT_REQUEST_TIMEOUT,
            keepalive: Duration::from_secs(DEFAULT_KEEP_ALIVE),
            channel_size: DEFAULT_CHANNEL_SIZE,
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
            storage_dir: self.storage_dir,
            tls: self.tls,
            manufactoring_url: self.manufactoring_url,
            timeout: self.timeout,
            keepalive: self.keepalive,
            channel_size: self.channel_size,
            insecure_ssl: self.insecure_ssl,
            _mark: PhantomData,
        }
    }

    /// Configure the send and connection timeout
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    /// Configure the MQTT keepalive
    pub fn set_keepalive(&mut self, keepalive: Duration) {
        self.keepalive = keepalive;
    }

    /// Configure the channel size
    pub fn set_channel_size(&mut self, channel_size: usize) {
        self.channel_size = channel_size;
    }

    /// Configure the insecure ssl
    pub fn set_insecure_ssl(&mut self, insecure_ssl: bool) {
        self.insecure_ssl = insecure_ssl;
    }
}

impl<'a, C> FdoConfigBuilder<'a, C, AddStorageDir> {
    /// Configure the storage directory
    pub fn set_storage(mut self, path: &'a Path) -> FdoConfigBuilder<'a, C, AddManufactoringUrl> {
        self.storage_dir = Some(path);

        self.bind()
    }
}

impl<'a, C> FdoConfigBuilder<'a, C, AddManufactoringUrl> {
    /// Sets the manufacturing url
    pub fn set_manufactoring_url(mut self, url: &'a Url) -> FdoConfigBuilder<'a, C, AddTls> {
        self.manufactoring_url = Some(url);

        self.bind()
    }
}

impl<'a, C> FdoConfigBuilder<'a, C, AddTls> {
    /// Sets the TLS configuration.
    pub fn set_tls(mut self, tls: &'a rustls::ClientConfig) -> FdoConfigBuilder<'a, C, AddCrypto> {
        self.tls = Some(tls);

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
    pub fn build(self) -> Result<FdoConfig<'a, C>, Error>
    where
        C: Crypto,
    {
        let manufactoring_url = self
            .manufactoring_url
            .ok_or(Error::new(ErrorKind::Invalid, "missing manufactoring url"))?
            .clone();
        let tls = self
            .tls
            .ok_or(Error::new(ErrorKind::Invalid, "missing tls"))?;
        let storage = self
            .storage_dir
            .ok_or(Error::new(ErrorKind::Invalid, "missing storage dir"))?;

        let crypto = self
            .crypto
            .ok_or(Error::new(ErrorKind::Invalid, "missing crypto"))?;

        Ok(FdoConfig {
            model_no: self.model_no,
            serial_no: self.serial_no,
            manufactoring_url,
            timeout: self.timeout,
            keepalive: self.keepalive,
            channel_size: self.channel_size,
            insecure_ssl: self.insecure_ssl,
            tls,
            storage,
            crypto,
        })
    }
}
