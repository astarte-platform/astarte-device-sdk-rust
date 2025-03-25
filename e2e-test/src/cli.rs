// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
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

use std::path::PathBuf;

use clap::Parser;
use reqwest::Url;

#[derive(Debug, Parser)]
pub(crate) struct Cli {
    /// Realm of the device.
    #[arg(long, short, env = "E2E_REALM", default_value = "test")]
    pub(crate) realm: String,
    /// Device id.
    #[arg(long, short, env = "E2E_DEVICE_ID")]
    pub(crate) device_id: String,
    /// Token with access to all the APIs.
    #[arg(long, short, env = "E2E_TOKEN")]
    pub(crate) token: String,
    /// Token to pair the device to astarte.
    #[arg(long, short, env = "E2E_PAIRING_TOKEN")]
    pub(crate) pairing_token: String,
    /// Token with access to all the APIs.
    #[arg(long, short, env = "E2E_BASE_URL", default_value = "astarte.localhost")]
    pub(crate) astarte_base_url: String,
    /// Token with access to all the APIs.
    #[arg(long, short, env = "E2E_STORE_DIR")]
    pub(crate) store_dir: PathBuf,
    /// Connect using a secure transport (HTTPS, WebSocketSecure, ...)
    #[arg(long, env = "E2E_SECURE_TRANSPORT", default_value = "false")]
    pub(crate) secure_transport: bool,
    /// Ignore SSL validation
    #[arg(long, env = "E2E_IGNORE_SSL", default_value = "false")]
    pub(crate) ignore_ssl: bool,
}

impl Cli {
    fn http_or_https(&self) -> &'static str {
        if self.secure_transport {
            "https"
        } else {
            "http"
        }
    }

    pub(crate) fn api_url(&self) -> eyre::Result<Url> {
        format!("{}://api.{}", self.http_or_https(), self.astarte_base_url)
            .parse()
            .map_err(Into::into)
    }

    pub(crate) fn pairing_url(&self) -> eyre::Result<Url> {
        self.api_url()?.join("/pairing").map_err(Into::into)
    }

    pub(crate) fn appengine_url(&self) -> eyre::Result<Url> {
        self.api_url()?.join("/appengine").map_err(Into::into)
    }
}
