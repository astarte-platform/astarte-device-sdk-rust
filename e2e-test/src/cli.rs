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

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use color_eyre::Section;
use eyre::eyre;
use reqwest::Url;

use crate::api::ApiClient;

#[derive(Debug, Parser)]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) url: AstarteUrl,

    /// Command to run
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Args)]
pub(crate) struct AstarteUrl {
    /// Astarte base domain.
    #[arg(
        long,
        short,
        env = "E2E_BASE_DOMAIN",
        default_value = "astarte.localhost"
    )]
    pub(crate) astarte_base_domain: String,
    /// Connect using a secure transport (HTTPS, WebSocketSecure, ...)
    #[arg(long, env = "E2E_SECURE_TRANSPORT", default_value = "false")]
    pub(crate) secure_transport: bool,
    /// Ignore SSL validation
    #[arg(long, env = "E2E_IGNORE_SSL", default_value = "false")]
    pub(crate) ignore_ssl: bool,
}

impl AstarteUrl {
    fn http_or_https(&self) -> &'static str {
        if self.secure_transport {
            "https"
        } else {
            "http"
        }
    }

    pub(crate) fn api_url(&self) -> eyre::Result<Url> {
        format!(
            "{}://api.{}",
            self.http_or_https(),
            self.astarte_base_domain
        )
        .parse()
        .map_err(Into::into)
    }

    pub(crate) fn pairing_url(&self) -> eyre::Result<Url> {
        self.api_url()?.join("/pairing").map_err(Into::into)
    }

    pub(crate) fn appengine_url(&self) -> eyre::Result<Url> {
        self.api_url()?.join("/appengine").map_err(Into::into)
    }

    pub(crate) fn appengine_websocket(&self) -> eyre::Result<Url> {
        let mut url = self.appengine_url()?;

        let Ok(mut path) = url.path_segments_mut() else {
            return Err(eyre!("couldn't get path segments").note(format!("for url {url}")));
        };

        path.extend(["v1", "socket", "websocket"]);

        drop(path);

        let scheme = if self.secure_transport { "wss" } else { "ws" };

        url.set_scheme(scheme).map_err(|()| {
            eyre!("couldn't set the scheme {scheme}").note(format!("for url {url}"))
        })?;

        Ok(url)
    }
}

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum Command {
    /// Run the e2e tests
    Run(Run),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct Run {
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
    #[arg(long, short, env = "E2E_STORE_DIR")]
    pub(crate) store_dir: PathBuf,
}

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) url: AstarteUrl,
    pub(crate) run: Run,
}

impl Config {
    pub(crate) fn new(url: AstarteUrl, run: Run) -> Self {
        Self { url, run }
    }

    pub(crate) fn api_client(&self) -> eyre::Result<ApiClient> {
        ApiClient::build(
            self.url.api_url()?,
            self.run.realm.clone(),
            self.run.device_id.clone(),
            &self.run.token,
        )
    }
}
