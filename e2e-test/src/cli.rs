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

use clap::Parser;
use reqwest::Url;

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

impl Cli {
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
}
