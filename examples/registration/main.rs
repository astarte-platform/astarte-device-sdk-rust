// This file is part of Astarte.
//
// Copyright 2021, 2026 SECO Mind Srl
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

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use astarte_device_sdk::pairing::api::registration::{RegisterDevice, register_device};
use clap::Parser;
use rustls_platform_verifier::BuilderVerifierExt;
use serde::Deserialize;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

#[derive(Deserialize)]
struct Config {
    realm: String,
    device_id: String,
    pairing_token: String,
    pairing_url: Url,
    #[serde(rename = "store_dir")]
    _store_dir: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to the config file for the example
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Limit run time of the transmission of the example (in seconds)
    #[arg(short, long)]
    timeout_secs: Option<NonZeroU32>,
    /// Limit number of iteration of the send loop
    #[arg(short, long)]
    loop_times: Option<NonZeroU32>,
    /// Ignore ssl errors
    #[arg(short, long, default_value = "false")]
    ignore_ssl: bool,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    init_tracing()?;

    let args = Args::parse();

    // Load configuration
    let file_path = args
        .config
        .as_deref()
        .unwrap_or_else(|| Path::new("./examples/object_datastream/configuration.json"));
    let file = std::fs::read_to_string(file_path)?;
    let Config {
        realm,
        device_id,
        pairing_token,
        pairing_url,
        _store_dir,
    } = serde_json::from_str(&file)?;

    info!(%device_id, "attempting to register the device");

    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_platform_verifier()?
    .with_no_client_auth();

    let args = RegisterDevice {
        tls,
        pairing_url: &pairing_url,
        token: &pairing_token,
        realm: &realm,
        device_id: &device_id,
    };
    let credentials_secret = register_device(args).await?;

    info!(
        credentials_secret,
        "device registered, received credentials secret"
    );

    Ok(())
}

fn init_tracing() -> eyre::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive("astarte_device_sdk=debug".parse()?)
                .from_env_lossy()
                .add_directive(LevelFilter::INFO.into()),
        )
        .try_init()?;

    Ok(())
}
