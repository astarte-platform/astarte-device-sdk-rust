/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::env;
use std::io::{stdout, IsTerminal};

use astarte_device_sdk::transport::mqtt::Credential;
use clap::Parser;
use eyre::{eyre, Context};
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use astarte_device_sdk::{builder::DeviceBuilder, prelude::*, transport::mqtt::MqttConfig};

use self::api::ApiClient;
use self::channel::Channel;
use self::cli::Cli;
use self::utils::retry;

pub(crate) mod api;
pub mod channel;
pub(crate) mod cli;
pub(crate) mod data;
pub(crate) mod device;
pub(crate) mod server;
pub(crate) mod utils;

const INTERFACE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/interfaces");

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();

    color_eyre::install()?;

    init_tracing()?;

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| eyre!("couldn't install default crypto provider"))?;

    let mut config = MqttConfig::new(
        &cli.realm,
        &cli.device_id,
        Credential::paring_token(cli.pairing_token.clone()),
        cli.pairing_url()?,
    );

    // Ignore SSL for local testing
    if cli.ignore_ssl {
        config.ignore_ssl_errors();
    }

    let api = ApiClient::build(
        cli.api_url()?,
        cli.realm.clone(),
        cli.device_id.clone(),
        &cli.token,
    )?;

    retry(20, || async { api.cluster_healthy().await }).await?;

    let (tx_cancel, mut cancel) = tokio::sync::broadcast::channel::<()>(2);
    let mut tasks = JoinSet::<eyre::Result<()>>::new();

    let appengine = cli.appengine_url()?;
    let mut channel = Channel::connect(
        &appengine,
        &cli.realm,
        &cli.token,
        &cli.device_id,
        &mut tasks,
        tx_cancel.subscribe(),
    )
    .await?;

    let (client, connection) = DeviceBuilder::new()
        .store_dir(&cli.store_dir)
        .await?
        .interface_directory(INTERFACE_DIR)?
        .connection(config)
        .build()
        .await?;

    channel::register_triggers(&mut channel).await?;

    tasks.spawn(async move {
        tokio::select! {
            res = cancel.recv() => {
                res.wrap_err("handle events errored")?;
            }
            res = connection.handle_events() => {
                res.wrap_err("handle events errored")?;
            }
        }

        Ok(())
    });

    tasks.spawn(async move {
        device::interfaces::check_add(&api, &client).await?;

        // Device
        device::individual::check(&mut channel, &client).await?;
        device::property::check(&mut channel, &client).await?;
        device::object::check(&mut channel, &client).await?;

        // Server
        server::individual::check(&api, &client).await?;
        server::property::check(&api, &client).await?;
        server::object::check(&api, &client).await?;

        device::interfaces::check_remove(&api, &client).await?;

        info!("e2e completed successfully");

        tx_cancel.send(())?;

        Ok(())
    });

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                error!(error = %err, "Task panicked");

                return Err(err.into());
            }
            Ok(Err(err)) => {
                error!(error = %err, "Task returned an error");

                return Err(err);
            }
        }
    }

    Ok(())
}

fn init_tracing() -> eyre::Result<()> {
    let fmt = tracing_subscriber::fmt::layer().with_ansi(stdout().is_terminal());

    // TODO: default to logging only the e2e test and astarte_device_sdk
    let env = EnvFilter::from_default_env();

    tracing_subscriber::registry()
        .with(fmt)
        .with(env)
        .try_init()?;

    Ok(())
}
