// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
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

use std::env;
use std::io::{IsTerminal, stdout};

use astarte_device_sdk::DeviceClient;
use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::transport::mqtt::{Credential, Mqtt, MqttArgs};
use clap::Parser;
use eyre::{Context, eyre};
use tokio::task::JoinSet;
use tracing::{error, info, trace};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use astarte_device_sdk::{builder::DeviceBuilder, prelude::*, transport::mqtt::MqttConfig};

use self::api::ApiClient;
use self::channel::Channel;
use self::cli::{Cli, Command, Config};
use self::utils::retry;

pub(crate) mod api;
pub mod channel;
pub(crate) mod cli;
pub(crate) mod data;
pub(crate) mod device;
pub(crate) mod server;
pub(crate) mod tls;
pub(crate) mod utils;

const INTERFACE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/interfaces");

pub(crate) type AstarteClient = DeviceClient<Mqtt<SqliteStore>>;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();

    color_eyre::install()?;

    init_tracing()?;

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| eyre!("couldn't install default crypto provider"))?;

    let config = match cli.command {
        Command::Run(run) => Config::new(cli.url, run),
        Command::Healthy { wait: true } => {
            let tls: rustls::ClientConfig = crate::tls::client_config()?;
            let client = reqwest::Client::builder()
                .use_preconfigured_tls(tls)
                .build()?;

            retry(20, || async {
                ApiClient::cluster_healthy(&client, &cli.url.api_url()?).await
            })
            .await?;

            info!("cluster is healthy");

            return Ok(());
        }
        Command::Healthy { wait: false } => {
            let tls = crate::tls::client_config()?;

            let client = reqwest::Client::builder()
                .use_preconfigured_tls(tls)
                .build()?;

            ApiClient::cluster_healthy(&client, &cli.url.api_url()?).await?;

            info!("cluster is healthy");

            return Ok(());
        }
    };

    let mut mqtt_config = MqttConfig::new(MqttArgs {
        realm: config.run.realm.to_string(),
        device_id: config.run.device_id.to_string(),
        credential: Credential::paring_token(config.run.pairing_token.clone()),
        pairing_url: config.url.pairing_url()?,
    });

    // Ignore SSL for local testing
    if config.url.ignore_ssl {
        mqtt_config = mqtt_config.ignore_ssl_errors();
    }

    let api = config.api_client()?;

    retry(20, || async { api.is_healthy().await }).await?;

    let (tx_cancel, cancel) = tokio::sync::broadcast::channel::<()>(2);
    let mut tasks = JoinSet::<eyre::Result<()>>::new();

    let appengine = config.url.appengine_websocket()?;
    let mut channel = Channel::connect(
        appengine,
        &config.run.realm,
        &config.run.token,
        &config.run.device_id,
        &mut tasks,
        cancel,
    )
    .await?;

    let store = SqliteStore::options()
        .with_writable_dir(&config.run.store_dir)
        .await?;

    let (mut client, connection) = DeviceBuilder::new()
        .writable_dir(&config.run.store_dir)
        .store(store)
        .interface_directory(INTERFACE_DIR)?
        .connection(mqtt_config)
        .build()
        .await?;

    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

    tasks.spawn(async move {
        channel::register_triggers(&mut channel).await?;

        device::interfaces::check_add(&api, &mut client).await?;

        // Device
        device::individual::check(&mut channel, &mut client).await?;
        device::property::check(&mut channel, &mut client).await?;
        device::object::check(&mut channel, &mut client).await?;
        device::update::check(&api, &mut channel, &mut client).await?;

        // Server
        server::individual::check(&api, &client).await?;
        server::property::check(&api, &client).await?;
        server::object::check(&api, &client).await?;

        device::interfaces::check_remove(&api, &mut client).await?;

        client.disconnect().await?;

        channel.next_device_disconnected().await?;

        info!("e2e completed successfully");

        tx_cancel.send(())?;

        Ok(())
    });

    let mut ret_res = Ok(());

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {
                trace!("task exited")
            }
            Err(err) if err.is_cancelled() => {
                trace!("task cancelled")
            }
            Err(err) => {
                error!(error = %err, "task panicked");

                tasks.abort_all();

                if ret_res.is_ok() {
                    ret_res = Err(err.into());
                }
            }
            Ok(Err(err)) => {
                error!(error = %err, "task returned an error");

                if ret_res.is_ok() {
                    ret_res = Err(err);
                }
            }
        }
    }

    ret_res
}

fn init_tracing() -> eyre::Result<()> {
    let fmt = tracing_subscriber::fmt::layer().with_ansi(stdout().is_terminal());

    let env = match std::env::var("RUST_LOG") {
        Ok(env) => env,
        Err(env::VarError::NotPresent) => "e2e_test=debug,astarte_device_sdk=debug".to_string(),
        Err(err) => {
            return Err(err).wrap_err("invalid RUST_LOG env variable");
        }
    };

    let env = EnvFilter::builder().parse(env)?;

    tracing_subscriber::registry()
        .with(fmt)
        .with(env)
        .try_init()?;

    Ok(())
}
