// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

//! Example on connecting to the Astarte MessageHub.

use std::{f64, time::Duration};

use astarte_device_sdk::{
    DeviceClient, DeviceConnection,
    aggregate::AstarteObject,
    builder::DeviceBuilder,
    client::RecvError,
    prelude::*,
    store::{SqliteStore, StoreCapabilities},
    transport::grpc::{Grpc, GrpcConfig, tonic::transport::Endpoint},
};
use eyre::OptionExt;
use tokio::task::JoinSet;
use tracing::{error, info, level_filters::LevelFilter, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

/// Unique ID for the current application to identify to the message hub with.
const NODE_UUID: Uuid = uuid::uuid!("0444d8c3-f3f1-4b89-9e68-6ffb50ec1839");
/// URL the MessageHub is listening on
const MESSAGE_HUB_URL: &str = "http://127.0.0.1:50051";
/// Writable directory to store the persistent state of the device
const STORE_DIRECTORY: &str = "./store-dir";

const AGGREGATED_DEVICE: &str =
    include_str!("../../docs/interfaces/org.astarte-platform.rust.get-started.Aggregated.json");
const INDIVIDUAL_DEVICE: &str = include_str!(
    "../../docs/interfaces/org.astarte-platform.rust.get-started.IndividualDevice.json"
);
const INDIVIDUAL_SERVER: &str = include_str!(
    "../../docs/interfaces/org.astarte-platform.rust.get-started.IndividualServer.json"
);
const PROPERTY_DEVICE: &str =
    include_str!("../../docs/interfaces/org.astarte-platform.rust.get-started.Property.json");

const OBJECT_UNIQ_STORED: &str = include_str!(
    "../retention/interfaces/org.astarte-platform.rust.examples.individual-datastream.StoredUniqDeviceObject.json"
);

/// Used to receive the IndividualDevice data.
///
/// This need to be an enum because we deserialize each endpoint in it's own variables
#[derive(Debug, FromEvent)]
#[from_event(
    interface = "org.astarte-platform.rust.get-started.IndividualServer",
    aggregation = "individual"
)]
enum ServerIndividual {
    #[mapping(endpoint = "/%{id}/data")]
    Double(f64),
}

async fn init() -> eyre::Result<(
    DeviceClient<Grpc<SqliteStore>>,
    DeviceConnection<Grpc<SqliteStore>>,
)> {
    tokio::fs::create_dir_all(&STORE_DIRECTORY).await?;

    let endpoint = Endpoint::from_static(MESSAGE_HUB_URL);
    let grpc_config = GrpcConfig::new(NODE_UUID, endpoint);

    let store = SqliteStore::options()
        .with_writable_dir(STORE_DIRECTORY)
        .await?;

    let (client, connection) = DeviceBuilder::new()
        .writable_dir(STORE_DIRECTORY)
        .store(store)
        .interface_str(AGGREGATED_DEVICE)?
        .interface_str(INDIVIDUAL_DEVICE)?
        .interface_str(INDIVIDUAL_SERVER)?
        .interface_str(PROPERTY_DEVICE)?
        .interface_str(OBJECT_UNIQ_STORED)?
        .connection(grpc_config)
        .build()
        .await?;

    Ok((client, connection))
}

async fn receive_data<S>(client: DeviceClient<Grpc<S>>) -> eyre::Result<()>
where
    S: PropertyStore + StoreCapabilities,
{
    loop {
        let event = match client.recv().await {
            Ok(event) => event,
            Err(RecvError::Disconnected) => {
                info!("client disconnected");
                return Ok(());
            }
            Err(err) => {
                error!(error = %eyre::Report::new(err), "received error from client");
                continue;
            }
        };

        match event.interface.as_str() {
            "org.astarte-platform.rust.get-started.IndividualServer" => {
                // parse the path to extract the id part
                // e.g. '/42/data' will strip the '/' and '/data' to return '42'
                let id = event
                    .path
                    .strip_prefix("/")
                    .and_then(|s| s.strip_suffix("/data"))
                    .ok_or_eyre("couldn't get endpoint id parameter")?
                    .to_string();

                let ServerIndividual::Double(value) = ServerIndividual::from_event(event)?;

                info!(id, value, "received new datastream on IndividualServer");
            }
            interface => {
                warn!(interface, "unhandled interface event received");

                continue;
            }
        }
    }
}

/// Aggregated object
#[derive(Debug, IntoAstarteObject)]
struct AggregatedDevice {
    double_endpoint: f64,
    string_endpoint: String,
}

/// Stored object datastream
#[derive(Debug, Clone, IntoAstarteObject)]
struct StoredObjectDatastream {
    longinteger: i64,
    boolean: bool,
}

impl StoredObjectDatastream {
    fn new(longinteger: i64, boolean: bool) -> Self {
        Self {
            longinteger,
            boolean,
        }
    }
}

/// Send data after an interval to every interface
async fn send_data<S>(mut client: DeviceClient<Grpc<S>>) -> eyre::Result<()>
where
    S: PropertyStore + StoreCapabilities,
{
    // Every 2 seconds send the data
    let mut interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        // Publish on the IndividualDevice
        // NOTE errors are inspected but not bubbled up to handle a sudden disconnection of the message hub
        let _ = client
            .send_individual(
                "org.astarte-platform.rust.get-started.IndividualDevice",
                "/double_endpoint",
                42.6.try_into()?,
            )
            .await
            .inspect_err(|e| error!(error = e.to_string(), "error while sending individual"));

        let stored_object = StoredObjectDatastream::new(2i64.pow(32), false);
        let stored_object = AstarteObject::try_from(stored_object)?;
        // NOTE errors are inspected but not bubbled up to handle a sudden disconnection of the message hub
        let _ = client
            .send_object(
                "org.astarte-platform.rust.examples.individual-datastream.StoredUniqDeviceObject",
                "/endpoint",
                stored_object,
            )
            .await
            .inspect_err(|e| error!(error = e.to_string(), "error while sending object"));
        // Publish on the Aggregaed
        let obj_data = AggregatedDevice {
            double_endpoint: 42.0,
            string_endpoint: "Sensor 1".to_string(),
        };
        // NOTE errors are inspected but not bubbled up to handle a sudden disconnection of the message hub
        let _ = client
            .send_object(
                "org.astarte-platform.rust.get-started.Aggregated",
                "/group_data",
                obj_data.try_into()?,
            )
            .await
            .inspect_err(|e| error!(error = e.to_string(), "error while sending object"));
        // Set the Property
        // NOTE errors are inspected but not bubbled up to handle a sudden disconnection of the message hub
        let _ = client
            .set_property(
                "org.astarte-platform.rust.get-started.Property",
                "/double_endpoint",
                42.0.try_into()?,
            )
            .await
            .inspect_err(|e| error!(error = e.to_string(), "error while setting property"));

        interval.tick().await;
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    init_tracing()?;
    let (client, connection) = init().await?;

    info!("connected to the MessageHub");

    let mut tasks = JoinSet::<eyre::Result<()>>::new();

    // task to poll updates from the connection
    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

    // receive events from the MessageHub
    tasks.spawn(receive_data(client.clone()));

    // send data to the MessageHub
    tasks.spawn(send_data(client));

    // cleanly close all the other tasks
    tasks.spawn(async move {
        tokio::signal::ctrl_c().await?;

        info!("SIGINT received, exiting");

        Ok(())
    });

    // handle tasks termination
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {
                // Close all the other tasks
                tasks.abort_all();
            }
            Ok(Err(err)) => {
                error!("task returned an error");

                return Err(err);
            }
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                error!("task panicked");

                return Err(err.into());
            }
        }
    }

    info!("device disconnected");

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
