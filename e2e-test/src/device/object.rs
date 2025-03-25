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

use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::{Client, DeviceClient};
use chrono::Utc;
use eyre::{bail, ensure, eyre};
use tracing::{info, instrument};

use crate::channel::IncomingData;
use crate::data::{InterfaceData, InterfaceDataObject};
use crate::utils::check_astarte_value;
use crate::Channel;

#[derive(Debug)]
struct DeviceAggregate {}

impl InterfaceData for DeviceAggregate {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.DeviceAggregate".to_string()
    }
}

impl InterfaceDataObject for DeviceAggregate {}

async fn validate_object<T>(
    channel: &mut Channel,
    client: &DeviceClient<SqliteStore>,
) -> eyre::Result<()>
where
    T: InterfaceDataObject,
{
    let data = <T as InterfaceDataObject>::data()?;
    let data_path = T::base_path();
    let interface_name = T::interface();

    client
        .send_object_with_timestamp(&interface_name, &data_path, data.clone(), Utc::now())
        .await?;

    let IncomingData {
        interface,
        path,
        value,
    } = channel.next_data_event().await?;

    ensure!(interface == interface_name);
    ensure!(path == data_path);

    let serde_json::Value::Object(map) = value else {
        bail!("received value that is not a mpa {value}")
    };

    ensure!(data.len() == map.len());

    for (key, data) in data.into_key_values() {
        let v = map
            .get(&key)
            .ok_or_else(|| eyre!("missing key in object {key}"))?;

        check_astarte_value(&data, v)?;
    }

    info!(interface, path, "validated");

    Ok(())
}

#[instrument(skip_all)]
pub(crate) async fn check(
    channel: &mut Channel,
    client: &DeviceClient<SqliteStore>,
) -> eyre::Result<()> {
    validate_object::<DeviceAggregate>(channel, client).await?;

    Ok(())
}
