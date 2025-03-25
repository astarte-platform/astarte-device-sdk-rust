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

use std::collections::HashMap;
use std::time::Duration;

use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::{Client, DeviceClient, DeviceEvent};
use eyre::{ensure, eyre, OptionExt};
use tracing::{info, instrument};

use crate::api::ApiClient;
use crate::data::{InterfaceData, InterfaceDataObject};
use crate::utils::convert_type_to_json;

#[derive(Debug)]
struct ServerAggregate {}

impl InterfaceData for ServerAggregate {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.ServerAggregate".to_string()
    }
}

impl InterfaceDataObject for ServerAggregate {}

async fn validate_object<T>(api: &ApiClient, client: &DeviceClient<SqliteStore>) -> eyre::Result<()>
where
    T: InterfaceDataObject,
{
    let exp = <T as InterfaceDataObject>::data()?;
    let data_path = T::base_path();
    let interface_name = T::interface();

    let obj: HashMap<String, serde_json::Value> = exp
        .iter()
        .map(|(key, value)| (key.clone(), convert_type_to_json(value)))
        .collect();

    api.send_on_interface(&interface_name, &data_path, obj)
        .await?;

    let DeviceEvent {
        interface,
        path,
        data,
    } = tokio::time::timeout(Duration::from_secs(2), client.recv()).await??;

    ensure!(interface == interface_name);
    ensure!(path == data_path);

    let obj = data
        .as_object()
        .ok_or_eyre("received value that is not an object")?;

    ensure!(exp.len() == obj.len());

    for (key, data) in exp.into_key_values() {
        let v = obj
            .get(&key)
            .ok_or_else(|| eyre!("missing key in object {key}"))?;

        ensure!(*v == data);
    }

    info!(interface, path, "validated");

    Ok(())
}

#[instrument(skip_all)]
pub(crate) async fn check(api: &ApiClient, client: &DeviceClient<SqliteStore>) -> eyre::Result<()> {
    validate_object::<ServerAggregate>(api, client).await?;

    Ok(())
}
