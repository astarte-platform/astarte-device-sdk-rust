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

use astarte_device_sdk::prelude::PropAccess;
use astarte_device_sdk::{AstarteData, Client};
use eyre::{OptionExt, ensure};
use tracing::{info, instrument};

use crate::channel::IncomingData;
use crate::data::{InterfaceData, all_type_data};
use crate::utils::check_astarte_value;
use crate::{AstarteClient, Channel};

#[derive(Debug)]
struct DeviceProperty {}

impl InterfaceData for DeviceProperty {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.DeviceProperty".to_string()
    }

    fn data() -> eyre::Result<HashMap<String, AstarteData>> {
        let data = all_type_data().map(|(name, value)| (format!("/sensor_1/{name}"), value));

        Ok(HashMap::from_iter(data))
    }
}

/// Send a value with a long integer > 2^53 + 1
#[derive(Debug)]
struct DevicePropertyOverflow {}

impl InterfaceData for DevicePropertyOverflow {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.DeviceProperty".to_string()
    }

    fn data() -> eyre::Result<HashMap<String, AstarteData>> {
        let mut data = HashMap::with_capacity(1);

        data.insert(
            "/overflow/longinteger_endpoint".to_string(),
            AstarteData::LongInteger(2i64.pow(55)),
        );
        data.insert(
            "/overflow/longintegerarray_endpoint".to_string(),
            AstarteData::LongIntegerArray(vec![2i64.pow(55); 4]),
        );

        Ok(data)
    }
}

pub(crate) async fn validate_property<T>(
    channel: &mut Channel,
    client: &mut AstarteClient,
    unset: bool,
) -> eyre::Result<()>
where
    T: InterfaceData,
{
    let data = T::data()?;
    let interface_name = T::interface();

    for (data_path, data) in data {
        // Ensure consistent state if the device already existed
        if client
            .property(&interface_name, &data_path)
            .await?
            .is_some()
        {
            client.unset_property(&interface_name, &data_path).await?;

            let IncomingData {
                interface, path, ..
            } = channel.next_data_event().await?;

            ensure!(interface == interface_name);
            ensure!(path == data_path);
        }

        // Send prop
        client
            .set_property(&interface_name, &data_path, data.clone())
            .await?;

        let IncomingData {
            interface,
            path,
            value,
        } = channel.next_data_event().await?;

        ensure!(interface == interface_name);
        ensure!(path == data_path);
        check_astarte_value(&data, &value)?;

        let prop = client
            .property(&interface, &path)
            .await?
            .ok_or_eyre(format!("cannot access property {interface}{path}"))?;

        ensure!(prop == data);

        // Unset
        if unset {
            client.unset_property(&interface_name, &data_path).await?;

            let IncomingData {
                interface, path, ..
            } = channel.next_data_event().await?;

            ensure!(interface == interface_name);
            ensure!(path == data_path);

            let prop = client.property(&interface, &path).await?;

            ensure!(prop.is_none());
        }

        info!(interface, path, "validated")
    }

    Ok(())
}

#[instrument(skip_all)]
pub(crate) async fn check(channel: &mut Channel, client: &mut AstarteClient) -> eyre::Result<()> {
    validate_property::<DeviceProperty>(channel, client, true).await?;
    validate_property::<DevicePropertyOverflow>(channel, client, true).await?;

    Ok(())
}
