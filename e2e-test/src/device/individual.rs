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

use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::{AstarteType, Client, DeviceClient};
use chrono::Utc;
use eyre::ensure;
use tracing::{info, instrument};

use crate::channel::IncomingData;
use crate::data::InterfaceData;
use crate::utils::check_astarte_value;
use crate::Channel;

#[derive(Debug)]
struct DeviceDatastream {}

impl InterfaceData for DeviceDatastream {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.DeviceDatastream".to_string()
    }
}

/// Send a value with a long integer > 2^53 + 1
#[derive(Debug)]
struct DeviceDatastreamOverflow {}

impl InterfaceData for DeviceDatastreamOverflow {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.DeviceDatastream".to_string()
    }

    fn data() -> eyre::Result<HashMap<String, AstarteType>> {
        let mut data = HashMap::with_capacity(2);

        data.insert(
            "/longinteger_endpoint".to_string(),
            AstarteType::LongInteger(2i64.pow(55)),
        );
        data.insert(
            "/longintegerarray_endpoint".to_string(),
            AstarteType::LongIntegerArray(vec![2i64.pow(55); 4]),
        );

        Ok(data)
    }
}

/// Test retention and reliability combinations
#[derive(Debug)]
struct CustomDeviceDatastream {}

impl InterfaceData for CustomDeviceDatastream {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.CustomDeviceDatastream".to_string()
    }

    fn data() -> eyre::Result<HashMap<String, AstarteType>> {
        let data = HashMap::from_iter(
            [
                ("/volatileUnreliable", AstarteType::LongInteger(42)),
                ("/volatileGuaranteed", AstarteType::Boolean(false)),
                ("/volatileUnique", AstarteType::Double(35.2)),
                ("/storedUnreliable", AstarteType::LongInteger(42)),
                ("/storedGuaranteed", AstarteType::Boolean(false)),
                ("/storedUnique", AstarteType::Double(35.2)),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );

        Ok(data)
    }
}

pub(crate) async fn validate_individual<T>(
    channel: &mut Channel,
    client: &DeviceClient<SqliteStore>,
) -> eyre::Result<()>
where
    T: InterfaceData,
{
    let data = T::data()?;
    let interface_name = T::interface();

    for (data_path, data) in data {
        client
            .send_with_timestamp(&interface_name, &data_path, data.clone(), Utc::now())
            .await?;

        let IncomingData {
            interface,
            path,
            value,
        } = channel.next_data_event().await?;

        ensure!(interface == interface_name);
        ensure!(path == data_path);
        check_astarte_value(&data, &value)?;

        info!(interface, path, "validated")
    }

    Ok(())
}

#[instrument(skip_all)]
pub(crate) async fn check(
    channel: &mut Channel,
    client: &DeviceClient<SqliteStore>,
) -> eyre::Result<()> {
    validate_individual::<DeviceDatastream>(channel, client).await?;
    validate_individual::<DeviceDatastreamOverflow>(channel, client).await?;
    validate_individual::<CustomDeviceDatastream>(channel, client).await?;

    Ok(())
}
