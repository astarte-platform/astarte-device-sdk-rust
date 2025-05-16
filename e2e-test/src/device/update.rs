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

//! Updates an interface major version

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use astarte_device_sdk::astarte_interfaces::{error::Error as InterfaceError, Interface};
use astarte_device_sdk::prelude::DynamicIntrospection;
use astarte_device_sdk::AstarteType;
use eyre::ensure;
use tracing::{debug, instrument};

use crate::api::ApiClient;
use crate::data::InterfaceData;
use crate::{retry, AstarteClient, Channel};

use super::individual::validate_individual;
use super::property::validate_property;

/// Send a value of type double
#[derive(Debug)]
struct DeviceProperty01 {}

impl InterfaceData for DeviceProperty01 {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.ForUpdateDeviceProperty".to_string()
    }

    fn data() -> eyre::Result<HashMap<String, AstarteType>> {
        let data = HashMap::from_iter(
            [("/sensor_1/endpoint", AstarteType::try_from(42.1)?)].map(|(k, v)| (k.to_string(), v)),
        );

        Ok(data)
    }
}

/// Send a value of type long integer
#[derive(Debug)]
struct DeviceProperty02 {}

impl InterfaceData for DeviceProperty02 {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.ForUpdateDeviceProperty".to_string()
    }

    fn data() -> eyre::Result<HashMap<String, AstarteType>> {
        let data = HashMap::from_iter(
            [("/sensor_1/endpoint", AstarteType::LongInteger(21))].map(|(k, v)| (k.to_string(), v)),
        );

        Ok(data)
    }
}

/// Send a value of type double
#[derive(Debug)]
struct DeviceDatastream01 {}

impl InterfaceData for DeviceDatastream01 {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.ForUpdateDeviceDatastream".to_string()
    }

    fn data() -> eyre::Result<HashMap<String, AstarteType>> {
        let data = HashMap::from_iter(
            [
                ("/sensor_1/stored", AstarteType::try_from(42.1)?),
                ("/sensor_1/volatile", AstarteType::try_from(42.1)?),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );

        Ok(data)
    }
}

/// Send a value of type long integer
#[derive(Debug)]
struct DeviceDatastream02 {}

impl InterfaceData for DeviceDatastream02 {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.ForUpdateDeviceDatastream".to_string()
    }

    fn data() -> eyre::Result<HashMap<String, AstarteType>> {
        let data = HashMap::from_iter(
            [
                ("/sensor_1/stored", AstarteType::LongInteger(21)),
                ("/sensor_1/volatile", AstarteType::LongInteger(21)),
            ]
            .map(|(k, v)| (k.to_string(), v)),
        );

        Ok(data)
    }
}

const UPDATED_INTERFACES: &[&str] = &[
    include_str!(
        "../../interfaces/update/org.astarte-platform.rust.e2etest.ForUpdateDeviceProperty.json"
    ),
    include_str!(
        "../../interfaces/update/org.astarte-platform.rust.e2etest.ForUpdateDeviceDatastream.json"
    ),
];

#[instrument(skip_all)]
pub(crate) async fn check(
    api: &ApiClient,
    channel: &mut Channel,
    client: &mut AstarteClient,
) -> eyre::Result<()> {
    validate_property::<DeviceProperty01>(channel, client, false).await?;
    validate_individual::<DeviceDatastream01>(channel, client).await?;

    let interfaces = UPDATED_INTERFACES
        .iter()
        .map(|i| Interface::from_str(i))
        .collect::<Result<Vec<Interface>, InterfaceError>>()?;

    client.extend_interfaces(interfaces).await?;

    retry(20, || async {
        let interfaces = api.interfaces().await?;
        let set = HashSet::<String>::from_iter(interfaces);

        debug!(interfaces = ?set);

        ensure!(
            set.contains(&DeviceProperty01::interface()),
            "missing interface"
        );
        ensure!(
            set.contains(&DeviceDatastream01::interface()),
            "missing interface"
        );

        Ok(())
    })
    .await?;

    validate_property::<DeviceProperty02>(channel, client, true).await?;
    validate_individual::<DeviceDatastream02>(channel, client).await?;

    Ok(())
}
