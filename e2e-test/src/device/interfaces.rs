use std::collections::HashSet;
use std::str::FromStr;

use astarte_device_sdk::prelude::DynamicIntrospection;
use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::{DeviceClient, Interface};
use eyre::ensure;
use tracing::{debug, instrument};

use crate::api::ApiClient;
use crate::retry;

#[instrument(skip_all)]
pub(crate) async fn check_add(
    api: &ApiClient,
    client: &DeviceClient<SqliteStore>,
) -> eyre::Result<()> {
    let mut expected = HashSet::<String>::from_iter(
        [
            "org.astarte-platform.rust.e2etest.DeviceAggregate",
            "org.astarte-platform.rust.e2etest.DeviceDatastream",
            "org.astarte-platform.rust.e2etest.ServerAggregate",
        ]
        .map(str::to_string),
    );

    retry(20, || async {
        let interfaces = api.interfaces().await?;
        let set = HashSet::<String>::from_iter(interfaces);

        debug!(interfaces = ?set);

        ensure!(expected.is_subset(&set), "missing interfaces");

        Ok(())
    })
    .await?;

    expected.extend(
        [
            "org.astarte-platform.rust.e2etest.DeviceProperty",
            "org.astarte-platform.rust.e2etest.ServerDatastream",
            "org.astarte-platform.rust.e2etest.ServerProperty",
        ]
        .map(str::to_string),
    );

    let additional_interfaces = read_additional_interfaces()?;
    // Add the remaining interfaces
    debug!("adding {} interfaces", additional_interfaces.len());
    client.extend_interfaces(additional_interfaces).await?;

    retry(20, || async {
        let interfaces = api.interfaces().await?;
        let set = HashSet::<String>::from_iter(interfaces);

        debug!(interfaces = ?set);

        ensure!(expected.is_subset(&set), "missing interfaces");

        Ok(())
    })
    .await?;

    Ok(())
}

#[instrument(skip_all)]
pub(crate) async fn check_remove(
    api: &ApiClient,
    client: &DeviceClient<SqliteStore>,
) -> eyre::Result<()> {
    let expected = HashSet::<String>::from_iter(
        [
            "org.astarte-platform.rust.e2etest.DeviceAggregate",
            "org.astarte-platform.rust.e2etest.DeviceDatastream",
            "org.astarte-platform.rust.e2etest.ServerAggregate",
        ]
        .map(str::to_string),
    );

    client
        .remove_interfaces(
            [
                "org.astarte-platform.rust.e2etest.DeviceProperty",
                "org.astarte-platform.rust.e2etest.ServerDatastream",
                "org.astarte-platform.rust.e2etest.ServerProperty",
            ]
            .map(str::to_string),
        )
        .await?;

    retry(20, || async {
        let interfaces = api.interfaces().await?;
        let set = HashSet::<String>::from_iter(interfaces);

        debug!(interfaces = ?set);

        ensure!(expected.is_subset(&set), "missing interfaces");

        Ok(())
    })
    .await?;

    Ok(())
}
fn read_additional_interfaces() -> eyre::Result<Vec<Interface>> {
    [
        include_str!(
            "../../interfaces/additional/org.astarte-platform.rust.e2etest.DeviceProperty.json"
        ),
        include_str!(
            "../../interfaces/additional/org.astarte-platform.rust.e2etest.ServerProperty.json"
        ),
        include_str!(
            "../../interfaces/additional/org.astarte-platform.rust.e2etest.ServerDatastream.json"
        ),
    ]
    .into_iter()
    .map(|i| Interface::from_str(i).map_err(Into::into))
    .collect()
}
