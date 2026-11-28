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

use tracing::instrument;

use crate::AstarteClient;
use crate::api::ApiClient;
use crate::data::{InterfaceData, InterfaceDataObject};

use super::individual::validate_individual;
use super::object::validate_object;

#[derive(Debug)]
struct EncryptedServerDatastream {}

impl InterfaceData for EncryptedServerDatastream {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.EncryptedServerDatastream".to_string()
    }
}

#[derive(Debug)]
struct EncryptedServerAggregate {}

impl InterfaceData for EncryptedServerAggregate {
    fn interface() -> String {
        "org.astarte-platform.rust.e2etest.EncryptedServerAggregate".to_string()
    }
}

impl InterfaceDataObject for EncryptedServerAggregate {}

#[instrument(skip_all)]
pub(crate) async fn check(api: &ApiClient, client: &AstarteClient) -> eyre::Result<()> {
    validate_individual::<EncryptedServerDatastream>(api, client).await?;
    validate_object::<EncryptedServerAggregate>(api, client).await?;

    Ok(())
}
