// This file is part of Astarte.
// Copyright 2023 SECO Mind Srl
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

//! Errors generated by the interface module.

use std::io;

use super::{mapping::endpoint::EndpointError, validation::VersionChangeError};

/// Error for parsing and validating an interface.
#[derive(thiserror::Error, Debug)]
pub enum InterfaceError {
    #[error("cannot parse interface JSON")]
    Parse(#[from] serde_json::Error),
    #[error("cannot read interface file")]
    Io(#[from] io::Error),
    #[error("wrong major and minor")]
    MajorMinor,
    #[error("interface not found")]
    InterfaceNotFound,
    #[error("mapping not found")]
    MappingNotFound,
    #[error("Database retention policy set to `use_ttl` but the TTL was not specified")]
    MissingTtl,
    #[error("invalid endpoint")]
    InvalidEndpoint(#[from] EndpointError),
    #[error("interface with no mappings")]
    EmptyMappings,
    #[error("object with inconsistent mappings")]
    InconsistentMapping,
    #[error("object with inconsistent endpoints")]
    InconsistentEndpoints,
    #[error("duplicate endpoint mapping '{endpoint}' and '{duplicate}'")]
    DuplicateMapping { endpoint: String, duplicate: String },
    #[error("object endpoint should have at least 2 levels: '{0}'")]
    ObjectEndpointTooShort(String),
    /// The name of the interface was changed.
    #[error(
        r#"this version has a different name than the previous version
    name: {name}
    prev_name: {prev_name}"#
    )]
    NameMismatch { name: String, prev_name: String },
    /// Invalid version change.
    #[error("invalid version: {0}")]
    Version(VersionChangeError),
}
