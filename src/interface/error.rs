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

use super::validation::VersionChange;

/// Error for parsing and validating an interface.
#[derive(thiserror::Error, Debug)]
pub enum Error {
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
}

/// Error for an interface validation.
#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    /// The new version is not a valid interface.
    #[error("the new version is invalid: {0}")]
    InvalidNew(Error),
    #[error("the previous version is invalid: {0}")]
    /// The previous version is not a valid interface.
    InvalidPrev(Error),
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

/// Error for changing the version of an interface.
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum VersionChangeError {
    /// The major version cannot be decreased.
    #[error("the major version decreased: {0}")]
    MajorDecresed(VersionChange),
    /// The minor version cannot be decreased.
    #[error("the minor version decreased: {0}")]
    MinorDecresed(VersionChange),
    // The interface is different but the version did not change.
    #[error("the version did not change: {0}")]
    SameVersion(VersionChange),
}