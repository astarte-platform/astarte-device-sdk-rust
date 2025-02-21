// This file is part of Astarte.
//
// Copyright 2023 - 2025 SECO Mind Srl
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

//! Mapping of an interface.

use endpoint::EndpointError;

use self::endpoint::Endpoint;

use crate::interface::MAX_INTERFACE_MAPPINGS;
use crate::schema::MappingType;

pub mod collection;
pub mod datastream;
pub mod endpoint;
pub mod path;
pub mod properties;

/// Error returned by the interface mappings.
#[derive(Debug, thiserror::Error)]
pub enum MappingError {
    /// An interface must have at least one mapping.
    #[error("an interface must have at lease one mapping")]
    Empty,
    /// An interface can have at max [`MAX_INTERFACE_MAPPINGS`] mappings.
    #[error(
        "too many mappings {0}, interfaces can have a max of {max} mappings",
        max = MAX_INTERFACE_MAPPINGS
    )]
    TooMany(usize),
    /// The endpoint of all the mappings of an interface must have an unique endpoint.
    ///
    /// This includes the parameters.
    #[error("the mappings has a duplicated endpoint {endpoint}")]
    Duplicated {
        /// The first duplicated endpoint
        endpoint: String,
    },
    /// Couldn't parse the mapping's endpoint
    #[error("couldn't parse the mapping's endpoint")]
    Endpoint(#[from] EndpointError),
    /// The object interface endpoints should have at least 2 levels.
    #[error("object endpoint should have at least 2 levels: '{0}'")]
    TooShortForObject(String),
}

/// Mapping of an interface.
pub trait InterfaceMapping {
    /// Returns a reference to the endpoint of an interface.
    fn endpoint(&self) -> &Endpoint<String>;
    /// Returns the mapping type.
    fn mapping_type(&self) -> MappingType;
    /// Returns the description of the mapping.
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    fn description(&self) -> Option<&str>;
    /// Returns the documentation of the mapping.
    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    fn doc(&self) -> Option<&str>;
}
