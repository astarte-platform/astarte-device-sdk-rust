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

//! Astarte interfaces

#![warn(clippy::dbg_macro, missing_docs, rustdoc::missing_crate_level_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod error;
pub mod interface;
pub mod mapping;
pub mod schema;

pub use self::interface::datastream::individual::DatastreamIndividual;
pub use self::interface::datastream::object::DatastreamObject;
pub use self::interface::properties::Properties;
pub use self::interface::{AggregationIndividual, Interface, Schema};

pub use self::mapping::datastream::individual::DatastreamIndividualMapping;
pub use self::mapping::datastream::object::DatastreamObjectMapping;
pub use self::mapping::endpoint::Endpoint;
pub use self::mapping::path::MappingPath;
pub use self::mapping::properties::PropertiesMapping;
pub use self::mapping::InterfaceMapping;
