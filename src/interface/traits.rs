/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use super::{BaseInterface, BaseMapping, MappingType, Ownership};
use itertools::{EitherOrBoth::*, Itertools};

pub(crate) trait Interface {
    fn base_interface(&self) -> &BaseInterface;

    fn name(&self) -> &str {
        self.base_interface().interface_name.as_ref()
    }

    fn version(&self) -> (i32, i32) {
        let major = self.base_interface().version_major;

        let minor = self.base_interface().version_minor;

        (major, minor)
    }

    fn ownership(&self) -> Ownership {
        self.base_interface().ownership
    }

    fn description(&self) -> Option<&str> {
        self.base_interface().description.as_deref()
    }

    fn doc(&self) -> Option<&str> {
        self.base_interface().doc.as_deref()
    }
}

pub(crate) trait Mapping {
    fn base_mapping(&self) -> &BaseMapping;

    fn endpoint(&self) -> &str {
        self.base_mapping().endpoint.as_ref()
    }

    fn mapping_type(&self) -> MappingType {
        self.base_mapping().mapping_type
    }

    fn description(&self) -> Option<&str> {
        self.base_mapping().description.as_deref()
    }

    fn doc(&self) -> Option<&str> {
        self.base_mapping().doc.as_deref()
    }

    fn is_compatible(&self, path: &str) -> bool {
        if !path.starts_with('/') {
            return false;
        }

        let endpoint_tokens = self.endpoint().trim_start_matches('/').split('/');
        let path_tokens = path.trim_start_matches('/').split('/');

        for pair in endpoint_tokens.zip_longest(path_tokens) {
            match pair {
                // Those two means tokens were not the same size, so not compatible
                Left(_) => return false,
                Right(_) => return false,
                Both(endpoint_token, path_token) => {
                    if endpoint_token.starts_with("%{") && endpoint_token.ends_with('}') {
                        continue;
                    }

                    if endpoint_token != path_token {
                        return false;
                    }
                }
            }
        }

        true
    }
}
