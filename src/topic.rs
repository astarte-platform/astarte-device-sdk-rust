// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Parses a topic into the different components.

use log::trace;

use crate::interface::mapping::path::{MappingError, MappingPath};

/// Error returned when parsing a topic.
///
/// We expect the topic to be in the form `<realm>/<device_id>/<interface>/<path>`.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum TopicError {
    #[error("topic is empty")]
    Empty,
    #[error(
        "the topic should be in the form <realm>/<device_id>/<interface>/<path>, received: {0}"
    )]
    Malformed(String),

    #[error("couldn't parse mapping for '{topic}'")]
    Maapping {
        #[source]
        err: MappingError,
        topic: String,
    },
}

impl TopicError {
    pub fn topic(&self) -> &str {
        match self {
            TopicError::Empty => "",
            TopicError::Malformed(topic) => topic,
            TopicError::Maapping { topic, .. } => topic,
        }
    }
}

pub(crate) fn parse_topic(topic: &str) -> Result<(&str, &str, &str, MappingPath<'_>), TopicError> {
    if topic.is_empty() {
        return Err(TopicError::Empty);
    }

    let mut parts = topic.splitn(3, '/');

    let realm = parts
        .next()
        .ok_or_else(|| TopicError::Malformed(topic.to_string()))?;

    trace!("realm: {}", realm);

    let device = parts
        .next()
        .ok_or_else(|| TopicError::Malformed(topic.to_string()))?;

    trace!("device: {}", device);

    let rest = parts
        .next()
        .ok_or_else(|| TopicError::Malformed(topic.to_string()))?;

    trace!("rest: {}", rest);

    let idx = rest
        .find('/')
        .ok_or_else(|| TopicError::Malformed(topic.to_string()))?;

    trace!("slash idx: {}", idx);

    let (interface, path) = rest.split_at(idx);

    trace!("interface: {}", interface);
    trace!("path: {}", path);

    if interface.is_empty() || path.is_empty() {
        return Err(TopicError::Malformed(topic.to_string()));
    }

    let path = MappingPath::try_from(path).map_err(|err| TopicError::Maapping {
        err,
        topic: topic.to_string(),
    })?;

    Ok((realm, device, interface, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_topic() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/com.interface.test/led/red".to_owned();
        let (realm, device, interface, path) = parse_topic(&topic).unwrap();

        assert_eq!(realm, "test");
        assert_eq!(device, "u-WraCwtK_G_fjJf63TiAw");
        assert_eq!(interface, "com.interface.test");
        assert_eq!(path, "/led/red");
    }

    #[test]
    fn test_parse_topic_empty() {
        let topic = "".to_owned();
        let err = parse_topic(&topic).unwrap_err();

        assert!(matches!(err, TopicError::Empty));
    }

    #[test]
    fn test_parse_topic_malformed() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/com.interface.test".to_owned();
        let err = parse_topic(&topic).unwrap_err();

        assert!(matches!(err, TopicError::Malformed(_)));
    }
}
