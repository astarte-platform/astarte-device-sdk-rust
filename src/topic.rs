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

/// Error returned when parsing a topic.
///
/// We expect the topic to be in the form `<realm>/<device_id>/<interface>/<path>`.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum TopicError {
    #[error("topic is empty")]
    Empty,
    #[error(
        "the topic should start with <realm>/<device_id> equal to {client_id}, received: {topic}"
    )]
    UnknownClientId { client_id: String, topic: String },
    #[error(
        "the topic should be in the form <realm>/<device_id>/<interface>/<path>, received: {0}"
    )]
    Malformed(String),
}

impl TopicError {
    pub fn topic(&self) -> &str {
        match self {
            TopicError::Empty => "",
            TopicError::UnknownClientId { topic, .. } => topic,
            TopicError::Malformed(topic) => topic,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ParsedTopic<'a> {
    pub(crate) interface: &'a str,
    pub(crate) path: &'a str,
}

impl<'a> ParsedTopic<'a> {
    pub(crate) fn try_parse(client_id: &str, topic: &'a str) -> Result<Self, TopicError> {
        if topic.is_empty() {
            return Err(TopicError::Empty);
        }

        let rest = topic
            .strip_prefix(client_id)
            .ok_or(TopicError::UnknownClientId {
                client_id: client_id.to_string(),
                topic: topic.to_string(),
            })?;

        let rest = rest
            .strip_prefix('/')
            .ok_or(TopicError::Malformed(topic.to_string()))?;

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

        Ok(ParsedTopic { interface, path })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLIENT_ID: &str = "test/u-WraCwtK_G_fjJf63TiAw";

    #[test]
    fn test_parse_topic() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/com.interface.test/led/red".to_owned();
        let ParsedTopic { interface, path } = ParsedTopic::try_parse(CLIENT_ID, &topic).unwrap();

        assert_eq!(interface, "com.interface.test");
        assert_eq!(path, "/led/red");
    }

    #[test]
    fn test_parse_topic_empty() {
        let topic = "".to_owned();
        let err = ParsedTopic::try_parse(CLIENT_ID, &topic).unwrap_err();

        assert!(matches!(err, TopicError::Empty));
    }

    #[test]
    fn test_parse_topic_client_id() {
        let err = ParsedTopic::try_parse(CLIENT_ID, CLIENT_ID).unwrap_err();

        assert!(matches!(err, TopicError::Malformed(_)));
    }

    #[test]
    fn test_parse_topic_malformed() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/com.interface.test".to_owned();
        let err = ParsedTopic::try_parse(CLIENT_ID, &topic).unwrap_err();

        assert!(matches!(err, TopicError::Malformed(_)));
    }

    #[test]
    fn test_parse_unknown_client_id() {
        let topic = "test/u-WraCwtK_G_different/com.interface.test/led/red".to_owned();
        let err = ParsedTopic::try_parse(CLIENT_ID, &topic).unwrap_err();

        assert!(matches!(err, TopicError::UnknownClientId { .. }));
    }
}
