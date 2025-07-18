// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use tracing::error;

use crate::{error::Report, store::SqliteStore};

use super::{IntrospectionInterface, SessionError, StoredSession};

mod statement;

impl StoredSession for SqliteStore {
    async fn store_introspection(&self, interfaces: &[IntrospectionInterface]) {
        let interfaces = interfaces.to_vec();

        let res = self
            .pool
            .acquire_writer(move |writer| writer.add_interfaces(&interfaces))
            .await;

        if let Err(err) = res {
            error!(error = %Report::new(&err), "Unexpected error in sqlite store introspection");
        }
    }

    async fn clear_introspection(&self) {
        let res = self
            .pool
            .acquire_writer(|writer| writer.clear_introspection())
            .await;

        if let Err(err) = res {
            error!(error = %Report::new(err), "Unexpected error in sqlite clear introspection");
        }
    }

    async fn add_interfaces(
        &self,
        interfaces: &[IntrospectionInterface<&str>],
    ) -> Result<(), SessionError> {
        let interfaces: Vec<IntrospectionInterface> = interfaces
            .iter()
            .map(|i| IntrospectionInterface::from(*i))
            .collect();

        self.pool
            .acquire_writer(move |writer| writer.add_interfaces(&interfaces))
            .await
            .map_err(SessionError::add_interfaces)
    }

    async fn load_introspection(&self) -> Result<Vec<IntrospectionInterface>, SessionError> {
        self.pool
            .acquire_reader(|reader| reader.load_introspection())
            .await
            .map_err(SessionError::load_introspection)
    }

    async fn remove_interfaces(
        &self,
        interfaces: &[IntrospectionInterface<&str>],
    ) -> Result<(), SessionError> {
        let interfaces: Vec<IntrospectionInterface> = interfaces
            .iter()
            .map(|i| IntrospectionInterface::from(*i))
            .collect();

        self.pool
            .acquire_writer(move |writer| writer.remove_interfaces(&interfaces))
            .await
            .map_err(SessionError::remove_interfaces)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::{
        session::{IntrospectionInterface, StoredSession},
        store::SqliteStore,
    };

    #[tokio::test]
    async fn should_add_or_replace_interface() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        const TO_ADD_NAME: &str = "com.test.Test1";
        const TO_ADD_NAME_1: &str = "com.test.TestDifferent";
        const TO_ADD_NAME_2: &str = "com.test.TestOtherDifferent";

        // test simple add
        let interface = IntrospectionInterface {
            name: TO_ADD_NAME.to_owned(),
            version_major: 0,
            version_minor: 1,
        };

        store.add_interfaces(&[interface.as_ref()]).await.unwrap();
        let stored = store.load_introspection().await.unwrap();
        assert_eq!(vec![interface.clone()], stored);

        // test replace or insert interface
        let mut interfaces = vec![
            // replaces the old one
            interface.clone(),
            IntrospectionInterface {
                name: TO_ADD_NAME_1.to_owned(),
                version_major: 1,
                version_minor: 0,
            },
            IntrospectionInterface {
                name: TO_ADD_NAME_2.to_owned(),
                version_major: 1,
                version_minor: 0,
            },
        ];
        let interfaces_ref: Vec<IntrospectionInterface<&str>> =
            interfaces.iter().map(|i| i.as_ref()).collect();

        store.add_interfaces(&interfaces_ref).await.unwrap();

        let mut stored = store.load_introspection().await.unwrap();
        interfaces.sort_unstable();
        stored.sort_unstable();
        assert_eq!(interfaces, stored);

        // test replace or insert interface with new version
        let mut interfaces = vec![
            IntrospectionInterface {
                name: TO_ADD_NAME.to_owned(),
                version_major: 1,
                version_minor: 0,
            },
            // even if an earlier version is passed interfaces get replaced
            IntrospectionInterface {
                name: TO_ADD_NAME_1.to_owned(),
                version_major: 0,
                version_minor: 1,
            },
            IntrospectionInterface {
                name: TO_ADD_NAME_2.to_owned(),
                version_major: 1,
                version_minor: 1,
            },
        ];
        let interfaces_ref: Vec<IntrospectionInterface<&str>> =
            interfaces.iter().map(|i| i.as_ref()).collect();

        store.add_interfaces(&interfaces_ref).await.unwrap();

        let mut stored = store.load_introspection().await.unwrap();

        interfaces.sort_unstable();
        stored.sort_unstable();

        assert_eq!(interfaces, stored);
    }

    #[tokio::test]
    async fn should_remove_interface() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        const TO_REMOVE_NAME: &str = "com.test.Test1";
        const TO_REMOVE_NAME_1: &str = "com.test.TestDifferent";
        const TO_REMOVE_NAME_2: &str = "com.test.TestOtherDifferent";

        // add test interface
        let interface = IntrospectionInterface {
            name: TO_REMOVE_NAME.to_owned(),
            version_major: 0,
            version_minor: 1,
        };

        // add interface
        store.add_interfaces(&[interface.as_ref()]).await.unwrap();
        let stored = store.load_introspection().await.unwrap();
        assert_eq!(vec![interface.clone()], stored);
        // test remove same interface
        store
            .remove_interfaces(&[interface.as_ref()])
            .await
            .unwrap();
        let stored = store.load_introspection().await.unwrap();
        // removed
        assert!(stored.is_empty());

        // test remove different minor
        let interface_different_min = IntrospectionInterface {
            name: TO_REMOVE_NAME.to_owned(),
            version_major: 1,
            version_minor: 0,
        };

        // add interface
        store.add_interfaces(&[interface.as_ref()]).await.unwrap();
        let stored = store.load_introspection().await.unwrap();
        assert_eq!(vec![interface.clone()], stored);
        // remove same name different minor
        store
            .remove_interfaces(&[interface_different_min.as_ref()])
            .await
            .unwrap();
        // nothing removed (all parameters have to match)
        let stored = store.load_introspection().await.unwrap();
        assert_eq!(vec![interface.clone()], stored);

        // rest remove multiple interfaces
        let other_interfaces = vec![
            IntrospectionInterface {
                name: TO_REMOVE_NAME_1.to_owned(),
                version_major: 1,
                version_minor: 1,
            },
            IntrospectionInterface {
                name: TO_REMOVE_NAME_2.to_owned(),
                version_major: 1,
                version_minor: 1,
            },
        ];
        let other_interfaces_ref: Vec<IntrospectionInterface<&str>> =
            other_interfaces.iter().map(|i| i.as_ref()).collect();
        // add interfaces
        store.add_interfaces(&other_interfaces_ref).await.unwrap();
        let mut stored = store.load_introspection().await.unwrap();
        let mut expected = other_interfaces.clone();
        expected.push(interface.clone());
        expected.sort_unstable();
        stored.sort_unstable();
        assert_eq!(stored, expected);
        // remove two interfaces
        store
            .remove_interfaces(&other_interfaces_ref)
            .await
            .unwrap();
        // nothing removed
        let stored = store.load_introspection().await.unwrap();
        assert_eq!(stored, vec![interface.clone()]);
    }

    #[tokio::test]
    async fn should_clear_interfaces() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        const TO_ADD_NAME: &str = "com.test.Test1";
        const TO_ADD_NAME_1: &str = "com.test.TestDifferent";
        const TO_ADD_NAME_2: &str = "com.test.TestOtherDifferent";

        // test clear one interface
        let interface = IntrospectionInterface {
            name: TO_ADD_NAME.to_owned(),
            version_major: 0,
            version_minor: 1,
        };
        store.add_interfaces(&[interface.as_ref()]).await.unwrap();
        assert_eq!(1, store.load_introspection().await.unwrap().len());
        // test clear
        store.clear_introspection().await;
        assert!(store.load_introspection().await.unwrap().is_empty());

        // test clear multiple interfaces
        let interfaces = [
            // replaces the old one
            interface.clone(),
            IntrospectionInterface {
                name: TO_ADD_NAME_1.to_owned(),
                version_major: 1,
                version_minor: 0,
            },
            IntrospectionInterface {
                name: TO_ADD_NAME_2.to_owned(),
                version_major: 1,
                version_minor: 0,
            },
        ];
        let interfaces_ref: Vec<IntrospectionInterface<&str>> =
            interfaces.iter().map(|i| i.as_ref()).collect();

        store.add_interfaces(&interfaces_ref).await.unwrap();
        assert_eq!(3, store.load_introspection().await.unwrap().len());
        store.clear_introspection().await;
        assert!(store.load_introspection().await.unwrap().is_empty())
    }

    #[tokio::test]
    async fn should_store_introspection() {
        let dir = tempfile::tempdir().unwrap();

        let store = SqliteStore::connect(dir.path()).await.unwrap();

        const TO_ADD_NAME: &str = "com.test.Test1";
        const TO_ADD_NAME_1: &str = "com.test.TestDifferent";
        const TO_ADD_NAME_2: &str = "com.test.TestOtherDifferent";

        // test simple add
        let interface = IntrospectionInterface {
            name: TO_ADD_NAME.to_owned(),
            version_major: 0,
            version_minor: 1,
        };

        store
            .store_introspection(std::slice::from_ref(&interface))
            .await;
        let stored = store.load_introspection().await.unwrap();
        assert_eq!(vec![interface.clone()], stored);

        // test replace or insert interface
        let mut interfaces = vec![
            // replaces the old one
            interface.clone(),
            IntrospectionInterface {
                name: TO_ADD_NAME_1.to_owned(),
                version_major: 1,
                version_minor: 0,
            },
            IntrospectionInterface {
                name: TO_ADD_NAME_2.to_owned(),
                version_major: 1,
                version_minor: 0,
            },
        ];

        store.store_introspection(&interfaces).await;

        let mut stored = store.load_introspection().await.unwrap();
        interfaces.sort_unstable();
        stored.sort_unstable();
        assert_eq!(interfaces, stored);
    }
}
