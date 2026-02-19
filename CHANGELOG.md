<!--
This file is part of Astarte.

Copyright 2025, 2026 SECO Mind Srl

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
-->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.9.10] - 2025-11-12

### Fixed

- Update rumqttc to include latest fixes
  [#528](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/528)
- Fix the resend process after a reconnection
  [#523](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/523)

## [v0.9.9] - 2025-09-18

- Forward port release v0.8.7
  [#514](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/514)

## [v0.8.7] - 2025-09-18

### Changed

- Forward port release v0.7.6
  [#512](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/512)

## [v0.7.6] - 2025-09-17

### Changed

- Forward port release v0.6.7
  [#510](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/510)

## [0.6.7] - 2025-09-17

### Fixed

- Send the MQTT timestamp field as BSON DateTime instead of String
  [#508](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/508)

## [0.9.8] - 2025-08-01

### Fixed

- Mark object messages as sent in stored messages [#495]

## [0.9.7] - 2025-06-12

## [0.8.6] - 2025-06-06

### Changed

- Update documentation, examples and readme.
  [#461](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/461)
- Box the GrpcError::Status code since the object is too big to return on the stack
  [#466](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/466)

## [0.9.6] - 2025-03-06

### Changed

- Use a default CryptoProvider when defining a Tls configuration
  [#425](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/425).

## [0.9.5] - 2025-03-04

### Changed

- Update `rumqttc` and `rustls` dependencies [#423]
- Drop `openssl` as a dependency and deprecate the feature [#423]

### Fixed

- Fix a bug in the MQTT connection that will wait for the keep-alive timeout before connecting
  [#403]

[#423]: https://github.com/astarte-platform/astarte-device-sdk-rust/pull/423

## [0.9.4] - 2025-02-27

## [0.8.5] - 2025-02-27

## [0.7.5] - 2025-02-27

### Fixed

- On reconnection send only device properties that are currently in the introspection
  [#413](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/413/)

## [0.9.3] - 2025-01-24

### Fixed

- Update `rumqttc` to fix a bug in the MQTT unsubscribe [#403]

### Changed

- Bump `rustls` and related dependencies to version `0.23` [#403]

[#403]: https://github.com/astarte-platform/astarte-device-sdk-rust/pull/403

## [0.9.2] - 2024-11-04

### Fixed

- Send purge device properties when `session_present` is fault
  [#390](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/390)

## [0.9.1] - 2024-09-25

### Changed

- Generate the certificate using `rcgen` from `rustls`.
  [#384](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/384)

## [0.9.0] - 2024-09-24

### Added

- Update the Dynamic Introspection to support adding or removing interfaces from a MessageHub Node
  [#330](https://github.com/astarte-platform/astarte-device-sdk-rust/issues/330)
- Implement the retention stored for the `SqliteStore` and the volatile with an in memory structure.
  [#363](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/363)

### Changed

- Use Empty type rather than Node to detach a Node
  [#340](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/340/).
- Handle the new return type of the Attach rpc, `MessageHubEvent`, which can either be an error or
  an Astarte message [#362](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/362)
- Retrieve the Node ID information from the grpc metadata also for the Attach rpc
  [#372](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/372).

## [0.8.4] - 2024-09-11

### Changed

- Improve the rendering of the documentation on docs.rs, showing all the features and which one
  needs to be activated for a specific item.

## [0.8.3] - 2024-08-22

### Changed

- Derive `Clone` for the MQTT `Credential` enum
  [#369](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/369)

## [0.8.2] - 2024-05-29

## [0.7.4] - 2024-05-27

## [0.6.6] - 2024-05-27

## [0.5.4] - 2024-05-22

### Fixed

- Purge property deletes only the server property
  [#342](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/342)

## [0.8.1] - 2024-05-03

### Fixed

- Correct the interfaces iterator logic to send the correct device introspection
  [#334](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/334)

## [0.8.0] - 2024-04-29

### Added

- Introduce Node ID into gRPC metadata.
- Add one or more interfaces at once with `extend_interfaces`
  [#293](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/293)
- Add a method `unset` to unset a property
  [#296](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/296)
- Return values for the `DynamicIntrospection` to check if/which interface where added/removed
  [#326](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/326)

### Changed

- Rename the enum `Aggregation` into `Value`
  [#296](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/296)
- Move the `AstarteType::Unset` to the `Value::Unset` for the astarte event
  [#296](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/296)
- Separate the `AstarteDeviceSdk` into `DeviceClient` and `DeviceConnection`
  [#311](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/311)

## [0.7.3] - 2024-04-09

## [0.6.5] - 2024-04-08

### Fixed

- Delete all interface's properties, using the correct mapping, when an interface is removed
  [#313](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/313)

## [0.7.2] - 2024-03-21

### Fixed

- Handle Unset from gRPC correctly

## [0.6.4] - 2024-03-20

## [0.5.3] - 2024-03-20

### Added

- Add semver-check for release

### Fixed

- Property reliability as Unique

## [0.7.1] - 2024-02-16

## Changed

- `MqttConfig` now receives `Into<String>` instead of `&str`
- Bump MSRV to 1.72.0.

## [0.6.3] - 2024-02-13

## [0.5.2] - 2024-01-30

### Added

- Expose the MQTT connection timeout option.

## [0.7.0] - 2024-01-22

### Added

- Handle MQTT connection errors by trying to reconnect.
- Make the reconnection attempt wait with an exponential back-off.
- Trait `PropAccess` to access the stored properties from the `AstarteDeviceSdk`.
- Trait `FromEvent` to convert a generic object aggregate into a Rust struct.
- Implementation of the connection over GRPC to the message hub.

### Changed

- Return a channel for the events when creating a device SDK.
- Make handle event loop block to handle the events.
- Create a shareable struct (`Arc`) of the `AstarteDeviceSdk` to not clone the device id and realm.
- Make the `DynError` trait bound shareable across threads.
- Added ownership field to the `StoredProp` struct.
- The `PropertyStore::store_prop` now receives the `StoredProp` struct.
- Improve the errors with more contexts.
- Remove the deprecated and unused Errors.
- Pass `AsRef<Path>` for paths instead of `&str`.

## [0.6.2] - 2023-10-19

### Fixed

- Allow escaped character in the `Interface` description and documentation.

## [0.6.1] - 2023-10-02

### Added

- Check if an interface exists and the type is the same of the value passed/received when sending or
  receiving data from Astarte.

### Fixed

- Unset of property send empty buffer instead of document with null value.
- Deserialize mixed integer BSON arrays from Astarte to the type specified in the interface
  (longinteger and integer)

### Deprecated

- Added a warning to the `AstarteDeviceSdk::get_property` method to use the `PropAccess` trait
  instead

## [0.6.0] - 2023-07-05

### Added

- Support for different case conventions on `AstarteAggregate` derive macro
  ([#126](https://github.com/astarte-platform/astarte-device-sdk-rust/issues/126)).
- Add support to store properties in volatile memory using `MemoryStore` if no database is provided.
- Make `AstarteDeviceSdk` generic over the storage type.
- Provide type aliases for `AstarteDeviceSdk` with `MemoryStore` and `SqliteStore`.

### Changed

- Expose `pairing::PairingError` to public visibility.
- Bump `MSRV` to 1.66.1.
- The `AstartDeviceSdk` now requires an owned `AstarteOptions` instance.
- Rename the main error in `Error` and give the other errors more specific names.
- Mark all errors as `#[non_exhaustive]`.
- Renamed the `AstarteSqliteDatabase` into `SqliteStore`.
- Added a new `AstarteType` for a generic `EmptyArray`.

### Fixed

- Solve a panic when deserializing an empty BSON array.

## [0.5.1] - 2023-02-06

### Fixed

- Lock version of flate2 to support rust v1.59.

## [0.5.0] - 2023-02-01

### Added

- Initial Astarte Device SDK release.
