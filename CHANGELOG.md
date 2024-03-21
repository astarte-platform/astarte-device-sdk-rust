# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Introduce Node ID into gRPC metadata.

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
- Trait `PropAccess` to access the stored properties from the
  `AstarteDeviceSdk`.
- Trait `FromEvent` to convert a generic object aggregate into a Rust struct.
- Implementation of the connection over GRPC to the message hub.

### Changed
- Return a channel for the events when creating a device SDK.
- Make handle event loop block to handle the events.
- Create a shareable struct (`Arc`) of the `AstarteDeviceSdk` to not clone the
  device id and realm.
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
- Check if an interface exists and the type is the same of the value
  passed/received when sending or receiving data from Astarte.

### Fixed
- Unset of property send empty buffer instead of document with null value.
- Deserialize mixed integer BSON arrays from Astarte to the type specified in
  the interface (longinteger and integer)

### Deprecated
- Added a warning to the `AstarteDeviceSdk::get_property` method to use the
  `PropAccess` trait instead

## [0.6.0] - 2023-07-05
### Added
- Support for different case conventions on `AstarteAggregate` derive macro
  ([#126](https://github.com/astarte-platform/astarte-device-sdk-rust/issues/126)).
- Add support to store properties in volatile memory using `MemoryStore` if no
  database is provided.
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
