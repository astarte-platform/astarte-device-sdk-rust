# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Handle MQTT connection errors by trying to reconnect.
- Make the reconnection attempt wait with an exponential back-off.

### Changed
- Return a channel for the events when creating a device SDK.
- Make handle event loop block to handle the events.
- Create a shareable struct (`Arc`) of the `AstarteDeviceSdk` to not clone the
  device id and realm.
- Make the `DynError` trait bound shareable across threads.

### Fixed
- Unset of property send empty buffer instead of document with null value.

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
