# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.4] - 2024-05-22
### Fixed
- Purge property deletes only the server property [#342](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/342)

## [0.6.5] - 2024-04-08
### Fixed
- Delete all interface's properties, using the correct mapping, when an
  interface is removed
  [#313](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/313)

## [0.6.4] - 2024-03-20

## [0.5.3] - 2024-03-20
### Added
- Add semver-check for release

### Fixed
- Property reliability as Unique

## [0.6.3] - 2024-02-13

## [0.5.2] - 2024-01-30
### Added
- Expose the MQTT connection timeout option.

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
