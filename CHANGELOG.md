# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - Unreleased
### Added
- Support for different case conventions on `AstarteAggregate` derive macro
  ([#126](https://github.com/astarte-platform/astarte-device-sdk-rust/issues/126)).

### Changed
- Expose `pairing::PairingError` to public visibility.
- Bump `MSRV` to 1.66.1.
- The `AstartDeviceSdk` now requires an owned `AstarteOptions` instance.
- Rename the main error in `Error` and give the other errors more specific names.
- Mark all errors as `#[non_exhaustive]`.

## [0.5.1] - 2023-02-06
### Fixed
- Lock version of flate2 to support rust v1.59.

## [0.5.0] - 2023-02-01
### Added
- Initial Astarte Device SDK release.
