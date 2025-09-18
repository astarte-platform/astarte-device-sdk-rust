<!--
This file is part of Astarte.

Copyright 2023 - 2025 SECO Mind Srl

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

## [v0.9.9] - 2025-09-18

## [v0.8.7] - 2025-09-18

## [0.7.6] - 2025-09-17

## [0.6.7] - 2025-09-17

## [0.9.8] - 2025-08-01

## [0.9.7] - 2025-06-12

## [0.8.6] - 2025-06-06

## [0.9.6] - 2025-03-06

## [0.9.5] - 2025-03-04

## [0.9.4] - 2025-02-27

## [0.8.5] - 2025-02-27

## [0.7.5] - 2025-02-27

## [0.9.3] - 2025-01-24

## [0.9.2] - 2024-11-04

## [0.9.1] - 2024-09-25

## [0.9.0] - 2024-09-24

### Changed

- Implement the FromEvent derive macro for individual interfaces, via the `aggregation` attribute to
  the macro [#375](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/375)
- Add the `allow_unset` attribute to permit `Option` values for `Value::Unset`
  [#378](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/378)

## [0.8.4] - 2024-09-11

## [0.8.3] - 2024-08-22

## [0.8.2] - 2024-05-29

## [0.7.4] - 2024-05-27

## [0.6.6] - 2024-05-27

## [0.5.4] - 2024-05-22

## [0.8.1] - 2024-05-03

## [0.8.0] - 2024-04-29

## [0.7.3] - 2024-04-09

## [0.6.5] - 2024-04-08

## [0.7.2] - 2024-03-21

## [0.6.4] - 2024-03-20

## [0.5.3] - 2024-03-20

## [0.7.1] - 2024-02-16

- Bump MSRV to 1.72.0.

## [0.6.3] - 2024-02-13

## [0.5.2] - 2024-01-30

## [0.7.0] - 2024-01-22

### Added

- Macro to implement the `FromEvent` trait on a generic struct.

### Changed

- Update the `AstarteAggregate` derive macro to syn `2`, see
  [#236](https://github.com/astarte-platform/astarte-device-sdk-rust/pull/236).

## [0.6.2] - 2023-10-19

## [0.6.1] - 2023-10-02

## [0.6.0] - 2023-07-05

## [0.5.1] - 2023-02-06

## [0.5.0] - 2023-02-01

### Added

- Initial Astarte Device SDK Derive release
