<!--
This file is part of Astarte.

Copyright 2021 - 2025 SECO Mind Srl

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

# Astarte Device SDK Rust &emsp;

[![Build Status]][actions] [![Latest Version]][crates.io] [![docs.rs]][docs] [![Code coverage]][codecov]

[Build Status]: https://img.shields.io/github/actions/workflow/status/astarte-platform/astarte-device-sdk-rust/ci.yaml?branch=master
[actions]: https://github.com/astarte-platform/astarte-device-sdk-rust/actions/workflows/ci.yaml?query=branch%3Amaster
[Latest Version]: https://img.shields.io/crates/v/astarte-device-sdk.svg
[crates.io]: https://crates.io/crates/astarte-device-sdk
[docs.rs]: https://img.shields.io/docsrs/astarte-device-sdk
[docs]: https://docs.rs/astarte-device-sdk/latest/astarte_device_sdk/
[Code coverage]: https://codecov.io/gh/astarte-platform/astarte-device-sdk-rust/branch/master/graph/badge.svg
[codecov]: https://codecov.io/gh/astarte-platform/astarte-device-sdk-rust

The Astarte Device SDK for Rust is a ready to use library that provides communication and
pairing primitives to an Astarte Cluster.

Quick links:

- [API documentation](https://docs.rs/astarte-device-sdk/latest/astarte_device_sdk/).
- [Astarte documentation](https://docs.astarte-platform.org/latest/001-intro_user.html) for more information regarding Astarte.
- [Get started](https://docs.rs/astarte-device-sdk/latest/astarte_device_sdk/_docs/_get_started/index.html) for a guide on how to use the SDK functionalities.
- [Connect to the Astarte MessageHub](https://docs.rs/astarte-device-sdk/latest/astarte_device_sdk/_docs/_connect_to_the_astarte_msghub/index.html) tutorial.
- [OS requirements](https://github.com/astarte-platform/astarte-device-sdk-rust/tree/master/docs/os-requirements.md) for system libraries.

## Use the library

You can add the library to your project with:

```sh
cargo new astarte-project && cd astarte-project
cargo add astarte-device-sdk --features='derive'
```

## Examples

Check out how to start with the SDK using one of the [included examples](https://github.com/astarte-platform/astarte-device-sdk-rust/tree/master/examples/README.md).
