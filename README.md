<!--
Copyright 2021,2022 SECO Mind Srl

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

*Warning: this SDK is experimental, correctness and API stability are currently not guaranteed*

The Astarte Device SDK for Rust is a ready to use library that provides communication and
pairing primitives to an Astarte Cluster.

See the [Astarte documentation](https://docs.astarte-platform.org/latest/001-intro_user.html)
for more information regarding Astarte and the available SDKs.

See the [Get started](docs/get-started.md) guide for a better understanding of the Rust SDK functionalities.

## Building the library

You can build the library using:
```sh
cargo build
```

## Examples

Check out how to start with the SDK using one of the [included examples](./examples/README.md).
