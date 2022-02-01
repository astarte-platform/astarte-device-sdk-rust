<!--
Copyright 2021,2022 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte Device SDK Rust

*Warning: this SDK is experimental, correctness and API stability are currently not guaranteed*

This package allows you to implement an Astarte Device using Rust.

## Building

You can build the SDK and the example with

```sh
cargo build
```

## Example

After building, you can run the example with

```sh
cargo run --example simple -- \
    --credentials-secret <credentials-secret>
    --device-id <device-id>
    --pairing-url <pairing-url>
    --realm <realm>
```
