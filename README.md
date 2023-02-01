<!--
Copyright 2021,2022 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte Device SDK Rust

*Warning: this SDK is experimental, correctness and API stability are currently not guaranteed*

The Astarte Device SDK for Rust is a ready to use library that provides communication and
pairing primitives to an Astarte Cluster.

See the [Astarte documentation](https://docs.astarte-platform.org/latest/001-intro_user.html)
for more information regarding Astarte and the available SDKs.

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
